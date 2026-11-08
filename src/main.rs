use bitacora::server::{instalar_senales, serve, ServerConfig};
use bitacora::{demo_value, Db, FsyncPolicy, Options, RecoveryPolicy};
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let code = match args.first().map(String::as_str) {
        Some("serve") => cmd_serve(&args[1..]),
        Some("cli") => cmd_cli(&args[1..]),
        Some("bench") => cmd_bench(&args[1..]),
        Some("__crashwrite") => cmd_crashwrite(&args[1..]),
        _ => {
            usage();
            2
        }
    };
    std::process::exit(code);
}

fn usage() {
    eprintln!(
        "bitacora: motor clave-valor desde cero

USO
  bitacora serve [--dir data] [--host 127.0.0.1] [--port 7379] [--fsync group|always|never]
                 [--fullfsync] [--segment-mb 64] [--max-conns 1024]
                 [--recuperacion estricto|reparar]
                 [--clave SECRETO] [--sin-clave]
                 [--timeout-lectura 300] [--timeout-escritura 60]
                 [--keys-max 10000] [--valor-max-mb 16]

  --recuperacion estricto (por defecto): si el WAL tiene corrupción, NO abre y no
                 modifica ningún fichero. Una recuperación incompleta nunca se
                 presenta como satisfactoria.
  --recuperacion reparar: abre saltándose los registros ilegibles, tras copiar
                 cada segmento afectado a NNNNNN.log.corrupto-<epoch>, e informa
                 de cuántos registros y bytes se perdieron.
  --clave: exige AUTH <clave> en cada conexión. También se puede dar en la
                 variable BITACORA_CLAVE, para no dejarla en la lista de
                 procesos. Sin clave, el servidor se niega a escuchar fuera de
                 localhost salvo que se le pase --sin-clave.
  --timeout-lectura / --timeout-escritura: segundos; 0 = sin límite. Sin
                 tiempos de espera, unas pocas conexiones mudas retienen todos
                 los hilos y el servidor queda inaccesible.
  --keys-max: tope de claves que devuelve KEYS. El cliente puede pedir menos
                 (KEYS prefijo limite), nunca más.
  --valor-max-mb: tamaño máximo de valor que se envía por el socket. Los
                 valores mayores siguen accesibles desde la API de Rust.

  bitacora cli   [--host 127.0.0.1] [--port 7379]
  bitacora bench [--threads 8] [--ops 200000] [--value-bytes 100] [--fsync never|group|always]
                 [--dir /tmp]"
    );
}

fn opt(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn opt_or<T: FromStr>(args: &[String], name: &str, default: T) -> T {
    opt(args, name).and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn has(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

fn fsync_opt(args: &[String], default: FsyncPolicy) -> Result<FsyncPolicy, String> {
    match opt(args, "--fsync") {
        None => Ok(default),
        Some(s) => FsyncPolicy::parse(&s).ok_or(format!("--fsync inválido: {s} (group|always|never)")),
    }
}

fn cmd_serve(args: &[String]) -> i32 {
    let mut opts = Options::new(opt(args, "--dir").unwrap_or_else(|| "data".into()));
    opts.fsync = match fsync_opt(args, FsyncPolicy::Group) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };
    opts.full_fsync = has(args, "--fullfsync");
    opts.max_segment_bytes = opt_or::<u64>(args, "--segment-mb", 64) << 20;
    opts.recovery = match opt(args, "--recuperacion") {
        None => RecoveryPolicy::Estricto,
        Some(s) => match RecoveryPolicy::parse(&s) {
            Some(p) => p,
            None => {
                eprintln!("--recuperacion inválido: {s} (estricto|reparar)");
                return 2;
            }
        },
    };
    let mut cfg = ServerConfig::new(
        opt(args, "--host").unwrap_or_else(|| "127.0.0.1".into()),
        opt_or(args, "--port", 7379),
    );
    cfg.max_conns = opt_or(args, "--max-conns", 1024);
    cfg.keys_limit = opt_or(args, "--keys-max", 10_000);
    cfg.max_value_bytes = opt_or::<usize>(args, "--valor-max-mb", 16) << 20;
    cfg.permitir_sin_clave = has(args, "--sin-clave");
    // La clave puede venir del entorno para no dejarla en la lista de procesos.
    cfg.clave = opt(args, "--clave").or_else(|| std::env::var("BITACORA_CLAVE").ok());
    let segundos = |n: u64| if n == 0 { None } else { Some(Duration::from_secs(n)) };
    cfg.read_timeout = segundos(opt_or(args, "--timeout-lectura", 300));
    cfg.write_timeout = segundos(opt_or(args, "--timeout-escritura", 60));
    // M3: SIGINT y SIGTERM pasan a pedir un apagado ordenado.
    instalar_senales();
    let t0 = Instant::now();
    let db = match Db::open(opts.clone()) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("bitacora: no se pudo abrir {}: {e}", opts.dir.display());
            return 1;
        }
    };
    let st = db.stats();
    // Una recuperación con pérdidas no se presenta nunca como satisfactoria.
    let rep = db.recovery_report();
    let aviso = if rep.con_perdidas() { " (RECUPERACIÓN CON PÉRDIDAS)" } else { "" };
    eprintln!(
        "bitacora: {} claves{} recuperadas de {} segmento(s) ({} bytes) en {:.1} ms · fsync={} · dir={}",
        st.keys,
        aviso,
        st.segments,
        st.disk_bytes,
        t0.elapsed().as_secs_f64() * 1000.0,
        st.fsync,
        opts.dir.display()
    );
    let db = Arc::new(db);
    if let Err(e) = serve(Arc::clone(&db), cfg) {
        eprintln!("bitacora: servidor: {e}");
        return 1;
    }
    // M3: apagado ordenado. `close` para el hilo de sincronización, espera a
    // que acabe una compactación en marcha y hace el fsync final. Sin esto,
    // Ctrl-C se lo llevaba todo por delante.
    if let Err(e) = db.close() {
        eprintln!("bitacora: al cerrar: {e}");
        return 1;
    }
    eprintln!("bitacora: cerrado limpiamente");
    0
}

fn cmd_cli(args: &[String]) -> i32 {
    let host = opt(args, "--host").unwrap_or_else(|| "127.0.0.1".into());
    let port: u16 = opt_or(args, "--port", 7379);
    let stream = match TcpStream::connect((host.as_str(), port)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("no se pudo conectar a {host}:{port}: {e}");
            return 1;
        }
    };
    let mut rd = BufReader::new(stream.try_clone().unwrap());
    let mut wr = stream;
    let stdin = io::stdin();
    let mut out = io::stdout();
    eprintln!("conectado a {host}:{port}. Órdenes: SET GET DEL EXISTS KEYS STATS COMPACT PING QUIT");
    loop {
        print!("bitacora> ");
        let _ = out.flush();
        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        if wr.write_all(line.as_bytes()).is_err() {
            break;
        }
        let mut resp = String::new();
        if rd.read_line(&mut resp).unwrap_or(0) == 0 {
            break;
        }
        print!("{resp}");
        let first = resp.trim_end();
        let extra = first
            .strip_prefix("KEYS ")
            .or_else(|| first.strip_prefix("STATS "))
            .and_then(|n| n.parse::<usize>().ok())
            .unwrap_or(0);
        for _ in 0..extra {
            let mut l = String::new();
            if rd.read_line(&mut l).unwrap_or(0) == 0 {
                break;
            }
            print!("  {l}");
        }
        if first == "BYE" {
            break;
        }
    }
    0
}

fn cmd_bench(args: &[String]) -> i32 {
    let threads: usize = opt_or(args, "--threads", 8);
    let ops: u64 = opt_or(args, "--ops", 200_000);
    let vbytes: usize = opt_or(args, "--value-bytes", 100);
    let fsync = match fsync_opt(args, FsyncPolicy::Never) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };
    let base = PathBuf::from(opt(args, "--dir").unwrap_or_else(|| std::env::temp_dir().to_string_lossy().into_owned()));
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    let dir = base.join(format!("bitacora-bench-{}-{nanos}", std::process::id()));

    let mut opts = Options::new(&dir);
    opts.fsync = fsync;
    opts.full_fsync = has(args, "--fullfsync");
    let db = Arc::new(Db::open(opts).expect("abrir bench"));
    let per = ops / threads as u64;
    let value = vec![b'x'; vbytes];

    println!("bitacora bench · {threads} hilos · {ops} ops · valor {vbytes} B · fsync={}", fsync.name());

    let t0 = Instant::now();
    let hs: Vec<_> = (0..threads as u64)
        .map(|t| {
            let db = Arc::clone(&db);
            let value = value.clone();
            thread::spawn(move || {
                for j in 0..per {
                    let key = format!("k{:010}", t * per + j);
                    db.put(key.as_bytes(), &value).expect("put");
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    let set_dt = t0.elapsed();

    let t1 = Instant::now();
    let hs: Vec<_> = (0..threads as u64)
        .map(|t| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let mut found = 0u64;
                for j in 0..per {
                    // cada hilo lee claves de otro hilo: lecturas cruzadas entre shards
                    let key = format!("k{:010}", ((t + 1) % threads as u64) * per + j);
                    if db.get(key.as_bytes()).expect("get").is_some() {
                        found += 1;
                    }
                }
                found
            })
        })
        .collect();
    let mut found = 0;
    for h in hs {
        found += h.join().unwrap();
    }
    let get_dt = t1.elapsed();

    let st = db.stats();
    let total = per * threads as u64;
    println!(
        "SET  {:>10.0} ops/s   ({:.2} s)",
        total as f64 / set_dt.as_secs_f64(),
        set_dt.as_secs_f64()
    );
    println!(
        "GET  {:>10.0} ops/s   ({:.2} s, {found}/{total} encontradas)",
        total as f64 / get_dt.as_secs_f64(),
        get_dt.as_secs_f64()
    );
    println!(
        "RAM índice ≈ {:.1} MB para {} claves · disco {:.1} MB en {} segmento(s)",
        st.index_bytes_est as f64 / 1e6,
        st.keys,
        st.disk_bytes as f64 / 1e6,
        st.segments
    );

    let t2 = Instant::now();
    drop(db);
    let db = Db::open_default(&dir).expect("reabrir");
    println!(
        "Reinicio: {} claves reconstruidas en {:.0} ms",
        db.len(),
        t2.elapsed().as_secs_f64() * 1000.0
    );
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
    0
}

/// Escribe sin parar hasta que lo maten. Lo usa la prueba de apagón.
fn cmd_crashwrite(args: &[String]) -> i32 {
    let dir = match args.first() {
        Some(d) => d,
        None => return 2,
    };
    let fsync = args
        .get(1)
        .and_then(|s| FsyncPolicy::parse(s))
        .unwrap_or(FsyncPolicy::Group);
    let start: u64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
    let mut opts = Options::new(dir);
    opts.fsync = fsync;
    opts.max_segment_bytes = 256 << 10;
    let db = Db::open(opts).expect("abrir");
    let mut i = start;
    loop {
        let key = format!("k{i}");
        db.put(key.as_bytes(), &demo_value(i)).expect("put");
        i += 1;
        if i % 50_000 == 0 {
            thread::sleep(Duration::from_micros(1));
        }
    }
}
