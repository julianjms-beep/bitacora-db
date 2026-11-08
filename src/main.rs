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
    opt(args, name)
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn has(args: &[String], name: &str) -> bool {
    args.iter().any(|a| a == name)
}

fn fsync_opt(args: &[String], default: FsyncPolicy) -> Result<FsyncPolicy, String> {
    match opt(args, "--fsync") {
        None => Ok(default),
        Some(s) => {
            FsyncPolicy::parse(&s).ok_or(format!("--fsync inválido: {s} (group|always|never)"))
        }
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
    let segundos = |n: u64| {
        if n == 0 {
            None
        } else {
            Some(Duration::from_secs(n))
        }
    };
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
    let aviso = if rep.con_perdidas() {
        " (RECUPERACIÓN CON PÉRDIDAS)"
    } else {
        ""
    };
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
    eprintln!(
        "conectado a {host}:{port}. Órdenes: SET GET DEL EXISTS KEYS STATS COMPACT PING QUIT"
    );
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

/// Percentiles exactos: se guarda la latencia de cada operación y se ordena.
/// Son 8 bytes por operación —1,6 MB por millón—, asumible, y evita el error de
/// aproximación de un histograma por cubos. Publicar un p99 aproximado sin
/// decirlo sería justo el tipo de cifra engañosa que este banco quiere evitar.
fn percentiles(v: &mut [u64]) -> [u64; 5] {
    v.sort_unstable();
    if v.is_empty() {
        return [0; 5];
    }
    let q = |f: f64| v[(((v.len() - 1) as f64) * f).round() as usize];
    [q(0.50), q(0.95), q(0.99), q(0.999), v[v.len() - 1]]
}

struct Fase {
    nombre: &'static str,
    ops: u64,
    dur: Duration,
    lat: Vec<u64>,
}

impl Fase {
    fn nueva(nombre: &'static str) -> Fase {
        Fase {
            nombre,
            ops: 0,
            dur: Duration::ZERO,
            lat: Vec::new(),
        }
    }

    fn imprime(&mut self) {
        let p = percentiles(&mut self.lat);
        let us = |n: u64| n as f64 / 1000.0;
        println!(
            "{:<22} {:>9.0} ops/s  {:>9.1} {:>9.1} {:>10.1} {:>10.1} {:>10.1}",
            self.nombre,
            self.ops as f64 / self.dur.as_secs_f64().max(1e-9),
            us(p[0]),
            us(p[1]),
            us(p[2]),
            us(p[3]),
            us(p[4]),
        );
    }
}

fn cmd_bench(args: &[String]) -> i32 {
    let threads: usize = opt_or(args, "--threads", 8);
    let ops: u64 = opt_or(args, "--ops", 200_000);
    let vbytes: usize = opt_or(args, "--value-bytes", 100);
    let segmb: u64 = opt_or(args, "--segment-mb", 4);
    let fsync = match fsync_opt(args, FsyncPolicy::Never) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{e}");
            return 2;
        }
    };
    let base = PathBuf::from(
        opt(args, "--dir").unwrap_or_else(|| std::env::temp_dir().to_string_lossy().into_owned()),
    );
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = base.join(format!("bitacora-bench-{}-{nanos}", std::process::id()));

    let mut opts = Options::new(&dir);
    opts.fsync = fsync;
    opts.full_fsync = has(args, "--fullfsync");
    opts.max_segment_bytes = segmb << 20;
    let db = Arc::new(Db::open(opts.clone()).expect("abrir bench"));
    let per = ops / threads as u64;
    let total = per * threads as u64;
    let value = vec![b'x'; vbytes];
    let fs_extra = if opts.full_fsync { " --fullfsync" } else { "" };

    let nucleos = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    println!(
        "bitacora bench · {} {} · {nucleos} nucleos · {threads} hilos · {total} ops · valor {vbytes} B · fsync={}{fs_extra} · segmentos de {segmb} MB",
        std::env::consts::OS,
        std::env::consts::ARCH,
        fsync.name(),
    );
    println!(
        "reproducir: bitacora bench --threads {threads} --ops {total} --value-bytes {vbytes} --fsync {}{fs_extra} --segment-mb {segmb}",
        fsync.name(),
    );
    println!();

    // --- SET ---
    let t0 = Instant::now();
    let hs: Vec<_> = (0..threads as u64)
        .map(|t| {
            let db = Arc::clone(&db);
            let value = value.clone();
            thread::spawn(move || {
                let mut lat = Vec::with_capacity(per as usize);
                for j in 0..per {
                    let key = format!("k{:010}", t * per + j);
                    let t1 = Instant::now();
                    db.put(key.as_bytes(), &value).expect("put");
                    lat.push(t1.elapsed().as_nanos() as u64);
                }
                lat
            })
        })
        .collect();
    let mut set = Fase::nueva("SET");
    for h in hs {
        set.lat.extend(h.join().unwrap());
    }
    set.dur = t0.elapsed();
    set.ops = total;

    // --- GET cruzado: cada hilo lee las claves de otro, para repartir shards ---
    let t0 = Instant::now();
    let hs: Vec<_> = (0..threads as u64)
        .map(|t| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let mut lat = Vec::with_capacity(per as usize);
                let mut halladas = 0u64;
                for j in 0..per {
                    let key = format!("k{:010}", ((t + 1) % threads as u64) * per + j);
                    let t1 = Instant::now();
                    let hay = db.get(key.as_bytes()).expect("get").is_some();
                    lat.push(t1.elapsed().as_nanos() as u64);
                    halladas += hay as u64;
                }
                (lat, halladas)
            })
        })
        .collect();
    let mut get = Fase::nueva("GET");
    let mut halladas = 0u64;
    for h in hs {
        let (l, e) = h.join().unwrap();
        get.lat.extend(l);
        halladas += e;
    }
    get.dur = t0.elapsed();
    get.ops = total;

    // --- GET mientras compacta ---
    // Antes se generan bytes muertos sobrescribiendo, para que la compactacion
    // tenga trabajo de verdad: medir "durante compactacion" sobre un log sin
    // basura daria una cifra bonita y falsa.
    for j in 0..(total / 4) {
        let key = format!("k{:010}", j);
        db.put(key.as_bytes(), &value).expect("put");
    }
    let st_antes = db.stats();
    let compactando = {
        let db = Arc::clone(&db);
        thread::spawn(move || db.compact())
    };
    let t0 = Instant::now();
    let hs: Vec<_> = (0..threads as u64)
        .map(|t| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let mut lat = Vec::with_capacity((per / 4) as usize);
                for j in 0..(per / 4) {
                    let key = format!("k{:010}", ((t + 1) % threads as u64) * per + j);
                    let t1 = Instant::now();
                    db.get(key.as_bytes()).expect("get");
                    lat.push(t1.elapsed().as_nanos() as u64);
                }
                lat
            })
        })
        .collect();
    let mut getc = Fase::nueva("GET con compactacion");
    for h in hs {
        getc.lat.extend(h.join().unwrap());
    }
    getc.dur = t0.elapsed();
    getc.ops = getc.lat.len() as u64;
    let informe = compactando.join().unwrap().expect("compactar");

    // --- Reapertura: reconstruccion del indice ---
    // Se mide AQUI y no al final del banco: despues del DELETE no quedaria
    // ninguna clave y el numero no diria nada. (La primera version de este
    // banco informaba de "66 ms para 0 claves", que es exactamente el tipo de
    // cifra sin sentido que hay que cazar antes de publicarla.)
    drop(db);
    let t0 = Instant::now();
    let db = Arc::new(Db::open(opts.clone()).expect("reabrir"));
    let dt_reapertura = t0.elapsed();
    let st_reabierta = db.stats();

    // --- DELETE ---
    let t0 = Instant::now();
    let hs: Vec<_> = (0..threads as u64)
        .map(|t| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let mut lat = Vec::with_capacity(per as usize);
                for j in 0..per {
                    let key = format!("k{:010}", t * per + j);
                    let t1 = Instant::now();
                    db.delete(key.as_bytes()).expect("delete");
                    lat.push(t1.elapsed().as_nanos() as u64);
                }
                lat
            })
        })
        .collect();
    let mut del = Fase::nueva("DELETE");
    for h in hs {
        del.lat.extend(h.join().unwrap());
    }
    del.dur = t0.elapsed();
    del.ops = total;

    println!(
        "{:<22} {:>15}  {:>9} {:>9} {:>10} {:>10} {:>10}",
        "fase", "rendimiento", "p50 us", "p95 us", "p99 us", "p99,9 us", "max us"
    );
    set.imprime();
    get.imprime();
    getc.imprime();
    del.imprime();

    println!();
    println!(
        "indice       {:>8.1} MB para {} claves ({:.0} B/clave)",
        st_antes.index_bytes_est as f64 / (1 << 20) as f64,
        st_antes.keys,
        st_antes.index_bytes_est as f64 / st_antes.keys.max(1) as f64
    );
    println!(
        "disco        {:>8.1} MB en {} segmentos · {:.1} MB vivos / {:.1} MB muertos antes de compactar",
        st_antes.disk_bytes as f64 / (1 << 20) as f64,
        st_antes.segments,
        st_antes.live_bytes as f64 / (1 << 20) as f64,
        st_antes.dead_bytes as f64 / (1 << 20) as f64
    );
    println!(
        "compactacion {:>8} segmentos retirados · {:.1} MB -> {:.1} MB",
        informe.segments_removed,
        informe.bytes_before as f64 / (1 << 20) as f64,
        informe.bytes_after as f64 / (1 << 20) as f64
    );
    println!("GET          {halladas} de {total} claves encontradas");

    println!(
        "reapertura   {:>8.0} ms para {} claves ({:.0} claves/s) · salud: {}",
        dt_reapertura.as_secs_f64() * 1000.0,
        st_reabierta.keys,
        st_reabierta.keys as f64 / dt_reapertura.as_secs_f64().max(1e-9),
        st_reabierta.health
    );
    drop(db);

    if has(args, "--conservar") {
        println!("\ndatos en {}", dir.display());
    } else if let Err(e) = std::fs::remove_dir_all(&dir) {
        eprintln!("aviso: no se pudo borrar {}: {e}", dir.display());
    }
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
        if i.is_multiple_of(50_000) {
            thread::sleep(Duration::from_micros(1));
        }
    }
}
