//! Servidor TCP: un hilo del sistema operativo por conexión, todos
//! compartiendo la misma `Db` a través de un `Arc`. No hay runtime async:
//! la concurrencia la gestiona el planificador del SO.

use crate::protocol::{parse, Command};
use crate::store::Db;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_conns: usize,
}

pub fn serve(db: Arc<Db>, cfg: ServerConfig) -> io::Result<()> {
    let listener = TcpListener::bind((cfg.host.as_str(), cfg.port))?;
    eprintln!(
        "bitacora: escuchando en {}:{} (máx. {} conexiones, un hilo por conexión). Prueba: nc {} {}",
        cfg.host, cfg.port, cfg.max_conns, cfg.host, cfg.port
    );
    let conns = Arc::new(AtomicUsize::new(0));
    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(e) => {
                eprintln!("bitacora: accept falló: {e}");
                continue;
            }
        };
        if conns.load(Ordering::Relaxed) >= cfg.max_conns {
            let mut s = &stream;
            let _ = s.write_all(b"ERR demasiadas conexiones\n");
            continue;
        }
        let _ = stream.set_nodelay(true);
        conns.fetch_add(1, Ordering::Relaxed);
        let db = Arc::clone(&db);
        let conns = Arc::clone(&conns);
        thread::Builder::new()
            .name("bitacora-conn".into())
            .spawn(move || {
                // A6: un panic aquí envenenaría los candados del motor y, peor,
                // se saltaría el decremento del contador de conexiones, así que
                // el límite se iría agotando solo hasta dejar el servidor
                // inaccesible. Atrapándolo, el daño se queda en esta conexión.
                //
                // `AssertUnwindSafe` está justificado: tras el panic no se
                // vuelve a tocar nada desde este cierre, y los candados del
                // motor se recuperan del envenenamiento (ver `store::lk`).
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    handle(&db, stream)
                }));
                match r {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        if !matches!(
                            e.kind(),
                            io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
                        ) {
                            eprintln!("bitacora: conexión cerrada con error: {e}");
                        }
                    }
                    Err(_) => eprintln!(
                        "bitacora: una conexión entró en panic; se cierra solo esa conexión. \
                         El estado en memoria puede haber quedado a medias: conviene reiniciar"
                    ),
                }
                conns.fetch_sub(1, Ordering::Relaxed);
            })?;
    }
    Ok(())
}

pub fn handle(db: &Db, stream: TcpStream) -> io::Result<()> {
    let mut rd = BufReader::new(stream.try_clone()?);
    let mut wr = BufWriter::new(stream);
    let mut line = Vec::with_capacity(512);
    loop {
        line.clear();
        if rd.read_until(b'\n', &mut line)? == 0 {
            return Ok(());
        }
        while matches!(line.last(), Some(b'\n') | Some(b'\r')) {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        let quit = match parse(&line) {
            Ok(cmd) => execute(db, cmd, &mut wr)?,
            Err(e) => {
                writeln!(wr, "ERR invalida {e}")?;
                false
            }
        };
        wr.flush()?;
        if quit {
            return Ok(());
        }
    }
}

/// Ejecuta una orden y escribe la respuesta. Devuelve `true` si hay que cerrar.
pub fn execute<W: Write>(db: &Db, cmd: Command<'_>, out: &mut W) -> io::Result<bool> {
    match cmd {
        Command::Set { key, value } => match db.put(key, value) {
            Ok(()) => out.write_all(b"OK\n")?,
            Err(e) => writeln!(out, "ERR {} {e}", e.categoria())?,
        },
        Command::Get(k) => match db.get(k) {
            Ok(Some(v)) => {
                out.write_all(b"VAL ")?;
                out.write_all(&v)?;
                out.write_all(b"\n")?;
            }
            Ok(None) => out.write_all(b"NIL\n")?,
            Err(e) => writeln!(out, "ERR {} {e}", e.categoria())?,
        },
        Command::Del(k) => match db.delete(k) {
            Ok(true) => out.write_all(b"OK\n")?,
            Ok(false) => out.write_all(b"NIL\n")?,
            Err(e) => writeln!(out, "ERR {} {e}", e.categoria())?,
        },
        Command::Exists(k) => writeln!(out, "INT {}", db.exists(k) as u8)?,
        Command::Keys(prefix) => {
            let ks = db.keys(prefix);
            writeln!(out, "KEYS {}", ks.len())?;
            for k in ks {
                out.write_all(&k)?;
                out.write_all(b"\n")?;
            }
        }
        Command::Stats => {
            let lines = db.stats().lines();
            writeln!(out, "STATS {}", lines.len())?;
            for l in lines {
                writeln!(out, "{l}")?;
            }
        }
        Command::Compact => match db.compact() {
            Ok(r) if r.ran => writeln!(
                out,
                "OK segmentos_retirados={} bytes_antes={} bytes_despues={}",
                r.segments_removed, r.bytes_before, r.bytes_after
            )?,
            Ok(_) => out.write_all(b"OK ya_habia_una_compactacion_en_marcha\n")?,
            Err(e) => writeln!(out, "ERR {} {e}", e.categoria())?,
        },
        Command::Ping => out.write_all(b"PONG\n")?,
        Command::Quit => {
            out.write_all(b"BYE\n")?;
            return Ok(true);
        }
    }
    Ok(false)
}
