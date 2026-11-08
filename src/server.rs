//! Servidor TCP: un hilo del sistema operativo por conexión, todos
//! compartiendo la misma `Db` a través de un `Arc`. No hay runtime async: la
//! concurrencia la gestiona el planificador del SO.
//!
//! # Lo que protege a un proceso expuesto a la red
//!
//! El motor puede ser impecable y aun así el servidor ser la pieza frágil. Lo
//! que hay aquí, y por qué:
//!
//! - **Tiempos de espera** (A2). Sin ellos, un cliente que abre el socket y no
//!   envía nada retiene un hilo para siempre; con `--max-conns` conexiones
//!   mudas el servidor queda inaccesible sin haber enviado un byte útil.
//! - **Tope en `KEYS`** (A3). Sin tope, un solo `KEYS` sobre 200.000 claves
//!   construye y ordena una lista de 200.000 elementos por cada cliente.
//! - **El fallo al crear un hilo no es fatal** (A4). Antes el `?` propagaba el
//!   error y **moría el servidor entero** en vez de rechazar esa conexión.
//! - **El límite de conexiones se comprueba en atómico** (A5). `load` y luego
//!   `fetch_add` no es atómico: varias conexiones simultáneas lo superaban.
//! - **Valores con salto de línea** (A7). El protocolo es por líneas, así que
//!   un valor con `\n` rompía el entramado y el cliente leía dos respuestas.
//! - **Apagado ordenado** (M3). SIGINT y SIGTERM dejan de aceptar, esperan a
//!   las conexiones en curso y permiten el `fsync` final.
//! - **Tope al tamaño de respuesta** (M4). Un valor de 1 GiB hacía que el
//!   servidor reservara 1 GiB por lectura concurrente.
//! - **Autenticación opcional** (M5). Y se niega a escuchar fuera de localhost
//!   sin clave, salvo que se le obligue.

use crate::protocol::{parse, Command};
use crate::store::Db;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub max_conns: usize,
    /// A2: tiempo máximo esperando datos del cliente. `None` = sin límite.
    pub read_timeout: Option<Duration>,
    /// A2: tiempo máximo escribiendo al cliente. `None` = sin límite.
    pub write_timeout: Option<Duration>,
    /// A3: claves que devuelve `KEYS` cuando el cliente no pide un límite.
    pub keys_limit: usize,
    /// M4: tamaño máximo de valor que se envía por el socket. Los valores más
    /// grandes siguen accesibles desde la API de Rust.
    pub max_value_bytes: usize,
    /// M5: si hay clave, cada conexión tiene que enviar `AUTH` antes de operar.
    pub clave: Option<String>,
    /// M5: permite escuchar fuera de localhost sin clave. Hay que pedirlo a
    /// propósito.
    pub permitir_sin_clave: bool,
    /// M3: parada ordenada de **este** servidor. El manejador de señales usa
    /// además un indicador global, pero este es por servidor para que varios
    /// en el mismo proceso (o varias pruebas a la vez) no se paren entre sí.
    pub parada: Arc<AtomicBool>,
}

impl ServerConfig {
    /// Valores razonables para desarrollo: localhost, sin clave.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        ServerConfig {
            host: host.into(),
            port,
            max_conns: 1024,
            read_timeout: Some(Duration::from_secs(300)),
            write_timeout: Some(Duration::from_secs(60)),
            keys_limit: 10_000,
            max_value_bytes: 16 << 20,
            clave: None,
            permitir_sin_clave: false,
            parada: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Mando para parar este servidor desde otro hilo.
    pub fn handle_parada(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.parada)
    }
}

/// M3: lo pone a `true` el manejador de señales.
static PARAR: AtomicBool = AtomicBool::new(false);

extern "C" fn al_recibir_senal(_sig: i32) {
    // Dentro de un manejador de señal solo se puede hacer lo que es
    // «async-signal-safe». Un store atómico lo es; un `println!` no.
    PARAR.store(true, Ordering::SeqCst);
}

/// Pide un apagado ordenado desde el propio programa.
pub fn pedir_parada() {
    PARAR.store(true, Ordering::SeqCst);
}

/// Instala los manejadores de SIGINT y SIGTERM (M3).
///
/// Sin esto, Ctrl-C mata el proceso en seco: no se ejecuta `Db::close`, así que
/// no hay `fsync` final y una compactación en marcha se corta por donde vaya.
pub fn instalar_senales() {
    const SIGINT: i32 = 2;
    const SIGTERM: i32 = 15;
    extern "C" {
        fn signal(sig: i32, handler: extern "C" fn(i32)) -> usize;
    }
    // SAFETY: `signal` con un manejador válido. El manejador solo hace un
    // store atómico, que es seguro en contexto de señal. No se desinstala
    // porque el proceso termina justo después.
    unsafe {
        signal(SIGINT, al_recibir_senal);
        signal(SIGTERM, al_recibir_senal);
    }
}

fn es_localhost(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "::1" | "localhost" | "[::1]")
}

/// Comparación en tiempo constante. Con `==` el tiempo de respuesta depende de
/// cuántos bytes coinciden, lo que permite adivinar la clave byte a byte.
/// La longitud sí se filtra, que es lo habitual y aceptable.
fn igual_en_tiempo_constante(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut dif = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        dif |= x ^ y;
    }
    dif == 0
}

pub fn serve(db: Arc<Db>, cfg: ServerConfig) -> io::Result<()> {
    // M5: escuchar en 0.0.0.0 sin autenticación es dar lectura, escritura y
    // COMPACT a quien llegue. No se hace por accidente.
    if !es_localhost(&cfg.host) && cfg.clave.is_none() && !cfg.permitir_sin_clave {
        return Err(io::Error::other(format!(
            "me niego a escuchar en {} sin autenticación: cualquiera tendría \
             lectura, escritura y COMPACT. Usa --clave <secreto>, o --sin-clave \
             si de verdad es lo que quieres",
            cfg.host
        )));
    }

    let listener = TcpListener::bind((cfg.host.as_str(), cfg.port))?;
    listener.set_nonblocking(true)?; // M3: para poder mirar si hay que parar
    eprintln!(
        "bitacora: escuchando en {}:{} · máx. {} conexiones · un hilo por conexión · \
         autenticación: {} · espera lectura: {} · KEYS máx. {}",
        cfg.host,
        cfg.port,
        cfg.max_conns,
        if cfg.clave.is_some() { "sí" } else { "NO" },
        match cfg.read_timeout {
            Some(d) => format!("{} s", d.as_secs()),
            None => "sin límite".into(),
        },
        cfg.keys_limit
    );

    let cfg = Arc::new(cfg);
    let conns = Arc::new(AtomicUsize::new(0));

    while !PARAR.load(Ordering::SeqCst) && !cfg.parada.load(Ordering::SeqCst) {
        let stream = match listener.accept() {
            Ok((s, _)) => s,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
                continue;
            }
            Err(e) => {
                eprintln!("bitacora: accept falló: {e}");
                continue;
            }
        };

        // A5: comprobar e incrementar en una sola operación atómica. Con
        // `load` y luego `fetch_add`, varias conexiones simultáneas pasaban la
        // comprobación a la vez y superaban el límite.
        let max = cfg.max_conns;
        let admitida = conns
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                if n >= max {
                    None
                } else {
                    Some(n + 1)
                }
            })
            .is_ok();
        if !admitida {
            let mut s = &stream;
            let _ = s.write_all(b"ERR interno demasiadas conexiones\n");
            continue;
        }

        // El socket aceptado tiene que ser bloqueante aunque el listener no lo
        // sea: POSIX no define si se hereda.
        let _ = stream.set_nonblocking(false);
        let _ = stream.set_nodelay(true);
        let _ = stream.set_read_timeout(cfg.read_timeout); // A2
        let _ = stream.set_write_timeout(cfg.write_timeout);

        // A4: para poder avisar al cliente si no hay hilos, hace falta una
        // copia del socket: el original se mueve al hilo.
        let aviso = stream.try_clone().ok();
        let db2 = Arc::clone(&db);
        let conns2 = Arc::clone(&conns);
        let cfg2 = Arc::clone(&cfg);
        let r = thread::Builder::new()
            .name("bitacora-conn".into())
            .spawn(move || {
                // A6: un panic aquí envenenaría los candados del motor y se
                // saltaría el decremento del contador, así que el límite se
                // iría agotando solo. Atrapándolo, el daño se queda en esta
                // conexión.
                //
                // `AssertUnwindSafe` está justificado: tras el panic no se
                // vuelve a tocar nada desde este cierre, y los candados del
                // motor se recuperan del envenenamiento (ver `store::lk`).
                let salida = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    handle(&db2, stream, &cfg2)
                }));
                match salida {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        if !matches!(
                            e.kind(),
                            io::ErrorKind::ConnectionReset
                                | io::ErrorKind::BrokenPipe
                                | io::ErrorKind::WouldBlock
                                | io::ErrorKind::TimedOut
                        ) {
                            eprintln!("bitacora: conexión cerrada con error: {e}");
                        }
                    }
                    Err(_) => eprintln!(
                        "bitacora: una conexión entró en panic; se cierra solo esa conexión. \
                         El estado en memoria puede haber quedado a medias: conviene reiniciar"
                    ),
                }
                conns2.fetch_sub(1, Ordering::SeqCst);
            });

        // A4: si no se puede crear el hilo (agotamiento, EAGAIN), se rechaza
        // ESTA conexión. Antes el `?` mataba el servidor entero.
        if let Err(e) = r {
            eprintln!("bitacora: no se pudo crear el hilo de la conexión: {e}");
            if let Some(s) = aviso {
                let mut s = &s;
                let _ = s.write_all(b"ERR interno no hay hilos disponibles\n");
            }
            conns.fetch_sub(1, Ordering::SeqCst);
        }
    }

    // M3: apagado ordenado. Ya no se aceptan conexiones; se espera un rato a
    // que terminen las que estaban en curso.
    let en_curso = conns.load(Ordering::SeqCst);
    eprintln!("bitacora: parando · {en_curso} conexión(es) en curso");
    let limite = Instant::now() + Duration::from_secs(5);
    while conns.load(Ordering::SeqCst) > 0 && Instant::now() < limite {
        thread::sleep(Duration::from_millis(20));
    }
    let quedan = conns.load(Ordering::SeqCst);
    if quedan > 0 {
        eprintln!("bitacora: {quedan} conexión(es) no terminaron a tiempo; se cierran");
    }
    Ok(())
}

pub fn handle(db: &Db, stream: TcpStream, cfg: &ServerConfig) -> io::Result<()> {
    let mut rd = BufReader::new(stream.try_clone()?);
    let mut wr = BufWriter::new(stream);
    let mut line = Vec::with_capacity(512);
    // M5: sin clave configurada, todo el mundo entra.
    let mut autenticado = cfg.clave.is_none();
    loop {
        if PARAR.load(Ordering::SeqCst) || cfg.parada.load(Ordering::SeqCst) {
            let _ = wr.write_all(b"ERR interno el servidor se esta parando\n");
            let _ = wr.flush();
            return Ok(());
        }
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
            Ok(cmd) => execute(db, cmd, &mut wr, cfg, &mut autenticado)?,
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
pub fn execute<W: Write>(
    db: &Db,
    cmd: Command<'_>,
    out: &mut W,
    cfg: &ServerConfig,
    autenticado: &mut bool,
) -> io::Result<bool> {
    // M5: antes de autenticarse solo se puede saludar, autenticarse y salir.
    if !*autenticado && !matches!(cmd, Command::Auth(_) | Command::Ping | Command::Quit) {
        out.write_all(b"ERR invalida hace falta AUTH antes de operar\n")?;
        return Ok(false);
    }
    match cmd {
        Command::Auth(clave) => match &cfg.clave {
            None => out.write_all(b"OK no hay autenticacion configurada\n")?,
            Some(esperada) => {
                if igual_en_tiempo_constante(clave, esperada.as_bytes()) {
                    *autenticado = true;
                    out.write_all(b"OK\n")?;
                } else {
                    *autenticado = false;
                    out.write_all(b"ERR invalida clave incorrecta\n")?;
                }
            }
        },
        Command::Set { key, value } => {
            // A7: el protocolo es por líneas. Un valor con `\n` haría que el
            // cliente leyera dos respuestas donde hay una, así que no se
            // acepta por aquí. La API de Rust sí admite binario con saltos.
            if value.contains(&b'\n') || value.contains(&b'\r') {
                out.write_all(
                    b"ERR invalida el valor no puede contener saltos de linea en este protocolo\n",
                )?;
            } else {
                match db.put(key, value) {
                    Ok(()) => out.write_all(b"OK\n")?,
                    Err(e) => writeln!(out, "ERR {} {e}", e.categoria())?,
                }
            }
        }
        Command::Get(k) => match db.get(k) {
            Ok(Some(v)) => {
                // M4: no reservar ni enviar respuestas enormes por el socket.
                if v.len() > cfg.max_value_bytes {
                    writeln!(
                        out,
                        "ERR invalida el valor ocupa {} bytes y el maximo por este protocolo es {}; usa la API de Rust",
                        v.len(),
                        cfg.max_value_bytes
                    )?;
                } else if v.contains(&b'\n') || v.contains(&b'\r') {
                    // A7: solo puede pasar si se escribió por la API de Rust.
                    out.write_all(
                        b"ERR interno el valor contiene saltos de linea y no se puede enviar por este protocolo\n",
                    )?;
                } else {
                    out.write_all(b"VAL ")?;
                    out.write_all(&v)?;
                    out.write_all(b"\n")?;
                }
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
        Command::Keys { prefix, limite } => {
            // A3: el límite del cliente nunca puede superar el del servidor.
            let lim = limite.unwrap_or(cfg.keys_limit).min(cfg.keys_limit);
            let (ks, hay_mas) = db.keys(prefix, lim);
            if hay_mas {
                writeln!(out, "KEYS {} truncado", ks.len())?;
            } else {
                writeln!(out, "KEYS {}", ks.len())?;
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn la_comparacion_en_tiempo_constante_distingue_bien() {
        assert!(igual_en_tiempo_constante(b"secreto", b"secreto"));
        assert!(!igual_en_tiempo_constante(b"secreto", b"secretO"));
        assert!(!igual_en_tiempo_constante(b"secreto", b"secret"));
        assert!(!igual_en_tiempo_constante(b"", b"x"));
        assert!(igual_en_tiempo_constante(b"", b""));
    }

    #[test]
    fn se_reconoce_localhost() {
        assert!(es_localhost("127.0.0.1"));
        assert!(es_localhost("::1"));
        assert!(es_localhost("localhost"));
        assert!(!es_localhost("0.0.0.0"));
        assert!(!es_localhost("192.168.1.10"));
    }
}
