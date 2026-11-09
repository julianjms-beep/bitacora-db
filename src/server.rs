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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Contadores del servidor, separados de los del motor porque cuentan otra
/// cosa: lo que pasa en la frontera con la red.
///
/// Viven en la configuración porque es lo único que comparten `serve` y cada
/// conexión, y `serve` la deja tras un `Arc`.
#[derive(Default, Debug)]
pub struct MetricasServidor {
    /// Conexiones vivas ahora mismo. **Es también el contador que aplica el
    /// límite**: una sola fuente de verdad, para que la métrica no pueda
    /// mentir respecto a la admisión.
    pub abiertas: AtomicUsize,
    pub aceptadas: AtomicU64,
    pub rechazadas_limite: AtomicU64,
    pub rechazadas_sin_hilos: AtomicU64,
    pub auth_fallidas: AtomicU64,
    pub conexiones_en_panic: AtomicU64,
    pub ordenes: AtomicU64,
    pub ordenes_invalidas: AtomicU64,
}

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
    /// Contadores de la frontera de red. Salen por `METRICS`.
    pub metricas: Arc<MetricasServidor>,
    /// Dónde puede escribir copias la orden `BACKUP`. `None` (por defecto) la
    /// desactiva.
    ///
    /// **Por qué un directorio fijo y no una ruta del cliente.** Si el cliente
    /// eligiera la ruta, un `AUTH` válido se convertiría en «escribe ficheros
    /// donde pueda el usuario del servidor», que es mucho más de lo que da
    /// acceso a una base de datos. Con esto, el cliente solo elige un nombre y
    /// el servidor decide el sitio.
    pub backup_dir: Option<std::path::PathBuf>,
}

/// Comprueba que `nombre` es **un solo componente de ruta** inofensivo.
///
/// Rechaza barras, `..`, nombres ocultos y cualquier cosa que no sea
/// `[A-Za-z0-9._-]`. Sin esto, `BACKUP ../../etc` escribiría fuera del
/// directorio de copias.
fn nombre_de_copia_valido(n: &[u8]) -> Result<&str, &'static str> {
    if n.is_empty() || n.len() > 64 {
        return Err("el nombre de la copia tiene que tener entre 1 y 64 caracteres");
    }
    if n[0] == b'.' {
        return Err("el nombre de la copia no puede empezar por punto");
    }
    if !n
        .iter()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
    {
        return Err("el nombre de la copia solo admite letras, dígitos, punto, guion y guion bajo");
    }
    std::str::from_utf8(n).map_err(|_| "el nombre de la copia no es texto válido")
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
            metricas: Arc::new(MetricasServidor::default()),
            backup_dir: None,
        }
    }

    /// Mando para parar este servidor desde otro hilo.
    pub fn handle_parada(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.parada)
    }

    /// Comprueba que la configuración del servidor se puede usar. La llama
    /// `serve` antes de abrir el socket.
    ///
    /// Igual que en el motor: solo se rechaza lo que **no hace lo que dice**.
    /// Un `max_conns` de 1 es raro pero coherente; un `max_conns` de 0 abre un
    /// puerto que rechaza absolutamente todo, lo cual nunca es lo que se
    /// pretendía.
    pub fn validar(&self) -> Result<(), String> {
        if self.max_conns == 0 {
            return Err(
                "max_conns = 0: el servidor escucharía y rechazaría todas las conexiones".into(),
            );
        }
        if self.keys_limit == 0 {
            return Err("keys_limit = 0: KEYS no devolvería nunca ninguna clave".into());
        }
        if self.max_value_bytes == 0 {
            return Err("max_value_bytes = 0: GET rechazaría todos los valores no vacíos".into());
        }
        if let Some(d) = self.read_timeout {
            if d.is_zero() {
                return Err("read_timeout = 0 no es «sin límite»: usa None para eso".into());
            }
        }
        if let Some(d) = self.write_timeout {
            if d.is_zero() {
                return Err("write_timeout = 0 no es «sin límite»: usa None para eso".into());
            }
        }
        if let Some(c) = &self.clave {
            if c.is_empty() {
                return Err("la clave está vacía: cualquier AUTH con cadena vacía entraría".into());
            }
        }
        Ok(())
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

/// Las métricas de la frontera de red, en el mismo formato que las del motor.
pub fn metricas_servidor(m: &MetricasServidor) -> crate::metricas::Registro {
    let mut r = crate::metricas::Registro::new();
    r.gauge(
        "bitacora_connections",
        "Conexiones abiertas ahora mismo. Es el mismo contador que aplica el límite.",
        m.abiertas.load(Ordering::Relaxed) as u64,
    );
    r.contador(
        "bitacora_connections_accepted_total",
        "Conexiones admitidas desde que arrancó el servidor.",
        m.aceptadas.load(Ordering::Relaxed),
    );
    r.familia(
        "bitacora_connections_rejected_total",
        "Conexiones rechazadas, por motivo.",
        "reason",
        &[
            ("limit", m.rechazadas_limite.load(Ordering::Relaxed)),
            ("no_threads", m.rechazadas_sin_hilos.load(Ordering::Relaxed)),
        ],
    );
    r.contador(
        "bitacora_auth_failures_total",
        "Intentos de AUTH con clave incorrecta.",
        m.auth_fallidas.load(Ordering::Relaxed),
    );
    r.contador(
        "bitacora_conn_panics_total",
        "Conexiones que terminaron en panic. Si no es 0, hay un fallo que merece informe.",
        m.conexiones_en_panic.load(Ordering::Relaxed),
    );
    r.familia(
        "bitacora_commands_total",
        "Órdenes recibidas por el protocolo: todas, y las que no se pudieron ejecutar.",
        "result",
        &[
            (
                "ok",
                m.ordenes
                    .load(Ordering::Relaxed)
                    .saturating_sub(m.ordenes_invalidas.load(Ordering::Relaxed)),
            ),
            ("rejected", m.ordenes_invalidas.load(Ordering::Relaxed)),
        ],
    );
    r
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

    if let Err(e) = cfg.validar() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, e));
    }

    let listener = TcpListener::bind((cfg.host.as_str(), cfg.port))?;
    listener.set_nonblocking(true)?; // M3: para poder mirar si hay que parar
    crate::log::info(
        "server_listening",
        &[
            ("host", (&cfg.host).into()),
            ("port", cfg.port.into()),
            ("max_conns", cfg.max_conns.into()),
            ("auth", cfg.clave.is_some().into()),
            (
                "read_timeout_s",
                cfg.read_timeout.map_or(0u64, |d| d.as_secs()).into(),
            ),
            ("keys_limit", cfg.keys_limit.into()),
        ],
    );

    let cfg = Arc::new(cfg);
    // El contador de conexiones vivas ES la métrica: si fueran dos variables,
    // una podría decir una cosa y la admisión otra.
    let conns = Arc::clone(&cfg.metricas);

    while !PARAR.load(Ordering::SeqCst) && !cfg.parada.load(Ordering::SeqCst) {
        let stream = match listener.accept() {
            Ok((s, _)) => s,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
                continue;
            }
            Err(e) => {
                crate::log::aviso("accept_failed", &[("error", e.to_string().into())]);
                continue;
            }
        };

        // A5: comprobar e incrementar en una sola operación atómica. Con
        // `load` y luego `fetch_add`, varias conexiones simultáneas pasaban la
        // comprobación a la vez y superaban el límite.
        let max = cfg.max_conns;
        let admitida = conns
            .abiertas
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                if n >= max {
                    None
                } else {
                    Some(n + 1)
                }
            })
            .is_ok();
        if !admitida {
            conns.rechazadas_limite.fetch_add(1, Ordering::Relaxed);
            crate::log::aviso(
                "conn_rejected",
                &[("reason", "limit".into()), ("max_conns", max.into())],
            );
            let mut s = &stream;
            let _ = s.write_all(b"ERR interno demasiadas conexiones\n");
            continue;
        }
        conns.aceptadas.fetch_add(1, Ordering::Relaxed);

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
        let conns2 = Arc::clone(&cfg.metricas);
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
                            crate::log::aviso(
                                "conn_error",
                                &[
                                    ("kind", format!("{:?}", e.kind()).into()),
                                    ("error", e.to_string().into()),
                                ],
                            );
                        }
                    }
                    Err(_) => {
                        conns2.conexiones_en_panic.fetch_add(1, Ordering::Relaxed);
                        crate::log::error(
                            "conn_panicked",
                            &[(
                                "note",
                                "se cierra solo esa conexión; el estado en memoria puede haber \
                                 quedado a medias y conviene reiniciar"
                                    .into(),
                            )],
                        );
                    }
                }
                conns2.abiertas.fetch_sub(1, Ordering::SeqCst);
            });

        // A4: si no se puede crear el hilo (agotamiento, EAGAIN), se rechaza
        // ESTA conexión. Antes el `?` mataba el servidor entero.
        if let Err(e) = r {
            conns.rechazadas_sin_hilos.fetch_add(1, Ordering::Relaxed);
            crate::log::error(
                "conn_rejected",
                &[
                    ("reason", "no_threads".into()),
                    ("error", e.to_string().into()),
                ],
            );
            if let Some(s) = aviso {
                let mut s = &s;
                let _ = s.write_all(b"ERR interno no hay hilos disponibles\n");
            }
            conns.abiertas.fetch_sub(1, Ordering::SeqCst);
        }
    }

    // M3: apagado ordenado. Ya no se aceptan conexiones; se espera un rato a
    // que terminen las que estaban en curso.
    let en_curso = conns.abiertas.load(Ordering::SeqCst);
    crate::log::info("server_stopping", &[("open_conns", en_curso.into())]);
    let limite = Instant::now() + Duration::from_secs(5);
    while conns.abiertas.load(Ordering::SeqCst) > 0 && Instant::now() < limite {
        thread::sleep(Duration::from_millis(20));
    }
    let quedan = conns.abiertas.load(Ordering::SeqCst);
    if quedan > 0 {
        crate::log::aviso(
            "server_stopped_with_open_conns",
            &[("open_conns", quedan.into())],
        );
    } else {
        crate::log::info("server_stopped", &[]);
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
                cfg.metricas.ordenes.fetch_add(1, Ordering::Relaxed);
                cfg.metricas
                    .ordenes_invalidas
                    .fetch_add(1, Ordering::Relaxed);
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
    cfg.metricas.ordenes.fetch_add(1, Ordering::Relaxed);
    // M5: antes de autenticarse solo se puede saludar, autenticarse y salir.
    //
    // `HEALTH` y `METRICS` **no** están entre las excepciones a propósito: el
    // estado interno y los contadores de un servidor no son información
    // pública. Para un `healthcheck` con clave, `bitacora health --clave`.
    if !*autenticado && !matches!(cmd, Command::Auth(_) | Command::Ping | Command::Quit) {
        cfg.metricas
            .ordenes_invalidas
            .fetch_add(1, Ordering::Relaxed);
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
                    cfg.metricas.auth_fallidas.fetch_add(1, Ordering::Relaxed);
                    // Sin dirección de origen: con un hilo por conexión, quien
                    // quiera correlacionar tiene el registro del sistema. Y la
                    // clave intentada NO se registra nunca.
                    crate::log::aviso("auth_failed", &[]);
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
        Command::Health => {
            let s = db.health();
            writeln!(out, "HEALTH {} {}", s.estado.name(), s.motivos.len())?;
            for m in &s.motivos {
                // Un motivo con salto de línea rompería el entramado igual que
                // un valor (A7). Los motivos los compone el motor, pero el
                // texto de un error del sistema puede traer cualquier cosa.
                let m = m.replace(['\n', '\r'], " ");
                writeln!(out, "{m}")?;
            }
        }
        Command::Metrics => {
            let mut r = db.metricas();
            r.anexa(&metricas_servidor(&cfg.metricas));
            let texto = r.texto();
            let n = texto.lines().count();
            writeln!(out, "METRICS {n}")?;
            out.write_all(texto.as_bytes())?;
        }
        Command::Backup(nombre) => match &cfg.backup_dir {
            None => out.write_all(
                b"ERR invalida este servidor no tiene directorio de copias: arrancalo con --backup-dir\n",
            )?,
            Some(base) => {
                let predeterminado = format!(
                    "copia-{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.as_secs())
                );
                let elegido = match nombre {
                    None => Ok(predeterminado.as_str()),
                    Some(n) => nombre_de_copia_valido(n),
                };
                match elegido {
                    Err(e) => writeln!(out, "ERR invalida {e}")?,
                    Ok(n) => {
                        let destino = base.join(n);
                        match db.backup(&destino) {
                            Ok(r) => writeln!(
                                out,
                                "OK {} segmentos={} bytes={} last_seq={} ms={:.1}",
                                destino.display(),
                                r.segments,
                                r.bytes,
                                r.last_seq,
                                r.ms
                            )?,
                            Err(e) => writeln!(out, "ERR {} {e}", e.categoria())?,
                        }
                    }
                }
            }
        },
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
