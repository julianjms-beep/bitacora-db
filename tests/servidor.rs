//! Gate 3 — endurecimiento del servidor TCP.
//!
//! El motor puede ser impecable y el servidor ser la pieza frágil. Cada prueba
//! de aquí corresponde a una forma concreta de tirarlo o de corromper una
//! respuesta, todas presentes antes de este gate.

mod common;

use bitacora::server::{serve, ServerConfig};
use bitacora::{Db, FsyncPolicy, Options};
use common::TempDir;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Puerto distinto por prueba, para que puedan correr en paralelo.
fn puerto(n: u16) -> u16 {
    47800 + n
}

fn opts(dir: &Path) -> Options {
    let mut o = Options::new(dir);
    o.fsync = FsyncPolicy::Never;
    o.auto_compact = false;
    o
}

struct Servidor {
    parada: Arc<AtomicBool>,
    hilo: Option<thread::JoinHandle<()>>,
    puerto: u16,
}

impl Servidor {
    /// Arranca un servidor y espera a que acepte conexiones.
    fn nuevo(db: Arc<Db>, cfg: ServerConfig) -> Servidor {
        let parada = cfg.handle_parada();
        let p = cfg.port;
        let hilo = thread::spawn(move || {
            let _ = serve(db, cfg);
        });
        let limite = Instant::now() + Duration::from_secs(5);
        while Instant::now() < limite {
            if TcpStream::connect(("127.0.0.1", p)).is_ok() {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        Servidor {
            parada,
            hilo: Some(hilo),
            puerto: p,
        }
    }

    fn conecta(&self) -> TcpStream {
        let s = TcpStream::connect(("127.0.0.1", self.puerto)).expect("conectar");
        s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
        s
    }

    /// Envía órdenes y devuelve las `n` primeras líneas de respuesta.
    fn pide(&self, ordenes: &str, n: usize) -> Vec<String> {
        let mut s = self.conecta();
        s.write_all(ordenes.as_bytes()).unwrap();
        s.flush().unwrap();
        let mut r = BufReader::new(s);
        let mut out = Vec::new();
        for _ in 0..n {
            let mut l = String::new();
            if r.read_line(&mut l).unwrap_or(0) == 0 {
                break;
            }
            out.push(l.trim_end().to_string());
        }
        out
    }
}

impl Drop for Servidor {
    fn drop(&mut self) {
        self.parada.store(true, Ordering::SeqCst);
        if let Some(h) = self.hilo.take() {
            let _ = h.join();
        }
    }
}

// ---------------------------------------------------------------------------
// M5 · autenticación
// ---------------------------------------------------------------------------

/// Sin AUTH no se puede operar; con la clave equivocada tampoco; con la buena sí.
#[test]
fn sin_auth_no_se_puede_operar() {
    let tmp = TempDir::new("srv-auth");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    let mut cfg = ServerConfig::new("127.0.0.1", puerto(1));
    cfg.clave = Some("secreto-largo".into());
    let srv = Servidor::nuevo(db, cfg);

    let r = srv.pide("SET k v\nQUIT\n", 1);
    assert!(r[0].starts_with("ERR invalida"), "debía exigir AUTH: {r:?}");
    assert!(
        r[0].contains("AUTH"),
        "el error debe decir qué falta: {r:?}"
    );

    // PING y QUIT sí se permiten antes de autenticarse.
    assert_eq!(srv.pide("PING\nQUIT\n", 1)[0], "PONG");

    let r = srv.pide("AUTH equivocada\nQUIT\n", 1);
    assert!(r[0].starts_with("ERR invalida"), "clave mala: {r:?}");

    let r = srv.pide("AUTH secreto-largo\nSET k v\nGET k\nQUIT\n", 3);
    assert_eq!(
        r,
        vec!["OK", "OK", "VAL v"],
        "con la clave buena debe funcionar"
    );
}

/// Escuchar fuera de localhost sin clave daría lectura, escritura y COMPACT a
/// cualquiera: el servidor se niega, salvo que se le obligue.
#[test]
fn se_niega_a_escuchar_fuera_de_localhost_sin_clave() {
    let tmp = TempDir::new("srv-expuesto");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    let cfg = ServerConfig::new("0.0.0.0", puerto(2));
    let e = match serve(Arc::clone(&db), cfg) {
        Ok(()) => panic!("no debería haber aceptado escuchar en 0.0.0.0 sin clave"),
        Err(e) => e.to_string(),
    };
    assert!(e.contains("me niego"), "mensaje inesperado: {e}");
    assert!(e.contains("--clave"), "debe decir cómo arreglarlo: {e}");
}

// ---------------------------------------------------------------------------
// A3 · KEYS acotado
// ---------------------------------------------------------------------------

/// `KEYS` devolvía todas las claves que hubiera. Ahora hay tope, se avisa de
/// que está truncado, y lo que sale son las primeras **en orden**.
#[test]
fn keys_respeta_el_tope_y_avisa() {
    let tmp = TempDir::new("srv-keys");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    for i in 0..100u64 {
        db.put(format!("k{i:03}").as_bytes(), b"v").unwrap();
    }
    let mut cfg = ServerConfig::new("127.0.0.1", puerto(3));
    cfg.keys_limit = 10;
    let srv = Servidor::nuevo(db, cfg);

    let r = srv.pide("KEYS\nQUIT\n", 11);
    assert_eq!(
        r[0], "KEYS 10 truncado",
        "debe avisar de que hay más: {r:?}"
    );
    assert_eq!(r[1], "k000", "y lo devuelto debe ser el principio EN ORDEN");
    assert_eq!(r[10], "k009");

    // El cliente puede pedir menos.
    let r = srv.pide("KEYS k00 3\nQUIT\n", 4);
    assert_eq!(r[0], "KEYS 3 truncado");
    assert_eq!(&r[1..4], &["k000", "k001", "k002"]);

    // Pero no más que el tope del servidor.
    let r = srv.pide("KEYS k 10000\nQUIT\n", 1);
    assert!(r[0].starts_with("KEYS "), "{r:?}");
    let n: usize = r[0].split_whitespace().nth(1).unwrap().parse().unwrap();
    assert!(
        n <= 10,
        "el cliente no puede superar el tope del servidor: {n}"
    );
}

// ---------------------------------------------------------------------------
// A7 · saltos de línea
// ---------------------------------------------------------------------------

/// El protocolo es por líneas. Un valor con `\n` hacía que el cliente leyera
/// dos respuestas donde había una; ahora no se acepta por el socket, y si está
/// guardado desde la API de Rust, `GET` lo dice en vez de corromper el marco.
#[test]
fn los_saltos_de_linea_no_rompen_el_entramado() {
    let tmp = TempDir::new("srv-nl");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    // Solo se pueden crear por la API binaria.
    db.put(b"conSalto", b"primera\nsegunda").unwrap();
    db.put(b"conRetorno", b"antes\rdespues").unwrap();
    let srv = Servidor::nuevo(Arc::clone(&db), ServerConfig::new("127.0.0.1", puerto(4)));

    let r = srv.pide("GET conSalto\nPING\nQUIT\n", 2);
    assert!(
        r[0].starts_with("ERR"),
        "no debe enviar un valor con salto: {r:?}"
    );
    assert_eq!(
        r[1], "PONG",
        "y el entramado debe seguir sano tras el error"
    );

    let r = srv.pide("GET conRetorno\nPING\nQUIT\n", 2);
    assert!(r[0].starts_with("ERR"), "{r:?}");
    assert_eq!(r[1], "PONG");

    // Y por el socket no se puede guardar uno con \r.
    let r = srv.pide("SET malo an\rtes\nQUIT\n", 1);
    assert!(r[0].starts_with("ERR invalida"), "{r:?}");
}

// ---------------------------------------------------------------------------
// M4 · tope de tamaño de respuesta
// ---------------------------------------------------------------------------

/// Un valor grande hacía que el servidor reservara y enviara ese tamaño por
/// cada lectura concurrente.
#[test]
fn no_envia_valores_mayores_que_el_tope() {
    let tmp = TempDir::new("srv-grande");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    db.put(b"grande", &vec![b'x'; 5000]).unwrap();
    db.put(b"pequeno", b"vale").unwrap();
    let mut cfg = ServerConfig::new("127.0.0.1", puerto(5));
    cfg.max_value_bytes = 1000;
    let srv = Servidor::nuevo(db, cfg);

    let r = srv.pide("GET grande\nGET pequeno\nQUIT\n", 2);
    assert!(r[0].starts_with("ERR invalida"), "debía rechazarlo: {r:?}");
    assert!(
        r[0].contains("5000"),
        "el error debe decir el tamaño: {r:?}"
    );
    assert_eq!(r[1], "VAL vale", "y lo pequeño sigue funcionando");
}

// ---------------------------------------------------------------------------
// A5 · límite de conexiones
// ---------------------------------------------------------------------------

/// `load` y luego `fetch_add` no es atómico, así que el límite se podía
/// superar. Con 4 conexiones y un tope de 2, al menos 2 tienen que ser
/// rechazadas, y al soltarlas las plazas tienen que volver a quedar libres.
///
/// No se cuentan plazas exactas a propósito: la sonda de arranque también usa
/// una, así que lo determinista es la cota, no el reparto.
#[test]
fn se_respeta_el_limite_de_conexiones() {
    let tmp = TempDir::new("srv-limite");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    let mut cfg = ServerConfig::new("127.0.0.1", puerto(6));
    cfg.max_conns = 2;
    let srv = Servidor::nuevo(db, cfg);

    // Cada conexión se queda abierta, reteniendo su plaza si fue admitida.
    let mut abiertas = Vec::new();
    let mut rechazadas = 0;
    let mut admitidas = 0;
    for _ in 0..4 {
        let mut s = srv.conecta();
        s.write_all(b"PING\n").unwrap();
        let mut l = String::new();
        BufReader::new(s.try_clone().unwrap())
            .read_line(&mut l)
            .unwrap();
        if l.contains("demasiadas conexiones") {
            rechazadas += 1;
        } else {
            assert_eq!(l.trim_end(), "PONG", "respuesta inesperada: {l:?}");
            admitidas += 1;
            abiertas.push(s);
        }
    }
    assert!(
        admitidas <= 2,
        "se admitieron {admitidas} conexiones con un tope de 2: el limite no se respeta"
    );
    assert!(
        rechazadas >= 2,
        "con 4 conexiones y tope 2 debían rechazarse al menos 2"
    );

    // Al soltarlas, las plazas vuelven a quedar libres.
    abiertas.clear();
    let limite = Instant::now() + Duration::from_secs(5);
    loop {
        let mut s = srv.conecta();
        s.write_all(b"PING\n").unwrap();
        let mut l = String::new();
        BufReader::new(s.try_clone().unwrap())
            .read_line(&mut l)
            .unwrap();
        if l.trim_end() == "PONG" {
            break;
        }
        assert!(Instant::now() < limite, "las plazas no se liberaron: {l:?}");
        thread::sleep(Duration::from_millis(50));
    }
}

// ---------------------------------------------------------------------------
// A2 · tiempos de espera
// ---------------------------------------------------------------------------

/// Un cliente que abre el socket y no envía nada retenía un hilo para siempre.
/// Con `--max-conns` conexiones mudas, el servidor quedaba inaccesible sin
/// haber enviado un byte útil.
#[test]
fn una_conexion_muda_se_cierra_por_tiempo() {
    let tmp = TempDir::new("srv-timeout");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    let mut cfg = ServerConfig::new("127.0.0.1", puerto(7));
    cfg.read_timeout = Some(Duration::from_millis(300));
    let srv = Servidor::nuevo(db, cfg);

    let s = srv.conecta();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let t0 = Instant::now();
    // No enviamos nada: el servidor debe cortar.
    let mut buf = [0u8; 16];
    let n = (&s).read(&mut buf).unwrap_or(0);
    let tardo = t0.elapsed();
    assert_eq!(
        n, 0,
        "el servidor debía cerrar la conexión muda, no responder"
    );
    assert!(
        tardo < Duration::from_secs(4),
        "tardó {tardo:?}: el tiempo de espera no se está aplicando"
    );
}

// ---------------------------------------------------------------------------
// M3 · apagado ordenado
// ---------------------------------------------------------------------------

/// `serve` tiene que volver cuando se pide la parada, para que `main` pueda
/// hacer el `fsync` final. Antes el bucle de `accept` no tenía salida.
#[test]
fn el_servidor_para_cuando_se_le_pide() {
    let tmp = TempDir::new("srv-parada");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    let cfg = ServerConfig::new("127.0.0.1", puerto(8));
    let parada = cfg.handle_parada();
    let db2 = Arc::clone(&db);
    let hilo = thread::spawn(move || serve(db2, cfg));

    // Esperar a que esté escuchando.
    let limite = Instant::now() + Duration::from_secs(5);
    while Instant::now() < limite && TcpStream::connect(("127.0.0.1", puerto(8))).is_err() {
        thread::sleep(Duration::from_millis(20));
    }

    parada.store(true, Ordering::SeqCst);
    let t0 = Instant::now();
    let r = hilo.join().expect("el hilo del servidor entró en panic");
    assert!(r.is_ok(), "serve debía volver sin error: {r:?}");
    assert!(
        t0.elapsed() < Duration::from_secs(8),
        "tardó {:?} en parar",
        t0.elapsed()
    );
    // Y el motor se puede cerrar limpiamente después.
    db.close().unwrap();
}
