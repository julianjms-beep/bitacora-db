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

// ---------------------------------------------------------------------------
// Gate 6 · observabilidad y copias por el protocolo
// ---------------------------------------------------------------------------

/// `HEALTH` y `METRICS` anuncian cuántas líneas traen, igual que `STATS`, para
/// que un cliente sepa cuándo parar de leer sin cerrar la conexión.
#[test]
fn health_y_metrics_anuncian_sus_lineas() {
    let tmp = TempDir::new("srv-salud");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    db.put(b"a", b"1").unwrap();
    let srv = Servidor::nuevo(Arc::clone(&db), ServerConfig::new("127.0.0.1", puerto(9)));

    let r = srv.pide("HEALTH\n", 1);
    assert_eq!(r[0], "HEALTH ok 0", "una base sana no tiene motivos");

    // La cabecera dice n; leer n+1 líneas tiene que traer exactamente n+1
    // porque la siguiente respuesta ya es de otra orden.
    let r = srv.pide("METRICS\nPING\n", 1);
    let n: usize = r[0]
        .split_whitespace()
        .nth(1)
        .expect("METRICS <n>")
        .parse()
        .expect("n numérico");
    assert!(n > 20, "solo {n} líneas de métricas");
    let r = srv.pide("METRICS\nPING\n", n + 2);
    assert_eq!(r.len(), n + 2);
    assert!(r[1].starts_with("# HELP "), "{}", r[1]);
    assert_eq!(r[n + 1], "PONG", "el entramado se ha desajustado");
    // Y las métricas del servidor están además de las del motor.
    let todo = r.join("\n");
    assert!(todo.contains("bitacora_keys 1"), "{todo}");
    assert!(todo.contains("bitacora_connections "), "{todo}");
    assert!(
        todo.contains("bitacora_connections_accepted_total "),
        "{todo}"
    );
    db.close().unwrap();
}

/// El estado interno y los contadores de un servidor con clave no son
/// información pública: `HEALTH` y `METRICS` también exigen `AUTH`.
#[test]
fn health_y_metrics_exigen_auth() {
    let tmp = TempDir::new("srv-salud-auth");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    let mut cfg = ServerConfig::new("127.0.0.1", puerto(10));
    cfg.clave = Some("secreta".into());
    let srv = Servidor::nuevo(Arc::clone(&db), cfg);

    for orden in ["HEALTH\n", "METRICS\n", "BACKUP\n"] {
        let r = srv.pide(orden, 1);
        assert!(
            r[0].starts_with("ERR invalida hace falta AUTH"),
            "{orden} sin AUTH devolvió: {}",
            r[0]
        );
    }
    let r = srv.pide("AUTH secreta\nHEALTH\n", 2);
    assert_eq!(r[1], "HEALTH ok 0");
    db.close().unwrap();
}

/// La copia por el protocolo escribe **dentro** del directorio configurado y
/// solo ahí: el cliente elige un nombre, no una ruta.
#[test]
fn backup_por_el_protocolo_no_deja_elegir_la_ruta() {
    let tmp = TempDir::new("srv-copia");
    let copias = TempDir::new("srv-copia-dst");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    for i in 0..30u32 {
        db.put(format!("k{i}").as_bytes(), b"v").unwrap();
    }
    let mut cfg = ServerConfig::new("127.0.0.1", puerto(11));
    cfg.backup_dir = Some(copias.path().to_path_buf());
    let srv = Servidor::nuevo(Arc::clone(&db), cfg);

    // Nada que se parezca a una ruta se acepta.
    for malo in [
        "BACKUP ../fuera\n",
        "BACKUP /tmp/absoluta\n",
        "BACKUP .oculta\n",
        "BACKUP ..\n",
        "BACKUP con/barra\n",
        "BACKUP con espacio\n",
    ] {
        let r = srv.pide(malo, 1);
        assert!(
            r[0].starts_with("ERR invalida"),
            "{malo:?} debería rechazarse, devolvió: {}",
            r[0]
        );
    }
    // Y nada se ha creado fuera del directorio de copias.
    assert!(!Path::new("/tmp/absoluta").exists());

    let r = srv.pide("BACKUP noche\n", 1);
    assert!(r[0].starts_with("OK "), "{}", r[0]);
    assert!(r[0].contains("segmentos="), "{}", r[0]);
    let hecha = copias.path().join("noche");
    assert!(hecha.is_dir(), "la copia tiene que estar en {hecha:?}");
    assert!(!common::segments(&hecha).is_empty());

    // Repetir el mismo nombre no machaca la copia anterior.
    let r = srv.pide("BACKUP noche\n", 1);
    assert!(r[0].starts_with("ERR invalida"), "{}", r[0]);

    db.close().unwrap();
    drop(db);

    // La copia se abre en estricto y tiene las 30 claves.
    let mut o = opts(&hecha);
    o.recovery = bitacora::RecoveryPolicy::Estricto;
    let copia = Db::open(o).expect("la copia abre sin corrupción");
    assert_eq!(copia.len(), 30);
    copia.close().unwrap();
}

/// Sin `--backup-dir`, la orden no existe a efectos prácticos: no se puede
/// dejar que un cliente elija dónde escribe el servidor.
#[test]
fn sin_directorio_de_copias_backup_se_niega() {
    let tmp = TempDir::new("srv-copia-no");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    let srv = Servidor::nuevo(Arc::clone(&db), ServerConfig::new("127.0.0.1", puerto(12)));
    let r = srv.pide("BACKUP\n", 1);
    assert!(r[0].contains("no tiene directorio de copias"), "{}", r[0]);
    db.close().unwrap();
}

/// Una configuración de servidor que no hace lo que dice no llega a abrir el
/// puerto.
#[test]
fn una_configuracion_absurda_no_llega_a_escuchar() {
    let tmp = TempDir::new("srv-config");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());

    /// Nombre del campo y cómo estropearlo.
    type Caso = (&'static str, Box<dyn Fn(&mut ServerConfig)>);
    let casos: Vec<Caso> = vec![
        (
            "max_conns",
            Box::new(|c: &mut ServerConfig| c.max_conns = 0),
        ),
        (
            "keys_limit",
            Box::new(|c: &mut ServerConfig| c.keys_limit = 0),
        ),
        (
            "max_value_bytes",
            Box::new(|c: &mut ServerConfig| c.max_value_bytes = 0),
        ),
        (
            "read_timeout",
            Box::new(|c: &mut ServerConfig| c.read_timeout = Some(Duration::ZERO)),
        ),
        (
            "clave",
            Box::new(|c: &mut ServerConfig| c.clave = Some(String::new())),
        ),
    ];
    for (campo, estropea) in casos {
        let mut cfg = ServerConfig::new("127.0.0.1", puerto(13));
        estropea(&mut cfg);
        let r = serve(Arc::clone(&db), cfg);
        match r {
            Ok(()) => panic!("{campo}: no debería haber escuchado"),
            Err(e) => {
                assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput, "{campo}: {e}");
                assert!(e.to_string().contains(campo), "{campo}: {e}");
            }
        }
    }
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// Gate 7 · caducidad, CAS y namespaces por el protocolo
// ---------------------------------------------------------------------------

#[test]
fn setex_y_ttl_por_el_protocolo() {
    let tmp = TempDir::new("srv-ttl");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    let srv = Servidor::nuevo(Arc::clone(&db), ServerConfig::new("127.0.0.1", puerto(14)));

    let r = srv.pide(
        "SETEX s 60 un valor con espacios\nTTL s\nGET s\nTTL no-esta\n",
        4,
    );
    assert_eq!(r[0], "OK");
    assert_eq!(r[1], "INT 60");
    assert_eq!(r[2], "VAL un valor con espacios");
    assert_eq!(r[3], "NIL", "una clave que no está no tiene TTL");

    // Una clave sin caducidad se distingue de una que no está.
    let r = srv.pide("SET fija v\nTTL fija\n", 2);
    assert_eq!(r[1], "INT -1", "-1 es «no caduca»");

    // Y 0 segundos no es un borrado disfrazado.
    let r = srv.pide("SETEX s 0 v\n", 1);
    assert!(r[0].starts_with("ERR invalida"), "{}", r[0]);
    db.close().unwrap();
}

/// La caducidad se ve de verdad desde el protocolo, no solo se declara.
#[test]
fn una_clave_caduca_y_el_protocolo_deja_de_verla() {
    let tmp = TempDir::new("srv-ttl-vence");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    let srv = Servidor::nuevo(Arc::clone(&db), ServerConfig::new("127.0.0.1", puerto(15)));

    // Por el protocolo el mínimo es 1 segundo, así que se espera 1,2 s.
    let r = srv.pide("SETEX corta 1 adios\nGET corta\n", 2);
    assert_eq!(r[1], "VAL adios");
    thread::sleep(Duration::from_millis(1200));
    let r = srv.pide("GET corta\nEXISTS corta\nTTL corta\nKEYS\n", 4);
    assert_eq!(r[0], "NIL");
    assert_eq!(r[1], "INT 0");
    assert_eq!(r[2], "NIL");
    assert_eq!(r[3], "KEYS 0", "KEYS tampoco la ve");
    db.close().unwrap();
}

#[test]
fn getseq_y_cas_por_el_protocolo() {
    let tmp = TempDir::new("srv-cas");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    let srv = Servidor::nuevo(Arc::clone(&db), ServerConfig::new("127.0.0.1", puerto(16)));

    // seq 0 = «no debe existir»: crea si no está, y solo una vez.
    let r = srv.pide("CAS k 0 primero\nCAS k 0 otro\nGET k\n", 3);
    assert_eq!(r[0], "OK");
    assert!(r[1].starts_with("NO "), "{}", r[1]);
    assert_eq!(r[2], "VAL primero");

    // GETSEQ da valor y seq juntos; con ese seq el CAS entra.
    let r = srv.pide("GETSEQ k\n", 1);
    let campos: Vec<&str> = r[0].splitn(3, ' ').collect();
    assert_eq!(campos[0], "VALSEQ", "{}", r[0]);
    assert_eq!(campos[2], "primero");
    let seq: u64 = campos[1].parse().expect("seq numérico");

    let r = srv.pide(&format!("CAS k {seq} segundo\nGET k\n"), 2);
    assert_eq!(r[0], "OK");
    assert_eq!(r[1], "VAL segundo");

    // Y con el seq viejo ya no: devuelve NO con el seq actual, para poder
    // reintentar sin otra consulta.
    let r = srv.pide(&format!("CAS k {seq} tercero\n"), 1);
    assert!(r[0].starts_with("NO "), "{}", r[0]);
    let actual: u64 = r[0][3..].parse().expect("NO <seq>");
    assert!(actual > seq, "el seq nuevo tiene que ser mayor: {actual}");
    assert_eq!(srv.pide("GET k\n", 1)[0], "VAL segundo", "no se tocó");

    // CASDEL igual.
    let r = srv.pide(&format!("CASDEL k {seq}\nCASDEL k {actual}\nGET k\n"), 3);
    assert!(r[0].starts_with("NO "), "{}", r[0]);
    assert_eq!(r[1], "OK");
    assert_eq!(r[2], "NIL");
    db.close().unwrap();
}

#[test]
fn use_pone_un_prefijo_solo_en_su_conexion() {
    let tmp = TempDir::new("srv-ns");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    let srv = Servidor::nuevo(Arc::clone(&db), ServerConfig::new("127.0.0.1", puerto(17)));

    let r = srv.pide("USE app\nSET k de app\nGET k\nKEYS\n", 5);
    assert_eq!(r[0], "OK app:");
    assert_eq!(r[1], "OK");
    assert_eq!(r[2], "VAL de app");
    assert_eq!(r[3], "KEYS 1");
    assert_eq!(r[4], "k", "KEYS devuelve la clave SIN el prefijo");

    // Otra conexión, sin USE, ve la clave real: esto no es aislamiento.
    let r = srv.pide("GET k\nGET app:k\n", 2);
    assert_eq!(r[0], "NIL", "sin USE, `k` es otra clave");
    assert_eq!(r[1], "VAL de app");

    // USE sin argumento lo quita, en la misma conexión.
    let r = srv.pide("USE app\nSET x 1\nUSE\nGET x\nGET app:x\n", 5);
    assert_eq!(r[2], "OK");
    assert_eq!(r[3], "NIL", "ya sin prefijo");
    assert_eq!(r[4], "VAL 1");

    // Un nombre con ':' se rechaza, con el mismo motivo que en la API.
    let r = srv.pide("USE a:b\n", 1);
    assert!(r[0].starts_with("ERR invalida"), "{}", r[0]);

    // Y el prefijo alcanza también a SETEX, TTL, CAS y DEL.
    let r = srv.pide(
        "USE app\nSETEX t 60 v\nTTL t\nCAS c 0 uno\nDEL k\nEXISTS k\n",
        6,
    );
    assert_eq!(r[1], "OK");
    assert_eq!(r[2], "INT 60");
    assert_eq!(r[3], "OK");
    assert_eq!(r[4], "OK");
    assert_eq!(r[5], "INT 0");
    assert_eq!(srv.pide("GET app:c\n", 1)[0], "VAL uno");
    db.close().unwrap();
}

#[test]
fn las_ordenes_nuevas_tambien_exigen_auth() {
    let tmp = TempDir::new("srv-nuevas-auth");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    let mut cfg = ServerConfig::new("127.0.0.1", puerto(18));
    cfg.clave = Some("secreta".into());
    let srv = Servidor::nuevo(Arc::clone(&db), cfg);

    for orden in [
        "SETEX k 60 v\n",
        "TTL k\n",
        "GETSEQ k\n",
        "CAS k 0 v\n",
        "CASDEL k 1\n",
        "USE app\n",
    ] {
        let r = srv.pide(orden, 1);
        assert!(
            r[0].starts_with("ERR invalida hace falta AUTH"),
            "{orden:?} sin AUTH devolvió: {}",
            r[0]
        );
    }
    let r = srv.pide("AUTH secreta\nUSE app\nSETEX k 60 v\nTTL k\n", 4);
    assert_eq!(r[1], "OK app:");
    assert_eq!(r[3], "INT 60");
    db.close().unwrap();
}
