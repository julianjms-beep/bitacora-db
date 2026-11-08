//! Gate 1 — qué pasa cuando el disco falla (C2) y durabilidad de los nombres
//! de fichero (C3).
//!
//! El fallo que arreglan estas pruebas: el hilo de sincronización adelantaba la
//! marca «esto está en disco» **también cuando el `fsync` había fallado**. La
//! promesa de durabilidad era falsa. Quedaba tapado por accidente porque el
//! error se guardaba aparte y nunca se limpiaba, así que todos los escritores
//! posteriores fallaban; pero cualquier cambio en ese manejo del error lo
//! convertía en pérdida silenciosa.
//!
//! Se usa la inyección de fallos `Options::fsync_falla_tras`, que hace que el
//! `fsync` número N+1 devuelva error.

mod common;

use bitacora::{Db, Error, FsyncPolicy, Options};
use common::TempDir;
use std::path::Path;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

fn opts(dir: &Path, fsync: FsyncPolicy, falla_tras: Option<u64>) -> Options {
    let mut o = Options::new(dir);
    o.fsync = fsync;
    o.auto_compact = false;
    o.fsync_falla_tras = falla_tras;
    o
}

// ---------------------------------------------------------------------------
// C2: un fsync fallido no se presenta como éxito
// ---------------------------------------------------------------------------

/// Con `fsync=always`, el `put` cuyo `fsync` falla devuelve error, la base
/// queda no escribible, la marca de durabilidad **no** llega a ese `seq`, y las
/// lecturas siguen funcionando.
#[test]
fn fsync_fallido_no_adelanta_la_marca_ni_se_declara_exito() {
    let tmp = TempDir::new("c2-always");
    let db = Db::open(opts(tmp.path(), FsyncPolicy::Always, Some(3))).unwrap();

    // Los tres primeros fsync van bien.
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();
    let st_ok = db.stats();
    assert_eq!(st_ok.health, "ok");
    assert_eq!(
        st_ok.synced_seq, st_ok.last_seq,
        "sin fallos, todo está en disco"
    );

    // El cuarto falla: el cliente se tiene que enterar.
    let e = match db.put(b"d", b"4") {
        Ok(()) => panic!("un fsync fallido no puede devolver OK"),
        Err(e) => e,
    };
    assert!(
        matches!(e, Error::NoEscribible(_)),
        "categoría equivocada: {} ({e})",
        e.categoria()
    );
    assert!(!e.reintentable(), "reintentar no arregla un disco caído");

    // Y la marca de durabilidad no ha avanzado hasta el registro que falló.
    let st = db.stats();
    assert!(
        st.synced_seq < st.last_seq,
        "la marca de durabilidad avanzó tras un fsync fallido: synced={} last={}",
        st.synced_seq,
        st.last_seq
    );
    assert!(
        st.health.starts_with("no-escribible"),
        "health: {}",
        st.health
    );

    // A partir de aquí no se acepta ninguna escritura más.
    assert!(
        db.put(b"e", b"5").is_err(),
        "no debe aceptar más escrituras"
    );
    assert!(db.delete(b"a").is_err(), "tampoco borrados");

    // Pero leer sigue funcionando: lo que estaba confirmado, está.
    assert_eq!(db.get(b"a").unwrap().unwrap(), b"1");
    assert_eq!(db.get(b"c").unwrap().unwrap(), b"3");
}

/// El modo de fallo que introduce el arreglo ingenuo: si no se avanza la marca
/// y tampoco se despierta a los que esperan, **el escritor se cuelga para
/// siempre**. Esta prueba falla por tiempo de espera si eso pasa.
#[test]
fn con_group_commit_el_escritor_no_se_cuelga_al_fallar_el_fsync() {
    let tmp = TempDir::new("c2-group");
    // El primer fsync ya falla.
    let db = Arc::new(Db::open(opts(tmp.path(), FsyncPolicy::Group, Some(0))).unwrap());

    let (tx, rx) = mpsc::channel();
    let d = Arc::clone(&db);
    std::thread::spawn(move || {
        let r = d.put(b"a", b"1").map_err(|e| e.to_string());
        let _ = tx.send(r);
    });

    let r = rx.recv_timeout(Duration::from_secs(10)).expect(
        "el escritor se ha colgado: al no avanzar la marca hay que despertar \
         a los que esperaban en la condición",
    );
    let e = match r {
        Ok(()) => panic!("el fsync falló, así que el put no puede devolver OK"),
        Err(e) => e,
    };
    assert!(e.contains("NO ESCRIBIBLE"), "error poco claro: {e}");
    assert!(db.stats().health.starts_with("no-escribible"));
}

/// Varios escritores esperando a la vez: todos tienen que recibir el error.
/// Ninguno se queda dentro esperando un `fsync` que ya no va a llegar.
#[test]
fn todos_los_escritores_que_esperaban_reciben_el_error() {
    let tmp = TempDir::new("c2-varios");
    let db = Arc::new(Db::open(opts(tmp.path(), FsyncPolicy::Group, Some(0))).unwrap());

    let (tx, rx) = mpsc::channel();
    for i in 0..8u64 {
        let d = Arc::clone(&db);
        let tx = tx.clone();
        std::thread::spawn(move || {
            let r = d.put(format!("k{i}").as_bytes(), b"v").is_err();
            let _ = tx.send(r);
        });
    }
    drop(tx);

    let mut recibidos = 0;
    let mut con_error = 0;
    while let Ok(fallo) = rx.recv_timeout(Duration::from_secs(10)) {
        recibidos += 1;
        if fallo {
            con_error += 1;
        }
    }
    assert_eq!(recibidos, 8, "algún escritor se quedó colgado");
    assert_eq!(con_error, 8, "todos los escritores deben recibir el error");
}

/// Tras reiniciar, la base vuelve a ser escribible y conserva lo que de verdad
/// llegó a disco. El estado de fallo es del proceso, no del fichero.
#[test]
fn tras_reiniciar_vuelve_a_ser_escribible() {
    let tmp = TempDir::new("c2-reinicio");
    {
        let db = Db::open(opts(tmp.path(), FsyncPolicy::Always, Some(2))).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        assert!(db.put(b"c", b"3").is_err(), "el tercer fsync debía fallar");
    }

    // Sin inyección de fallos: el disco va bien otra vez.
    let db = Db::open(opts(tmp.path(), FsyncPolicy::Always, None)).unwrap();
    assert_eq!(db.stats().health, "ok", "un proceso nuevo empieza sano");
    assert_eq!(db.get(b"a").unwrap().unwrap(), b"1");
    assert_eq!(db.get(b"b").unwrap().unwrap(), b"2");
    db.put(b"d", b"4").unwrap();
    assert_eq!(db.get(b"d").unwrap().unwrap(), b"4");
}

/// Un fallo de `fsync` al cerrar un segmento (`roll`) también deja la base no
/// escribible, en vez de pasar inadvertido.
#[test]
fn fallo_al_cerrar_segmento_tambien_deja_la_base_no_escribible() {
    let tmp = TempDir::new("c2-roll");
    let mut o = opts(tmp.path(), FsyncPolicy::Never, None);
    o.max_segment_bytes = 400; // segmentos minúsculos para forzar varios roll
    o.fsync_falla_tras = Some(0); // el primer fsync (el del roll) falla
    let db = Db::open(o).unwrap();

    // Con fsync=never los put no sincronizan; el primer fsync llega al rodar.
    let mut fallo = false;
    for i in 0..60u64 {
        if db
            .put(format!("k{i}").as_bytes(), b"valor-de-relleno")
            .is_err()
        {
            fallo = true;
            break;
        }
    }
    assert!(
        fallo,
        "al cerrar un segmento el fsync debía fallar y propagarse"
    );
    assert!(db.stats().health.starts_with("no-escribible"));
}

// ---------------------------------------------------------------------------
// C3: durabilidad de los nombres de fichero
// ---------------------------------------------------------------------------

/// No se puede observar un `fsync` de directorio desde una prueba, así que esto
/// **no demuestra C3**: comprueba que añadirlo no ha roto el ciclo de vida de
/// los segmentos (crear, rodar, compactar, borrar y reabrir). La corrección de
/// C3 se sostiene por revisión de código y está anotada como no verificable por
/// prueba en `docs/RECUPERACION.md`.
#[test]
fn el_ciclo_de_vida_de_los_segmentos_sigue_funcionando() {
    let tmp = TempDir::new("c3-ciclo");
    let mut o = opts(tmp.path(), FsyncPolicy::Group, None);
    o.max_segment_bytes = 4 << 10;
    {
        let db = Db::open(o.clone()).unwrap();
        for ronda in 0..10u64 {
            for i in 0..50u64 {
                db.put(
                    format!("k{i}").as_bytes(),
                    format!("r{ronda}-{i}").as_bytes(),
                )
                .unwrap();
            }
        }
        let rep = db.compact().unwrap();
        assert!(
            rep.ran && rep.segments_removed > 0,
            "la compactación debía retirar segmentos"
        );
        assert_eq!(db.len(), 50);
    }
    let db = Db::open(o).unwrap();
    assert_eq!(db.len(), 50);
    for i in 0..50u64 {
        assert_eq!(
            db.get(format!("k{i}").as_bytes()).unwrap().unwrap(),
            format!("r9-{i}").into_bytes()
        );
    }
    assert_eq!(db.stats().health, "ok");
}
