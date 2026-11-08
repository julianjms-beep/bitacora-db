//! Pruebas de concurrencia con hilos reales del sistema operativo.

mod common;

use bitacora::{Db, FsyncPolicy, Options};
use common::TempDir;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn opts(dir: &std::path::Path) -> Options {
    let mut o = Options::new(dir);
    o.fsync = FsyncPolicy::Never;
    o
}

#[test]
fn escrituras_paralelas_sin_perdidas_y_coherentes_tras_reinicio() {
    let tmp = TempDir::new("parallel");
    let o = opts(tmp.path());
    const T: u64 = 8;
    const N: u64 = 3000;
    {
        let db = Arc::new(Db::open(o.clone()).unwrap());
        let hs: Vec<_> = (0..T)
            .map(|t| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for i in 0..N {
                        db.put(
                            format!("t{t}-k{i}").as_bytes(),
                            format!("v{t}-{i}").as_bytes(),
                        )
                        .unwrap();
                        // lectura cruzada: la clave que otro hilo quizá ya escribió
                        let other = (t + 1) % T;
                        if let Some(v) = db.get(format!("t{other}-k{i}").as_bytes()).unwrap() {
                            assert_eq!(v, format!("v{other}-{i}").into_bytes());
                        }
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(db.len() as u64, T * N);
    }
    let db = Db::open(o).unwrap();
    assert_eq!(db.len() as u64, T * N);
    for t in 0..T {
        for i in (0..N).step_by(97) {
            assert_eq!(
                db.get(format!("t{t}-k{i}").as_bytes()).unwrap().unwrap(),
                format!("v{t}-{i}").into_bytes()
            );
        }
    }
}

/// Muchos hilos machacando la misma clave: lo que ve la memoria al final
/// tiene que ser exactamente lo que reconstruye el log al reiniciar.
#[test]
fn clave_caliente_memoria_y_log_coinciden() {
    let tmp = TempDir::new("hot");
    let o = opts(tmp.path());
    let en_memoria;
    {
        let db = Arc::new(Db::open(o.clone()).unwrap());
        let hs: Vec<_> = (0..8u64)
            .map(|t| {
                let db = Arc::clone(&db);
                thread::spawn(move || {
                    for i in 0..2000u64 {
                        db.put(b"hot", format!("t{t}-{i}").as_bytes()).unwrap();
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        en_memoria = db.get(b"hot").unwrap().unwrap();
        assert!(
            en_memoria.ends_with(b"-1999"),
            "la última escritura de algún hilo"
        );
    }
    let db = Db::open(o).unwrap();
    assert_eq!(db.get(b"hot").unwrap().unwrap(), en_memoria);
}

#[test]
fn compactar_mientras_se_escribe_y_se_lee() {
    let tmp = TempDir::new("compact-live");
    let mut o = opts(tmp.path());
    o.max_segment_bytes = 16 << 10;
    o.auto_compact = false;
    let db = Arc::new(Db::open(o.clone()).unwrap());
    let stop = Arc::new(AtomicBool::new(false));

    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut last = vec![0u64; 200];
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let k = (n % 200) as usize;
                db.put(format!("k{k}").as_bytes(), format!("v{n}").as_bytes())
                    .unwrap();
                last[k] = n;
                n += 1;
            }
            last
        })
    };
    let reader = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut reads = 0u64;
            while !stop.load(Ordering::Relaxed) {
                for k in 0..200 {
                    if let Some(v) = db.get(format!("k{k}").as_bytes()).unwrap() {
                        assert!(
                            v.starts_with(b"v"),
                            "valor basura leído durante la compactación"
                        );
                        reads += 1;
                    }
                }
            }
            reads
        })
    };

    let mut compactions = 0;
    for _ in 0..8 {
        thread::sleep(Duration::from_millis(60));
        let rep = db.compact().unwrap();
        if rep.ran && rep.segments_removed > 0 {
            compactions += 1;
        }
    }
    stop.store(true, Ordering::Relaxed);
    let last = writer.join().unwrap();
    let reads = reader.join().unwrap();
    assert!(
        compactions >= 3,
        "deberían haberse ejecutado varias compactaciones ({compactions})"
    );
    assert!(
        reads > 1000,
        "el lector debería haber leído mucho ({reads})"
    );

    for (k, n) in last.iter().enumerate() {
        assert_eq!(
            db.get(format!("k{k}").as_bytes()).unwrap().unwrap(),
            format!("v{n}").into_bytes()
        );
    }
    drop(db);
    let db = Db::open(o).unwrap();
    assert_eq!(db.len(), 200);
    for (k, n) in last.iter().enumerate() {
        assert_eq!(
            db.get(format!("k{k}").as_bytes()).unwrap().unwrap(),
            format!("v{n}").into_bytes()
        );
    }
}

#[test]
fn group_commit_con_muchos_hilos_devuelve_cuando_esta_en_disco() {
    let tmp = TempDir::new("group");
    let mut o = opts(tmp.path());
    o.fsync = FsyncPolicy::Group;
    let db = Arc::new(Db::open(o).unwrap());
    let hs: Vec<_> = (0..16u64)
        .map(|t| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for i in 0..200u64 {
                    db.put(format!("g{t}-{i}").as_bytes(), b"x").unwrap();
                    let st = db.stats();
                    assert!(st.synced_seq <= st.last_seq);
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }
    let st = db.stats();
    assert_eq!(st.keys, 16 * 200);
    // tras el último put, todo lo escrito está sincronizado
    assert_eq!(st.synced_seq, st.last_seq);
}
