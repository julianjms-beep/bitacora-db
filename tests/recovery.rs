//! Pruebas de durabilidad: apagón real (SIGKILL a mitad de escritura),
//! cola rota, bit corrupto, borrados + compactación + reinicio.

mod common;

use bitacora::{demo_value, Db, FsyncPolicy, Options};
use common::{segments, TempDir};
use std::io::Write;
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

fn opts(dir: &std::path::Path) -> Options {
    let mut o = Options::new(dir);
    o.fsync = FsyncPolicy::Never;
    o.auto_compact = false;
    o
}

/// Lanza el binario escribiendo k0,k1,k2... y lo mata con SIGKILL. Al reabrir,
/// las claves presentes deben ser exactamente un prefijo k0..k(n-1) con sus
/// valores correctos: ni corrupción, ni huecos, ni basura.
#[test]
fn sobrevive_a_un_kill_9_a_mitad_de_escritura() {
    let tmp = TempDir::new("kill9");
    let mut start = 0u64;
    for ronda in 0..3 {
        let mut child = Command::new(env!("CARGO_BIN_EXE_bitacora"))
            .args(["__crashwrite", tmp.path().to_str().unwrap(), "group", &start.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("lanzar escritor");
        thread::sleep(Duration::from_millis(350));
        child.kill().expect("kill -9");
        let _ = child.wait();

        let db = Db::open(opts(tmp.path())).expect("reabrir tras el apagón");
        let n = db.len() as u64;
        assert!(n > start, "ronda {ronda}: debería haber escrito algo (n={n}, start={start})");
        for i in 0..n {
            let v = db.get(format!("k{i}").as_bytes()).unwrap();
            assert_eq!(v.as_deref(), Some(&demo_value(i)[..]), "ronda {ronda}: k{i} corrupta o perdida");
        }
        assert!(db.get(format!("k{n}").as_bytes()).unwrap().is_none(), "ronda {ronda}: hay huecos");
        drop(db);
        start = n;
    }
}

#[test]
fn cola_rota_se_recorta_y_lo_demas_sobrevive() {
    let tmp = TempDir::new("torn");
    {
        let db = Db::open(opts(tmp.path())).unwrap();
        for i in 0..200u64 {
            db.put(format!("k{i}").as_bytes(), &demo_value(i)).unwrap();
        }
    }
    // Simula un apagón: añade al final del segmento 40 bytes de un registro
    // válido que nunca se terminó de escribir.
    let (path, len_ok) = segments(tmp.path()).into_iter().max_by_key(|(_, l)| *l).unwrap();
    let mut rec = Vec::new();
    bitacora::wal::encode(&mut rec, 999, bitacora::wal::Kind::Put, b"k_rota", &vec![b'z'; 500]);
    rec.truncate(40);
    std::fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(&rec).unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().len(), len_ok + 40);

    let db = Db::open(opts(tmp.path())).unwrap();
    assert_eq!(db.len(), 200);
    for i in 0..200u64 {
        assert_eq!(db.get(format!("k{i}").as_bytes()).unwrap().unwrap(), demo_value(i));
    }
    assert!(db.get(b"k_rota").unwrap().is_none());
    assert_eq!(std::fs::metadata(&path).unwrap().len(), len_ok, "la cola rota debe recortarse");
}

#[test]
fn un_bit_corrupto_descarta_solo_ese_registro() {
    let tmp = TempDir::new("bitflip");
    {
        let db = Db::open(opts(tmp.path())).unwrap();
        for i in 0..50u64 {
            db.put(format!("k{i}").as_bytes(), &demo_value(i)).unwrap();
        }
    }
    let (path, len) = segments(tmp.path()).into_iter().max_by_key(|(_, l)| *l).unwrap();
    let mut bytes = std::fs::read(&path).unwrap();
    let last = demo_value(49).len() as u64; // el último valor está al final del fichero
    let idx = (len - last + 2) as usize;
    bytes[idx] ^= 0x01;
    std::fs::write(&path, &bytes).unwrap();

    let db = Db::open(opts(tmp.path())).unwrap();
    assert_eq!(db.len(), 49, "solo el registro corrupto se pierde");
    assert!(db.get(b"k49").unwrap().is_none());
    assert_eq!(db.get(b"k48").unwrap().unwrap(), demo_value(48));
}

#[test]
fn borrados_sobreviven_a_compactacion_y_reinicio() {
    let tmp = TempDir::new("del-compact");
    let mut o = opts(tmp.path());
    o.max_segment_bytes = 8 << 10; // segmentos pequeños para forzar varios
    {
        let db = Db::open(o.clone()).unwrap();
        for ronda in 0..20u64 {
            for i in 0..100u64 {
                db.put(format!("k{i}").as_bytes(), format!("r{ronda}-{i}").as_bytes()).unwrap();
            }
        }
        for i in (0..100u64).step_by(2) {
            assert!(db.delete(format!("k{i}").as_bytes()).unwrap());
        }
        assert!(!db.delete(b"k0").unwrap(), "borrar dos veces devuelve false");
        let before = common::disk_bytes(tmp.path());
        let rep = db.compact().unwrap();
        assert!(rep.ran && rep.segments_removed > 0);
        let after = common::disk_bytes(tmp.path());
        assert!(after < before / 4, "compactar debe liberar espacio: {before} -> {after}");
        assert_eq!(db.len(), 50);
    }
    let db = Db::open(o).unwrap();
    assert_eq!(db.len(), 50);
    for i in 0..100u64 {
        let v = db.get(format!("k{i}").as_bytes()).unwrap();
        if i % 2 == 0 {
            assert!(v.is_none(), "k{i} debía seguir borrada tras reiniciar");
        } else {
            assert_eq!(v.unwrap(), format!("r19-{i}").into_bytes());
        }
    }
}

#[test]
fn claves_y_valores_binarios_y_limites() {
    let tmp = TempDir::new("binario");
    let db = Db::open(opts(tmp.path())).unwrap();
    let key: Vec<u8> = (0..=255u8).collect();
    let val: Vec<u8> = (0..70_000u32).map(|i| (i % 251) as u8).collect();
    db.put(&key, &val).unwrap();
    db.put(b"vacio", b"").unwrap();
    assert_eq!(db.get(&key).unwrap().unwrap(), val);
    assert_eq!(db.get(b"vacio").unwrap().unwrap(), b"");
    assert!(db.put(b"", b"x").is_err(), "clave vacía rechazada");
    assert!(db.put(&vec![b'k'; 70_000], b"x").is_err(), "clave de 70 KB rechazada");
    drop(db);
    let db = Db::open(opts(tmp.path())).unwrap();
    assert_eq!(db.get(&key).unwrap().unwrap(), val);
    assert_eq!(db.keys(b"va"), vec![b"vacio".to_vec()]);
}
