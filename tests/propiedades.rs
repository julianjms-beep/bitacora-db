//! Gate 4 — pruebas por propiedades y barrido aleatorio del lector del WAL.
//!
//! Las pruebas de los gates anteriores comprueban casos concretos que yo elegí,
//! y eso tiene un sesgo evidente: solo encuentran lo que se me ocurrió buscar.
//! Aquí se generan miles de casos al azar y, en vez de comparar con un
//! resultado esperado, se comprueban **invariantes**:
//!
//! - Todo registro codificado se vuelve a leer idéntico.
//! - El lector **nunca** entra en panic, cualquiera que sea el contenido, y
//!   **siempre avanza**: sin eso, el rastreo de resincronización podría quedarse
//!   en un bucle infinito.
//! - La recuperación nunca **inventa** datos: toda clave recuperada era una de
//!   las escritas, con su valor correcto.
//! - El motor se comporta igual que un `HashMap` de referencia, también
//!   después de reiniciar.
//!
//! # Sobre las dependencias
//!
//! No se usa `proptest`: el generador es propio y sin dependencias, para que
//! `cargo test` siga funcionando sin red y el «cero dependencias» del proyecto
//! sea literal también para las pruebas. Lo que se pierde es el *shrinking*
//! automático, que reduce un fallo al caso mínimo. A cambio, cada propiedad
//! tiene una semilla fija y cada aserción imprime su número de iteración, así
//! que un fallo se reproduce exacto.

mod common;

use bitacora::wal::{self, Kind, ReadOutcome, SegmentReader, HEADER, MAX_KEY};
use bitacora::{Db, FsyncPolicy, Options, RecoveryPolicy};
use common::TempDir;
use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;

/// xorshift64*, determinista. No es criptográfico y no pretende serlo: hace
/// falta que sea reproducible, no impredecible.
struct Az(u64);

impl Az {
    fn new(semilla: u64) -> Az {
        Az(semilla | 1)
    }

    fn u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Entero en `0..n`.
    fn hasta(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.u64() % n as u64) as usize
        }
    }

    fn bytes(&mut self, n: usize) -> Vec<u8> {
        (0..n).map(|_| (self.u64() & 0xff) as u8).collect()
    }
}

/// Semilla base. Por defecto 0, para que `cargo test` sea reproducible y la CI
/// siempre compruebe los mismos casos. Con `BITACORA_SEMILLA=n` se barre otra
/// franja del espacio, que es como se usa esto para buscar fallos de verdad.
fn semilla_base() -> u64 {
    std::env::var("BITACORA_SEMILLA")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

/// Multiplicador de iteraciones, para barridos largos: `BITACORA_ITERS=20`.
fn iters(base: u64) -> u64 {
    let m: u64 = std::env::var("BITACORA_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    base * m.max(1)
}

fn opts(dir: &Path, pol: RecoveryPolicy) -> Options {
    let mut o = Options::new(dir);
    o.fsync = FsyncPolicy::Never;
    o.auto_compact = false;
    o.recovery = pol;
    o
}

fn lector(buf: Vec<u8>) -> SegmentReader<Cursor<Vec<u8>>> {
    let len = buf.len() as u64;
    SegmentReader::new(Cursor::new(buf), len)
}

// ---------------------------------------------------------------------------
// El formato
// ---------------------------------------------------------------------------

/// Cualquier tanda de registros se vuelve a leer idéntica: mismas claves,
/// mismos valores, mismo orden, mismos `seq`.
#[test]
fn todo_registro_vuelve_igual() {
    for iter in 0..iters(300) {
        let mut az = Az::new(0xC0FFEE ^ iter ^ semilla_base());
        let cuantos = 1 + az.hasta(8);
        let mut esperados = Vec::new();
        let mut buf = Vec::new();
        for i in 0..cuantos {
            let klen = 1 + az.hasta(40);
            let vlen = az.hasta(300);
            let k = az.bytes(klen);
            let kind = if az.hasta(4) == 0 {
                Kind::Del
            } else {
                Kind::Put
            };
            let v = if kind == Kind::Del {
                Vec::new()
            } else {
                az.bytes(vlen)
            };
            wal::encode(&mut buf, (i + 1) as u64, kind, &k, &v);
            esperados.push((i as u64 + 1, kind, k, v));
        }

        let mut rd = lector(buf);
        let mut leidos = Vec::new();
        loop {
            match rd.siguiente().unwrap() {
                ReadOutcome::Record(r) => leidos.push((r.seq, r.kind, r.key, r.value)),
                ReadOutcome::End => break,
                ReadOutcome::IncompleteTail { at, cause } => panic!(
                    "iter {iter}: un log recién escrito no puede tener una cola \
                     incompleta en {at} ({})",
                    cause.name()
                ),
                ReadOutcome::Corrupt { at, cause, .. } => panic!(
                    "iter {iter}: un log recién escrito no puede estar corrupto \
                     en {at} ({})",
                    cause.name()
                ),
            }
        }
        assert_eq!(leidos, esperados, "iter {iter}: ida y vuelta distinta");
    }
}

// ---------------------------------------------------------------------------
// Barrido aleatorio del lector
// ---------------------------------------------------------------------------

/// El lector tiene que sobrevivir a **cualquier** secuencia de bytes sin entrar
/// en panic y sin quedarse dando vueltas. La segunda parte importa de verdad:
/// el rastreo de resincronización busca candidatos byte a byte, así que si
/// alguna rama no avanzara el desplazamiento, el bucle sería infinito.
#[test]
fn el_lector_sobrevive_a_cualquier_basura_y_siempre_avanza() {
    for iter in 0..iters(600) {
        let mut az = Az::new(0xBA5E ^ iter ^ semilla_base());
        // Tres clases de entrada: basura pura, un log válido mutado, y un log
        // válido truncado por un sitio arbitrario.
        let clase = az.hasta(3);
        let buf = match clase {
            0 => {
                let n = az.hasta(600);
                az.bytes(n)
            }
            _ => {
                let mut b = Vec::new();
                for i in 0..(1 + az.hasta(6)) {
                    let kl = 1 + az.hasta(20);
                    let k = az.bytes(kl);
                    let vl = az.hasta(100);
                    let v = az.bytes(vl);
                    wal::encode(&mut b, i as u64 + 1, Kind::Put, &k, &v);
                }
                if clase == 1 && !b.is_empty() {
                    for _ in 0..(1 + az.hasta(5)) {
                        let pos = az.hasta(b.len());
                        b[pos] ^= (az.u64() & 0xff) as u8;
                    }
                } else if !b.is_empty() {
                    let corte = az.hasta(b.len());
                    b.truncate(corte);
                }
                b
            }
        };

        let total = buf.len() as u64;
        let mut rd = lector(buf);
        let mut anterior = 0u64;
        let mut vueltas = 0u32;
        loop {
            vueltas += 1;
            assert!(
                vueltas < 10_000,
                "iter {iter} (clase {clase}): el lector no termina; probablemente \
                 alguna rama no avanza el desplazamiento"
            );
            let salida = rd
                .siguiente()
                .unwrap_or_else(|e| panic!("iter {iter}: error de E/S leyendo de memoria: {e}"));
            let ahora = rd.offset();
            match salida {
                ReadOutcome::End => break,
                ReadOutcome::IncompleteTail { at, .. } => {
                    assert!(
                        at <= total,
                        "iter {iter}: cola fuera del fichero: {at} > {total}"
                    );
                    break;
                }
                ReadOutcome::Corrupt { at, resync_at, .. } => {
                    assert!(
                        resync_at > at,
                        "iter {iter}: resincronizar hacia atrás o en el mismo sitio \
                         ({at} -> {resync_at}) sería un bucle infinito"
                    );
                    assert!(resync_at <= total, "iter {iter}: resync fuera del fichero");
                }
                ReadOutcome::Record(r) => {
                    assert!(!r.key.is_empty(), "iter {iter}: clave vacía aceptada");
                    assert!(
                        r.key.len() <= MAX_KEY,
                        "iter {iter}: clave sobredimensionada"
                    );
                    assert!(
                        r.vpos >= r.offset + HEADER as u64,
                        "iter {iter}: el valor no puede empezar dentro de la cabecera"
                    );
                    assert!(
                        r.vpos + r.value.len() as u64 <= total,
                        "iter {iter}: el valor se sale del fichero"
                    );
                }
            }
            assert!(
                ahora > anterior,
                "iter {iter} (clase {clase}): el desplazamiento no avanzó ({anterior} -> {ahora})"
            );
            anterior = ahora;
        }
    }
}

// ---------------------------------------------------------------------------
// La recuperación no inventa
// ---------------------------------------------------------------------------

/// Tras corromper bytes al azar, todo lo que se recupere tiene que ser algo que
/// se escribió, con su valor correcto. Perder datos es admisible y se declara;
/// **inventarlos no lo es nunca**, porque sería corrupción silenciosa.
#[test]
fn la_recuperacion_nunca_inventa_datos() {
    for iter in 0..iters(40) {
        let mut az = Az::new(0xDA7A ^ iter ^ semilla_base());
        let tmp = TempDir::new(&format!("prop-inventa-{iter}"));
        let n = 20 + az.hasta(40);
        let mut modelo: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        {
            let db = Db::open(opts(tmp.path(), RecoveryPolicy::Estricto)).unwrap();
            for i in 0..n {
                let k = format!("k{i:04}").into_bytes();
                let vl = 1 + az.hasta(60);
                let v = az.bytes(vl);
                db.put(&k, &v).unwrap();
                modelo.insert(k, v);
            }
        }

        let (ruta, len) = common::segments(tmp.path())
            .into_iter()
            .max_by_key(|(_, l)| *l)
            .unwrap();
        let mut bytes = std::fs::read(&ruta).unwrap();
        for _ in 0..(1 + az.hasta(3)) {
            let pos = az.hasta(len as usize);
            bytes[pos] ^= 1 << az.hasta(8);
        }
        std::fs::write(&ruta, &bytes).unwrap();

        // Reparar puede perder registros; no puede fabricarlos.
        let db = match Db::open(opts(tmp.path(), RecoveryPolicy::Reparar)) {
            Ok(db) => db,
            Err(e) => panic!("iter {iter}: reparar debería poder abrir: {e}"),
        };
        for (k, esperado) in &modelo {
            if let Some(v) = db.get(k).unwrap() {
                assert_eq!(
                    &v,
                    esperado,
                    "iter {iter}: la clave {} se recuperó con un valor distinto del escrito",
                    String::from_utf8_lossy(k)
                );
            }
        }
        // Y no puede aparecer ninguna clave que nunca se escribiera.
        let (claves, _) = db.keys(b"", 10_000);
        for k in claves {
            assert!(
                modelo.contains_key(&k),
                "iter {iter}: apareció una clave que nunca se escribió: {}",
                String::from_utf8_lossy(&k)
            );
        }
    }
}

// ---------------------------------------------------------------------------
// El motor contra un modelo
// ---------------------------------------------------------------------------

/// Una secuencia aleatoria de operaciones tiene que dejar el motor exactamente
/// donde lo dejaría un `HashMap`, antes y después de reiniciar. Esto cubre
/// combinaciones que no se me habrían ocurrido a mano: sobrescribir, borrar lo
/// que no existe, reusar una clave recién borrada, compactar en medio.
#[test]
fn el_motor_coincide_con_un_modelo() {
    for iter in 0..iters(25) {
        let mut az = Az::new(0x4040 ^ iter ^ semilla_base());
        let tmp = TempDir::new(&format!("prop-modelo-{iter}"));
        let mut o = opts(tmp.path(), RecoveryPolicy::Estricto);
        o.max_segment_bytes = 2 << 10; // segmentos pequeños: muchos roll
        let mut modelo: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        {
            let db = Db::open(o.clone()).unwrap();
            for paso in 0..300 {
                // Pocas claves distintas, para que haya colisiones de verdad.
                let k = format!("c{}", az.hasta(25)).into_bytes();
                match az.hasta(10) {
                    0..=5 => {
                        let vl = az.hasta(80);
                        let v = az.bytes(vl);
                        db.put(&k, &v).unwrap();
                        modelo.insert(k.clone(), v);
                    }
                    6..=7 => {
                        let habia = db.delete(&k).unwrap();
                        let habia_modelo = modelo.remove(&k).is_some();
                        assert_eq!(
                            habia, habia_modelo,
                            "iter {iter} paso {paso}: DEL devolvió {habia} y el modelo {habia_modelo}"
                        );
                    }
                    8 => {
                        db.compact().unwrap();
                    }
                    _ => {}
                }
                assert_eq!(
                    db.get(&k).unwrap().as_ref(),
                    modelo.get(&k),
                    "iter {iter} paso {paso}: discrepancia en {}",
                    String::from_utf8_lossy(&k)
                );
                assert_eq!(
                    db.exists(&k),
                    modelo.contains_key(&k),
                    "iter {iter} paso {paso}: EXISTS discrepa"
                );
            }
            assert_eq!(
                db.len(),
                modelo.len(),
                "iter {iter}: número de claves distinto"
            );
        }

        // Y lo mismo después de reiniciar.
        let db = Db::open(o).unwrap();
        assert_eq!(
            db.len(),
            modelo.len(),
            "iter {iter}: tras reiniciar sobran o faltan claves"
        );
        for (k, v) in &modelo {
            assert_eq!(
                db.get(k).unwrap().as_ref(),
                Some(v),
                "iter {iter}: tras reiniciar cambió {}",
                String::from_utf8_lossy(k)
            );
        }
        let (claves, hay_mas) = db.keys(b"", 10_000);
        assert!(!hay_mas, "iter {iter}: no debería truncar con tope 10.000");
        assert_eq!(
            claves.len(),
            modelo.len(),
            "iter {iter}: KEYS no coincide con el modelo"
        );
    }
}
