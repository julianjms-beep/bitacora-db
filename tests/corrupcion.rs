//! Gate 0 — integridad del WAL.
//!
//! Estas pruebas existen por un fallo real y medido: antes, **un solo bit
//! corrupto en medio de un segmento destruía todos los registros posteriores y
//! además recortaba el fichero**, así que la pérdida era permanente. La prueba
//! que había (`un_bit_corrupto_descarta_solo_ese_registro`) corrompía el
//! *último* registro, el único caso en que ese comportamiento no se nota.
//!
//! Lo que se comprueba aquí:
//!
//! - Corrupción al principio, en medio y al final del segmento.
//! - Las tres causas: cabecera inválida, CRC incorrecto y truncamiento.
//! - Que los registros válidos **posteriores** se conservan.
//! - Que el modo estricto (por defecto) se niega a abrir y **no toca ningún
//!   byte** del fichero.
//! - Que el modo reparar deja una copia del original antes de nada y declara
//!   las pérdidas en el informe.

mod common;

use bitacora::{Db, FsyncPolicy, Options, RecoveryPolicy};
use common::{segments, TempDir};
use std::path::{Path, PathBuf};

fn opts(dir: &Path, pol: RecoveryPolicy) -> Options {
    let mut o = Options::new(dir);
    o.fsync = FsyncPolicy::Never;
    o.auto_compact = false;
    o.recovery = pol;
    o
}

/// Escribe `n` claves `k0..k(n-1)` con valores reconocibles.
fn crea(dir: &Path, n: u64) {
    let db = Db::open(opts(dir, RecoveryPolicy::Estricto)).unwrap();
    for i in 0..n {
        db.put(format!("k{i}").as_bytes(), format!("valor-de-la-clave-{i}").as_bytes())
            .unwrap();
    }
    drop(db);
}

/// El segmento más grande del directorio (el que tiene los datos).
fn segmento(dir: &Path) -> (PathBuf, u64) {
    segments(dir).into_iter().max_by_key(|(_, l)| *l).unwrap()
}

/// Voltea un bit en la posición `off` del fichero.
fn voltea(path: &Path, off: u64) {
    let mut b = std::fs::read(path).unwrap();
    b[off as usize] ^= 0x01;
    std::fs::write(path, &b).unwrap();
}

/// Cuenta las claves `k0..k(n-1)` que siguen legibles y con el valor correcto.
fn supervivientes(db: &Db, n: u64) -> u64 {
    let mut vivas = 0;
    for i in 0..n {
        if let Some(v) = db.get(format!("k{i}").as_bytes()).unwrap() {
            assert_eq!(
                v,
                format!("valor-de-la-clave-{i}").into_bytes(),
                "k{i} está presente pero con el valor cambiado: eso sería corrupción silenciosa"
            );
            vivas += 1;
        }
    }
    vivas
}

// ---------------------------------------------------------------------------
// El fallo original, reproducido
// ---------------------------------------------------------------------------

/// Reproduce el experimento que encontró el fallo: 50 claves y un bit volteado
/// al 40% del fichero. Antes quedaban 20 claves y el fichero se recortaba de
/// 2.530 a 1.000 bytes. Ahora el modo estricto se niega a abrir y **no
/// modifica nada**.
#[test]
fn fallo_original_estricto_se_niega_y_no_toca_el_fichero() {
    let tmp = TempDir::new("gate0-original-estricto");
    crea(tmp.path(), 50);
    let (path, len) = segmento(tmp.path());
    let antes = std::fs::read(&path).unwrap();
    voltea(&path, len * 40 / 100);
    let tras_corromper = std::fs::read(&path).unwrap();

    let e = match Db::open(opts(tmp.path(), RecoveryPolicy::Estricto)) {
        Ok(_) => panic!("con corrupción, el modo estricto debe negarse a abrir"),
        Err(e) => e,
    };
    let msg = e.to_string();
    assert!(msg.contains("recuperación abortada"), "mensaje inesperado: {msg}");
    assert!(msg.contains("NO se ha modificado ningún fichero"), "mensaje inesperado: {msg}");

    let despues = std::fs::read(&path).unwrap();
    assert_eq!(
        despues, tras_corromper,
        "el modo estricto no debe modificar ni un byte del fichero"
    );
    assert_eq!(despues.len(), antes.len(), "y mucho menos recortarlo");
}

/// El mismo caso en modo reparar: se pierde **una** clave, no treinta, y queda
/// copia del original.
#[test]
fn fallo_original_reparar_conserva_lo_posterior() {
    let tmp = TempDir::new("gate0-original-reparar");
    crea(tmp.path(), 50);
    let (path, len) = segmento(tmp.path());
    let bytes_antes = std::fs::metadata(&path).unwrap().len();
    voltea(&path, len * 40 / 100);

    let db = Db::open(opts(tmp.path(), RecoveryPolicy::Reparar)).unwrap();
    let rep = db.recovery_report();
    assert!(rep.con_perdidas(), "la recuperación NO fue íntegra y debe decirlo");
    assert_eq!(rep.corrupt_records, 1, "una sola zona ilegible");
    assert!(rep.corrupt_bytes > 0);

    // 49 de 50: antes del arreglo quedaban 20.
    assert_eq!(supervivientes(&db, 50), 49, "solo debe perderse el registro corrupto");

    // El original se preserva y el fichero de datos no se recorta.
    assert_eq!(rep.quarantined.len(), 1, "debe haber una copia de seguridad");
    assert!(rep.quarantined[0].exists());
    assert_eq!(
        std::fs::metadata(&rep.quarantined[0]).unwrap().len(),
        bytes_antes,
        "la copia debe ser del fichero completo"
    );
    assert_eq!(std::fs::metadata(&path).unwrap().len(), bytes_antes, "no se recorta nada");
}

// ---------------------------------------------------------------------------
// Corrupción en distintas posiciones
// ---------------------------------------------------------------------------

/// Corrupción al principio, en el medio y en tres cuartos del fichero: en los
/// tres casos se pierde un registro y se conserva todo lo demás.
#[test]
fn corrupcion_en_distintas_posiciones() {
    for (nombre, pct) in [("principio", 2u64), ("medio", 50), ("tres-cuartos", 75)] {
        let tmp = TempDir::new(&format!("gate0-pos-{nombre}"));
        crea(tmp.path(), 40);
        let (path, len) = segmento(tmp.path());
        voltea(&path, len * pct / 100);

        let db = Db::open(opts(tmp.path(), RecoveryPolicy::Reparar)).unwrap();
        let vivas = supervivientes(&db, 40);
        assert_eq!(
            vivas, 39,
            "corrupción en el {nombre}: debían sobrevivir 39 de 40, sobrevivieron {vivas}"
        );
        assert_eq!(db.recovery_report().corrupt_records, 1);
    }
}

/// Dos corrupciones separadas en el mismo segmento: se pierden dos registros.
#[test]
fn dos_corrupciones_en_el_mismo_segmento() {
    let tmp = TempDir::new("gate0-dos");
    crea(tmp.path(), 60);
    let (path, len) = segmento(tmp.path());
    voltea(&path, len * 25 / 100);
    voltea(&path, len * 70 / 100);

    let db = Db::open(opts(tmp.path(), RecoveryPolicy::Reparar)).unwrap();
    assert_eq!(db.recovery_report().corrupt_records, 2);
    assert_eq!(supervivientes(&db, 60), 58);
}

/// Corrupción en el ÚLTIMO registro. No hay nada válido detrás, así que es
/// indistinguible de una escritura interrumpida: se trata como cola incompleta,
/// se recorta y **no** se declaran pérdidas de datos confirmados. Es el único
/// caso que cubría la prueba antigua.
#[test]
fn corrupcion_en_el_ultimo_registro_es_cola_incompleta() {
    let tmp = TempDir::new("gate0-ultimo");
    crea(tmp.path(), 30);
    let (path, len) = segmento(tmp.path());
    voltea(&path, len - 3);

    // No hace falta modo reparar: una cola incompleta es el caso normal.
    let db = Db::open(opts(tmp.path(), RecoveryPolicy::Estricto)).unwrap();
    let rep = db.recovery_report();
    assert!(!rep.con_perdidas(), "una cola incompleta no es pérdida de datos confirmados");
    assert_eq!(rep.truncated_tails.len(), 1);
    assert_eq!(supervivientes(&db, 30), 29);
    assert!(std::fs::metadata(&path).unwrap().len() < len, "la cola debe recortarse");
}

// ---------------------------------------------------------------------------
// Las tres causas: cabecera, CRC y truncamiento
// ---------------------------------------------------------------------------

/// Una cabecera destrozada (longitudes imposibles) no permite confiar en las
/// longitudes: el lector tiene que rastrear. Lo demás se conserva igual.
#[test]
fn cabecera_destrozada_resincroniza_rastreando() {
    let tmp = TempDir::new("gate0-cabecera");
    crea(tmp.path(), 40);
    let (path, len) = segmento(tmp.path());
    // Machaca 6 bytes a 0xff en mitad del fichero, lo que invalida key_len y
    // val_len de cualquier registro que empiece ahí.
    let mut b = std::fs::read(&path).unwrap();
    let off = (len * 50 / 100) as usize;
    for i in 0..6 {
        b[off + i] = 0xff;
    }
    std::fs::write(&path, &b).unwrap();

    let db = Db::open(opts(tmp.path(), RecoveryPolicy::Reparar)).unwrap();
    let rep = db.recovery_report();
    assert!(rep.con_perdidas());
    let vivas = supervivientes(&db, 40);
    assert!(
        vivas >= 38,
        "con la cabecera destrozada deben perderse 1 o 2 registros, no más; sobrevivieron {vivas}"
    );
    assert!(
        rep.detalle.iter().any(|d| d.contains("CORRUPCIÓN")),
        "el informe debe nombrar la corrupción: {:?}",
        rep.detalle
    );
}

/// Truncar el fichero por el medio deja una cola incompleta, no corrupción:
/// no hay nada válido detrás que proteger.
#[test]
fn truncamiento_por_el_medio_es_cola_incompleta() {
    let tmp = TempDir::new("gate0-truncado");
    crea(tmp.path(), 40);
    let (path, len) = segmento(tmp.path());
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(len / 2).unwrap();
    drop(f);

    let db = Db::open(opts(tmp.path(), RecoveryPolicy::Estricto)).unwrap();
    let rep = db.recovery_report();
    assert!(!rep.con_perdidas(), "un truncamiento no es corrupción intermedia");
    let vivas = supervivientes(&db, 40);
    assert!(vivas > 0 && vivas < 40, "debe sobrevivir el prefijo: {vivas}");
    // Y lo que sobrevive es un prefijo exacto, sin huecos.
    for i in 0..vivas {
        assert!(
            db.get(format!("k{i}").as_bytes()).unwrap().is_some(),
            "k{i} debería estar: lo recuperado tiene que ser un prefijo sin huecos"
        );
    }
}

/// Un segmento de basura entera no contiene nada legible. No se puede declarar
/// una recuperación satisfactoria, pero tampoco hay registros confirmados que
/// perder: queda como cola incompleta en el byte 0.
#[test]
fn segmento_de_basura_no_se_declara_recuperado() {
    let tmp = TempDir::new("gate0-basura");
    crea(tmp.path(), 10);
    let (path, _) = segmento(tmp.path());
    let basura: Vec<u8> = (0..800u32).map(|i| (i % 256) as u8).collect();
    std::fs::write(&path, &basura).unwrap();

    let db = Db::open(opts(tmp.path(), RecoveryPolicy::Estricto)).unwrap();
    assert_eq!(supervivientes(&db, 10), 0, "no debe inventarse ninguna clave");
    assert_eq!(db.recovery_report().truncated_tails.len(), 1);
}

// ---------------------------------------------------------------------------
// Lo que no debe cambiar
// ---------------------------------------------------------------------------

/// Un fichero intacto no produce ni avisos ni pérdidas: el camino normal sigue
/// siendo silencioso y el informe lo confirma.
#[test]
fn sin_corrupcion_no_hay_informe_de_perdidas() {
    let tmp = TempDir::new("gate0-limpio");
    crea(tmp.path(), 100);
    let db = Db::open(opts(tmp.path(), RecoveryPolicy::Estricto)).unwrap();
    let rep = db.recovery_report();
    assert!(!rep.con_perdidas());
    assert_eq!(rep.corrupt_records, 0);
    assert_eq!(rep.truncated_tails.len(), 0);
    assert_eq!(rep.quarantined.len(), 0);
    assert_eq!(rep.records_ok, 100);
    assert_eq!(supervivientes(&db, 100), 100);
}

/// La corrupción sale en `STATS`, para que un operador la vea sin leer el log.
#[test]
fn stats_declara_la_corrupcion() {
    let tmp = TempDir::new("gate0-stats");
    crea(tmp.path(), 30);
    let (path, len) = segmento(tmp.path());
    voltea(&path, len * 50 / 100);
    let db = Db::open(opts(tmp.path(), RecoveryPolicy::Reparar)).unwrap();
    let lineas = db.stats().lines();
    assert!(
        lineas.iter().any(|l| l == "recovery_corrupt_records 1"),
        "STATS debe declarar las pérdidas: {lineas:?}"
    );
}
