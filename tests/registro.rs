//! Gate 6 — el registro estructurado, de punta a punta.
//!
//! Las pruebas unitarias de `log` comprueban el formateador. Esto comprueba lo
//! otro: que lo que acaba **en el fichero**, emitido por el motor de verdad y
//! con una ruta hostil por medio, sigue siendo JSON válido línea a línea.
//!
//! Va en su propio fichero de pruebas a propósito: `log::configurar` solo se
//! puede llamar una vez por proceso, y cada fichero de `tests/` es un binario
//! aparte.
//!
//! Dos cosas se defienden aquí:
//!
//! 1. **Cada línea es un objeto JSON completo y válido**, incluso cuando un
//!    campo lleva comillas, barras invertidas o caracteres de control. Si no lo
//!    fuera, el recolector descartaría el evento justo cuando más falta hace.
//! 2. **Las claves y los valores de la base de datos no aparecen en el log.**
//!    Un registro con las claves del cliente sería una copia parcial de los
//!    datos, con otros permisos y otra retención.

mod common;

use bitacora::log::{self, Formato, Nivel};
use bitacora::{Db, FsyncPolicy, Options};
use common::TempDir;

/// Validador de un objeto JSON. Devuelve las claves de primer nivel, o el
/// motivo por el que la línea no es JSON.
///
/// Es a propósito estricto: rechaza caracteres de control crudos dentro de una
/// cadena, que es exactamente el fallo que un `format!` descuidado produce.
fn claves_json(s: &str) -> Result<Vec<String>, String> {
    let b: Vec<char> = s.chars().collect();
    let mut i = 0usize;
    let mut claves = Vec::new();
    let salta = |i: &mut usize| {
        while *i < b.len() && b[*i].is_whitespace() {
            *i += 1;
        }
    };
    let cadena = |i: &mut usize| -> Result<String, String> {
        if b.get(*i) != Some(&'"') {
            return Err(format!("se esperaba una comilla en {i}"));
        }
        *i += 1;
        let mut out = String::new();
        loop {
            match b.get(*i) {
                None => return Err("cadena sin cerrar".into()),
                Some('"') => {
                    *i += 1;
                    return Ok(out);
                }
                Some('\\') => {
                    *i += 1;
                    match b.get(*i) {
                        Some('"') => out.push('"'),
                        Some('\\') => out.push('\\'),
                        Some('/') => out.push('/'),
                        Some('b') => out.push('\u{8}'),
                        Some('f') => out.push('\u{c}'),
                        Some('n') => out.push('\n'),
                        Some('r') => out.push('\r'),
                        Some('t') => out.push('\t'),
                        Some('u') => {
                            let hex: String = b
                                .get(*i + 1..*i + 5)
                                .ok_or("\\u incompleto")?
                                .iter()
                                .collect();
                            let n = u32::from_str_radix(&hex, 16)
                                .map_err(|_| format!("\\u{hex} no es hexadecimal"))?;
                            out.push(char::from_u32(n).ok_or("\\u fuera de rango")?);
                            *i += 4;
                        }
                        otro => return Err(format!("escape desconocido: {otro:?}")),
                    }
                    *i += 1;
                }
                Some(c) if (*c as u32) < 0x20 => {
                    return Err(format!("carácter de control crudo U+{:04X}", *c as u32))
                }
                Some(c) => {
                    out.push(*c);
                    *i += 1;
                }
            }
        }
    };

    salta(&mut i);
    if b.get(i) != Some(&'{') {
        return Err("no empieza por {".into());
    }
    i += 1;
    salta(&mut i);
    if b.get(i) == Some(&'}') {
        return Ok(claves);
    }
    loop {
        salta(&mut i);
        claves.push(cadena(&mut i)?);
        salta(&mut i);
        if b.get(i) != Some(&':') {
            return Err(format!("falta ':' en {i}"));
        }
        i += 1;
        salta(&mut i);
        match b.get(i) {
            Some('"') => {
                cadena(&mut i)?;
            }
            Some(c) if *c == '-' || c.is_ascii_digit() => {
                let ini = i;
                while i < b.len()
                    && (b[i].is_ascii_digit() || matches!(b[i], '-' | '+' | '.' | 'e' | 'E'))
                {
                    i += 1;
                }
                let n: String = b[ini..i].iter().collect();
                n.parse::<f64>()
                    .map_err(|_| format!("{n} no es un número"))?;
            }
            _ => {
                let resto: String = b[i..].iter().collect();
                let lit = ["true", "false", "null"]
                    .iter()
                    .find(|l| resto.starts_with(**l))
                    .ok_or_else(|| format!("valor desconocido en {i}"))?;
                i += lit.len();
            }
        }
        salta(&mut i);
        match b.get(i) {
            Some(',') => i += 1,
            Some('}') => {
                i += 1;
                salta(&mut i);
                if i != b.len() {
                    return Err(format!("sobran {} caracteres tras el objeto", b.len() - i));
                }
                return Ok(claves);
            }
            otro => return Err(format!("se esperaba ',' o '}}', hay {otro:?}")),
        }
    }
}

#[test]
fn el_registro_json_es_json_valido_linea_a_linea() {
    let base = TempDir::new("registro");
    let fichero = base.path().join("eventos.jsonl");
    // Un directorio de datos con una comilla y una barra invertida en el
    // nombre. Los dos son legales en un sistema de ficheros y los dos rompen
    // el JSON si no se escapan; la ruta sale en el evento de apertura.
    let datos = base.path().join(r#"datos "raros" \ y tal"#);
    std::fs::create_dir_all(&datos).unwrap();

    log::configurar(Nivel::Info, Formato::Json, Some(&fichero)).expect("configurar el registro");

    const CLAVE: &[u8] = b"clave-secreta-xyz";
    const VALOR: &[u8] = b"valor-secreto-abc";
    {
        let mut o = Options::new(&datos);
        o.fsync = FsyncPolicy::Never;
        o.auto_compact = false;
        let db = Db::open(o).unwrap();
        db.put(CLAVE, VALOR).unwrap();
        for i in 0..50u32 {
            db.put(format!("k{i}").as_bytes(), b"relleno").unwrap();
        }
        db.compact().unwrap();
        db.backup(base.path().join("copia")).unwrap();
        db.close().unwrap();
    }

    let texto = std::fs::read_to_string(&fichero).expect("leer el registro");
    let lineas: Vec<&str> = texto.lines().collect();
    assert!(
        lineas.len() >= 3,
        "deberían haberse emitido al menos apertura, copia y cierre: {lineas:?}"
    );

    let mut eventos = Vec::new();
    for l in &lineas {
        let claves = claves_json(l).unwrap_or_else(|e| panic!("línea no es JSON ({e}): {l}"));
        // Las tres claves obligatorias, y en cabeza.
        assert_eq!(claves.first().map(String::as_str), Some("ts"), "{l}");
        assert_eq!(claves.get(1).map(String::as_str), Some("level"), "{l}");
        assert_eq!(claves.get(2).map(String::as_str), Some("event"), "{l}");
        // Ningún campo repetido: un objeto con dos "dir" es ambiguo.
        let mut ordenadas = claves.clone();
        ordenadas.sort();
        let antes = ordenadas.len();
        ordenadas.dedup();
        assert_eq!(antes, ordenadas.len(), "campo repetido en {l}");
        let ev = l
            .split(r#""event":""#)
            .nth(1)
            .and_then(|r| r.split('"').next())
            .unwrap()
            .to_string();
        eventos.push(ev);
    }

    assert!(
        eventos.contains(&"engine_opened".to_string()),
        "{eventos:?}"
    );
    assert!(
        eventos.contains(&"engine_closed".to_string()),
        "{eventos:?}"
    );
    assert!(eventos.contains(&"backup_done".to_string()), "{eventos:?}");

    // La ruta hostil ha salido escapada, no cruda.
    let apertura = lineas
        .iter()
        .find(|l| l.contains(r#""event":"engine_opened""#))
        .unwrap();
    assert!(
        apertura.contains(r#"datos \"raros\" \\ y tal"#),
        "la ruta tiene que ir escapada: {apertura}"
    );

    // Y lo más importante: los datos del cliente no están en el log.
    let secreto = String::from_utf8(CLAVE.to_vec()).unwrap();
    let secreto_v = String::from_utf8(VALOR.to_vec()).unwrap();
    assert!(
        !texto.contains(&secreto),
        "la clave del cliente ha aparecido en el registro"
    );
    assert!(
        !texto.contains(&secreto_v),
        "el valor del cliente ha aparecido en el registro"
    );

    // Configurar dos veces no se queda callado.
    assert!(log::configurar(Nivel::Debug, Formato::Texto, None).is_err());
    assert_eq!(log::descartadas(), 0, "no se ha perdido ninguna línea");
}
