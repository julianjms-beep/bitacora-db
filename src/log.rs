//! Registro de eventos, estructurado y sin dependencias externas.
//!
//! El titular del proyecto es «cero dependencias», así que no hay `tracing` ni
//! `log`: esto emite las líneas a mano. Son unas 200 líneas y hacen lo único
//! que de verdad hace falta para operar el motor: una línea por evento, con
//! campos nombrados, en un formato que se pueda leer con los ojos (`texto`) o
//! con una máquina (`json`).
//!
//! # Decisiones que conviene conocer
//!
//! * **Nunca se registran claves ni valores.** Un registro que incluyera las
//!   claves del cliente convertiría el fichero de log en una copia parcial de
//!   los datos, con otros permisos y otra retención. Se registran tamaños,
//!   contadores y motivos; nunca el contenido.
//! * **Los nombres que salen son en inglés** (`warn`, `engine_opened`,
//!   `fsync_failed`), igual que `STATS` y las métricas. Todo lo que cruza la
//!   frontera hacia una máquina usa los mismos nombres; el código y la
//!   documentación siguen en español.
//! * **Una línea nunca hace fallar una operación.** Si la escritura del log
//!   falla, se descarta la línea y se cuenta en `descartadas()`, que sale en
//!   las métricas. Perder observabilidad es malo; parar el motor porque el log
//!   no se puede escribir es peor.
//! * **Se configura una vez.** Cambiar el destino en marcha exigiría un candado
//!   en el camino de cada línea; el registro no debe ser un punto de
//!   contención. Sin configurar, el destino es `stderr` en formato texto y
//!   nivel `info`: lo mismo que hacía el motor antes de que esto existiera.

use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Nivel {
    Debug,
    Info,
    Aviso,
    Error,
}

impl Nivel {
    pub fn parse(s: &str) -> Option<Nivel> {
        match s.to_ascii_lowercase().as_str() {
            "debug" => Some(Nivel::Debug),
            "info" => Some(Nivel::Info),
            "aviso" | "warn" => Some(Nivel::Aviso),
            "error" => Some(Nivel::Error),
            _ => None,
        }
    }

    /// Nombre que sale en la línea. `warn` y no `aviso`: es la palabra que
    /// esperan los recolectores de logs, y esto se emite para máquinas.
    pub fn name(self) -> &'static str {
        match self {
            Nivel::Debug => "debug",
            Nivel::Info => "info",
            Nivel::Aviso => "warn",
            Nivel::Error => "error",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Formato {
    /// Una línea legible: `<marca> <nivel> <evento> campo=valor ...`
    Texto,
    /// Un objeto JSON por línea (JSON Lines), para `jq`, Loki o similares.
    Json,
}

impl Formato {
    pub fn parse(s: &str) -> Option<Formato> {
        match s.to_ascii_lowercase().as_str() {
            "texto" | "text" => Some(Formato::Texto),
            "json" => Some(Formato::Json),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Formato::Texto => "texto",
            Formato::Json => "json",
        }
    }
}

/// Valor de un campo. Los números salen sin comillas para que un consumidor
/// pueda sumarlos sin convertirlos.
pub enum Valor<'a> {
    Txt(&'a str),
    Cadena(String),
    Num(u64),
    Ent(i64),
    Dec(f64),
    Bool(bool),
}

impl<'a> From<&'a str> for Valor<'a> {
    fn from(v: &'a str) -> Self {
        Valor::Txt(v)
    }
}
impl<'a> From<&'a String> for Valor<'a> {
    fn from(v: &'a String) -> Self {
        Valor::Txt(v.as_str())
    }
}
impl From<String> for Valor<'_> {
    fn from(v: String) -> Self {
        Valor::Cadena(v)
    }
}
impl From<u64> for Valor<'_> {
    fn from(v: u64) -> Self {
        Valor::Num(v)
    }
}
impl From<u32> for Valor<'_> {
    fn from(v: u32) -> Self {
        Valor::Num(v as u64)
    }
}
impl From<u16> for Valor<'_> {
    fn from(v: u16) -> Self {
        Valor::Num(v as u64)
    }
}
impl From<usize> for Valor<'_> {
    fn from(v: usize) -> Self {
        Valor::Num(v as u64)
    }
}
impl From<i64> for Valor<'_> {
    fn from(v: i64) -> Self {
        Valor::Ent(v)
    }
}
impl From<f64> for Valor<'_> {
    fn from(v: f64) -> Self {
        Valor::Dec(v)
    }
}
impl From<bool> for Valor<'_> {
    fn from(v: bool) -> Self {
        Valor::Bool(v)
    }
}

/// Atajo para una ruta: `&Path` no se puede guardar como `&str` porque en
/// general no es UTF-8 válido, así que se convierte con pérdida.
pub fn ruta(p: &Path) -> Valor<'static> {
    Valor::Cadena(p.to_string_lossy().into_owned())
}

fn escribe_cadena_json(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            // El resto de caracteres de control hay que escaparlos: un 0x01
            // crudo dentro de una cadena no es JSON válido.
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

fn escribe_valor_json(out: &mut String, v: &Valor<'_>) {
    match v {
        Valor::Txt(s) => escribe_cadena_json(out, s),
        Valor::Cadena(s) => escribe_cadena_json(out, s),
        Valor::Num(n) => {
            let _ = write!(out, "{n}");
        }
        Valor::Ent(n) => {
            let _ = write!(out, "{n}");
        }
        // `NaN` e `Infinity` no existen en JSON. Emitirlos haría ilegible la
        // línea entera para cualquier consumidor estricto.
        Valor::Dec(x) => {
            if x.is_finite() {
                let _ = write!(out, "{x}");
            } else {
                out.push_str("null");
            }
        }
        Valor::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
    }
}

/// Construye la línea JSON. Función pura: es la que se prueba.
pub fn linea_json(ts: &str, nivel: Nivel, evento: &str, campos: &[(&str, Valor<'_>)]) -> String {
    let mut s = String::with_capacity(128);
    s.push_str("{\"ts\":");
    escribe_cadena_json(&mut s, ts);
    s.push_str(",\"level\":");
    escribe_cadena_json(&mut s, nivel.name());
    s.push_str(",\"event\":");
    escribe_cadena_json(&mut s, evento);
    for (k, v) in campos {
        s.push(',');
        escribe_cadena_json(&mut s, k);
        s.push(':');
        escribe_valor_json(&mut s, v);
    }
    s.push('}');
    s
}

/// Construye la línea de texto. Se entrecomilla solo lo que lo necesita, como
/// hace logfmt, para que `grep` siga siendo útil.
pub fn linea_texto(ts: &str, nivel: Nivel, evento: &str, campos: &[(&str, Valor<'_>)]) -> String {
    let mut s = String::with_capacity(128);
    let _ = write!(s, "{ts} {:<5} {evento}", nivel.name());
    for (k, v) in campos {
        s.push(' ');
        s.push_str(k);
        s.push('=');
        match v {
            Valor::Txt(t) => escribe_texto_valor(&mut s, t),
            Valor::Cadena(t) => escribe_texto_valor(&mut s, t),
            otro => escribe_valor_json(&mut s, otro),
        }
    }
    s
}

fn escribe_texto_valor(out: &mut String, t: &str) {
    let necesita = t.is_empty()
        || t.chars()
            .any(|c| c == ' ' || c == '"' || c == '=' || (c as u32) < 0x20);
    if necesita {
        escribe_cadena_json(out, t);
    } else {
        out.push_str(t);
    }
}

// --- Marca de tiempo ------------------------------------------------------
//
// Sin dependencias no hay `chrono`, así que la conversión de segundos desde la
// época a fecha civil se hace aquí. Es el algoritmo de Howard Hinnant
// (`civil_from_days`), exacto para todo el rango del calendario proléptico
// gregoriano. Las pruebas lo comprueban contra fechas conocidas, incluido un
// 29 de febrero.

fn civil_desde_dias(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (y + (m <= 2) as i64, m, d)
}

/// `2026-11-09T18:03:11.482Z`. Ordenable como texto, que es justo lo que se
/// quiere de una marca de tiempo en un log.
pub fn rfc3339_desde_epoch(secs: u64, ms: u32) -> String {
    let dias = (secs / 86_400) as i64;
    let resto = secs % 86_400;
    let (y, m, d) = civil_desde_dias(dias);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{ms:03}Z",
        resto / 3600,
        (resto % 3600) / 60,
        resto % 60
    )
}

pub fn marca(t: SystemTime) -> String {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => rfc3339_desde_epoch(d.as_secs(), d.subsec_millis()),
        // Reloj antes de 1970: no se inventa una fecha, se dice.
        Err(_) => "1970-01-01T00:00:00.000Z".to_string(),
    }
}

// --- Destino --------------------------------------------------------------

enum Destino {
    Stderr,
    Fichero(Mutex<File>),
}

struct Salida {
    nivel: Nivel,
    formato: Formato,
    destino: Destino,
    descartadas: AtomicU64,
}

static SALIDA: OnceLock<Salida> = OnceLock::new();

fn salida() -> &'static Salida {
    SALIDA.get_or_init(|| Salida {
        nivel: Nivel::Info,
        formato: Formato::Texto,
        destino: Destino::Stderr,
        descartadas: AtomicU64::new(0),
    })
}

/// Configura el registro. Solo se puede llamar una vez y antes de emitir nada;
/// después devuelve error y no cambia nada.
pub fn configurar(nivel: Nivel, formato: Formato, fichero: Option<&Path>) -> std::io::Result<()> {
    let destino = match fichero {
        None => Destino::Stderr,
        Some(p) => Destino::Fichero(Mutex::new(
            OpenOptions::new().create(true).append(true).open(p)?,
        )),
    };
    SALIDA
        .set(Salida {
            nivel,
            formato,
            destino,
            descartadas: AtomicU64::new(0),
        })
        .map_err(|_| std::io::Error::other("el registro ya estaba configurado"))
}

/// Cuántas líneas se han perdido porque no se pudieron escribir. Sale en las
/// métricas: si no es 0, lo que se está leyendo del log está incompleto.
pub fn descartadas() -> u64 {
    salida().descartadas.load(Ordering::Relaxed)
}

pub fn nivel_actual() -> Nivel {
    salida().nivel
}

pub fn evento(nivel: Nivel, evento: &str, campos: &[(&str, Valor<'_>)]) {
    let s = salida();
    if nivel < s.nivel {
        return;
    }
    let ts = marca(SystemTime::now());
    let mut linea = match s.formato {
        Formato::Json => linea_json(&ts, nivel, evento, campos),
        Formato::Texto => linea_texto(&ts, nivel, evento, campos),
    };
    linea.push('\n');
    // Una escritura por línea: `Stderr` lleva su propio candado y el fichero se
    // abre en modo `append` bajo mutex, así que dos hilos no se entremezclan.
    let r = match &s.destino {
        Destino::Stderr => std::io::stderr().write_all(linea.as_bytes()),
        Destino::Fichero(m) => {
            let mut f = m.lock().unwrap_or_else(|e| e.into_inner());
            f.write_all(linea.as_bytes())
        }
    };
    if r.is_err() {
        s.descartadas.fetch_add(1, Ordering::Relaxed);
    }
}

pub fn debug(ev: &str, campos: &[(&str, Valor<'_>)]) {
    evento(Nivel::Debug, ev, campos);
}
pub fn info(ev: &str, campos: &[(&str, Valor<'_>)]) {
    evento(Nivel::Info, ev, campos);
}
pub fn aviso(ev: &str, campos: &[(&str, Valor<'_>)]) {
    evento(Nivel::Aviso, ev, campos);
}
pub fn error(ev: &str, campos: &[(&str, Valor<'_>)]) {
    evento(Nivel::Error, ev, campos);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn las_fechas_salen_bien() {
        assert_eq!(rfc3339_desde_epoch(0, 0), "1970-01-01T00:00:00.000Z");
        // Un 29 de febrero: si la conversión de días a fecha estuviera mal,
        // aquí saldría 1 de marzo.
        assert_eq!(
            rfc3339_desde_epoch(951_782_400, 0),
            "2000-02-29T00:00:00.000Z"
        );
        assert_eq!(
            rfc3339_desde_epoch(946_684_799, 999),
            "1999-12-31T23:59:59.999Z"
        );
        assert_eq!(
            rfc3339_desde_epoch(1_794_182_400, 7),
            "2026-11-09T00:00:00.007Z"
        );
        assert_eq!(
            rfc3339_desde_epoch(1_794_182_400 + 3661, 482),
            "2026-11-09T01:01:01.482Z"
        );
    }

    /// Un mensaje de error del sistema puede traer comillas, saltos de línea o
    /// caracteres de control. Si no se escapan, la línea deja de ser JSON y el
    /// recolector descarta el evento justo cuando más falta hacía.
    #[test]
    fn se_escapa_lo_que_rompe_el_json() {
        let l = linea_json(
            "T",
            Nivel::Error,
            "fsync_failed",
            &[("error", "dice \"no\" y salta\naqui".into())],
        );
        assert_eq!(
            l,
            r#"{"ts":"T","level":"error","event":"fsync_failed","error":"dice \"no\" y salta\naqui"}"#
        );

        let l = linea_json("T", Nivel::Info, "x", &[("c", "a\u{1}b\\c".into())]);
        assert!(l.contains(r#""c":"a\u0001b\\c""#), "{l}");
    }

    #[test]
    fn los_numeros_no_van_entrecomillados() {
        let l = linea_json(
            "T",
            Nivel::Info,
            "engine_opened",
            &[("keys", 200_000u64.into()), ("degraded", false.into())],
        );
        assert!(l.contains(r#""keys":200000"#), "{l}");
        assert!(l.contains(r#""degraded":false"#), "{l}");
    }

    /// `NaN` no es JSON. Si se emitiera crudo, la línea entera sería ilegible.
    #[test]
    fn nan_e_infinito_salen_como_null() {
        let l = linea_json(
            "T",
            Nivel::Info,
            "x",
            &[("a", f64::NAN.into()), ("b", f64::INFINITY.into())],
        );
        assert!(l.contains(r#""a":null"#), "{l}");
        assert!(l.contains(r#""b":null"#), "{l}");
        let l = linea_json("T", Nivel::Info, "x", &[("a", 0.5f64.into())]);
        assert!(l.contains(r#""a":0.5"#), "{l}");
    }

    #[test]
    fn el_texto_entrecomilla_solo_lo_necesario() {
        let l = linea_texto(
            "T",
            Nivel::Aviso,
            "conn_rejected",
            &[
                ("reason", "limit".into()),
                ("detail", "no hay hilos".into()),
            ],
        );
        assert_eq!(
            l,
            r#"T warn  conn_rejected reason=limit detail="no hay hilos""#
        );
    }

    #[test]
    fn los_niveles_se_ordenan_y_se_leen() {
        assert!(Nivel::Debug < Nivel::Info && Nivel::Info < Nivel::Aviso);
        assert!(Nivel::Aviso < Nivel::Error);
        assert_eq!(Nivel::parse("WARN"), Some(Nivel::Aviso));
        assert_eq!(Nivel::parse("aviso"), Some(Nivel::Aviso));
        assert_eq!(Nivel::parse("ruido"), None);
        assert_eq!(Formato::parse("JSON"), Some(Formato::Json));
        assert_eq!(Formato::parse("yaml"), None);
    }
}
