//! Protocolo de texto, una orden por línea (funciona con `nc`):
//!
//! ```text
//! AUTH clave           -> OK | ERR
//! SET clave valor      -> OK
//! GET clave            -> VAL <valor> | NIL
//! DEL clave            -> OK | NIL
//! EXISTS clave         -> INT 1 | INT 0
//! KEYS [prefijo]       -> KEYS <n>  seguido de n líneas
//! STATS                -> STATS <n> seguido de n líneas "nombre valor"
//! HEALTH               -> HEALTH <ok|aviso|critico> <n> seguido de n motivos
//! METRICS              -> METRICS <n> seguido de n líneas (Prometheus)
//! BACKUP [nombre]      -> OK <ruta> segmentos=.. bytes=.. | ERR
//! COMPACT              -> OK ...
//! PING                 -> PONG
//! QUIT                 -> BYE
//! ```
//! Las claves no llevan espacios; el valor es el resto de la línea.

#[derive(Debug, PartialEq, Eq)]
pub enum Command<'a> {
    /// Autenticación por secreto compartido. Solo hace falta si el servidor se
    /// arrancó con una clave.
    Auth(&'a [u8]),
    Set {
        key: &'a [u8],
        value: &'a [u8],
    },
    Get(&'a [u8]),
    Del(&'a [u8]),
    Exists(&'a [u8]),
    /// `KEYS [prefijo] [limite]`. Sin límite explícito, el servidor aplica el
    /// suyo: A3, antes no había ninguno.
    Keys {
        prefix: &'a [u8],
        limite: Option<usize>,
    },
    Stats,
    /// Semáforo de operación: `ok`, `aviso` o `critico`, con los motivos.
    Health,
    /// Métricas en el formato de exposición de Prometheus.
    Metrics,
    /// `BACKUP [nombre]`: copia en caliente dentro del directorio de copias
    /// que se configuró al arrancar. El nombre es **un solo componente de
    /// ruta**, nunca una ruta: el cliente no elige dónde escribe el servidor.
    Backup(Option<&'a [u8]>),
    Compact,
    Ping,
    Quit,
}

fn trim(mut s: &[u8]) -> &[u8] {
    while let [b' ' | b'\t' | b'\r' | b'\n', rest @ ..] = s {
        s = rest;
    }
    while let [rest @ .., b' ' | b'\t' | b'\r' | b'\n'] = s {
        s = rest;
    }
    s
}

/// Separa la primera palabra del resto (sin los espacios que los separan).
fn split_word(s: &[u8]) -> (&[u8], &[u8]) {
    match s.iter().position(|&b| b == b' ' || b == b'\t') {
        Some(i) => {
            let mut rest = &s[i..];
            while let [b' ' | b'\t', r @ ..] = rest {
                rest = r;
            }
            (&s[..i], rest)
        }
        None => (s, &[]),
    }
}

pub fn parse(line: &[u8]) -> Result<Command<'_>, &'static str> {
    let line = trim(line);
    let (cmd, rest) = split_word(line);
    let upper = cmd.to_ascii_uppercase();
    match upper.as_slice() {
        b"SET" => {
            let (key, value) = split_word(rest);
            if key.is_empty() {
                return Err("uso: SET clave valor");
            }
            Ok(Command::Set { key, value })
        }
        b"GET" => one(rest, "uso: GET clave").map(Command::Get),
        b"DEL" | b"DELETE" => one(rest, "uso: DEL clave").map(Command::Del),
        b"EXISTS" => one(rest, "uso: EXISTS clave").map(Command::Exists),
        b"KEYS" => {
            let (prefix, resto) = split_word(rest);
            let (lim, _) = split_word(resto);
            let limite = if lim.is_empty() {
                None
            } else {
                match std::str::from_utf8(lim)
                    .ok()
                    .and_then(|s| s.parse::<usize>().ok())
                {
                    Some(n) => Some(n),
                    None => return Err("uso: KEYS [prefijo] [limite]"),
                }
            };
            Ok(Command::Keys { prefix, limite })
        }
        b"STATS" | b"INFO" => Ok(Command::Stats),
        b"HEALTH" => Ok(Command::Health),
        b"METRICS" => Ok(Command::Metrics),
        b"BACKUP" => {
            let (nombre, more) = split_word(rest);
            if !more.is_empty() {
                return Err("uso: BACKUP [nombre] (un nombre, sin espacios ni rutas)");
            }
            Ok(Command::Backup(if nombre.is_empty() {
                None
            } else {
                Some(nombre)
            }))
        }
        b"COMPACT" => Ok(Command::Compact),
        b"AUTH" => {
            let (clave, _) = split_word(rest);
            if clave.is_empty() {
                Err("uso: AUTH clave")
            } else {
                Ok(Command::Auth(clave))
            }
        }
        b"PING" => Ok(Command::Ping),
        b"QUIT" | b"EXIT" => Ok(Command::Quit),
        b"" => Err("línea vacía"),
        _ => Err(
            "comando desconocido (AUTH SET GET DEL EXISTS KEYS STATS HEALTH METRICS COMPACT PING QUIT)",
        ),
    }
}

fn one<'a>(rest: &'a [u8], usage: &'static str) -> Result<&'a [u8], &'static str> {
    let (key, more) = split_word(rest);
    if key.is_empty() || !more.is_empty() {
        return Err(usage);
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_con_espacios_en_el_valor() {
        assert_eq!(
            parse(b"set user:1 Ana Garcia\r\n").unwrap(),
            Command::Set {
                key: b"user:1",
                value: b"Ana Garcia"
            }
        );
    }

    #[test]
    fn get_y_errores() {
        assert_eq!(parse(b"GET a").unwrap(), Command::Get(b"a"));
        assert!(parse(b"GET").is_err());
        assert!(parse(b"GET a b").is_err());
        assert!(parse(b"HOLA").is_err());
        assert_eq!(
            parse(b"KEYS").unwrap(),
            Command::Keys {
                prefix: b"",
                limite: None
            }
        );
        assert_eq!(
            parse(b"KEYS us").unwrap(),
            Command::Keys {
                prefix: b"us",
                limite: None
            }
        );
        assert_eq!(
            parse(b"KEYS us 10").unwrap(),
            Command::Keys {
                prefix: b"us",
                limite: Some(10)
            }
        );
        assert!(
            parse(b"KEYS us diez").is_err(),
            "un límite no numérico es un error"
        );
        assert_eq!(parse(b"AUTH secreto").unwrap(), Command::Auth(b"secreto"));
        assert!(parse(b"AUTH").is_err(), "AUTH sin clave es un error");
        assert_eq!(parse(b"health\n").unwrap(), Command::Health);
        assert_eq!(parse(b"METRICS").unwrap(), Command::Metrics);
        assert_eq!(parse(b"BACKUP").unwrap(), Command::Backup(None));
        assert_eq!(
            parse(b"BACKUP noche").unwrap(),
            Command::Backup(Some(b"noche"))
        );
        assert!(
            parse(b"BACKUP a b").is_err(),
            "un nombre con espacios no se acepta: no es una ruta"
        );
    }
}
