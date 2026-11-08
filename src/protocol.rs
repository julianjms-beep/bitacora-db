//! Protocolo de texto, una orden por línea (funciona con `nc`):
//!
//! ```text
//! SET clave valor      -> OK
//! GET clave            -> VAL <valor> | NIL
//! DEL clave            -> OK | NIL
//! EXISTS clave         -> INT 1 | INT 0
//! KEYS [prefijo]       -> KEYS <n>  seguido de n líneas
//! STATS                -> STATS <n> seguido de n líneas "nombre valor"
//! COMPACT              -> OK ...
//! PING                 -> PONG
//! QUIT                 -> BYE
//! ```
//! Las claves no llevan espacios; el valor es el resto de la línea.

#[derive(Debug, PartialEq, Eq)]
pub enum Command<'a> {
    Set { key: &'a [u8], value: &'a [u8] },
    Get(&'a [u8]),
    Del(&'a [u8]),
    Exists(&'a [u8]),
    Keys(&'a [u8]),
    Stats,
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
        b"KEYS" => Ok(Command::Keys(split_word(rest).0)),
        b"STATS" | b"INFO" => Ok(Command::Stats),
        b"COMPACT" => Ok(Command::Compact),
        b"PING" => Ok(Command::Ping),
        b"QUIT" | b"EXIT" => Ok(Command::Quit),
        b"" => Err("línea vacía"),
        _ => Err("comando desconocido (SET GET DEL EXISTS KEYS STATS COMPACT PING QUIT)"),
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
            Command::Set { key: b"user:1", value: b"Ana Garcia" }
        );
    }

    #[test]
    fn get_y_errores() {
        assert_eq!(parse(b"GET a").unwrap(), Command::Get(b"a"));
        assert!(parse(b"GET").is_err());
        assert!(parse(b"GET a b").is_err());
        assert!(parse(b"HOLA").is_err());
        assert_eq!(parse(b"KEYS").unwrap(), Command::Keys(b""));
        assert_eq!(parse(b"KEYS us").unwrap(), Command::Keys(b"us"));
    }
}
