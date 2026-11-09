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
//! SETEX clave segs val -> OK
//! TTL clave            -> INT <segundos> | INT -1 (no caduca) | NIL (no está)
//! GETSEQ clave         -> VALSEQ <seq> <valor> | NIL
//! CAS clave seq valor  -> OK | NO <seq_actual>   (seq 0 = no debe existir)
//! CASDEL clave seq     -> OK | NO <seq_actual>
//! USE [namespace]      -> OK <prefijo>           (solo para esta conexión)
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
    /// `SETEX clave <segundos> valor`: escribe con caducidad.
    SetEx {
        key: &'a [u8],
        segundos: u64,
        value: &'a [u8],
    },
    /// `TTL clave`: segundos que le quedan.
    Ttl(&'a [u8]),
    /// `GETSEQ clave`: el valor **y** su número de secuencia, en una sola
    /// respuesta. Pedirlos por separado sería una carrera: el `seq` podría ser
    /// más nuevo que el valor, y entonces un `CAS` con ese `seq` escribiría
    /// encima de un cambio ajeno.
    GetSeq(&'a [u8]),
    /// `CAS clave <seq> valor`: escribe solo si la clave sigue en ese número de
    /// secuencia. `seq = 0` significa «la clave no debe existir».
    Cas {
        key: &'a [u8],
        seq: u64,
        value: &'a [u8],
    },
    /// `CASDEL clave <seq>`: borra solo si la clave sigue en ese número.
    CasDel {
        key: &'a [u8],
        seq: u64,
    },
    /// `USE [namespace]`: pone un prefijo delante de las claves de **esta**
    /// conexión. Sin argumento, lo quita.
    Use(Option<&'a [u8]>),
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
        b"SETEX" => {
            let (key, resto) = split_word(rest);
            let (secs, value) = split_word(resto);
            if key.is_empty() || secs.is_empty() {
                return Err("uso: SETEX clave segundos valor");
            }
            let segundos = numero(secs).ok_or("SETEX: los segundos tienen que ser un número")?;
            if segundos == 0 {
                return Err("SETEX con 0 segundos no es una escritura, es un borrado: usa DEL");
            }
            Ok(Command::SetEx {
                key,
                segundos,
                value,
            })
        }
        b"TTL" => one(rest, "uso: TTL clave").map(Command::Ttl),
        b"GETSEQ" => one(rest, "uso: GETSEQ clave").map(Command::GetSeq),
        b"CAS" => {
            let (key, resto) = split_word(rest);
            let (s, value) = split_word(resto);
            if key.is_empty() || s.is_empty() {
                return Err("uso: CAS clave seq valor (seq 0 = la clave no debe existir)");
            }
            let seq = numero(s).ok_or("CAS: el seq tiene que ser un número")?;
            Ok(Command::Cas { key, seq, value })
        }
        b"CASDEL" => {
            let (key, resto) = split_word(rest);
            let (s, more) = split_word(resto);
            if key.is_empty() || s.is_empty() || !more.is_empty() {
                return Err("uso: CASDEL clave seq");
            }
            let seq = numero(s).ok_or("CASDEL: el seq tiene que ser un número")?;
            Ok(Command::CasDel { key, seq })
        }
        b"USE" => {
            let (ns, more) = split_word(rest);
            if !more.is_empty() {
                return Err("uso: USE [namespace] (un nombre, sin espacios)");
            }
            Ok(Command::Use(if ns.is_empty() { None } else { Some(ns) }))
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
            "comando desconocido (AUTH USE SET SETEX GET GETSEQ CAS CASDEL DEL EXISTS TTL \
             KEYS STATS HEALTH METRICS BACKUP COMPACT PING QUIT)",
        ),
    }
}

fn numero(s: &[u8]) -> Option<u64> {
    std::str::from_utf8(s).ok()?.parse().ok()
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
        assert_eq!(
            parse(b"SETEX s 60 un valor con espacios").unwrap(),
            Command::SetEx {
                key: b"s",
                segundos: 60,
                value: b"un valor con espacios"
            }
        );
        assert!(parse(b"SETEX s 0 v").is_err(), "0 segundos es un borrado");
        assert!(parse(b"SETEX s diez v").is_err());
        assert!(parse(b"SETEX s").is_err());
        assert_eq!(parse(b"TTL k").unwrap(), Command::Ttl(b"k"));
        assert_eq!(parse(b"GETSEQ k").unwrap(), Command::GetSeq(b"k"));
        assert_eq!(
            parse(b"CAS k 7 nuevo valor").unwrap(),
            Command::Cas {
                key: b"k",
                seq: 7,
                value: b"nuevo valor"
            }
        );
        assert_eq!(
            parse(b"CAS k 0 ").unwrap(),
            Command::Cas {
                key: b"k",
                seq: 0,
                value: b""
            },
            "seq 0 = la clave no debe existir"
        );
        assert!(parse(b"CAS k noesnumero v").is_err());
        assert_eq!(
            parse(b"CASDEL k 9").unwrap(),
            Command::CasDel { key: b"k", seq: 9 }
        );
        assert!(parse(b"CASDEL k 9 sobra").is_err());
        assert_eq!(parse(b"USE app").unwrap(), Command::Use(Some(b"app")));
        assert_eq!(parse(b"USE").unwrap(), Command::Use(None));
        assert!(parse(b"USE a b").is_err());
    }
}
