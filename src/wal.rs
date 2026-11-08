//! Formato de los registros del WAL (Write-Ahead Log) y lector de segmentos.
//!
//! Cada operación (PUT o DEL) se escribe al final de un fichero como un
//! registro binario:
//!
//! ```text
//! ┌────────┬────────┬─────────┬─────────┬──────┬───────┬───────┐
//! │ crc32  │ seq    │ key_len │ val_len │ kind │ key   │ value │
//! │ u32 LE │ u64 LE │ u32 LE  │ u32 LE  │ u8   │ bytes │ bytes │
//! └────────┴────────┴─────────┴─────────┴──────┴───────┴───────┘
//! ```
//!
//! - El CRC cubre todo lo que va detrás de él.
//! - `seq` es un número de secuencia global y creciente. Gracias a él el
//!   estado se reconstruye sin depender del orden de los ficheros, lo que
//!   permite que la compactación escriba en ficheros nuevos sin romper nada.
//!
//! # Cola incompleta frente a corrupción
//!
//! Un registro que no pasa las comprobaciones puede significar dos cosas muy
//! distintas, y confundirlas cuesta datos:
//!
//! - **Cola incompleta** ([`ReadOutcome::IncompleteTail`]): el fichero termina
//!   en un registro a medio escribir y **no hay ningún registro válido
//!   detrás**. Es lo que deja un apagón, y recortar el fichero ahí no pierde
//!   nada que estuviera confirmado.
//! - **Corrupción** ([`ReadOutcome::Corrupt`]): un registro no valida, pero
//!   **sí hay registros válidos detrás**. Recortar aquí destruiría datos
//!   confirmados, así que el lector resincroniza y sigue leyendo.
//!
//! El lector distingue los dos casos buscando el siguiente registro válido, y
//! lo hace en dos pasos:
//!
//! 1. **Confiando en la cabecera.** Si la corrupción cayó en la clave o en el
//!    valor, la cabecera sigue siendo buena, así que el registro siguiente
//!    empieza en `offset + HEADER + key_len + val_len`. Se comprueba primero
//!    porque es el caso más frecuente (la carga ocupa mucho más que los 21
//!    bytes de cabecera) y porque es exacto.
//! 2. **Rastreando byte a byte.** Si la cabecera misma está corrupta, las
//!    longitudes no sirven y hay que buscar. Se prueba cada desplazamiento y
//!    solo se acepta donde el CRC cuadre, lo que hace un falso positivo del
//!    orden de 2⁻³² por candidato; además se exige que el candidato termine
//!    justo al final del fichero o que otro registro válido lo siga, para
//!    bajar aún más esa probabilidad.
//!
//! Lo que el formato **no** permite recuperar está documentado en
//! `docs/RECUPERACION.md`.

use crate::crc32::Hasher;
use std::io::{self, BufRead, Read, Seek, SeekFrom};

/// Bytes de cabecera fija: crc(4) + seq(8) + key_len(4) + val_len(4) + kind(1).
pub const HEADER: usize = 4 + 8 + 4 + 4 + 1;
pub const MAX_KEY: usize = 64 * 1024;
pub const MAX_VALUE: usize = 1 << 30;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Put = 0,
    Del = 1,
}

/// Añade un registro codificado al final de `out`.
pub fn encode(out: &mut Vec<u8>, seq: u64, kind: Kind, key: &[u8], value: &[u8]) {
    let base = out.len();
    out.reserve(HEADER + key.len() + value.len());
    out.extend_from_slice(&[0, 0, 0, 0]); // hueco para el crc
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.push(kind as u8);
    out.extend_from_slice(key);
    out.extend_from_slice(value);
    let mut h = Hasher::new();
    h.update(&out[base + 4..]);
    out[base..base + 4].copy_from_slice(&h.finish().to_le_bytes());
}

#[derive(Debug)]
pub struct ReadRecord {
    pub seq: u64,
    pub kind: Kind,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    /// Posición del registro dentro del fichero.
    pub offset: u64,
    /// Posición absoluta del valor dentro del fichero.
    pub vpos: u64,
}

/// Por qué no se pudo leer un registro. Va en el informe de recuperación para
/// que el operador sepa qué le pasó a sus datos, no solo que algo pasó.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    /// La cabecera declara longitudes imposibles o un `kind` desconocido.
    CabeceraInvalida,
    /// La cabecera es plausible pero el fichero se acaba antes del registro.
    Truncado,
    /// Todo encaja pero el CRC no coincide: hay bytes cambiados.
    CrcIncorrecto,
}

impl Cause {
    pub fn name(self) -> &'static str {
        match self {
            Cause::CabeceraInvalida => "cabecera inválida",
            Cause::Truncado => "registro truncado",
            Cause::CrcIncorrecto => "crc incorrecto",
        }
    }
}

pub enum ReadOutcome {
    Record(ReadRecord),
    /// Fin limpio del fichero.
    End,
    /// El fichero acaba en un registro a medio escribir y no hay nada válido
    /// detrás: escritura interrumpida por un apagón. Recortar en `at` es
    /// seguro.
    IncompleteTail { at: u64, cause: Cause },
    /// Un registro no valida pero hay registros válidos detrás. **Recortar
    /// aquí destruiría datos confirmados.** El lector continúa en `resync_at`.
    Corrupt {
        /// Dónde empieza la zona ilegible.
        at: u64,
        /// Dónde se ha encontrado el siguiente registro válido.
        resync_at: u64,
        cause: Cause,
        /// Si se recuperó el sincronismo confiando en las longitudes de la
        /// cabecera (`true`) o rastreando byte a byte (`false`).
        por_cabecera: bool,
    },
}

/// Cabecera ya descodificada, sin validar contra el CRC.
struct Head {
    crc: u32,
    seq: u64,
    klen: usize,
    vlen: usize,
    kind: Kind,
}

fn decode_head(h: &[u8; HEADER]) -> Option<Head> {
    let kind = match h[20] {
        0 => Kind::Put,
        1 => Kind::Del,
        _ => return None,
    };
    Some(Head {
        crc: u32::from_le_bytes(h[0..4].try_into().unwrap()),
        seq: u64::from_le_bytes(h[4..12].try_into().unwrap()),
        klen: u32::from_le_bytes(h[12..16].try_into().unwrap()) as usize,
        vlen: u32::from_le_bytes(h[16..20].try_into().unwrap()) as usize,
        kind,
    })
}

/// Resultado de intentar leer un registro en una posición concreta.
enum Parsed {
    Ok(ReadRecord),
    Bad(Cause),
}

/// Lee registros secuencialmente de un segmento, distingue la cola incompleta
/// de la corrupción y resincroniza tras un registro ilegible.
///
/// Necesita `Seek` porque resincronizar exige volver a leer desde posiciones
/// arbitrarias. El camino normal (sin corrupción) es secuencial y no hace
/// ningún `seek`.
pub struct SegmentReader<R: BufRead + Seek> {
    r: R,
    offset: u64,
    file_len: u64,
    /// `true` cuando la posición del lector ya no es `offset` y hay que
    /// recolocarla antes de la siguiente lectura secuencial.
    needs_seek: bool,
}

impl<R: BufRead + Seek> SegmentReader<R> {
    pub fn new(r: R, file_len: u64) -> Self {
        SegmentReader { r, offset: 0, file_len, needs_seek: false }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Intenta leer un registro en `off`, dejando el lector colocado justo
    /// detrás de lo que haya leído. No toca `self.offset`.
    fn parse_at(&mut self, off: u64, seek: bool) -> io::Result<Parsed> {
        if seek {
            self.r.seek(SeekFrom::Start(off))?;
        }
        if self.file_len - off < HEADER as u64 {
            return Ok(Parsed::Bad(Cause::Truncado));
        }
        let mut h = [0u8; HEADER];
        if !read_full(&mut self.r, &mut h)? {
            return Ok(Parsed::Bad(Cause::Truncado));
        }
        let head = match decode_head(&h) {
            Some(x) => x,
            None => return Ok(Parsed::Bad(Cause::CabeceraInvalida)),
        };
        if head.klen == 0 || head.klen > MAX_KEY || head.vlen > MAX_VALUE {
            return Ok(Parsed::Bad(Cause::CabeceraInvalida));
        }
        let total = HEADER as u64 + head.klen as u64 + head.vlen as u64;
        if off + total > self.file_len {
            return Ok(Parsed::Bad(Cause::Truncado));
        }
        let mut key = vec![0u8; head.klen];
        if !read_full(&mut self.r, &mut key)? {
            return Ok(Parsed::Bad(Cause::Truncado));
        }
        let mut value = vec![0u8; head.vlen];
        if !read_full(&mut self.r, &mut value)? {
            return Ok(Parsed::Bad(Cause::Truncado));
        }
        let mut hs = Hasher::new();
        hs.update(&h[4..]);
        hs.update(&key);
        hs.update(&value);
        if hs.finish() != head.crc {
            return Ok(Parsed::Bad(Cause::CrcIncorrecto));
        }
        Ok(Parsed::Ok(ReadRecord {
            seq: head.seq,
            kind: head.kind,
            key,
            value,
            offset: off,
            vpos: off + HEADER as u64 + head.klen as u64,
        }))
    }

    /// ¿Hay un registro con el CRC correcto empezando exactamente en `off`?
    /// Devuelve dónde termina.
    fn crc_valido_en(&mut self, off: u64) -> io::Result<Option<u64>> {
        match self.parse_at(off, true)? {
            Parsed::Ok(r) => {
                Ok(Some(off + HEADER as u64 + r.key.len() as u64 + r.value.len() as u64))
            }
            Parsed::Bad(_) => Ok(None),
        }
    }

    /// Busca el siguiente registro válido a partir de un registro ilegible que
    /// empieza en `at`. Devuelve `(offset, por_cabecera)`, o `None` si no queda
    /// nada legible, que es lo que distingue la corrupción de la cola
    /// incompleta.
    fn resincroniza(&mut self, at: u64, head_hint: Option<u64>) -> io::Result<Option<(u64, bool)>> {
        // Paso 1: confiar en las longitudes de la cabecera. Aquí no se adivina
        // nada —el desplazamiento sale de una cabecera intacta—, así que basta
        // con que el CRC del registro siguiente cuadre.
        if let Some(cand) = head_hint {
            if cand > at && cand < self.file_len && self.crc_valido_en(cand)?.is_some() {
                return Ok(Some((cand, true)));
            }
        }
        // Paso 2: rastreo byte a byte. Aquí sí se adivina, así que se prefiere
        // un candidato "fuerte": CRC correcto Y seguido de otro registro
        // válido o del fin del fichero. Si no hay ninguno, se acepta el primer
        // candidato "débil" (solo CRC): perder registros que sí se pueden leer
        // es peor que arriesgar una coincidencia de 1 entre 4.000 millones.
        // Esta distinción importa cuando un registro bueno queda encajado
        // entre dos corrupciones (prueba `dos_corrupciones_en_el_mismo_segmento`).
        let mut debil: Option<u64> = None;
        let mut cand = at + 1;
        while cand + HEADER as u64 <= self.file_len {
            if let Some(fin) = self.crc_valido_en(cand)? {
                if fin >= self.file_len || self.crc_valido_en(fin)?.is_some() {
                    return Ok(Some((cand, false)));
                }
                if debil.is_none() {
                    debil = Some(cand);
                }
            }
            cand += 1;
        }
        Ok(debil.map(|c| (c, false)))
    }

    /// Longitud total que declara la cabecera en `off`, si es plausible.
    fn longitud_declarada(&mut self, off: u64) -> io::Result<Option<u64>> {
        if self.file_len - off < HEADER as u64 {
            return Ok(None);
        }
        self.r.seek(SeekFrom::Start(off))?;
        let mut h = [0u8; HEADER];
        if !read_full(&mut self.r, &mut h)? {
            return Ok(None);
        }
        let head = match decode_head(&h) {
            Some(x) => x,
            None => return Ok(None),
        };
        if head.klen == 0 || head.klen > MAX_KEY || head.vlen > MAX_VALUE {
            return Ok(None);
        }
        Ok(Some(off + HEADER as u64 + head.klen as u64 + head.vlen as u64))
    }

    pub fn next(&mut self) -> io::Result<ReadOutcome> {
        let start = self.offset;
        if start >= self.file_len {
            return Ok(ReadOutcome::End);
        }
        let seek = self.needs_seek;
        self.needs_seek = false;
        match self.parse_at(start, seek)? {
            Parsed::Ok(r) => {
                self.offset = start + HEADER as u64 + r.key.len() as u64 + r.value.len() as u64;
                Ok(ReadOutcome::Record(r))
            }
            Parsed::Bad(cause) => {
                // Pista de longitud: solo sirve si la cabecera era plausible.
                let head_hint = self.longitud_declarada(start)?;
                self.needs_seek = true;
                match self.resincroniza(start, head_hint)? {
                    Some((resync_at, por_cabecera)) => {
                        self.offset = resync_at;
                        Ok(ReadOutcome::Corrupt { at: start, resync_at, cause, por_cabecera })
                    }
                    None => {
                        self.offset = self.file_len;
                        Ok(ReadOutcome::IncompleteTail { at: start, cause })
                    }
                }
            }
        }
    }
}

fn read_full<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<bool> {
    match r.read_exact(buf) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn two_records() -> Vec<u8> {
        let mut buf = Vec::new();
        encode(&mut buf, 1, Kind::Put, b"hola", b"mundo");
        encode(&mut buf, 2, Kind::Del, b"adios", b"");
        buf
    }

    fn reader(buf: Vec<u8>) -> SegmentReader<Cursor<Vec<u8>>> {
        let len = buf.len() as u64;
        SegmentReader::new(Cursor::new(buf), len)
    }

    #[test]
    fn ida_y_vuelta() {
        let mut rd = reader(two_records());
        match rd.next().unwrap() {
            ReadOutcome::Record(r) => {
                assert_eq!(r.seq, 1);
                assert_eq!(r.kind, Kind::Put);
                assert_eq!(r.key, b"hola");
                assert_eq!(r.value, b"mundo");
                assert_eq!(r.offset, 0);
                assert_eq!(r.vpos, (HEADER + 4) as u64);
            }
            _ => panic!("esperaba registro"),
        }
        match rd.next().unwrap() {
            ReadOutcome::Record(r) => {
                assert_eq!(r.seq, 2);
                assert_eq!(r.kind, Kind::Del);
                assert_eq!(r.key, b"adios");
            }
            _ => panic!("esperaba registro"),
        }
        assert!(matches!(rd.next().unwrap(), ReadOutcome::End));
    }

    /// Un registro cortado al final y sin nada detrás es una cola incompleta:
    /// recortarla no pierde nada.
    #[test]
    fn cola_cortada_es_cola_incompleta() {
        let mut buf = two_records();
        let first_len = (HEADER + 4 + 5) as u64;
        buf.truncate(buf.len() - 3);
        let mut rd = reader(buf);
        assert!(matches!(rd.next().unwrap(), ReadOutcome::Record(_)));
        match rd.next().unwrap() {
            ReadOutcome::IncompleteTail { at, .. } => assert_eq!(at, first_len),
            _ => panic!("esperaba cola incompleta"),
        }
    }

    /// Un bit cambiado en la CARGA del primer registro, con un registro válido
    /// detrás: es corrupción, y el sincronismo se recupera por la cabecera.
    #[test]
    fn bit_en_la_carga_resincroniza_por_cabecera() {
        let mut buf = two_records();
        let first_len = (HEADER + 4 + 5) as u64;
        buf[HEADER + 2] ^= 0x40; // un byte de la clave del primer registro
        let mut rd = reader(buf);
        match rd.next().unwrap() {
            ReadOutcome::Corrupt { at, resync_at, cause, por_cabecera } => {
                assert_eq!(at, 0);
                assert_eq!(resync_at, first_len, "debe saltar justo al segundo registro");
                assert_eq!(cause, Cause::CrcIncorrecto);
                assert!(por_cabecera, "la cabecera estaba intacta");
            }
            _ => panic!("esperaba corrupción, no cola incompleta"),
        }
        // Y el registro siguiente se lee entero.
        match rd.next().unwrap() {
            ReadOutcome::Record(r) => assert_eq!(r.key, b"adios"),
            _ => panic!("el segundo registro debe sobrevivir"),
        }
        assert!(matches!(rd.next().unwrap(), ReadOutcome::End));
    }

    /// Un byte cambiado en la CABECERA del primer registro: las longitudes ya
    /// no sirven, así que hay que rastrear. El segundo registro sobrevive.
    #[test]
    fn bit_en_la_cabecera_resincroniza_rastreando() {
        let mut buf = two_records();
        let first_len = (HEADER + 4 + 5) as u64;
        buf[12] ^= 0x10; // key_len del primer registro
        let mut rd = reader(buf);
        match rd.next().unwrap() {
            ReadOutcome::Corrupt { at, resync_at, .. } => {
                assert_eq!(at, 0);
                assert_eq!(resync_at, first_len);
            }
            _ => panic!("esperaba corrupción"),
        }
        match rd.next().unwrap() {
            ReadOutcome::Record(r) => assert_eq!(r.key, b"adios"),
            _ => panic!("el segundo registro debe sobrevivir"),
        }
    }

    /// Corrupción en el ÚLTIMO registro: no hay nada válido detrás, así que es
    /// indistinguible de una escritura a medias y se trata como cola.
    #[test]
    fn corrupcion_en_el_ultimo_registro_es_cola_incompleta() {
        let mut buf = two_records();
        let first_len = HEADER + 4 + 5;
        buf[first_len + 2] ^= 0x40;
        let mut rd = reader(buf);
        assert!(matches!(rd.next().unwrap(), ReadOutcome::Record(_)));
        match rd.next().unwrap() {
            ReadOutcome::IncompleteTail { at, .. } => assert_eq!(at, first_len as u64),
            _ => panic!("esperaba cola incompleta"),
        }
    }

    /// Tres registros, el del medio corrupto: se pierde uno y solo uno.
    #[test]
    fn solo_se_pierde_el_registro_corrupto() {
        let mut buf = Vec::new();
        encode(&mut buf, 1, Kind::Put, b"a", b"1111");
        let len1 = buf.len();
        encode(&mut buf, 2, Kind::Put, b"b", b"2222");
        let len2 = buf.len();
        encode(&mut buf, 3, Kind::Put, b"c", b"3333");
        buf[len1 + HEADER + 1] ^= 0xff; // en el valor del segundo
        let mut rd = reader(buf);
        let mut vistas = Vec::new();
        let mut corruptos = 0;
        loop {
            match rd.next().unwrap() {
                ReadOutcome::Record(r) => vistas.push(r.key),
                ReadOutcome::Corrupt { resync_at, .. } => {
                    corruptos += 1;
                    assert_eq!(resync_at, len2 as u64);
                }
                ReadOutcome::IncompleteTail { .. } => panic!("no debería haber cola incompleta"),
                ReadOutcome::End => break,
            }
        }
        assert_eq!(corruptos, 1);
        assert_eq!(vistas, vec![b"a".to_vec(), b"c".to_vec()]);
    }

    /// Dos corrupciones en el mismo segmento, con registros válidos entre
    /// medias y al final.
    #[test]
    fn dos_corrupciones_en_el_mismo_segmento() {
        let mut buf = Vec::new();
        let mut cortes = Vec::new();
        for i in 0..5u64 {
            encode(&mut buf, i + 1, Kind::Put, format!("k{i}").as_bytes(), b"valorvalor");
            cortes.push(buf.len());
        }
        buf[cortes[0] + HEADER + 2] ^= 0x0f; // dentro del registro 2
        buf[cortes[2] + HEADER + 2] ^= 0x0f; // dentro del registro 4
        let mut rd = reader(buf);
        let mut vistas = Vec::new();
        let mut corruptos = 0;
        loop {
            match rd.next().unwrap() {
                ReadOutcome::Record(r) => vistas.push(String::from_utf8(r.key).unwrap()),
                ReadOutcome::Corrupt { .. } => corruptos += 1,
                ReadOutcome::IncompleteTail { .. } => panic!("no esperaba cola incompleta"),
                ReadOutcome::End => break,
            }
        }
        assert_eq!(corruptos, 2);
        assert_eq!(vistas, vec!["k0", "k2", "k4"]);
    }

    /// Un fichero de basura entera no tiene nada legible: cola incompleta en 0,
    /// y nunca se declara una recuperación satisfactoria.
    #[test]
    fn basura_entera_no_es_legible() {
        let buf: Vec<u8> = (0..500u32).map(|i| (i % 256) as u8).collect();
        let mut rd = reader(buf);
        assert!(matches!(rd.next().unwrap(), ReadOutcome::IncompleteTail { at: 0, .. }));
    }

    /// Un `kind` desconocido se señala como cabecera inválida, no como CRC.
    #[test]
    fn kind_desconocido_es_cabecera_invalida() {
        let mut buf = two_records();
        let first_len = (HEADER + 4 + 5) as u64;
        buf[20] = 7;
        let mut rd = reader(buf);
        match rd.next().unwrap() {
            ReadOutcome::Corrupt { cause, resync_at, .. } => {
                assert_eq!(cause, Cause::CabeceraInvalida);
                assert_eq!(resync_at, first_len);
            }
            _ => panic!("esperaba corrupción por cabecera inválida"),
        }
    }
}
