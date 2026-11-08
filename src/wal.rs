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
//! - Un registro que no pasa las comprobaciones se considera "roto" (escritura
//!   a medias por un apagón) y marca el final útil del fichero.

use crate::crc32::Hasher;
use std::io::{self, BufRead, Read};

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

pub enum ReadOutcome {
    Record(ReadRecord),
    /// Fin limpio del fichero.
    End,
    /// Registro incompleto o corrupto que empieza en `at`.
    Torn { at: u64 },
}

/// Lee registros secuencialmente de un segmento y detecta la cola rota.
pub struct SegmentReader<R: BufRead> {
    r: R,
    offset: u64,
    file_len: u64,
}

impl<R: BufRead> SegmentReader<R> {
    pub fn new(r: R, file_len: u64) -> Self {
        SegmentReader { r, offset: 0, file_len }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn next(&mut self) -> io::Result<ReadOutcome> {
        let start = self.offset;
        if start >= self.file_len {
            return Ok(ReadOutcome::End);
        }
        if self.file_len - start < HEADER as u64 {
            return Ok(ReadOutcome::Torn { at: start });
        }
        let mut h = [0u8; HEADER];
        if !read_full(&mut self.r, &mut h)? {
            return Ok(ReadOutcome::Torn { at: start });
        }
        let crc = u32::from_le_bytes(h[0..4].try_into().unwrap());
        let seq = u64::from_le_bytes(h[4..12].try_into().unwrap());
        let klen = u32::from_le_bytes(h[12..16].try_into().unwrap()) as usize;
        let vlen = u32::from_le_bytes(h[16..20].try_into().unwrap()) as usize;
        let kind = match h[20] {
            0 => Kind::Put,
            1 => Kind::Del,
            _ => return Ok(ReadOutcome::Torn { at: start }),
        };
        let total = HEADER as u64 + klen as u64 + vlen as u64;
        if klen == 0 || klen > MAX_KEY || vlen > MAX_VALUE || start + total > self.file_len {
            return Ok(ReadOutcome::Torn { at: start });
        }
        let mut key = vec![0u8; klen];
        if !read_full(&mut self.r, &mut key)? {
            return Ok(ReadOutcome::Torn { at: start });
        }
        let mut value = vec![0u8; vlen];
        if !read_full(&mut self.r, &mut value)? {
            return Ok(ReadOutcome::Torn { at: start });
        }
        let mut hs = Hasher::new();
        hs.update(&h[4..]);
        hs.update(&key);
        hs.update(&value);
        if hs.finish() != crc {
            return Ok(ReadOutcome::Torn { at: start });
        }
        self.offset = start + total;
        Ok(ReadOutcome::Record(ReadRecord {
            seq,
            kind,
            key,
            value,
            offset: start,
            vpos: start + HEADER as u64 + klen as u64,
        }))
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

    #[test]
    fn ida_y_vuelta() {
        let buf = two_records();
        let len = buf.len() as u64;
        let mut rd = SegmentReader::new(Cursor::new(buf), len);
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

    #[test]
    fn cola_cortada_se_detecta() {
        let mut buf = two_records();
        let first_len = (HEADER + 4 + 5) as u64;
        buf.truncate(buf.len() - 3);
        let len = buf.len() as u64;
        let mut rd = SegmentReader::new(Cursor::new(buf), len);
        assert!(matches!(rd.next().unwrap(), ReadOutcome::Record(_)));
        match rd.next().unwrap() {
            ReadOutcome::Torn { at } => assert_eq!(at, first_len),
            _ => panic!("esperaba cola rota"),
        }
    }

    #[test]
    fn bit_cambiado_se_detecta() {
        let mut buf = two_records();
        buf[HEADER + 2] ^= 0x40; // un bit de la clave del primer registro
        let len = buf.len() as u64;
        let mut rd = SegmentReader::new(Cursor::new(buf), len);
        assert!(matches!(rd.next().unwrap(), ReadOutcome::Torn { at: 0 }));
    }
}
