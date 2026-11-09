//! BITÁCORA: motor de base de datos clave-valor construido desde cero.
//!
//! - `crc32`: suma de comprobación propia para detectar registros rotos.
//! - `wal`: formato binario de los registros y lector de segmentos.
//! - `store`: el motor (índice en memoria por shards + segmentos en disco,
//!   fsync por grupos, compactación concurrente).
//! - `protocol` y `server`: protocolo de texto por TCP, un hilo por conexión.
//! - `log`: emisor de eventos (texto o JSON por línea), sin dependencias.

pub mod crc32;
pub mod error;
pub mod log;
pub mod metricas;
pub mod protocol;
pub mod server;
pub mod store;
pub mod wal;

pub use error::{Error, Result};
pub use store::{
    BackupReport, CompactReport, Db, Estado, FsyncPolicy, Options, RecoveryPolicy, RecoveryReport,
    Salud, Stats,
};

/// Valor determinista para la clave `k{i}`; lo usan la prueba de apagón y el bench.
pub fn demo_value(i: u64) -> Vec<u8> {
    format!("valor-{i}-{:x}", i.wrapping_mul(0x9E37_79B9_7F4A_7C15)).into_bytes()
}
