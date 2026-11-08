//! Errores tipados del motor.
//!
//! Antes todo era `io::Error`, buena parte con `ErrorKind::Other` y un texto.
//! Eso hacía imposible lo único que de verdad necesita saber quien llama:
//! **¿tiene sentido reintentar?** Una clave inválida no se arregla
//! reintentando; un `fsync` fallido tampoco, pero por motivos opuestos; un
//! error de E/S transitorio sí.
//!
//! Cada variante responde a esa pregunta con [`Error::reintentable`], y
//! [`Error::categoria`] da una palabra estable para registros y para el
//! protocolo, de modo que un cliente pueda decidir sin analizar prosa.

use std::fmt;
use std::io;

/// El `Result` del motor.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    /// La petición no es válida: clave vacía, clave de más de 64 KiB, valor de
    /// más de 1 GiB. Reintentarla da el mismo resultado.
    Invalida(&'static str),

    /// El motor no acepta escrituras: un `fsync` falló, o no se pudo deshacer
    /// una escritura parcial. Las lecturas siguen funcionando. Hace falta
    /// reiniciar; ver `docs/RECUPERACION.md`.
    NoEscribible(String),

    /// Al abrir se encontró corrupción y la política es
    /// [`crate::RecoveryPolicy::Estricto`]: **no se ha modificado ningún
    /// fichero**. El texto dice qué se encontró y dónde.
    Corrupcion(String),

    /// Una invariante del motor no se cumple. No es culpa de la petición, y
    /// merece un informe de fallo.
    Interno(String),

    /// Error de entrada/salida del sistema.
    Io(io::Error),
}

impl Error {
    /// Palabra estable para registros, métricas y protocolo. No cambia aunque
    /// cambie el texto del mensaje.
    pub fn categoria(&self) -> &'static str {
        match self {
            Error::Invalida(_) => "invalida",
            Error::NoEscribible(_) => "no-escribible",
            Error::Corrupcion(_) => "corrupcion",
            Error::Interno(_) => "interno",
            Error::Io(_) => "io",
        }
    }

    /// ¿Tiene sentido que el cliente reintente la misma operación?
    ///
    /// `Invalida` no: dará lo mismo. `NoEscribible` no: hace falta reiniciar el
    /// servidor. `Corrupcion` no: hace falta intervención humana. `Interno` e
    /// `Io` sí: pueden ser transitorios.
    pub fn reintentable(&self) -> bool {
        match self {
            Error::Invalida(_) | Error::NoEscribible(_) | Error::Corrupcion(_) => false,
            Error::Interno(_) | Error::Io(_) => true,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Invalida(m) => write!(f, "petición inválida: {m}"),
            Error::NoEscribible(m) => write!(f, "base de datos NO ESCRIBIBLE: {m}"),
            Error::Corrupcion(m) => write!(f, "{m}"),
            Error::Interno(m) => write!(f, "fallo interno: {m}"),
            Error::Io(e) => write!(f, "error de E/S: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

/// Para quien siga trabajando con `io::Error`, como el servidor TCP. La
/// categoría se conserva en el texto.
impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Io(io) => io,
            Error::Invalida(m) => io::Error::new(io::ErrorKind::InvalidInput, m),
            otro => io::Error::other(otro.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn las_categorias_son_estables() {
        assert_eq!(Error::Invalida("x").categoria(), "invalida");
        assert_eq!(Error::NoEscribible("x".into()).categoria(), "no-escribible");
        assert_eq!(Error::Corrupcion("x".into()).categoria(), "corrupcion");
        assert_eq!(Error::Interno("x".into()).categoria(), "interno");
        assert_eq!(Error::Io(io::Error::other("x")).categoria(), "io");
    }

    /// Lo que el cliente necesita saber: una clave mala no se arregla
    /// reintentando, y un disco caído tampoco, pero un error de E/S sí puede.
    #[test]
    fn solo_lo_transitorio_es_reintentable() {
        assert!(!Error::Invalida("clave vacía").reintentable());
        assert!(!Error::NoEscribible("fsync".into()).reintentable());
        assert!(!Error::Corrupcion("crc".into()).reintentable());
        assert!(Error::Interno("invariante".into()).reintentable());
        assert!(Error::Io(io::Error::other("disco lleno")).reintentable());
    }

    #[test]
    fn un_error_de_io_se_convierte_en_los_dos_sentidos_sin_perder_la_causa() {
        let original = io::Error::new(io::ErrorKind::PermissionDenied, "sin permiso");
        let propio: Error = original.into();
        assert_eq!(propio.categoria(), "io");
        let vuelta: io::Error = propio.into();
        assert_eq!(vuelta.kind(), io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn el_texto_dice_de_que_va() {
        assert!(Error::NoEscribible("el disco".into())
            .to_string()
            .contains("NO ESCRIBIBLE"));
        assert!(Error::Invalida("clave vacía").to_string().contains("inválida"));
    }
}
