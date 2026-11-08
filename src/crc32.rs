//! CRC-32 (IEEE 802.3) implementado a mano con una tabla de 256 entradas
//! calculada en tiempo de compilación. Cada registro del WAL lleva su CRC:
//! si el ordenador se apaga a mitad de una escritura, el CRC no cuadra y el
//! registro se descarta en lugar de corromper la base de datos.

const POLY: u32 = 0xEDB8_8320;

const fn make_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 == 1 { POLY ^ (c >> 1) } else { c >> 1 };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

static TABLE: [u32; 256] = make_table();

/// Acumulador: permite calcular el CRC por trozos (cabecera, clave, valor)
/// sin copiar los datos a un buffer intermedio.
pub struct Hasher {
    state: u32,
}

impl Hasher {
    pub fn new() -> Self {
        Hasher { state: 0xFFFF_FFFF }
    }

    pub fn update(&mut self, data: &[u8]) {
        let mut c = self.state;
        for &b in data {
            c = TABLE[((c ^ b as u32) & 0xFF) as usize] ^ (c >> 8);
        }
        self.state = c;
    }

    pub fn finish(&self) -> u32 {
        self.state ^ 0xFFFF_FFFF
    }
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

pub fn crc32(data: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(data);
    h.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_conocido() {
        // Vector de prueba estándar del CRC-32 IEEE.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn por_trozos_igual_que_entero() {
        let mut h = Hasher::new();
        h.update(b"1234");
        h.update(b"56789");
        assert_eq!(h.finish(), crc32(b"123456789"));
    }

    #[test]
    fn vacio() {
        assert_eq!(crc32(b""), 0);
    }
}
