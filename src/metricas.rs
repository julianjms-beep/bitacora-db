//! Métricas en el formato de exposición de Prometheus, escritas a mano.
//!
//! Sin dependencias no hay crate `prometheus`, y tampoco hace falta: el formato
//! de texto es deliberadamente simple. Lo que **sí** hace falta es respetar sus
//! reglas, porque un recolector estricto descarta lo que no las cumple:
//!
//! * `# HELP` y `# TYPE` aparecen **una vez por familia**, antes de sus
//!   muestras, y todas las muestras de una familia van juntas.
//! * Un `counter` solo sube (y se reinicia a 0 al reiniciar el proceso, que es
//!   algo que el recolector sabe manejar); un `gauge` sube y baja. Declarar un
//!   gauge como counter hace que las gráficas de tasa salgan mal.
//! * Por convención, los contadores acumulativos acaban en `_total` y los
//!   valores en bytes o segundos llevan la unidad en el nombre.
//! * No se emiten marcas de tiempo: las pone el recolector al recoger.
//!
//! Esto **no** incluye un servidor HTTP. El texto se sirve por la orden
//! `METRICS` del protocolo y por `bitacora metrics`; en `docs/OPERACION.md`
//! está cómo conectarlo a un recolector con dos líneas de shell.

use std::fmt::Write as _;

#[derive(Default)]
pub struct Registro {
    buf: String,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tipo {
    Gauge,
    Counter,
}

impl Tipo {
    fn name(self) -> &'static str {
        match self {
            Tipo::Gauge => "gauge",
            Tipo::Counter => "counter",
        }
    }
}

impl Registro {
    pub fn new() -> Registro {
        Registro {
            buf: String::with_capacity(4096),
        }
    }

    fn cabecera(&mut self, nombre: &str, ayuda: &str, t: Tipo) {
        // `# HELP` no admite saltos de línea sin escapar: romperían el formato.
        let ayuda = ayuda.replace('\\', "\\\\").replace('\n', "\\n");
        let _ = writeln!(self.buf, "# HELP {nombre} {ayuda}");
        let _ = writeln!(self.buf, "# TYPE {nombre} {}", t.name());
    }

    pub fn gauge(&mut self, nombre: &str, ayuda: &str, valor: u64) {
        self.cabecera(nombre, ayuda, Tipo::Gauge);
        let _ = writeln!(self.buf, "{nombre} {valor}");
    }

    pub fn contador(&mut self, nombre: &str, ayuda: &str, valor: u64) {
        self.cabecera(nombre, ayuda, Tipo::Counter);
        let _ = writeln!(self.buf, "{nombre} {valor}");
    }

    /// Una familia con varias muestras distinguidas por una etiqueta:
    /// `bitacora_ops_total{op="put"} 42`.
    ///
    /// Los valores de etiqueta que se le pasan son constantes del programa, no
    /// datos del cliente; aun así se escapan, porque una etiqueta con una
    /// comilla rompería la línea entera.
    pub fn familia(&mut self, nombre: &str, ayuda: &str, etiqueta: &str, muestras: &[(&str, u64)]) {
        self.cabecera(nombre, ayuda, Tipo::Counter);
        for (v, n) in muestras {
            let _ = writeln!(self.buf, "{nombre}{{{etiqueta}=\"{}\"}} {n}", escapa(v));
        }
    }

    pub fn familia_gauge(
        &mut self,
        nombre: &str,
        ayuda: &str,
        etiqueta: &str,
        muestras: &[(&str, u64)],
    ) {
        self.cabecera(nombre, ayuda, Tipo::Gauge);
        for (v, n) in muestras {
            let _ = writeln!(self.buf, "{nombre}{{{etiqueta}=\"{}\"}} {n}", escapa(v));
        }
    }

    pub fn texto(&self) -> &str {
        &self.buf
    }

    pub fn en_texto(self) -> String {
        self.buf
    }

    /// Añade al final lo que haya producido otro registro (por ejemplo, el
    /// servidor sus conexiones después de las métricas del motor).
    pub fn anexa(&mut self, otro: &Registro) {
        self.buf.push_str(&otro.buf);
    }
}

fn escapa(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn un_gauge_sale_con_su_cabecera() {
        let mut r = Registro::new();
        r.gauge("bitacora_keys", "Claves vivas.", 7);
        assert_eq!(
            r.texto(),
            "# HELP bitacora_keys Claves vivas.\n# TYPE bitacora_keys gauge\nbitacora_keys 7\n"
        );
    }

    /// Un contador declarado como gauge haría que `rate()` saliera mal, así
    /// que el tipo tiene que ir bien.
    #[test]
    fn los_contadores_se_declaran_counter() {
        let mut r = Registro::new();
        r.contador("bitacora_fsync_total", "fsync hechos.", 3);
        assert!(r.texto().contains("# TYPE bitacora_fsync_total counter"));
    }

    #[test]
    fn una_familia_agrupa_sus_muestras_bajo_una_sola_cabecera() {
        let mut r = Registro::new();
        r.familia(
            "bitacora_ops_total",
            "Operaciones.",
            "op",
            &[("put", 1), ("get", 2)],
        );
        let t = r.texto();
        assert_eq!(t.matches("# TYPE").count(), 1, "{t}");
        assert!(t.contains("bitacora_ops_total{op=\"put\"} 1"), "{t}");
        assert!(t.contains("bitacora_ops_total{op=\"get\"} 2"), "{t}");
    }

    #[test]
    fn se_escapa_lo_que_rompe_el_formato() {
        let mut r = Registro::new();
        r.familia("m", "con un salto\ny una barra \\", "k", &[("a\"b", 1)]);
        let t = r.texto();
        assert!(
            t.contains("# HELP m con un salto\\ny una barra \\\\\n"),
            "{t}"
        );
        assert!(t.contains("m{k=\"a\\\"b\"} 1"), "{t}");
        // Y la cabecera sigue siendo UNA línea, que es lo que importa.
        assert_eq!(t.lines().count(), 3, "{t}");
    }
}
