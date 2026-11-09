//! Gate 6 — lo que hace falta para operar esto: configuración validada, un
//! solo escritor por directorio, salud, métricas y copias de seguridad.
//!
//! Lo que estas pruebas defienden, en orden de gravedad:
//!
//! 1. **Dos procesos no pueden abrir el mismo directorio.** Era posible, y los
//!    dos elegían el mismo número de segmento y el mismo número de secuencia:
//!    registros entremezclados en un fichero y `seq` duplicados, que es
//!    exactamente lo que la recuperación usa para decidir qué versión gana.
//! 2. **Una copia en caliente contiene todo lo confirmado antes del corte**,
//!    aunque se esté escribiendo y compactando a la vez, y no contiene ningún
//!    registro partido.
//! 3. **Una configuración que no hace lo que dice se rechaza al abrir**, no
//!    tres horas después.
//! 4. **`HEALTH` no dice `ok` cuando hay pérdida de datos o el disco ha
//!    fallado**, y las métricas cumplen el formato que dicen cumplir.

mod common;

use bitacora::{Db, Error, Estado, FsyncPolicy, Options, RecoveryPolicy};
use common::TempDir;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn opts(dir: &Path) -> Options {
    let mut o = Options::new(dir);
    o.fsync = FsyncPolicy::Group;
    o.auto_compact = false;
    o
}

// ---------------------------------------------------------------------------
// Configuración validada
// ---------------------------------------------------------------------------

/// Cada uno de estos valores hacía algo distinto de lo que decía, y ninguno
/// fallaba al abrir.
#[test]
fn la_configuracion_que_no_hace_lo_que_dice_se_rechaza_al_abrir() {
    let tmp = TempDir::new("config");

    /// Nombre del campo y cómo estropearlo.
    type Caso = (&'static str, Box<dyn Fn(&mut Options)>);
    let casos: Vec<Caso> = vec![
        // Se convertía en 1 en silencio.
        ("shards", Box::new(|o: &mut Options| o.shards = 0)),
        // Medido: 100 % de un núcleo con el servidor ocioso.
        (
            "group_interval",
            Box::new(|o: &mut Options| o.group_interval = Duration::ZERO),
        ),
        // Deja la auto-compactación encendida y sin dispararse jamás.
        (
            "compact_dead_ratio",
            Box::new(|o: &mut Options| o.compact_dead_ratio = 1.5),
        ),
        (
            "compact_dead_ratio",
            Box::new(|o: &mut Options| o.compact_dead_ratio = f64::NAN),
        ),
        // Un get que coincida con el final de una compactación daría error.
        (
            "read_retry_timeout",
            Box::new(|o: &mut Options| o.read_retry_timeout = Duration::ZERO),
        ),
    ];

    for (campo, estropea) in casos {
        let mut o = opts(tmp.path());
        estropea(&mut o);
        match Db::open(o) {
            Ok(_) => panic!("se abrió con {campo} inválido"),
            Err(e) => {
                assert_eq!(
                    e.categoria(),
                    "configuracion",
                    "{campo}: la categoría tiene que decir que es configuración, no E/S"
                );
                assert!(!e.reintentable(), "{campo}: reintentar no lo va a arreglar");
                assert!(
                    e.to_string().contains(campo),
                    "{campo}: el mensaje tiene que nombrar el campo: {e}"
                );
            }
        }
    }

    // Y la configuración por defecto, por supuesto, abre.
    let db = Db::open(opts(tmp.path())).expect("la configuración por defecto tiene que abrir");
    db.close().unwrap();
}

/// Un segmento pequeño **sí** se acepta: hace exactamente lo que dice (muchos
/// ficheros) y es una decisión legítima de quien configura. Hubo un umbral
/// mínimo aquí y rompía pruebas que quieren provocar cierres de segmento.
#[test]
fn un_segmento_pequeno_es_una_decision_legitima_no_un_error() {
    let tmp = TempDir::new("config-seg");
    let mut o = opts(tmp.path());
    o.max_segment_bytes = 400;
    let db = Db::open(o).expect("400 bytes por segmento es raro, pero no es un error");
    for i in 0..20u32 {
        db.put(format!("k{i}").as_bytes(), b"0123456789").unwrap();
    }
    assert_eq!(db.len(), 20);
    assert!(
        db.stats().segments > 1,
        "con segmentos de 400 bytes tiene que haber abierto varios"
    );
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// Un solo escritor por directorio
// ---------------------------------------------------------------------------

/// El agujero más grave que ha salido en este gate: nada impedía dos
/// escritores sobre la misma carpeta.
#[test]
fn un_segundo_escritor_sobre_el_mismo_directorio_no_puede_abrir() {
    let tmp = TempDir::new("bloqueo");
    let primero = Db::open(opts(tmp.path())).unwrap();
    primero.put(b"k", b"v").unwrap();

    match Db::open(opts(tmp.path())) {
        Ok(_) => panic!(
            "dos Db sobre el mismo directorio: los dos eligen el mismo número de \
             segmento y el mismo seq, y se entremezclan los registros"
        ),
        Err(e) => {
            assert_eq!(e.categoria(), "configuracion");
            assert!(
                e.to_string().contains("otro proceso"),
                "el mensaje tiene que decir que ya está en uso: {e}"
            );
        }
    }

    // Y en cuanto el primero cierra, el siguiente abre. Esto es lo que hace
    // que el bloqueo sea usable y no una trampa: `close()` lo suelta antes de
    // devolver, no cuando al recolector le parezca.
    primero.close().unwrap();
    drop(primero);
    let segundo = Db::open(opts(tmp.path())).expect("tras cerrar, tiene que poder abrirse");
    assert_eq!(segundo.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    segundo.close().unwrap();
}

/// El fichero de bloqueo no es un segmento: ni la recuperación ni la copia lo
/// pueden confundir con datos.
#[test]
fn el_fichero_de_bloqueo_no_se_confunde_con_un_segmento() {
    let tmp = TempDir::new("bloqueo-fichero");
    let db = Db::open(opts(tmp.path())).unwrap();
    db.put(b"a", b"1").unwrap();
    let st = db.stats();
    db.close().unwrap();
    drop(db);

    let lock = tmp.path().join("LOCK");
    assert!(lock.exists(), "debería existir el fichero de bloqueo");
    // La invariante: el motor cuenta exactamente los .log que hay en el disco.
    // Si el LOCK se colara como segmento, aquí habría uno de más.
    assert_eq!(
        st.segments as usize,
        common::segments(tmp.path()).len(),
        "el LOCK no es un segmento"
    );

    // Y al reabrir, tampoco. (Son dos: al abrir se añade un segmento activo
    // nuevo, porque el anterior ya tiene datos.)
    let db = Db::open(opts(tmp.path())).unwrap();
    assert_eq!(db.len(), 1);
    assert_eq!(
        db.stats().segments as usize,
        common::segments(tmp.path()).len(),
        "el LOCK no es un segmento tras reabrir"
    );
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// Copia de seguridad en caliente
// ---------------------------------------------------------------------------

/// Abre una copia en modo **estricto**: si tuviera un registro partido o un
/// CRC malo, esto falla en vez de callarse.
fn abre_la_copia(dir: &Path) -> Db {
    let mut o = Options::new(dir);
    o.fsync = FsyncPolicy::Never;
    o.auto_compact = false;
    o.recovery = RecoveryPolicy::Estricto;
    Db::open(o).expect("la copia tiene que abrir en modo estricto, sin corrupción")
}

/// La prueba central de la copia: con cuatro hilos escribiendo sin parar, todo
/// lo que estaba confirmado antes del corte está en la copia, y la copia abre
/// en modo estricto (ningún registro partido, ningún CRC malo).
#[test]
fn la_copia_en_caliente_contiene_todo_lo_confirmado_antes_del_corte() {
    let tmp = TempDir::new("copia-datos");
    let destino = TempDir::new("copia-destino");
    let db = Arc::new(
        Db::open({
            let mut o = opts(tmp.path());
            // Varios segmentos, para que la copia tenga que recorrer más de uno.
            o.max_segment_bytes = 16 << 10;
            o
        })
        .unwrap(),
    );

    // Estas 500 claves están confirmadas antes de empezar: tienen que estar.
    const SEGURAS: u32 = 500;
    for i in 0..SEGURAS {
        db.put(format!("segura{i}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }

    let parar = Arc::new(AtomicBool::new(false));
    let hilos: Vec<_> = (0..4u32)
        .map(|t| {
            let db = Arc::clone(&db);
            let parar = Arc::clone(&parar);
            thread::spawn(move || {
                let mut i = 0u32;
                while !parar.load(Ordering::Relaxed) {
                    let k = format!("ruido{t}-{i}");
                    db.put(k.as_bytes(), b"mientras se copia").unwrap();
                    i += 1;
                }
                i
            })
        })
        .collect();

    // Que los hilos arranquen de verdad antes de copiar.
    thread::sleep(Duration::from_millis(50));
    let informe = db.backup(destino.path()).expect("la copia tiene que salir");
    parar.store(true, Ordering::Relaxed);
    let escritas: u32 = hilos.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(escritas > 0, "los hilos de ruido no han escrito nada");
    assert!(informe.segments > 0 && informe.bytes > 0);

    db.close().unwrap();
    drop(db);

    let copia = abre_la_copia(destino.path());
    for i in 0..SEGURAS {
        let v = copia.get(format!("segura{i}").as_bytes()).unwrap();
        assert_eq!(
            v.as_deref(),
            Some(format!("v{i}").as_bytes()),
            "segura{i} estaba confirmada antes del corte y no está en la copia"
        );
    }
    // Y lo que haya de las escrituras concurrentes tiene que estar bien: una
    // clave a medias sería un registro partido, y la apertura estricta ya lo
    // habría rechazado. Aquí se comprueba además el contenido.
    let (claves, _) = copia.keys(b"ruido", 1_000_000);
    for k in &claves {
        assert_eq!(
            copia.get(k).unwrap().as_deref(),
            Some(&b"mientras se copia"[..]),
            "una clave de la copia tiene el valor cambiado"
        );
    }
    assert_eq!(copia.health().estado, Estado::Ok);
    copia.close().unwrap();
}

/// Copiar mientras compacta. Sin el candado de retirada, la compactación podía
/// borrar un segmento a medio copiar y la copia quedaría sin esos datos.
#[test]
fn copiar_mientras_compacta_no_pierde_ningun_segmento() {
    let tmp = TempDir::new("copia-compacta");
    let destino = TempDir::new("copia-compacta-dst");
    let db = Arc::new(
        Db::open({
            let mut o = opts(tmp.path());
            o.max_segment_bytes = 16 << 10;
            o
        })
        .unwrap(),
    );

    // 800 claves y luego sobrescribirlas todas: así hay basura de verdad y la
    // compactación tiene trabajo que hacer (y segmentos que retirar).
    const N: u32 = 800;
    for i in 0..N {
        db.put(format!("k{i}").as_bytes(), b"primera version")
            .unwrap();
    }
    for i in 0..N {
        db.put(format!("k{i}").as_bytes(), b"segunda version")
            .unwrap();
    }
    let antes = db.stats();
    assert!(antes.dead_bytes > 0, "hace falta basura para compactar");
    assert!(antes.segments > 2, "hacen falta varios segmentos");

    let compactando = {
        let db = Arc::clone(&db);
        thread::spawn(move || db.compact())
    };
    let informe = db.backup(destino.path()).expect("la copia tiene que salir");
    let rep = compactando.join().unwrap().expect("la compactación");
    assert!(informe.segments > 0);

    db.close().unwrap();
    drop(db);

    let copia = abre_la_copia(destino.path());
    assert_eq!(
        copia.len() as u32,
        N,
        "la copia tiene que tener las {N} claves vivas (compactación: {} segmentos retirados)",
        rep.segments_removed
    );
    for i in 0..N {
        assert_eq!(
            copia.get(format!("k{i}").as_bytes()).unwrap().as_deref(),
            Some(&b"segunda version"[..]),
            "k{i} no tiene la versión vigente en la copia"
        );
    }
    copia.close().unwrap();
}

#[test]
fn la_copia_se_niega_a_mezclarse_con_otra_y_a_copiarse_sobre_si_misma() {
    let tmp = TempDir::new("copia-negar");
    let destino = TempDir::new("copia-negar-dst");
    let db = Db::open(opts(tmp.path())).unwrap();
    db.put(b"a", b"1").unwrap();

    // Sobre el propio directorio de datos, nunca.
    match db.backup(tmp.path()) {
        Ok(_) => panic!("no debería poder copiarse sobre sí misma"),
        Err(e) => assert_eq!(e.categoria(), "invalida"),
    }

    db.backup(destino.path()).expect("la primera copia");
    db.put(b"b", b"2").unwrap();

    // Una segunda copia encima mezclaría segmentos de dos momentos distintos:
    // un segmento viejo puede traer una clave que el nuevo ya había borrado.
    match db.backup(destino.path()) {
        Ok(_) => panic!("no debería poder copiar sobre una copia existente"),
        Err(e) => {
            assert_eq!(e.categoria(), "invalida");
            assert!(e.to_string().contains("vacío"), "{e}");
        }
    }
    db.close().unwrap();
}

/// Una copia restaurada es una base de datos normal: se puede escribir en ella
/// y vuelve a tener su propio bloqueo.
#[test]
fn una_copia_restaurada_se_puede_usar_como_base_de_datos() {
    let tmp = TempDir::new("restaurar");
    let destino = TempDir::new("restaurar-dst");
    {
        let db = Db::open(opts(tmp.path())).unwrap();
        for i in 0..50u32 {
            db.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        db.delete(b"k7").unwrap();
        db.backup(destino.path()).unwrap();
        db.close().unwrap();
    }
    let copia = abre_la_copia(destino.path());
    assert_eq!(copia.len(), 49, "la clave borrada no debe reaparecer");
    assert!(copia.get(b"k7").unwrap().is_none());
    copia.close().unwrap();
    drop(copia);

    // Reabrir en modo normal y escribir.
    let db = Db::open(opts(destino.path())).unwrap();
    db.put(b"nueva", b"vale").unwrap();
    assert_eq!(db.get(b"nueva").unwrap().as_deref(), Some(&b"vale"[..]));
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// Salud
// ---------------------------------------------------------------------------

#[test]
fn una_base_recien_abierta_esta_ok() {
    let tmp = TempDir::new("salud-ok");
    let db = Db::open(opts(tmp.path())).unwrap();
    let s = db.health();
    assert_eq!(s.estado, Estado::Ok);
    assert!(s.motivos.is_empty(), "{:?}", s.motivos);
    assert_eq!(s.estado.codigo_salida(), 0);
    assert_eq!(db.stats().estado, "ok");
    db.close().unwrap();
}

/// Si el disco falla, `HEALTH` tiene que decirlo. Un semáforo que sigue en
/// verde con la base no escribible es peor que no tener semáforo.
#[test]
fn health_dice_critico_cuando_el_fsync_ha_fallado() {
    let tmp = TempDir::new("salud-fsync");
    let mut o = opts(tmp.path());
    o.fsync = FsyncPolicy::Always;
    o.fsync_falla_tras = Some(1);
    let db = Db::open(o).unwrap();

    db.put(b"a", b"1").expect("la primera escritura sí entra");
    let fallo = db.put(b"b", b"2");
    assert!(matches!(fallo, Err(Error::NoEscribible(_))), "{fallo:?}");

    let s = db.health();
    assert_eq!(s.estado, Estado::Critico, "motivos: {:?}", s.motivos);
    assert_eq!(s.estado.codigo_salida(), 1, "un healthcheck debe fallar");
    assert!(
        s.motivos.iter().any(|m| m.contains("no-escribible")),
        "{:?}",
        s.motivos
    );
    assert_eq!(db.stats().estado, "critico");
    // Y las lecturas siguen funcionando, que es la otra mitad de la promesa.
    assert_eq!(db.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
}

// ---------------------------------------------------------------------------
// Métricas
// ---------------------------------------------------------------------------

/// Comprueba el formato de exposición, no solo que haya texto: un recolector
/// estricto descarta lo que no cumple estas reglas, y entonces no hay métricas
/// aunque el servidor crea que las está dando.
#[test]
fn las_metricas_cumplen_el_formato_de_exposicion() {
    let tmp = TempDir::new("metricas");
    let db = Db::open(opts(tmp.path())).unwrap();
    db.put(b"a", b"uno").unwrap();
    db.put(b"b", b"dos").unwrap();
    db.get(b"a").unwrap();
    db.get(b"no-existe").unwrap();
    db.delete(b"b").unwrap();
    let _ = db.put(b"", b"clave vacia"); // un error, para el contador

    let texto = db.metricas().en_texto();

    let mut con_help: Vec<&str> = Vec::new();
    let mut con_type: Vec<&str> = Vec::new();
    let mut vistas: Vec<String> = Vec::new();
    for linea in texto.lines() {
        if let Some(resto) = linea.strip_prefix("# HELP ") {
            let (nombre, ayuda) = resto.split_once(' ').expect("HELP sin texto de ayuda");
            assert!(!ayuda.is_empty());
            assert!(
                !con_help.contains(&nombre),
                "{nombre}: dos cabeceras HELP para la misma familia"
            );
            con_help.push(nombre);
        } else if let Some(resto) = linea.strip_prefix("# TYPE ") {
            let (nombre, tipo) = resto.split_once(' ').expect("TYPE sin tipo");
            assert!(
                tipo == "gauge" || tipo == "counter",
                "{nombre}: tipo {tipo} desconocido"
            );
            assert!(
                con_help.contains(&nombre),
                "{nombre}: TYPE antes de su HELP"
            );
            assert!(!con_type.contains(&nombre), "{nombre}: dos TYPE");
            con_type.push(nombre);
            // Los contadores acumulativos acaban en _total por convención.
            if tipo == "counter" {
                assert!(
                    nombre.ends_with("_total"),
                    "{nombre}: es un counter y no acaba en _total"
                );
            }
        } else {
            assert!(!linea.starts_with('#'), "comentario desconocido: {linea}");
            let (muestra, valor) = linea.rsplit_once(' ').expect("muestra sin valor");
            valor
                .parse::<f64>()
                .unwrap_or_else(|_| panic!("valor no numérico en: {linea}"));
            let familia = muestra.split_once('{').map_or(muestra, |(n, _)| n);
            assert!(
                con_type.contains(&familia),
                "{familia}: muestra sin TYPE declarado antes"
            );
            // Las muestras de una familia van juntas: si ya se vio otra
            // familia después de esta, está intercalada.
            if vistas.last().map(String::as_str) != Some(familia) {
                assert!(
                    !vistas.iter().any(|v| v == familia),
                    "{familia}: muestras separadas por otra familia"
                );
                vistas.push(familia.to_string());
            }
        }
    }
    assert_eq!(con_help.len(), con_type.len(), "HELP y TYPE descompensados");
    assert!(con_help.len() > 15, "solo {} familias", con_help.len());

    // Y los valores dicen la verdad.
    let valor = |nombre: &str| -> u64 {
        texto
            .lines()
            .find(|l| l.starts_with(nombre) && !l.starts_with('#'))
            .unwrap_or_else(|| panic!("no está la métrica {nombre}"))
            .rsplit(' ')
            .next()
            .unwrap()
            .parse()
            .unwrap()
    };
    assert_eq!(valor("bitacora_up"), 1);
    assert_eq!(valor("bitacora_health_state"), 0);
    assert_eq!(valor("bitacora_writable"), 1);
    assert_eq!(valor("bitacora_keys"), 1, "quedaba una clave");
    assert_eq!(valor(r#"bitacora_ops_total{op="put"}"#), 2);
    assert_eq!(valor(r#"bitacora_ops_total{op="get"}"#), 2);
    assert_eq!(valor(r#"bitacora_ops_total{op="delete"}"#), 1);
    assert_eq!(valor(r#"bitacora_get_results_total{result="hit"}"#), 1);
    assert_eq!(valor(r#"bitacora_get_results_total{result="miss"}"#), 1);
    assert_eq!(valor("bitacora_errors_total"), 1, "la clave vacía");
    assert!(valor("bitacora_fsync_total") > 0);
    db.close().unwrap();
}

/// Los contadores de operaciones también salen por `STATS`, que es la vía que
/// ya existía: añadir líneas no debe romper a quien lee la cuenta.
#[test]
fn stats_sigue_anunciando_cuantas_lineas_trae() {
    let tmp = TempDir::new("stats");
    let db = Db::open(opts(tmp.path())).unwrap();
    db.put(b"a", b"1").unwrap();
    let lineas = db.stats().lines();
    for l in &lineas {
        assert!(
            l.split(' ').count() >= 2,
            "cada línea de STATS es «nombre valor»: {l}"
        );
    }
    assert!(lineas.iter().any(|l| l == "ops_put 1"));
    assert!(lineas.iter().any(|l| l.starts_with("health ")));
    assert!(lineas.iter().any(|l| l == "estado ok"));
    db.close().unwrap();
}
