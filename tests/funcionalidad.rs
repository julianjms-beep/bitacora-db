//! Gate 7 — lote, compare-and-swap, caducidad y namespaces.
//!
//! Lo que estas pruebas defienden, en orden de gravedad:
//!
//! 1. **Una clave caducada no resucita al reiniciar**, y tampoco resucita el
//!    valor que esa clave tenía *antes* del `put_ttl`. Si la recuperación se
//!    limitara a ignorar el registro vencido, el `put` anterior ganaría y
//!    volvería un dato que el cliente creía borrado.
//! 2. **Un CAS no se lo lleva más de uno.** Con ocho hilos peleando por la
//!    misma clave, el valor final tiene que cuadrar con el número de éxitos.
//! 3. **Un lote no se aplica a medias por un error de forma**: si una
//!    operación es inválida, no se escribe ninguna.
//! 4. La contabilidad de bytes vivos y muertos sigue cuadrando con los 8 bytes
//!    extra de la caducidad, porque es la que decide cuándo compactar.

mod common;

use bitacora::{Db, Error, FsyncPolicy, Op, Options, RecoveryPolicy};
use common::TempDir;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
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
// Caducidad
// ---------------------------------------------------------------------------

#[test]
fn una_clave_con_caducidad_deja_de_verse_cuando_toca() {
    let tmp = TempDir::new("ttl-basico");
    let db = Db::open(opts(tmp.path())).unwrap();

    db.put_ttl(b"corta", b"adios", Duration::from_millis(120))
        .unwrap();
    db.put_ttl(b"larga", b"sigo", Duration::from_secs(600))
        .unwrap();
    db.put(b"eterna", b"siempre").unwrap();

    assert_eq!(db.get(b"corta").unwrap().as_deref(), Some(&b"adios"[..]));
    assert!(db.exists(b"corta"));
    assert_eq!(db.len(), 3);
    // `ttl` dice lo que queda, y distingue «no caduca» de «no está».
    assert!(db.ttl(b"corta").unwrap().unwrap() <= Duration::from_millis(120));
    assert_eq!(db.ttl(b"eterna"), Some(None), "sin caducidad");
    assert_eq!(db.ttl(b"no-existe"), None);

    thread::sleep(Duration::from_millis(200));

    assert_eq!(db.get(b"corta").unwrap(), None, "ya debería haber caducado");
    assert!(!db.exists(b"corta"));
    assert_eq!(db.ttl(b"corta"), None);
    assert_eq!(db.len(), 2, "len no cuenta las caducadas");
    let (ks, _) = db.keys(b"", 100);
    assert_eq!(ks.len(), 2, "KEYS tampoco: {ks:?}");
    assert!(!ks.iter().any(|k| k == b"corta"));
    // Y lo que no ha caducado sigue ahí.
    assert_eq!(db.get(b"larga").unwrap().as_deref(), Some(&b"sigo"[..]));
    assert_eq!(db.get(b"eterna").unwrap().as_deref(), Some(&b"siempre"[..]));

    // Borrar algo caducado es borrar lo que no existe.
    assert!(!db.delete(b"corta").unwrap());
    db.close().unwrap();
}

/// La prueba más importante del TTL: la caducidad es durable y **no deja
/// resucitar al valor anterior de la misma clave**.
#[test]
fn una_clave_caducada_no_resucita_al_reiniciar_ni_el_valor_que_tenia_antes() {
    let tmp = TempDir::new("ttl-reinicio");
    {
        let db = Db::open(opts(tmp.path())).unwrap();
        // Primero un valor normal, y luego el mismo nombre con caducidad corta.
        db.put(b"sesion", b"VALOR-VIEJO").unwrap();
        db.put_ttl(b"sesion", b"token", Duration::from_millis(80))
            .unwrap();
        db.put_ttl(b"otra", b"vive", Duration::from_secs(600))
            .unwrap();
        thread::sleep(Duration::from_millis(150));
        assert_eq!(db.get(b"sesion").unwrap(), None);
        db.close().unwrap();
    }
    let db = Db::open(opts(tmp.path())).unwrap();
    assert_eq!(
        db.get(b"sesion").unwrap(),
        None,
        "la clave caducada ha reaparecido, o peor: ha reaparecido VALOR-VIEJO"
    );
    assert!(!db.exists(b"sesion"));
    assert_eq!(db.len(), 1, "solo debería quedar 'otra'");
    assert_eq!(db.get(b"otra").unwrap().as_deref(), Some(&b"vive"[..]));
    // La que no ha caducado conserva su caducidad tras el reinicio: es durable.
    let queda = db.ttl(b"otra").unwrap().unwrap();
    assert!(
        queda > Duration::from_secs(500) && queda <= Duration::from_secs(600),
        "quedaban {queda:?}"
    );
    db.close().unwrap();
}

#[test]
fn un_put_normal_quita_la_caducidad_y_la_compactacion_la_conserva() {
    let tmp = TempDir::new("ttl-persist");
    let db = Db::open(opts(tmp.path())).unwrap();
    db.put_ttl(b"k", b"con ttl", Duration::from_millis(100))
        .unwrap();
    db.put(b"k", b"sin ttl").unwrap();
    assert_eq!(db.ttl(b"k"), Some(None), "el put normal quita la caducidad");
    thread::sleep(Duration::from_millis(150));
    assert_eq!(
        db.get(b"k").unwrap().as_deref(),
        Some(&b"sin ttl"[..]),
        "no debería caducar: ya no tiene caducidad"
    );

    // Y al revés: una caducidad lejana sobrevive a una compactación.
    db.put_ttl(b"lejana", b"v", Duration::from_secs(3600))
        .unwrap();
    for i in 0..200u32 {
        db.put(format!("relleno{i}").as_bytes(), b"xxxxxxxxxx")
            .unwrap();
    }
    db.compact().unwrap();
    let queda = db
        .ttl(b"lejana")
        .expect("sigue estando")
        .expect("sigue teniendo caducidad");
    assert!(
        queda > Duration::from_secs(3500),
        "la compactación se ha comido la caducidad: quedan {queda:?}"
    );
    assert_eq!(db.get(b"lejana").unwrap().as_deref(), Some(&b"v"[..]));
    db.close().unwrap();
}

/// La compactación es lo único que recupera de verdad la RAM de una clave
/// caducada. Hasta entonces solo ha dejado de verse.
#[test]
fn la_compactacion_retira_las_caducadas_del_indice() {
    let tmp = TempDir::new("ttl-compacta");
    let mut o = opts(tmp.path());
    o.max_segment_bytes = 8 << 10;
    let db = Db::open(o).unwrap();

    for i in 0..300u32 {
        db.put_ttl(
            format!("efimera{i}").as_bytes(),
            b"se va a caducar",
            Duration::from_millis(150),
        )
        .unwrap();
    }
    for i in 0..50u32 {
        db.put(format!("firme{i}").as_bytes(), b"se queda").unwrap();
    }
    let antes = db.stats();
    assert_eq!(antes.keys, 350);

    thread::sleep(Duration::from_millis(250));
    let medio = db.stats();
    assert_eq!(medio.keys, 50, "las caducadas ya no se cuentan");
    assert!(
        medio.index_bytes_est > 300 * 40,
        "pero siguen ocupando RAM: {} bytes",
        medio.index_bytes_est
    );

    let rep = db.compact().unwrap();
    assert!(
        rep.expired_removed >= 300,
        "la compactación debería haber retirado las 300: {}",
        rep.expired_removed
    );
    let despues = db.stats();
    assert_eq!(despues.keys, 50);
    assert!(
        despues.index_bytes_est < medio.index_bytes_est / 2,
        "el índice tenía que encogerse: {} -> {}",
        medio.index_bytes_est,
        despues.index_bytes_est
    );
    for i in 0..50u32 {
        assert_eq!(
            db.get(format!("firme{i}").as_bytes()).unwrap().as_deref(),
            Some(&b"se queda"[..])
        );
    }
    db.close().unwrap();
}

/// Los 8 bytes de la caducidad tienen que entrar en la contabilidad de bytes:
/// es la que decide cuándo se compacta.
#[test]
fn la_contabilidad_de_bytes_cuadra_con_la_caducidad() {
    let tmp = TempDir::new("ttl-bytes");
    let db = Db::open(opts(tmp.path())).unwrap();
    db.put_ttl(b"a", b"12345", Duration::from_secs(600))
        .unwrap();
    db.put(b"b", b"12345").unwrap();
    let st = db.stats();
    // El registro con caducidad ocupa exactamente 8 bytes más que el otro, y
    // `live_bytes` tiene que reflejar los dos tal como están en el disco.
    assert_eq!(
        st.live_bytes,
        common::disk_bytes(tmp.path()),
        "los bytes vivos deberían ser todo el log: no hay nada muerto todavía"
    );
    assert_eq!(st.dead_bytes, 0);

    // Al sobrescribir, lo viejo pasa a muerto con su tamaño real (con los 8).
    db.put(b"a", b"otro").unwrap();
    let st2 = db.stats();
    assert_eq!(
        st2.live_bytes + st2.dead_bytes,
        common::disk_bytes(tmp.path()),
        "vivos + muertos tiene que ser el log entero"
    );
    db.close().unwrap();
}

#[test]
fn un_ttl_de_cero_no_es_un_borrado_disfrazado() {
    let tmp = TempDir::new("ttl-cero");
    let db = Db::open(opts(tmp.path())).unwrap();
    match db.put_ttl(b"k", b"v", Duration::ZERO) {
        Err(Error::Invalida(m)) => assert!(m.contains("borrado"), "{m}"),
        otro => panic!("un TTL de 0 tenía que rechazarse: {otro:?}"),
    }
    assert!(!db.exists(b"k"));
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// Lote
// ---------------------------------------------------------------------------

#[test]
fn un_lote_se_aplica_en_orden_y_con_un_solo_fsync() {
    let tmp = TempDir::new("lote");
    let db = Db::open(opts(tmp.path())).unwrap();
    db.put(b"previa", b"1").unwrap();
    let fsyncs_antes = db.stats().fsyncs;

    let n = db
        .write_batch(&[
            Op::Put {
                key: b"a",
                value: b"uno",
            },
            Op::Put {
                key: b"b",
                value: b"dos",
            },
            Op::Delete { key: b"previa" },
            Op::Delete { key: b"no-esta" }, // no escribe nada
            Op::PutTtl {
                key: b"c",
                value: b"tres",
                ttl: Duration::from_secs(600),
            },
            // El orden manda: esta sobrescribe la primera.
            Op::Put {
                key: b"a",
                value: b"uno-corregido",
            },
        ])
        .unwrap();
    assert_eq!(n, 5, "el borrado de una clave ausente no escribe registro");

    assert_eq!(
        db.get(b"a").unwrap().as_deref(),
        Some(&b"uno-corregido"[..]),
        "manda la última operación del lote"
    );
    assert_eq!(db.get(b"b").unwrap().as_deref(), Some(&b"dos"[..]));
    assert_eq!(db.get(b"c").unwrap().as_deref(), Some(&b"tres"[..]));
    assert!(db.ttl(b"c").unwrap().is_some(), "la caducidad del lote");
    assert_eq!(db.get(b"previa").unwrap(), None);

    let st = db.stats();
    assert_eq!(st.keys, 3);
    // Lo que justifica el lote: cinco registros y **un** fsync.
    let fsyncs = st.fsyncs - fsyncs_antes;
    assert!(
        fsyncs <= 2,
        "cinco escrituras deberían costar un fsync (o dos con el de ronda), no {fsyncs}"
    );
    db.close().unwrap();
}

/// Un lote con una operación inválida no se aplica a medias.
#[test]
fn un_lote_con_una_operacion_invalida_no_escribe_nada() {
    let tmp = TempDir::new("lote-invalido");
    let db = Db::open(opts(tmp.path())).unwrap();
    let r = db.write_batch(&[
        Op::Put {
            key: b"buena",
            value: b"v",
        },
        Op::Put {
            key: b"", // clave vacía: inválida
            value: b"v",
        },
        Op::Put {
            key: b"otra",
            value: b"v",
        },
    ]);
    assert!(matches!(r, Err(Error::Invalida(_))), "{r:?}");
    assert_eq!(db.len(), 0, "no debería haberse escrito NADA");
    assert!(!db.exists(b"buena"));
    assert_eq!(common::disk_bytes(tmp.path()), 0, "el log sigue vacío");

    // Y un lote vacío es un no-op, no un error.
    assert_eq!(db.write_batch(&[]).unwrap(), 0);
    db.close().unwrap();
}

#[test]
fn un_lote_sobrevive_al_reinicio() {
    let tmp = TempDir::new("lote-reinicio");
    {
        let db = Db::open(opts(tmp.path())).unwrap();
        let ops: Vec<Op> = (0..500)
            .map(|_| Op::Put {
                key: b"x",
                value: b"y",
            })
            .collect();
        assert_eq!(db.write_batch(&ops).unwrap(), 500);
        db.put(b"ultima", b"z").unwrap();
        db.close().unwrap();
    }
    let db = Db::open(opts(tmp.path())).unwrap();
    assert_eq!(db.get(b"x").unwrap().as_deref(), Some(&b"y"[..]));
    assert_eq!(db.get(b"ultima").unwrap().as_deref(), Some(&b"z"[..]));
    assert_eq!(db.len(), 2);
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// Compare-and-swap
// ---------------------------------------------------------------------------

#[test]
fn el_cas_cubre_crear_cambiar_y_borrar_condicional() {
    let tmp = TempDir::new("cas");
    let db = Db::open(opts(tmp.path())).unwrap();

    // Crear si no existe.
    assert!(db.compare_and_swap(b"k", None, Some(b"primero")).unwrap());
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"primero"[..]));
    // Otra vez no: ya existe.
    assert!(!db.compare_and_swap(b"k", None, Some(b"otro")).unwrap());
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"primero"[..]));

    // Cambiar si vale lo que creo.
    assert!(db
        .compare_and_swap(b"k", Some(b"primero"), Some(b"segundo"))
        .unwrap());
    assert!(!db
        .compare_and_swap(b"k", Some(b"primero"), Some(b"tercero"))
        .unwrap());
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"segundo"[..]));

    // Borrar si vale lo que creo.
    assert!(!db
        .compare_and_swap(b"k", Some(b"nada que ver"), None)
        .unwrap());
    assert!(db.compare_and_swap(b"k", Some(b"segundo"), None).unwrap());
    assert!(!db.exists(b"k"));
    // Borrar lo que ya no está, pidiendo que no esté: es un éxito.
    assert!(db.compare_and_swap(b"k", None, None).unwrap());

    // Una clave caducada cuenta como ausente.
    db.put_ttl(b"ef", b"v", Duration::from_millis(80)).unwrap();
    assert!(!db.compare_and_swap(b"ef", None, Some(b"x")).unwrap());
    thread::sleep(Duration::from_millis(150));
    assert!(
        db.compare_and_swap(b"ef", None, Some(b"x")).unwrap(),
        "caducada = ausente"
    );
    db.close().unwrap();
}

#[test]
fn el_cas_por_secuencia_distingue_lo_que_el_cas_por_valor_no_puede() {
    let tmp = TempDir::new("cas-seq");
    let db = Db::open(opts(tmp.path())).unwrap();
    db.put(b"k", b"A").unwrap();
    let (seq, v) = db.get_seq(b"k").unwrap().unwrap();
    assert_eq!(v, b"A");
    assert_eq!(db.seq(b"k"), Some(seq));

    // Alguien lo cambia y lo vuelve a dejar igual: el valor es el mismo, pero
    // la clave SÍ se ha tocado. El CAS por valor no lo nota; el de secuencia sí.
    db.put(b"k", b"B").unwrap();
    db.put(b"k", b"A").unwrap();
    assert!(
        db.compare_and_swap(b"k", Some(b"A"), Some(b"C")).unwrap(),
        "el CAS por valor no puede distinguirlo, y no es un fallo suyo"
    );
    db.put(b"k", b"A").unwrap();
    assert!(
        !db.compare_and_swap_seq(b"k", Some(seq), Some(b"C"))
            .unwrap(),
        "el CAS por secuencia sí tiene que fallar: la clave cambió"
    );
    let ahora = db.seq(b"k").unwrap();
    assert!(db
        .compare_and_swap_seq(b"k", Some(ahora), Some(b"C"))
        .unwrap());
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"C"[..]));

    // Y con `None` exige que no exista.
    assert!(!db.compare_and_swap_seq(b"k", None, Some(b"D")).unwrap());
    assert!(db.compare_and_swap_seq(b"nueva", None, Some(b"D")).unwrap());
    db.close().unwrap();
}

/// Ocho hilos incrementando un contador con CAS. Si el CAS no fuera atómico,
/// se perderían incrementos y el valor final no cuadraría con los éxitos.
#[test]
fn un_cas_no_se_lo_lleva_mas_de_uno() {
    let tmp = TempDir::new("cas-carrera");
    let db = Arc::new(Db::open(opts(tmp.path())).unwrap());
    db.put(b"contador", b"0").unwrap();

    const HILOS: u64 = 8;
    const POR_HILO: u64 = 60;
    let exitos = Arc::new(AtomicU64::new(0));
    let intentos = Arc::new(AtomicU64::new(0));

    let hs: Vec<_> = (0..HILOS)
        .map(|_| {
            let db = Arc::clone(&db);
            let exitos = Arc::clone(&exitos);
            let intentos = Arc::clone(&intentos);
            thread::spawn(move || {
                for _ in 0..POR_HILO {
                    // Bucle de concurrencia optimista: leer, calcular, CAS.
                    loop {
                        intentos.fetch_add(1, Ordering::Relaxed);
                        let actual = db.get(b"contador").unwrap().unwrap();
                        let n: u64 = String::from_utf8(actual.clone()).unwrap().parse().unwrap();
                        let nuevo = (n + 1).to_string();
                        if db
                            .compare_and_swap(b"contador", Some(&actual), Some(nuevo.as_bytes()))
                            .unwrap()
                        {
                            exitos.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }

    let valor = db.get(b"contador").unwrap().unwrap();
    let n: u64 = String::from_utf8(valor).unwrap().parse().unwrap();
    assert_eq!(
        n,
        HILOS * POR_HILO,
        "se han perdido incrementos: el CAS deja pasar a dos a la vez"
    );
    assert_eq!(exitos.load(Ordering::Relaxed), HILOS * POR_HILO);
    // Si no hubiera habido ninguna colisión, la prueba no estaría probando
    // nada. Con 8 hilos sobre una clave tiene que haber reintentos.
    let i = intentos.load(Ordering::Relaxed);
    assert!(
        i > HILOS * POR_HILO,
        "sin colisiones ({i} intentos para {} incrementos) la prueba no vale",
        HILOS * POR_HILO
    );
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// Namespaces
// ---------------------------------------------------------------------------

#[test]
fn un_namespace_es_un_prefijo_y_se_ve_desde_fuera() {
    let tmp = TempDir::new("ns");
    let db = Db::open(opts(tmp.path())).unwrap();
    let app = db.namespace(b"app").unwrap();
    let otra = db.namespace(b"otra").unwrap();

    app.put(b"k", b"de app").unwrap();
    otra.put(b"k", b"de otra").unwrap();
    db.put(b"k", b"sin namespace").unwrap();

    assert_eq!(app.get(b"k").unwrap().as_deref(), Some(&b"de app"[..]));
    assert_eq!(otra.get(b"k").unwrap().as_deref(), Some(&b"de otra"[..]));
    assert_eq!(
        db.get(b"k").unwrap().as_deref(),
        Some(&b"sin namespace"[..]),
        "la clave sin prefijo es otra clave distinta"
    );
    // Y la clave real es la que uno espera: esto NO es aislamiento.
    assert_eq!(db.get(b"app:k").unwrap().as_deref(), Some(&b"de app"[..]));
    assert_eq!(app.prefijo(), b"app:");

    // `keys` devuelve las claves sin el prefijo.
    app.put(b"otra-clave", b"v").unwrap();
    let (ks, _) = app.keys(b"", 100);
    assert_eq!(ks, vec![b"k".to_vec(), b"otra-clave".to_vec()]);
    assert_eq!(app.len(), 2);
    assert_eq!(otra.len(), 1);
    assert_eq!(db.len(), 4, "la base los cuenta todos juntos");

    assert!(app.delete(b"k").unwrap());
    assert!(!app.exists(b"k"));
    assert!(otra.exists(b"k"), "borrar en uno no toca el otro");
    db.close().unwrap();
}

/// El namespace con ':' se rechaza, y no por purismo: dos namespaces distintos
/// acabarían escribiendo en la misma clave real.
#[test]
fn un_nombre_de_namespace_con_el_separador_se_rechaza() {
    let tmp = TempDir::new("ns-malo");
    let db = Db::open(opts(tmp.path())).unwrap();
    assert!(db.namespace(b"").is_err(), "vacío");
    match db.namespace(b"a:b") {
        Err(Error::Invalida(m)) => assert!(m.contains("misma clave real"), "{m}"),
        Err(otro) => panic!("rechazado, pero por el motivo equivocado: {otro}"),
        Ok(_) => panic!("un nombre con ':' tenía que rechazarse"),
    }
    // La colisión que esto evita: ns "a" + clave "b:c" sería "a:b:c", igual que
    // ns "a:b" + clave "c".
    let a = db.namespace(b"a").unwrap();
    a.put(b"b:c", b"v").unwrap();
    assert_eq!(db.get(b"a:b:c").unwrap().as_deref(), Some(&b"v"[..]));
    db.close().unwrap();
}

#[test]
fn un_namespace_tambien_tiene_caducidad_y_cas() {
    let tmp = TempDir::new("ns-todo");
    let db = Db::open(opts(tmp.path())).unwrap();
    let ns = db.namespace(b"s").unwrap();

    ns.put_ttl(b"tmp", b"v", Duration::from_millis(100))
        .unwrap();
    assert!(ns.ttl(b"tmp").unwrap().is_some());
    assert!(ns.compare_and_swap(b"c", None, Some(b"uno")).unwrap());
    assert!(!ns.compare_and_swap(b"c", None, Some(b"dos")).unwrap());

    thread::sleep(Duration::from_millis(150));
    assert_eq!(ns.get(b"tmp").unwrap(), None);
    assert_eq!(ns.len(), 1);
    db.close().unwrap();
}

// ---------------------------------------------------------------------------
// Todo junto, y que siga abriendo en estricto
// ---------------------------------------------------------------------------

/// El formato nuevo tiene que poder leerse en modo **estricto**: si los
/// registros con caducidad no fueran legibles del todo, esto fallaría.
#[test]
fn un_log_con_caducidad_lote_y_cas_abre_en_modo_estricto() {
    let tmp = TempDir::new("todo-junto");
    {
        let db = Db::open(opts(tmp.path())).unwrap();
        let ns = db.namespace(b"app").unwrap();
        for i in 0..100u32 {
            ns.put(format!("k{i}").as_bytes(), b"normal").unwrap();
            ns.put_ttl(
                format!("t{i}").as_bytes(),
                b"con caducidad",
                Duration::from_secs(600),
            )
            .unwrap();
        }
        db.write_batch(&[
            Op::Put {
                key: b"lote1",
                value: b"a",
            },
            Op::PutTtl {
                key: b"lote2",
                value: b"b",
                ttl: Duration::from_secs(600),
            },
            Op::Delete { key: b"app:k0" },
        ])
        .unwrap();
        db.compare_and_swap(b"cas", None, Some(b"c")).unwrap();
        db.close().unwrap();
    }
    let mut o = opts(tmp.path());
    o.recovery = RecoveryPolicy::Estricto;
    let db = Db::open(o).unwrap();
    assert!(
        !db.recovery_report().con_perdidas(),
        "nada debería haberse perdido: {:?}",
        db.recovery_report().detalle
    );
    assert_eq!(db.len(), 1 + 1 + 1 + 99 + 100, "{}", db.len());
    let ns = db.namespace(b"app").unwrap();
    assert_eq!(ns.get(b"k0").unwrap(), None, "el lote lo borró");
    assert_eq!(ns.get(b"k1").unwrap().as_deref(), Some(&b"normal"[..]));
    assert_eq!(
        ns.get(b"t1").unwrap().as_deref(),
        Some(&b"con caducidad"[..])
    );
    assert_eq!(db.get(b"cas").unwrap().as_deref(), Some(&b"c"[..]));
    assert_eq!(db.health().estado, bitacora::Estado::Ok);
    db.close().unwrap();
}
