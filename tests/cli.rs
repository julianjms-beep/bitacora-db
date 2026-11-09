//! Gate 6 — las órdenes que ejecuta quien opera esto, probadas sobre el
//! binario de verdad.
//!
//! Lo que se comprueba no es solo que impriman algo, sino **los códigos de
//! salida**: `bitacora health` está pensado para ser el `healthcheck` de un
//! contenedor o de systemd, y `verificar` para decidir si una copia sirve. Una
//! orden que devuelve 0 cuando debía devolver 1 convierte la vigilancia en
//! decoración.

mod common;

use common::TempDir;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn bitacora(args: &[&str]) -> (i32, String, String) {
    let s = Command::new(env!("CARGO_BIN_EXE_bitacora"))
        .args(args)
        .output()
        .expect("ejecutar bitacora");
    (
        s.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&s.stdout).into_owned(),
        String::from_utf8_lossy(&s.stderr).into_owned(),
    )
}

/// Crea una base de datos usando el propio binario y la cierra. Se espera a que
/// haya escrito de verdad, en vez de confiar en un plazo fijo.
fn siembra(dir: &Path, bytes_minimos: u64) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_bitacora"))
        .args(["__crashwrite", dir.to_str().unwrap(), "group", "0"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("lanzar escritor");
    let limite = Instant::now() + Duration::from_secs(30);
    while Instant::now() < limite {
        if common::disk_bytes(dir) > bytes_minimos {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        common::disk_bytes(dir) > bytes_minimos,
        "el escritor no llegó a escribir {bytes_minimos} bytes"
    );
}

#[test]
fn verificar_dice_la_verdad_y_con_el_codigo_de_salida_correcto() {
    let tmp = TempDir::new("cli-verificar");
    siembra(tmp.path(), 2000);

    let (cod, out, err) = bitacora(&["verificar", "--dir", tmp.path().to_str().unwrap()]);
    assert_eq!(cod, 0, "salida {cod}; stderr: {err}");
    assert!(out.contains("INTEGRO: si"), "{out}");
    assert!(out.contains("estado="), "{out}");

    // Ahora se corrompe un byte en medio del primer segmento: `verificar` tiene
    // que decir que no, y con salida distinta de 0.
    let (ruta, len) = common::segments(tmp.path()).remove(0);
    assert!(len > 100);
    let mut datos = std::fs::read(&ruta).unwrap();
    let medio = datos.len() / 2;
    datos[medio] ^= 0xFF;
    std::fs::write(&ruta, &datos).unwrap();

    let (cod, out, _) = bitacora(&["verificar", "--dir", tmp.path().to_str().unwrap()]);
    assert_eq!(cod, 1, "una copia corrupta no puede dar salida 0: {out}");
    assert!(out.contains("INTEGRO: no"), "{out}");
}

#[test]
fn copiar_restaurar_y_verificar_de_punta_a_punta() {
    let origen = TempDir::new("cli-origen");
    let copia = TempDir::new("cli-copia");
    let destino = TempDir::new("cli-destino");
    siembra(origen.path(), 2000);

    let dir_copia = copia.path().join("hecha");
    let (cod, out, err) = bitacora(&[
        "backup",
        "--dir",
        origen.path().to_str().unwrap(),
        "--a",
        dir_copia.to_str().unwrap(),
    ]);
    assert_eq!(cod, 0, "stderr: {err}");
    assert!(out.contains("copia hecha"), "{out}");
    assert!(!common::segments(&dir_copia).is_empty());

    // El fichero de bloqueo no se copia: no es un dato.
    assert!(
        !dir_copia.join("LOCK").exists(),
        "el LOCK no debería estar en la copia"
    );

    let dir_destino = destino.path().join("restaurado");
    let (cod, out, err) = bitacora(&[
        "restore",
        "--de",
        dir_copia.to_str().unwrap(),
        "--a",
        dir_destino.to_str().unwrap(),
    ]);
    assert_eq!(cod, 0, "stderr: {err}");
    assert!(out.contains("restaurados"), "{out}");

    let (cod, out, _) = bitacora(&["verificar", "--dir", dir_destino.to_str().unwrap()]);
    assert_eq!(cod, 0);
    assert!(out.contains("INTEGRO: si"), "{out}");

    // Y restaurar encima se niega, con explicación.
    let (cod, _, err) = bitacora(&[
        "restore",
        "--de",
        dir_copia.to_str().unwrap(),
        "--a",
        dir_destino.to_str().unwrap(),
    ]);
    assert_eq!(cod, 1);
    assert!(err.contains("resucitar"), "{err}");
}

#[test]
fn restore_rechaza_un_origen_que_no_es_una_copia() {
    let vacio = TempDir::new("cli-vacio");
    let destino = TempDir::new("cli-vacio-dst");
    let (cod, _, err) = bitacora(&[
        "restore",
        "--de",
        vacio.path().to_str().unwrap(),
        "--a",
        destino.path().to_str().unwrap(),
    ]);
    assert_eq!(cod, 1);
    assert!(err.contains("no parece una copia"), "{err}");
}

#[test]
fn una_configuracion_absurda_no_arranca_el_servidor() {
    let tmp = TempDir::new("cli-config");
    // Medido: con 0 ms el hilo de sincronización se come un núcleo entero.
    let (cod, _, err) = bitacora(&[
        "serve",
        "--dir",
        tmp.path().to_str().unwrap(),
        "--port",
        "47931",
        "--group-ms",
        "0",
    ]);
    assert_eq!(cod, 1, "stderr: {err}");
    assert!(err.contains("group_interval"), "{err}");
}

/// Servidor lanzado como proceso, para probar `health` y `metrics` tal como los
/// usaría un operador.
struct Servidor(Child);

impl Servidor {
    fn nuevo(dir: &Path, puerto: u16) -> Servidor {
        let p = puerto.to_string();
        let child = Command::new(env!("CARGO_BIN_EXE_bitacora"))
            .args([
                "serve",
                "--dir",
                dir.to_str().unwrap(),
                "--port",
                &p,
                "--fsync",
                "never",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("lanzar servidor");
        let mut srv = Servidor(child);
        let limite = Instant::now() + Duration::from_secs(30);
        while Instant::now() < limite {
            if TcpStream::connect(("127.0.0.1", puerto)).is_ok() {
                return srv;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        // Envuelto en `Servidor` desde el principio para que, si esto se va por
        // el `panic`, el `Drop` se lleve el proceso hijo. Sin eso, una prueba
        // que falla deja un servidor suelto ocupando el puerto, y la siguiente
        // ejecución falla por un motivo que no tiene nada que ver.
        let _ = srv.0.kill();
        let _ = srv.0.wait();
        panic!("el servidor no llegó a escuchar en el puerto {puerto}");
    }
}

impl Drop for Servidor {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn health_y_metrics_contra_un_servidor_de_verdad() {
    let tmp = TempDir::new("cli-health");
    let puerto = 47932u16;
    let _srv = Servidor::nuevo(tmp.path(), puerto);
    let p = puerto.to_string();

    // Un par de claves, por el protocolo.
    let s = TcpStream::connect(("127.0.0.1", puerto)).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut w = s.try_clone().unwrap();
    let mut r = BufReader::new(s);
    w.write_all(b"SET a uno\nSET b dos\nQUIT\n").unwrap();
    for _ in 0..3 {
        let mut l = String::new();
        r.read_line(&mut l).unwrap();
    }
    let mut resto = String::new();
    let _ = r.read_to_string(&mut resto);

    let (cod, out, err) = bitacora(&["health", "--port", &p]);
    assert_eq!(cod, 0, "un servidor sano tiene que dar 0: {out} {err}");
    assert_eq!(out.trim(), "ok", "{out}");

    let (cod, out, err) = bitacora(&["metrics", "--port", &p]);
    assert_eq!(cod, 0, "stderr: {err}");
    let familias: Vec<&str> = out
        .lines()
        .filter(|l| l.starts_with("# TYPE "))
        .map(|l| l.split(' ').nth(2).unwrap())
        .collect();
    assert!(familias.len() > 20, "solo {} familias", familias.len());
    assert!(out.contains("bitacora_keys 2"), "{out}");
    // Las del motor y las del servidor, juntas.
    assert!(familias.contains(&"bitacora_connections"), "{familias:?}");
    assert!(familias.contains(&"bitacora_ops_total"), "{familias:?}");

    // Contra un puerto donde no hay nadie: salida 2, «no se pudo preguntar».
    // Un `healthcheck` tiene que distinguirlo de «está mal».
    let (cod, _, err) = bitacora(&["health", "--port", "47999"]);
    assert_eq!(cod, 2, "{err}");
    assert!(err.contains("no se pudo conectar"), "{err}");
}
