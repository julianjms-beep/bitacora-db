# Auditoría técnica de BITÁCORA

**Fecha:** 8 de noviembre de 2026 · **Versión auditada:** `main`, commit de la licencia MIT
**Alcance:** las 2.101 líneas de `src/` y `tests/` al completo.

## Método y evidencia

Lo que se ha hecho realmente, para que cada afirmación de este informe se pueda comprobar:

* Lectura completa de `store.rs` (891 líneas), `wal.rs` (208), `server.rs` (142),
  `protocol.rs` (107), `crc32.rs` (84), `main.rs` (276) y las tres suites de pruebas.
* `cargo test --release`: **17 pruebas, 17 en verde, 0 fallos.**
* `cargo clippy --all-targets`: **14 avisos**, todos cosméticos (auto-deref, `io::Error::other`,
  `is_multiple_of`, `map_or`). Ninguno de corrección.
* `cargo fmt --check`: **36 diferencias** de formato.
* `grep unsafe`: **un solo bloque**, en `sync_fd`, correcto y con comentario `SAFETY`.
* Un experimento de corrupción ejecutado contra el binario de release (ver C1).

Lo que **no** se ha hecho y por tanto no se afirma: no se ha ejecutado Miri, ni `loom`, ni
fuzzing, ni pruebas en Linux (todo lo medido es macOS/arm64), ni se han reproducido los
benchmarks que publica el README.

## Estado a 8 de noviembre de 2026

Resueltos en los gates 0 a 3: **C1, C2, C3, A1, A2, A3, A4, A5, A6, A7, M1, M2,
M3, M4, M5, M6 y M8**, más una fuga de conexiones que no estaba en este informe
(un hilo que entraba en panic nunca decrementaba el contador). Pruebas: 17 al
auditar, **55** ahora.

Pendientes: **M7** (`KEYS`, `len()` y `stats()` no son instantáneas coherentes;
documentado, no arreglado), **M9** (CRC32 no detecta manipulación; documentado
como límite de seguridad), **B1**, **B2** y **B3** (sin CI, sin property-based,
sin fuzzing, 0 doc-tests). Los límites de recuperación que el formato actual no
permite salvar están en `docs/RECUPERACION.md`.

## Resumen

| Criticidad | Nº | Naturaleza |
|---|---|---|
| Crítico | 3 | Pérdida de datos silenciosa y permanente |
| Alto | 7 | Denegación de servicio, caída del servidor, pérdida en caminos de error |
| Medio | 9 | Robustez, límites de recursos, seguridad, API |
| Bajo | 3 | Documentación y utillaje |

**Veredicto:** el núcleo de almacenamiento está bien diseñado y el camino feliz es sólido. No
está listo para producción por tres motivos concretos (C1–C3) y porque el servidor TCP carece de
todas las defensas que necesita un proceso expuesto a la red.

---

# CRÍTICO

## C1 · Un bit corrupto en medio de un segmento destruye todos los registros posteriores, y los borra del disco

**Dónde:** `wal.rs:87-136` (`SegmentReader::next` devuelve `Torn` ante cualquier fallo) y
`store.rs:317-325` (`open` llama a `file.set_len(at)` y `sync_all`).

**Qué pasa:** el lector trata el primer registro que no cuadra como «fin útil del fichero» y
`open()` **recorta el fichero en ese byte**. Es la decisión correcta para una escritura a medias
al final del fichero (un apagón), y catastrófica para una corrupción en el medio, donde lo que
viene detrás son registros válidos y ya confirmados al cliente.

**Evidencia medida.** 50 claves escritas con `--fsync always`; un único bit volteado al 40% del
fichero; reapertura:

```
antes:   keys 50   ·  fichero 2.530 bytes
después: keys 20   ·  fichero 1.000 bytes
k0  → VAL valor-de-la-clave-numero-0
k49 → NIL
```

**30 de 50 claves perdidas (60%) por un bit**, y el fichero truncado, así que no queda forma de
rescatarlas después. La base de datos abre con normalidad y declara 20 claves como si fuese la
verdad: el único aviso es una línea en `stderr`.

**La prueba existente da confianza falsa.** `tests/recovery.rs:79-98`
(`un_bit_corrupto_descarta_solo_ese_registro`) corrompe un byte del **último** registro
(`idx = len - last + 2`), y por eso pasa. Su nombre y la afirmación del README («a corrupt bit
discarding only that record») describen un comportamiento que el motor no tiene.

**Tratado en el Gate 0.** Ver `docs/RECUPERACION.md`.

## C2 · La marca de durabilidad avanza aunque el `fsync` haya fallado

**Dónde:** `store.rs:880-886`.

```rust
if last > synced {
    if let Err(e) = inner.sync_file(&file) {
        *inner.sync_error.lock().unwrap() = Some(e.to_string());
    }
    inner.mark_synced(last);          // ← se ejecuta también si el fsync falló
}
```

**Qué pasa:** `synced` es la promesa «esto está en disco», y se adelanta incluso cuando el
`fsync` ha devuelto error. Hoy el daño queda tapado porque `wait_synced` consulta `sync_error`
después de despertar y ese error **nunca se limpia**, de modo que todo escritor posterior también
falla. Es decir: el comportamiento es fail-closed por accidente, no por diseño, y aun así tiene
dos consecuencias malas:

* Un único `EIO` transitorio deja la base de datos devolviendo error para siempre, sin más vía de
  recuperación que reiniciar.
* La marca de durabilidad es mentira. Cualquier cambio futuro en el manejo de ese error —
  limpiarlo, o un camino que no lo consulte — convierte esto en pérdida silenciosa de datos ya
  confirmados.

**RESUELTO en el Gate 1.** El estado de durabilidad pasa a vivir entero bajo un único candado
(`SyncState { seq, failed }`), lo que evita la inversión de orden que tendría un candado aparte
frente a `wait_synced`. `mark_synced` solo se llama cuando el `fsync` terminó bien; ante fallo,
`mark_failed` despierta a los que esperaban —si no, el arreglo ingenuo los cuelga para siempre—
y la base queda no escribible hasta reiniciar. Seis pruebas en `tests/durabilidad.rs`, con
inyección de fallos vía `Options::fsync_falla_tras`. Ver `docs/RECUPERACION.md`.

## C3 · No se sincroniza el directorio al crear ni al borrar segmentos

**Dónde:** `open_segment` (`store.rs:215-221`), `roll` (`671-683`), `new_out` (`707-717`),
`compact_inner` (`820-822`).

**Qué pasa:** en POSIX, crear o borrar un fichero no es durable hasta que se hace `fsync` del
**directorio** que lo contiene. El motor sincroniza el contenido de los segmentos nuevos pero
nunca el directorio. Secuencia de pérdida total, realista:

1. La compactación escribe los segmentos nuevos y los sincroniza (bien).
2. Borra los viejos con `fs::remove_file`.
3. Apagón antes de que el sistema de ficheros haga durable la entrada de directorio.
4. Al arrancar: los viejos ya no están y los nuevos no existen para el directorio.

**RESUELTO en el Gate 1.** `sync_dir` tras crear cada segmento (`open`, `roll`, `new_out`), tras
copiar un segmento a cuarentena, y en la compactación antes y después de borrar los viejos. No es
verificable por prueba —un `fsync` no se observa sin cortar la corriente de verdad— y así está
anotado en `docs/RECUPERACION.md`, límite 11.

---

# ALTO

## A1 · El deshacer de una escritura parcial puede dejar basura en medio del log

`store.rs:429-434` y `515-518`: si `write_all` falla se intenta `file.set_len(start)` y **se
ignora el resultado** (`let _ =`). Si ese `set_len` también falla (disco lleno, error de E/S),
queda un registro a medias en medio del segmento activo y la siguiente escritura se añade detrás.
Al reabrir se dispara el mecanismo de C1. Además `w.len` no se actualiza en el camino de error,
así que la longitud en memoria y la del fichero se separan.
**Arreglo:** si el deshacer falla, no seguir escribiendo en ese segmento: marcar la base de datos
como no escribible y propagar un error distinguible.

## A2 · Sin tiempos de espera en las conexiones: denegación de servicio trivial

`server.rs:60-87`: no hay `set_read_timeout` ni `set_write_timeout`. Un cliente que abre el socket
y no envía nada retiene un hilo indefinidamente. Con `--max-conns` (1024 por defecto), 1024
conexiones mudas dejan el servidor inaccesible sin enviar un solo byte útil. Lo mismo al escribir:
un cliente que no lee bloquea al servidor dentro de `write_all`.
**Arreglo:** tiempos de espera de lectura y escritura configurables, y cierre de inactivas.

## A3 · `KEYS` sin prefijo no tiene límite

`store.rs:538-550` recorre los 64 shards, copia **todas** las claves a un `Vec` y lo ordena;
`server.rs:111-118` las escribe todas. Con 200.000 claves son 200.000 elementos, un `sort` y una
respuesta de megabytes, por cada cliente que lo pida.
**Arreglo:** límite obligatorio con paginación por cursor y un tope configurable.

## A4 · Si falla crear un hilo, muere el servidor entero

`server.rs:43-55`: `thread::Builder::spawn(...)?` propaga el error y **termina `serve()`**. Bajo
agotamiento de hilos (`EAGAIN`), en vez de rechazar esa conexión se cae el proceso. Y `conns` ya
se había incrementado, así que el contador queda sobrestimado.
**Arreglo:** tratar el fallo de `spawn` como rechazo de esa conexión, nunca como error fatal.

## A5 · Carrera en el límite de conexiones

`server.rs:34-40`: `conns.load()` y luego `conns.fetch_add()` no son atómicos. Varias conexiones
simultáneas pueden pasar la comprobación a la vez y superar `max_conns`.
**Arreglo:** `fetch_update` o CAS; mejor, un semáforo.

## A6 · Candados envenenados convierten un panic en una caída total

Cada `lock().unwrap()` del motor entra en panic si otro hilo entró en panic sosteniendo ese
candado. En un servidor eso transforma un fallo aislado en un proceso inservible que sigue
aceptando conexiones y responde a todas con panic.
**Arreglo:** un accesor que recupere el candado envenenado (`unwrap_or_else(|e| e.into_inner())`)
junto con una marca de «estado dudoso», o capturar el panic en el borde de cada conexión.

## A7 · Un valor con `\n` rompe el protocolo, y el README afirma lo contrario

El README dice que los valores no pueden llevar saltos de línea por el cable pero que «la API de
Rust es binaria y no tiene ese límite». Las dos cosas no son compatibles: un valor con `\n`
escrito por la API y leído por TCP produce `VAL primera\nsegunda\n`, que el cliente lee como dos
respuestas. La prueba `claves_y_valores_binarios_y_limites` cubre la API, no el protocolo.
**Arreglo:** marco con longitud explícita (`VAL <n>\r\n<bytes>`) o rechazo en el protocolo de los
valores con `\n`; y corregir el README.

---

# MEDIO

| ID | Problema | Dónde |
|---|---|---|
| M1 | `get()` puede devolver error «segmento desaparecido» tras 32 intentos aunque el dato exista; es espera activa con `yield_now` | `store.rs:465-485` |
| M2 | `live`/`dead` con `Relaxed` y `fetch_sub` sin protección: puede envolverse a 18 exabytes, y además decide la auto-compactación | `store.rs:449-454, 527-529, 685-696` |
| M3 | Sin apagado ordenado: `serve` no atiende señales, así que SIGINT/SIGTERM no ejecutan `close()` ni el `fsync` final | `server.rs:26`, `main.rs` |
| M4 | `vec![0u8; vlen]` admite hasta 1 GiB por registro, en recuperación y en cada `get`, sin tope global de memoria | `wal.rs:112-118`, `store.rs:479` |
| M5 | Sin autenticación: con `--host 0.0.0.0` cualquiera tiene lectura, escritura y `COMPACT` | `server.rs:19-24` |
| M6 | Errores sin tipar: todo es `io::Error`, varios con `ErrorKind::Other` y texto; el cliente no puede distinguir validación de fallo de durabilidad | todo el motor |
| M7 | `KEYS`, `len()` y `stats()` toman los shards de uno en uno: no son instantáneas coherentes, y no está documentado | `store.rs:538-583` |
| M8 | Límite de valor inconsistente: el escritor rechaza `>= 1 GiB`, el lector acepta `== 1 GiB` | `store.rs:231` vs `wal.rs:109` |
| M9 | CRC32 detecta corrupción accidental, no manipulación: quien pueda escribir en los ficheros puede falsificar registros. No documentado como límite de seguridad | `crc32.rs`, `wal.rs` |

---

# BAJO

* **B1** · Un registro mayor que `max_segment_bytes` crea un segmento que lo excede
  (`store.rs:419` exige `w.len > 0`). Correcto, pero no documentado.
* **B2** · Una compactación abortada por cierre deja un segmento de salida con duplicados que
  cuentan como bytes muertos hasta la siguiente compactación (`store.rs:750-757`).
* **B3** · Sin CI, sin clippy ni rustfmt en el repositorio (14 avisos y 36 ficheros sin formatear
  al auditar), sin documentación de API publicada y con 0 doc-tests.

---

# Lo que está bien

Para calibrar: esto no es un prototipo. Lo siguiente se ha revisado y es correcto.

* **El formato del WAL.** El CRC cubre todo lo que va detrás, la cabecera es de tamaño fijo y el
  `seq` global permite reconstruir el estado sin depender del orden de los ficheros — que es
  precisamente lo que permite que la compactación escriba en segmentos nuevos sin coordinación.
* **El orden de las operaciones en `put`.** Escribir el registro y actualizar el índice dentro del
  mismo candado garantiza que el orden del índice y el del log coinciden. Está probado.
* **El group commit.** El `fsync` se hace fuera del candado de escritura y no hay despertares
  perdidos: un escritor que marca `sync_req` justo después de que el hilo de sincronización lo
  haya limpiado provoca otra ronda.
* **La compactación concurrente.** El compare-and-swap manual contra `cur` antes de mover el
  puntero es correcto, y el orden «primero fuera del mapa, luego del disco» mantiene válidos a los
  lectores que ya tenían el descriptor.
* **El único `unsafe`** está justificado (elegir entre `fsync` y `F_FULLFSYNC` en macOS, que la
  librería estándar no deja distinguir), es correcto y lleva su comentario `SAFETY`.
* **La prueba del `kill -9`** es de verdad: lanza el binario, lo mata tres veces y exige que las
  claves recuperadas sean un prefijo exacto. Eso es más de lo que hacen muchos proyectos.

---

# El conflicto que hay que resolver antes de implementar

El titular del proyecto es **«zero external dependencies»**, y varias cosas pedidas son
dependencias:

| Lo que se pide | Dependencia habitual | Veredicto |
|---|---|---|
| Property-based testing | `proptest` / `quickcheck` | **Sí**, como `dev-dependency`: no afecta a quien use la librería |
| Fuzzing | `cargo-fuzz` + `arbitrary` | **Sí**, en un sub-crate aparte (`fuzz/`), fuera del grafo de la librería |
| Logging estructurado | `tracing` / `log` | **No**: rompería el titular. Emisor propio de líneas JSON (~80 líneas) |
| Métricas Prometheus | `prometheus` | **No**: el formato de texto se genera a mano desde `stats()` |
| Pruebas de concurrencia | `loom` | **Sí**, como `dev-dependency` |
| Comparar con Redis | — | Exige instalar Redis solo para medir; no entra en el repositorio |

Recomendación: mantener **cero dependencias de ejecución** como invariante del proyecto y
permitirlas solo en `dev-dependencies` y en el sub-crate de fuzzing.

## Sobre las funcionalidades pedidas

Encajan con la arquitectura y son baratas: **operaciones batch** (un solo `fsync` por lote: el
group commit ya está hecho), **compare-and-swap** (el índice ya está por shards con su candado),
**TTL por clave** (8 bytes más en el registro y filtrado al leer; la compactación ya descarta lo
muerto) y **namespaces** (prefijo de clave, sin cambios en el motor).

No encajan sin romper el diseño: **transacciones atómicas multi-clave** (exigen un registro de
commit y cambiar el formato del WAL) y **snapshots** (exigen contar referencias a los segmentos
para que la compactación no borre lo que un snapshot usa). Son un proyecto en sí mismas y van
después de C1–C3, no antes.

---

# Plan por fases

Cada fase acaba con pruebas en verde y se puede parar ahí.

| Fase | Contenido | Por qué en este orden |
|---|---|---|
| **0** | C1 + pruebas de corrupción en todas las posiciones | Es la pérdida de datos más grave y la más fácil de provocar |
| **1** | C2, C3 | Las otras dos vías de pérdida de datos |
| **2** | A1, A6, M1, M2, M8 y errores tipados (M6) | Robustez del motor y una API de errores estable antes de añadir nada |
| **3** | A2, A3, A4, A5, A7, M3, M4, M5 | Endurecer el servidor: esperas, límites, apagado ordenado, autenticación |
| **4** | B3: CI con tests, clippy y rustfmt; property-based; fuzzing del lector; inyección de fallos de E/S | Red de seguridad antes de tocar el rendimiento |
| **5** | Benchmarks reproducibles con p50/p95/p99 y RAM; medición durante compactación y al recuperar | Medir antes de optimizar |
| **6** | Observabilidad: líneas JSON, métricas, `HEALTH`, documentación operativa, copia y restauración | Lo necesario para operarlo |
| **7** | Batch, CAS, TTL, namespaces | Funcionalidad, solo sobre una base sólida |
