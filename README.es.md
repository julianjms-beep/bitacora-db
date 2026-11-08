# BITÁCORA · motor de base de datos clave-valor desde cero

Motor de almacenamiento clave-valor escrito en **Rust sin ninguna dependencia
externa** (solo la librería estándar). Objetivos de diseño:

| Objetivo | Cómo se resuelve | Dónde |
|---|---|---|
| Concurrencia a nivel de SO | Un hilo del sistema por conexión TCP; índice partido en 64 *shards* con `RwLock`; escritor único con `Mutex`; *group commit* con hilo de sincronización | `src/server.rs`, `src/store.rs` |
| No corromperse si se va la luz | WAL *append-only* con CRC-32 por registro; al arrancar se recorta la cola rota; `fsync` configurable | `src/wal.rs`, `src/store.rs` |
| Optimizar la RAM | Los valores viven en disco, en memoria solo `clave → (fichero, posición, longitud, seq)` (24 bytes + clave); lectura con `pread` sin copias intermedias; compactación que recicla espacio | `src/store.rs` |

Nombre: *bitácora* es la traducción natural de *log*, que es el corazón del diseño.

[Read this in English](README.md)

---

## Arrancar

Requisitos: Rust (`cargo`). Nada más.

```bash
git clone https://github.com/<usuario>/bitacora-db
cd bitacora-db
cargo build --release

# servidor (crea ./data)
./target/release/bitacora serve --dir data --port 7379 --fsync group

# en otra terminal: cliente propio…
./target/release/bitacora cli
# …o cualquier cliente de texto
nc 127.0.0.1 7379
```

Sesión de ejemplo con `nc`:

```
SET user:1 Ana Garcia      → OK
GET user:1                 → VAL Ana Garcia
EXISTS user:1              → INT 1
KEYS user:                 → KEYS 1  /  user:1
DEL user:1                 → OK
GET user:1                 → NIL
STATS                      → STATS 14  /  keys 0 / ... / synced_seq 3 / health ok
COMPACT                    → OK segmentos_retirados=1 bytes_antes=132 bytes_despues=68
PING                       → PONG
QUIT                       → BYE
```

Opciones de `serve`: `--dir`, `--host`, `--port`, `--fsync group|always|never`,
`--fullfsync`, `--segment-mb` (tamaño de cada fichero de log), `--max-conns`,
`--recuperacion estricto|reparar`.

Pruebas y benchmark:

```bash
cargo test                                   # 59 pruebas (unitarias + integración)
./target/release/bitacora bench --threads 8 --ops 200000 --fsync group
```

---

## Cómo funciona

### 1. El disco: un log que solo crece (WAL)

Toda operación se escribe **al final** de un fichero (`data/000001.log`,
`000002.log`, …). Nunca se modifica nada in situ. Un registro:

```
┌────────┬────────┬─────────┬─────────┬──────┬───────┬───────┐
│ crc32  │ seq    │ key_len │ val_len │ kind │ key   │ value │
│ 4 B    │ 8 B    │ 4 B     │ 4 B     │ 1 B  │ n B   │ m B   │
└────────┴────────┴─────────┴─────────┴──────┴───────┴───────┘
```

* `crc32` cubre todo lo que va detrás (implementación propia, `src/crc32.rs`).
* `seq` es un contador global y creciente. Permite reconstruir el estado **sin
  depender del orden de los ficheros**, y por eso la compactación puede escribir
  en ficheros nuevos sin romper nada.
* `kind` = PUT o DEL (una *lápida*: el borrado también es un registro, porque
  nunca borramos nada del log directamente).

**Qué pasa si se va la luz a mitad de una escritura.** El último registro queda
a medias. Al arrancar, el lector (`SegmentReader`) comprueba longitud y CRC de
cada registro; en cuanto uno no cuadra, considera que ahí acabó lo útil, recorta
el fichero en ese byte y sigue. Todo lo anterior se conserva intacto. La prueba
`sobrevive_a_un_kill_9_a_mitad_de_escritura` lanza el binario escribiendo a toda
velocidad, lo mata con `SIGKILL` tres veces y comprueba que las claves
recuperadas son exactamente un prefijo `k0..k(n-1)` con sus valores correctos:
ni huecos, ni basura.

### 2. La memoria: índice sin valores

```rust
struct Loc { file_id: u32, vlen: u32, vpos: u64, seq: u64 }   // 24 bytes
HashMap<Box<[u8]>, Loc>                                        // clave → Loc
```

Al leer, se busca la clave en el índice y se hace **un solo `pread`** del valor
en la posición exacta del fichero (`read_exact_at`): sin buscar en el fichero,
sin leer de más, sin candado sobre el fichero (cada hilo lee con su propia
posición). El SO cachea en su *page cache* lo que se usa a menudo, así que la
RAM del proceso crece con el número de claves y no con el tamaño de los datos.

Medido: 200.000 claves con valores de 100 B → **11,8 MB de índice** frente a
26 MB de datos; el reinicio reconstruye las 200.000 claves en **~130 ms**.

Detalles que ahorran memoria: claves como `Box<[u8]>` (sin capacidad sobrante
de `Vec`), un solo buffer reutilizado por escritor, `shrink_to_fit` tras la
recuperación, lectores TCP con buffer de 8 KB.

### 3. La concurrencia: hilos de verdad

* **Servidor**: `TcpListener` + `thread::spawn` por conexión. Son hilos del
  sistema operativo planificados por el kernel; no hay *runtime* async. Todas
  las conexiones comparten la misma `Db` mediante `Arc`.
* **Lecturas**: el índice está partido en 64 *shards*, cada uno con su
  `RwLock`. Dos lecturas de claves distintas casi nunca compiten por el mismo
  candado, y los lectores **nunca** tocan el candado de escritura.
* **Escrituras**: el log es secuencial por naturaleza, así que hay un único
  `Mutex<Writer>`. Dentro del candado: codificar, `write`, actualizar el índice.
  Mantener la actualización del índice dentro del mismo candado garantiza que
  el orden del índice coincide con el orden del log (la prueba
  `clave_caliente_memoria_y_log_coinciden` machaca una clave desde 8 hilos y
  comprueba que, tras reiniciar, la memoria y el log dicen lo mismo).
* **Group commit**: con `--fsync group`, un `put` escribe su registro, suelta
  el candado y espera en una `Condvar` a que un hilo de sincronización haga el
  `fsync`. Ese hilo sincroniza **una vez** y despierta a todos los que esperaban:
  cien escritores concurrentes comparten un solo `fsync`. El `fsync` se hace
  fuera del candado de escritura para no frenar a los demás.
* **Compactación concurrente**: reescribe solo los registros vivos de los
  segmentos cerrados a segmentos nuevos **mientras siguen entrando lecturas y
  escrituras**. Para cada registro comprueba que el índice aún apunta a él, lo
  copia y, bajo el candado del shard, mueve el puntero solo si nadie ha escrito
  esa clave entretanto (*compare-and-swap* manual). Los ficheros viejos se
  retiran primero del mapa y luego del disco: un lector que ya tuviera el
  descriptor sigue leyendo bien; uno que llegue después vuelve a mirar el índice.

### 4. Durabilidad: tres políticas

| `--fsync` | Qué garantiza | SET/s (8 hilos) | SET/s (64 hilos) |
|---|---|---|---|
| `never` | Sobrevive a que maten el proceso; un apagón pierde lo que el SO tenía en caché | 151.000 | — |
| `group` | Cada `OK` significa "está en disco"; los `fsync` se comparten | 48.000 | 69.000 |
| `always` | `fsync` dentro del candado en cada escritura | 27.000 | — |
| `group --fullfsync` | Además vacía la caché del propio disco (`F_FULLFSYNC`) | — | 5.700 |
| `always --fullfsync` | Lo más paranoico posible | 240 | — |

GET: ~1.000.000 ops/s en todos los modos (es lectura de índice + `pread`).
Medido en este Mac con valores de 100 B; `cargo build --release`.

Nota macOS: `fsync()` en macOS manda los datos al disco pero no obliga al disco
a vaciar su propia caché; para eso existe `F_FULLFSYNC`. La librería estándar de
Rust usa siempre `F_FULLFSYNC`, por eso el motor llama directamente a `fsync()`
del sistema y deja el completo detrás de `--fullfsync`. La fila de 64 hilos
muestra lo que aporta el group commit: 240 → 5.700 escrituras/s con la misma
garantía, porque cada `F_FULLFSYNC` (≈4 ms) sirve a ~24 escrituras a la vez.

### 5. Compactación

Las claves sobrescritas y las borradas dejan registros muertos en el log. Cuando
los bytes muertos superan el 50 % (y 16 MB) al cerrar un segmento, se lanza una
compactación en segundo plano; también se puede pedir con `COMPACT`. Las
lápidas se descartan al compactar porque siempre se compactan **todos** los
segmentos cerrados a la vez, así que ningún PUT antiguo puede "resucitar"
(prueba `borrados_sobreviven_a_compactacion_y_reinicio`).

---

## Estructura

```
src/crc32.rs     CRC-32 propio (tabla en tiempo de compilación)
src/wal.rs       formato de registro, codificador, lector con detección de cola rota
src/store.rs     el motor: índice por shards, segmentos, group commit, compactación
src/protocol.rs  parser del protocolo de texto
src/server.rs    TCP, un hilo por conexión
src/main.rs      serve / cli / bench
tests/           recuperación (kill -9, cola rota, bit corrupto) y concurrencia
```

## Corrupción y cola incompleta no son lo mismo

Un registro que no pasa las comprobaciones puede significar dos cosas muy
distintas, y confundirlas cuesta datos:

* **Cola incompleta**: el fichero acaba en un registro a medio escribir y **no
  hay nada válido detrás**. Es lo que deja un apagón, y recortar ahí no pierde
  nada que se hubiera confirmado.
* **Corrupción**: un registro no valida pero **sí hay registros válidos
  detrás**. Recortar aquí destruiría datos confirmados, así que el lector
  resincroniza y conserva todo lo posterior.

Hasta el 8 de noviembre de 2026 el motor no hacía esa distinción, y un solo bit
volteado al 40% de un segmento destruía 30 de 50 claves **y** recortaba el
fichero de 2.530 a 1.000 bytes. Ante corrupción hay ahora dos modos:

| `--recuperacion` | Comportamiento |
|---|---|
| `estricto` (por defecto) | **No abre y no modifica ni un byte.** El error dice fichero, desplazamiento, bytes y causa |
| `reparar` | Abre saltándose lo ilegible, tras copiar cada segmento afectado a `NNNNNN.log.corrupto-<epoch>`; informa de lo que se perdió |

`STATS` publica `recovery_corrupt_records`, `recovery_corrupt_bytes` y
`recovery_truncated_tails`, así que una recuperación con pérdidas nunca se
presenta como íntegra. Las garantías de cada operación y los diez límites
explícitos de lo que la recuperación **no** puede hacer están en
[docs/RECUPERACION.md](docs/RECUPERACION.md).

## Cuando falla el disco

Un `fsync` fallido **no se reintenta**: la base queda no escribible hasta
reiniciar, y las lecturas siguen funcionando. En Linux, un `fsync` que falla
puede haber descartado ya las páginas sucias, así que un segundo `fsync`
devuelve éxito sobre un dato que nunca llegó al disco; seguir aceptando
escrituras después de eso es prometer una durabilidad que no se puede cumplir.
PostgreSQL tomó la misma decisión en 2018.

En concreto: la marca de durabilidad **no** avanza (antes sí, y eso convertía
«está en disco» en una mentira), los escritores que esperaban el `fsync` en
grupo se despiertan y reciben el error en vez de colgarse, `STATS` publica
`health no-escribible (motivo)`, y `put`/`delete` fallan con un mensaje
explícito. El estado es del proceso, no de los ficheros: al reiniciar vuelve a
ser escribible con lo que de verdad llegó al disco.

Crear y borrar segmentos sincroniza ahora también el **directorio** que los
contiene: en POSIX el nombre de un fichero no es durable sin eso, así que un
apagón justo después de compactar podría dejar los segmentos nuevos sin entrada
de directorio y los viejos ya borrados.

## Errores tipados

Cada operación devuelve `Result<T, bitacora::Error>`, con cinco variantes:
`Invalida`, `NoEscribible`, `Corrupcion`, `Interno` e `Io`. Cada una responde a
lo único que de verdad necesita saber quien llama —**¿sirve de algo
reintentar?**— con `Error::reintentable()`, y `Error::categoria()` da una
palabra estable para registros y para el protocolo, así que un cliente nunca
tiene que analizar prosa. Las líneas de error por el socket son ahora
`ERR <categoría> <texto>`.

Esto sustituye al `io::Error` de antes, que no distinguía «tu clave es
inválida» de «el disco acaba de fallar», que son justo los dos casos cuya
respuesta correcta es opuesta. Los cambios incompatibles están en
[docs/CAMBIOS.md](docs/CAMBIOS.md).

## Endurecer la superficie expuesta

El motor puede ser impecable y el servidor ser la pieza frágil. Lo que hay, y
qué evita cada cosa:

| Defensa | Sin ella |
|---|---|
| Tiempos de espera de lectura y escritura | Un cliente que abre el socket y no envía nada retiene un hilo para siempre; con `--max-conns` conexiones mudas el servidor queda inaccesible sin enviar un byte útil |
| Tope en `KEYS`, con aviso `truncado` | Un solo `KEYS` sobre 200.000 claves construye y ordena una lista de 200.000 elementos por cliente |
| El fallo al crear un hilo no es fatal | Con los hilos agotados moría el servidor entero en vez de rechazar esa conexión |
| Admisión atómica de conexiones | `load` y luego `fetch_add` no es atómico, así que el límite se podía superar |
| Saltos de línea rechazados por el socket | El protocolo es por líneas: un valor con `\n` hacía que el cliente leyera dos respuestas donde había una |
| Apagado ordenado con SIGINT/SIGTERM | Ctrl-C mataba el proceso en seco: sin `fsync` final y con la compactación cortada por donde fuera |
| Tope al tamaño de respuesta | Un valor de 1 GiB suponía 1 GiB reservado por lectura concurrente |
| `AUTH` opcional, y negarse a escuchar fuera de localhost sin él | Cualquiera que llegase al puerto tenía lectura, escritura y `COMPACT` |

`AUTH` usa un secreto compartido, comparado en tiempo constante, que viene de
`--clave` o de la variable `BITACORA_CLAVE` para no dejarlo en la lista de
procesos.

## Cómo se mantiene esto honesto

La CI corre en cada push, en **Linux y macOS**: `cargo fmt --check`,
`clippy --all-targets -- -D warnings` (sin avisos, no «con pocos avisos»), la
suite completa, y la suite otra vez en un solo hilo para que se vea si unas
pruebas dependen de otras. El trabajo en dos sistemas se gana el sitio: el
motor se escribió en macOS, y el de Linux compila las ramas
`#[cfg(not(target_os = "macos"))]` que aquí no se compilan nunca. En una de
ellas había escondido un error de tipos.

Las pruebas por propiedades usan semilla fija para que `cargo test` sea
reproducible, y la CI les pasa `BITACORA_SEMILLA=<número de ejecución>`, así que
cada vuelta explora una franja distinta del espacio en vez de repetir los mismos
965 casos. Un fallo imprime su semilla y su iteración, y se reproduce exacto.

## Pruebas (59)

* `crc32`: vector estándar, cálculo por trozos.
* `wal`: ida y vuelta, cola cortada, bit cambiado.
* `protocol`: parseo y errores.
* Recuperación: **`kill -9` real** a mitad de escritura (×3), la cola rota se
  recorta, borrados + compactación + reinicio, claves/valores binarios y límites.
* Corrupción (`tests/corrupcion.rs`, 10 pruebas): un bit volteado al principio,
  en el medio y en tres cuartos del segmento; dos corrupciones en el mismo
  segmento; una cabecera destrozada; un truncamiento por el medio; un segmento
  de basura entera. Cada una comprueba que **los registros válidos posteriores
  sobreviven**, que el modo estricto se niega a abrir dejando el fichero
  idéntico byte a byte, y que el modo reparar deja copia del original.
* Concurrencia: 8 hilos × 3.000 escrituras con lecturas cruzadas y reinicio,
  clave caliente desde 8 hilos coherente con el log, compactación mientras se
  escribe y se lee, group commit con 16 hilos (`synced_seq == last_seq` al final).
* Propiedades y barrido aleatorio (`tests/propiedades.rs`, 4 pruebas): todo
  registro vuelve idéntico; el lector no entra en panic con ninguna secuencia de
  bytes **y siempre avanza** (sin eso, la resincronización podría quedarse en un
  bucle infinito); la recuperación nunca *inventa* datos, solo los pierde; y el
  motor coincide con un modelo `HashMap` a lo largo de una secuencia aleatoria
  de operaciones, antes y después de reiniciar. Se han barrido 48.000 casos con
  cinco semillas a mano, además de lo que hace la CI.
* Endurecimiento del servidor (`tests/servidor.rs`, 8 pruebas): operar sin
  `AUTH`, clave equivocada, negarse a escuchar fuera de localhost sin clave, el
  tope de `KEYS` y su orden, valores con saltos de línea en los dos sentidos,
  el tope de tamaño de respuesta, el límite de conexiones y que las plazas se
  liberan, el cierre de una conexión muda por tiempo, y que `serve` vuelve
  cuando se le pide.
* Durabilidad con el disco fallando (`tests/durabilidad.rs`, 6 pruebas, con
  errores de `fsync` inyectados): la marca no avanza más allá de un `fsync`
  fallido, el escritor recibe el error en vez de colgarse, **todos** los
  escritores que esperaban lo reciben, un fallo al cerrar un segmento se
  propaga, y tras reiniciar vuelve a ser escribible.

## Decisiones y límites (para defenderlo)

* **¿Por qué Rust?** Hilos nativos del SO, sin recolector de basura (la RAM es
  la que se ve), control explícito de `fsync`, y el compilador impide carreras de
  datos: cada acceso compartido pasa por `Mutex`/`RwLock`/atómicos.
* **¿Por qué estilo Bitcask (índice en RAM, valores en disco) y no un B-tree o
  LSM?** Es el diseño más simple que da escrituras secuenciales, una sola
  lectura de disco por `GET` y recuperación trivial. El coste: hay que poder
  tener todas las claves en RAM, y el arranque lee todo el log (200k claves en
  ~130 ms; a 50 M claves habría que añadir *hint files*).
* **Visibilidad vs durabilidad**: con `group`, una escritura es visible para
  otros hilos en cuanto entra en el índice, pero el cliente solo recibe `OK`
  cuando está en disco. Es el mismo compromiso que hacen Redis (AOF `everysec`) y
  la mayoría de motores con group commit.
* **Protocolo de texto por líneas**: los valores no pueden contener saltos de
  línea (en la API Rust sí, es binaria). Elegido para poder probarlo con `nc`.
* **Un hilo por conexión** escala a cientos de clientes, no a cien mil; para
  eso habría que pasar a un *pool* o a `epoll/kqueue`. Está limitado por
  `--max-conns`.
* **Sin transacciones multi-clave ni TTL**: fuera del alcance a propósito.

## Licencia

MIT — ver [LICENSE](LICENSE). Úsalo, modifícalo y distribúyelo; solo hay que conservar el aviso de copyright.
