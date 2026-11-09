# Cambios incompatibles

Los gates de endurecimiento (8 y 9 de noviembre de 2026) han introducido
cambios que rompen compatibilidad. Están todos aquí, por gate, con qué hacer en
cada caso.

**El formato binario del WAL no ha cambiado en ninguno de ellos.** Los ficheros
de datos escritos antes se siguen leyendo sin migración.

---

## Gate 0 — integridad del WAL

### Una base de datos con corrupción ya no abre

Es el cambio de comportamiento más visible. Antes, un registro ilegible se
trataba como fin de fichero: la base abría, el log se recortaba ahí y los
registros posteriores desaparecían **en silencio**. Medido: un bit volteado al
40% de un segmento destruía 30 de 50 claves.

Ahora, si hay registros válidos detrás del ilegible, eso es corrupción y la
política por defecto (`RecoveryPolicy::Estricto`) **hace fallar `Db::open` sin
modificar ningún fichero**.

*Qué hacer:* si prefieres que abra pase lo que pase, usa `--recuperacion
reparar` (o `Options::recovery = RecoveryPolicy::Reparar`), que además copia
cada segmento afectado antes de tocarlo y declara las pérdidas. Una cola de
escritura incompleta —lo que deja un apagón— sigue recortándose como antes: ahí
no hay nada confirmado que perder.

### `wal::ReadOutcome` cambia de variantes

`Torn { at }` desaparece y se reparte en dos, porque confundirlas era el fallo:

```rust
// antes
ReadOutcome::Torn { at }

// ahora
ReadOutcome::IncompleteTail { at, cause }                       // recortar es seguro
ReadOutcome::Corrupt { at, resync_at, cause, por_cabecera }     // NO recortar
```

### `wal::SegmentReader` exige `Seek`

La cota pasa de `R: BufRead` a `R: BufRead + Seek`, porque resincronizar tras un
registro ilegible obliga a releer desde posiciones arbitrarias.
`BufReader<File>` y `Cursor<Vec<u8>>` ya lo cumplen.

### `STATS` tiene tres líneas más

`recovery_corrupt_records`, `recovery_corrupt_bytes` y
`recovery_truncated_tails`. Un cliente que diera por fijo el número de líneas
de `STATS` hay que ajustarlo; el formato de cada línea no cambia.

---

## Gate 1 — durabilidad

### Tras un `fsync` fallido no se vuelve a escribir

Antes, el hilo de sincronización adelantaba la marca «esto está en disco»
también cuando el `fsync` había fallado. Ahora no la adelanta, y la base queda
**no escribible hasta reiniciar**: `put` y `delete` devuelven
`Error::NoEscribible` y las lecturas siguen funcionando.

*Por qué no se reintenta:* en Linux, un `fsync` que falla puede haber
descartado ya las páginas sucias, así que un segundo `fsync` devuelve éxito
sobre un dato que nunca llegó al disco. Misma decisión que PostgreSQL en 2018.

*Qué hacer:* tratar ese error como fatal para el proceso y reiniciar. Al
reabrir se lee lo que de verdad hay en disco.

### `STATS` tiene una línea más

`health`, que vale `ok` o `no-escribible (motivo)`.

---

## Gate 2 — robustez y API de errores

### La API pública ya no devuelve `io::Result`

Este es el cambio que afecta a cualquiera que use la librería como dependencia.

```rust
// antes
pub fn put(&self, key: &[u8], value: &[u8]) -> std::io::Result<()>

// ahora
pub fn put(&self, key: &[u8], value: &[u8]) -> bitacora::Result<()>
```

Afecta a `Db::open`, `open_default`, `put`, `get`, `delete`, `compact` y
`close`. El tipo nuevo es `bitacora::Error`, con cinco variantes: `Invalida`,
`NoEscribible`, `Corrupcion`, `Interno` e `Io`.

*Qué hacer:* si tu código solo hace `?` hacia un `Box<dyn Error>` o
`anyhow::Error`, no hay nada que cambiar. Si necesitas un `io::Error`, hay
`impl From<Error> for io::Error`. Y si quieres aprovecharlo, `categoria()` da
una palabra estable y `reintentable()` responde a si merece la pena reintentar:

```rust
match db.put(k, v) {
    Err(e) if !e.reintentable() => return Err(e),  // no insistas
    Err(e) => reintentar_luego(e),
    Ok(()) => {}
}
```

### Las líneas de error del protocolo llevan categoría

```
antes:  ERR valor de más de 1 GiB
ahora:  ERR invalida petición inválida: valor de más de 1 GiB
```

El formato pasa de `ERR <texto>` a `ERR <categoría> <texto>`, con la categoría
tomada de `Error::categoria()` (`invalida`, `no-escribible`, `corrupcion`,
`interno`, `io`). Un cliente que solo comprobaba el prefijo `ERR` sigue
funcionando; uno que leyera el resto de la línea como mensaje, no.

### Un valor de 1 GiB justos ahora se acepta

El escritor rechazaba `>= 1 GiB` y el lector aceptaba `<= 1 GiB`, así que había
un tamaño ilegal al escribir y legal al leer. Ahora los dos rechazan solo
`> 1 GiB`.

### `get` puede tardar hasta 2 segundos en un caso raro

Cuando la compactación retira un segmento entre que `get` consulta el índice y
abre el fichero, `get` reintenta. Antes eran 32 vueltas de espera activa y
después un error genérico; ahora hay un plazo (`Options::read_retry_timeout`,
2 s por defecto), se cede la CPU, y agotarlo devuelve `Error::Interno` dejando
claro que es un fallo del motor y no una clave ausente.

### Un panic ya no inutiliza el proceso

Los candados se recuperan del envenenamiento en vez de hacer entrar en panic a
todas las operaciones siguientes, y el servidor atrapa el panic en el borde de
cada conexión, lo que además arregla una fuga: un hilo de conexión que entraba
en panic nunca decrementaba el contador de conexiones, así que el límite se iba
agotando solo. **Límite honesto:** si el hilo que entró en panic dejó el estado
a medias, sigue a medias; recuperarse evita la cascada, no arregla los datos.
Tras un panic conviene reiniciar.

### `Options` tiene tres campos más

`recovery`, `read_retry_timeout` y `fsync_falla_tras` (este último marcado
`#[doc(hidden)]`, solo para inyección de fallos en pruebas). Si construyes
`Options` con `Options::new(dir)` y luego asignas campos, no hay nada que
cambiar; si lo construías con un literal de estructura, hay que añadirlos.

---

## Gate 3 — endurecimiento del servidor

### `Db::keys` cambia de firma

```rust
// antes
pub fn keys(&self, prefix: &[u8]) -> Vec<Vec<u8>>

// ahora
pub fn keys(&self, prefix: &[u8], limite: usize) -> (Vec<Vec<u8>>, bool)
```

El segundo elemento dice si había más. Sin tope, un solo `KEYS` sobre 200.000
claves construía y ordenaba una lista de 200.000 elementos por cada cliente que
lo pidiera. El resultado sigue siendo las `limite` primeras **en orden**, no las
primeras que se encuentren: por dentro se usa un montículo acotado, así que la
memoria es O(limite) en vez de O(claves).

### `ServerConfig` tiene siete campos más

`read_timeout`, `write_timeout`, `keys_limit`, `max_value_bytes`, `clave`,
`permitir_sin_clave` y `parada`. *Qué hacer:* construirlo con
`ServerConfig::new(host, puerto)` y asignar lo que haga falta, en vez de con un
literal de estructura.

### `server::handle` y `server::execute` cambian de firma

Reciben `&ServerConfig`, y `execute` además `&mut bool` con el estado de
autenticación de la conexión.

### El servidor se niega a escuchar fuera de localhost sin clave

Escuchar en `0.0.0.0` sin autenticación es dar lectura, escritura y `COMPACT` a
quien llegue. Ahora `serve` falla con un mensaje que dice cómo arreglarlo.
*Qué hacer:* `--clave <secreto>` (o la variable `BITACORA_CLAVE`, para no
dejarla en la lista de procesos), o `--sin-clave` si de verdad se quiere así.

### Las conexiones inactivas se cierran

Por defecto, 300 s de espera de lectura y 60 s de escritura; `0` las desactiva.
Sin tiempos de espera, unas pocas conexiones mudas retenían todos los hilos y
el servidor quedaba inaccesible sin recibir un byte útil. *Qué hacer:* si tienes
clientes que abren una conexión y la dejan parada más de 300 s, súbelo con
`--timeout-lectura` o manda `PING` de vez en cuando.

### El protocolo gana órdenes y cambia dos respuestas

* **Nueva orden** `AUTH <clave>`, obligatoria antes de operar si el servidor se
  arrancó con clave. `PING` y `QUIT` se permiten sin autenticarse.
* `KEYS` acepta ahora `KEYS [prefijo] [limite]`.
* La cabecera de `KEYS` puede llevar sufijo: `KEYS <n> truncado`. Un cliente
  que leyera el segundo campo como número sigue funcionando; uno que exigiera
  exactamente dos campos, no.
* `SET` **rechaza** valores con `\n` o `\r`: el protocolo es por líneas, y
  antes un valor así hacía que el cliente leyera dos respuestas donde había
  una. La API de Rust sigue aceptando binario con saltos.
* `GET` devuelve error, en vez de corromper el entramado, si el valor contiene
  saltos de línea, y si supera `--valor-max-mb` (16 MB por defecto). Esos
  valores siguen accesibles desde la API de Rust.

---

## Gate 4 — red de seguridad

### `wal::SegmentReader::next` pasa a llamarse `siguiente`

Se llamaba igual que `Iterator::next` sin serlo, que es justo la clase de nombre
que induce a error (y lo avisaba clippy). Solo afecta a quien use
`SegmentReader` directamente.

### Nada más cambia

La CI, el formato, las pruebas por propiedades y el barrido aleatorio no tocan
la API ni el protocolo.

---

## Gate 5 — banco de pruebas

### Nada cambia

El banco (`bitacora bench`) se reescribió con percentiles exactos, medición
durante la compactación y tiempo de reapertura. No toca la API, el protocolo ni
el formato de los datos.

---

## Gate 6 — observabilidad y operación

**El formato binario del WAL sigue sin cambiar.** Los datos escritos por
cualquier versión anterior se leen sin migración.

### Un segundo proceso ya no puede abrir el mismo directorio

Es el cambio más importante de este gate, y cierra un agujero que **no estaba
en el informe de auditoría**: nada impedía que dos procesos abrieran la misma
carpeta de datos. Los dos calculaban por separado el mismo número de segmento y
el mismo número de secuencia, abrían el mismo `NNNNNN.log` en modo añadir con
su propia idea de por dónde iba, y escribían entremezclados duplicando los
`seq` que la recuperación usa para decidir qué versión de una clave gana.
Bastaba con arrancar dos veces `bitacora serve --dir data`.

Ahora `Db::open` toma un `flock` exclusivo sobre `<dir>/LOCK` y falla con
`Error::Configuracion` si otro proceso lo tiene. El bloqueo lo suelta el núcleo
cuando el proceso muere, muera como muera, así que un `kill -9` o un apagón no
dejan un bloqueo fantasma. `close()` lo suelta antes de devolver, de modo que
reabrir justo después funciona siempre y no «casi siempre».

*Qué hacer:* nada, salvo que tuvieras dos procesos sobre la misma carpeta, en
cuyo caso lo que tenías era corrupción silenciosa. **Sobre NFS `flock` no es de
fiar**, y ahí este bloqueo no protege.

### `Error` tiene una variante nueva: `Configuracion`

Rompe cualquier `match` exhaustivo sobre `Error` sin brazo `_`. Su
`categoria()` es `"configuracion"` y `reintentable()` es `false`: hay que
cambiar la configuración, no reintentar.

### `Db::open` valida la configuración antes de tocar el disco

Se rechaza lo que **no hace lo que dice**, con un mensaje que explica qué
pasaba antes:

| Valor | Qué hacía |
|---|---|
| `shards = 0` | Se convertía en 1 **en silencio** |
| `group_interval = 0` con `fsync = group` | **Medido**: 100 % de un núcleo con el servidor ocioso, frente a 0,5 % con 2 ms |
| `compact_dead_ratio` fuera de [0, 1] o `NaN` | Por encima de 1 deja la auto-compactación activada y sin dispararse jamás |
| `read_retry_timeout = 0` | Un `get` que coincidiera con el final de una compactación devolvía error interno en la primera vuelta |
| `shards > 65536` | Cada `STATS`, `KEYS` o `len()` toma un candado por partición |

Lo que **no** se valida es igual de deliberado: `max_segment_bytes` no tiene
mínimo. Un segmento pequeño hace exactamente lo que dice —muchos ficheros— y es
una decisión de quien configura. Hubo un mínimo durante el desarrollo de este
gate y lo único que conseguía era romper pruebas que quieren provocar cierres
de segmento.

### `ServerConfig::validar` y dos campos nuevos

`serve` valida antes de abrir el socket y devuelve `InvalidInput` si
`max_conns`, `keys_limit` o `max_value_bytes` son 0, si un tiempo de espera es
`Some(0)` (que no es «sin límite»: eso es `None`) o si la clave está vacía.

`ServerConfig` gana `metricas: Arc<MetricasServidor>` y
`backup_dir: Option<PathBuf>`. Construirlo con literal de struct se rompe;
`ServerConfig::new`, que es la vía documentada, no.

### `Stats` tiene quince campos y quince líneas más

`estado` (`ok`/`aviso`/`critico`) y los contadores de operaciones. **`health`
no cambia de significado**: sigue siendo `ok` o `no-escribible (motivo)`, para
no romper a quien ya lo vigile. `STATS` anuncia su número de líneas, así que un
cliente que lo lea no se entera; uno que las tuviera contadas a mano, sí.
Construir `Stats` con literal de struct se rompe.

### El protocolo gana tres órdenes

```text
HEALTH          -> HEALTH <ok|aviso|critico> <n> seguido de n motivos
METRICS         -> METRICS <n> seguido de n líneas en formato Prometheus
BACKUP [nombre] -> OK <ruta> segmentos=.. bytes=.. last_seq=.. ms=..
```

Las tres **exigen `AUTH`** si el servidor tiene clave: el estado interno y los
contadores no son información pública. `BACKUP` solo existe si el servidor
arrancó con `--backup-dir`, y el cliente elige **un nombre, no una ruta**: se
rechaza cualquier cosa con barras, `..`, punto inicial o caracteres fuera de
`[A-Za-z0-9._-]`. Si el cliente pudiera dar la ruta, un `AUTH` válido sería
permiso para escribir ficheros donde pueda el usuario del servidor.

`Command` gana las variantes `Health`, `Metrics` y `Backup`, lo que rompe un
`match` exhaustivo sobre él.

### La salida por `stderr` del servidor cambia de forma

Donde había prosa en español hay ahora una línea por evento con campos
nombrados, en texto o en JSON (`--log`, `--log-nivel`, `--log-fichero`). El
catálogo completo está en `OPERACION.md`. No es una API, pero quien tuviera un
`grep` sobre esas líneas tendrá que cambiarlo.

Dos consecuencias buscadas: **nunca se registran claves ni valores** —un log
con las claves del cliente sería una copia parcial de los datos con otros
permisos y otra retención—, y el detalle de la apertura lo emite el motor en
`engine_opened`, no `main`, para que no haya dos versiones de la misma verdad.

### Métodos nuevos

`Db::health()`, `Db::metricas()`, `Db::backup()`, `Options::validar()`,
`ServerConfig::validar()`, y los tipos `Estado`, `Salud`, `BackupReport`,
`MetricasServidor`. Los módulos `log` y `metricas` son nuevos y públicos.

### Los contadores van por partición, y por qué importa

Está medido en `RENDIMIENTO.md`: con los contadores en una sola estructura
global, SET con `fsync=never` bajaba un 21 % mientras el p50 no se movía —ocho
hilos peleándose por la misma línea de caché—. Repartidos por partición del
índice y alineados a 128 bytes, la diferencia deja de ser medible en esta
máquina. No afecta a la API.
