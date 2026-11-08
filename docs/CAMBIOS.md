# Cambios incompatibles

Los tres primeros gates de endurecimiento (8 de noviembre de 2026) han
introducido cambios que rompen compatibilidad. Están todos aquí, con qué hacer
en cada caso.

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
