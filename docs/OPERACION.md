# Operar BITÁCORA

Lo que hace falta para tener esto en marcha y enterarse cuando algo va mal.
Todo lo que aparece aquí existe en el código; lo que **no** existe también está
dicho, al final.

## Arrancar

```bash
bitacora serve --dir /var/lib/bitacora \
               --host 127.0.0.1 --port 7379 \
               --fsync group \
               --log json --log-nivel info \
               --backup-dir /var/backups/bitacora
```

Lo que conviene decidir a propósito, no por defecto:

| Opción | Qué decide |
|---|---|
| `--fsync group` | Cada `OK` significa «está en disco». `never` es mucho más rápido y pierde lo que el sistema tuviera en caché si hay apagón. `always` tiene buena mediana y **la peor cola** (ver `RENDIMIENTO.md`). |
| `--fullfsync` | Además vacía la caché del propio disco. Es la única opción honesta si el disco miente sobre sus escrituras, y cuesta un orden de magnitud. |
| `--clave` o `BITACORA_CLAVE` | Sin clave, el servidor se niega a escuchar fuera de `localhost`. La variable de entorno evita dejar el secreto en la lista de procesos. |
| `--backup-dir` | Sin esto, la orden `BACKUP` del protocolo no existe. |
| `--log json` | Para un recolector. Sin ella, texto legible en `stderr`. |

**Un solo proceso por directorio.** Al abrir se toma un `flock` sobre
`<dir>/LOCK`, así que un segundo `bitacora serve` sobre la misma carpeta falla
con un error de categoría `configuracion` y no abre. Antes del Gate 6 esto no
estaba: dos procesos elegían el mismo número de segmento y el mismo número de
secuencia, escribían entremezclados en el mismo fichero y duplicaban los `seq`
que la recuperación usa para decidir qué versión de una clave gana. El bloqueo
lo suelta el núcleo cuando el proceso muere, muera como muera, así que un
apagón no deja un bloqueo fantasma. **Sobre NFS, `flock` no es de fiar**: no
conviene poner un directorio de datos en un montaje de red.

### Apagado

`SIGINT` y `SIGTERM` dejan de aceptar conexiones, esperan hasta 5 segundos a
las que estén en curso, paran el hilo de sincronización, esperan a que acabe
una compactación en marcha y hacen el `fsync` final. En systemd:
`KillSignal=SIGTERM` y `TimeoutStopSec=30`. Un `SIGKILL` también es seguro —hay
una prueba que lo hace tres veces y exige que las claves recuperadas sean un
prefijo exacto—, pero se pierde lo que no estuviera sincronizado.

## Enterarse: el registro

Una línea por evento. En `--log json` es un objeto por línea, con `ts`
(RFC 3339 UTC con milisegundos), `level` (`debug`/`info`/`warn`/`error`),
`event` y los campos del evento:

```json
{"ts":"2026-11-09T09:14:02.481Z","level":"error","event":"fsync_failed","error":"...","note":"la base de datos deja de aceptar escrituras hasta reiniciar"}
```

**Nunca se registran claves ni valores**, a propósito: un fichero de log con
las claves del cliente sería una copia parcial de los datos, con otros permisos
y otra retención. Hay una prueba que escribe una clave y un valor conocidos y
exige que no aparezcan en el log.

### Catálogo de eventos

| Evento | Nivel | Qué significa y qué hacer |
|---|---|---|
| `engine_opened` | info | Apertura normal. Mirar `data_loss`: si es `true`, **la recuperación no fue íntegra**. |
| `engine_open_failed` | error | No abrió. `category` dice si es configuración, corrupción o E/S. |
| `engine_closed` | info | Cierre limpio, con el `synced_seq` final. |
| `engine_close_failed` | error | El `fsync` final falló: lo último escrito puede no estar en disco. |
| `recovery_tail_truncated` | warn | Normal después de un apagón o un `kill -9`: se recortaron bytes que nunca se confirmaron a nadie. No se perdió nada confirmado. |
| `recovery_data_loss` | error | **Se perdieron datos.** Solo sale en modo `reparar`. Ver `RECUPERACION.md` y los ficheros `.corrupto-<epoch>`. |
| `recovery_detail` | error | Una línea por zona ilegible, con fichero y byte. |
| `fsync_failed` | error | El disco falló. El motor **no acepta más escrituras** hasta reiniciar, y es a propósito: ver `RECUPERACION.md`. Hay que mirar el disco y reiniciar. |
| `compaction_done` | info | Compactación terminada, con bytes antes y después. |
| `compaction_failed` / `auto_compaction_failed` | error | Se abandonó sin borrar ningún segmento. Si `category` es `corrupcion`, hay un segmento ilegible y toca intervenir. |
| `backup_done` | info | Copia hecha: destino, segmentos, bytes y `last_seq` incluido. |
| `backup_failed` | error | La copia **no** se hizo. Si vigilas copias, vigila este evento. |
| `server_listening` | info | Puerto, límite de conexiones y si hay autenticación. |
| `server_stopping` / `server_stopped` | info | Apagado ordenado. |
| `server_stopped_with_open_conns` | warn | Se cerraron conexiones que no terminaron en 5 s. |
| `server_failed` | error | `serve` devolvió error: normalmente el puerto ocupado o configuración inválida. |
| `accept_failed` | warn | Un `accept` falló. Suelto, es ruido; sostenido, hay agotamiento de descriptores. |
| `conn_rejected` | warn/error | `reason=limit` (se alcanzó `--max-conns`) o `reason=no_threads` (el sistema no deja crear hilos). |
| `conn_error` | warn | Una conexión terminó con error, salvo los normales de cliente que se va. |
| `conn_panicked` | error | **Un fallo del programa.** Se cierra solo esa conexión, pero el estado en memoria puede haber quedado a medias: conviene reiniciar y abrir un informe. |
| `auth_failed` | warn | `AUTH` con clave incorrecta. Sostenido = alguien probando claves. |

Si `bitacora_log_dropped_total` no es 0, hay líneas que no se pudieron escribir
y lo que estás leyendo está incompleto.

## Enterarse: las métricas

`METRICS` por el protocolo, o `bitacora metrics`, devuelven el formato de
exposición de Prometheus. **No hay servidor HTTP**; el puente son dos líneas:

```bash
# systemd timer o cron: deja el fichero para node_exporter
bitacora metrics --port 7379 > /var/lib/node_exporter/bitacora.prom.tmp \
  && mv /var/lib/node_exporter/bitacora.prom.tmp /var/lib/node_exporter/bitacora.prom
```

El `mv` no es adorno: escribir directamente sobre el fichero que el recolector
está leyendo le da una muestra a medias.

### Las que de verdad hay que vigilar

| Métrica | Alerta |
|---|---|
| `bitacora_writable` | **0 = crítico.** Un `fsync` falló; no se aceptan escrituras. |
| `bitacora_health_state` | 0 ok, 1 aviso, 2 crítico. Es el resumen de `HEALTH`. |
| `bitacora_recovery_corrupt_records` | **Distinto de 0 = se perdieron datos** en la última apertura. |
| `bitacora_dead_bytes` / `bitacora_disk_bytes` | Si la proporción sube y no baja, la compactación no está haciendo su trabajo. |
| `bitacora_last_seq - bitacora_synced_seq` | Lo que un apagón se llevaría ahora mismo. Con `fsync=group` es ~0; con `never` crece. |
| `bitacora_backups_total{result="failed"}` | Una copia fallida es una copia que no tienes. |
| `bitacora_conn_panics_total` | Cualquier valor distinto de 0 merece un informe de fallo. |
| `bitacora_connections_rejected_total{reason="limit"}` | Subiendo: hace falta más `--max-conns` o hay conexiones que no se cierran. |
| `bitacora_auth_failures_total` | Subiendo: alguien probando claves. |
| `bitacora_get_retries_total` | Subiendo mucho: la compactación y las lecturas se pisan. |
| `bitacora_fsync_total` | Escrituras por `fsync` dice cuánto está amortizando el group commit. |

El resto (`bitacora_keys`, `bitacora_index_bytes`, `bitacora_ops_total`,
`bitacora_bytes_*`) son para capacidad y para gráficas, no para alertar.

## El semáforo: `HEALTH`

```bash
bitacora health --port 7379   # imprime ok | aviso | critico y los motivos
echo $?                       # 0 = ok o aviso, 1 = critico, 2 = no se pudo preguntar
```

Eso es exactamente lo que necesita un `healthcheck` de contenedor o de systemd.
Qué pone cada estado:

* **`critico`**: un `fsync` falló (no escribible), o se perdieron registros al
  recuperar. Las dos cosas exigen a una persona.
* **`aviso`**: se recortó una cola incompleta al abrir (normal tras un apagón),
  hay ficheros en cuarentena de una reparación, o más del 80 % del log es basura
  con `auto_compact` desactivado.
* **`ok`**: ninguna de las anteriores.

**Lo que `HEALTH` no es.** No es una sonda: informa de fallos que el motor ya
conoce, no comprueba que el disco siga respondiendo ni mide latencias, porque
nada en el motor las mide. Un disco que se ha vuelto lento, o que se llenará en
una hora, sale aquí como `ok` hasta que una operación falla de verdad. Para eso
hacen falta las métricas del sistema, no estas.

## Copias de seguridad

### En caliente, con el servidor en marcha

```bash
printf 'AUTH %s\nBACKUP noche\nQUIT\n' "$BITACORA_CLAVE" | nc 127.0.0.1 7379
```

La copia va a `<--backup-dir>/noche`. **El cliente elige el nombre, no la
ruta**: se rechaza cualquier cosa con barras, `..`, puntos al principio o
caracteres fuera de `[A-Za-z0-9._-]`. Si el cliente pudiera dar la ruta, un
`AUTH` válido se convertiría en «escribe ficheros donde pueda el usuario del
servidor», que es mucho más de lo que da acceso a una base de datos.

**Qué garantiza.** La copia contiene exactamente todos los registros escritos
antes del instante en que empieza, y ninguno parcial. Restaurarla da la base de
datos tal como estaba en ese instante. Dos cosas lo consiguen: la longitud del
segmento activo se toma bajo el candado de escritura, y ese contador solo avanza
después de que un registro esté escrito entero; y mientras se copia, la
compactación no puede retirar segmentos. Una compactación en curso no se
interrumpe: solo espera para borrar los viejos.

**Qué no garantiza.** No coordina con nada que haya fuera de la base de datos.
No vuelve a leer la copia ni recalcula los CRC —para eso está `verificar`—. No
comprime ni es incremental: una copia de 30 GB mueve 30 GB. Y retrasa la
compactación mientras dura.

### En frío, con la base parada

```bash
bitacora backup --dir /var/lib/bitacora --a /var/backups/bitacora/manual
```

Si un servidor tiene el directorio abierto, el bloqueo lo impide y lo dice, en
vez de convertirse en un segundo escritor.

### Comprobar antes de confiar

```bash
bitacora verificar --dir /var/backups/bitacora/noche
```

Lee **todo** el log en modo estricto y dice `INTEGRO: si` o `INTEGRO: no` con el
motivo, más claves, segmentos y estado. Salida 0 si se puede usar. Una copia sin
verificar no es una copia: es un directorio.

### Restaurar

```bash
systemctl stop bitacora
mv /var/lib/bitacora /var/lib/bitacora.roto
bitacora restore --de /var/backups/bitacora/noche --a /var/lib/bitacora
bitacora verificar --dir /var/lib/bitacora
systemctl start bitacora
```

`restore` **se niega** a escribir en un directorio que ya tenga segmentos
`.log`, y no es un capricho: mezclar segmentos de dos momentos distintos puede
resucitar una clave borrada, porque el estado se reconstruye por número de
secuencia y un segmento viejo trae registros que el nuevo ya no tiene.

### Qué guardar

La regla de siempre: **tres copias, dos medios, una fuera**. Los segmentos
cerrados no cambian nunca, así que `rsync` sobre un directorio de copias
aprovecha eso bien… hasta la siguiente compactación, que reescribe todo. El
fichero `LOCK` no se copia: no es un dato.

## Lo que no hay

Dicho aquí para que nadie lo descubra en producción:

* **No hay servidor HTTP** para las métricas. El puente de arriba es la vía.
* **No hay TLS, y `AUTH` viaja en claro.** Esto se expone en `localhost` o
  detrás de un túnel, no en una red abierta.
* **No hay límite de intentos de `AUTH`.** La comparación de la clave es en
  tiempo constante, pero nada frena a quien pruebe sin parar; para eso hace
  falta el cortafuegos.
* **No hay rotación de logs.** Con `--log-fichero` hay que ponerle `logrotate`
  con `copytruncate`, porque el fichero se abre una vez y se mantiene abierto.
* **`KEYS`, `len()` y `STATS` no son instantáneas coherentes** (M7): se
  consultan las particiones de una en una. Para un panel da igual; para una
  auditoría, no.
* **El CRC32 detecta corrupción accidental, no manipulación** (M9). Quien pueda
  escribir en los ficheros puede falsificar registros con su CRC correcto. Los
  permisos del directorio son la defensa.
* **El bloqueo del directorio no funciona sobre NFS.**
