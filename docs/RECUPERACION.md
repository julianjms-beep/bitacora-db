# Recuperación: garantías y límites

Este documento dice **exactamente** qué sobrevive a qué, y qué no. Está escrito
para que nadie tenga que deducirlo leyendo el código, y para que ninguna pérdida
de datos pase inadvertida.

## Las dos situaciones, que no son la misma

Cuando un registro del WAL no pasa las comprobaciones puede significar dos cosas
muy distintas, y confundirlas cuesta datos:

| | Cola de escritura incompleta | Corrupción |
|---|---|---|
| Qué es | El fichero acaba en un registro a medio escribir y **no hay nada válido detrás** | Un registro no valida pero **sí hay registros válidos detrás** |
| Causa típica | Apagón o `kill -9` durante un `write` | Bit podrido en el disco, error de E/S, cable malo, RAM mala al escribir |
| Qué se perdió | Nada confirmado: ese registro nunca recibió un `OK` | Un registro que **sí** se confirmó a un cliente |
| Qué hace el motor | Recorta el fichero ahí. Es seguro | **No recorta nada.** Resincroniza y conserva lo posterior |
| Es normal | Sí, es la recuperación de siempre tras un fallo | No. Es un incidente que hay que investigar |

Hasta el 8 de noviembre de 2026 el motor no distinguía los dos casos: trataba
toda anomalía como cola incompleta y recortaba el fichero. Medido: **un bit
volteado al 40% de un segmento destruía 30 de 50 claves y recortaba el fichero de
2.530 a 1.000 bytes**, de forma permanente y silenciosa. Ese es el fallo que este
documento y `tests/corrupcion.rs` existen para que no vuelva.

## Cómo se localiza el registro siguiente

El formato no tiene marca de sincronismo, así que hay que deducir dónde empieza
el registro siguiente. Se hace en dos pasos, sin cambiar el formato binario:

1. **Confiando en la cabecera.** Si la corrupción cayó en la clave o en el valor,
   los 21 bytes de cabecera siguen siendo buenos y el registro siguiente empieza
   en `offset + HEADER + key_len + val_len`. Es exacto, y es el caso más
   frecuente, porque la carga ocupa mucho más que la cabecera.
2. **Rastreando byte a byte.** Si la cabecera misma está corrupta, las longitudes
   no sirven. Se prueba cada desplazamiento y solo se acepta donde el CRC-32
   cuadre sobre la longitud que ese candidato declara.

En el paso 2 se prefiere un candidato **fuerte** (CRC correcto y seguido de otro
registro válido o del fin del fichero). Si no hay ninguno, se acepta el primero
**débil** (solo CRC): perder registros que sí se pueden leer es peor que asumir
una coincidencia de 1 entre 4.295 millones. Esta distinción no es teórica — sin
ella, un registro bueno encajado entre dos corrupciones se descartaba también
(prueba `dos_corrupciones_en_el_mismo_segmento`).

## Los dos modos

Se elige con `--recuperacion` o con `Options::recovery`.

### `estricto` (por defecto)

Si hay corrupción, **`open` falla y no se modifica ni un byte**. El error dice
cuántas zonas ilegibles hay, cuántos bytes, en qué fichero, en qué
desplazamiento, por qué causa, y qué hacer a continuación. Una recuperación
incompleta nunca se presenta como satisfactoria.

Es el modo por defecto a propósito: ante corrupción, lo correcto es que un humano
decida, no que el proceso siga adelante con menos datos de los que cree tener.

### `reparar`

Abre saltándose las zonas ilegibles. Antes de nada, **copia cada segmento
afectado** a `NNNNNN.log.corrupto-<epoch>` (copia completa, byte a byte), así que
el original queda disponible para un análisis o un rescate posterior. Después:

* Informa por `stderr` de cada zona ilegible, con fichero, desplazamiento y causa.
* La línea de arranque incluye `(RECUPERACIÓN CON PÉRDIDAS)`.
* `STATS` publica `recovery_corrupt_records`, `recovery_corrupt_bytes` y
  `recovery_truncated_tails`.
* `Db::recovery_report()` da el detalle completo, y `con_perdidas()` responde a
  «¿fue íntegra esta recuperación?».

## Garantías por operación

| Operación | Qué garantiza |
|---|---|
| `SET` con `--fsync always` | Al devolver `OK`, el registro ha pasado por `fsync`. Con `--fullfsync`, además la caché del propio disco está vacía |
| `SET` con `--fsync group` | Al devolver `OK`, el registro está en disco; muchos escritores comparten un `fsync` |
| `SET` con `--fsync never` | Sobrevive a que **maten el proceso**, no a un apagón del equipo |
| `DEL` | Las mismas garantías que `SET`: la lápida es un registro más |
| `GET` | Devuelve el último valor confirmado, o nada. **No vuelve a comprobar el CRC** (ver límites) |
| Apertura | Reconstruye el estado o falla; con `estricto`, nunca abre con menos datos de los que declara |
| Compactación | Si encuentra datos ilegibles, **se abandona sin borrar ningún segmento** |

## Límites de recuperación, explícitos

Esto es lo que **no** puede hacer, con el formato actual y con esta
implementación.

1. **Corrupción en el último registro del fichero.** Si no hay ningún registro
   válido detrás, es indistinguible de una escritura interrumpida, así que se
   trata como cola incompleta y se recorta. No se puede hacer mejor sin cambiar
   el formato.
2. **El CRC solo se comprueba al abrir, no en cada `GET`.** Un bit que se podra
   en el disco con el proceso ya en marcha devolverá un valor corrupto sin
   avisar, porque `GET` hace un `pread` del valor y no revalida nada. Es el
   límite más importante de esta lista.
3. **No hay redundancia.** Un registro ilegible se pierde; no se reconstruye. No
   hay réplicas, ni paridad, ni códigos de corrección de errores.
4. **CRC-32 detecta corrupción accidental, no manipulación.** Quien pueda
   escribir en los ficheros puede fabricar registros con su CRC correcto. Esto
   no es un mecanismo de seguridad.
5. **Colisión de CRC.** Un valor corrupto cuyo CRC coincida por casualidad pasa
   inadvertido: 1 entre 4.295 millones por registro.
6. **Falso positivo al rastrear.** En el paso 2, un tramo de bytes puede parecer
   un registro válido por azar. La probabilidad es del orden de 2⁻³² por
   candidato, lo que en un segmento de 64 MB da en torno a un 1,5% de que exista
   algún candidato espurio en alguna parte. Se mitiga prefiriendo el candidato
   fuerte; no se elimina.
7. **El rastreo es O(n) por corrupción.** Con la cabecera destrozada en un
   segmento de 64 MB, la reapertura puede tardar bastante. No hay cota superior
   configurable todavía.
8. **La corrupción del índice en memoria no se detecta.** Si la RAM falla, el
   índice puede apuntar mal y no hay checksum que lo cace.
9. **No hay comprobación proactiva.** No existe un «fregado» en segundo plano que
   recorra los segmentos buscando bits podridos antes de que un cliente los pida.
10. **Probado con bits volteados a mano, en macOS/arm64.** No se ha probado con
    fallos reales de disco, ni en Linux, ni con inyección de fallos de E/S a
    nivel de sistema de ficheros.

## Procedimiento ante corrupción

1. **No reiniciar en bucle.** Con el modo estricto el proceso no abrirá, y eso es
   lo correcto: no insistas con `reparar` sin leer antes el mensaje.
2. **Copiar el directorio de datos entero** a otro disco antes de tocar nada.
3. Leer el error: dice fichero, desplazamiento, bytes y causa.
4. Si hay una copia de seguridad reciente y buena, restaurarla es preferible a
   reparar: reparar pierde los registros ilegibles.
5. Si no hay copia, arrancar con `--recuperacion reparar` y **guardar** los
   ficheros `.corrupto-*` que genera.
6. Comprobar `STATS` y anotar `recovery_corrupt_records` y
   `recovery_corrupt_bytes`: son los datos que faltan.
7. Sospechar del hardware. Una corrupción intermedia no la causa un apagón:
   apunta al disco, al cable o a la memoria.

## Qué haría falta para mejorar esto

Pendiente, y ninguna de estas cosas está hecha:

* **Comprobar el CRC en cada `GET`** (límite 2). Cuesta rendimiento: habría que
  leer también la clave y la cabecera, no solo el valor. Debería ser opcional.
* **Fregado en segundo plano** que recorra los segmentos y avise de bits podridos
  antes de que los pida un cliente (límite 9).
* **Marca de sincronismo por registro** (un número mágico de 4 u 8 bytes al
  principio). Haría el rastreo del paso 2 inmediato y casi infalible, y
  resolvería el límite 6. **Pero cambia el formato binario**, así que exige un
  número de versión en la cabecera del segmento y un plan de migración: no se
  hace sin ese análisis.
* **Cota configurable para el rastreo** (límite 7).
* **Inyección de fallos de E/S** y pruebas en Linux (límite 10).
