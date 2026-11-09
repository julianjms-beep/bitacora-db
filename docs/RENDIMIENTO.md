# Rendimiento: metodología y resultados

Las cifras de aquí se pueden reproducir con una orden, y la orden sale impresa
en la cabecera de cada ejecución. Lo que **no** se ha medido también está dicho.

## Metodología

* **Máquina**: MacBook Air, Apple Silicon (arm64), 10 núcleos, macOS. Una sola
  máquina, un portátil, sin aislamiento de CPU.
* **Carga**: 200.000 operaciones, valores de 100 B, claves de 11 B, segmentos de
  4 MB, 8 hilos salvo donde se indique.
* **Percentiles exactos**: se guarda la latencia de **cada** operación y se
  ordena. No es un histograma por cubos, así que no hay error de aproximación.
* **Qué entra en una medición**: el tiempo completo de la llamada a la API de
  Rust, incluido el `format!` de la clave. Con `group`, un `SET` no vuelve hasta
  que su registro está en disco.
* **GET cruzado**: cada hilo lee las claves escritas por otro, para que las
  lecturas se repartan entre shards en vez de concentrarse.
* **Tres repeticiones** de cada configuración rápida; se publica el rango, no la
  mejor cifra.

```bash
cargo build --release
./target/release/bitacora bench --threads 8 --ops 200000 --fsync group --segment-mb 4
```

## Resultados

Rangos de tres ejecuciones. Latencias en microsegundos.

### Escritura, 8 hilos

| `--fsync` | SET ops/s | p50 | p95 | p99 | p99,9 | máx |
|---|---|---|---|---|---|---|
| `never` | 155.700 – 165.500 | 5,4 | 10 – 12 | **1.290 – 1.388** | 4.177 – 5.578 | 12.959 – 22.702 |
| `always` | 23.830 | 36,8 | 62 | 139 | **44.580** | **915.855** |
| `group` | 49.100 – 52.400 | 150 – 154 | 232 – 270 | **286 – 416** | 430 – 1.185 | 957 – 4.915 |
| `group --fullfsync` | 1.080 | 7.961 | 9.221 | 12.194 | 17.257 | 20.172 |

### Lectura, 8 hilos, `group`

| fase | ops/s | p50 | p95 | p99 | p99,9 |
|---|---|---|---|---|---|
| GET | 996.000 – 1.156.000 | 6,1 – 7,2 | 9,0 – 9,6 | 9,8 – 11,8 | 60 – 204 |
| GET mientras compacta | 1.037.000 – 1.111.000 | 5,0 – 6,8 | 8,0 – 8,7 | 9,1 – 11,2 | **67 – 309** |

### Otras cifras

| | |
|---|---|
| DELETE (`group`, 8 hilos) | 55.400 – 60.600 ops/s · p50 128–139 µs · p99 267–325 µs |
| Índice en RAM | 11,3 MB para 200.000 claves = **59 B por clave** |
| Disco | 31,5 MB (25,2 vivos / 6,3 muertos) antes de compactar |
| Compactación | 7 segmentos retirados · 28,0 MB → 21,7 MB |
| Reapertura | **124 – 138 ms** para 200.000 claves = 1,45 – 1,61 M claves/s |

## Lo que enseñan estas cifras

Cuatro cosas que el rendimiento medio por sí solo no muestra.

**1. El group commit cambia latencia media por cola predecible.** Con `never`,
la mediana es 28 veces mejor (5,4 µs frente a 150) pero el **p99 es 3 ó 4 veces
peor** (1,3 ms frente a 0,3). El motivo: sin `fsync`, las escrituras se acumulan
en la caché del sistema y el núcleo las vacía a ratos, creando parones grandes.
Con group commit, cada escritura espera un `fsync` compartido, así que el coste
se reparte de forma uniforme. Si lo que importa es el peor caso y no el caso
típico, `group` gana **también** en latencia.

**2. `always` tiene la peor cola de todas, y por diseño.** Su mediana es
excelente (36,8 µs, mejor que `group`), pero el `fsync` se hace **dentro del
candado de escritura**, así que un `fsync` lento bloquea a todos los
escritores: p99,9 de 44 ms y un máximo de **916 ms**. Una cifra de rendimiento
medio habría presentado `always` como razonable; el percentil 99,9 dice que no
lo es.

**3. La compactación no daña el caso típico, pero sí la cola.** La mediana y el
p99 de las lecturas no empeoran de forma apreciable —a veces salen mejor, que es
ruido y caché caliente, no una mejora—, pero el **p99,9 pasa de 60–204 µs a
67–309 µs**. Hay interferencia, está en la cola, y se puede cuantificar.

**4. El group commit se amortiza con la concurrencia.** Con `--fullfsync`, cada
`F_FULLFSYNC` cuesta unos 8 ms. Con 8 hilos salen 1.080 escrituras/s; con 64
hilos, **6.616/s con la misma garantía**: seis veces más, porque cada `fsync`
sirve a muchos más escritores a la vez. Es la razón de ser del diseño, medida.

## Lo que costó la observabilidad (Gate 6)

Añadir contadores de operaciones es tocar el camino de cada `put` y cada `get`,
así que se midió antes de publicarlo, comparando el binario del Gate 6 contra el
del Gate 5 **alternados** (A/B/A/B) en la misma máquina y el mismo rato, que es
la única forma de no confundir el cambio con el estado del portátil.

**Lo que salió, y el error que destapó.** Con los contadores en una sola
estructura global, SET con `fsync=never` bajaba de 141-162k a 104-129k
operaciones por segundo (**−21 %**) en las cuatro rondas, siempre en la misma
dirección, mientras el **p50 no se movía** (5,6-6,2 µs frente a 6,1-6,8 µs). Un
coste que no aparece en la mediana pero sí en el rendimiento agregado es la
firma de ocho hilos quitándose unos a otros la misma línea de caché.

**El arreglo** fue repartir los contadores por partición del índice, con la
estructura alineada a 128 bytes para que no comparta línea con el candado del
mapa. Cada hilo toca entonces, normalmente, una línea distinta, y leerlos no
cuesta ninguna pasada extra porque `stats()` ya recorría las particiones.

**Después del arreglo no se puede medir la diferencia en esta máquina**, y
conviene decirlo tal cual en vez de declarar «coste cero». Con 10 rondas
alternadas:

| | mediana SET (`never`) | rango |
|---|---|---|
| Gate 5 | ~153.000 ops/s | 86.000 – 166.000 |
| Gate 6 | ~139.000 ops/s | 89.000 – 164.000 |

El **mismo** binario del Gate 5 osciló entre 86.000 y 166.000 según la ronda: el
ruido del portátil es casi el doble, mucho mayor que cualquier efecto que
quedara. En GET, que es el camino de más frecuencia (un millón de operaciones
por segundo), los dos quedan indistinguibles: 1.008-1.081k frente a
1.010-1.077k. Resolver una diferencia de pocos puntos exigiría una máquina
tranquila y muchas más repeticiones, que es justo lo que esta no es.

De paso, esto es un aviso sobre las cifras de la tabla de arriba: se tomaron en
un rato tranquilo. El mismo binario, en una máquina cargada, da la mitad. Son
reproducibles en método, no en valor absoluto.

## Lo que aportan los lotes, y lo que cuesta el TTL (Gate 7)

**Los lotes.** Escribir en lotes de 100 con `write_batch` frente a operación a
operación, con `fsync=group`, medido en la **misma tirada** cada vez para que el
ruido del portátil afecte a los dos lados por igual:

| tirada | SET suelto | SET en lotes de 100 | veces |
|---|---|---|---|
| 1 | 37.661 ops/s | 631.354 ops/s | 16,8× |
| 2 | 28.187 ops/s | 639.786 ops/s | 22,7× |
| 3 | 40.752 ops/s | 522.606 ops/s | 12,8× |
| 4 | 46.535 ops/s | 819.975 ops/s | 17,6× |

**Entre 13 y 23 veces más rápido**, y la mediana por operación baja de 165-206
µs a 7-11 µs. Es exactamente lo que promete el lote: un `fsync` para todo el
lote en lugar de uno por escritura. Lo que **no** promete es atomicidad, y eso
está dicho en `CAMBIOS.md` y en su documentación.

La proporción varía tanto porque lo que oscila es el SET suelto; el lote es
estable. Por eso se publica el rango de la proporción y no una cifra sola.

**El coste del TTL en RAM.** La caducidad va en el índice para que `exists`,
`len` y `KEYS` no toquen el disco, así que la entrada crece 8 bytes y los pagan
**todas** las claves:

| | por entrada del índice | con claves de 11 B |
|---|---|---|
| Antes del Gate 7 | 24 (Loc) + 16 + 8 = 48 B | 59 B/clave |
| Después | 32 (Loc) + 16 + 8 = 56 B | **68 B/clave** |

Los 68 B/clave son los que publica `STATS` y los que imprime el banco (16,1 MB
para 250.000 claves, constante en las cuatro tiradas). Conviene ser preciso con
lo que esa cifra es: una **estimación por fórmula** —bytes de clave más una
constante por entrada—, no una medida de la memoria residente del proceso, que
este banco sigue sin medir.

## Lo que no se mide, y por qué

* **No hay comparación con Redis.** Una comparación honesta exige igualar la
  garantía de durabilidad (`appendfsync everysec` frente a group commit), el
  tamaño de valor, el número de clientes y el camino de red, y ejecutar las dos
  en la misma máquina en el mismo estado térmico. Publicar un número sin eso
  sería propaganda, no una medición. Lo que sí se puede decir: estas cifras son
  del **motor**, y Redis tiene un camino de red que aquí no se cuenta.
* **No se mide el servidor TCP.** El banco llama a la API de Rust directamente.
  El coste del protocolo de texto, del `BufReader` por conexión y del hilo por
  conexión **no está** en ninguna de estas cifras.
* **Un portátil no es un servidor.** Sin aislamiento de CPU, con límites
  térmicos y con otras cosas corriendo. Los máximos (ese 916 ms, los 90 ms de un
  GET con 64 hilos) incluyen planificación del sistema, no solo el motor.
* **La caché de páginas está caliente.** Los 200.000 valores son 20 MB y caben
  de sobra en RAM, así que los GET casi nunca tocan el disco de verdad. Con un
  conjunto de datos mayor que la memoria las lecturas serían mucho más lentas, y
  esa medición falta.
* **Un solo tamaño de clave y de valor.** No hay barrido de tamaños ni
  distribuciones realistas (zipf, claves calientes).
* **No se mide el consumo de CPU ni la memoria residente del proceso**, solo la
  estimación del índice que publica `STATS`.

## Si alguna vez se publica una comparación

Lo mínimo para que no sea engañosa: misma máquina, mismo estado térmico, misma
garantía de durabilidad declarada en los dos lados, varias repeticiones con
rangos, percentiles y no solo medias, y el método completo publicado al lado de
los números.
