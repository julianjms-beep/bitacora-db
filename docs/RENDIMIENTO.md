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
