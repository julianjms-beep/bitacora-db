# BITÁCORA · a key-value storage engine written from scratch

A persistent key-value store in **Rust with zero external dependencies** — standard
library only. Write-ahead log, CRC-32 per record, in-memory index sharded 64 ways,
group-commit `fsync`, concurrent compaction, TCP text protocol.

*Bitácora* is Spanish for a ship's logbook: entries are appended in order and never
erased. It is the natural translation of *log*, which is the heart of the design.

[Léeme en español](README.es.md)

---

## The 30-second version

Store three keys, kill the process with `SIGKILL` mid-flight, start it again:

```console
$ bitacora serve --dir data --port 7399 &
$ printf 'SET name Carlos\nSET city Madrid\nSET goals 41\nQUIT\n' | nc 127.0.0.1 7399
OK
OK
OK
BYE

$ kill -9 %1          # pull the plug

$ bitacora serve --dir data --port 7399 &
$ printf 'GET name\nGET goals\nKEYS\nQUIT\n' | nc 127.0.0.1 7399
VAL Carlos
VAL 41
KEYS 3
city
goals
name
BYE
```

Every `OK` you received is still there. That guarantee — and what it costs — is what
this repository is about.

## Design goals

| Goal | How it is met | Where |
|---|---|---|
| OS-level concurrency | One OS thread per TCP connection; index split into 64 shards behind `RwLock`; single writer behind `Mutex`; group commit with a dedicated sync thread | `src/server.rs`, `src/store.rs` |
| Never corrupt on power loss | Append-only WAL with a CRC-32 per record; a torn tail is truncated at startup; configurable `fsync` | `src/wal.rs`, `src/store.rs` |
| Keep RAM proportional to keys, not data | Values live on disk; memory holds only `key → (file, offset, length, seq)` (24 bytes + key); reads are a single `pread`; compaction reclaims space | `src/store.rs` |

---

## Quick start

Requirements: Rust (`cargo`). Nothing else.

```bash
git clone https://github.com/<user>/bitacora-db
cd bitacora-db
cargo build --release

# server (creates ./data)
./target/release/bitacora serve --dir data --port 7379 --fsync group

# in another terminal: the bundled client…
./target/release/bitacora cli
# …or any text client
nc 127.0.0.1 7379
```

The protocol is one command per line:

```
SET user:1 Ana Garcia      → OK
GET user:1                 → VAL Ana Garcia
EXISTS user:1              → INT 1
KEYS user:                 → KEYS 1  /  user:1
DEL user:1                 → OK
GET user:1                 → NIL
STATS                      → STATS 14  /  keys 0 / … / synced_seq 3 / health ok
COMPACT                    → OK segmentos_retirados=1 bytes_antes=132 bytes_despues=68
PING                       → PONG
QUIT                       → BYE
```

`serve` options: `--dir`, `--host`, `--port`, `--fsync group|always|never`,
`--fullfsync`, `--segment-mb` (size of each log file), `--max-conns`,
`--recuperacion estricto|reparar`.

Tests and benchmark:

```bash
cargo test                                   # 59 tests (unit + integration)
./target/release/bitacora bench --threads 8 --ops 200000 --fsync group
```

---

## How it works

### 1. On disk: a log that only grows

Every operation is appended to the **end** of a file (`data/000001.log`,
`000002.log`, …). Nothing is ever modified in place. One record:

```
┌────────┬────────┬─────────┬─────────┬──────┬───────┬───────┐
│ crc32  │ seq    │ key_len │ val_len │ kind │ key   │ value │
│ 4 B    │ 8 B    │ 4 B     │ 4 B     │ 1 B  │ n B   │ m B   │
└────────┴────────┴─────────┴─────────┴──────┴───────┴───────┘
```

* `crc32` covers everything that follows it (own implementation, `src/crc32.rs`).
* `seq` is a global, monotonic counter. State can be rebuilt **without depending on
  file order**, which is why compaction can write into fresh files without breaking
  anything.
* `kind` is PUT or DEL — a *tombstone*: a delete is also a record, because nothing is
  ever removed from the log directly.

**What happens when the power goes out mid-write.** The last record is left
half-written. At startup the reader (`SegmentReader`) checks the length and CRC of
every record; the first one that does not add up is treated as the end of useful
data, the file is truncated at that byte, and startup continues. Everything before it
is intact. The test `sobrevive_a_un_kill_9_a_mitad_de_escritura` launches the binary
writing at full speed, kills it with `SIGKILL` three times, and asserts that the
recovered keys are exactly a prefix `k0..k(n-1)` with the right values: no holes, no
garbage.

### 2. In memory: an index without values

```rust
struct Loc { file_id: u32, vlen: u32, vpos: u64, seq: u64 }   // 24 bytes
HashMap<Box<[u8]>, Loc>                                        // key → Loc
```

A read looks the key up in the index and issues **a single `pread`** for the value at
its exact offset (`read_exact_at`): no scanning, no over-reading, and no lock on the
file — each thread reads with its own offset. The OS keeps hot data in its page
cache, so process RAM grows with the number of keys, not with the size of the data.

Measured: 200,000 keys with 100 B values → **11.8 MB of index** against 26 MB of
data; a restart rebuilds all 200,000 keys in **~130 ms**.

Details that save memory: keys as `Box<[u8]>` (no spare `Vec` capacity), one reusable
buffer per writer, `shrink_to_fit` after recovery, 8 KB buffered TCP readers.

### 3. Concurrency: real threads

* **Server**: `TcpListener` + `thread::spawn` per connection. These are OS threads
  scheduled by the kernel; there is no async runtime. All connections share one `Db`
  through an `Arc`.
* **Reads**: the index is split into 64 shards, each with its own `RwLock`. Two reads
  of different keys almost never contend on the same lock, and readers **never** touch
  the write lock.
* **Writes**: the log is sequential by nature, so there is a single `Mutex<Writer>`.
  Inside the lock: encode, `write`, update the index. Keeping the index update inside
  the same lock guarantees that index order matches log order (the test
  `clave_caliente_memoria_y_log_coinciden` hammers one key from 8 threads and checks
  that after a restart memory and log agree).
* **Group commit**: with `--fsync group`, a `put` writes its record, releases the
  lock, and waits on a `Condvar` for a sync thread to run the `fsync`. That thread
  syncs **once** and wakes everyone waiting: a hundred concurrent writers share a
  single `fsync`. The `fsync` happens outside the write lock so it does not stall
  other writers.
* **Concurrent compaction**: live records from closed segments are rewritten into new
  segments **while reads and writes keep coming in**. For each record it verifies that
  the index still points at it, copies it, and — under the shard lock — moves the
  pointer only if nobody wrote that key in the meantime (a hand-rolled
  compare-and-swap). Old files are removed from the map first and from disk second: a
  reader already holding the descriptor keeps reading correctly, and one arriving
  later consults the index again.

### 4. Durability: three policies

| `--fsync` | What it guarantees | SET/s (8 threads) | SET/s (64 threads) |
|---|---|---|---|
| `never` | Survives the process being killed; a power cut loses whatever the OS held in cache | 151,000 | — |
| `group` | Every `OK` means "it is on disk"; `fsync` calls are shared | 48,000 | 69,000 |
| `always` | `fsync` inside the lock on every write | 27,000 | — |
| `group --fullfsync` | Also flushes the drive's own cache (`F_FULLFSYNC`) | — | 5,700 |
| `always --fullfsync` | As paranoid as it gets | 240 | — |

GET: ~1,000,000 ops/s in every mode (an index lookup plus a `pread`). Measured on an
Apple Silicon MacBook Air with 100 B values, `cargo build --release`.

A note on macOS: `fsync()` sends data to the drive but does not force the drive to
flush its own cache — that is what `F_FULLFSYNC` is for. The Rust standard library
always uses `F_FULLFSYNC`, so this engine calls the system's `fsync()` directly and
leaves the full flush behind `--fullfsync`. The 64-thread row is what group commit
buys you: 240 → 5,700 writes/s at the same guarantee, because each `F_FULLFSYNC`
(≈4 ms) serves about 24 writes at once.

### 5. Compaction

Overwritten and deleted keys leave dead records in the log. When dead bytes pass 50%
(and 16 MB) as a segment is closed, a background compaction starts; it can also be
requested with `COMPACT`. Tombstones are dropped during compaction because **all**
closed segments are always compacted together, so no old PUT can be resurrected
(test `borrados_sobreviven_a_compactacion_y_reinicio`).

---

## Layout

```
src/crc32.rs     own CRC-32 (table built at compile time)
src/wal.rs       record format, encoder, reader with torn-tail detection
src/store.rs     the engine: sharded index, segments, group commit, compaction
src/protocol.rs  text protocol parser
src/server.rs    TCP, one thread per connection
src/main.rs      serve / cli / bench
tests/           recovery (kill -9, torn tail, flipped bit) and concurrency
```

## Corruption is not the same as a torn tail

A record that fails its checks means one of two very different things, and
conflating them costs data:

* **Torn tail** — the file ends in a half-written record and **nothing valid
  follows**. That is what a power cut leaves behind; truncating there loses
  nothing that was ever acknowledged.
* **Corruption** — a record fails to validate but **valid records follow it**.
  Truncating here would destroy acknowledged data, so the reader resynchronises
  and keeps everything after it.

Until 8 November 2026 this engine did not make that distinction, and a single
flipped bit 40% into a segment destroyed 30 of 50 keys *and* truncated the file
from 2,530 to 1,000 bytes. On corruption, recovery now has two modes:

| `--recuperacion` | Behaviour |
|---|---|
| `estricto` (default) | **Refuses to open and modifies no byte.** The error names the file, offset, byte count and cause |
| `reparar` | Opens, skipping unreadable regions, after copying every affected segment to `NNNNNN.log.corrupto-<epoch>`; reports exactly what was lost |

`STATS` publishes `recovery_corrupt_records`, `recovery_corrupt_bytes` and
`recovery_truncated_tails`, so a lossy recovery is never presented as a clean
one. Guarantees per operation, and the ten explicit limits of what recovery
cannot do, are in [docs/RECUPERACION.md](docs/RECUPERACION.md).

## When the disk fails

A failed `fsync` is **not retried**. The database is marked unwritable until
restart; reads keep working. On Linux a failed `fsync` may already have dropped
the dirty pages, so a second `fsync` can return success for data that never
reached the disk — continuing to accept writes after that is promising
durability you cannot deliver. PostgreSQL made the same call in 2018.

Concretely: the durability watermark does **not** advance (it used to, which
made "it is on disk" a lie), writers waiting on a group commit are woken and get
the error rather than hanging, `STATS` reports `health no-escribible (reason)`,
and `put`/`delete` fail with an explicit message. The state belongs to the
process, not the files: a restart comes up writable with whatever actually made
it to disk.

Creating and deleting segments now also `fsync` the containing **directory** —
in POSIX a file name is not durable without it, so a power cut right after
compaction could otherwise leave the new segments with no directory entry and
the old ones already unlinked.

## Typed errors

Every operation returns `Result<T, bitacora::Error>`, with five variants:
`Invalida`, `NoEscribible`, `Corrupcion`, `Interno` and `Io`. Each one answers
the only question a caller really has — **is retrying worth anything?** — via
`Error::reintentable()`, and `Error::categoria()` gives a stable word for logs
and the wire, so a client never has to parse prose. Error lines on the socket
are now `ERR <category> <text>`.

This replaced `io::Error` everywhere, which could not distinguish "your key is
invalid" from "the disk just failed" — the two cases with opposite correct
responses. See [docs/CAMBIOS.md](docs/CAMBIOS.md) for the incompatible changes.

## Hardening the exposed surface

The engine can be flawless and the server still be the fragile part. What is in
place, and what it prevents:

| Guard | Without it |
|---|---|
| Read and write timeouts | A client that opens a socket and sends nothing holds a thread forever; `--max-conns` mute connections make the server unreachable without sending a useful byte |
| `KEYS` cap, with a `truncado` marker | One `KEYS` over 200,000 keys builds and sorts a 200,000-element list per client |
| Thread-spawn failure is not fatal | Under thread exhaustion the whole server died instead of refusing that one connection |
| Atomic connection admission | `load` then `fetch_add` is not atomic, so the limit could be exceeded |
| Newlines refused on the wire | The protocol is line-based, so a value containing `\n` made the client read two replies where there was one |
| Graceful shutdown on SIGINT/SIGTERM | Ctrl-C killed the process mid-flight: no final `fsync`, compaction cut wherever it was |
| Response size cap | A 1 GiB value meant 1 GiB reserved per concurrent read |
| Optional `AUTH`, and refusing to listen off-localhost without it | Anyone who could reach the port had read, write and `COMPACT` |

`AUTH` takes a shared secret, compared in constant time, from `--clave` or the
`BITACORA_CLAVE` environment variable so it does not sit in the process list.

## How this is kept honest

CI runs on every push, on **both Linux and macOS**: `cargo fmt --check`,
`clippy --all-targets -- -D warnings` (no warnings, not "few warnings"), the
full suite, and the suite again single-threaded so test interdependence shows
up. The cross-platform job earns its place — the engine was written on macOS,
and the Linux job compiles the `#[cfg(not(target_os = "macos"))]` paths that
never build locally. A type error was hiding in exactly one of them.

The property tests use a fixed seed so `cargo test` is reproducible, and CI
passes `BITACORA_SEMILLA=<run number>` so every run explores a different slice
of the random space instead of re-checking the same 965 cases. A failure prints
its seed and iteration, so it replays exactly.

## Tests (59)

* `crc32`: standard vector, chunked computation.
* `wal`: round trip, truncated tail, flipped bit.
* `protocol`: parsing and errors.
* Recovery: a **real `kill -9`** mid-write (×3), torn tail truncated, deletes +
  compaction + restart, binary keys and values, and size limits.
* Corruption (`tests/corrupcion.rs`, 10 tests): a flipped bit at the start, the
  middle and three quarters of a segment; two corruptions in one segment; a
  wrecked header; a mid-file truncation; a segment of pure garbage. Each one
  asserts that the **valid records after the damage survive**, that strict mode
  refuses to open and leaves the file byte-identical, and that repair mode
  quarantines the original first.
* Concurrency: 8 threads × 3,000 writes with interleaved reads and a restart, one hot
  key from 8 threads staying consistent with the log, compaction while reading and
  writing, group commit with 16 threads (`synced_seq == last_seq` at the end).
* Properties and random sweep (`tests/propiedades.rs`, 4 tests): every record
  round-trips identically; the reader never panics on arbitrary bytes **and
  always advances** (without that, resynchronisation could loop forever);
  recovery never *fabricates* data, only loses it; and the engine matches a
  `HashMap` model across a random operation sequence, before and after restart.
  48,000 cases across five seeds have been run by hand on top of CI.
* Server hardening (`tests/servidor.rs`, 8 tests): operating before `AUTH`,
  a wrong key, refusing to bind off-localhost without one, the `KEYS` cap and
  its ordering, values with newlines in both directions, the response size cap,
  the connection limit and that slots are released, a mute connection being
  timed out, and `serve` returning on request.
* Durability under disk failure (`tests/durabilidad.rs`, 6 tests, via injected
  `fsync` errors): the watermark does not advance past a failed sync, the writer
  gets the error instead of hanging, **all** waiting writers get it, a failure
  while rolling a segment propagates, and a restart comes up writable again.

## Design decisions and limits

* **Why Rust?** Native OS threads, no garbage collector (the RAM you see is the RAM
  you have), explicit control over `fsync`, and a compiler that rules out data races:
  every shared access goes through `Mutex`/`RwLock`/atomics.
* **Why Bitcask-style (index in RAM, values on disk) instead of a B-tree or an LSM
  tree?** It is the simplest design that gives sequential writes, a single disk read
  per `GET`, and trivial recovery. The cost: every key must fit in RAM, and startup
  reads the whole log (200k keys in ~130 ms; at 50M keys this would need hint files).
* **Visibility vs durability**: with `group`, a write becomes visible to other threads
  as soon as it enters the index, but the client only gets `OK` once it is on disk.
  This is the same trade-off Redis makes with AOF `everysec`, and most engines with
  group commit.
* **Line-based text protocol**: values cannot contain newlines over the wire (the Rust
  API is binary and has no such limit). Chosen so the server can be driven with `nc`.
* **One thread per connection** scales to hundreds of clients, not a hundred thousand;
  that would call for a thread pool or `epoll`/`kqueue`. Bounded by `--max-conns`.
* **No multi-key transactions and no TTL** — deliberately out of scope.

## License

MIT — see [LICENSE](LICENSE). Use it, change it, ship it; just keep the copyright notice.
