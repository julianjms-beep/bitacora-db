//! El motor. Diseño tipo Bitcask:
//!
//! - **Disco**: segmentos de log append-only (`000001.log`, `000002.log`...).
//!   Solo se escribe al final del segmento activo; nunca se modifica nada
//!   in situ. Eso es lo que hace al WAL resistente a apagones.
//! - **Memoria**: un índice `clave -> (fichero, posición, longitud, seq)` de
//!   24 bytes por entrada más la clave. Los valores NUNCA viven en RAM: se leen
//!   del disco con `pread` cuando hacen falta. La RAM crece con el número de
//!   claves, no con el tamaño de los datos.
//! - **Concurrencia**: el índice está partido en `shards`, cada uno con su
//!   propio `RwLock`, así las lecturas de claves distintas no se estorban.
//!   Las escrituras pasan por un único `Mutex` (el log es secuencial por
//!   naturaleza) y los lectores nunca lo tocan.
//! - **Durabilidad**: `fsync` por grupos (group commit): un hilo sincroniza
//!   el fichero y despierta a todos los escritores que esperaban. Muchas
//!   escrituras concurrentes comparten un solo `fsync`.
//! - **Compactación**: reescribe solo los registros vivos de los segmentos
//!   cerrados en segmentos nuevos, con lecturas y escrituras en marcha.

use crate::error::{Error, Result};
use crate::wal::{self, Kind, ReadOutcome, SegmentReader, HEADER, MAX_KEY, MAX_VALUE};
use std::collections::{BinaryHeap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Toma un `Mutex` recuperándose del envenenamiento.
///
/// A6: con `.lock().unwrap()`, un único panic con el candado tomado convierte
/// el proceso en inservible, porque todas las operaciones posteriores entran en
/// panic al tocar ese candado. En un servidor eso transforma un fallo aislado
/// en una caída total que además sigue aceptando conexiones.
///
/// **Límite honesto:** si el hilo que entró en panic dejó el estado a medias,
/// sigue a medias. Recuperarse evita la cascada; no arregla los datos. Tras un
/// panic conviene reiniciar. El servidor además atrapa el panic en el borde de
/// cada conexión, para que en el caso habitual no llegue a envenenar nada.
fn lk<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// Lectura compartida de un `RwLock`, recuperándose del envenenamiento. Ver [`lk`].
fn rd<T>(l: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    l.read().unwrap_or_else(|e| e.into_inner())
}

/// Escritura exclusiva de un `RwLock`, recuperándose del envenenamiento. Ver [`lk`].
fn wr<T>(l: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    l.write().unwrap_or_else(|e| e.into_inner())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Nunca llama a fsync: rapidísimo, pero un apagón pierde lo que el SO
    /// aún tenía en caché. Matar el proceso NO pierde nada.
    Never,
    /// fsync en cada escritura, dentro del candado: lo más seguro y lo más lento.
    Always,
    /// Group commit: una escritura devuelve OK cuando su registro está en
    /// disco, pero muchas escrituras concurrentes comparten el mismo fsync.
    Group,
}

impl FsyncPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "never" | "nunca" => Some(FsyncPolicy::Never),
            "always" | "siempre" => Some(FsyncPolicy::Always),
            "group" | "grupo" => Some(FsyncPolicy::Group),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            FsyncPolicy::Never => "never",
            FsyncPolicy::Always => "always",
            FsyncPolicy::Group => "group",
        }
    }
}

/// Qué hacer si al abrir se detecta **corrupción** (un registro ilegible con
/// registros válidos detrás), que es distinto de una cola de escritura
/// incompleta por un apagón.
///
/// Una cola incompleta es el caso normal tras un fallo y se recorta siempre:
/// no hay nada confirmado detrás que perder. La corrupción no.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryPolicy {
    /// **Por defecto.** Si hay corrupción, `open` falla y **no modifica ningún
    /// fichero**. Una recuperación incompleta no se presenta nunca como
    /// satisfactoria.
    Estricto,
    /// Abre saltándose los registros ilegibles, después de copiar cada
    /// segmento afectado a `NNNNNN.log.corrupto-<epoch>`. El informe
    /// ([`Db::recovery_report`]) dice exactamente qué se perdió.
    Reparar,
}

impl RecoveryPolicy {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "estricto" | "strict" => Some(RecoveryPolicy::Estricto),
            "reparar" | "repair" => Some(RecoveryPolicy::Reparar),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            RecoveryPolicy::Estricto => "estricto",
            RecoveryPolicy::Reparar => "reparar",
        }
    }
}

/// Qué pasó exactamente al reconstruir el índice. Se consulta con
/// [`Db::recovery_report`] y sale en `STATS`.
#[derive(Clone, Debug, Default)]
pub struct RecoveryReport {
    pub segments: u64,
    pub records_ok: u64,
    /// Registros ilegibles que se saltaron (solo en modo `Reparar`).
    pub corrupt_records: u64,
    /// Bytes ilegibles saltados.
    pub corrupt_bytes: u64,
    /// Colas de escritura incompleta recortadas: `(fichero, byte)`.
    pub truncated_tails: Vec<(PathBuf, u64)>,
    /// Copias de seguridad hechas antes de tocar nada.
    pub quarantined: Vec<PathBuf>,
    /// Detalle legible de cada zona ilegible.
    pub detalle: Vec<String>,
}

impl RecoveryReport {
    /// `true` si se perdió algún registro válido. Si es `true`, la
    /// recuperación **no** fue íntegra.
    pub fn con_perdidas(&self) -> bool {
        self.corrupt_records > 0
    }
}

#[derive(Clone, Debug)]
pub struct Options {
    pub dir: PathBuf,
    pub fsync: FsyncPolicy,
    /// `false`: `fsync()` del sistema. `true`: además vacía la caché del propio
    /// disco (`F_FULLFSYNC` en macOS, `sync_all` en Linux); mucho más lento.
    pub full_fsync: bool,
    /// Tamaño a partir del cual se cierra el segmento activo y se abre otro.
    pub max_segment_bytes: u64,
    /// Número de particiones del índice (más shards = menos contención).
    pub shards: usize,
    /// Tiempo máximo que espera el hilo de sincronización antes de hacer fsync.
    pub group_interval: Duration,
    pub auto_compact: bool,
    pub compact_min_dead_bytes: u64,
    pub compact_dead_ratio: f64,
    /// Qué hacer ante corrupción al abrir. Ver [`RecoveryPolicy`].
    pub recovery: RecoveryPolicy,
    /// Plazo máximo que `get` reintenta cuando la compactación acaba de
    /// retirar el segmento al que apuntaba el índice. Agotarlo indica un fallo
    /// interno, no una clave ausente.
    pub read_retry_timeout: Duration,
    /// **Inyección de fallos, solo para pruebas.** Hace que el `fsync` número
    /// `N+1` falle, para poder comprobar qué hace el motor cuando el disco
    /// falla de verdad. En producción se deja en `None`.
    #[doc(hidden)]
    pub fsync_falla_tras: Option<u64>,
}

impl Options {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Options {
            dir: dir.into(),
            fsync: FsyncPolicy::Group,
            full_fsync: false,
            max_segment_bytes: 64 << 20,
            shards: 64,
            group_interval: Duration::from_millis(2),
            auto_compact: true,
            compact_min_dead_bytes: 16 << 20,
            compact_dead_ratio: 0.5,
            recovery: RecoveryPolicy::Estricto,
            read_retry_timeout: Duration::from_secs(2),
            fsync_falla_tras: None,
        }
    }
}

/// Marca temporal de "borrado" en el índice mientras se reconstruye el estado.
const TOMB: u32 = u32::MAX;

/// Estado de durabilidad del motor.
struct SyncState {
    /// Último `seq` garantizado en disco.
    seq: u64,
    /// Si un `fsync` falló, por qué. **Es definitivo**: no se limpia.
    ///
    /// Un `fsync` fallido no se puede reintentar con criterio. En Linux, un
    /// `fsync` que devuelve error puede haber descartado ya las páginas sucias,
    /// de modo que un segundo `fsync` devuelve éxito sin que el dato haya
    /// llegado nunca al disco. Seguir aceptando escrituras después de eso es
    /// prometer durabilidad en falso, así que el motor se declara no
    /// escribible y exige reiniciar: al reabrir se lee lo que de verdad hay en
    /// disco. Es la misma decisión que tomó PostgreSQL en 2018.
    failed: Option<String>,
}

/// Dónde está el valor de una clave. 24 bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Loc {
    file_id: u32,
    vlen: u32,
    vpos: u64,
    seq: u64,
}

#[inline]
fn rec_len(klen: usize, vlen: u32) -> u64 {
    (HEADER + klen) as u64 + vlen as u64
}

fn fnv1a(key: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

struct Shard {
    map: RwLock<HashMap<Box<[u8]>, Loc>>,
}

impl Shard {
    fn set(&self, key: &[u8], loc: Loc) -> Option<Loc> {
        let mut m = wr(&self.map);
        match m.get_mut(key) {
            Some(e) => Some(std::mem::replace(e, loc)),
            None => {
                m.insert(Box::from(key), loc);
                None
            }
        }
    }
}

struct Writer {
    id: u32,
    file: Arc<File>,
    len: u64,
    next_seq: u64,
    next_id: u32,
    buf: Vec<u8>,
}

struct Inner {
    opts: Options,
    shards: Vec<Shard>,
    /// Descriptores de lectura de todos los segmentos (incluido el activo).
    segments: RwLock<HashMap<u32, Arc<File>>>,
    writer: Mutex<Writer>,
    /// Estado de durabilidad: hasta dónde está en disco y si el disco falló.
    /// Las dos cosas van bajo el **mismo** candado a propósito: `wait_synced`
    /// espera sosteniéndolo, así que un estado de fallo en otro candado daría
    /// una inversión de orden y podría bloquear.
    synced: Mutex<SyncState>,
    synced_cv: Condvar,
    sync_req: Mutex<bool>,
    sync_req_cv: Condvar,
    /// Cuántos `fsync` se han hecho. Solo se cuenta con inyección de fallos.
    syncs_hechos: AtomicU64,
    stop: AtomicBool,
    compacting: AtomicBool,
    live: AtomicU64,
    dead: AtomicU64,
    /// Qué pasó al reconstruir el índice al abrir.
    recovery: RecoveryReport,
}

pub struct Db {
    inner: Arc<Inner>,
    syncer: Mutex<Option<JoinHandle<()>>>,
    closed: AtomicBool,
}

#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub keys: u64,
    pub segments: u64,
    pub disk_bytes: u64,
    pub live_bytes: u64,
    pub dead_bytes: u64,
    pub index_bytes_est: u64,
    pub last_seq: u64,
    pub synced_seq: u64,
    pub fsync: String,
    pub compacting: bool,
    /// `ok`, o `no-escribible (motivo)` si un `fsync` falló.
    pub health: String,
    /// Zonas ilegibles saltadas al abrir. Si no es 0, **se perdieron datos**.
    pub recovery_corrupt_records: u64,
    pub recovery_corrupt_bytes: u64,
    pub recovery_truncated_tails: u64,
}

impl Stats {
    pub fn lines(&self) -> Vec<String> {
        vec![
            format!("keys {}", self.keys),
            format!("segments {}", self.segments),
            format!("disk_bytes {}", self.disk_bytes),
            format!("live_bytes {}", self.live_bytes),
            format!("dead_bytes {}", self.dead_bytes),
            format!("index_bytes_est {}", self.index_bytes_est),
            format!("last_seq {}", self.last_seq),
            format!("synced_seq {}", self.synced_seq),
            format!("fsync {}", self.fsync),
            format!("compacting {}", self.compacting),
            format!("health {}", self.health),
            format!("recovery_corrupt_records {}", self.recovery_corrupt_records),
            format!("recovery_corrupt_bytes {}", self.recovery_corrupt_bytes),
            format!("recovery_truncated_tails {}", self.recovery_truncated_tails),
        ]
    }
}

#[derive(Clone, Debug, Default)]
pub struct CompactReport {
    pub ran: bool,
    pub segments_removed: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

fn seg_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("{id:06}.log"))
}

fn open_segment(dir: &Path, id: u32) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(seg_path(dir, id))
}

fn validate(key: &[u8], value: Option<&[u8]>) -> Result<()> {
    if key.is_empty() || key.len() > MAX_KEY {
        return Err(Error::Invalida("clave vacía o de más de 64 KiB"));
    }
    if let Some(v) = value {
        // M8: el lector acepta `vlen <= MAX_VALUE`, así que el escritor tiene
        // que rechazar exactamente lo mismo. Antes rechazaba `>=`, de modo que
        // un valor de 1 GiB justos era ilegal al escribir y legal al leer.
        if v.len() > MAX_VALUE {
            return Err(Error::Invalida("valor de más de 1 GiB"));
        }
    }
    Ok(())
}

/// Resta sin poder envolverse por abajo. M2: `fetch_sub` a secas podía dejar el
/// contador en 18 exabytes si dos hilos descontaban el mismo registro, y esos
/// contadores deciden cuándo se compacta.
fn sub_sat(a: &AtomicU64, v: u64) {
    let _ = a.try_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
        Some(cur.saturating_sub(v))
    });
}

/// En macOS la librería estándar de Rust convierte `sync_data`/`sync_all` en
/// `F_FULLFSYNC` (vacía también la caché del disco; ~4 ms). Para poder elegir,
/// llamamos nosotros al `fsync()` del sistema (rápido, los datos llegan al
/// disco pero pueden quedarse en su caché) o a `F_FULLFSYNC` (lento, seguro
/// incluso si se va la luz en el propio disco).
#[cfg(target_os = "macos")]
fn sync_fd(f: &File, full: bool) -> io::Result<()> {
    use std::os::unix::io::AsRawFd;
    extern "C" {
        fn fsync(fd: i32) -> i32;
        fn fcntl(fd: i32, cmd: i32, ...) -> i32;
    }
    const F_FULLFSYNC: i32 = 51;
    // SAFETY: el descriptor es válido mientras viva `f`; F_FULLFSYNC no toma
    // argumentos extra.
    let r = unsafe {
        if full {
            fcntl(f.as_raw_fd(), F_FULLFSYNC)
        } else {
            fsync(f.as_raw_fd())
        }
    };
    if r == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(target_os = "macos"))]
fn sync_fd(f: &File, full: bool) -> io::Result<()> {
    if full {
        f.sync_all()
    } else {
        f.sync_data()
    }
}

/// Sincroniza el **directorio**, no un fichero.
///
/// En POSIX, crear o borrar un fichero no es durable hasta que se sincroniza el
/// directorio que lo contiene. Sin esto, un apagón justo después de compactar
/// puede dejar los segmentos nuevos sin entrada de directorio y los viejos ya
/// borrados: pérdida total.
///
/// Se usa `fsync` y nunca `F_FULLFSYNC`, porque lo que hay que hacer durable
/// son los metadatos del directorio, y `F_FULLFSYNC` está pensado para el
/// contenido de los ficheros.
fn sync_dir(dir: &Path) -> io::Result<()> {
    let f = File::open(dir)?;
    #[cfg(target_os = "macos")]
    {
        sync_fd(&f, false)
    }
    #[cfg(not(target_os = "macos"))]
    {
        f.sync_all()
    }
}

impl Db {
    pub fn open_default(dir: impl Into<PathBuf>) -> Result<Db> {
        Db::open(Options::new(dir))
    }

    /// Abre (o crea) la base de datos y reconstruye el índice leyendo todos
    /// los segmentos. Si el último registro de un fichero está a medias, lo
    /// recorta: así se recupera de un apagón.
    pub fn open(opts: Options) -> Result<Db> {
        fs::create_dir_all(&opts.dir)?;
        let mut ids: Vec<u32> = Vec::new();
        for entry in fs::read_dir(&opts.dir)? {
            let name = entry?.file_name();
            let name = name.to_string_lossy();
            if let Some(stem) = name.strip_suffix(".log") {
                if let Ok(id) = stem.parse::<u32>() {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();

        let nsh = opts.shards.max(1);
        let shards: Vec<Shard> = (0..nsh)
            .map(|_| Shard {
                map: RwLock::new(HashMap::new()),
            })
            .collect();
        let mut segments: HashMap<u32, Arc<File>> = HashMap::new();
        let mut next_seq = 1u64;
        let mut total = 0u64;

        let mut report = RecoveryReport {
            segments: ids.len() as u64,
            ..Default::default()
        };
        // Todo lo destructivo se aplaza hasta saber si hay corrupción: en modo
        // estricto no se modifica ni un byte de los datos del usuario.
        let mut pending_truncate: Vec<(PathBuf, Arc<File>, u64)> = Vec::new();
        let mut corruptos_en: Vec<u32> = Vec::new();

        for &id in &ids {
            let path = seg_path(&opts.dir, id);
            let file = Arc::new(open_segment(&opts.dir, id)?);
            let mut len = file.metadata()?.len();
            let mut lector =
                SegmentReader::new(BufReader::with_capacity(1 << 16, file.try_clone()?), len);
            loop {
                match lector.siguiente()? {
                    ReadOutcome::End => break,
                    ReadOutcome::IncompleteTail { at, cause } => {
                        report.truncated_tails.push((path.clone(), at));
                        report.detalle.push(format!(
                            "{}: cola de escritura incompleta en el byte {at} ({}); se recortan {} byte(s) nunca confirmados",
                            path.display(),
                            cause.name(),
                            len - at
                        ));
                        pending_truncate.push((path.clone(), Arc::clone(&file), at));
                        len = at;
                        break;
                    }
                    ReadOutcome::Corrupt {
                        at,
                        resync_at,
                        cause,
                        por_cabecera,
                    } => {
                        report.corrupt_records += 1;
                        report.corrupt_bytes += resync_at - at;
                        if !corruptos_en.contains(&id) {
                            corruptos_en.push(id);
                        }
                        report.detalle.push(format!(
                            "{}: CORRUPCIÓN en los bytes {at}..{resync_at} ({}); sincronismo recuperado {}; los registros posteriores se conservan",
                            path.display(),
                            cause.name(),
                            if por_cabecera { "por la cabecera" } else { "rastreando byte a byte" }
                        ));
                        continue;
                    }
                    ReadOutcome::Record(r) => {
                        report.records_ok += 1;
                        if r.seq >= next_seq {
                            next_seq = r.seq + 1;
                        }
                        let vlen = if r.kind == Kind::Put {
                            r.value.len() as u32
                        } else {
                            TOMB
                        };
                        let loc = Loc {
                            file_id: id,
                            vlen,
                            vpos: r.vpos,
                            seq: r.seq,
                        };
                        let sh = &shards[(fnv1a(&r.key) as usize) % nsh];
                        let mut m = wr(&sh.map);
                        match m.get_mut(&r.key[..]) {
                            Some(e) => {
                                if r.seq > e.seq {
                                    *e = loc;
                                }
                            }
                            None => {
                                m.insert(r.key.into_boxed_slice(), loc);
                            }
                        }
                    }
                }
            }
            total += len;
            segments.insert(id, file);
        }

        // La corrupción no se trata nunca como un incidente menor.
        if !corruptos_en.is_empty() {
            match opts.recovery {
                RecoveryPolicy::Estricto => {
                    let mut msg = format!(
                        "recuperación abortada: {} zona(s) ilegible(s), {} byte(s), en {} segmento(s). \
                         NO se ha modificado ningún fichero.\n",
                        report.corrupt_records,
                        report.corrupt_bytes,
                        corruptos_en.len()
                    );
                    for d in &report.detalle {
                        msg.push_str("  - ");
                        msg.push_str(d);
                        msg.push('\n');
                    }
                    msg.push_str(
                        "  Para abrir saltándose lo ilegible (copiando antes cada segmento afectado): --recuperacion reparar",
                    );
                    return Err(Error::Corrupcion(msg));
                }
                RecoveryPolicy::Reparar => {
                    let ts = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    // Preservar el original ANTES de cualquier cambio.
                    for id in &corruptos_en {
                        let src = seg_path(&opts.dir, *id);
                        let dst = opts.dir.join(format!("{id:06}.log.corrupto-{ts}"));
                        fs::copy(&src, &dst)?;
                        report.quarantined.push(dst);
                    }
                    // La copia de seguridad no sirve de nada si no es durable.
                    sync_dir(&opts.dir)?;
                    eprintln!(
                        "bitacora: AVISO — recuperación CON PÉRDIDAS: {} zona(s) ilegible(s), {} byte(s). \
                         Copia previa de {} segmento(s) en .corrupto-{ts}",
                        report.corrupt_records,
                        report.corrupt_bytes,
                        report.quarantined.len()
                    );
                    for d in &report.detalle {
                        eprintln!("bitacora:   - {d}");
                    }
                }
            }
        }

        // Solo ahora se toca el disco, y solo para quitar colas que nunca se
        // confirmaron a ningún cliente.
        for (path, file, at) in pending_truncate {
            eprintln!(
                "bitacora: {} acaba en una escritura incompleta; se recorta en el byte {at}",
                path.display()
            );
            file.set_len(at)?;
            file.sync_all()?;
        }

        // Las lápidas ya han hecho su trabajo: fuera del índice.
        let mut live = 0u64;
        for s in &shards {
            let mut m = wr(&s.map);
            m.retain(|_, l| l.vlen != TOMB);
            for (k, l) in m.iter() {
                live += rec_len(k.len(), l.vlen);
            }
            m.shrink_to_fit();
        }

        // Segmento activo: reutiliza el último si está vacío, si no crea uno nuevo.
        let (active_id, active_file) = match ids.last() {
            Some(&last) if segments[&last].metadata()?.len() == 0 => {
                (last, Arc::clone(&segments[&last]))
            }
            _ => {
                let id = ids.last().map_or(1, |l| l + 1);
                let f = Arc::new(open_segment(&opts.dir, id)?);
                sync_dir(&opts.dir)?; // C3
                segments.insert(id, Arc::clone(&f));
                (id, f)
            }
        };

        let fsync = opts.fsync;
        let inner = Arc::new(Inner {
            opts,
            shards,
            segments: RwLock::new(segments),
            writer: Mutex::new(Writer {
                id: active_id,
                file: active_file,
                len: 0,
                next_seq,
                next_id: active_id + 1,
                buf: Vec::with_capacity(4096),
            }),
            synced: Mutex::new(SyncState {
                seq: next_seq - 1,
                failed: None,
            }),
            synced_cv: Condvar::new(),
            sync_req: Mutex::new(false),
            sync_req_cv: Condvar::new(),
            syncs_hechos: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            compacting: AtomicBool::new(false),
            live: AtomicU64::new(live),
            dead: AtomicU64::new(total.saturating_sub(live)),
            recovery: report,
        });

        let syncer = if fsync == FsyncPolicy::Group {
            let me = Arc::clone(&inner);
            Some(
                thread::Builder::new()
                    .name("bitacora-sync".into())
                    .spawn(move || syncer_loop(me))?,
            )
        } else {
            None
        };

        Ok(Db {
            inner,
            syncer: Mutex::new(syncer),
            closed: AtomicBool::new(false),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        validate(key, Some(value))?;
        let inner = &*self.inner;
        inner.check_writable()?;
        let seq;
        {
            let mut w = lk(&inner.writer);
            let rlen = rec_len(key.len(), value.len() as u32);
            if w.len > 0 && w.len + rlen > inner.opts.max_segment_bytes {
                self.inner.roll(&mut w)?;
            }
            seq = w.next_seq;
            let start = w.len;
            {
                let Writer { file, buf, .. } = &mut *w;
                buf.clear();
                wal::encode(buf, seq, Kind::Put, key, value);
                let mut f: &File = file;
                if let Err(e) = f.write_all(buf) {
                    // Escritura a medias. Hay que deshacerla, porque dejar un
                    // registro roto en MEDIO del log hace que al reabrir se
                    // pierda todo lo posterior (ver docs/RECUPERACION.md).
                    if let Err(e2) = file.set_len(start) {
                        // A1: no se ha podido deshacer. Seguir escribiendo en
                        // este segmento garantizaría basura intermedia, así que
                        // la base deja de aceptar escrituras.
                        inner.mark_failed(&e2);
                        return Err(inner.no_escribible(&format!(
                            "no se pudo deshacer una escritura parcial en el byte {start}: \
                             {e2} (el fallo original fue: {e})"
                        )));
                    }
                    return Err(e.into());
                }
            }
            w.next_seq += 1;
            w.len = start + rlen;
            if inner.opts.fsync == FsyncPolicy::Always {
                inner.sync_file_or_fail(&w.file)?;
                inner.mark_synced(seq);
            }
            let loc = Loc {
                file_id: w.id,
                vlen: value.len() as u32,
                vpos: start + (HEADER + key.len()) as u64,
                seq,
            };
            let old = inner.shard(key).set(key, loc);
            inner.live.fetch_add(rlen, Ordering::Relaxed);
            if let Some(o) = old {
                let ol = rec_len(key.len(), o.vlen);
                sub_sat(&inner.live, ol);
                inner.dead.fetch_add(ol, Ordering::Relaxed);
            }
        }
        if inner.opts.fsync == FsyncPolicy::Group {
            inner.wait_synced(seq)?;
        }
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let inner = &*self.inner;
        let shard = inner.shard(key);
        // La compactación puede retirar un segmento entre que se consulta el
        // índice y que se abre el fichero. El índice ya apunta al sitio nuevo,
        // así que basta con volver a mirar.
        //
        // M1: antes eran 32 vueltas de espera activa y, al agotarlas, un error
        // genérico aunque el dato existiera. Ahora hay un plazo de verdad, se
        // cede la CPU en vez de quemarla, y el error dice que es un fallo
        // interno y no una clave ausente.
        let limite = Instant::now() + inner.opts.read_retry_timeout;
        let mut vueltas = 0u32;
        loop {
            let loc = match rd(&shard.map).get(key) {
                Some(l) => *l,
                None => return Ok(None),
            };
            if let Some(file) = rd(&inner.segments).get(&loc.file_id).map(Arc::clone) {
                let mut buf = vec![0u8; loc.vlen as usize];
                if loc.vlen > 0 {
                    file.read_exact_at(&mut buf, loc.vpos)?;
                }
                return Ok(Some(buf));
            }
            vueltas += 1;
            if Instant::now() >= limite {
                return Err(Error::Interno(format!(
                    "la clave está en el índice pero su segmento \
                     ({}) desapareció y no reapareció en {:?} ({vueltas} intentos). \
                     Esto no significa que la clave no exista",
                    loc.file_id, inner.opts.read_retry_timeout
                )));
            }
            if vueltas < 64 {
                thread::yield_now();
            } else {
                thread::sleep(Duration::from_micros(200));
            }
        }
    }

    /// Qué pasó al reconstruir el índice al abrir: registros leídos, zonas
    /// ilegibles saltadas, colas recortadas y copias de seguridad hechas.
    /// `con_perdidas()` dice si la recuperación fue íntegra o no.
    pub fn recovery_report(&self) -> &RecoveryReport {
        &self.inner.recovery
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        rd(&self.inner.shard(key).map).contains_key(key)
    }

    /// Devuelve `true` si la clave existía. Escribe una lápida en el log.
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        validate(key, None)?;
        let inner = &*self.inner;
        inner.check_writable()?;
        let seq;
        {
            let mut w = lk(&inner.writer);
            let shard = inner.shard(key);
            let old = match rd(&shard.map).get(key) {
                Some(l) => *l,
                None => return Ok(false),
            };
            let rlen = rec_len(key.len(), 0);
            if w.len > 0 && w.len + rlen > inner.opts.max_segment_bytes {
                self.inner.roll(&mut w)?;
            }
            seq = w.next_seq;
            let start = w.len;
            {
                let Writer { file, buf, .. } = &mut *w;
                buf.clear();
                wal::encode(buf, seq, Kind::Del, key, &[]);
                let mut f: &File = file;
                if let Err(e) = f.write_all(buf) {
                    // Mismo razonamiento que en `put`: ver A1.
                    if let Err(e2) = file.set_len(start) {
                        inner.mark_failed(&e2);
                        return Err(inner.no_escribible(&format!(
                            "no se pudo deshacer una lápida parcial en el byte {start}: \
                             {e2} (el fallo original fue: {e})"
                        )));
                    }
                    return Err(e.into());
                }
            }
            w.next_seq += 1;
            w.len = start + rlen;
            if inner.opts.fsync == FsyncPolicy::Always {
                inner.sync_file_or_fail(&w.file)?;
                inner.mark_synced(seq);
            }
            wr(&shard.map).remove(key);
            let ol = rec_len(key.len(), old.vlen);
            sub_sat(&inner.live, ol);
            inner.dead.fetch_add(ol + rlen, Ordering::Relaxed);
        }
        if inner.opts.fsync == FsyncPolicy::Group {
            inner.wait_synced(seq)?;
        }
        Ok(true)
    }

    /// Las `limite` primeras claves que empiezan por `prefix`, en orden
    /// alfabético. Devuelve además si había más.
    ///
    /// A3: antes copiaba **todas** las claves a un `Vec` y lo ordenaba. Con
    /// 200.000 claves eso es un vector de 200.000 elementos y un `sort` por
    /// cada cliente que lo pidiera, sin ningún tope. Ahora se usa un montículo
    /// acotado: la memoria es O(limite) y el resultado sigue siendo las
    /// `limite` primeras **en orden**, no las primeras que se encuentren, que
    /// con 64 shards sería un orden arbitrario.
    pub fn keys(&self, prefix: &[u8], limite: usize) -> (Vec<Vec<u8>>, bool) {
        let mut heap: BinaryHeap<Vec<u8>> = BinaryHeap::new();
        let mut total = 0usize;
        for s in &self.inner.shards {
            let m = rd(&s.map);
            for k in m.keys() {
                if !k.starts_with(prefix) {
                    continue;
                }
                total += 1;
                if limite == 0 {
                    continue;
                }
                heap.push(k.to_vec());
                if heap.len() > limite {
                    heap.pop(); // fuera la mayor: nos quedamos con las menores
                }
            }
        }
        let mut out = heap.into_vec();
        out.sort();
        (out, total > limite)
    }

    pub fn len(&self) -> usize {
        self.inner.shards.iter().map(|s| rd(&s.map).len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn stats(&self) -> Stats {
        let inner = &*self.inner;
        let mut keys = 0u64;
        let mut key_bytes = 0u64;
        for s in &inner.shards {
            let m = rd(&s.map);
            keys += m.len() as u64;
            for k in m.keys() {
                key_bytes += k.len() as u64;
            }
        }
        let (segments, disk_bytes) = {
            let segs = rd(&inner.segments);
            let disk = segs
                .values()
                .map(|f| f.metadata().map(|m| m.len()).unwrap_or(0))
                .sum();
            (segs.len() as u64, disk)
        };
        // Ambos bajo el candado de escritura: así `synced_seq <= last_seq` siempre.
        let (last_seq, synced_seq, health) = {
            let w = lk(&inner.writer);
            let s = lk(&inner.synced);
            let health = match &s.failed {
                None => "ok".to_string(),
                Some(m) => format!("no-escribible ({m})"),
            };
            (w.next_seq - 1, s.seq, health)
        };
        Stats {
            keys,
            segments,
            disk_bytes,
            live_bytes: inner.live.load(Ordering::Relaxed),
            dead_bytes: inner.dead.load(Ordering::Relaxed),
            // 24 bytes de Loc + 16 del puntero gordo a la clave + ~8 de la tabla hash.
            index_bytes_est: key_bytes + keys * (24 + 16 + 8),
            last_seq,
            synced_seq,
            fsync: inner.opts.fsync.name().to_string(),
            compacting: inner.compacting.load(Ordering::Relaxed),
            health,
            recovery_corrupt_records: inner.recovery.corrupt_records,
            recovery_corrupt_bytes: inner.recovery.corrupt_bytes,
            recovery_truncated_tails: inner.recovery.truncated_tails.len() as u64,
        }
    }

    /// Reescribe los registros vivos de todos los segmentos cerrados y borra
    /// los viejos. Puede ejecutarse con lecturas y escrituras en marcha.
    pub fn compact(&self) -> Result<CompactReport> {
        self.inner.compact()
    }

    pub fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let inner = &*self.inner;
        inner.stop.store(true, Ordering::Release);
        {
            let mut r = lk(&inner.sync_req);
            *r = true;
            inner.sync_req_cv.notify_all();
        }
        if let Some(h) = lk(&self.syncer).take() {
            let _ = h.join();
        }
        while inner.compacting.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(2));
        }
        let w = lk(&inner.writer);
        inner.sync_file(&w.file)?;
        Ok(())
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl Inner {
    #[inline]
    fn shard(&self, key: &[u8]) -> &Shard {
        &self.shards[(fnv1a(key) as usize) % self.shards.len()]
    }

    fn sync_file(&self, f: &File) -> io::Result<()> {
        if let Some(n) = self.opts.fsync_falla_tras {
            if self.syncs_hechos.fetch_add(1, Ordering::Relaxed) >= n {
                return Err(io::Error::other(
                    "fallo de E/S inyectado en fsync (solo pruebas)",
                ));
            }
        }
        sync_fd(f, self.opts.full_fsync)
    }

    /// Como `sync_file`, pero un fallo deja la base de datos no escribible.
    fn sync_file_or_fail(&self, f: &File) -> Result<()> {
        match self.sync_file(f) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.mark_failed(&e);
                Err(self.no_escribible(&e.to_string()))
            }
        }
    }

    fn no_escribible(&self, motivo: &str) -> Error {
        Error::NoEscribible(format!(
            "un fsync falló ({motivo}). Las escrituras se rechazan hasta \
             reiniciar: un fsync fallido no se puede reintentar con criterio, \
             porque el sistema puede haber descartado ya los datos sin escribirlos"
        ))
    }

    /// Avanza la marca de durabilidad. **Solo** se llama cuando el `fsync`
    /// terminó bien: adelantarla tras un fallo convertiría la promesa «está en
    /// disco» en una mentira.
    fn mark_synced(&self, seq: u64) {
        let mut g = lk(&self.synced);
        if seq > g.seq {
            g.seq = seq;
            self.synced_cv.notify_all();
        }
    }

    /// Declara el fallo de durabilidad y despierta a todos los que esperaban,
    /// que es lo que evita que se queden colgados para siempre al no avanzar
    /// la marca.
    fn mark_failed(&self, e: &io::Error) {
        let mut g = lk(&self.synced);
        if g.failed.is_none() {
            g.failed = Some(e.to_string());
            eprintln!("bitacora: FALLO DE DURABILIDAD, no se vuelve a escribir: {e}");
        }
        self.synced_cv.notify_all();
    }

    /// Rechaza la operación si la base de datos ya no es escribible.
    fn check_writable(&self) -> Result<()> {
        let g = lk(&self.synced);
        match &g.failed {
            Some(m) => Err(self.no_escribible(m)),
            None => Ok(()),
        }
    }

    /// Pide un fsync al hilo de sincronización y espera a que `seq` esté en
    /// disco. Devuelve error si el disco falló: nunca devuelve `Ok` para un
    /// dato que no esté confirmado.
    fn wait_synced(&self, seq: u64) -> Result<()> {
        {
            let mut r = lk(&self.sync_req);
            *r = true;
            self.sync_req_cv.notify_one();
        }
        let mut g = lk(&self.synced);
        loop {
            if let Some(m) = &g.failed {
                let m = m.clone();
                drop(g);
                return Err(self.no_escribible(&m));
            }
            if g.seq >= seq {
                return Ok(());
            }
            g = self.synced_cv.wait(g).unwrap();
        }
    }

    /// Cierra el segmento activo (sincronizándolo) y abre el siguiente.
    fn roll(self: &Arc<Self>, w: &mut Writer) -> Result<()> {
        self.sync_file_or_fail(&w.file)?;
        self.mark_synced(w.next_seq - 1);
        let id = w.next_id;
        w.next_id += 1;
        let f = Arc::new(open_segment(&self.opts.dir, id)?);
        // C3: crear el fichero no es durable hasta sincronizar el directorio.
        sync_dir(&self.opts.dir)?;
        wr(&self.segments).insert(id, Arc::clone(&f));
        w.file = f;
        w.id = id;
        w.len = 0;
        self.maybe_auto_compact();
        Ok(())
    }

    fn maybe_auto_compact(self: &Arc<Self>) {
        if !self.opts.auto_compact || self.compacting.load(Ordering::Relaxed) {
            return;
        }
        let dead = self.dead.load(Ordering::Relaxed);
        let live = self.live.load(Ordering::Relaxed);
        if dead < self.opts.compact_min_dead_bytes {
            return;
        }
        if (dead as f64) / ((dead + live).max(1) as f64) < self.opts.compact_dead_ratio {
            return;
        }
        let me = Arc::clone(self);
        let _ = thread::Builder::new()
            .name("bitacora-compact".into())
            .spawn(move || {
                if let Err(e) = me.compact() {
                    eprintln!("bitacora: la compactación falló: {e}");
                }
            });
    }

    fn new_out(&self) -> Result<OutSeg> {
        let id = {
            let mut w = lk(&self.writer);
            let id = w.next_id;
            w.next_id += 1;
            id
        };
        let f = Arc::new(open_segment(&self.opts.dir, id)?);
        // C3: el nombre nuevo no es durable hasta sincronizar el directorio.
        sync_dir(&self.opts.dir)?;
        wr(&self.segments).insert(id, Arc::clone(&f));
        Ok(OutSeg {
            id,
            file: f,
            len: 0,
            buf: Vec::with_capacity(4096),
        })
    }

    fn compact(&self) -> Result<CompactReport> {
        if self.compacting.swap(true, Ordering::AcqRel) {
            return Ok(CompactReport::default());
        }
        let r = self.compact_inner();
        self.compacting.store(false, Ordering::Release);
        r
    }

    fn compact_inner(&self) -> Result<CompactReport> {
        let mut old_ids: Vec<u32> = {
            let w = lk(&self.writer);
            let segs = rd(&self.segments);
            segs.keys().copied().filter(|&id| id != w.id).collect()
        };
        old_ids.sort_unstable();
        if old_ids.is_empty() {
            return Ok(CompactReport {
                ran: true,
                ..Default::default()
            });
        }

        let mut out: Option<OutSeg> = None;
        let mut copied = 0u64;
        let mut scanned = 0u64;

        for &id in &old_ids {
            let path = seg_path(&self.opts.dir, id);
            let file = File::open(&path)?;
            let len = file.metadata()?.len();
            scanned += len;
            let mut lector = SegmentReader::new(BufReader::with_capacity(1 << 16, file), len);
            loop {
                if self.stop.load(Ordering::Relaxed) {
                    // Cierre en marcha: abandonar sin tocar los ficheros viejos.
                    // Lo ya copiado son duplicados inofensivos (mismo seq).
                    if let Some(o) = out.take() {
                        self.sync_file(&o.file)?;
                    }
                    return Ok(CompactReport::default());
                }
                let r = match lector.siguiente()? {
                    ReadOutcome::Record(r) => r,
                    ReadOutcome::Corrupt { at, cause, .. } => {
                        // Integridad antes que rendimiento. Si un segmento se
                        // ha corrompido con el proceso en marcha, se abandona
                        // la compactación: nunca se borra un segmento del que
                        // no se ha podido copiar todo lo vivo.
                        return Err(Error::Corrupcion(format!(
                            "compactación abandonada: {} tiene datos ilegibles en el byte {at} ({}). \
                             No se ha borrado ningún segmento",
                            path.display(),
                            cause.name()
                        )));
                    }
                    _ => break,
                };
                if r.kind == Kind::Del {
                    continue; // las lápidas sobran: los PUT viejos no se copian
                }
                let shard = self.shard(&r.key);
                let cur = match rd(&shard.map).get(&r.key[..]) {
                    Some(l) => *l,
                    None => continue,
                };
                if cur.file_id != id || cur.vpos != r.vpos {
                    continue; // hay una versión más nueva en otro sitio
                }
                let rlen = rec_len(r.key.len(), r.value.len() as u32);
                let needs_new = match &out {
                    None => true,
                    Some(o) => o.len > 0 && o.len + rlen > self.opts.max_segment_bytes,
                };
                if needs_new {
                    if let Some(o) = out.take() {
                        self.sync_file(&o.file)?;
                    }
                    out = Some(self.new_out()?);
                }
                let o = out.as_mut().unwrap();
                o.buf.clear();
                wal::encode(&mut o.buf, r.seq, Kind::Put, &r.key, &r.value);
                {
                    let mut f: &File = &o.file;
                    f.write_all(&o.buf)?;
                }
                let newloc = Loc {
                    file_id: o.id,
                    vlen: r.value.len() as u32,
                    vpos: o.len + (HEADER + r.key.len()) as u64,
                    seq: r.seq,
                };
                o.len += rlen;
                copied += rlen;
                // Solo movemos el puntero si nadie ha escrito la clave mientras tanto.
                let mut m = wr(&shard.map);
                if let Some(e) = m.get_mut(&r.key[..]) {
                    if *e == cur {
                        *e = newloc;
                    }
                }
            }
        }
        if let Some(o) = out.take() {
            self.sync_file(&o.file)?;
        }
        // C3: los segmentos nuevos tienen que ser durables ANTES de borrar los
        // viejos. Si no, un apagón aquí deja los viejos borrados y los nuevos
        // sin entrada de directorio.
        sync_dir(&self.opts.dir)?;

        // Retirar los segmentos viejos: primero del mapa (los lectores que ya
        // tenían el descriptor siguen leyendo sin problema), luego del disco.
        {
            let mut segs = wr(&self.segments);
            for id in &old_ids {
                segs.remove(id);
            }
        }
        for id in &old_ids {
            fs::remove_file(seg_path(&self.opts.dir, *id))?;
        }
        // Y el borrado tampoco es durable sin esto.
        sync_dir(&self.opts.dir)?;
        self.recount();
        Ok(CompactReport {
            ran: true,
            segments_removed: old_ids.len(),
            bytes_before: scanned,
            bytes_after: copied,
        })
    }

    /// Recalcula bytes vivos/muertos desde cero (O(claves)); solo tras compactar.
    fn recount(&self) {
        let total: u64 = {
            let segs = rd(&self.segments);
            segs.values()
                .map(|f| f.metadata().map(|m| m.len()).unwrap_or(0))
                .sum()
        };
        let mut live = 0u64;
        for s in &self.shards {
            let m = rd(&s.map);
            for (k, l) in m.iter() {
                live += rec_len(k.len(), l.vlen);
            }
        }
        self.live.store(live, Ordering::Relaxed);
        self.dead
            .store(total.saturating_sub(live), Ordering::Relaxed);
    }
}

struct OutSeg {
    id: u32,
    file: Arc<File>,
    len: u64,
    buf: Vec<u8>,
}

/// Hilo de group commit: espera a que alguien pida un fsync (o al intervalo),
/// sincroniza el segmento activo UNA vez y despierta a todos los que esperaban.
fn syncer_loop(inner: Arc<Inner>) {
    loop {
        {
            let mut req = lk(&inner.sync_req);
            if !*req && !inner.stop.load(Ordering::Acquire) {
                let (g, _) = inner
                    .sync_req_cv
                    .wait_timeout(req, inner.opts.group_interval)
                    .unwrap();
                req = g;
            }
            *req = false;
        }
        let stopping = inner.stop.load(Ordering::Acquire);
        // Instantánea bajo el candado; el fsync se hace FUERA para no frenar
        // a los escritores. Todo lo anterior a `last` ya está en este fichero
        // (o en segmentos previos, sincronizados al cerrarlos).
        let (file, last) = {
            let w = lk(&inner.writer);
            (Arc::clone(&w.file), w.next_seq - 1)
        };
        let ya = lk(&inner.synced).seq;
        if last > ya {
            match inner.sync_file(&file) {
                Ok(()) => inner.mark_synced(last),
                // La marca NO avanza: el dato no está en disco. Pero hay que
                // despertar a los que esperaban para que vean el fallo en vez
                // de quedarse colgados.
                Err(e) => inner.mark_failed(&e),
            }
        }
        if stopping {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// M2: descontar más de lo que hay no debe envolverse a 18 exabytes. Esos
    /// contadores deciden cuándo se compacta, así que un valor absurdo no es
    /// solo cosmético.
    #[test]
    fn sub_sat_no_se_envuelve() {
        let a = AtomicU64::new(10);
        sub_sat(&a, 3);
        assert_eq!(a.load(Ordering::Relaxed), 7);
        sub_sat(&a, 100);
        assert_eq!(
            a.load(Ordering::Relaxed),
            0,
            "debe quedarse en 0, no envolverse"
        );
        sub_sat(&a, 1);
        assert_eq!(a.load(Ordering::Relaxed), 0);
    }

    /// A6: un panic con el candado tomado envenena el candado. Con
    /// `.lock().unwrap()` eso hacía entrar en panic a todas las operaciones
    /// siguientes, es decir, convertía un fallo aislado en un proceso
    /// inservible. Los auxiliares se recuperan.
    #[test]
    fn los_candados_se_recuperan_del_envenenamiento() {
        let anterior = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {})); // el panic es a propósito

        let m = Arc::new(Mutex::new(1u32));
        let m2 = Arc::clone(&m);
        let _ = thread::spawn(move || {
            let _g = m2.lock().unwrap();
            panic!("envenenando a propósito");
        })
        .join();

        let l = Arc::new(RwLock::new(2u32));
        let l2 = Arc::clone(&l);
        let _ = thread::spawn(move || {
            let _g = l2.write().unwrap();
            panic!("envenenando a propósito");
        })
        .join();

        std::panic::set_hook(anterior);

        assert!(m.lock().is_err(), "el candado debería estar envenenado");
        assert_eq!(*lk(&m), 1, "lk tiene que recuperarse y devolver el valor");
        assert!(l.read().is_err(), "el RwLock debería estar envenenado");
        assert_eq!(*rd(&l), 2, "rd tiene que recuperarse");
        assert_eq!(*wr(&l), 2, "wr tiene que recuperarse");
    }
}
