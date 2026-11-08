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

use crate::wal::{self, Kind, ReadOutcome, SegmentReader, HEADER, MAX_KEY, MAX_VALUE};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

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
        }
    }
}

/// Marca temporal de "borrado" en el índice mientras se reconstruye el estado.
const TOMB: u32 = u32::MAX;

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
        let mut m = self.map.write().unwrap();
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
    /// Último `seq` que está garantizado en disco.
    synced: Mutex<u64>,
    synced_cv: Condvar,
    sync_req: Mutex<bool>,
    sync_req_cv: Condvar,
    sync_error: Mutex<Option<String>>,
    stop: AtomicBool,
    compacting: AtomicBool,
    live: AtomicU64,
    dead: AtomicU64,
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

fn validate(key: &[u8], value: Option<&[u8]>) -> io::Result<()> {
    if key.is_empty() || key.len() > MAX_KEY {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "clave vacía o de más de 64 KiB",
        ));
    }
    if let Some(v) = value {
        if v.len() >= MAX_VALUE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "valor de 1 GiB o más",
            ));
        }
    }
    Ok(())
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

impl Db {
    pub fn open_default(dir: impl Into<PathBuf>) -> io::Result<Db> {
        Db::open(Options::new(dir))
    }

    /// Abre (o crea) la base de datos y reconstruye el índice leyendo todos
    /// los segmentos. Si el último registro de un fichero está a medias, lo
    /// recorta: así se recupera de un apagón.
    pub fn open(opts: Options) -> io::Result<Db> {
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
            .map(|_| Shard { map: RwLock::new(HashMap::new()) })
            .collect();
        let mut segments: HashMap<u32, Arc<File>> = HashMap::new();
        let mut next_seq = 1u64;
        let mut total = 0u64;

        for &id in &ids {
            let path = seg_path(&opts.dir, id);
            let file = open_segment(&opts.dir, id)?;
            let mut len = file.metadata()?.len();
            let mut rd = SegmentReader::new(BufReader::with_capacity(1 << 16, file.try_clone()?), len);
            loop {
                match rd.next()? {
                    ReadOutcome::End => break,
                    ReadOutcome::Torn { at } => {
                        eprintln!(
                            "bitacora: {} tiene un registro incompleto en el byte {at} (escritura a medias); se recorta",
                            path.display()
                        );
                        file.set_len(at)?;
                        file.sync_all()?;
                        len = at;
                        break;
                    }
                    ReadOutcome::Record(r) => {
                        if r.seq >= next_seq {
                            next_seq = r.seq + 1;
                        }
                        let vlen = if r.kind == Kind::Put { r.value.len() as u32 } else { TOMB };
                        let loc = Loc { file_id: id, vlen, vpos: r.vpos, seq: r.seq };
                        let sh = &shards[(fnv1a(&r.key) as usize) % nsh];
                        let mut m = sh.map.write().unwrap();
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
            segments.insert(id, Arc::new(file));
        }

        // Las lápidas ya han hecho su trabajo: fuera del índice.
        let mut live = 0u64;
        for s in &shards {
            let mut m = s.map.write().unwrap();
            m.retain(|_, l| l.vlen != TOMB);
            for (k, l) in m.iter() {
                live += rec_len(k.len(), l.vlen);
            }
            m.shrink_to_fit();
        }

        // Segmento activo: reutiliza el último si está vacío, si no crea uno nuevo.
        let (active_id, active_file) = match ids.last() {
            Some(&last) if segments[&last].metadata()?.len() == 0 => (last, Arc::clone(&segments[&last])),
            _ => {
                let id = ids.last().map_or(1, |l| l + 1);
                let f = Arc::new(open_segment(&opts.dir, id)?);
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
            synced: Mutex::new(next_seq - 1),
            synced_cv: Condvar::new(),
            sync_req: Mutex::new(false),
            sync_req_cv: Condvar::new(),
            sync_error: Mutex::new(None),
            stop: AtomicBool::new(false),
            compacting: AtomicBool::new(false),
            live: AtomicU64::new(live),
            dead: AtomicU64::new(total.saturating_sub(live)),
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

        Ok(Db { inner, syncer: Mutex::new(syncer), closed: AtomicBool::new(false) })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        validate(key, Some(value))?;
        let inner = &*self.inner;
        let seq;
        {
            let mut w = inner.writer.lock().unwrap();
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
                let mut f: &File = &**file;
                if let Err(e) = f.write_all(buf) {
                    // Escritura a medias: deshacerla para no dejar basura delante
                    // de los registros siguientes.
                    let _ = file.set_len(start);
                    return Err(e);
                }
            }
            w.next_seq += 1;
            w.len = start + rlen;
            if inner.opts.fsync == FsyncPolicy::Always {
                inner.sync_file(&w.file)?;
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
                inner.live.fetch_sub(ol, Ordering::Relaxed);
                inner.dead.fetch_add(ol, Ordering::Relaxed);
            }
        }
        if inner.opts.fsync == FsyncPolicy::Group {
            inner.wait_synced(seq)?;
        }
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let inner = &*self.inner;
        let shard = inner.shard(key);
        for _ in 0..32 {
            let loc = match shard.map.read().unwrap().get(key) {
                Some(l) => *l,
                None => return Ok(None),
            };
            let file = match inner.segments.read().unwrap().get(&loc.file_id) {
                Some(f) => Arc::clone(f),
                // La compactación acaba de retirar ese segmento: el índice ya
                // apunta al nuevo sitio, basta con volver a mirar.
                None => {
                    thread::yield_now();
                    continue;
                }
            };
            let mut buf = vec![0u8; loc.vlen as usize];
            if loc.vlen > 0 {
                file.read_exact_at(&mut buf, loc.vpos)?;
            }
            return Ok(Some(buf));
        }
        Err(io::Error::new(io::ErrorKind::Other, "segmento desaparecido durante la lectura"))
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        self.inner.shard(key).map.read().unwrap().contains_key(key)
    }

    /// Devuelve `true` si la clave existía. Escribe una lápida en el log.
    pub fn delete(&self, key: &[u8]) -> io::Result<bool> {
        validate(key, None)?;
        let inner = &*self.inner;
        let seq;
        {
            let mut w = inner.writer.lock().unwrap();
            let shard = inner.shard(key);
            let old = match shard.map.read().unwrap().get(key) {
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
                let mut f: &File = &**file;
                if let Err(e) = f.write_all(buf) {
                    let _ = file.set_len(start);
                    return Err(e);
                }
            }
            w.next_seq += 1;
            w.len = start + rlen;
            if inner.opts.fsync == FsyncPolicy::Always {
                inner.sync_file(&w.file)?;
                inner.mark_synced(seq);
            }
            shard.map.write().unwrap().remove(key);
            let ol = rec_len(key.len(), old.vlen);
            inner.live.fetch_sub(ol, Ordering::Relaxed);
            inner.dead.fetch_add(ol + rlen, Ordering::Relaxed);
        }
        if inner.opts.fsync == FsyncPolicy::Group {
            inner.wait_synced(seq)?;
        }
        Ok(true)
    }

    /// Claves que empiezan por `prefix`, ordenadas.
    pub fn keys(&self, prefix: &[u8]) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for s in &self.inner.shards {
            let m = s.map.read().unwrap();
            for k in m.keys() {
                if k.starts_with(prefix) {
                    out.push(k.to_vec());
                }
            }
        }
        out.sort();
        out
    }

    pub fn len(&self) -> usize {
        self.inner.shards.iter().map(|s| s.map.read().unwrap().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn stats(&self) -> Stats {
        let inner = &*self.inner;
        let mut keys = 0u64;
        let mut key_bytes = 0u64;
        for s in &inner.shards {
            let m = s.map.read().unwrap();
            keys += m.len() as u64;
            for k in m.keys() {
                key_bytes += k.len() as u64;
            }
        }
        let (segments, disk_bytes) = {
            let segs = inner.segments.read().unwrap();
            let disk = segs
                .values()
                .map(|f| f.metadata().map(|m| m.len()).unwrap_or(0))
                .sum();
            (segs.len() as u64, disk)
        };
        // Ambos bajo el candado de escritura: así `synced_seq <= last_seq` siempre.
        let (last_seq, synced_seq) = {
            let w = inner.writer.lock().unwrap();
            (w.next_seq - 1, *inner.synced.lock().unwrap())
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
        }
    }

    /// Reescribe los registros vivos de todos los segmentos cerrados y borra
    /// los viejos. Puede ejecutarse con lecturas y escrituras en marcha.
    pub fn compact(&self) -> io::Result<CompactReport> {
        self.inner.compact()
    }

    pub fn close(&self) -> io::Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let inner = &*self.inner;
        inner.stop.store(true, Ordering::Release);
        {
            let mut r = inner.sync_req.lock().unwrap();
            *r = true;
            inner.sync_req_cv.notify_all();
        }
        if let Some(h) = self.syncer.lock().unwrap().take() {
            let _ = h.join();
        }
        while inner.compacting.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(2));
        }
        let w = inner.writer.lock().unwrap();
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
        sync_fd(f, self.opts.full_fsync)
    }

    fn mark_synced(&self, seq: u64) {
        let mut g = self.synced.lock().unwrap();
        if seq > *g {
            *g = seq;
            self.synced_cv.notify_all();
        }
    }

    /// Pide un fsync al hilo de sincronización y espera a que `seq` esté en disco.
    fn wait_synced(&self, seq: u64) -> io::Result<()> {
        {
            let mut r = self.sync_req.lock().unwrap();
            *r = true;
            self.sync_req_cv.notify_one();
        }
        let mut g = self.synced.lock().unwrap();
        while *g < seq {
            g = self.synced_cv.wait(g).unwrap();
        }
        drop(g);
        if let Some(e) = self.sync_error.lock().unwrap().as_ref() {
            return Err(io::Error::new(io::ErrorKind::Other, e.clone()));
        }
        Ok(())
    }

    /// Cierra el segmento activo (sincronizándolo) y abre el siguiente.
    fn roll(self: &Arc<Self>, w: &mut Writer) -> io::Result<()> {
        self.sync_file(&w.file)?;
        self.mark_synced(w.next_seq - 1);
        let id = w.next_id;
        w.next_id += 1;
        let f = Arc::new(open_segment(&self.opts.dir, id)?);
        self.segments.write().unwrap().insert(id, Arc::clone(&f));
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

    fn new_out(&self) -> io::Result<OutSeg> {
        let id = {
            let mut w = self.writer.lock().unwrap();
            let id = w.next_id;
            w.next_id += 1;
            id
        };
        let f = Arc::new(open_segment(&self.opts.dir, id)?);
        self.segments.write().unwrap().insert(id, Arc::clone(&f));
        Ok(OutSeg { id, file: f, len: 0, buf: Vec::with_capacity(4096) })
    }

    fn compact(&self) -> io::Result<CompactReport> {
        if self.compacting.swap(true, Ordering::AcqRel) {
            return Ok(CompactReport::default());
        }
        let r = self.compact_inner();
        self.compacting.store(false, Ordering::Release);
        r
    }

    fn compact_inner(&self) -> io::Result<CompactReport> {
        let mut old_ids: Vec<u32> = {
            let w = self.writer.lock().unwrap();
            let segs = self.segments.read().unwrap();
            segs.keys().copied().filter(|&id| id != w.id).collect()
        };
        old_ids.sort_unstable();
        if old_ids.is_empty() {
            return Ok(CompactReport { ran: true, ..Default::default() });
        }

        let mut out: Option<OutSeg> = None;
        let mut copied = 0u64;
        let mut scanned = 0u64;

        for &id in &old_ids {
            let path = seg_path(&self.opts.dir, id);
            let file = File::open(&path)?;
            let len = file.metadata()?.len();
            scanned += len;
            let mut rd = SegmentReader::new(BufReader::with_capacity(1 << 16, file), len);
            loop {
                if self.stop.load(Ordering::Relaxed) {
                    // Cierre en marcha: abandonar sin tocar los ficheros viejos.
                    // Lo ya copiado son duplicados inofensivos (mismo seq).
                    if let Some(o) = out.take() {
                        self.sync_file(&o.file)?;
                    }
                    return Ok(CompactReport::default());
                }
                let r = match rd.next()? {
                    ReadOutcome::Record(r) => r,
                    _ => break,
                };
                if r.kind == Kind::Del {
                    continue; // las lápidas sobran: los PUT viejos no se copian
                }
                let shard = self.shard(&r.key);
                let cur = match shard.map.read().unwrap().get(&r.key[..]) {
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
                    let mut f: &File = &*o.file;
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
                let mut m = shard.map.write().unwrap();
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

        // Retirar los segmentos viejos: primero del mapa (los lectores que ya
        // tenían el descriptor siguen leyendo sin problema), luego del disco.
        {
            let mut segs = self.segments.write().unwrap();
            for id in &old_ids {
                segs.remove(id);
            }
        }
        for id in &old_ids {
            fs::remove_file(seg_path(&self.opts.dir, *id))?;
        }
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
            let segs = self.segments.read().unwrap();
            segs.values().map(|f| f.metadata().map(|m| m.len()).unwrap_or(0)).sum()
        };
        let mut live = 0u64;
        for s in &self.shards {
            let m = s.map.read().unwrap();
            for (k, l) in m.iter() {
                live += rec_len(k.len(), l.vlen);
            }
        }
        self.live.store(live, Ordering::Relaxed);
        self.dead.store(total.saturating_sub(live), Ordering::Relaxed);
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
            let mut req = inner.sync_req.lock().unwrap();
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
            let w = inner.writer.lock().unwrap();
            (Arc::clone(&w.file), w.next_seq - 1)
        };
        let synced = *inner.synced.lock().unwrap();
        if last > synced {
            if let Err(e) = inner.sync_file(&file) {
                *inner.sync_error.lock().unwrap() = Some(e.to_string());
            }
            inner.mark_synced(last);
        }
        if stopping {
            break;
        }
    }
}
