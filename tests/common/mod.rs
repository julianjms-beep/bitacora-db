#![allow(dead_code)]
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Carpeta temporal única que se borra sola al terminar la prueba.
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Self {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let p = std::env::temp_dir().join(format!("bitacora-test-{tag}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Ficheros .log de la carpeta, con su tamaño, ordenados por nombre.
pub fn segments(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut v: Vec<(PathBuf, u64)> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().map_or(false, |x| x == "log"))
        .map(|p| {
            let len = std::fs::metadata(&p).unwrap().len();
            (p, len)
        })
        .collect();
    v.sort();
    v
}

pub fn disk_bytes(dir: &Path) -> u64 {
    segments(dir).iter().map(|(_, l)| *l).sum()
}
