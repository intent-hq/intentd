//! Location and identity of the Intent-managed official Antigravity bridge.

use std::io::Read;
use std::path::{Path, PathBuf};

pub const VERSION: &str = "1.1.1";
pub const SERVER: &str = "agy_acp_server.par";
pub const HARNESS: &str = "localharness_external";
pub const ARCHIVE_SHA256: &str = "fdfa915652cdb7ba8085cc8fffed072cbe009251aa2c951aabdda07a8c28a189";
pub const FILES: [(&str, u64, &str); 2] = [
    (
        SERVER,
        802_163_856,
        "9d900b93031fc42397f88206e14eba4193729bbef631a70b18e7a19631a6dfac",
    ),
    (
        HARNESS,
        116_766_704,
        "e0a8ef9d80a1ffb178f945159dda33f73d4a5be65516642542352584b834fa2a",
    ),
];

#[must_use]
pub const fn supported_host() -> bool {
    cfg!(all(target_os = "macos", target_arch = "aarch64"))
}

#[must_use]
pub fn install_root(home: &Path) -> PathBuf {
    home.join(".local/share/intent/providers/antigravity")
}

/// Bounded metadata-only discovery. Full integrity checks occur during setup.
#[must_use]
pub fn managed_binary(home: &Path) -> Option<PathBuf> {
    let root = install_root(home);
    let version = root.join(VERSION);
    if ![&root, &version]
        .iter()
        .all(|path| std::fs::symlink_metadata(path).is_ok_and(|meta| meta.is_dir()))
    {
        return None;
    }
    let ready = version.join("ready");
    if !std::fs::symlink_metadata(&ready)
        .is_ok_and(|meta| meta.is_file() && meta.len() == ARCHIVE_SHA256.len() as u64)
        || std::fs::File::open(ready)
            .ok()
            .and_then(|file| {
                let mut marker = String::new();
                file.take(65).read_to_string(&mut marker).ok()?;
                Some(marker)
            })
            .as_deref()
            != Some(ARCHIVE_SHA256)
    {
        return None;
    }
    FILES
        .iter()
        .all(|(name, bytes, _)| {
            let path = version.join(name);
            std::fs::symlink_metadata(&path)
                .is_ok_and(|meta| meta.is_file() && meta.len() == *bytes)
                && intent_core::path_utils::is_executable_file(&path)
        })
        .then(|| version.join(SERVER))
}
