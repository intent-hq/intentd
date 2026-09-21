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

/// Reject incomplete official bundles, including the small shell launcher used
/// by manual installs. Never execute a candidate or interpret arbitrary shell
/// code: opaque custom adapters retain the usual executable-file semantics.
#[must_use]
pub(crate) fn is_complete_candidate(path: &Path) -> bool {
    use intent_core::path_utils::is_executable_file;

    if !is_executable_file(path) {
        return false;
    }
    let resolved = path.canonicalize().unwrap_or_else(|_| path.to_owned());
    let server = if resolved.file_name().is_some_and(|name| name == SERVER) {
        Some(resolved)
    } else {
        launcher_server(&resolved)
    };
    server.is_none_or(|server| {
        is_executable_file(&server)
            && server
                .parent()
                .is_some_and(|parent| is_executable_file(&parent.join(HARNESS)))
    })
}

/// Only recognize a complete two-line literal-path launcher. Do not parse
/// variables, substitutions, extra commands, or scripts larger than 4 KiB.
fn launcher_server(path: &Path) -> Option<PathBuf> {
    let metadata = std::fs::metadata(path).ok()?;
    if !metadata.is_file() || metadata.len() > 4096 {
        return None;
    }
    let mut script = String::new();
    std::fs::File::open(path)
        .ok()?
        .take(4097)
        .read_to_string(&mut script)
        .ok()?;
    if script.len() > 4096 {
        return None;
    }
    let (shebang, command) = script.split_once('\n')?;
    if !["#!/bin/sh", "#!/bin/bash", "#!/bin/zsh"].contains(&shebang.trim_end()) {
        return None;
    }
    let word = command
        .trim()
        .strip_prefix("exec ")?
        .strip_suffix(" \"$@\"")?;
    let target = if let Some(literal) = word.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        (!literal.contains('\'')).then_some(literal)?
    } else if let Some(literal) = word.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        (!literal.contains(['"', '$', '`', '\\'])).then_some(literal)?
    } else {
        (!word
            .chars()
            .any(|c| c.is_whitespace() || "'\"$`\\;&|<>()*?[]{}~#".contains(c)))
        .then_some(word)?
    };
    let target = PathBuf::from(target);
    (target.is_absolute() && target.file_name().is_some_and(|name| name == SERVER))
        .then_some(target)
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
