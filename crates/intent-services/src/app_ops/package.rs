//! App packages (PROTOCOL §5.60 "App packages"): a ZIP archive of a bundle
//! directory, unpacked into a private scratch directory that the caller
//! then reads with [`super::bundle::read_bundle_dir`], so a package and a
//! workspace install of the same files give the same bundle hash.
//!
//! The whole central directory is checked before any entry is
//! decompressed: names (path escapes), kinds (symlinks, special files),
//! encryption, compression methods, the layout and the declared sizes
//! against the bundle limits. Each entry is then decompressed through a
//! reader capped at its declared size plus one byte, so an archive that
//! inflates beyond what it declares (a zip bomb) stops at the cap.

use std::collections::{BTreeSet, HashSet};
use std::io::{Read, Write as _};
use std::path::Path;

use intent_core::apps::AppError;
use intent_core::Error;
use serde_json::json;
use zip::CompressionMethod;

use super::bundle::{
    is_bundle_path, MANIFEST_FILE, MAX_FILES, MAX_FILE_BYTES, MAX_MANIFEST_BYTES,
    MAX_SIGNATURE_BYTES, MAX_TOTAL_BYTES, SIGNATURE_FILE,
};

/// Largest package `file.attachmentUpload.begin { purpose: "app-package" }`
/// accepts: the 64 MiB bundle limit plus room for zip headers.
pub(crate) const MAX_PACKAGE_BYTES: u64 = 80 * 1024 * 1024;

/// Most central directory entries read, skipped entries included.
pub(crate) const MAX_ENTRIES: usize = 10_000;

/// Root folder of macOS resource forks, skipped before the layout check.
const MACOS_FORKS: &str = "__MACOSX/";

const S_IFMT: u32 = 0o170_000;
const S_IFREG: u32 = 0o100_000;
const S_IFDIR: u32 = 0o040_000;
const S_IFLNK: u32 = 0o120_000;

pub(crate) fn invalid_package(detail: impl Into<String>, entry: Option<&str>) -> Error {
    let detail = detail.into();
    AppError::params(
        "invalid-package",
        format!("invalid package: {detail}"),
        json!({ "detail": detail, "entry": entry }),
    )
    .into_error()
}

fn invalid_bundle(detail: impl Into<String>, path: Option<&str>) -> Error {
    let detail = detail.into();
    AppError::params(
        "invalid-bundle",
        format!("invalid bundle: {detail}"),
        json!({ "detail": detail, "path": path }),
    )
    .into_error()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    File,
    Dir,
    Symlink,
    Special,
}

struct Entry {
    index: usize,
    /// The raw entry name (UTF-8), trailing `/` kept for directories.
    name: String,
    kind: Kind,
    size: u64,
}

/// Whether a raw entry name is a path escape or otherwise unsafe to
/// write: not relative, `\`, NUL or control characters, a drive prefix,
/// or an empty, `.` or `..` segment. A trailing `/` (a directory entry)
/// is allowed.
fn is_unsafe_name(name: &str) -> bool {
    let trimmed = name.strip_suffix('/').unwrap_or(name);
    let bytes = trimmed.as_bytes();
    trimmed.is_empty()
        || trimmed.starts_with('/')
        || trimmed.contains('\\')
        || trimmed.chars().any(super::bundle::is_control_char)
        || (bytes.len() >= 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic())
        || trimmed
            .split('/')
            .any(|seg| seg.is_empty() || seg == "." || seg == "..")
}

fn entry_kind(name: &str, unix_mode: Option<u32>, symlink: bool) -> Kind {
    if symlink {
        return Kind::Symlink;
    }
    match unix_mode.map(|m| m & S_IFMT) {
        Some(S_IFLNK) => Kind::Symlink,
        Some(S_IFDIR) => Kind::Dir,
        // Some tools store permission bits only (type 0).
        Some(0 | S_IFREG) | None => {
            if name.ends_with('/') {
                Kind::Dir
            } else {
                Kind::File
            }
        }
        Some(_) => Kind::Special,
    }
}

/// The prefix to remove from every entry: `""` when `intent-app.json` is
/// at the root, `"<folder>/"` when every entry is inside one top-level
/// folder that holds it.
fn package_root(entries: &[Entry]) -> Result<String, Error> {
    if entries
        .iter()
        .any(|e| e.kind != Kind::Dir && e.name == MANIFEST_FILE)
    {
        return Ok(String::new());
    }
    let tops: BTreeSet<&str> = entries
        .iter()
        .map(|e| e.name.split('/').next().unwrap_or(""))
        .collect();
    let layout = || {
        invalid_package(
            "intent-app.json is not at the package root or inside exactly one top-level folder",
            None,
        )
    };
    let [top] = tops.into_iter().collect::<Vec<_>>()[..] else {
        return Err(layout());
    };
    let prefix = format!("{top}/");
    let manifest = format!("{prefix}{MANIFEST_FILE}");
    let inside = entries.iter().all(|e| e.name.starts_with(&prefix));
    let has_manifest = entries
        .iter()
        .any(|e| e.kind != Kind::Dir && e.name == manifest);
    if inside && has_manifest {
        Ok(prefix)
    } else {
        Err(layout())
    }
}

/// Unpack the package at `package` into `dest` (an existing, empty,
/// private directory). Refuses every rule of §5.60 "App packages" with
/// `invalid-package` or `invalid-bundle`. The caller reads `dest` with
/// [`super::bundle::read_bundle_dir`] afterwards and deletes it.
pub(crate) fn unpack_package(package: &Path, dest: &Path) -> Result<(), Error> {
    let file = std::fs::File::open(package)
        .map_err(|e| Error::Internal(format!("open app package failed: {e}")))?;
    let mut archive = zip::ZipArchive::new(std::io::BufReader::new(file))
        .map_err(|e| invalid_package(format!("not a usable ZIP archive: {e}"), None))?;
    // The reader keeps one entry per name, so a duplicate is visible only
    // here, before any entry is read.
    if archive.has_duplicate_names() {
        return Err(invalid_package("two entries have the same name", None));
    }
    if archive.len() > MAX_ENTRIES {
        return Err(invalid_package(
            format!("the archive has more than {MAX_ENTRIES} entries"),
            None,
        ));
    }

    // Pass 1: the central directory only, nothing decompressed.
    let mut entries = Vec::new();
    for index in 0..archive.len() {
        let raw = archive
            .by_index_raw(index)
            .map_err(|e| invalid_package(format!("a damaged entry: {e}"), None))?;
        let Ok(name) = std::str::from_utf8(raw.name_raw()).map(str::to_owned) else {
            let lossy = String::from_utf8_lossy(raw.name_raw()).into_owned();
            return Err(invalid_package("an entry name is not UTF-8", Some(&lossy)));
        };
        if name.starts_with(MACOS_FORKS) {
            continue;
        }
        if is_unsafe_name(&name) {
            return Err(invalid_package(
                "an entry name is not a safe relative path",
                Some(&name),
            ));
        }
        if raw.encrypted() {
            return Err(invalid_package(
                "encrypted entries are not supported",
                Some(&name),
            ));
        }
        let kind = entry_kind(&name, raw.unix_mode(), raw.is_symlink());
        if kind == Kind::File
            && !matches!(
                raw.compression(),
                CompressionMethod::Stored | CompressionMethod::Deflated
            )
        {
            return Err(invalid_package(
                format!("compression method {} is not supported", raw.compression()),
                Some(&name),
            ));
        }
        let size = raw.size();
        entries.push(Entry {
            index,
            name,
            kind,
            size,
        });
    }

    let prefix = package_root(&entries)?;
    let mut files: Vec<(usize, String, u64)> = Vec::new();
    let mut file_paths: HashSet<String> = HashSet::new();
    let mut dir_paths: HashSet<String> = HashSet::new();
    let mut total: u64 = 0;
    for entry in &entries {
        let stripped = &entry.name[prefix.len()..];
        let path = stripped.strip_suffix('/').unwrap_or(stripped);
        if path.is_empty() {
            continue; // the top-level folder itself
        }
        match entry.kind {
            Kind::Dir => {
                dir_paths.insert(path.to_owned());
                continue;
            }
            Kind::Symlink => return Err(invalid_bundle("symlinks are not allowed", Some(path))),
            Kind::Special => {
                return Err(invalid_bundle("special files are not allowed", Some(path)));
            }
            Kind::File => {}
        }
        if !is_bundle_path(path) {
            return Err(invalid_bundle(
                "a bundle path breaks the path rules",
                Some(path),
            ));
        }
        if !file_paths.insert(path.to_owned()) {
            return Err(invalid_package(
                "two entries have the same name",
                Some(&entry.name),
            ));
        }
        if path == SIGNATURE_FILE && entry.size > MAX_SIGNATURE_BYTES {
            return Err(invalid_bundle(
                "intent-app.sig is larger than 16 KiB",
                Some(SIGNATURE_FILE),
            ));
        }
        if path == MANIFEST_FILE && entry.size > MAX_MANIFEST_BYTES {
            return Err(invalid_bundle(
                "intent-app.json is larger than 64 KiB",
                Some(MANIFEST_FILE),
            ));
        }
        if entry.size > MAX_FILE_BYTES {
            return Err(invalid_bundle("a file is larger than 16 MiB", Some(path)));
        }
        total = total.saturating_add(entry.size);
        if total > MAX_TOTAL_BYTES {
            return Err(invalid_bundle("the bundle is larger than 64 MiB", None));
        }
        files.push((entry.index, path.to_owned(), entry.size));
        if files.len() > MAX_FILES {
            return Err(invalid_bundle("the bundle has more than 2000 files", None));
        }
    }
    // A file may not also be a folder of another entry (`a` and `a/b`).
    for path in file_paths.iter().chain(dir_paths.iter()) {
        let mut ancestor = path.as_str();
        while let Some((parent, _)) = ancestor.rsplit_once('/') {
            if file_paths.contains(parent) {
                return Err(invalid_package(
                    "a file entry is also a folder of another entry",
                    Some(&format!("{prefix}{parent}")),
                ));
            }
            ancestor = parent;
        }
    }
    if dir_paths.iter().any(|d| file_paths.contains(d)) {
        return Err(invalid_package("a file entry is also a folder entry", None));
    }

    // Pass 2: decompress each file, capped at its declared size + 1.
    for (index, path, declared) in files {
        let shown = format!("{prefix}{path}");
        let target = dest.join(&path);
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                invalid_package(format!("cannot create a folder: {e}"), Some(&shown))
            })?;
        }
        let mut out = create_new_no_follow(&target).map_err(|e| {
            if e.kind() == std::io::ErrorKind::AlreadyExists {
                invalid_package(
                    "two entries name the same file on this file system",
                    Some(&shown),
                )
            } else {
                Error::Internal(format!("write app package entry failed: {e}"))
            }
        })?;
        let entry = archive
            .by_index(index)
            .map_err(|e| invalid_package(format!("cannot read an entry: {e}"), Some(&shown)))?;
        let mut capped = entry.take(declared.saturating_add(1));
        let mut buf = vec![0u8; 64 * 1024];
        let mut written: u64 = 0;
        loop {
            let n = capped
                .read(&mut buf)
                .map_err(|e| invalid_package(format!("cannot read an entry: {e}"), Some(&shown)))?;
            if n == 0 {
                break;
            }
            written += n as u64;
            if written > declared {
                return Err(invalid_package(
                    "an entry decompresses to more bytes than it declares",
                    Some(&shown),
                ));
            }
            out.write_all(&buf[..n])
                .map_err(|e| Error::Internal(format!("write app package entry failed: {e}")))?;
        }
        if written != declared {
            return Err(invalid_package(
                "an entry decompresses to fewer bytes than it declares",
                Some(&shown),
            ));
        }
        out.flush()
            .map_err(|e| Error::Internal(format!("write app package entry failed: {e}")))?;
    }
    Ok(())
}

#[cfg(unix)]
fn create_new_no_follow(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt as _;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(not(unix))]
fn create_new_no_follow(path: &Path) -> std::io::Result<std::fs::File> {
    // `create_new` never follows or replaces an existing entry.
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
}
