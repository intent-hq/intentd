//! [`BlobStorage`] over files below one root folder: key `a/b/c` is the file
//! `<root>/a/b/c`.
//!
//! Symlinks below the root are never followed: a symlinked folder or blob is
//! an error (the root itself may be a symlink, e.g. a relocated data dir).
//! [`BlobStorage::replace`] writes a sibling temp file ending in
//! [`TEMP_SUFFIX`] and renames it over the blob, so an open handle keeps the
//! complete old file and a fresh open sees the complete new one.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use super::{BlobEntry, BlobFile, BlobKey, BlobKind, BlobStorage, TempSweep};

/// Suffix of the temp files [`BlobStorage::replace`] writes.
pub const TEMP_SUFFIX: &str = ".compact";

/// Blobs as files below [`Self::root`].
#[derive(Debug, Clone)]
pub struct LocalBlobStorage {
    root: PathBuf,
}

impl LocalBlobStorage {
    /// Storage rooted at `root`. Nothing is created until a blob is opened
    /// to append.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// The root folder.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, key: &BlobKey) -> PathBuf {
        let mut path = self.root.clone();
        path.extend(key.segments());
        path
    }

    /// Refuse to traverse a symlink (or non-folder) at any segment of `key`.
    fn check(&self, key: &BlobKey, walk: Walk) -> io::Result<()> {
        if walk == Walk::Create {
            fs::create_dir_all(&self.root)?;
        }
        let parts = key.segments();
        let mut cur = self.root.clone();
        for (i, part) in parts.iter().enumerate() {
            cur.push(part);
            let last = i + 1 == parts.len();
            match fs::symlink_metadata(&cur) {
                Ok(m) if m.file_type().is_symlink() => {
                    if last && walk == Walk::Delete {
                        return Ok(());
                    }
                    return Err(io::Error::other(format!(
                        "refusing to follow symlink {}",
                        cur.display()
                    )));
                }
                Ok(m) if !last && !m.is_dir() => {
                    return Err(io::Error::other(format!(
                        "{} is not a folder",
                        cur.display()
                    )));
                }
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    if last || walk != Walk::Create {
                        return Ok(());
                    }
                    match fs::create_dir(&cur) {
                        Ok(()) => {}
                        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                            // Lost a creation race: re-check what now exists.
                            let m = fs::symlink_metadata(&cur)?;
                            if m.file_type().is_symlink() || !m.is_dir() {
                                return Err(io::Error::other(format!(
                                    "{} is not a folder",
                                    cur.display()
                                )));
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

/// How [`LocalBlobStorage::check`] treats the segments of a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Walk {
    /// Create missing folders; the final segment must not be a symlink.
    Create,
    /// Stop at the first missing folder; the final segment must not be a symlink.
    Read,
    /// Stop at the first missing folder; the final segment may be a symlink
    /// (removing a link never touches its target).
    Delete,
}

impl BlobStorage for LocalBlobStorage {
    fn display_path(&self, key: &BlobKey) -> PathBuf {
        self.path(key)
    }

    fn open_append(&self, key: &BlobKey) -> io::Result<Box<dyn BlobFile>> {
        let path = self.path(key);
        let open = || {
            self.check(key, Walk::Create)?;
            let mut opts = OpenOptions::new();
            opts.create(true).read(true).append(true);
            no_follow(opts).open(&path)
        };
        // A concurrent prune may remove the freshly created (still empty) folder.
        let file = match open() {
            Err(e) if e.kind() == io::ErrorKind::NotFound => open()?,
            other => other?,
        };
        Ok(Box::new(file))
    }

    /// On Windows, std opens every file with `FILE_SHARE_DELETE` and
    /// `fs::rename` retries a refused `MoveFileExW` with POSIX rename
    /// semantics, so replacing a blob that readers (or an old append handle)
    /// hold open works on Windows 10 1607+ NTFS; elsewhere the rename may
    /// still fail, and the old blob is kept.
    fn replace(&self, key: &BlobKey, contents: &[u8]) -> io::Result<Box<dyn BlobFile>> {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = self.path(key);
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| io::Error::other("blob key has no name"))?;
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp = path.with_file_name(format!("{name}.{}-{seq}{TEMP_SUFFIX}", std::process::id()));
        let mut opts = OpenOptions::new();
        opts.create_new(true).read(true).append(true);
        let mut file = no_follow(opts).open(&tmp)?;
        let result = file
            .write_all(contents)
            .and_then(|()| file.flush())
            .and_then(|()| rename(&tmp, &path));
        if let Err(e) = result {
            let _ = fs::remove_file(&tmp);
            return Err(e);
        }
        Ok(Box::new(file))
    }

    fn open_read(&self, key: &BlobKey) -> io::Result<Option<Box<dyn BlobFile>>> {
        self.check(key, Walk::Read)?;
        let mut opts = OpenOptions::new();
        opts.read(true);
        match no_follow(opts).open(self.path(key)) {
            Ok(f) => Ok(Some(Box::new(f))),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn exists(&self, key: &BlobKey) -> io::Result<bool> {
        self.check(key, Walk::Read)?;
        Ok(fs::symlink_metadata(self.path(key)).is_ok_and(|m| m.is_file()))
    }

    fn modified(&self, key: &BlobKey) -> io::Result<SystemTime> {
        fs::metadata(self.path(key)).and_then(|m| m.modified())
    }

    fn delete(&self, key: &BlobKey) -> io::Result<()> {
        self.check(key, Walk::Delete)
            .and_then(|()| ignore_missing(fs::remove_file(self.path(key))))
    }

    /// `remove_dir_all` never follows symlinks inside the tree, and a
    /// symlinked `prefix` is removed as a link.
    fn delete_prefix(&self, prefix: &BlobKey) -> io::Result<()> {
        let dir = self.path(prefix);
        self.check(prefix, Walk::Delete)
            .and_then(|()| match fs::symlink_metadata(&dir) {
                Ok(m) if m.file_type().is_symlink() => fs::remove_file(&dir),
                Ok(_) => fs::remove_dir_all(&dir),
                Err(e) => Err(e),
            })
            .or_else(|e| ignore_missing(Err(e)))
    }

    fn list(&self, prefix: &BlobKey) -> io::Result<Vec<BlobEntry>> {
        let entries = match fs::read_dir(self.path(prefix)) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut out = Vec::new();
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            let kind = if file_type.is_dir() {
                BlobKind::Folder
            } else if file_type.is_file() {
                BlobKind::Blob
            } else {
                continue;
            };
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            out.push(BlobEntry { name, kind });
        }
        Ok(out)
    }

    fn prune(&self, prefix: &BlobKey) {
        let _ = fs::remove_dir(self.path(prefix));
    }

    fn remove_stale_temps(&self, prefix: &BlobKey, cutoff: SystemTime) -> TempSweep {
        let mut sweep = TempSweep::default();
        let Ok(entries) = fs::read_dir(self.path(prefix)) else {
            return sweep;
        };
        for entry in entries.flatten() {
            let is_temp = entry
                .file_name()
                .to_str()
                .is_some_and(|n| n.ends_with(TEMP_SUFFIX));
            if !is_temp || !entry.file_type().is_ok_and(|t| t.is_file()) {
                continue;
            }
            let path = entry.path();
            let stale = fs::symlink_metadata(&path)
                .and_then(|m| m.modified())
                .is_ok_and(|t| t < cutoff);
            if !stale {
                continue;
            }
            match ignore_missing(fs::remove_file(&path)) {
                Ok(()) => sweep.deleted += 1,
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "blob storage: stale temp delete failed");
                    sweep.failed += 1;
                }
            }
        }
        sweep
    }
}

#[cfg(test)]
thread_local! {
    /// Tests set this to make [`rename`] fail like a refused Windows rename.
    pub(super) static FAIL_RENAME: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn rename(from: &Path, to: &Path) -> io::Result<()> {
    #[cfg(test)]
    if FAIL_RENAME.with(std::cell::Cell::get) {
        return Err(io::Error::from(io::ErrorKind::PermissionDenied));
    }
    fs::rename(from, to)
}

/// Open options that refuse a symlinked final component (`O_NOFOLLOW`).
fn no_follow(
    #[cfg_attr(
        not(unix),
        expect(unused_mut, reason = "only the unix O_NOFOLLOW block mutates")
    )]
    mut opts: OpenOptions,
) -> OpenOptions {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.custom_flags(libc::O_NOFOLLOW);
    }
    opts
}

fn ignore_missing(r: io::Result<()>) -> io::Result<()> {
    match r {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}
