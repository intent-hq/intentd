//! Blob storage: named byte streams under a folder-like key space.
//!
//! The trait is synchronous and mirrors the file operations its first
//! consumer (the script output store) makes, one to one: callers run on
//! blocking threads and keep open handles across appends, seeks and
//! compaction, so a coarser API would change their behaviour. An object
//! store backend can wrap an async client behind it, or the trait can become
//! async when that backend lands.
//!
//! [`LocalBlobStorage`] keeps blobs as files below one root folder.

use std::fmt;
use std::io::{self, Read, Seek, Write};
use std::path::PathBuf;
use std::time::SystemTime;

mod local;

pub use local::{LocalBlobStorage, TEMP_SUFFIX};

#[cfg(test)]
mod contract;

/// A blob's place in the key space: zero or more segments, like a relative
/// path. No segment is empty, `.` or `..`, or holds a separator or NUL, so a
/// key can never name something outside its storage.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct BlobKey(Vec<String>);

impl BlobKey {
    /// The root of the key space (no segments).
    #[must_use]
    pub fn root() -> Self {
        Self::default()
    }

    /// `self` with `segment` appended.
    ///
    /// # Errors
    ///
    /// `InvalidInput` when `segment` is empty, `.` or `..`, or holds `/`,
    /// `\` or NUL.
    pub fn child(&self, segment: impl Into<String>) -> io::Result<Self> {
        let segment = segment.into();
        let invalid = segment.is_empty()
            || segment == "."
            || segment == ".."
            || segment.contains(['/', '\\', '\0']);
        if invalid {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid blob key segment {segment:?}"),
            ));
        }
        let mut segments = self.0.clone();
        segments.push(segment);
        Ok(Self(segments))
    }

    /// The key's segments, outermost first.
    #[must_use]
    pub fn segments(&self) -> &[String] {
        &self.0
    }
}

/// An open blob: read, append (for a handle opened to append) and seek. A
/// handle keeps reading the blob it opened even after the key is replaced.
pub trait BlobFile: Read + Write + Seek + Send + fmt::Debug {
    /// The blob's current size in bytes.
    ///
    /// # Errors
    ///
    /// When the size cannot be read.
    fn len(&self) -> io::Result<u64>;

    /// Whether the blob is empty.
    ///
    /// # Errors
    ///
    /// As [`Self::len`].
    fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }
}

impl BlobFile for std::fs::File {
    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }
}

/// What a listed child of a key is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlobKind {
    /// A key with children.
    Folder,
    /// A blob.
    Blob,
}

/// One child of a listed key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobEntry {
    /// The child's last key segment.
    pub name: String,
    /// Folder or blob.
    pub kind: BlobKind,
}

/// Outcome of [`BlobStorage::remove_stale_temps`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TempSweep {
    /// Leftover temp blobs deleted.
    pub deleted: usize,
    /// Temp blobs that could not be deleted (each logged as a warning).
    pub failed: usize,
}

/// Where blobs are kept. Every method is a blocking call.
pub trait BlobStorage: Send + Sync + fmt::Debug + 'static {
    /// Where `key` is, for messages and logs. [`LocalBlobStorage`] returns
    /// the real file path.
    fn display_path(&self, key: &BlobKey) -> PathBuf;

    /// Open `key` to read and append, creating it (and its parent folders)
    /// when missing.
    ///
    /// # Errors
    ///
    /// When the blob cannot be opened or created.
    fn open_append(&self, key: &BlobKey) -> io::Result<Box<dyn BlobFile>>;

    /// Replace `key` with `contents` atomically and return a read + append
    /// handle on the new blob. A handle opened before keeps reading the
    /// complete old blob; an open after sees the complete new one. On error
    /// the old blob is unchanged.
    ///
    /// # Errors
    ///
    /// When the new blob cannot be written or put in place.
    fn replace(&self, key: &BlobKey, contents: &[u8]) -> io::Result<Box<dyn BlobFile>>;

    /// Open `key` to read; `None` when there is no such blob.
    ///
    /// # Errors
    ///
    /// When the blob exists but cannot be opened.
    fn open_read(&self, key: &BlobKey) -> io::Result<Option<Box<dyn BlobFile>>>;

    /// Whether `key` is a blob.
    ///
    /// # Errors
    ///
    /// When the storage cannot tell.
    fn exists(&self, key: &BlobKey) -> io::Result<bool>;

    /// When `key` was last written.
    ///
    /// # Errors
    ///
    /// `NotFound` when there is no such blob; other errors when the time
    /// cannot be read.
    fn modified(&self, key: &BlobKey) -> io::Result<SystemTime>;

    /// Delete the blob `key`. A missing blob is not an error.
    ///
    /// # Errors
    ///
    /// When the blob exists but cannot be deleted.
    fn delete(&self, key: &BlobKey) -> io::Result<()>;

    /// Delete `prefix` and everything below it. A missing prefix is not an
    /// error.
    ///
    /// # Errors
    ///
    /// When something below the prefix cannot be deleted.
    fn delete_prefix(&self, prefix: &BlobKey) -> io::Result<()>;

    /// The direct children of `prefix`; empty when it is missing. Children
    /// that are neither a folder nor a blob are left out.
    ///
    /// # Errors
    ///
    /// When `prefix` exists but cannot be listed.
    fn list(&self, prefix: &BlobKey) -> io::Result<Vec<BlobEntry>>;

    /// Forget `prefix` if nothing is below it; otherwise do nothing.
    fn prune(&self, prefix: &BlobKey);

    /// Delete the temp blobs [`Self::replace`] left directly below `prefix`
    /// (a crash mid-replace) that were last written before `cutoff`.
    fn remove_stale_temps(&self, prefix: &BlobKey, cutoff: SystemTime) -> TempSweep;
}
