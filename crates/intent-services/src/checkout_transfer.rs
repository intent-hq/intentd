//! `ws.file.pull`: copy a file or folder from one agent's checkout into
//! another's, in ranged pieces like `ws.file.getAttachment` (monorepo
//! `docs/protocol/methods/files-terminal-browser.md` § Agent file pull).
//!
//! The source answers [`PieceSource::read`]. Its first piece makes a
//! snapshot outside the checkout (a copy of the file, or the folder packed as
//! one pax tar archive) named by a `transferId`, so later pieces read fixed
//! bytes. The caller's side drives [`pull_into`]: it asks for each piece
//! where its copy ends, writes them to a part file under the checkout's
//! git-ignored `.intent/pulls/`, and keeps the copy only when its size and
//! sha256 match the first piece's. Both ends confine their path to their
//! checkout after resolving links, and both apply the cap.

use std::collections::HashMap;
use std::future::Future;
use std::io::{Read as _, Seek as _, Write as _};
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use intent_core::node_link::{CheckoutEntryKind, CheckoutPiece};
use intent_core::remote_launch::workspace_api::MAX_LINK_ATTACHMENT_CHUNK_BYTES;
pub use intent_core::remote_launch::workspace_api::{MAX_PULL_BYTES, MAX_PULL_ENTRIES};
use intent_core::{AgentId, Error, Result, WorkspaceId};

/// How long a snapshot waits for its next piece before it is removed.
pub const SNAPSHOT_IDLE_TTL: Duration = Duration::from_secs(2 * 60);

/// Most live snapshots one source agent has at a time.
pub const MAX_SNAPSHOTS_PER_SOURCE: usize = 4;

/// Where a caller's part files and unpacked folders go, git-ignored.
const PULLS_DIR: &str = ".intent/pulls";

fn refused(code: &str, detail: impl std::fmt::Display) -> Error {
    Error::InvalidParams(format!("file-pull-{code}: {detail}"))
}

fn outside(path: &str) -> Error {
    refused("outside-checkout", path)
}

fn mismatch(detail: impl std::fmt::Display) -> Error {
    refused("mismatch", detail)
}

fn io(context: &str, error: &std::io::Error) -> Error {
    Error::Internal(format!("file pull: {context}: {error}"))
}

fn encode(bytes: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn decode(data: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.decode(data).ok()
}

fn lower_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

#[cfg(test)]
fn sha256_of(bytes: &[u8]) -> String {
    use sha2::Digest as _;
    lower_hex(&sha2::Sha256::digest(bytes))
}

fn sha256_file(path: &Path) -> std::io::Result<String> {
    use sha2::Digest as _;
    let mut file = std::fs::File::open(path)?;
    let mut hasher = sha2::Sha256::new();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(lower_hex(&hasher.finalize()))
}

fn is_git(part: &std::ffi::OsStr) -> bool {
    part.to_str()
        .is_some_and(|part| part.eq_ignore_ascii_case(".git"))
}

/// `rel`'s parts when it is a plain relative path: no `..`, not absolute,
/// and no `.git` component at any depth (ASCII case-insensitive). `""` and
/// `.` are the checkout itself.
fn plain_parts(rel: &str) -> Option<Vec<&str>> {
    let mut parts = Vec::new();
    for component in Path::new(rel).components() {
        match component {
            Component::Normal(part) if !is_git(part) => parts.push(part.to_str()?),
            Component::CurDir => {}
            _ => return None,
        }
    }
    Some(parts)
}

/// Whether `canonical` lies in `root` outside any `.git`.
fn confined(root: &Path, canonical: &Path) -> bool {
    canonical
        .strip_prefix(root)
        .is_ok_and(|rel| !rel.components().any(|part| is_git(part.as_os_str())))
}

/// The canonical checkout and the canonical form of `rel` in it, which
/// must exist and stay inside it after links are followed.
fn resolve_source(checkout: &Path, rel: &str) -> Result<(PathBuf, PathBuf)> {
    let parts = plain_parts(rel).ok_or_else(|| outside(rel))?;
    let root = std::fs::canonicalize(checkout)
        .map_err(|_| refused("source-unavailable", "the agent's checkout is gone"))?;
    let joined = parts
        .iter()
        .fold(root.clone(), |path, part| path.join(part));
    let Ok(canonical) = std::fs::canonicalize(&joined) else {
        // A broken link reads as missing, wherever it pointed.
        return Err(refused("not-found", rel));
    };
    if !confined(&root, &canonical) {
        return Err(outside(rel));
    }
    Ok((root, canonical))
}

/// What a destination asks a source for: one piece.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PieceRequest {
    pub path: String,
    pub offset: u64,
    pub length: u64,
    pub transfer_id: Option<String>,
}

/// One live snapshot.
struct Snapshot {
    source: String,
    path: String,
    file: PathBuf,
    kind: CheckoutEntryKind,
    size: u64,
    used: Instant,
    /// False while it is being made: it holds its source's slot, but no
    /// piece reads it and no sweep removes it.
    ready: bool,
}

/// Serves pieces of files and folders in agents' checkouts from snapshots
/// under its staging folder. A snapshot lives until its last piece is
/// read, it sits idle past the TTL, its source is forgotten, or the source
/// is dropped.
pub struct PieceSource {
    staging: PathBuf,
    cap: u64,
    max_entries: u64,
    ttl: Duration,
    snapshots: Mutex<HashMap<String, Snapshot>>,
}

impl PieceSource {
    /// A source staging snapshots under `staging` (created on first use),
    /// with the [`MAX_PULL_BYTES`] and [`MAX_PULL_ENTRIES`] caps.
    #[must_use]
    pub fn new(staging: PathBuf) -> Self {
        Self::with_caps(staging, MAX_PULL_BYTES, MAX_PULL_ENTRIES)
    }

    /// [`Self::new`] with other caps.
    #[must_use]
    pub fn with_caps(staging: PathBuf, cap: u64, max_entries: u64) -> Self {
        Self {
            staging,
            cap,
            max_entries,
            ttl: SNAPSHOT_IDLE_TTL,
            snapshots: Mutex::new(HashMap::new()),
        }
    }

    /// The same source with another idle TTL.
    #[must_use]
    pub fn idle_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    fn snapshots(&self) -> std::sync::MutexGuard<'_, HashMap<String, Snapshot>> {
        self.snapshots
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn remove_where(&self, mut doomed: impl FnMut(&Snapshot) -> bool) {
        self.snapshots().retain(|_, snapshot| {
            let remove = doomed(snapshot);
            if remove {
                let _ = std::fs::remove_file(&snapshot.file);
            }
            !remove
        });
    }

    /// Remove every ready snapshot idle past the TTL.
    pub fn sweep(&self) {
        let now = Instant::now();
        let ttl = self.ttl;
        self.remove_where(|snapshot| snapshot.ready && now.duration_since(snapshot.used) >= ttl);
    }

    /// Sweep `source` every half TTL (at most every 30 s) on the current
    /// tokio runtime, until it is dropped.
    pub fn spawn_sweeper(source: &std::sync::Arc<Self>) {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let weak = std::sync::Arc::downgrade(source);
        let every = (source.ttl / 2).clamp(Duration::from_secs(1), Duration::from_secs(30));
        // caller-binding: allow — removes expired snapshot files; acts for no caller
        runtime.spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                let Some(source) = weak.upgrade() else {
                    return;
                };
                source.sweep();
            }
        });
    }

    /// Remove every snapshot of `source` (its checkout is gone).
    pub fn forget(&self, source: &str) {
        self.remove_where(|snapshot| snapshot.source == source);
    }

    /// Remove snapshot `transfer_id` (its pull failed).
    pub fn release(&self, transfer_id: &str) {
        let mut snapshots = self.snapshots();
        if let Some(snapshot) = snapshots.remove(transfer_id) {
            let _ = std::fs::remove_file(snapshot.file);
        }
    }

    /// One piece of `read.path` in `checkout`, the checkout of agent
    /// `source`. Without a transfer id the piece starts a snapshot (offset
    /// 0 only); with one it reads that snapshot, which must be `source`'s
    /// for the same path. Blocking: run it off the async runtime.
    ///
    /// # Errors
    ///
    /// `InvalidParams` with a `file-pull-*` code: `outside-checkout`,
    /// `not-found`, `too-large`, `busy`, `transfer-expired`, `mismatch`
    /// (an offset past the end) or `source-unavailable`; `Internal` for an
    /// I/O failure.
    pub fn read(
        &self,
        source: &str,
        checkout: &Path,
        read: &PieceRequest,
    ) -> Result<CheckoutPiece> {
        self.sweep();
        let length = read.length.min(MAX_LINK_ATTACHMENT_CHUNK_BYTES);
        let expired = || refused("transfer-expired", "no live snapshot; pull again");
        let (id, first) = match &read.transfer_id {
            Some(_) if read.offset == 0 => {
                return Err(mismatch("a transferId names only pieces past offset 0"))
            }
            Some(id) => (id.clone(), None),
            None if read.offset != 0 => {
                return Err(mismatch("a piece past offset 0 needs its transferId"))
            }
            None => {
                let (id, sha256, skipped) = self.snapshot(source, checkout, &read.path)?;
                (id, Some((sha256, skipped)))
            }
        };
        let (file, kind, size) = {
            let mut snapshots = self.snapshots();
            let snapshot = snapshots
                .get_mut(&id)
                .filter(|snapshot| {
                    snapshot.ready && snapshot.source == source && snapshot.path == read.path
                })
                .ok_or_else(expired)?;
            snapshot.used = Instant::now();
            (snapshot.file.clone(), snapshot.kind, snapshot.size)
        };
        if read.offset >= size && !(read.offset == 0 && size == 0) {
            return Err(mismatch(format!(
                "offset {} is at or past the end ({size} bytes)",
                read.offset
            )));
        }
        let data = read_range(&file, read.offset, length)?;
        if read.offset + data.len() as u64 >= size {
            self.remove_where(|snapshot| snapshot.file == file);
        }
        let (sha256, skipped) = match first {
            Some((sha256, skipped)) => (Some(sha256), skipped),
            None => (None, None),
        };
        Ok(CheckoutPiece {
            kind,
            size,
            offset: read.offset,
            data: encode(&data),
            sha256,
            transfer_id: id,
            skipped,
        })
    }

    /// Make the snapshot of `rel` in `checkout` for `source` and register
    /// it; returns its id, sha256 and, for a folder, the skipped entries.
    fn snapshot(
        &self,
        source: &str,
        checkout: &Path,
        rel: &str,
    ) -> Result<(String, String, Option<u64>)> {
        let (root, canonical) = resolve_source(checkout, rel)?;
        let id = uuid::Uuid::new_v4().simple().to_string();
        let file = self.staging.join(&id);
        {
            // Count and reserve under one lock, so concurrent first pieces
            // never pass the cap together.
            let mut snapshots = self.snapshots();
            let live = snapshots
                .values()
                .filter(|snapshot| snapshot.source == source)
                .count();
            if live >= MAX_SNAPSHOTS_PER_SOURCE {
                return Err(refused(
                    "busy",
                    format!(
                        "agent {source} already serves {MAX_SNAPSHOTS_PER_SOURCE} pulls; retry later"
                    ),
                ));
            }
            snapshots.insert(
                id.clone(),
                Snapshot {
                    source: source.to_owned(),
                    path: rel.to_owned(),
                    file: file.clone(),
                    kind: CheckoutEntryKind::File,
                    size: 0,
                    used: Instant::now(),
                    ready: false,
                },
            );
        }
        let made = self.make(&root, &canonical, &file, rel);
        let mut snapshots = self.snapshots();
        match (made, snapshots.get_mut(&id)) {
            (Ok((kind, size, skipped, sha256)), Some(snapshot)) => {
                snapshot.kind = kind;
                snapshot.size = size;
                snapshot.used = Instant::now();
                snapshot.ready = true;
                Ok((id, sha256, skipped))
            }
            (made, _) => {
                snapshots.remove(&id);
                let _ = std::fs::remove_file(&file);
                // Forgotten while it was made: its checkout went away.
                made.and(Err(refused(
                    "source-unavailable",
                    "the agent's checkout went away",
                )))
            }
        }
    }

    /// Write the snapshot of `canonical` to `file`: its kind, size, skipped
    /// entries and sha256.
    fn make(
        &self,
        root: &Path,
        canonical: &Path,
        file: &Path,
        rel: &str,
    ) -> Result<(CheckoutEntryKind, u64, Option<u64>, String)> {
        std::fs::create_dir_all(&self.staging).map_err(|e| io("make the snapshot", &e))?;
        let meta = std::fs::metadata(canonical).map_err(|_| refused("not-found", rel))?;
        let made = if meta.is_file() {
            copy_file(root, canonical, file, self.cap, rel)
                .map(|size| (CheckoutEntryKind::File, size, None))
        } else if meta.is_dir() {
            pack_folder(root, canonical, file, self.cap, self.max_entries)
                .map(|(size, skipped)| (CheckoutEntryKind::Directory, size, Some(skipped)))
        } else {
            Err(refused("not-found", rel))
        };
        let made = made.and_then(|(kind, size, skipped)| {
            let sha256 = sha256_file(file).map_err(|e| io("make the snapshot", &e))?;
            Ok((kind, size, skipped, sha256))
        });
        made.map_err(|error| match error {
            Error::InvalidParams(message) if message == OVER_CAP => refused(
                "too-large",
                format!(
                    "{rel} is over the limit of {} bytes or {} entries",
                    self.cap, self.max_entries
                ),
            ),
            other => other,
        })
    }
}

impl Drop for PieceSource {
    fn drop(&mut self) {
        for (_, snapshot) in self.snapshots().drain() {
            let _ = std::fs::remove_file(snapshot.file);
        }
    }
}

/// The internal marker of a snapshot stopped at a cap.
const OVER_CAP: &str = "over-cap";

fn over_cap() -> Error {
    Error::InvalidParams(OVER_CAP.to_owned())
}

/// Open `canonical` (already resolved inside `root`) without following a
/// final link, and check the opened file is still the one at a confined
/// path: a swap for a link between the check and the open is refused.
fn open_confined(root: &Path, canonical: &Path, shown: &str) -> Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(canonical).map_err(|_| outside(shown))?;
    let again = std::fs::canonicalize(canonical).map_err(|_| outside(shown))?;
    if again != canonical || !confined(root, &again) {
        return Err(outside(shown));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let opened = file.metadata().map_err(|e| io("read the source", &e))?;
        let there = std::fs::symlink_metadata(&again).map_err(|_| outside(shown))?;
        if (opened.dev(), opened.ino()) != (there.dev(), there.ino()) || !opened.is_file() {
            return Err(outside(shown));
        }
    }
    Ok(file)
}

/// Copy `source` (opened once, see [`open_confined`]) to `dest`, stopping
/// past `cap`.
fn copy_file(root: &Path, source: &Path, dest: &Path, cap: u64, shown: &str) -> Result<u64> {
    let input = open_confined(root, source, shown)?;
    let mut output = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dest)
        .map_err(|e| io("make the snapshot", &e))?;
    let copied = std::io::copy(&mut input.take(cap + 1), &mut output)
        .map_err(|e| io("make the snapshot", &e))?;
    if copied > cap {
        return Err(over_cap());
    }
    Ok(copied)
}

/// At most `length` bytes of `path` from `offset`.
fn read_range(path: &Path, offset: u64, length: u64) -> Result<Vec<u8>> {
    let mut file = std::fs::File::open(path).map_err(|e| io("read the snapshot", &e))?;
    file.seek(std::io::SeekFrom::Start(offset))
        .map_err(|e| io("read the snapshot", &e))?;
    let mut data = Vec::new();
    file.take(length)
        .read_to_end(&mut data)
        .map_err(|e| io("read the snapshot", &e))?;
    Ok(data)
}

/// Counts what the archive writes and fails past the cap, so an oversized
/// folder is never packed whole.
struct CountingWriter<W> {
    inner: W,
    written: u64,
    cap: u64,
    over: std::rc::Rc<std::cell::Cell<bool>>,
}

impl<W: std::io::Write> std::io::Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.written + buf.len() as u64 > self.cap {
            self.over.set(true);
            return Err(std::io::Error::other("the folder is over the pull cap"));
        }
        let written = self.inner.write(buf)?;
        self.written += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Reads exactly `remaining` bytes from a file opened for packing: an end
/// before that (the file shrank) is an error, and marks `changed`.
struct Exact<R> {
    inner: R,
    remaining: u64,
    changed: std::rc::Rc<std::cell::Cell<bool>>,
}

impl<R: std::io::Read> std::io::Read for Exact<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.remaining == 0 {
            return Ok(0);
        }
        let cap = buf
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        let read = self.inner.read(&mut buf[..cap])?;
        if read == 0 {
            self.changed.set(true);
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "a file shrank while the folder was packed",
            ));
        }
        self.remaining -= read as u64;
        Ok(read)
    }
}

/// Packing state: the archive, the entry count and the skipped entries.
struct Packer<W: std::io::Write> {
    builder: tar::Builder<W>,
    root: PathBuf,
    entries: u64,
    max_entries: u64,
    skipped: u64,
    /// The folders being walked, so a link back to one is not walked twice.
    ancestors: Vec<PathBuf>,
    /// Set when a file shrank while it was packed.
    changed: std::rc::Rc<std::cell::Cell<bool>>,
}

/// Pack `dir` (canonical, inside the canonical `root`) into `archive` as a
/// pax tar of its regular files and folders with entry paths relative to
/// `dir`. Links that resolve inside `root` (not in `.git`) are followed and
/// stored as their targets; `.git`, links that lead outside and anything
/// else are skipped. Returns the archive's size and the skipped count;
/// stops as soon as the archive passes `cap` bytes or `max_entries`.
fn pack_folder(
    root: &Path,
    dir: &Path,
    archive: &Path,
    cap: u64,
    max_entries: u64,
) -> Result<(u64, u64)> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(archive)
        .map_err(|e| io("make the snapshot", &e))?;
    let over = std::rc::Rc::new(std::cell::Cell::new(false));
    let mut packer = Packer {
        builder: tar::Builder::new(CountingWriter {
            inner: std::io::BufWriter::new(file),
            written: 0,
            cap,
            over: over.clone(),
        }),
        root: root.to_path_buf(),
        entries: 0,
        max_entries,
        skipped: 0,
        ancestors: vec![dir.to_path_buf()],
        changed: std::rc::Rc::default(),
    };
    let changed = packer.changed.clone();
    let result = packer.walk(dir, Path::new("")).and_then(|()| {
        let skipped = packer.skipped;
        let mut counting = packer
            .builder
            .into_inner()
            .map_err(|e| io("pack the folder", &e))?;
        counting
            .inner
            .flush()
            .map_err(|e| io("pack the folder", &e))?;
        Ok((counting.written, skipped))
    });
    if over.get() {
        return Err(over_cap());
    }
    if changed.get() {
        return Err(refused(
            "source-unavailable",
            "a file changed while the folder was packed; retry the pull",
        ));
    }
    result
}

impl<W: std::io::Write> Packer<W> {
    fn walk(&mut self, dir: &Path, rel: &Path) -> Result<()> {
        let mut names: Vec<_> = std::fs::read_dir(dir)
            .map_err(|e| io("read the folder", &e))?
            .filter_map(|entry| entry.ok().map(|entry| entry.file_name()))
            .collect();
        names.sort();
        for name in names {
            let entry_rel = rel.join(&name);
            let resolved = std::fs::canonicalize(dir.join(&name)).ok();
            let Some(canonical) =
                resolved.filter(|canonical| !is_git(&name) && confined(&self.root, canonical))
            else {
                self.skipped += 1;
                continue;
            };
            match std::fs::metadata(&canonical) {
                Ok(meta) if meta.is_dir() => {
                    if self.ancestors.contains(&canonical) {
                        self.skipped += 1;
                        continue;
                    }
                    self.append(
                        &entry_rel,
                        tar::EntryType::Directory,
                        0o755,
                        0,
                        std::io::empty(),
                    )?;
                    self.ancestors.push(canonical.clone());
                    self.walk(&canonical, &entry_rel)?;
                    self.ancestors.pop();
                }
                Ok(meta) if meta.is_file() => {
                    let opened = open_confined(&self.root, &canonical, "").and_then(|file| {
                        let meta = file.metadata().map_err(|e| io("read the folder", &e))?;
                        Ok((meta, file))
                    });
                    let Ok((opened_meta, file)) = opened else {
                        self.skipped += 1;
                        continue;
                    };
                    let mode = if executable(&meta) { 0o755 } else { 0o644 };
                    // The size of the file as opened; a later change to it
                    // fails the pack rather than corrupting the archive.
                    let size = opened_meta.len();
                    let exact = Exact {
                        inner: file,
                        remaining: size,
                        changed: self.changed.clone(),
                    };
                    self.append(&entry_rel, tar::EntryType::Regular, mode, size, exact)?;
                }
                _ => self.skipped += 1,
            }
        }
        Ok(())
    }

    /// One ustar entry; a path too long for the header goes in a pax
    /// `path` record before it.
    fn append(
        &mut self,
        rel: &Path,
        kind: tar::EntryType,
        mode: u32,
        size: u64,
        data: impl std::io::Read,
    ) -> Result<()> {
        self.entries += 1;
        if self.entries > self.max_entries {
            return Err(over_cap());
        }
        let mut header = tar::Header::new_ustar();
        header.set_entry_type(kind);
        header.set_mode(mode);
        header.set_size(size);
        header.set_mtime(0);
        if header.set_path(rel).is_err() {
            let path = rel.to_string_lossy();
            self.builder
                .append_pax_extensions([("path", path.as_bytes())])
                .map_err(|e| io("pack the folder", &e))?;
            let short: String = rel
                .file_name()
                .map(|name| name.to_string_lossy().chars().take(90).collect())
                .unwrap_or_default();
            header
                .set_path(short)
                .map_err(|e| io("pack the folder", &e))?;
        }
        header.set_cksum();
        // A file that grew since it was opened is cut at `size`.
        self.builder
            .append(&header, data.take(size))
            .map_err(|e| io("pack the folder", &e))
    }
}

#[cfg(unix)]
fn executable(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    meta.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn executable(_meta: &std::fs::Metadata) -> bool {
    false
}

/// Where a pull lands in the caller's checkout.
struct Destination {
    /// `to` as given, for the result.
    rel: String,
    /// The canonical checkout.
    root: PathBuf,
    /// The final path's components below `root`, resolved when the pull
    /// began; every write walks them again without following links.
    parts: Vec<String>,
    /// The checkout's root, opened before the first piece is fetched: every
    /// write walks from this handle, never from the path again.
    handle: place::Folder,
}

impl Destination {
    fn dest(&self) -> PathBuf {
        self.parts
            .iter()
            .fold(self.root.clone(), |path, part| path.join(part))
    }

    /// A refusal for what was found on the way to the destination.
    fn refuse(&self) -> impl Fn(place::Entry) -> Error + '_ {
        move |found| match found {
            place::Entry::Other => conflict(&self.rel, "a file is in the way of its folder"),
            _ => outside(&self.rel),
        }
    }
}

/// Check `to` in `dest_root` before anything is fetched: a plain relative
/// path (not under `.intent`) whose existing part resolves inside the
/// checkout (not in `.git` or `.intent`) and is a folder.
fn resolve_destination(dest_root: &Path, to: &str) -> Result<Destination> {
    let parts = plain_parts(to)
        .filter(|parts| {
            parts
                .first()
                .is_some_and(|first| !first.eq_ignore_ascii_case(".intent"))
        })
        .ok_or_else(|| outside(to))?;
    let root = std::fs::canonicalize(dest_root)
        .map_err(|e| io("the caller's checkout is unavailable", &e))?;
    let dest = parts
        .iter()
        .fold(root.clone(), |path, part| path.join(part));
    let mut anchor = dest
        .parent()
        .map_or_else(|| root.clone(), Path::to_path_buf);
    while std::fs::symlink_metadata(&anchor).is_err() {
        match anchor.parent() {
            Some(parent) if anchor != root => anchor = parent.to_path_buf(),
            _ => return Err(outside(to)),
        }
    }
    let canonical = std::fs::canonicalize(&anchor).map_err(|_| outside(to))?;
    if !confined(&root, &canonical) {
        return Err(outside(to));
    }
    if !canonical.is_dir() {
        return Err(conflict(to, "a file is in the way of its folder"));
    }
    // `to` itself, below its existing (confined) ancestor.
    let rest = dest.strip_prefix(&anchor).unwrap_or(Path::new(""));
    let resolved = canonical.join(rest);
    let parts: Vec<String> = resolved
        .strip_prefix(&root)
        .map_err(|_| outside(to))?
        .components()
        .map(|part| part.as_os_str().to_string_lossy().into_owned())
        .collect();
    if parts
        .first()
        .is_none_or(|first| first.eq_ignore_ascii_case(".intent"))
    {
        return Err(outside(to));
    }
    let handle = place::Folder::root(&root).map_err(|_| outside(to))?;
    Ok(Destination {
        rel: to.to_owned(),
        root,
        parts,
        handle,
    })
}

fn conflict(to: &str, why: &str) -> Error {
    refused("destination-conflict", format!("{to}: {why}"))
}

/// A pull being written: the bytes go to a part file under
/// `.intent/pulls/`, removed (with any folder it unpacked and any parent
/// it made) if the pull does not finish.
struct PieceSink {
    to: Destination,
    kind: CheckoutEntryKind,
    size: u64,
    sha256: String,
    /// The checkout's `.intent/pulls/`, opened from the root handle
    /// without following links.
    pulls: place::Folder,
    /// The part file's name in `pulls`.
    part: String,
    file: Option<std::fs::File>,
    hasher: sha2::Sha256,
    written: u64,
    /// The unpacked folder's name in `pulls`.
    unpacked: Option<String>,
    made: Vec<PathBuf>,
}

impl PieceSink {
    fn begin(to: Destination, kind: CheckoutEntryKind, size: u64, sha256: String) -> Result<Self> {
        use sha2::Digest as _;
        check_existing(&to, kind)?;
        let refuse = |_| outside(PULLS_DIR);
        let mut kept = Vec::new();
        let pulls = to
            .handle
            .child(".intent", true, &mut kept, &refuse)?
            .child("pulls", true, &mut kept, &refuse)?;
        if pulls.entry(".gitignore")? == place::Entry::Missing {
            pulls
                .create_file(".gitignore")?
                .write_all(b"*\n")
                .map_err(|e| io("prepare .intent/pulls", &e))?;
        }
        let part = format!("{}.part", uuid::Uuid::new_v4().simple());
        let file = pulls.create_file(&part)?;
        Ok(Self {
            to,
            kind,
            size,
            sha256,
            pulls,
            part,
            file: Some(file),
            hasher: sha2::Sha256::new(),
            written: 0,
            unpacked: None,
            made: Vec::new(),
        })
    }

    fn append(&mut self, bytes: &[u8]) -> Result<()> {
        use sha2::Digest as _;
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| Error::Internal("file pull already finished".into()))?;
        file.write_all(bytes)
            .map_err(|e| io("write the copy", &e))?;
        self.hasher.update(bytes);
        self.written += bytes.len() as u64;
        Ok(())
    }

    fn finish(mut self) -> Result<Value> {
        use sha2::Digest as _;
        let mut written = self
            .file
            .take()
            .ok_or_else(|| Error::Internal("file pull already finished".into()))?;
        written.sync_all().map_err(|e| io("write the copy", &e))?;
        let digest = lower_hex(&self.hasher.clone().finalize());
        if self.written != self.size || !digest.eq_ignore_ascii_case(&self.sha256) {
            return Err(mismatch(format!(
                "{} arrived as {} bytes with sha256 {digest}, but the source has {} bytes with sha256 {}; try again",
                self.to.rel, self.written, self.size, self.sha256
            )));
        }
        let files = match self.kind {
            CheckoutEntryKind::File => {
                let (parent, name) = self.parent()?;
                if parent.entry(&name)? == place::Entry::Folder {
                    return Err(conflict(&self.to.rel, "a folder is there"));
                }
                self.pulls.rename(&self.part, &parent, &name)?;
                1
            }
            CheckoutEntryKind::Directory => {
                // The archive is read back through the descriptor it was
                // written with, never reopened by path.
                written
                    .seek(std::io::SeekFrom::Start(0))
                    .map_err(|e| io("unpack the folder", &e))?;
                self.place_folder(written)?
            }
        };
        self.made.clear();
        Ok(json!({
            "path": self.to.rel,
            "kind": self.kind,
            "size": self.size,
            "sha256": self.sha256,
            "files": files,
        }))
    }

    /// The destination's parent folder, walked from the root without
    /// following links and made where missing, and the destination's name.
    fn parent(&mut self) -> Result<(place::Folder, String)> {
        let (name, parents) = self
            .to
            .parts
            .split_last()
            .ok_or_else(|| outside(&self.to.rel))?;
        let refuse = self.to.refuse();
        let parent = self
            .to
            .handle
            .walk(parents, true, &mut self.made, &refuse)?;
        Ok((parent, name.clone()))
    }

    /// Unpack the verified archive into a folder under `.intent/pulls/`,
    /// then move it into place, merging into a folder already there.
    /// Returns the number of regular files written.
    fn place_folder(&mut self, archive: std::fs::File) -> Result<u64> {
        let name = self.part.replace(".part", ".d");
        let mut kept = Vec::new();
        let unpacked = self
            .pulls
            .child(&name, true, &mut kept, &|_| outside(PULLS_DIR))?;
        self.unpacked = Some(name.clone());
        let files = unpack(archive, &unpacked)?;
        let (parent, last) = self.parent()?;
        match parent.entry(&last)? {
            place::Entry::Missing => {
                self.pulls.rename(&name, &parent, &last)?;
                self.unpacked = None;
                return Ok(files);
            }
            place::Entry::Folder => {}
            place::Entry::Link => return Err(outside(&self.to.rel)),
            place::Entry::Other => return Err(conflict(&self.to.rel, "a file is there")),
        }
        let refuse = self.to.refuse();
        let into = parent.child(&last, false, &mut Vec::new(), &refuse)?;
        let mut moves = Vec::new();
        plan_merge(&unpacked, &into, &[], &self.to.rel, &mut moves)?;
        for parts in moves {
            let Some((entry, parents)) = parts.split_last() else {
                continue;
            };
            let from = unpacked.walk(parents, false, &mut Vec::new(), &|_| outside(PULLS_DIR))?;
            let to = into.walk(parents, false, &mut Vec::new(), &refuse)?;
            if to.entry(entry)? == place::Entry::Folder {
                return Err(conflict(
                    &self.to.rel,
                    "a file and a folder have the same name",
                ));
            }
            from.rename(entry, &to, entry)?;
        }
        Ok(files)
    }
}

/// Refuse a destination whose existing entry does not fit `kind`: a file
/// onto a folder or a folder onto a file conflicts, and a folder onto a
/// link would write through it. A file replaces a file or a link.
fn check_existing(to: &Destination, kind: CheckoutEntryKind) -> Result<()> {
    let Ok(meta) = std::fs::symlink_metadata(to.dest()) else {
        return Ok(());
    };
    match kind {
        CheckoutEntryKind::File if meta.is_dir() => Err(conflict(&to.rel, "a folder is there")),
        CheckoutEntryKind::Directory if meta.file_type().is_symlink() => Err(outside(&to.rel)),
        CheckoutEntryKind::Directory if !meta.is_dir() => Err(conflict(&to.rel, "a file is there")),
        _ => Ok(()),
    }
}

/// Set `file`'s mode (0644 or 0755) through its handle.
fn set_mode(file: &std::fs::File, mode: u32) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        file.set_permissions(std::fs::Permissions::from_mode(mode))
            .map_err(|e| io("unpack the folder", &e))
    }
    #[cfg(not(unix))]
    {
        let _ = (file, mode);
        Ok(())
    }
}

/// Unpack `archive` into the empty folder `into`, allowing only what a
/// snapshot holds: regular files and folders with plain relative paths
/// (no `.git`), modes 0644 or 0755, and pax records setting only `path`
/// and `size`, each path once. Returns the number of regular files.
fn unpack(archive: std::fs::File, into: &place::Folder) -> Result<u64> {
    let mut archive = tar::Archive::new(archive);
    let (mut files, mut entries) = (0, 0);
    let mut seen = std::collections::HashSet::new();
    for entry in archive.entries().map_err(|e| io("unpack the folder", &e))? {
        let mut entry = entry.map_err(|_| mismatch("the archive is malformed"))?;
        entries += 1;
        if entries > MAX_PULL_ENTRIES {
            return Err(refused(
                "too-large",
                format!("over the limit of {MAX_PULL_ENTRIES} entries"),
            ));
        }
        let name = String::from_utf8_lossy(&entry.path_bytes()).into_owned();
        let not_allowed = |what: &str| mismatch(format!("the archive holds {what} {name}"));
        let kind = entry.header().entry_type();
        let file = kind == tar::EntryType::Regular;
        if !(file || kind == tar::EntryType::Directory) {
            return Err(not_allowed("a link or special entry"));
        }
        let Some(parts) = plain_parts(&name).filter(|parts| !parts.is_empty()) else {
            return Err(not_allowed("the path"));
        };
        let parts: Vec<String> = parts.into_iter().map(str::to_owned).collect();
        if !seen.insert(name.trim_end_matches('/').to_owned()) {
            return Err(not_allowed("twice the path"));
        }
        let mode = entry
            .header()
            .mode()
            .map_err(|_| not_allowed("a bad mode on"))?
            & 0o7777;
        if !(mode == 0o644 || mode == 0o755) {
            return Err(not_allowed("a mode not allowed on"));
        }
        if let Ok(Some(records)) = entry.pax_extensions() {
            for record in records {
                let record = record.map_err(|_| not_allowed("a bad pax record on"))?;
                if !matches!(record.key_bytes(), b"path" | b"size") {
                    return Err(not_allowed("a pax record not allowed on"));
                }
            }
        }
        // Written through handles from `into`: no entry follows a link.
        let refuse = |_| outside(&name);
        let mut created_dirs = Vec::new();
        if file {
            let (last, parents) = parts.split_last().ok_or_else(|| not_allowed("the path"))?;
            let parent = into.walk(parents, true, &mut created_dirs, &refuse)?;
            let mut out = parent.create_file(last)?;
            std::io::copy(&mut entry, &mut out)
                .map_err(|_| mismatch("the archive is malformed"))?;
            set_mode(&out, mode)?;
            files += 1;
        } else {
            into.walk(&parts, true, &mut created_dirs, &refuse)?;
        }
    }
    Ok(files)
}

/// The entries (relative to `from`, as components) that merge the
/// unpacked folder `from` into the existing folder `into`: a new entry
/// moves whole, a file replaces a file or a link (as a link, never through
/// it), and a folder merges into a folder. A folder onto a link is outside
/// the checkout; a file against a folder conflicts. Both sides are read
/// through handles; found before anything moves.
fn plan_merge(
    from: &place::Folder,
    into: &place::Folder,
    rel: &[String],
    shown: &str,
    moves: &mut Vec<Vec<String>>,
) -> Result<()> {
    for (name, incoming_dir) in from.list()? {
        let mut entry_rel = rel.to_vec();
        entry_rel.push(name.clone());
        let label = format!("{shown}/{}", entry_rel.join("/"));
        match (incoming_dir, into.entry(&name)?) {
            (_, place::Entry::Missing) | (false, place::Entry::Other | place::Entry::Link) => {
                moves.push(entry_rel);
            }
            (true, place::Entry::Link) => return Err(outside(&label)),
            (true, place::Entry::Folder) => {
                let refuse = |_| outside(&label);
                let inner_from = from.child(&name, false, &mut Vec::new(), &refuse)?;
                let inner_into = into.child(&name, false, &mut Vec::new(), &refuse)?;
                plan_merge(&inner_from, &inner_into, &entry_rel, shown, moves)?;
            }
            _ => return Err(conflict(&label, "a file and a folder have the same name")),
        }
    }
    Ok(())
}

impl Drop for PieceSink {
    fn drop(&mut self) {
        self.file.take();
        let _ = self.pulls.remove_file(&self.part);
        if let Some(unpacked) = self.unpacked.take() {
            let _ = self.pulls.remove_tree(&unpacked);
        }
        for dir in self.made.drain(..).rev() {
            let _ = std::fs::remove_dir(dir);
        }
    }
}

/// Run `f` on a blocking thread.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Internal(format!("file pull task failed: {e}")))?
}

/// Whether pulling `path` to `to` within one checkout copies it onto
/// itself or a folder into itself.
fn onto_itself(path: &str, to: &str) -> bool {
    match (plain_parts(path), plain_parts(to)) {
        (Some(path), Some(to)) => to.starts_with(&path),
        _ => false,
    }
}

/// Pull into `to` in the checkout `dest_root`, asking `fetch(offset,
/// transfer_id)` for each piece where the copy ends. `same_source` is the
/// source path when the source checkout is this one. `to` is checked
/// before the first fetch; the copy is kept only whole, at most `cap`
/// bytes, with the first piece's size and sha256. Returns `{ path, kind,
/// size, sha256, files, skipped }`.
///
/// # Errors
///
/// A `file-pull-*` `InvalidParams` for a refused destination, an oversize
/// or broken transfer, or `fetch`'s own error (a lost link, say). Nothing
/// is left behind on failure.
pub async fn pull_into<F, Fut>(
    dest_root: &Path,
    to: &str,
    same_source: Option<&str>,
    cap: u64,
    mut fetch: F,
) -> Result<Value>
where
    F: FnMut(u64, Option<String>) -> Fut,
    Fut: Future<Output = Result<CheckoutPiece>>,
{
    let (root, rel) = (dest_root.to_path_buf(), to.to_owned());
    let destination = blocking(move || resolve_destination(&root, &rel)).await?;
    if same_source.is_some_and(|path| onto_itself(path, to)) {
        return Err(conflict(to, "it is the source itself, or inside it"));
    }
    let first = fetch(0, None).await?;
    let sha256 = first
        .sha256
        .clone()
        .filter(|_| first.offset == 0)
        .ok_or_else(|| mismatch("the first piece has no sha256"))?;
    if first.size > cap {
        return Err(refused(
            "too-large",
            format!("{} bytes; limit {cap}", first.size),
        ));
    }
    let (kind, size, transfer, skipped) = (
        first.kind,
        first.size,
        first.transfer_id.clone(),
        first.skipped.unwrap_or(0),
    );
    let mut writer = blocking(move || PieceSink::begin(destination, kind, size, sha256)).await?;
    let mut piece = first;
    loop {
        if piece.offset != writer.written
            || piece.size != size
            || piece.kind != kind
            || piece.transfer_id != transfer
        {
            return Err(mismatch("a piece did not start where the copy ended"));
        }
        let bytes = decode(&piece.data).ok_or_else(|| mismatch("a piece is not base64"))?;
        if writer.written + bytes.len() as u64 > size {
            return Err(mismatch("the source sent more bytes than the snapshot has"));
        }
        let done = bytes.is_empty();
        writer = blocking(move || writer.append(&bytes).map(|()| writer)).await?;
        if done || writer.written >= size {
            break;
        }
        piece = fetch(writer.written, Some(transfer.clone())).await?;
    }
    let mut result = blocking(move || writer.finish()).await?;
    result["skipped"] = json!(skipped);
    Ok(result)
}

/// Which caller opened a transfer, for which source and path.
struct Opened {
    caller: Option<AgentId>,
    from: AgentId,
    path: String,
    used: Instant,
}

/// The backend's record of open transfers: a `transferId` answers only the
/// caller that opened it, for the same source and path.
#[derive(Default)]
pub(crate) struct PullTransfers(Mutex<HashMap<String, Opened>>);

impl PullTransfers {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Opened>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl crate::Services {
    fn pulls(&self) -> std::sync::Arc<PieceSource> {
        self.pulls
            .get_or_init(|| {
                // Snapshots never outlive the daemon that made them.
                let staging = self.script_output_config.data_dir.join("pulls");
                let _ = std::fs::remove_dir_all(&staging);
                let source = std::sync::Arc::new(PieceSource::new(staging));
                PieceSource::spawn_sweeper(&source);
                source
            })
            .clone()
    }

    /// Drop the snapshots local agent `agent` serves (it is going away).
    pub(crate) fn forget_pulls_of(&self, agent: &AgentId) {
        if let Some(pulls) = self.pulls.get() {
            pulls.forget(agent.as_str());
        }
    }

    /// The working directory of local agent `agent` of `workspace_id` on
    /// this backend; empty when it has none.
    async fn agent_root(&self, workspace_id: &WorkspaceId, agent: &AgentId) -> String {
        crate::file_ops::resolve_root(
            &self.store,
            workspace_id,
            Some(agent),
            self.chief_cwd_root.as_deref(),
        )
        .await
    }

    /// One piece of `read.path` in the checkout of `from`, an agent of
    /// `workspace_id`, for `caller`: read on its machine for a remote
    /// agent, in its working directory here otherwise. A later piece's
    /// `transferId` must be one `caller` opened for the same source and
    /// path. Serves both `ws.file.pull` here and a remote caller's
    /// `file.pullData`.
    ///
    /// # Errors
    ///
    /// `InvalidParams` with a `file-pull-*` code: `unknown-agent` for an
    /// agent that does not exist or is in another workspace,
    /// `transfer-expired` for a transfer the caller did not open, and the
    /// source's refusals (see [`PieceSource::read`]).
    pub async fn pull_piece(
        &self,
        workspace_id: &WorkspaceId,
        caller: Option<&AgentId>,
        from: &AgentId,
        read: PieceRequest,
    ) -> Result<CheckoutPiece> {
        let unknown = || refused("unknown-agent", from);
        match self.store.get_agent_session_summary(from).await {
            Ok(session) if session.workspace_id == *workspace_id => {}
            Ok(_) | Err(Error::NotFound(_)) => return Err(unknown()),
            Err(error) => return Err(error),
        }
        if let Some(id) = &read.transfer_id {
            let now = Instant::now();
            let mut opened = self.pull_transfers.lock();
            opened.retain(|_, open| now.duration_since(open.used) < SNAPSHOT_IDLE_TTL);
            match opened.get_mut(id) {
                Some(open)
                    if open.caller.as_ref() == caller
                        && open.from == *from
                        && open.path == read.path =>
                {
                    open.used = now;
                }
                _ => return Err(refused("transfer-expired", "no live snapshot; pull again")),
            }
        }
        let piece = if self.store.get_remote_agent(from).await?.is_some() {
            self.feature_remote_launch_port()
                .checkout_piece(self, from, read.clone())
                .await?
        } else {
            let root = self.agent_root(workspace_id, from).await;
            if root.is_empty() {
                return Err(refused("source-unavailable", "the agent has no checkout"));
            }
            let pulls = self.pulls();
            let (source, request) = (from.to_string(), read.clone());
            blocking(move || pulls.read(&source, Path::new(&root), &request)).await?
        };
        let done =
            piece.offset + decode(&piece.data).map_or(0, |data| data.len() as u64) >= piece.size;
        let mut opened = self.pull_transfers.lock();
        if done {
            opened.remove(&piece.transfer_id);
        } else if read.transfer_id.is_none() {
            opened.insert(
                piece.transfer_id.clone(),
                Opened {
                    caller: caller.cloned(),
                    from: from.clone(),
                    path: read.path,
                    used: Instant::now(),
                },
            );
        }
        Ok(piece)
    }

    /// `ws.file.pull` for a caller whose checkout is on this backend: the
    /// pieces come from [`Self::pull_piece`].
    pub(crate) async fn file_pull_here(
        &self,
        workspace_id: WorkspaceId,
        caller: Option<AgentId>,
        from: AgentId,
        path: String,
        to: Option<String>,
    ) -> Result<Value> {
        let Some(caller) = caller else {
            return Err(Error::InvalidParams(
                "ws.file.pull needs an agent caller: it copies into the calling agent's checkout"
                    .to_owned(),
            ));
        };
        self.require_member(&workspace_id).await?;
        let dest_root = self.agent_root(&workspace_id, &caller).await;
        let caller = Some(caller);
        if dest_root.is_empty() {
            return Err(Error::Internal(
                "workspace has no resolved filesystem root".to_string(),
            ));
        }
        let same = self.store.get_remote_agent(&from).await?.is_none()
            && self.agent_root(&workspace_id, &from).await == dest_root;
        let to = to.unwrap_or_else(|| path.clone());
        let opened = Mutex::new(None::<String>);
        let (workspace_id, caller, from, path) = (&workspace_id, caller.as_ref(), &from, &path);
        let pulled = pull_into(
            Path::new(&dest_root),
            &to,
            same.then_some(path.as_str()),
            MAX_PULL_BYTES,
            |offset, transfer_id| {
                let opened = &opened;
                async move {
                    let piece = self
                        .pull_piece(
                            workspace_id,
                            caller,
                            from,
                            PieceRequest {
                                path: path.clone(),
                                offset,
                                length: MAX_LINK_ATTACHMENT_CHUNK_BYTES,
                                transfer_id,
                            },
                        )
                        .await?;
                    *opened
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) =
                        Some(piece.transfer_id.clone());
                    Ok(piece)
                }
            },
        )
        .await;
        if pulled.is_err() {
            // A failed pull drops its snapshot here at once; a remote
            // source's expires on its machine.
            let opened = opened
                .into_inner()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(id) = opened {
                self.pull_transfers.lock().remove(&id);
                self.pulls().release(&id);
            }
        }
        pulled
    }
}

mod place;

#[cfg(test)]
mod tests;
