//! On-disk per-run script output store.
//!
//! Each script run's PTY output is appended to
//! `<data_dir>/script-output/<workspaceId>/<scriptId>/<runId>.log`. The store
//! lives in the daemon data dir, never in a worktree. It is self-contained: the
//! root directory and the per-run byte cap are plain constructor parameters, so
//! callers resolve `scripts.outputMaxBytesPerRun` themselves.
//!
//! - Writes go through [`RunOutputWriter`], which flushes every chunk to the
//!   OS (a read right after exit sees every byte) and never fails its caller:
//!   a disk error logs one warning and disables that run's writer, so the PTY
//!   reader and the live `script:output` stream keep going.
//! - Past the cap the oldest bytes are dropped and the newest kept: the file is
//!   compacted to its newest half (realigned to a line start when possible)
//!   before the chunk that would overflow it is appended. Compaction writes a
//!   sibling temp file and renames it over the log, so a concurrent read keeps
//!   its open handle on a complete (old) file and a fresh open sees the
//!   complete new one: readers never see a log shrink mid-read, and neither
//!   side ever waits on a lock. If the rename fails (e.g. a filesystem or old
//!   Windows that refuses to replace an open file), the log is never
//!   truncated or rewritten in place: the old file is kept, chunks are still
//!   appended up to twice the cap (past that they are dropped from disk; the
//!   live `script:output` stream is unaffected) and compaction is retried
//!   after every further quarter cap of output.
//! - Sweeps also delete compaction temp files (`*.compact`) left behind by a
//!   crash once they are a few minutes old, even when retention is disabled.
//! - Reads stream the file and hold at most the requested lines (plus one line
//!   at a time), never the whole log.
//! - Every byte of a run has a stream position that never goes back within
//!   one store (one daemon boot): compaction drops bytes without moving the
//!   kept ones, a reopened run continues where it stopped, and output that
//!   could not be saved still advances the count. All writers of a run share
//!   one state, and a cursor read takes its bytes and positions under that
//!   state's lock, so they always agree.
//! - IDs are percent-encoded into a single safe path segment, so no ID can
//!   name `.`/`..` or contain a separator and escape the root. Symlinks below
//!   the root are never followed: a symlinked folder or log is an error (the
//!   root itself may be a symlink, e.g. a relocated data dir).

use std::collections::HashMap;
use std::fmt::Write as _;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use intent_core::blob::{BlobFile, BlobKey, BlobKind, BlobStorage, LocalBlobStorage};
use intent_core::{Error, Result};
use serde_json::Value;

/// Directory under the daemon data dir that holds every script output log.
pub(crate) const SCRIPT_OUTPUT_DIR: &str = "script-output";

const LOG_EXT: &str = ".log";
/// Longest encoded path segment accepted (common filesystem `NAME_MAX`).
const MAX_SEGMENT_BYTES: usize = 255;
/// Age past which a compaction temp file is a crash leftover: a live
/// compaction finishes in well under a second.
const STALE_COMPACT_AGE: Duration = Duration::from_secs(300);
/// Chunk size for backward tail scans and line counting.
const SCAN_CHUNK: usize = 64 * 1024;

/// Identifies one run's log: the decoded workspace, script and run IDs.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RunKey {
    pub workspace: String,
    pub script: String,
    pub run: String,
}

/// The trailing lines of a run's log plus its total line count, using the
/// `split('\n')` line model of `script.output`'s legacy string read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TailText {
    /// The last `n` lines joined by `\n` (the whole log when it is shorter).
    pub text: String,
    /// `text.split('\n').count()` over the whole log.
    pub total_lines: usize,
}

/// A run log's newest bytes with their stream positions (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CursorTail {
    /// The raw log bytes at `[start, end)`.
    pub bytes: Vec<u8>,
    pub start: u64,
    pub end: u64,
    /// Position of the oldest byte the log keeps contiguous with the end
    /// (saved output older than unsaved output is left out).
    pub log_start: u64,
    /// `split('\n')` line count from `log_start`.
    pub total_lines: usize,
}

/// One run's write state, shared by its writers and cursor reads.
#[derive(Debug)]
struct RunState {
    file: Option<Box<dyn BlobFile>>,
    /// Bytes in the log.
    len: u64,
    /// Stream position of the log's first byte (`base + len` is the end).
    /// Bytes before `contiguous` precede output that was not saved, so their
    /// positions are unknown and cursor reads leave them out.
    base: u64,
    /// Log offset where the bytes contiguous with the end begin.
    contiguous: u64,
    cap: u64,
    retry: CompactionRetry,
    writers: usize,
}

impl RunState {
    fn new(cap: u64) -> Self {
        Self {
            file: None,
            len: 0,
            base: 0,
            contiguous: 0,
            cap,
            retry: CompactionRetry::default(),
            writers: 0,
        }
    }
}

/// Run states by log key. Lock order: this map, then a run's state. An entry
/// with no writer, `base == 0` and `contiguous == 0` is dropped, since a
/// missing entry means exactly that.
type Runs = Arc<Mutex<HashMap<BlobKey, Arc<Mutex<RunState>>>>>;

/// An open run log, the stream position of its first byte, and the log
/// range `[from, len)` to read: the bytes contiguous with the run's end.
type PositionedRead = (Box<dyn BlobFile>, u64, u64, u64);

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Outcome of [`ScriptOutputStore::sweep`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SweepReport {
    /// Expired logs deleted.
    pub deleted: usize,
    /// Expired logs skipped because their run is live.
    pub skipped_live: usize,
    /// Logs that could not be inspected or deleted (each logged as a warning).
    pub failed: usize,
    /// Stale compaction temp files (`*.compact`) deleted.
    pub stale_temps_deleted: usize,
}

/// The script output store: one log per run, kept in a [`BlobStorage`]
/// (on the daemon, files under `<data_dir>/script-output`).
#[derive(Debug, Clone)]
pub(crate) struct ScriptOutputStore {
    storage: Arc<dyn BlobStorage>,
    max_bytes_per_run: u64,
    runs: Runs,
}

impl ScriptOutputStore {
    /// A store rooted at `<data_dir>/script-output` whose run logs never grow
    /// past `max_bytes_per_run` bytes (clamped to at least 1). Nothing is
    /// created on disk until a writer opens.
    pub(crate) fn new(data_dir: &Path, max_bytes_per_run: u64) -> Self {
        Self::with_storage(
            Arc::new(LocalBlobStorage::new(data_dir.join(SCRIPT_OUTPUT_DIR))),
            max_bytes_per_run,
        )
    }

    /// A store over `storage` whose run logs never grow past
    /// `max_bytes_per_run` bytes (clamped to at least 1).
    pub(crate) fn with_storage(storage: Arc<dyn BlobStorage>, max_bytes_per_run: u64) -> Self {
        Self {
            storage,
            max_bytes_per_run: max_bytes_per_run.max(1),
            runs: Runs::default(),
        }
    }

    /// The store root (`<data_dir>/script-output`).
    #[cfg(test)]
    pub(crate) fn root(&self) -> PathBuf {
        self.storage.display_path(&BlobKey::root())
    }

    /// The per-run byte cap applied to writers opened from now on.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "wired by the script output cleanup change")
    )]
    pub(crate) fn max_bytes_per_run(&self) -> u64 {
        self.max_bytes_per_run
    }

    /// Change the per-run byte cap (clamped to at least 1) for writers opened
    /// from now on; open writers keep the cap they were opened with.
    pub(crate) fn set_max_bytes_per_run(&mut self, max_bytes_per_run: u64) {
        self.max_bytes_per_run = max_bytes_per_run.max(1);
    }

    /// `<workspaceId>`.
    fn workspace_key(workspace_id: &str) -> Result<BlobKey> {
        segment_key(&BlobKey::root(), encode_segment(workspace_id)?)
    }

    /// `<workspaceId>/<scriptId>`.
    fn script_key(workspace_id: &str, script_id: &str) -> Result<BlobKey> {
        segment_key(
            &Self::workspace_key(workspace_id)?,
            encode_segment(script_id)?,
        )
    }

    /// `<workspaceId>/<scriptId>/<runId>.log`.
    fn run_key(workspace_id: &str, script_id: &str, run_id: &str) -> Result<BlobKey> {
        let run = encode_segment(run_id)?;
        if run.len() + LOG_EXT.len() > MAX_SEGMENT_BYTES {
            return Err(Error::InvalidParams(format!(
                "script output run id is too long ({} bytes)",
                run_id.len()
            )));
        }
        segment_key(
            &Self::script_key(workspace_id, script_id)?,
            format!("{run}{LOG_EXT}"),
        )
    }

    /// `<root>/<workspaceId>`.
    #[cfg(test)]
    pub(crate) fn workspace_dir(&self, workspace_id: &str) -> Result<PathBuf> {
        Ok(self
            .storage
            .display_path(&Self::workspace_key(workspace_id)?))
    }

    /// `<root>/<workspaceId>/<scriptId>`.
    #[cfg(test)]
    pub(crate) fn script_dir(&self, workspace_id: &str, script_id: &str) -> Result<PathBuf> {
        Ok(self
            .storage
            .display_path(&Self::script_key(workspace_id, script_id)?))
    }

    /// `<root>/<workspaceId>/<scriptId>/<runId>.log`.
    #[cfg(test)]
    pub(crate) fn run_path(
        &self,
        workspace_id: &str,
        script_id: &str,
        run_id: &str,
    ) -> Result<PathBuf> {
        Ok(self
            .storage
            .display_path(&Self::run_key(workspace_id, script_id, run_id)?))
    }

    /// Open (creating parents) the append writer for one run. Never fails: an
    /// invalid ID or disk error logs a warning and yields a disabled writer
    /// whose appends are no-ops on disk but still advance the run's position.
    /// Reopening an existing run's log appends to it and continues its
    /// positions; writers open at the same time share one state.
    pub(crate) fn open_writer(
        &self,
        workspace_id: &str,
        script_id: &str,
        run_id: &str,
    ) -> RunOutputWriter {
        let key = match Self::run_key(workspace_id, script_id, run_id) {
            Ok(key) => key,
            Err(e) => {
                tracing::warn!(workspace = %workspace_id, script = %script_id, run = %run_id, error = %e, "script output log disabled: invalid id");
                let mut state = RunState::new(self.max_bytes_per_run);
                state.writers = 1;
                return RunOutputWriter {
                    storage: Arc::clone(&self.storage),
                    key: None,
                    path: PathBuf::new(),
                    state: Arc::new(Mutex::new(state)),
                    runs: Arc::clone(&self.runs),
                };
            }
        };
        let path = self.storage.display_path(&key);
        let mut runs = lock(&self.runs);
        let known = runs.contains_key(&key);
        let state = Arc::clone(
            runs.entry(key.clone())
                .or_insert_with(|| Arc::new(Mutex::new(RunState::new(self.max_bytes_per_run)))),
        );
        {
            let mut s = lock(&state);
            s.writers += 1;
            s.cap = self.max_bytes_per_run;
            if s.file.is_none() {
                match open_append(&*self.storage, &key, &path, s.cap) {
                    Ok((file, on_disk, len)) => {
                        // The log on disk holds the newest bytes before the
                        // run's end; positions in a new store count from 0.
                        let base = if known {
                            (s.base + s.len).saturating_sub(on_disk)
                        } else {
                            0
                        };
                        if on_disk != s.len {
                            s.contiguous = 0;
                        }
                        s.contiguous = s.contiguous.saturating_sub(on_disk - len);
                        s.base = base + (on_disk - len);
                        s.len = len;
                        s.file = Some(file);
                    }
                    Err(e) => {
                        tracing::warn!(path = %path.display(), error = %e, "script output log disabled: open failed");
                    }
                }
            }
        }
        drop(runs);
        RunOutputWriter {
            storage: Arc::clone(&self.storage),
            key: Some(key),
            path,
            state,
            runs: Arc::clone(&self.runs),
        }
    }

    /// Whether the run's log exists.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "wired by the script output cleanup change")
    )]
    pub(crate) fn exists(&self, workspace_id: &str, script_id: &str, run_id: &str) -> Result<bool> {
        let key = Self::run_key(workspace_id, script_id, run_id)?;
        self.storage
            .exists(&key)
            .map_err(|e| self.key_err(&key, &e))
    }

    /// The last `max_lines` lines (at least 1) of a run's log and its total
    /// line count, or `None` when the run has no log.
    pub(crate) fn tail(
        &self,
        workspace_id: &str,
        script_id: &str,
        run_id: &str,
        max_lines: usize,
    ) -> Result<Option<TailText>> {
        let key = Self::run_key(workspace_id, script_id, run_id)?;
        let Some(mut file) = self.open_read(&key)? else {
            return Ok(None);
        };
        let err = |e: io::Error| self.key_err(&key, &e);
        let text = tail_lines(&mut *file, max_lines.max(1)).map_err(err)?;
        file.seek(SeekFrom::Start(0)).map_err(err)?;
        let total_lines = count_newlines(&mut *file).map_err(err)? + 1;
        Ok(Some(TailText { text, total_lines }))
    }

    /// At most the trailing `max_bytes` bytes of a run's log, or `None` when
    /// the run has no log.
    pub(crate) fn tail_bytes(
        &self,
        workspace_id: &str,
        script_id: &str,
        run_id: &str,
        max_bytes: u64,
    ) -> Result<Option<Vec<u8>>> {
        let key = Self::run_key(workspace_id, script_id, run_id)?;
        let Some(mut file) = self.open_read(&key)? else {
            return Ok(None);
        };
        let err = |e: io::Error| self.key_err(&key, &e);
        let len = file.len().map_err(err)?;
        file.seek(SeekFrom::Start(len.saturating_sub(max_bytes)))
            .map_err(err)?;
        let mut bytes = Vec::new();
        file.take(max_bytes).read_to_end(&mut bytes).map_err(err)?;
        Ok(Some(bytes))
    }

    /// The newest `max_lines` lines (at least 1) of a run's log, cut to its
    /// newest `max_bytes` bytes (starting after a newline when the cut lands
    /// inside a line and the window holds one), with their stream positions;
    /// `None` when the run has no log.
    pub(crate) fn cursor_tail(
        &self,
        workspace_id: &str,
        script_id: &str,
        run_id: &str,
        max_lines: usize,
        max_bytes: u64,
    ) -> Result<Option<CursorTail>> {
        let key = Self::run_key(workspace_id, script_id, run_id)?;
        let err = |e: io::Error| self.key_err(&key, &e);
        let Some((mut file, base, from, len)) = self.positioned_read(&key).map_err(err)? else {
            return Ok(None);
        };
        let file = &mut *file;
        let cut = tail_cut(file, len, max_lines.max(1))
            .map_err(err)?
            .max(from);
        let bytes = if len - cut > max_bytes {
            newest_bytes_at_line_start(file, len, max_bytes).map_err(err)?
        } else {
            file.seek(SeekFrom::Start(cut)).map_err(err)?;
            let mut bytes = Vec::new();
            (&mut *file)
                .take(len - cut)
                .read_to_end(&mut bytes)
                .map_err(err)?;
            bytes
        };
        file.seek(SeekFrom::Start(from)).map_err(err)?;
        let total_lines = count_newlines(&mut (&mut *file).take(len - from)).map_err(err)? + 1;
        Ok(Some(CursorTail {
            start: base + len - bytes.len() as u64,
            end: base + len,
            log_start: base + from,
            total_lines,
            bytes,
        }))
    }

    /// Open a run's log with the stream position of its first byte and the
    /// length to read, taken atomically with the run's writes.
    fn positioned_read(&self, key: &BlobKey) -> io::Result<Option<PositionedRead>> {
        // Held throughout, so no writer opens (or reopens) the run meanwhile.
        let runs = lock(&self.runs);
        let Some(state) = runs.get(key) else {
            let Some(file) = self.storage.open_read(key)? else {
                return Ok(None);
            };
            let len = file.len()?;
            return Ok(Some((file, 0, 0, len)));
        };
        let s = lock(state);
        let Some(file) = self.storage.open_read(key)? else {
            return Ok(None);
        };
        let len = s.len.min(file.len()?);
        Ok(Some((
            file,
            s.base + s.len - len,
            s.contiguous.min(len),
            len,
        )))
    }

    /// The newest→oldest `{ items, nextToken }` page of a run's log lines —
    /// identical to [`intent_services::feature_api::pagination::paginate_text_lines`] over the whole
    /// log, without loading it. A missing log yields an empty page.
    pub(crate) fn paginate(
        &self,
        workspace_id: &str,
        script_id: &str,
        run_id: &str,
        limit: Option<i64>,
        token: Option<&str>,
    ) -> Result<Value> {
        let key = Self::run_key(workspace_id, script_id, run_id)?;
        let Some(file) = self.open_read(&key)? else {
            return Ok(
                intent_services::feature_api::pagination::paginate_text_lines("", limit, token),
            );
        };
        paginate_file(file, limit, token)
            .map(|(page, _)| page)
            .map_err(|e| self.key_err(&key, &e))
    }

    /// Delete one run's log. A missing log is not an error.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "wired by the script output cleanup change")
    )]
    pub(crate) fn delete_run(
        &self,
        workspace_id: &str,
        script_id: &str,
        run_id: &str,
    ) -> Result<()> {
        let key = Self::run_key(workspace_id, script_id, run_id)?;
        self.forget(&key);
        self.storage
            .delete(&key)
            .map_err(|e| self.key_err(&key, &e))
    }

    /// Delete every log of one script. A missing folder is not an error.
    pub(crate) fn delete_script(&self, workspace_id: &str, script_id: &str) -> Result<()> {
        let key = Self::script_key(workspace_id, script_id)?;
        self.forget(&key);
        self.storage
            .delete_prefix(&key)
            .map_err(|e| self.key_err(&key, &e))
    }

    /// Delete every log of one workspace. A missing folder is not an error.
    pub(crate) fn delete_workspace(&self, workspace_id: &str) -> Result<()> {
        let key = Self::workspace_key(workspace_id)?;
        self.forget(&key);
        self.storage
            .delete_prefix(&key)
            .map_err(|e| self.key_err(&key, &e))
    }

    /// Decoded IDs of every workspace folder under the root (symlinks and
    /// unrecognized entries are skipped). Empty when the root is missing.
    pub(crate) fn workspace_ids(&self) -> Vec<String> {
        self.decoded_children(&BlobKey::root(), true, &mut SweepReport::default())
            .into_iter()
            .map(|(_, id)| id)
            .collect()
    }

    /// Delete every log last modified more than `max_age` ago, except those
    /// for which `is_live` returns true, then prune emptied folders.
    /// `max_age == 0` disables log expiry (`scripts.outputRetentionHours = 0`).
    /// Compaction temp files older than [`STALE_COMPACT_AGE`] are deleted
    /// either way. Unrecognized entries are left alone; failures are logged
    /// and counted.
    pub(crate) fn sweep(
        &self,
        max_age: Duration,
        is_live: impl Fn(&RunKey) -> bool,
    ) -> SweepReport {
        let mut report = SweepReport::default();
        let now = SystemTime::now();
        let log_cutoff = if max_age.is_zero() {
            None
        } else {
            now.checked_sub(max_age)
        };
        let temp_cutoff = now.checked_sub(STALE_COMPACT_AGE);
        for (ws_key, workspace_id) in self.decoded_children(&BlobKey::root(), true, &mut report) {
            for (script_key, script_id) in self.decoded_children(&ws_key, true, &mut report) {
                if let Some(cutoff) = temp_cutoff {
                    let temps = self.storage.remove_stale_temps(&script_key, cutoff);
                    report.stale_temps_deleted += temps.deleted;
                    report.failed += temps.failed;
                }
                let Some(cutoff) = log_cutoff else { continue };
                for (log_key, run_id) in self.decoded_children(&script_key, false, &mut report) {
                    let key = RunKey {
                        workspace: workspace_id.clone(),
                        script: script_id.clone(),
                        run: run_id,
                    };
                    let modified = match self.storage.modified(&log_key) {
                        Ok(t) => t,
                        Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                        Err(e) => {
                            tracing::warn!(path = %self.storage.display_path(&log_key).display(), error = %e, "script output sweep: stat failed");
                            report.failed += 1;
                            continue;
                        }
                    };
                    if modified >= cutoff {
                        continue;
                    }
                    if is_live(&key) {
                        report.skipped_live += 1;
                        continue;
                    }
                    match self.storage.delete(&log_key) {
                        Ok(()) => {
                            self.forget(&log_key);
                            report.deleted += 1;
                        }
                        Err(e) => {
                            tracing::warn!(path = %self.storage.display_path(&log_key).display(), error = %e, "script output sweep: delete failed");
                            report.failed += 1;
                        }
                    }
                }
                self.storage.prune(&script_key);
            }
            if log_cutoff.is_some() {
                self.storage.prune(&ws_key);
            }
        }
        report
    }

    /// Drop the positions of runs without writers at or under `prefix`: their
    /// logs are going away.
    fn forget(&self, prefix: &BlobKey) {
        lock(&self.runs).retain(|key, state| {
            !key.segments().starts_with(prefix.segments()) || lock(state).writers > 0
        });
    }

    fn open_read(&self, key: &BlobKey) -> Result<Option<Box<dyn BlobFile>>> {
        self.storage
            .open_read(key)
            .map_err(|e| self.key_err(key, &e))
    }

    /// Children of `prefix` (folders when `dirs`, else `*.log` blobs) with
    /// decoded IDs. Unlistable folders are logged and counted; foreign names
    /// are skipped.
    fn decoded_children(
        &self,
        prefix: &BlobKey,
        dirs: bool,
        report: &mut SweepReport,
    ) -> Vec<(BlobKey, String)> {
        let entries = match self.storage.list(prefix) {
            Ok(entries) => entries,
            Err(e) => {
                tracing::warn!(path = %self.storage.display_path(prefix).display(), error = %e, "script output sweep: read_dir failed");
                report.failed += 1;
                return Vec::new();
            }
        };
        let mut out = Vec::new();
        for entry in entries {
            let segment = if dirs {
                if entry.kind != BlobKind::Folder {
                    continue;
                }
                entry.name.as_str()
            } else {
                if entry.kind != BlobKind::Blob {
                    continue;
                }
                let Some(stem) = entry.name.strip_suffix(LOG_EXT) else {
                    continue;
                };
                stem
            };
            let Some(id) = decode_segment(segment) else {
                continue;
            };
            if let Ok(key) = prefix.child(entry.name.clone()) {
                out.push((key, id));
            }
        }
        out
    }

    fn key_err(&self, key: &BlobKey, e: &io::Error) -> Error {
        io_err(&self.storage.display_path(key), e)
    }
}

/// `parent` with the encoded segment `segment` appended.
fn segment_key(parent: &BlobKey, segment: String) -> Result<BlobKey> {
    parent
        .child(segment)
        .map_err(|e| Error::InvalidParams(format!("script output id: {e}")))
}

/// Append writer for one run's log. Every [`append`](Self::append) is written
/// and flushed to the OS before it returns. Disk errors never propagate: the
/// first one logs a warning and disables the run's log.
#[derive(Debug)]
pub(crate) struct RunOutputWriter {
    storage: Arc<dyn BlobStorage>,
    /// `None` for a writer disabled by an invalid ID.
    key: Option<BlobKey>,
    path: PathBuf,
    state: Arc<Mutex<RunState>>,
    runs: Runs,
}

/// Backoff state after a failed compaction (see [`write_capped`]).
#[derive(Debug, Default)]
struct CompactionRetry {
    /// Output bytes still to pass before the next compaction attempt (0 when
    /// no retry is pending).
    credit: u64,
    /// Whether this run already warned about a failed compaction.
    warned: bool,
}

impl RunOutputWriter {
    /// The log path (empty for a writer disabled by an invalid ID).
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "wired by the script output cleanup change")
    )]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Whether appends still reach disk.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "wired by the script output cleanup change")
    )]
    pub(crate) fn is_enabled(&self) -> bool {
        lock(&self.state).file.is_some()
    }

    /// The log's current size in bytes.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "wired by the script output cleanup change")
    )]
    pub(crate) fn len(&self) -> u64 {
        lock(&self.state).len
    }

    /// The run's stream position after its last byte: where the next
    /// appended chunk starts.
    pub(crate) fn end(&self) -> u64 {
        let s = lock(&self.state);
        s.base + s.len
    }

    /// Append one output chunk, compacting first so the log never exceeds the
    /// cap (oldest bytes dropped). Never fails; see the type docs.
    pub(crate) fn append(&mut self, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }
        let mut guard = lock(&self.state);
        let s = &mut *guard;
        let end = s.base + s.len + chunk.len() as u64;
        let written = match (s.file.as_mut(), self.key.as_ref()) {
            (Some(file), Some(key)) => {
                let log = Log {
                    storage: &*self.storage,
                    key,
                    path: &self.path,
                };
                write_capped(log, file, &mut s.len, s.cap, &mut s.retry, chunk).unwrap_or_else(
                    |e| {
                        tracing::warn!(path = %self.path.display(), error = %e, "script output log write failed; disabling log for this run");
                        s.file = None;
                        Written::Dropped
                    },
                )
            }
            _ => Written::Dropped,
        };
        match written {
            Written::Appended => {}
            Written::Compacted { dropped } => s.contiguous = s.contiguous.saturating_sub(dropped),
            Written::Dropped => s.contiguous = s.len,
        }
        s.base = end - s.len;
    }
}

impl Drop for RunOutputWriter {
    fn drop(&mut self) {
        let Some(key) = &self.key else { return };
        let mut runs = lock(&self.runs);
        let mut s = lock(&self.state);
        s.writers = s.writers.saturating_sub(1);
        if s.writers > 0 {
            return;
        }
        s.file = None;
        if s.base == 0
            && s.contiguous == 0
            && runs.get(key).is_some_and(|r| Arc::ptr_eq(r, &self.state))
        {
            drop(s);
            runs.remove(key);
        }
    }
}

/// Where one run's log is kept, for [`write_capped`].
#[derive(Clone, Copy)]
struct Log<'a> {
    storage: &'a dyn BlobStorage,
    key: &'a BlobKey,
    path: &'a Path,
}

/// Append `chunk` to `log` (open as `file`, read + append, size `*len`),
/// keeping it within `cap` bytes. Compaction replaces `*file` with the handle
/// of the replaced log ([`BlobStorage::replace`]); the open log is never
/// truncated or rewritten in place, because concurrent readers hold it.
/// If the replacement fails, the old log is kept and chunks are still appended
/// while the log stays within twice the cap; a chunk that would pass that is
/// dropped from disk (the caller's live stream still carries it). Compaction
/// is retried once another quarter cap of output has gone by, and the first
/// failure per run logs one warning.
fn write_capped(
    log: Log<'_>,
    file: &mut Box<dyn BlobFile>,
    len: &mut u64,
    cap: u64,
    retry: &mut CompactionRetry,
    chunk: &[u8],
) -> io::Result<Written> {
    let chunk_len = chunk.len() as u64;
    let new_len = len.saturating_add(chunk_len);
    if new_len <= cap {
        return append_chunk(&mut **file, len, chunk);
    }
    let fits_fallback = new_len <= cap.saturating_mul(2);
    if retry.credit > 0 {
        retry.credit = retry.credit.saturating_sub(chunk_len);
        return if fits_fallback {
            append_chunk(&mut **file, len, chunk)
        } else {
            Ok(Written::Dropped)
        };
    }
    // `cap` fits in usize whenever the chunk does not (chunk_len > cap below).
    let mut kept: Vec<u8> = if chunk_len >= cap {
        Vec::new()
    } else {
        let budget = (cap / 2).min(cap - chunk_len).min(*len);
        newest_bytes_at_line_start(&mut **file, *len, budget)?
    };
    let tail = if chunk_len > cap {
        &chunk[chunk.len() - usize::try_from(cap).expect("cap < chunk length fits in usize")..]
    } else {
        chunk
    };
    let dropped = *len - kept.len() as u64;
    kept.extend_from_slice(tail);
    match log.storage.replace(log.key, &kept) {
        Ok(new_file) => {
            *file = new_file;
            *len = kept.len() as u64;
            retry.credit = 0;
            Ok(Written::Compacted { dropped })
        }
        Err(e) => {
            if !retry.warned {
                retry.warned = true;
                tracing::warn!(path = %log.path.display(), error = %e, "script output log compaction failed; keeping the old log (output past twice the cap is not saved) and retrying");
            }
            retry.credit = (cap / 4).max(1);
            if fits_fallback {
                append_chunk(&mut **file, len, chunk)
            } else {
                Ok(Written::Dropped)
            }
        }
    }
}

fn append_chunk(file: &mut dyn BlobFile, len: &mut u64, chunk: &[u8]) -> io::Result<Written> {
    file.write_all(chunk)?;
    file.flush()?;
    *len += chunk.len() as u64;
    Ok(Written::Appended)
}

/// What [`write_capped`] did with a chunk.
enum Written {
    Appended,
    /// The log was rewritten to its newest bytes plus the chunk; `dropped`
    /// bytes of the old log are gone (a chunk past the cap also loses its
    /// oldest bytes, and then `dropped` is the whole old log).
    Compacted {
        dropped: u64,
    },
    /// The chunk was not saved.
    Dropped,
}

/// The newest `budget` bytes of a `len`-byte file, starting after the first
/// newline when the window opens mid-line (and a newline exists to realign on).
fn newest_bytes_at_line_start(
    file: &mut dyn BlobFile,
    len: u64,
    budget: u64,
) -> io::Result<Vec<u8>> {
    if budget == 0 {
        return Ok(Vec::new());
    }
    let start = len - budget;
    // Read one extra preceding byte to learn whether `start` is a line start.
    let read_from = start.saturating_sub(1);
    file.seek(SeekFrom::Start(read_from))?;
    let mut buf = Vec::with_capacity(usize::try_from(len - read_from).unwrap_or(0));
    file.take(len - read_from).read_to_end(&mut buf)?;
    if start == 0 {
        return Ok(buf);
    }
    let window = &buf[1..];
    if buf[0] == b'\n' {
        return Ok(window.to_vec());
    }
    Ok(match window.iter().position(|&b| b == b'\n') {
        Some(i) if i + 1 < window.len() => window[i + 1..].to_vec(),
        _ => window.to_vec(),
    })
}

/// Open a run's log for append (creating folders, never through a symlink)
/// and enforce `cap` on what is already there. Returns the file, its size
/// when opened, and its size after any compaction.
fn open_append(
    storage: &dyn BlobStorage,
    key: &BlobKey,
    path: &Path,
    cap: u64,
) -> io::Result<(Box<dyn BlobFile>, u64, u64)> {
    let mut file = storage.open_append(key)?;
    let on_disk = file.len()?;
    let mut len = on_disk;
    if len > cap {
        // Reopened under a smaller cap: keep the newest bytes now.
        // If the replace fails the oversized log is kept; the writer retries
        // compaction on its next append.
        let kept = newest_bytes_at_line_start(&mut *file, len, cap)?;
        match storage.replace(key, &kept) {
            Ok(new_file) => {
                file = new_file;
                len = kept.len() as u64;
            }
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "script output log compaction failed on reopen; keeping the old log");
            }
        }
    }
    Ok((file, on_disk, len))
}

/// The trailing `n` `'\n'`-delimited lines, read backwards in chunks.
fn tail_lines(file: &mut dyn BlobFile, n: usize) -> io::Result<String> {
    let len = file.len()?;
    let cut = tail_cut(file, len, n)?;
    file.seek(SeekFrom::Start(cut))?;
    let mut out = Vec::new();
    file.take(len - cut).read_to_end(&mut out)?;
    Ok(String::from_utf8_lossy(&out).into_owned())
}

/// Where the trailing `n` `'\n'`-delimited lines of the first `len` bytes
/// start, scanning backwards in chunks.
fn tail_cut(file: &mut dyn BlobFile, len: u64, n: usize) -> io::Result<u64> {
    let mut pos = len;
    let mut newlines = 0usize;
    let mut buf = vec![0u8; SCAN_CHUNK];
    while pos > 0 {
        let step = usize::try_from(pos.min(SCAN_CHUNK as u64)).expect("chunk fits in usize");
        pos -= step as u64;
        file.seek(SeekFrom::Start(pos))?;
        file.read_exact(&mut buf[..step])?;
        for i in (0..step).rev() {
            if buf[i] == b'\n' {
                newlines += 1;
                if newlines == n {
                    return Ok(pos + i as u64 + 1);
                }
            }
        }
    }
    Ok(0)
}

fn count_newlines(file: &mut (impl Read + ?Sized)) -> io::Result<usize> {
    let mut buf = vec![0u8; SCAN_CHUNK];
    let mut count = 0;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            return Ok(count);
        }
        count += buf[..n].iter().fold(0, |c, &b| c + usize::from(b == b'\n'));
    }
}

/// Stream `file` twice in fixed-size chunks: once to count lines up to the
/// last non-blank one (mirroring `paginate_text_lines`' trailing-blank trim),
/// once to copy out only the page window's lines. Also returns the largest
/// line buffer held, which is only ever a returned line.
fn paginate_file(
    mut file: Box<dyn BlobFile>,
    limit: Option<i64>,
    token: Option<&str>,
) -> io::Result<(Value, usize)> {
    let mut kept_len = 0usize;
    let mut probe = BlankProbe::default();
    scan_lines(&mut *file, |index, event| {
        match event {
            LineEvent::Bytes(bytes) => probe.feed(bytes),
            LineEvent::End => {
                if !probe.finish() {
                    kept_len = index + 1;
                }
            }
        }
        true
    })?;
    let (win, cut) =
        intent_services::feature_api::pagination::text_page_window(kept_len, limit, token);
    let mut items: Vec<String> = Vec::with_capacity(win.end - win.start);
    let mut peak = 0usize;
    if win.end > win.start {
        file.seek(SeekFrom::Start(0))?;
        let mut line = Vec::new();
        scan_lines(&mut *file, |index, event| {
            if index < win.start {
                return true;
            }
            match event {
                LineEvent::Bytes(bytes) => {
                    line.extend_from_slice(bytes);
                    true
                }
                LineEvent::End => {
                    peak = peak.max(line.len());
                    items.push(String::from_utf8_lossy(&line).into_owned());
                    line.clear();
                    index + 1 < win.end
                }
            }
        })?;
    }
    if let (Some(cut), Some(last)) = (cut, items.last_mut()) {
        let keep = intent_services::feature_api::pagination::floor_char_boundary(last, cut);
        last.truncate(keep);
    }
    let page = intent_services::feature_api::pagination::bound_text_page(
        &items,
        win.start,
        win.next_token,
    );
    Ok((page, peak))
}

/// One step of [`scan_lines`]: a piece of the current line, or its end.
enum LineEvent<'a> {
    Bytes(&'a [u8]),
    End,
}

/// Walk every `split('\n')` line (index from 0) in `SCAN_CHUNK` reads,
/// reporting each line as zero or more byte pieces followed by `End`, until
/// `f` returns false. The final (possibly empty) segment is always ended.
/// Never buffers a line.
fn scan_lines(
    file: &mut dyn BlobFile,
    mut f: impl FnMut(usize, LineEvent<'_>) -> bool,
) -> io::Result<()> {
    let mut buf = vec![0u8; SCAN_CHUNK];
    let mut index = 0usize;
    loop {
        let n = match file.read(&mut buf) {
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        if n == 0 {
            f(index, LineEvent::End);
            return Ok(());
        }
        let mut rest = &buf[..n];
        while let Some(i) = rest.iter().position(|&b| b == b'\n') {
            if i > 0 && !f(index, LineEvent::Bytes(&rest[..i])) {
                return Ok(());
            }
            if !f(index, LineEvent::End) {
                return Ok(());
            }
            index += 1;
            rest = &rest[i + 1..];
        }
        if !rest.is_empty() && !f(index, LineEvent::Bytes(rest)) {
            return Ok(());
        }
    }
}

/// Streaming equivalent of `String::from_utf8_lossy(line).trim().is_empty()`:
/// a line is blank iff it is valid UTF-8 made only of whitespace (any invalid
/// byte decodes to U+FFFD, which is not whitespace). Holds at most one
/// partial UTF-8 sequence (4 bytes).
#[derive(Default)]
struct BlankProbe {
    non_blank: bool,
    pending: [u8; 4],
    pending_len: usize,
    want: usize,
}

impl BlankProbe {
    fn feed(&mut self, bytes: &[u8]) {
        for &b in bytes {
            if self.non_blank {
                return;
            }
            if self.want == 0 {
                match b {
                    b'\t' | b'\x0b' | b'\x0c' | b'\r' | b' ' => {}
                    0xc2..=0xdf => self.start(b, 2),
                    0xe0..=0xef => self.start(b, 3),
                    0xf0..=0xf4 => self.start(b, 4),
                    // Other ASCII, stray continuation bytes, invalid leads.
                    _ => self.non_blank = true,
                }
                continue;
            }
            self.pending[self.pending_len] = b;
            self.pending_len += 1;
            if self.pending_len == self.want {
                let whitespace = std::str::from_utf8(&self.pending[..self.want])
                    .ok()
                    .and_then(|s| s.chars().next())
                    .is_some_and(char::is_whitespace);
                self.non_blank = !whitespace;
                self.want = 0;
                self.pending_len = 0;
            }
        }
    }

    fn start(&mut self, lead: u8, want: usize) {
        self.pending[0] = lead;
        self.pending_len = 1;
        self.want = want;
    }

    /// Whether the line just ended was blank; resets for the next line.
    fn finish(&mut self) -> bool {
        let blank = !self.non_blank && self.want == 0;
        *self = Self::default();
        blank
    }
}

/// Encode an ID as one safe path segment: `[A-Za-z0-9_-]` pass through, every
/// other byte (including `.`, `/`, `\`) becomes `%XX`. Injective, so distinct
/// IDs never share a path, and the result can never be `.`, `..` or contain a
/// separator. Empty IDs, NUL bytes and over-long results are rejected.
pub(crate) fn encode_segment(id: &str) -> Result<String> {
    if id.is_empty() {
        return Err(Error::InvalidParams(
            "script output id must not be empty".into(),
        ));
    }
    if id.contains('\0') {
        return Err(Error::InvalidParams(
            "script output id must not contain NUL".into(),
        ));
    }
    let mut out = String::with_capacity(id.len());
    for &b in id.as_bytes() {
        if b.is_ascii_alphanumeric() || b == b'_' || b == b'-' {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    if out.len() > MAX_SEGMENT_BYTES {
        return Err(Error::InvalidParams(format!(
            "script output id is too long ({} bytes)",
            id.len()
        )));
    }
    Ok(out)
}

/// Inverse of [`encode_segment`]; `None` for names it could not have produced.
pub(crate) fn decode_segment(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' {
            let hex = segment.get(i + 1..i + 3)?;
            let v = u8::from_str_radix(hex, 16).ok()?;
            if v.is_ascii_alphanumeric() || v == b'_' || v == b'-' || hex != format!("{v:02X}") {
                return None;
            }
            out.push(v);
            i += 3;
        } else if b.is_ascii_alphanumeric() || b == b'_' || b == b'-' {
            out.push(b);
            i += 1;
        } else {
            return None;
        }
    }
    let id = String::from_utf8(out).ok()?;
    (!id.is_empty()).then_some(id)
}

fn io_err(path: &Path, e: &io::Error) -> Error {
    Error::Internal(format!("script output {}: {e}", path.display()))
}

#[cfg(test)]
mod tests;
