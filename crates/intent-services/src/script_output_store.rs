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
//!   before the chunk that would overflow it is appended.
//! - Reads stream the file and hold at most the requested lines (plus one line
//!   at a time), never the whole log.
//! - IDs are percent-encoded into a single safe path segment, so no ID can
//!   name `.`/`..` or contain a separator and escape the root. Symlinks below
//!   the root are never followed: a symlinked folder or log is an error (the
//!   root itself may be a symlink, e.g. a relocated data dir).

use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use intent_core::{Error, Result};
use serde_json::{json, Value};

/// Directory under the daemon data dir that holds every script output log.
pub(crate) const SCRIPT_OUTPUT_DIR: &str = "script-output";

const LOG_EXT: &str = ".log";
/// Longest encoded path segment accepted (common filesystem `NAME_MAX`).
const MAX_SEGMENT_BYTES: usize = 255;
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

/// Outcome of [`ScriptOutputStore::sweep`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SweepReport {
    /// Expired logs deleted.
    pub deleted: usize,
    /// Expired logs skipped because their run is live.
    pub skipped_live: usize,
    /// Logs that could not be inspected or deleted (each logged as a warning).
    pub failed: usize,
}

/// The on-disk script output store rooted at `<data_dir>/script-output`.
#[derive(Debug, Clone)]
pub(crate) struct ScriptOutputStore {
    root: PathBuf,
    max_bytes_per_run: u64,
}

impl ScriptOutputStore {
    /// A store rooted at `<data_dir>/script-output` whose run logs never grow
    /// past `max_bytes_per_run` bytes (clamped to at least 1). Nothing is
    /// created on disk until a writer opens.
    pub(crate) fn new(data_dir: &Path, max_bytes_per_run: u64) -> Self {
        Self {
            root: data_dir.join(SCRIPT_OUTPUT_DIR),
            max_bytes_per_run: max_bytes_per_run.max(1),
        }
    }

    /// The store root (`<data_dir>/script-output`).
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "wired by the script output cleanup change")
    )]
    pub(crate) fn root(&self) -> &Path {
        &self.root
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

    /// `<root>/<workspaceId>`.
    pub(crate) fn workspace_dir(&self, workspace_id: &str) -> Result<PathBuf> {
        Ok(self.root.join(encode_segment(workspace_id)?))
    }

    /// `<root>/<workspaceId>/<scriptId>`.
    pub(crate) fn script_dir(&self, workspace_id: &str, script_id: &str) -> Result<PathBuf> {
        Ok(self
            .workspace_dir(workspace_id)?
            .join(encode_segment(script_id)?))
    }

    /// `<root>/<workspaceId>/<scriptId>/<runId>.log`.
    pub(crate) fn run_path(
        &self,
        workspace_id: &str,
        script_id: &str,
        run_id: &str,
    ) -> Result<PathBuf> {
        let run = encode_segment(run_id)?;
        if run.len() + LOG_EXT.len() > MAX_SEGMENT_BYTES {
            return Err(Error::InvalidParams(format!(
                "script output run id is too long ({} bytes)",
                run_id.len()
            )));
        }
        Ok(self
            .script_dir(workspace_id, script_id)?
            .join(format!("{run}{LOG_EXT}")))
    }

    /// Open (creating parents) the append writer for one run. Never fails: an
    /// invalid ID or disk error logs a warning and yields a disabled writer
    /// whose appends are no-ops. Reopening an existing run's log appends to it.
    pub(crate) fn open_writer(
        &self,
        workspace_id: &str,
        script_id: &str,
        run_id: &str,
    ) -> RunOutputWriter {
        let path = match self.run_path(workspace_id, script_id, run_id) {
            Ok(path) => path,
            Err(e) => {
                tracing::warn!(workspace = %workspace_id, script = %script_id, run = %run_id, error = %e, "script output log disabled: invalid id");
                return RunOutputWriter::disabled(PathBuf::new(), self.max_bytes_per_run);
            }
        };
        match open_append(&self.root, &path, self.max_bytes_per_run) {
            Ok((file, len)) => RunOutputWriter {
                path,
                file: Some(file),
                len,
                cap: self.max_bytes_per_run,
            },
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "script output log disabled: open failed");
                RunOutputWriter::disabled(path, self.max_bytes_per_run)
            }
        }
    }

    /// Whether the run's log exists.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "wired by the script output cleanup change")
    )]
    pub(crate) fn exists(&self, workspace_id: &str, script_id: &str, run_id: &str) -> Result<bool> {
        let path = self.run_path(workspace_id, script_id, run_id)?;
        check_below_root(&self.root, &path, Walk::Read).map_err(|e| io_err(&path, &e))?;
        Ok(fs::symlink_metadata(&path).is_ok_and(|m| m.is_file()))
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
        let path = self.run_path(workspace_id, script_id, run_id)?;
        let Some(mut file) = open_read(&self.root, &path)? else {
            return Ok(None);
        };
        let text = tail_lines(&mut file, max_lines.max(1)).map_err(|e| io_err(&path, &e))?;
        file.seek(SeekFrom::Start(0))
            .map_err(|e| io_err(&path, &e))?;
        let total_lines = count_newlines(&mut file).map_err(|e| io_err(&path, &e))? + 1;
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
        let path = self.run_path(workspace_id, script_id, run_id)?;
        let Some(mut file) = open_read(&self.root, &path)? else {
            return Ok(None);
        };
        let len = file.metadata().map_err(|e| io_err(&path, &e))?.len();
        file.seek(SeekFrom::Start(len.saturating_sub(max_bytes)))
            .map_err(|e| io_err(&path, &e))?;
        let mut bytes = Vec::new();
        file.take(max_bytes)
            .read_to_end(&mut bytes)
            .map_err(|e| io_err(&path, &e))?;
        Ok(Some(bytes))
    }

    /// The newest→oldest `{ items, nextToken }` page of a run's log lines —
    /// identical to [`crate::pagination::paginate_text_lines`] over the whole
    /// log, without loading it. A missing log yields an empty page.
    pub(crate) fn paginate(
        &self,
        workspace_id: &str,
        script_id: &str,
        run_id: &str,
        limit: Option<i64>,
        token: Option<&str>,
    ) -> Result<Value> {
        let path = self.run_path(workspace_id, script_id, run_id)?;
        let Some(file) = open_read(&self.root, &path)? else {
            return Ok(crate::pagination::paginate_text_lines("", limit, token));
        };
        paginate_file(file, limit, token)
            .map(|(page, _)| page)
            .map_err(|e| io_err(&path, &e))
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
        let path = self.run_path(workspace_id, script_id, run_id)?;
        check_below_root(&self.root, &path, Walk::Delete)
            .and_then(|()| ignore_missing(fs::remove_file(&path)))
            .map_err(|e| io_err(&path, &e))
    }

    /// Delete every log of one script. A missing folder is not an error.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "wired by the script output cleanup change")
    )]
    pub(crate) fn delete_script(&self, workspace_id: &str, script_id: &str) -> Result<()> {
        let dir = self.script_dir(workspace_id, script_id)?;
        remove_tree(&self.root, &dir)
    }

    /// Delete every log of one workspace. A missing folder is not an error.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "wired by the script output cleanup change")
    )]
    pub(crate) fn delete_workspace(&self, workspace_id: &str) -> Result<()> {
        let dir = self.workspace_dir(workspace_id)?;
        remove_tree(&self.root, &dir)
    }

    /// Delete every log last modified more than `max_age` ago, except those
    /// for which `is_live` returns true, then prune emptied folders.
    /// `max_age == 0` disables the sweep (`scripts.outputRetentionHours = 0`).
    /// Unrecognized entries are left alone; failures are logged and counted.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "wired by the script output cleanup change")
    )]
    pub(crate) fn sweep(
        &self,
        max_age: Duration,
        is_live: impl Fn(&RunKey) -> bool,
    ) -> SweepReport {
        let mut report = SweepReport::default();
        if max_age.is_zero() {
            return report;
        }
        let Some(cutoff) = SystemTime::now().checked_sub(max_age) else {
            return report;
        };
        for (ws_dir, workspace_id) in decoded_children(&self.root, true, &mut report) {
            for (script_dir, script_id) in decoded_children(&ws_dir, true, &mut report) {
                for (path, run_id) in decoded_children(&script_dir, false, &mut report) {
                    let key = RunKey {
                        workspace: workspace_id.clone(),
                        script: script_id.clone(),
                        run: run_id,
                    };
                    let modified = match fs::metadata(&path).and_then(|m| m.modified()) {
                        Ok(t) => t,
                        Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                        Err(e) => {
                            tracing::warn!(path = %path.display(), error = %e, "script output sweep: stat failed");
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
                    match ignore_missing(fs::remove_file(&path)) {
                        Ok(()) => report.deleted += 1,
                        Err(e) => {
                            tracing::warn!(path = %path.display(), error = %e, "script output sweep: delete failed");
                            report.failed += 1;
                        }
                    }
                }
                let _ = fs::remove_dir(&script_dir);
            }
            let _ = fs::remove_dir(&ws_dir);
        }
        report
    }
}

/// Append writer for one run's log. Every [`append`](Self::append) is written
/// and flushed to the OS before it returns. Disk errors never propagate: the
/// first one logs a warning and disables the writer.
#[derive(Debug)]
pub(crate) struct RunOutputWriter {
    path: PathBuf,
    file: Option<File>,
    len: u64,
    cap: u64,
}

impl RunOutputWriter {
    fn disabled(path: PathBuf, cap: u64) -> Self {
        Self {
            path,
            file: None,
            len: 0,
            cap,
        }
    }

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
        self.file.is_some()
    }

    /// The log's current size in bytes, as tracked by this writer.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "wired by the script output cleanup change")
    )]
    pub(crate) fn len(&self) -> u64 {
        self.len
    }

    /// Append one output chunk, compacting first so the log never exceeds the
    /// cap (oldest bytes dropped). Never fails; see the type docs.
    pub(crate) fn append(&mut self, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }
        let Some(file) = self.file.as_mut() else {
            return;
        };
        let result = write_capped(file, &mut self.len, self.cap, chunk);
        if let Err(e) = result {
            tracing::warn!(path = %self.path.display(), error = %e, "script output log write failed; disabling log for this run");
            self.file = None;
        }
    }
}

/// Append `chunk` to `file` (opened read + append, size `*len`) without ever
/// exceeding `cap` bytes.
fn write_capped(file: &mut File, len: &mut u64, cap: u64, chunk: &[u8]) -> io::Result<()> {
    let chunk_len = chunk.len() as u64;
    if *len + chunk_len <= cap {
        file.write_all(chunk)?;
        file.flush()?;
        *len += chunk_len;
        return Ok(());
    }
    // `cap` fits in usize whenever the chunk does not (chunk_len > cap below).
    let mut kept: Vec<u8> = if chunk_len >= cap {
        Vec::new()
    } else {
        let budget = (cap / 2).min(cap - chunk_len).min(*len);
        newest_bytes_at_line_start(file, *len, budget)?
    };
    let chunk = if chunk_len > cap {
        &chunk[chunk.len() - usize::try_from(cap).expect("cap < chunk length fits in usize")..]
    } else {
        chunk
    };
    kept.extend_from_slice(chunk);
    file.set_len(0)?;
    // Append mode writes at the (now zero) end regardless of the cursor.
    file.write_all(&kept)?;
    file.flush()?;
    *len = kept.len() as u64;
    Ok(())
}

/// The newest `budget` bytes of a `len`-byte file, starting after the first
/// newline when the window opens mid-line (and a newline exists to realign on).
fn newest_bytes_at_line_start(file: &mut File, len: u64, budget: u64) -> io::Result<Vec<u8>> {
    if budget == 0 {
        return Ok(Vec::new());
    }
    let start = len - budget;
    // Read one extra preceding byte to learn whether `start` is a line start.
    let read_from = start.saturating_sub(1);
    file.seek(SeekFrom::Start(read_from))?;
    let mut buf = Vec::with_capacity(usize::try_from(len - read_from).unwrap_or(0));
    Read::by_ref(file)
        .take(len - read_from)
        .read_to_end(&mut buf)?;
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

/// How [`check_below_root`] treats the components between the root and a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Walk {
    /// Create missing folders; the final component must not be a symlink.
    Create,
    /// Stop at the first missing folder; the final component must not be a symlink.
    Read,
    /// Stop at the first missing folder; the final component may be a symlink
    /// (removing a link never touches its target).
    Delete,
}

/// Refuse to traverse a symlink (or non-folder) at any component between
/// `root` and `path`. The root itself may be a symlink.
fn check_below_root(root: &Path, path: &Path, walk: Walk) -> io::Result<()> {
    let rel = path
        .strip_prefix(root)
        .map_err(|_| io::Error::other("path is outside the script output root"))?;
    if walk == Walk::Create {
        fs::create_dir_all(root)?;
    }
    let parts: Vec<_> = rel.components().collect();
    let mut cur = root.to_path_buf();
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

/// Open options that refuse a symlinked final component (`O_NOFOLLOW`).
fn no_follow(mut opts: OpenOptions) -> OpenOptions {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        opts.custom_flags(libc::O_NOFOLLOW);
    }
    opts
}

/// Open a run's log for append (creating folders, never through a symlink)
/// and enforce `cap` on what is already there. Returns the file and its size.
fn open_append(root: &Path, path: &Path, cap: u64) -> io::Result<(File, u64)> {
    let open = || {
        check_below_root(root, path, Walk::Create)?;
        let mut opts = OpenOptions::new();
        opts.create(true).read(true).append(true);
        no_follow(opts).open(path)
    };
    // A concurrent sweep may prune the freshly created (still empty) folder.
    let mut file = match open() {
        Err(e) if e.kind() == io::ErrorKind::NotFound => open()?,
        other => other?,
    };
    let mut len = file.metadata()?.len();
    if len > cap {
        // Reopened under a smaller cap: keep the newest bytes now.
        let kept = newest_bytes_at_line_start(&mut file, len, cap)?;
        file.set_len(0)?;
        file.write_all(&kept)?;
        file.flush()?;
        len = kept.len() as u64;
    }
    Ok((file, len))
}

fn open_read(root: &Path, path: &Path) -> Result<Option<File>> {
    check_below_root(root, path, Walk::Read).map_err(|e| io_err(path, &e))?;
    let mut opts = OpenOptions::new();
    opts.read(true);
    match no_follow(opts).open(path) {
        Ok(f) => Ok(Some(f)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(io_err(path, &e)),
    }
}

/// Remove a folder tree below the root without traversing a symlinked parent.
/// `remove_dir_all` itself never follows symlinks inside the tree, and a
/// symlinked `dir` is removed as a link.
fn remove_tree(root: &Path, dir: &Path) -> Result<()> {
    check_below_root(root, dir, Walk::Delete)
        .and_then(|()| match fs::symlink_metadata(dir) {
            Ok(m) if m.file_type().is_symlink() => fs::remove_file(dir),
            Ok(_) => fs::remove_dir_all(dir),
            Err(e) => Err(e),
        })
        .or_else(|e| ignore_missing(Err(e)))
        .map_err(|e| io_err(dir, &e))
}

/// The trailing `n` `'\n'`-delimited lines, read backwards in chunks.
fn tail_lines(file: &mut File, n: usize) -> io::Result<String> {
    let len = file.metadata()?.len();
    let mut pos = len;
    let mut newlines = 0usize;
    let mut cut = 0u64;
    let mut buf = vec![0u8; SCAN_CHUNK];
    'scan: while pos > 0 {
        let step = usize::try_from(pos.min(SCAN_CHUNK as u64)).expect("chunk fits in usize");
        pos -= step as u64;
        file.seek(SeekFrom::Start(pos))?;
        file.read_exact(&mut buf[..step])?;
        for i in (0..step).rev() {
            if buf[i] == b'\n' {
                newlines += 1;
                if newlines == n {
                    cut = pos + i as u64 + 1;
                    break 'scan;
                }
            }
        }
    }
    file.seek(SeekFrom::Start(cut))?;
    let mut out = Vec::new();
    file.take(len - cut).read_to_end(&mut out)?;
    Ok(String::from_utf8_lossy(&out).into_owned())
}

fn count_newlines(file: &mut File) -> io::Result<usize> {
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
    mut file: File,
    limit: Option<i64>,
    token: Option<&str>,
) -> io::Result<(Value, usize)> {
    let mut kept_len = 0usize;
    let mut probe = BlankProbe::default();
    scan_lines(&mut file, |index, event| {
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
    let win = crate::pagination::page_window(kept_len, limit, token);
    let mut items: Vec<String> = Vec::with_capacity(win.end - win.start);
    let mut peak = 0usize;
    if win.end > win.start {
        file.seek(SeekFrom::Start(0))?;
        let mut line = Vec::new();
        scan_lines(&mut file, |index, event| {
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
    items.reverse();
    Ok((json!({ "items": items, "nextToken": win.next_token }), peak))
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
fn scan_lines(file: &mut File, mut f: impl FnMut(usize, LineEvent<'_>) -> bool) -> io::Result<()> {
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

/// Children of `dir` (folders when `dirs`, else `*.log` files) with decoded
/// IDs. Unreadable folders are logged and counted; foreign names are skipped.
fn decoded_children(dir: &Path, dirs: bool, report: &mut SweepReport) -> Vec<(PathBuf, String)> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => {
            tracing::warn!(path = %dir.display(), error = %e, "script output sweep: read_dir failed");
            report.failed += 1;
            return Vec::new();
        }
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let Ok(kind) = entry.file_type() else {
            continue;
        };
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let segment = if dirs {
            if !kind.is_dir() {
                continue;
            }
            name
        } else {
            if !kind.is_file() {
                continue;
            }
            let Some(stem) = name.strip_suffix(LOG_EXT) else {
                continue;
            };
            stem
        };
        if let Some(id) = decode_segment(segment) {
            out.push((entry.path(), id));
        }
    }
    out
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

fn ignore_missing(r: io::Result<()>) -> io::Result<()> {
    match r {
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

fn io_err(path: &Path, e: &io::Error) -> Error {
    Error::Internal(format!("script output {}: {e}", path.display()))
}

#[cfg(test)]
mod tests;
