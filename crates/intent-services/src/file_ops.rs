//! Wire-policy + filesystem glue for the `file.*` methods (PROTOCOL §5.10).
//!
//! Ports the TS `buildFileApi` / `LocalFileSystemAdapter` semantics: the
//! workspace root is `worktreePath || repositoryPath` (falling back to the
//! process CWD when unset, mirroring `path.resolve('', rel)`), every path is
//! validated within that root via a lexical prefix check (Node's `path.resolve`
//! then `startsWith`, no symlink resolution), and all access/IO failures
//! surface as [`Error::Internal`] (-32603), matching the TS handler which
//! wraps the builder errors in `INTERNAL_ERROR`.

use std::path::{Component, Path, PathBuf};

use intent_core::{Error, Result, Workspace, WorkspaceId};
use intent_store::Store;
use serde_json::{json, Value};

use crate::git_ops;

/// TS `LocalFileSystemAdapter` access-denied message.
const ACCESS_DENIED: &str = "Access denied: path outside workspace";

/// Workspace-relative directory attachments are placed into by
/// `file.placeAttachment` (intent-hq/monorepo#1948). Lives under `.intent/`
/// so the default `.intent/.gitignore` (`*` with only `.gitignore` and
/// `config.json` re-included) keeps placed files out of git tracking,
/// auto-commit, and attribution.
pub(crate) const ATTACHMENTS_DIR: &str = ".intent/attachments";

/// Resolve the workspace filesystem root the way the TS protocol adapter does:
/// `worktreePath || repositoryPath`, else empty (→ CWD-relative resolution).
pub(crate) fn workspace_root(ws: &Workspace) -> String {
    git_ops::worktree_path(ws)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Load the workspace and resolve its filesystem root, preferring the calling
/// agent's sandbox path when available (CoW containment). A missing workspace
/// (or any load error) falls through to an empty root, mirroring the TS handler
/// which swallows `getWorkspace` failures and proceeds with `workspacePath=''`.
pub(crate) async fn resolve_root(
    store: &Store,
    workspace_id: &WorkspaceId,
    caller_agent_id: Option<&intent_core::AgentId>,
) -> String {
    // CoW containment: prefer the calling agent's sandbox path when present.
    if let Some(agent_id) = caller_agent_id {
        if let Ok(session) = store.get_agent_session(agent_id).await {
            if let Some(sandbox_path) = session.sandbox_path {
                return sandbox_path;
            }
        }
    }
    match store.get_workspace(workspace_id).await {
        Ok(ws) => workspace_root(&ws),
        Err(_) => String::new(),
    }
}

/// Normalize `..`/`.` segments lexically (no filesystem access), like the
/// normalization Node's `path.resolve` performs.
fn normalize_lexical(p: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for comp in p.components() {
        match comp {
            Component::ParentDir => {
                out.pop();
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// Mirror `path.resolve(base, rel)`: absolute `rel` wins; otherwise `rel` is
/// joined onto `base` (or the process CWD when `base` is empty/relative), then
/// normalized lexically.
fn node_resolve(base: &str, rel: &str) -> PathBuf {
    let rel_path = Path::new(rel);
    let combined = if rel_path.is_absolute() {
        PathBuf::from(rel)
    } else {
        let base_path = Path::new(base);
        if base.is_empty() {
            std::env::current_dir().unwrap_or_default().join(rel)
        } else if base_path.is_absolute() {
            base_path.join(rel)
        } else {
            std::env::current_dir()
                .unwrap_or_default()
                .join(base)
                .join(rel)
        }
    };
    normalize_lexical(&combined)
}

/// TS `isWithinWorkspace`, hardened to a path-boundary check: the resolved
/// path must BE the root or sit under it across a path separator — a raw
/// string prefix would let `/tmp/ws-escape` pass for root `/tmp/ws`. An
/// empty root matches everything (as in JS).
fn is_within(root: &str, full: &Path) -> bool {
    let root = root.trim_end_matches(['/', '\\']);
    if root.is_empty() {
        return true;
    }
    let full = full.to_string_lossy();
    match full.strip_prefix(root) {
        Some(rest) => rest.is_empty() || rest.starts_with('/') || rest.starts_with('\\'),
        None => false,
    }
}

/// Workspace-relative, forward-slash form of `path` for attribution rows:
/// resolve against `root` the same way the file ops do, then strip the root
/// prefix and normalize. `None` when the root is empty or the resolved path
/// does not sit under it.
pub(crate) fn workspace_relative(root: &str, path: &str) -> Option<String> {
    if root.is_empty() {
        return None;
    }
    let full = node_resolve(root, path);
    let rel = full.strip_prefix(root).ok()?;
    Some(crate::file_tracking::normalize_path(&rel.to_string_lossy()))
}

/// Resolve `rel` against `root` and enforce the within-workspace guard.
fn resolve_within(root: &str, rel: &str) -> Result<PathBuf> {
    let full = node_resolve(root, rel);
    if is_within(root, &full) {
        Ok(full)
    } else {
        Err(Error::Internal(ACCESS_DENIED.to_string()))
    }
}

fn io_err(e: std::io::Error) -> Error {
    Error::Internal(e.to_string())
}

/// Resolve an attachment-registry `stored_path` against the canonical root
/// with the within-workspace guard — shared by the `getAttachment` copy path
/// and the `getAttachmentInfo` exists-probe so a tampered row can never read
/// or probe outside the store.
pub(crate) fn resolve_attachment_source(
    canonical_root: &str,
    stored_path: &str,
) -> Result<PathBuf> {
    resolve_within(canonical_root, stored_path)
}

/// `file.read` → bare UTF-8 string.
pub(crate) fn read(root: &str, path: &str) -> Result<Value> {
    let full = resolve_within(root, path)?;
    let content = std::fs::read_to_string(&full).map_err(io_err)?;
    Ok(Value::String(content))
}

/// Maximum bytes served per `file.readChunk` call BEFORE base64 encoding.
/// Matches the transfer chunk cap ([`crate::transfer_import::IMPORT_MAX_CHUNK_BYTES`])
/// so the ~21.4 MiB encoded frame stays comfortably under the 40 MiB
/// outbound cap (PROTOCOL §1.3).
pub(crate) const READ_CHUNK_MAX_BYTES: usize = crate::transfer_import::IMPORT_MAX_CHUNK_BYTES;

/// `file.readChunk` → `{ content (base64), bytesRead, size }` — one
/// offset-windowed slice of a workspace file's raw bytes, the FE-ward
/// binary counterpart of the UTF-8-only `file.read` (PROTOCOL §5.9;
/// intent-hq/monorepo#2458). `length` is capped at
/// [`READ_CHUNK_MAX_BYTES`] decoded (over-cap → -32602); a read at/past
/// EOF returns an empty chunk with `bytesRead: 0`; a short window at EOF
/// returns just the remaining bytes. Directories are rejected as -32602;
/// a missing file surfaces as -32603 per the existing file-op convention.
///
/// Unlike the TS-parity CWD fallback the string `file.*` ops keep, an
/// empty root (unknown or pathless workspace) is rejected outright:
/// [`is_within`] treats an empty root as matching every path, so falling
/// through would let an arbitrary `workspaceId` + absolute path turn this
/// endpoint into an unrestricted raw-byte file reader.
pub(crate) fn read_chunk(root: &str, path: &str, offset: u64, length: u64) -> Result<Value> {
    use base64::Engine as _;
    use std::io::{Read as _, Seek as _};

    if root.is_empty() {
        return Err(Error::Internal(ACCESS_DENIED.to_string()));
    }
    if length == 0 {
        return Err(Error::InvalidParams("length must be positive".to_string()));
    }
    if length > READ_CHUNK_MAX_BYTES as u64 {
        return Err(Error::InvalidParams(format!(
            "length of {length} bytes exceeds the {READ_CHUNK_MAX_BYTES} byte cap"
        )));
    }
    let full = resolve_within(root, path)?;
    let md = std::fs::metadata(&full).map_err(io_err)?;
    if md.is_dir() {
        return Err(Error::InvalidParams(format!(
            "path is a directory — file.readChunk serves regular files: {path}"
        )));
    }
    let size = md.len();
    let len = if offset >= size {
        0
    } else {
        (size - offset).min(length) as usize
    };
    let mut buf = vec![0u8; len];
    if len > 0 {
        let mut file = std::fs::File::open(&full).map_err(io_err)?;
        file.seek(std::io::SeekFrom::Start(offset))
            .map_err(io_err)?;
        file.read_exact(&mut buf).map_err(io_err)?;
    }
    let content = base64::engine::general_purpose::STANDARD.encode(&buf);
    Ok(json!({ "content": content, "bytesRead": len, "size": size }))
}

/// `file.write` → `{ ok, path, size }` (size = UTF-16 code-unit length, matching
/// JS `content.length`). Parent directories are created.
pub(crate) fn write(root: &str, path: &str, content: &str) -> Result<Value> {
    let full = resolve_within(root, path)?;
    if let Some(parent) = full.parent() {
        std::fs::create_dir_all(parent).map_err(io_err)?;
    }
    std::fs::write(&full, content).map_err(io_err)?;
    let size = content.encode_utf16().count();
    Ok(json!({ "ok": true, "path": path, "size": size }))
}

/// Reduce a client-supplied attachment file name to a safe basename: keep
/// only the final path component (either separator style), then drop any
/// remaining `..`/`.`/empty outcome. `None` when nothing usable is left.
pub(crate) fn sanitize_attachment_name(file_name: &str) -> Option<String> {
    let base = file_name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or_default()
        .trim();
    if base.is_empty() || base == "." || base == ".." {
        return None;
    }
    Some(base.to_string())
}

/// Split a file name into (stem, extension-with-dot) for collision suffixing:
/// `report.tar.gz` → (`report.tar`, `.gz`), `Makefile` → (`Makefile`, ``),
/// `.env` → (`.env`, ``) — a leading dot is part of the stem, not an extension.
fn split_name(name: &str) -> (&str, &str) {
    let first = name.chars().next().map(char::len_utf8).unwrap_or(0);
    match name[first..].rfind('.') {
        Some(i) => name.split_at(first + i),
        None => (name, ""),
    }
}

/// Payload source for `place_attachment`.
pub(crate) enum AttachmentSource<'a> {
    /// Decoded payload bytes (the base64 `data` arm).
    Bytes(&'a [u8]),
    /// Absolute host-local file to copy (the `sourcePath` arm) — streamed via
    /// `fs::copy`, never buffered in memory.
    CopyFrom(&'a std::path::Path),
}

/// `file.placeAttachment` → `{ ok, path, fileName, size }` (PROTOCOL §5.9;
/// intent-hq/monorepo#1948). Places the payload into `.intent/attachments/`
/// under a collision-safe name: the sanitized basename as-is when free, else
/// `<stem>-2<ext>`, `<stem>-3<ext>`, … (bounded, then a UUID fallback). The
/// name is claimed atomically (`create_new`), so concurrent placements can
/// never pick the same one and overwrite each other. `path` in the result is
/// workspace-relative; `size` is the byte length written. The directory sits
/// under `.intent/`, whose default `.gitignore` excludes it from git tracking
/// (and therefore auto-commit and attribution).
pub(crate) fn place_attachment(
    root: &str,
    file_name: &str,
    source: AttachmentSource<'_>,
) -> Result<Value> {
    let name = sanitize_attachment_name(file_name).ok_or_else(|| {
        Error::InvalidParams(format!("invalid attachment fileName: {file_name:?}"))
    })?;
    // Classify a doomed `sourcePath` up front (before the directory and the
    // destination name are claimed) so the caller gets an actionable -32602
    // instead of an opaque -32603 "Internal error" from the copy step: a
    // dragged FOLDER is the common real-world case, alongside a vanished or
    // unreadable file (intent-hq/monorepo#2144).
    if let AttachmentSource::CopyFrom(src) = &source {
        match std::fs::metadata(src) {
            Ok(md) if md.is_dir() => {
                return Err(Error::InvalidParams(format!(
                    "sourcePath is a directory — only individual files can be attached: {}",
                    src.display()
                )));
            }
            Ok(md) if !md.is_file() => {
                return Err(Error::InvalidParams(format!(
                    "sourcePath is not a regular file: {}",
                    src.display()
                )));
            }
            // `NotADirectory`: an intermediate path component is a file
            // (e.g. `/tmp/file/child`) — client-invalid, same as NotFound.
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    || e.kind() == std::io::ErrorKind::NotADirectory =>
            {
                return Err(Error::InvalidParams(format!(
                    "sourcePath does not exist: {}",
                    src.display()
                )));
            }
            Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => {
                return Err(Error::InvalidParams(format!(
                    "sourcePath is not readable by the daemon (permission denied): {}",
                    src.display()
                )));
            }
            // Any other stat outcome falls through to the copy below, whose
            // error carries the I/O detail.
            _ => {}
        }
    }
    let dir = resolve_within(root, ATTACHMENTS_DIR)?;
    std::fs::create_dir_all(&dir).map_err(io_err)?;
    // Belt-and-braces exclusion: an ignore-all `.gitignore` inside the
    // attachments directory keeps placed files out of git even when the
    // repo carries a customized `.intent/.gitignore` that does not cover
    // `attachments/`.
    let marker = dir.join(".gitignore");
    if !marker.exists() {
        std::fs::write(&marker, "*\n").map_err(io_err)?;
    }

    let (stem, ext) = split_name(&name);
    let mut chosen = name.clone();
    let mut n = 2u32;
    let full = loop {
        let candidate = dir.join(&chosen);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(_) => break candidate,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                chosen = if n > 1000 {
                    format!("{stem}-{}{ext}", uuid::Uuid::new_v4().simple())
                } else {
                    format!("{stem}-{n}{ext}")
                };
                n += 1;
            }
            Err(e) => return Err(io_err(e)),
        }
    };

    let size = match source {
        AttachmentSource::Bytes(bytes) => {
            std::fs::write(&full, bytes).map_err(io_err)?;
            bytes.len() as u64
        }
        // The stat above classified the common cases; this residual arm
        // covers races (file replaced/removed between stat and copy) and
        // genuine I/O failures.
        AttachmentSource::CopyFrom(src) => std::fs::copy(src, &full).map_err(|e| {
            let _ = std::fs::remove_file(&full);
            match e.kind() {
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory => {
                    Error::InvalidParams(format!("sourcePath does not exist: {}", src.display()))
                }
                std::io::ErrorKind::PermissionDenied => Error::InvalidParams(format!(
                    "sourcePath is not readable by the daemon (permission denied): {}",
                    src.display()
                )),
                // `fs::copy` reports a non-regular-file source (e.g. the
                // path swapped to a directory between stat and copy) as
                // InvalidInput.
                std::io::ErrorKind::InvalidInput => Error::InvalidParams(format!(
                    "sourcePath is not a regular file: {}",
                    src.display()
                )),
                _ => Error::Internal(format!("failed to copy sourcePath {}: {e}", src.display())),
            }
        })?,
    };
    let rel = format!("{ATTACHMENTS_DIR}/{chosen}");
    Ok(json!({
        "ok": true,
        "path": rel,
        "fileName": chosen,
        "size": size,
    }))
}

/// MCP `ws.file.getAttachment` copy op (PROTOCOL §6.8): copy a registered
/// attachment from the CANONICAL workspace store (`canonical_root` +
/// `record.stored_path`) into the caller's working directory (`dest_root`,
/// which is the sandbox clone for CoW-sandboxed callers) under `dest_dir`
/// (default [`ATTACHMENTS_DIR`] — git-ignored by construction). Returns
/// `{ path, fileName, mimeType?, size, uploadedAt }` with `path` relative to
/// `dest_root`. The copy is skipped when the destination already holds an
/// identical file (same size + bytes); a partial copy is removed on failure.
///
/// The deleted-from-disk case is a DISTINCT error from an unknown id (which
/// the caller maps before reaching here): the registry row exists but the
/// stored file is gone, so the error names the original `fileName` and
/// `uploadedAt` and instructs the model to continue without the file rather
/// than retry.
pub(crate) fn get_attachment(
    canonical_root: &str,
    dest_root: &str,
    record: &intent_store::AttachmentRecord,
    dest_dir: Option<&str>,
) -> Result<Value> {
    if canonical_root.is_empty() || dest_root.is_empty() {
        return Err(Error::Internal(
            "workspace has no resolved filesystem root".to_string(),
        ));
    }
    // Source side: the stored path must stay inside the canonical store —
    // a tampered registry row must never read outside it.
    let src = resolve_attachment_source(canonical_root, &record.stored_path)?;
    if !src.is_file() {
        return Err(Error::Internal(format!(
            "attachment file was deleted from the workspace store: \"{}\" (uploaded {}) no \
             longer exists on disk. Continue without this file — do not retry the download.",
            record.file_name, record.uploaded_at
        )));
    }
    let dir = dest_dir.unwrap_or(ATTACHMENTS_DIR);
    let dest_dir_full = resolve_within(dest_root, dir)?;
    std::fs::create_dir_all(&dest_dir_full).map_err(io_err)?;
    // Keep retrieved copies out of git tracking (same ignore-all marker
    // `place_attachment` drops), whatever directory the caller picked.
    let marker = dest_dir_full.join(".gitignore");
    if !marker.exists() {
        std::fs::write(&marker, "*\n").map_err(io_err)?;
    }
    // resolve_within on the joined relative path guards a crafted fileName.
    let rel = format!("{}/{}", dir.trim_end_matches('/'), record.file_name);
    let dest = resolve_within(dest_root, &rel)?;
    let identical = match (std::fs::metadata(&src), std::fs::metadata(&dest)) {
        (Ok(s), Ok(d)) if s.len() == d.len() => {
            // Same size — confirm contents before skipping the copy.
            matches!(
                (std::fs::read(&src), std::fs::read(&dest)),
                (Ok(a), Ok(b)) if a == b
            )
        }
        _ => false,
    };
    if !identical {
        std::fs::copy(&src, &dest).map_err(|e| {
            let _ = std::fs::remove_file(&dest);
            Error::Internal(format!(
                "failed to copy attachment \"{}\": {e}",
                record.file_name
            ))
        })?;
    }
    let size = std::fs::metadata(&dest)
        .map(|m| m.len())
        .unwrap_or_default();
    let mut result = json!({
        "path": rel,
        "fileName": record.file_name,
        "size": size,
        "uploadedAt": record.uploaded_at,
    });
    if let Some(mime) = &record.mime_type {
        result["mimeType"] = json!(mime);
    }
    Ok(result)
}

/// `file.list` → bare array of `{ name, type }`.
pub(crate) fn list(root: &str, path: &str) -> Result<Value> {
    let full = resolve_within(root, path)?;
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(&full).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let kind = if entry.file_type().map_err(io_err)?.is_dir() {
            "directory"
        } else {
            "file"
        };
        entries.push(json!({ "name": name, "type": kind }));
    }
    Ok(Value::Array(entries))
}

/// Build the relative path of a child entry under the requested directory,
/// dropping a leading `.`/`./`/empty base so root entries are bare names.
fn join_rel(base: &str, name: &str) -> String {
    let b = base.trim();
    let b = b.strip_prefix("./").unwrap_or(b);
    let b = b.trim_matches('/');
    if b.is_empty() || b == "." {
        name.to_string()
    } else {
        format!("{b}/{name}")
    }
}

/// `file.tree` → bare array of `{ path, name, isDirectory }` for the entries
/// directly under `path` (defaulting to the workspace root). The FE anchors the
/// explorer here and lazy-lists children via `file.list`, so a shallow listing
/// is sufficient. Shares the same within-workspace guard as the other file ops.
pub(crate) fn tree(root: &str, path: &str) -> Result<Value> {
    let full = resolve_within(root, path)?;
    let mut entries = Vec::new();
    for entry in std::fs::read_dir(&full).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let is_directory = entry.file_type().map_err(io_err)?.is_dir();
        let rel = join_rel(path, &name);
        entries.push(json!({ "path": rel, "name": name, "isDirectory": is_directory }));
    }
    Ok(Value::Array(entries))
}

/// `file.delete` → `{ ok, path, deleted: true }` (rejects directories).
pub(crate) fn delete(root: &str, path: &str) -> Result<Value> {
    let full = resolve_within(root, path)?;
    if !full.exists() {
        return Err(Error::Internal(format!("File not found: {path}")));
    }
    if full.is_dir() {
        return Err(Error::Internal(format!(
            "Cannot delete directory with this method: {path}"
        )));
    }
    std::fs::remove_file(&full).map_err(io_err)?;
    Ok(json!({ "ok": true, "path": path, "deleted": true }))
}

/// `file.mkdir` → `{ ok, path, created: true }` or `{ ok, path, existed: true }`.
pub(crate) fn mkdir(root: &str, path: &str) -> Result<Value> {
    let full = resolve_within(root, path)?;
    if full.exists() {
        if full.is_dir() {
            return Ok(json!({ "ok": true, "path": path, "existed": true }));
        }
        return Err(Error::Internal(format!(
            "Path exists but is not a directory: {path}"
        )));
    }
    std::fs::create_dir_all(&full).map_err(io_err)?;
    Ok(json!({ "ok": true, "path": path, "created": true }))
}

/// `file.exists` → `{ exists, isFile, isDirectory }`, mirroring the legacy
/// `intent-server.cjs` `fileExists` shape so retirement-wave consumers
/// (`RemoteExecutor`, `RemoteMetadataFS`, `RemoteFileSystemService`,
/// `MetadataSyncService`) swap over 1:1. Any lookup error yields the same
/// all-false shape as the legacy handler, rather than surfacing as `-32603`.
pub(crate) fn exists(root: &str, path: &str) -> Result<Value> {
    let full = resolve_within(root, path)?;
    match std::fs::metadata(&full) {
        Ok(md) => Ok(json!({
            "exists": true,
            "isFile": md.is_file(),
            "isDirectory": md.is_dir(),
        })),
        Err(_) => Ok(json!({
            "exists": false,
            "isFile": false,
            "isDirectory": false,
        })),
    }
}

/// `file.stat` → `{ size, mtime, isFile, isDirectory, isSymlink, permissions }`,
/// mirroring the legacy `intent-server.cjs` `stat` shape. Symlinks are followed
/// for size/type reporting (matching `fs.lstatSync` + `fs.statSync` when the
/// entry is a symlink), and `permissions` is the octal mode string ("0644").
pub(crate) fn stat(root: &str, path: &str) -> Result<Value> {
    let full = resolve_within(root, path)?;
    let lmd = std::fs::symlink_metadata(&full).map_err(io_err)?;
    let is_symlink = lmd.file_type().is_symlink();
    let md = if is_symlink {
        std::fs::metadata(&full).map_err(io_err)?
    } else {
        lmd
    };
    let mtime = md
        .modified()
        .ok()
        .map(mtime_iso)
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".to_string());
    let permissions = permissions_octal(&md);
    Ok(json!({
        "size": md.len(),
        "mtime": mtime,
        "isFile": md.is_file(),
        "isDirectory": md.is_dir(),
        "isSymlink": is_symlink,
        "permissions": permissions,
    }))
}

/// Format a `SystemTime` as the ISO-8601 string legacy `stat` produced via
/// `Date.prototype.toISOString` (millisecond precision, always `Z`). Falls back
/// to the epoch when the timestamp is outside the representable range.
fn mtime_iso(t: std::time::SystemTime) -> String {
    let dur = t.duration_since(std::time::UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs() as i64;
    let ms = dur.subsec_millis();
    match time::OffsetDateTime::from_unix_timestamp(secs) {
        Ok(dt) => format!(
            "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
            dt.year(),
            u8::from(dt.month()),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
            ms,
        ),
        Err(_) => "1970-01-01T00:00:00.000Z".to_string(),
    }
}

/// Render POSIX permission bits as the legacy octal string ("0" + mode & 0o777).
/// On non-Unix targets we fall back to "0000" — the FE only inspects this
/// string on POSIX hosts, mirroring the legacy behaviour.
#[cfg(unix)]
fn permissions_octal(md: &std::fs::Metadata) -> String {
    use std::os::unix::fs::PermissionsExt;
    let mode = md.permissions().mode() & 0o777;
    format!("0{mode:o}")
}

#[cfg(not(unix))]
fn permissions_octal(_md: &std::fs::Metadata) -> String {
    "0000".to_string()
}

/// `file.rename` → `{ ok, oldPath, newPath, renamed: true, isDirectory }`.
pub(crate) fn rename(root: &str, old_path: &str, new_path: &str) -> Result<Value> {
    let old_full = node_resolve(root, old_path);
    let new_full = node_resolve(root, new_path);
    if !is_within(root, &old_full) || !is_within(root, &new_full) {
        return Err(Error::Internal(ACCESS_DENIED.to_string()));
    }
    if !old_full.exists() {
        return Err(Error::Internal(format!(
            "Source file not found: {old_path}"
        )));
    }
    if new_full.exists() {
        return Err(Error::Internal(format!(
            "Destination already exists: {new_path}"
        )));
    }
    // `symlink_metadata` (no follow): a symlink-to-directory is renamed and
    // attributed as the single symlink entry git actually tracks — walking the
    // link target would record rows for paths git never reports.
    let is_directory = std::fs::symlink_metadata(&old_full)
        .map(|m| m.is_dir())
        .unwrap_or(false);
    if let Some(parent) = new_full.parent() {
        std::fs::create_dir_all(parent).map_err(io_err)?;
    }
    std::fs::rename(&old_full, &new_full).map_err(io_err)?;
    Ok(json!({
        "ok": true,
        "oldPath": old_path,
        "newPath": new_path,
        "renamed": true,
        "isDirectory": is_directory,
    }))
}

/// Recursively list the regular files under `dir` (workspace-relative),
/// returned as paths relative to `dir` itself, sorted. Used by the
/// directory-rename attribution path (monorepo#957) to enumerate the moved
/// tree. Enforces the same within-workspace guard as the other file ops
/// (out-of-root dirs yield an empty list). Best-effort: unreadable entries
/// and symlinks (neither dir nor regular file without following) are skipped.
pub(crate) fn walk_files(root: &str, dir: &str) -> Vec<String> {
    let base = node_resolve(root, dir);
    if !is_within(root, &base) {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut stack = vec![base.clone()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(t) if t.is_dir() => stack.push(path),
                Ok(t) if t.is_file() => {
                    if let Ok(rel) = path.strip_prefix(&base) {
                        out.push(rel.to_string_lossy().into_owned());
                    }
                }
                _ => {}
            }
        }
    }
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct TempRoot {
        path: PathBuf,
    }

    impl TempRoot {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("intentd-fileops-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&path).unwrap();
            // Canonicalize so the root matches the resolved paths' prefix (the
            // OS temp dir may be a symlink, e.g. macOS `/var` → `/private/var`).
            let path = std::fs::canonicalize(&path).unwrap();
            Self { path }
        }
        fn root(&self) -> String {
            self.path.to_string_lossy().into_owned()
        }
    }

    impl Drop for TempRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[test]
    fn write_then_read_round_trips_bare_string() {
        let t = TempRoot::new();
        let root = t.root();
        let w = write(&root, "sub/hello.txt", "héllo").unwrap();
        assert_eq!(w, json!({ "ok": true, "path": "sub/hello.txt", "size": 5 }));
        let r = read(&root, "sub/hello.txt").unwrap();
        assert_eq!(r, Value::String("héllo".to_string()));
    }

    #[test]
    fn list_returns_bare_array_with_types() {
        let t = TempRoot::new();
        let root = t.root();
        write(&root, "a.txt", "x").unwrap();
        mkdir(&root, "d").unwrap();
        let mut items = list(&root, ".").unwrap().as_array().unwrap().clone();
        items.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        assert_eq!(
            items,
            vec![
                json!({ "name": "a.txt", "type": "file" }),
                json!({ "name": "d", "type": "directory" }),
            ]
        );
    }

    #[test]
    fn list_defaults_to_dot() {
        let t = TempRoot::new();
        let root = t.root();
        write(&root, "only.txt", "x").unwrap();
        let items = list(&root, ".").unwrap();
        assert_eq!(items.as_array().unwrap().len(), 1);
    }

    #[test]
    fn tree_returns_root_entries_with_three_fields() {
        let t = TempRoot::new();
        let root = t.root();
        write(&root, "a.txt", "x").unwrap();
        mkdir(&root, "d").unwrap();
        let mut items = tree(&root, ".").unwrap().as_array().unwrap().clone();
        items.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
        assert_eq!(
            items,
            vec![
                json!({ "path": "a.txt", "name": "a.txt", "isDirectory": false }),
                json!({ "path": "d", "name": "d", "isDirectory": true }),
            ]
        );
    }

    #[test]
    fn tree_prefixes_subpath_in_path_field() {
        let t = TempRoot::new();
        let root = t.root();
        write(&root, "sub/inner.txt", "x").unwrap();
        let items = tree(&root, "sub").unwrap();
        let items = items.as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(
            items[0],
            json!({ "path": "sub/inner.txt", "name": "inner.txt", "isDirectory": false })
        );
    }

    #[test]
    fn tree_rejects_path_traversal() {
        let t = TempRoot::new();
        let root = t.root();
        match tree(&root, "..") {
            Err(Error::Internal(m)) => assert_eq!(m, ACCESS_DENIED),
            other => panic!("expected access denied, got {other:?}"),
        }
    }

    #[test]
    fn delete_removes_file_and_rejects_dirs() {
        let t = TempRoot::new();
        let root = t.root();
        write(&root, "gone.txt", "x").unwrap();
        let d = delete(&root, "gone.txt").unwrap();
        assert_eq!(
            d,
            json!({ "ok": true, "path": "gone.txt", "deleted": true })
        );
        mkdir(&root, "dir").unwrap();
        assert!(matches!(delete(&root, "dir"), Err(Error::Internal(_))));
        assert!(matches!(delete(&root, "missing"), Err(Error::Internal(_))));
    }

    #[test]
    fn mkdir_created_then_existed() {
        let t = TempRoot::new();
        let root = t.root();
        assert_eq!(
            mkdir(&root, "x/y").unwrap(),
            json!({ "ok": true, "path": "x/y", "created": true })
        );
        assert_eq!(
            mkdir(&root, "x/y").unwrap(),
            json!({ "ok": true, "path": "x/y", "existed": true })
        );
    }

    #[test]
    fn rename_reports_directory_flag() {
        let t = TempRoot::new();
        let root = t.root();
        write(&root, "a.txt", "x").unwrap();
        let r = rename(&root, "a.txt", "b.txt").unwrap();
        assert_eq!(
            r,
            json!({
                "ok": true, "oldPath": "a.txt", "newPath": "b.txt",
                "renamed": true, "isDirectory": false
            })
        );
        mkdir(&root, "d1").unwrap();
        let rd = rename(&root, "d1", "d2").unwrap();
        assert_eq!(rd["isDirectory"], json!(true));
        // Destination exists / source missing both error.
        write(&root, "c.txt", "x").unwrap();
        assert!(matches!(
            rename(&root, "c.txt", "b.txt"),
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            rename(&root, "nope", "z.txt"),
            Err(Error::Internal(_))
        ));
    }

    #[test]
    fn path_traversal_is_rejected() {
        let t = TempRoot::new();
        let root = t.root();
        for res in [
            read(&root, "../escape.txt"),
            write(&root, "../escape.txt", "x"),
            list(&root, ".."),
            delete(&root, "../escape.txt"),
            mkdir(&root, "../escape"),
            rename(&root, "../a", "b"),
            rename(&root, "a", "../b"),
            exists(&root, "../escape.txt"),
            stat(&root, "../escape.txt"),
        ] {
            match res {
                Err(Error::Internal(m)) => assert_eq!(m, ACCESS_DENIED),
                other => panic!("expected access denied, got {other:?}"),
            }
        }
    }

    #[test]
    fn exists_reports_present_absent_and_type() {
        let t = TempRoot::new();
        let root = t.root();
        write(&root, "a.txt", "x").unwrap();
        mkdir(&root, "d").unwrap();
        assert_eq!(
            exists(&root, "a.txt").unwrap(),
            json!({ "exists": true, "isFile": true, "isDirectory": false })
        );
        assert_eq!(
            exists(&root, "d").unwrap(),
            json!({ "exists": true, "isFile": false, "isDirectory": true })
        );
        // A missing path is not an error — legacy handler returned the same shape.
        assert_eq!(
            exists(&root, "missing.txt").unwrap(),
            json!({ "exists": false, "isFile": false, "isDirectory": false })
        );
    }

    #[test]
    fn stat_returns_full_legacy_shape() {
        let t = TempRoot::new();
        let root = t.root();
        write(&root, "a.txt", "hello").unwrap();
        let s = stat(&root, "a.txt").unwrap();
        assert_eq!(s["size"], json!(5u64));
        assert_eq!(s["isFile"], json!(true));
        assert_eq!(s["isDirectory"], json!(false));
        assert_eq!(s["isSymlink"], json!(false));
        let mtime = s["mtime"].as_str().expect("mtime is string");
        assert!(mtime.ends_with('Z'), "mtime {mtime} not Z-terminated");
        assert!(mtime.contains('T'), "mtime {mtime} missing T separator");
        let perms = s["permissions"].as_str().expect("perms is string");
        assert!(
            perms.starts_with('0') && perms.len() >= 4,
            "unexpected permissions: {perms}"
        );

        mkdir(&root, "sub").unwrap();
        let ds = stat(&root, "sub").unwrap();
        assert_eq!(ds["isFile"], json!(false));
        assert_eq!(ds["isDirectory"], json!(true));

        assert!(matches!(stat(&root, "nope"), Err(Error::Internal(_))));
    }

    fn decode_chunk(v: &Value) -> Vec<u8> {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD
            .decode(v["content"].as_str().expect("content is base64 string"))
            .expect("valid base64")
    }

    #[test]
    fn read_chunk_serves_offset_windows_and_eof_short_read() {
        let t = TempRoot::new();
        let root = t.root();
        // Binary bytes (invalid UTF-8) — exactly what `file.read` rejects.
        let bytes: Vec<u8> = (0..=255u8).collect();
        std::fs::write(std::path::Path::new(&root).join("blob.bin"), &bytes).unwrap();

        // Full read in one window.
        let full = read_chunk(&root, "blob.bin", 0, 1024).unwrap();
        assert_eq!(full["size"], json!(256u64));
        assert_eq!(full["bytesRead"], json!(256));
        assert_eq!(decode_chunk(&full), bytes);

        // Interior window.
        let mid = read_chunk(&root, "blob.bin", 100, 50).unwrap();
        assert_eq!(mid["bytesRead"], json!(50));
        assert_eq!(decode_chunk(&mid), &bytes[100..150]);

        // Short read at EOF: window extends past the end → remaining bytes.
        let tail = read_chunk(&root, "blob.bin", 250, 50).unwrap();
        assert_eq!(tail["bytesRead"], json!(6));
        assert_eq!(tail["size"], json!(256u64));
        assert_eq!(decode_chunk(&tail), &bytes[250..]);

        // Offset at/past EOF: empty chunk, not an error.
        let past = read_chunk(&root, "blob.bin", 256, 16).unwrap();
        assert_eq!(past["bytesRead"], json!(0));
        assert_eq!(decode_chunk(&past), Vec::<u8>::new());
        let far = read_chunk(&root, "blob.bin", 10_000, 16).unwrap();
        assert_eq!(far["bytesRead"], json!(0));
    }

    #[test]
    fn read_chunk_rejects_containment_directory_cap_and_zero_length() {
        let t = TempRoot::new();
        let root = t.root();
        write(&root, "a.txt", "x").unwrap();
        mkdir(&root, "dir").unwrap();

        // Containment: same ACCESS_DENIED as the other file ops.
        match read_chunk(&root, "../escape.bin", 0, 16) {
            Err(Error::Internal(m)) => assert_eq!(m, ACCESS_DENIED),
            other => panic!("expected access denied, got {other:?}"),
        }
        // Directory → -32602 naming the cause.
        match read_chunk(&root, "dir", 0, 16) {
            Err(Error::InvalidParams(m)) => {
                assert!(m.contains("directory"), "unexpected message: {m}");
            }
            other => panic!("expected InvalidParams, got {other:?}"),
        }
        // Over-cap length → -32602 naming the cap.
        match read_chunk(&root, "a.txt", 0, READ_CHUNK_MAX_BYTES as u64 + 1) {
            Err(Error::InvalidParams(m)) => {
                assert!(
                    m.contains(&READ_CHUNK_MAX_BYTES.to_string()),
                    "unexpected message: {m}"
                );
            }
            other => panic!("expected InvalidParams, got {other:?}"),
        }
        // Zero length → -32602.
        assert!(matches!(
            read_chunk(&root, "a.txt", 0, 0),
            Err(Error::InvalidParams(_))
        ));
        // Missing file → -32603 per the existing file-op convention.
        assert!(matches!(
            read_chunk(&root, "missing.bin", 0, 16),
            Err(Error::Internal(_))
        ));
        // Empty root (unknown/pathless workspace) → ACCESS_DENIED, never the
        // CWD fallback: an empty root passes is_within for every path, which
        // would make this an unrestricted raw-byte reader for absolute paths.
        match read_chunk("", "/etc/hostname", 0, 16) {
            Err(Error::Internal(m)) => assert_eq!(m, ACCESS_DENIED),
            other => panic!("expected access denied for empty root, got {other:?}"),
        }
    }

    fn place_bytes(root: &str, file_name: &str, bytes: &[u8]) -> Result<Value> {
        place_attachment(root, file_name, AttachmentSource::Bytes(bytes))
    }

    #[test]
    fn place_attachment_writes_into_attachments_dir() {
        let t = TempRoot::new();
        let root = t.root();
        let r = place_bytes(&root, "report.har", b"payload").unwrap();
        assert_eq!(
            r,
            json!({
                "ok": true,
                "path": ".intent/attachments/report.har",
                "fileName": "report.har",
                "size": 7
            })
        );
        let on_disk =
            std::fs::read(std::path::Path::new(&root).join(".intent/attachments/report.har"))
                .unwrap();
        assert_eq!(on_disk, b"payload");
        // The belt-and-braces ignore-all marker inside attachments/ exists.
        let marker = std::fs::read_to_string(
            std::path::Path::new(&root).join(".intent/attachments/.gitignore"),
        )
        .unwrap();
        assert_eq!(marker, "*\n");
    }

    #[test]
    fn place_attachment_collision_safe_naming() {
        let t = TempRoot::new();
        let root = t.root();
        let a = place_bytes(&root, "dump.tar.gz", b"one").unwrap();
        let b = place_bytes(&root, "dump.tar.gz", b"two").unwrap();
        let c = place_bytes(&root, "dump.tar.gz", b"three").unwrap();
        assert_eq!(a["fileName"], json!("dump.tar.gz"));
        assert_eq!(b["fileName"], json!("dump.tar-2.gz"));
        assert_eq!(c["fileName"], json!("dump.tar-3.gz"));
        // Extension-less and dotfile names suffix at the end.
        place_bytes(&root, "Makefile", b"x").unwrap();
        let m2 = place_bytes(&root, "Makefile", b"y").unwrap();
        assert_eq!(m2["fileName"], json!("Makefile-2"));
        place_bytes(&root, ".env", b"x").unwrap();
        let e2 = place_bytes(&root, ".env", b"y").unwrap();
        assert_eq!(e2["fileName"], json!(".env-2"));
        // Collisions never clobber: the originals still hold their bytes.
        let dir = std::path::Path::new(&root).join(".intent/attachments");
        assert_eq!(std::fs::read(dir.join("dump.tar.gz")).unwrap(), b"one");
        assert_eq!(std::fs::read(dir.join("dump.tar-2.gz")).unwrap(), b"two");
    }

    /// Regression (intentd#1090 review): names whose first character is
    /// multi-byte UTF-8 must not panic in `split_name` — placement and
    /// collision suffixing both work.
    #[test]
    fn place_attachment_multibyte_leading_names() {
        let t = TempRoot::new();
        let root = t.root();
        let a = place_bytes(&root, "截图.png", b"one").unwrap();
        assert_eq!(a["fileName"], json!("截图.png"));
        let b = place_bytes(&root, "截图.png", b"two").unwrap();
        assert_eq!(b["fileName"], json!("截图-2.png"));
        let c = place_bytes(&root, "é.txt", b"x").unwrap();
        assert_eq!(c["fileName"], json!("é.txt"));
        let d = place_bytes(&root, "é", b"x").unwrap();
        assert_eq!(d["fileName"], json!("é"));
        let e = place_bytes(&root, "é", b"y").unwrap();
        assert_eq!(e["fileName"], json!("é-2"));
    }

    #[test]
    fn place_attachment_sanitizes_names_and_rejects_unusable() {
        let t = TempRoot::new();
        let root = t.root();
        // Path components are stripped down to the basename — no escape.
        let r = place_bytes(&root, "../../etc/passwd", b"x").unwrap();
        assert_eq!(r["fileName"], json!("passwd"));
        let w = place_bytes(&root, "C:\\Users\\me\\file.txt", b"x").unwrap();
        assert_eq!(w["fileName"], json!("file.txt"));
        for bad in ["", "   ", ".", "..", "dir/"] {
            assert!(
                matches!(place_bytes(&root, bad, b"x"), Err(Error::InvalidParams(_))),
                "expected InvalidParams for {bad:?}"
            );
        }
    }

    #[test]
    fn place_attachment_copy_from_source_path() {
        let t = TempRoot::new();
        let root = t.root();
        let src = std::path::Path::new(&root).join("src.bin");
        std::fs::write(&src, b"copied bytes").unwrap();
        let r = place_attachment(&root, "src.bin", AttachmentSource::CopyFrom(&src)).unwrap();
        assert_eq!(r["fileName"], json!("src.bin"));
        assert_eq!(r["size"], json!(12));
        assert_eq!(
            std::fs::read(std::path::Path::new(&root).join(".intent/attachments/src.bin")).unwrap(),
            b"copied bytes"
        );
        // A missing source fails as classified InvalidParams (monorepo#2144)
        // without leaving the claimed placeholder behind.
        let missing = std::path::Path::new(&root).join("nope.bin");
        let err = place_attachment(&root, "gone.bin", AttachmentSource::CopyFrom(&missing));
        match err {
            Err(Error::InvalidParams(msg)) => {
                assert!(msg.contains("does not exist"), "unexpected message: {msg}");
            }
            other => panic!("expected InvalidParams, got {other:?}"),
        }
        assert!(!std::path::Path::new(&root)
            .join(".intent/attachments/gone.bin")
            .exists());
    }

    /// Regression (monorepo#2144): a dragged FOLDER used to fail the copy step
    /// with an opaque -32603 "Internal error"; it must be rejected up front as
    /// InvalidParams naming the cause, with no placeholder left behind.
    #[test]
    fn place_attachment_copy_from_directory_is_classified() {
        let t = TempRoot::new();
        let root = t.root();
        let subdir = std::path::Path::new(&root).join("some-folder");
        std::fs::create_dir_all(&subdir).unwrap();
        let err = place_attachment(&root, "some-folder", AttachmentSource::CopyFrom(&subdir));
        match err {
            Err(Error::InvalidParams(msg)) => {
                assert!(msg.contains("directory"), "unexpected message: {msg}");
            }
            other => panic!("expected InvalidParams, got {other:?}"),
        }
        assert!(!std::path::Path::new(&root)
            .join(".intent/attachments/some-folder")
            .exists());
    }

    /// An intermediate path component that is a file (e.g. `/tmp/file/child`)
    /// stats as `NotADirectory` — classified as the same client-invalid
    /// "does not exist" as a plain missing path, never `-32603 Internal`.
    #[test]
    fn place_attachment_copy_from_file_intermediate_component_is_classified() {
        let t = TempRoot::new();
        let root = t.root();
        let file = std::path::Path::new(&root).join("plain.txt");
        std::fs::write(&file, b"x").unwrap();
        let bogus = file.join("child.txt");
        let err = place_attachment(&root, "child.txt", AttachmentSource::CopyFrom(&bogus));
        match err {
            Err(Error::InvalidParams(msg)) => {
                assert!(msg.contains("does not exist"), "unexpected message: {msg}");
            }
            other => panic!("expected InvalidParams, got {other:?}"),
        }
    }

    /// A symlink to a regular file is accepted: `fs::metadata` follows links,
    /// matching the Finder/Explorer drag behavior for aliased files.
    #[cfg(unix)]
    #[test]
    fn place_attachment_copy_from_symlink_to_file() {
        use std::os::unix::fs::symlink;
        let t = TempRoot::new();
        let root = t.root();
        let target = std::path::Path::new(&root).join("real.txt");
        std::fs::write(&target, b"linked").unwrap();
        let link = std::path::Path::new(&root).join("alias.txt");
        symlink(&target, &link).unwrap();
        let r = place_attachment(&root, "alias.txt", AttachmentSource::CopyFrom(&link)).unwrap();
        assert_eq!(r["fileName"], json!("alias.txt"));
        assert_eq!(r["size"], json!(6));
    }

    #[cfg(unix)]
    #[test]
    fn stat_follows_symlinks_and_flags_them() {
        use std::os::unix::fs::symlink;
        let t = TempRoot::new();
        let root = t.root();
        write(&root, "target.txt", "abc").unwrap();
        symlink(
            std::path::Path::new(&root).join("target.txt"),
            std::path::Path::new(&root).join("link.txt"),
        )
        .unwrap();
        let s = stat(&root, "link.txt").unwrap();
        assert_eq!(s["isSymlink"], json!(true));
        assert_eq!(s["isFile"], json!(true));
        assert_eq!(s["size"], json!(3u64));
    }
}
