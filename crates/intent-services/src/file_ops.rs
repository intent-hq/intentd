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

/// TS `isWithinWorkspace`: raw string prefix of the resolved path against the
/// workspace root (an empty root matches everything, as in JS).
fn is_within(root: &str, full: &Path) -> bool {
    full.to_string_lossy().starts_with(root)
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

/// `file.read` → bare UTF-8 string.
pub(crate) fn read(root: &str, path: &str) -> Result<Value> {
    let full = resolve_within(root, path)?;
    let content = std::fs::read_to_string(&full).map_err(io_err)?;
    Ok(Value::String(content))
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
    format!("0{:o}", mode)
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
    let is_directory = old_full.is_dir();
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
