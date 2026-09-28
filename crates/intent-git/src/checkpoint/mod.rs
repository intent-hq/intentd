//! Transfer-compatible, ref-free checkpoint codec. Callers must quiesce every
//! writer for the whole capture (including submodules). The second read detects
//! races; it is not a substitute for the runtime's shared capture barrier.
//!
//! Objects are deliberately unanchored here. Retention, upload quarantine and
//! durable promotion belong to the caller. Restore writes only a new private
//! checkout; callers must not expose it until the complete manifest is restored.

use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};

use git2::{Index, Oid, Repository};
use intent_core::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::map_git_err;

pub const WIP_SENTINEL: &str = "intent-transfer: WIP snapshot";
pub const INDEX_TREE_TRAILER: &str = "Intent-Index-Tree:";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Snapshot {
    pub object_format: String,
    pub head: String,
    pub index: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
}

/// Additional runtime-owned directories (relative to this checkout), e.g. a
/// provider's spawn directory. These are never included, even when staged.
/// Tracked excluded material fails capture rather than making a lossy index.
#[derive(Debug, Default)]
pub struct CaptureOptions {
    pub excluded_paths: Vec<PathBuf>,
}

fn invalid(message: impl Into<String>) -> Error {
    Error::Internal(format!("checkpoint: {}", message.into()))
}

/// Whether a portable repository-relative path is safe to join below a root.
#[must_use]
pub fn safe_relative_path(path: &str) -> bool {
    !path.is_empty()
        && !path.contains(['\\', ':', '\0'])
        && path
            .split('/')
            .all(|p| !p.is_empty() && p != "." && p != ".." && !p.eq_ignore_ascii_case(".git"))
        && Path::new(path)
            .components()
            .all(|p| matches!(p, Component::Normal(_)))
}

fn excluded(path: &Path, options: &CaptureOptions) -> bool {
    [
        ".intent/attachments",
        "tool-outputs",
        ".intent/secrets",
        ".codex/auth.json",
        ".claude/.credentials.json",
        ".gemini/oauth_creds.json",
        ".ssh",
        ".aws/credentials",
    ]
    .iter()
    .any(|p| path.starts_with(p))
        || options.excluded_paths.iter().any(|p| path.starts_with(p))
}

/// The same trailer is read by workspace transfer, including legacy bundles.
#[must_use]
pub fn parse_index_tree_trailer(message: &str) -> Option<Oid> {
    message
        .lines()
        .find_map(|l| l.strip_prefix(INDEX_TREE_TRAILER))
        .and_then(|v| Oid::from_str(v.trim()).ok())
}

/// Write the shared transfer index anchor and WIP commit without moving refs.
/// The caller decides whether to attach it to a transfer branch or retain it in
/// an immutable checkpoint. Both parents are needed for bundle object closure.
///
/// # Errors
/// Returns an error when any source object, signature or object write fails.
pub fn write_wip(
    repo: &Repository,
    head: Oid,
    index_tree: Oid,
    work_tree: Oid,
) -> Result<(Oid, Oid)> {
    let head = repo.find_commit(head).map_err(map_git_err)?;
    let index_tree = repo.find_tree(index_tree).map_err(map_git_err)?;
    let work_tree = repo.find_tree(work_tree).map_err(map_git_err)?;
    let signature = match repo.signature() {
        Ok(s) => s,
        Err(e) if e.code() == git2::ErrorCode::NotFound => {
            git2::Signature::now("Intent", "intent@localhost").map_err(map_git_err)?
        }
        Err(e) => return Err(map_git_err(e)),
    };
    let anchor = repo
        .commit(
            None,
            &signature,
            &signature,
            "intent-transfer: index state anchor",
            &index_tree,
            &[&head],
        )
        .map_err(map_git_err)?;
    let anchor_commit = repo.find_commit(anchor).map_err(map_git_err)?;
    let message = format!("{WIP_SENTINEL}\n\n{INDEX_TREE_TRAILER} {}", index_tree.id());
    let wip = repo
        .commit(
            None,
            &signature,
            &signature,
            &message,
            &work_tree,
            &[&head, &anchor_commit],
        )
        .map_err(map_git_err)?;
    Ok((anchor, wip))
}

#[derive(PartialEq, Eq)]
struct Cut {
    head: Oid,
    branch: Option<String>,
    index_bytes: Option<Vec<u8>>,
    index_tree: Oid,
    work_tree: Oid,
}

fn path_from_bytes(bytes: &[u8]) -> PathBuf {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        PathBuf::from(std::ffi::OsStr::from_bytes(bytes))
    }
    #[cfg(not(unix))]
    PathBuf::from(String::from_utf8_lossy(bytes).into_owned())
}

fn read_cut(path: &Path, options: &CaptureOptions) -> Result<Cut> {
    let repo = Repository::open(path).map_err(map_git_err)?;
    let config = repo.config().map_err(map_git_err)?;
    if config
        .get_string("extensions.objectFormat")
        .is_ok_and(|f| f != "sha1")
    {
        return Err(invalid("only SHA-1 repositories are supported"));
    }
    let head_ref = repo.head().map_err(|_| invalid("unborn HEAD"))?;
    let head = head_ref.peel_to_commit().map_err(map_git_err)?;
    let branch = if head_ref.is_branch() {
        Some(head_ref.shorthand().map_err(map_git_err)?.to_owned())
    } else {
        None
    };
    let workdir = repo.workdir().ok_or_else(|| invalid("bare source"))?;
    let mut index = repo.index().map_err(map_git_err)?;
    let index_bytes = match std::fs::read(index.path().ok_or_else(|| invalid("index has no path"))?)
    {
        Ok(b) => Some(b),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(invalid(e.to_string())),
    };
    if index.has_conflicts() {
        return Err(invalid("unresolved index conflicts"));
    }
    let mut head_index = Index::new().map_err(map_git_err)?;
    head_index
        .read_tree(&head.tree().map_err(map_git_err)?)
        .map_err(map_git_err)?;
    for entry in index.iter().chain(head_index.iter()) {
        if excluded(&path_from_bytes(&entry.path), options) {
            return Err(invalid("runtime-owned material is tracked"));
        }
        if entry.flags_extended & 0x2000 != 0 {
            return Err(invalid("intent-to-add index is unsupported"));
        }
        if entry.flags_extended & 0x4000 != 0 {
            return Err(invalid("sparse index is unsupported"));
        }
    }
    let index_tree = index.write_tree().map_err(map_git_err)?;
    // Keep staged gitlinks even when a child moved HEAD. Its actual HEAD/WIP
    // belongs to a separate repository entry.
    let gitlinks: Vec<_> = index.iter().filter(|e| e.mode == 0o16_0000).collect();
    let mut opts = git2::StatusOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(false);
    let statuses = repo.statuses(Some(&mut opts)).map_err(map_git_err)?;
    let mut nested = Vec::new();
    for status in statuses
        .iter()
        .filter(|s| s.status().contains(git2::Status::WT_NEW))
    {
        let rel = path_from_bytes(status.path_bytes());
        let full = workdir.join(&rel);
        if std::fs::symlink_metadata(&full).is_ok_and(|m| m.is_dir()) && full.join(".git").exists()
        {
            nested.push(rel);
        }
    }
    let mut skip = |rel: &Path, _: &[u8]| -> i32 {
        let special = std::fs::symlink_metadata(workdir.join(rel))
            .is_ok_and(|m| !m.is_dir() && !m.is_file() && !m.file_type().is_symlink());
        i32::from(excluded(rel, options) || special || nested.iter().any(|p| rel.starts_with(p)))
    };
    index
        .add_all(["*"], git2::IndexAddOption::DEFAULT, Some(&mut skip))
        .map_err(map_git_err)?;
    for entry in gitlinks {
        index.add(&entry).map_err(map_git_err)?;
    }
    // add_all selects Git-visible paths, but its clean filters normalize file
    // bytes (e.g. CRLF). A checkpoint preserves the actual worktree; overwrite
    // only this in-memory index with raw blobs, never follow symlink contents.
    let candidates: Vec<_> = index.iter().filter(|e| e.mode != 0o16_0000).collect();
    for mut entry in candidates {
        let relative = path_from_bytes(&entry.path);
        let full = workdir.join(&relative);
        let metadata = match std::fs::symlink_metadata(&full) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                index.remove_path(&relative).map_err(map_git_err)?;
                continue;
            }
            Err(e) => return Err(invalid(e.to_string())),
        };
        if metadata.file_type().is_symlink() {
            let target = std::fs::read_link(&full).map_err(|e| invalid(e.to_string()))?;
            entry.id = repo
                .blob(target.as_os_str().as_encoded_bytes())
                .map_err(map_git_err)?;
            entry.mode = 0o12_0000;
        } else if metadata.is_file() {
            let mut writer = repo.blob_writer(None).map_err(map_git_err)?;
            let mut file = std::fs::File::open(&full).map_err(|e| invalid(e.to_string()))?;
            std::io::copy(&mut file, &mut writer).map_err(|e| invalid(e.to_string()))?;
            entry.id = writer.commit().map_err(map_git_err)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                entry.mode = if metadata.permissions().mode() & 0o111 == 0 {
                    0o10_0644
                } else {
                    0o10_0755
                };
            }
        } else {
            index.remove_path(&relative).map_err(map_git_err)?;
            continue;
        }
        index.add(&entry).map_err(map_git_err)?;
    }
    let work_tree = index.write_tree().map_err(map_git_err)?;
    Ok(Cut {
        head: head.id(),
        branch,
        index_bytes,
        index_tree,
        work_tree,
    })
}

/// Capture without ever writing the live index or refs. Unsupported index
/// shapes fail explicitly. Cancellation may leave unreachable objects only.
///
/// # Errors
/// Rejects unsupported repositories/indices, read failures and detected source changes.
pub fn capture(path: &Path, options: &CaptureOptions) -> Result<Snapshot> {
    capture_checked(path, options, || Ok(()))
}

fn capture_checked(
    path: &Path,
    options: &CaptureOptions,
    between: impl FnOnce() -> Result<()>,
) -> Result<Snapshot> {
    for p in &options.excluded_paths {
        if !p.to_str().is_some_and(safe_relative_path) {
            return Err(invalid("invalid excluded path"));
        }
    }
    let cut = read_cut(path, options)?;
    let repo = Repository::open(path).map_err(map_git_err)?;
    let (index, wip) = write_wip(&repo, cut.head, cut.index_tree, cut.work_tree)?;
    between()?;
    if read_cut(path, options)? != cut {
        return Err(invalid("source changed during capture"));
    }
    let clean = repo.find_commit(cut.head).map_err(map_git_err)?.tree_id() == cut.work_tree
        && cut.work_tree == cut.index_tree;
    Ok(Snapshot {
        object_format: "sha1".into(),
        head: cut.head.to_string(),
        index: index.to_string(),
        wip: (!clean).then(|| wip.to_string()),
        branch: cut.branch,
    })
}

/// Revalidate an earlier capture while still holding the same capture barrier.
///
/// # Errors
/// Rejects missing or invalid snapshot objects and any detected source changes.
pub fn verify_source(path: &Path, snapshot: &Snapshot, options: &CaptureOptions) -> Result<()> {
    let cut = read_cut(path, options)?;
    let repo = Repository::open(path).map_err(map_git_err)?;
    let (head, index, work) = validate(&repo, snapshot)?;
    if (cut.head, cut.index_tree, cut.work_tree, cut.branch)
        != (head, index, work, snapshot.branch.clone())
    {
        return Err(invalid("source changed during capture"));
    }
    Ok(())
}

fn oid(value: &str) -> Result<Oid> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    {
        return Err(invalid("invalid SHA-1 OID"));
    }
    Oid::from_str(value).map_err(map_git_err)
}

fn validate(repo: &Repository, snapshot: &Snapshot) -> Result<(Oid, Oid, Oid)> {
    if snapshot.object_format != "sha1" {
        return Err(invalid("unsupported object format"));
    }
    if let Some(branch) = &snapshot.branch {
        if !git2::Reference::is_valid_name(&format!("refs/heads/{branch}")) {
            return Err(invalid("invalid branch"));
        }
    }
    let head = repo
        .find_commit(oid(&snapshot.head)?)
        .map_err(map_git_err)?;
    let index = repo
        .find_commit(oid(&snapshot.index)?)
        .map_err(map_git_err)?;
    if index.parent_count() != 1 || index.parent_id(0).map_err(map_git_err)? != head.id() {
        return Err(invalid("invalid index anchor"));
    }
    let work = match &snapshot.wip {
        None if index.tree_id() == head.tree_id() => head.tree_id(),
        None => return Err(invalid("missing dirty WIP")),
        Some(wip) => {
            let wip = repo.find_commit(oid(wip)?).map_err(map_git_err)?;
            if wip.parent_count() != 2
                || wip.parent_id(0).map_err(map_git_err)? != head.id()
                || wip.parent_id(1).map_err(map_git_err)? != index.id()
                || !wip.message().is_ok_and(|m| {
                    m.starts_with(WIP_SENTINEL)
                        && parse_index_tree_trailer(m) == Some(index.tree_id())
                })
            {
                return Err(invalid("invalid WIP anchor"));
            }
            wip.tree_id()
        }
    };
    Ok((head.id(), index.tree_id(), work))
}

fn copy_objects(source: &Repository, target: &Repository, roots: &[Oid]) -> Result<()> {
    let source_odb = source.odb().map_err(map_git_err)?;
    let target_odb = target.odb().map_err(map_git_err)?;
    let mut seen = HashSet::new();
    let mut pending = roots.to_vec();
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        let obj = source_odb.read(id).map_err(map_git_err)?;
        match obj.kind() {
            git2::ObjectType::Commit => {
                let commit = source.find_commit(id).map_err(map_git_err)?;
                pending.push(commit.tree_id());
                pending.extend(commit.parent_ids());
            }
            git2::ObjectType::Tree => {
                pending.extend(
                    source
                        .find_tree(id)
                        .map_err(map_git_err)?
                        .iter()
                        .filter(|e| e.filemode() != 0o16_0000)
                        .map(|e| e.id()),
                );
            }
            git2::ObjectType::Blob => {}
            _ => return Err(invalid("unexpected object type")),
        }
        if target_odb
            .write(obj.kind(), obj.data())
            .map_err(map_git_err)?
            != id
        {
            return Err(invalid("object hash mismatch"));
        }
    }
    Ok(())
}

/// Hydrate a NEW checkout from local objects (no network, hooks, filters or
/// alternates). On failure remove the incomplete private checkout. The caller
/// restores parent before child and publishes the complete directory atomically.
///
/// # Errors
/// Rejects invalid/missing objects, existing destinations and filesystem failures.
pub fn restore(source_path: &Path, snapshot: &Snapshot, destination: &Path) -> Result<()> {
    let source = Repository::open(source_path).map_err(map_git_err)?;
    let (head, index_tree, work_tree) = validate(&source, snapshot)?;
    for tree in [
        head,
        oid(&snapshot.index)?,
        snapshot
            .wip
            .as_deref()
            .map(oid)
            .transpose()?
            .unwrap_or(head),
    ] {
        let commit = source.find_commit(tree).map_err(map_git_err)?;
        let mut index = Index::new().map_err(map_git_err)?;
        index
            .read_tree(&commit.tree().map_err(map_git_err)?)
            .map_err(map_git_err)?;
        for entry in index.iter() {
            if excluded(&path_from_bytes(&entry.path), &CaptureOptions::default()) {
                return Err(invalid("runtime-owned path in snapshot"));
            }
        }
    }
    std::fs::create_dir(destination)
        .map_err(|e| invalid(format!("destination must be new: {e}")))?;
    let result = (|| {
        let target = Repository::init(destination).map_err(map_git_err)?;
        let mut roots = vec![head, oid(&snapshot.index)?];
        if let Some(wip) = &snapshot.wip {
            roots.push(oid(wip)?);
        }
        copy_objects(&source, &target, &roots)?;
        let tree = target.find_tree(work_tree).map_err(map_git_err)?;
        let mut options = git2::build::CheckoutBuilder::new();
        options.force().disable_filters(true);
        target
            .checkout_tree(tree.as_object(), Some(&mut options))
            .map_err(map_git_err)?;
        let mut index = target.index().map_err(map_git_err)?;
        index
            .read_tree(&target.find_tree(index_tree).map_err(map_git_err)?)
            .map_err(map_git_err)?;
        index.write().map_err(map_git_err)?;
        if let Some(branch) = &snapshot.branch {
            let reference = format!("refs/heads/{branch}");
            target
                .reference(&reference, head, false, "restore checkpoint")
                .map_err(map_git_err)?;
            target.set_head(&reference).map_err(map_git_err)?;
        } else {
            target.set_head_detached(head).map_err(map_git_err)?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_dir_all(destination);
    }
    result
}

#[cfg(test)]
mod tests;
