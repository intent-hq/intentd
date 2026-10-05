//! Claude 2.1.280 FIt/AK/Epe and Ur/ke/Ht source resolution, supported POSIX
//! subset. Native Git discovery is a filesystem walk, not a Git subprocess.
//! Unknown/symlinked or unverifiable layouts defer instead of guessing a root.
use std::path::{Component, Path, PathBuf};

use serde_json::{json, Value};

use super::DeferredReason;

type Result<T> = std::result::Result<T, DeferredReason>;

pub(super) struct LocalSources {
    pub paths: Vec<PathBuf>,
    pub provenance: Value,
}

fn metadata(path: &Path) -> Result<Option<std::fs::Metadata>> {
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.is_dir() || m.is_file() => Ok(Some(m)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        _ => Err(DeferredReason::PolicyAcquisition),
    }
}

fn real_directory(path: &Path) -> Result<PathBuf> {
    let real = std::fs::canonicalize(path).map_err(|_| DeferredReason::PolicyAcquisition)?;
    if real != path || !metadata(&real)?.is_some_and(|m| m.is_dir()) {
        return Err(DeferredReason::PolicyAcquisition);
    }
    Ok(real)
}

fn owner(path: &Path, optional: bool) -> Result<Value> {
    let Some(meta) = metadata(path)? else {
        return if optional {
            Ok(Value::Null)
        } else {
            Err(DeferredReason::PolicyAcquisition)
        };
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        // SAFETY: geteuid has no preconditions and does not mutate process state.
        let uid = unsafe { libc::geteuid() };
        if meta.uid() != uid {
            return Err(DeferredReason::PolicyAcquisition);
        }
        Ok(json!({"uid":meta.uid(),"mode":meta.mode(),"directory":meta.is_dir()}))
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        Err(DeferredReason::Platform)
    }
}

fn read_link_file(path: &Path) -> Result<String> {
    use std::io::Read as _;
    if !metadata(path)?.is_some_and(|m| m.is_file()) {
        return Err(DeferredReason::PolicyAcquisition);
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW);
    }
    let file = options
        .open(path)
        .map_err(|_| DeferredReason::PolicyAcquisition)?;
    if !file
        .metadata()
        .map_err(|_| DeferredReason::PolicyAcquisition)?
        .is_file()
    {
        return Err(DeferredReason::PolicyAcquisition);
    }
    let mut text = String::new();
    file.take(65537)
        .read_to_string(&mut text)
        .map_err(|_| DeferredReason::PolicyAcquisition)?;
    if text.len() > 65536 {
        return Err(DeferredReason::PolicyAcquisition);
    }
    Ok(text)
}

fn resolve_link(base: &Path, value: &str) -> Result<PathBuf> {
    let value = value.trim();
    if value.is_empty() || value.contains(['\0', '\n', '\r']) {
        return Err(DeferredReason::PolicyAcquisition);
    }
    let joined = base.join(value);
    let mut path = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::RootDir => path.push("/"),
            Component::Normal(part) => path.push(part),
            Component::CurDir => {}
            Component::ParentDir => {
                path.pop();
            }
            Component::Prefix(_) => return Err(DeferredReason::PolicyAcquisition),
        }
    }
    Ok(path)
}

fn canonical_git_root(root: &Path, evidence: &mut Vec<Value>) -> Result<PathBuf> {
    let entry = root.join(".git");
    let meta = metadata(&entry)?.ok_or(DeferredReason::PolicyAcquisition)?;
    evidence.push(json!({"path":entry,"owner":owner(&entry,false)?}));
    if meta.is_dir() {
        return Ok(root.to_owned());
    }
    let text = read_link_file(&entry)?;
    let target = text
        .trim()
        .strip_prefix("gitdir:")
        .ok_or(DeferredReason::PolicyAcquisition)?;
    let gitdir = real_directory(&resolve_link(root, target)?)?;
    let common_path = gitdir.join("commondir");
    evidence
        .push(json!({"path":entry,"content":text,"gitdir":gitdir,"owner":owner(&gitdir,false)?}));
    let Some(_) = metadata(&common_path)? else {
        // Ordinary submodule/separate-git-dir entry, not a linked worktree.
        // Native Ht falls back to the discovered checkout root in this case.
        evidence.push(json!({"path":common_path,"absent":true}));
        return Ok(root.to_owned());
    };
    let common_text = read_link_file(&common_path)?;
    let common = real_directory(&resolve_link(&gitdir, &common_text)?)?;
    if common.file_name().is_none_or(|name| name != ".git")
        || gitdir.parent() != Some(common.join("worktrees").as_path())
    {
        return Err(DeferredReason::PolicyAcquisition);
    }
    let reciprocal = gitdir.join("gitdir");
    let reciprocal_text = read_link_file(&reciprocal)?;
    if resolve_link(&gitdir, &reciprocal_text)? != entry {
        return Err(DeferredReason::PolicyAcquisition);
    }
    let canonical = common
        .parent()
        .ok_or(DeferredReason::PolicyAcquisition)?
        .to_owned();
    evidence.push(json!({"common":common,"owner":owner(&common,false)?,"content":common_text,"reciprocal":reciprocal,"backlink":reciprocal_text}));
    Ok(canonical)
}

pub(super) fn resolve(workspace: &Path, home: &Path) -> Result<LocalSources> {
    let workspace = real_directory(workspace)?;
    let home = real_directory(home)?;
    if workspace.ancestors().count() > 1024 {
        return Err(DeferredReason::PolicyAcquisition);
    }
    let mut evidence = Vec::new();
    let mut git_root = None;
    for ancestor in workspace.ancestors().take(1024) {
        let entry = ancestor.join(".git");
        if metadata(&entry)?.is_some() {
            git_root = Some(ancestor.to_owned());
            break;
        }
    }
    let mut local_root = workspace.clone();
    if let Some(root) = &git_root {
        let canonical = canonical_git_root(root, &mut evidence)?;
        // AK leaves the local consent store at cwd if canonical root is HOME.
        if canonical != workspace && canonical != home {
            evidence.push(json!({"canonical":canonical,"owner":owner(&canonical,false)?,"git":owner(&canonical.join(".git"),false)?,"claude":owner(&canonical.join(".claude"),true)?}));
            local_root = canonical;
        }
    }
    // Epe merges legacy cwd local settings before the canonical local file.
    // The acquisition layer examines every document and defers unsupported
    // projections rather than silently flattening their native precedence.
    let mut paths = vec![
        workspace.join(".claude/settings.json"),
        workspace.join(".claude/settings.local.json"),
    ];
    if local_root != workspace {
        paths.push(local_root.join(".claude/settings.local.json"));
    }
    for root in [&workspace, &local_root] {
        let claude = root.join(".claude");
        if metadata(&claude)?.is_some_and(|m| !m.is_dir()) {
            return Err(DeferredReason::PolicyAcquisition);
        }
        evidence.push(json!({"root":root,"claude":owner(&claude,true)?}));
    }
    Ok(LocalSources {
        paths,
        provenance: json!({"cwd":workspace,"gitRoot":git_root,"localRoot":local_root,"evidence":evidence}),
    })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt as _;

    #[test]
    fn different_owner_is_unresolved_instead_of_assuming_cwd() {
        // SAFETY: geteuid has no preconditions and does not mutate process state.
        let current = unsafe { libc::geteuid() };
        if std::fs::metadata("/").unwrap().uid() != current {
            assert!(matches!(
                owner(Path::new("/"), false),
                Err(DeferredReason::PolicyAcquisition)
            ));
        }
    }
}
