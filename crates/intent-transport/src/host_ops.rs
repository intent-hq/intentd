//! `host.*` host-services: `checkGit` + `listDirectory` + `directoryStatus`
//! + `checkAuggie` (§5.14).
//!
//! These additive methods sit on the existing `host.*` capability-probe surface
//! (alongside `host.status` + `host.openExternal`) and let the FE delegate
//! host-machine probes to the daemon: Git binary detection, repo-folder
//! browsing, directory status (worktree-aware `.git` walk), and auggie binary
//! discovery. They resolve on the daemon host so callers reach the worktrees
//! that actually live there — answered on both UDS and WSS, like the rest of
//! `host.*`.
//!
//! Ported from the Electron FE: `packages/cloudlands-fe/src/shared/main/`
//! `find-binary.ts` (the git binary resolver) and `packages/cloudlands-fe/src/`
//! `features/file/main/file.ipc.ts` (the `file:getDirectoryStatus` handler,
//! including `findParentGitDir` + `expandPath`). The auggie resolver is
//! NOT re-ported: it reuses [`intent_services::auggie_discovery`] (a
//! re-export of `intent_context::discovery`), the canonical port of
//! `auggie-path.ts`. The pure operations accept injected resolvers / a `home`
//! root so they unit-test cleanly with a temp directory.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::{json, Value};

/// Resolves a binary by name to an absolute path on the daemon host. Injected
/// so `check_git` is unit-testable without spawning `which`/`where`.
pub(crate) trait BinaryResolver: Send + Sync {
    fn find(&self, name: &str) -> Option<PathBuf>;
}

/// Captures the version line from a resolved binary (typically by running
/// `<path> --version`). Injected so `check_git`/`check_auggie` are unit-testable
/// without spawning a real subprocess.
pub(crate) trait VersionProbe: Send + Sync {
    fn probe(&self, path: &Path) -> Option<String>;
}

/// Production resolver: search PATH (`which`/`where`) then OS-common dirs.
pub(crate) struct OsBinaryResolver;

impl BinaryResolver for OsBinaryResolver {
    fn find(&self, name: &str) -> Option<PathBuf> {
        find_binary(name)
    }
}

/// Production version probe: run `<path> --version` with a short timeout and
/// return the first non-empty trimmed line of stdout.
pub(crate) struct OsVersionProbe;

impl VersionProbe for OsVersionProbe {
    fn probe(&self, path: &Path) -> Option<String> {
        run_version(path)
    }
}

/// Locate a binary by `name`. Mirrors the FE `findBinary` strategy: ask the OS
/// (`which` on unix, `where` on Windows), then fall back to OS-common
/// directories. Returns the first existing absolute path, or `None`. Internal
/// helper — exposed only to `host.checkGit`.
pub(crate) fn find_binary(name: &str) -> Option<PathBuf> {
    if name.is_empty() {
        return None;
    }
    if let Some(path) = lookup_in_path(name) {
        if path.is_file() || path.is_symlink() {
            return Some(path);
        }
    }
    for dir in common_dirs() {
        let candidate = dir.join(binary_filename(name));
        if candidate.is_file() || candidate.is_symlink() {
            return Some(candidate);
        }
    }
    None
}

/// Run `which`/`where` to consult PATH. Returns the first non-empty trimmed
/// line of stdout as a `PathBuf`.
fn lookup_in_path(name: &str) -> Option<PathBuf> {
    let probe = if cfg!(windows) { "where" } else { "which" };
    let output = Command::new(probe)
        .arg(name)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .map(str::trim)
        .find(|s| !s.is_empty())
        .map(PathBuf::from)
}

/// OS-common directories to scan after PATH (task spec). Mirrors the FE: the
/// Homebrew prefixes first on macOS, then `/usr/*`; `Program Files\Git\{cmd,bin}`
/// on Windows; the standard `/usr/*` tree on other Unix.
fn common_dirs() -> Vec<PathBuf> {
    if cfg!(target_os = "macos") {
        vec![
            PathBuf::from("/opt/homebrew/bin"),
            PathBuf::from("/usr/local/bin"),
            PathBuf::from("/usr/bin"),
        ]
    } else if cfg!(target_os = "windows") {
        vec![
            PathBuf::from(r"C:\Program Files\Git\cmd"),
            PathBuf::from(r"C:\Program Files\Git\bin"),
            PathBuf::from(r"C:\Program Files (x86)\Git\cmd"),
            PathBuf::from(r"C:\Program Files (x86)\Git\bin"),
        ]
    } else {
        vec![
            PathBuf::from("/usr/bin"),
            PathBuf::from("/bin"),
            PathBuf::from("/usr/local/bin"),
        ]
    }
}

/// Append `.exe` on Windows so `dir.join(name)` resolves the real executable
/// when scanning common dirs.
fn binary_filename(name: &str) -> String {
    if cfg!(windows) && !name.to_lowercase().ends_with(".exe") {
        format!("{name}.exe")
    } else {
        name.to_string()
    }
}

/// Run `<path> --version` (5s timeout) and return the first trimmed non-empty
/// line of stdout, or `None` on failure.
fn run_version(path: &Path) -> Option<String> {
    let mut child = Command::new(path)
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(Some(status)) = child.try_wait() {
            if !status.success() {
                return None;
            }
            break;
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            return None;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut buf = String::new();
    if let Some(mut out) = child.stdout.take() {
        use std::io::Read;
        let _ = out.read_to_string(&mut buf);
    }
    buf.lines()
        .map(str::trim)
        .find(|s| !s.is_empty())
        .map(String::from)
}

/// Build the `host.checkGit` result. `available:false` (never an RPC error)
/// when not found or when the version probe fails. Pure: takes injected
/// resolver + probe so tests don't hit the host.
pub(crate) fn check_git_with(resolver: &dyn BinaryResolver, probe: &dyn VersionProbe) -> Value {
    build_check_result(resolver.find("git"), probe)
}

/// Production `check_git` — uses the real resolver + version probe.
pub(crate) fn check_git() -> Value {
    check_git_with(&OsBinaryResolver, &OsVersionProbe)
}

/// Build the `host.checkAuggie` result, given a pre-resolved candidate path
/// (the caller has already applied the user-settings → discovery precedence).
/// `available:false` when `resolved` is `None` or the version probe fails.
pub(crate) fn check_auggie_with(resolved: Option<PathBuf>, probe: &dyn VersionProbe) -> Value {
    build_check_result(resolved, probe)
}

/// Production `check_auggie` — uses the canonical resolver from
/// `intent_services::auggie_discovery` (re-export of `intent_context::`
/// `discovery::find_auggie`) and the real version probe. Settings precedence
/// is applied by the caller via [`resolve_auggie_path`].
pub(crate) fn check_auggie(configured: Option<&str>) -> Value {
    check_auggie_with(resolve_auggie_path(configured), &OsVersionProbe)
}

/// Apply the auggie path precedence: (1) user-configured `configured` path
/// when set and existing; else (2) `intent_services::auggie_discovery::`
/// `find_auggie()`. Pure (only consults the filesystem) so the precedence is
/// testable without touching settings.
pub(crate) fn resolve_auggie_path(configured: Option<&str>) -> Option<PathBuf> {
    if let Some(s) = configured.map(str::trim).filter(|s| !s.is_empty()) {
        let p = PathBuf::from(s);
        if p.is_file() || p.is_symlink() {
            return Some(p);
        }
    }
    intent_services::auggie_discovery::find_auggie()
}

/// Common `{ available, version?, path? }` body builder shared by `checkGit`
/// and `checkAuggie`. `path` is `None` ⇒ `available:false`. A successful probe
/// includes the trimmed `version` + resolver `path`.
fn build_check_result(path: Option<PathBuf>, probe: &dyn VersionProbe) -> Value {
    let Some(path) = path else {
        return json!({ "available": false });
    };
    let Some(version) = probe.probe(&path) else {
        return json!({ "available": false });
    };
    json!({
        "available": true,
        "version": version,
        "path": path.to_string_lossy(),
    })
}

/// Expand `~` / `~/...` to `home` (mirrors the FE `expandPath`). Anything else
/// passes through verbatim.
pub(crate) fn expand_path(input: &str, home: &Path) -> PathBuf {
    if input == "~" {
        return home.to_path_buf();
    }
    if let Some(rest) = input.strip_prefix("~/") {
        return home.join(rest);
    }
    PathBuf::from(input)
}

/// Walk up from `start` looking for a `.git` directory OR a worktree `.git`
/// file (whose content starts with `gitdir:`). Returns the git-root path or
/// `None` when none is found before the filesystem root. Mirrors the FE
/// `findParentGitDir`.
pub(crate) fn find_parent_git_dir(start: &Path) -> Option<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        if has_git_marker(&current) {
            return Some(current);
        }
        let parent = current.parent()?;
        if parent == current || parent.as_os_str().is_empty() {
            return None;
        }
        current = parent.to_path_buf();
    }
}

/// `true` if `dir/.git` is a directory, or a file whose first non-empty line
/// starts with `gitdir:` (the git-worktree pointer).
fn has_git_marker(dir: &Path) -> bool {
    let git = dir.join(".git");
    match std::fs::metadata(&git) {
        Ok(meta) if meta.is_dir() => true,
        Ok(meta) if meta.is_file() => std::fs::read_to_string(&git)
            .map(|s| s.trim_start().starts_with("gitdir:"))
            .unwrap_or(false),
        _ => false,
    }
}

/// Build the `host.listDirectory` result. `path` defaults to `home` when
/// `None`/empty. `parent` is `null` at the filesystem root. Entries include
/// hidden files (the FE filters), sorted directories-first then by name.
/// Returns the error message (mapped to `-32603` by the caller) on IO errors.
pub(crate) fn list_directory_with(path: Option<&str>, home: &Path) -> Result<Value, String> {
    let target = match path {
        Some(p) if !p.is_empty() => expand_path(p, home),
        _ => home.to_path_buf(),
    };
    let read = std::fs::read_dir(&target).map_err(|e| format!("{}: {e}", target.display()))?;

    let mut entries: Vec<(String, PathBuf, bool, bool)> = Vec::new();
    for entry in read.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let entry_path = entry.path();
        let is_dir = entry
            .file_type()
            .map(|t| t.is_dir())
            .unwrap_or_else(|_| entry_path.is_dir());
        let is_git_repo = is_dir && has_git_marker(&entry_path);
        entries.push((name, entry_path, is_dir, is_git_repo));
    }
    entries.sort_by(|a, b| match (a.2, b.2) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.0.cmp(&b.0),
    });
    let entries_json: Vec<Value> = entries
        .into_iter()
        .map(|(name, p, is_dir, is_git_repo)| {
            json!({
                "name": name,
                "path": p.to_string_lossy(),
                "isDirectory": is_dir,
                "isGitRepo": is_git_repo,
            })
        })
        .collect();

    let parent = target
        .parent()
        .filter(|p| !p.as_os_str().is_empty() && *p != target)
        .map(Path::to_path_buf);
    Ok(json!({
        "path": target.to_string_lossy(),
        "parent": parent
            .map(|p| Value::String(p.to_string_lossy().into_owned()))
            .unwrap_or(Value::Null),
        "home": home.to_string_lossy(),
        "entries": entries_json,
    }))
}

/// Production `list_directory` — resolves `home` from the environment.
pub(crate) fn list_directory(path: Option<&str>) -> Result<Value, String> {
    list_directory_with(path, &home_dir())
}

/// Build the `host.directoryStatus` result. Mirrors the FE
/// `file:getDirectoryStatus` handler — worktree-aware `.git` detection, a
/// parent-git-root walk, and a `relativePathFromGitRoot` only when the path is
/// a subdirectory of an outer repo.
pub(crate) fn directory_status_with(path: &str, home: &Path) -> Value {
    let expanded = expand_path(path, home);
    let mut exists = false;
    let mut is_directory = false;
    let mut is_empty = true;
    let mut is_git_repo = false;
    let mut parent_git_root: Option<PathBuf> = None;
    let mut relative_path_from_git_root: Option<String> = None;
    let mut is_subdirectory_of_git_repo = false;

    if let Ok(meta) = std::fs::metadata(&expanded) {
        exists = true;
        is_directory = meta.is_dir();
        if is_directory {
            if let Ok(mut iter) = std::fs::read_dir(&expanded) {
                is_empty = iter.next().is_none();
            } else {
                is_empty = true;
            }
            is_git_repo = has_git_marker(&expanded);
            if !is_git_repo {
                if let Some(root) = find_parent_git_dir(&expanded) {
                    is_subdirectory_of_git_repo = true;
                    relative_path_from_git_root = expanded
                        .strip_prefix(&root)
                        .ok()
                        .map(|p| p.to_string_lossy().into_owned());
                    parent_git_root = Some(root);
                }
            }
        }
    }

    let mut result = json!({
        "exists": exists,
        "isDirectory": is_directory,
        "isEmpty": is_empty,
        "isGitRepo": is_git_repo,
        "isSubdirectoryOfGitRepo": is_subdirectory_of_git_repo,
        "path": expanded.to_string_lossy(),
    });
    if let Some(root) = parent_git_root {
        result["parentGitRoot"] = json!(root.to_string_lossy());
    }
    if let Some(rel) = relative_path_from_git_root {
        result["relativePathFromGitRoot"] = json!(rel);
    }
    result
}

/// Production `directory_status` — resolves `home` from the environment.
pub(crate) fn directory_status(path: &str) -> Value {
    directory_status_with(path, &home_dir())
}

/// Resolve the host home directory. Falls back to `/` so a missing `HOME`
/// degrades gracefully instead of panicking.
fn home_dir() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home);
    }
    #[cfg(windows)]
    if let Some(profile) = std::env::var_os("USERPROFILE") {
        return PathBuf::from(profile);
    }
    PathBuf::from("/")
}

#[cfg(test)]
mod tests;
