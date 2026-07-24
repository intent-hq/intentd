//! `host.*` host-services: `checkGit` + `listDirectory` + `directoryStatus`
//!   + `checkAuggie` + `findBinary` + `toolAvailability` + `env` + `findApp`
//!   + `listInstalledEditors` (§5.14).
//!
//! These additive methods sit on the existing `host.*` capability-probe surface
//! (alongside `host.status` + `host.openExternal`) and let the FE delegate
//! host-machine probes to the daemon: Git binary detection, repo-folder
//! browsing, directory status (worktree-aware `.git` walk), auggie binary
//! discovery, generic binary resolution (`findBinary`), a batch tool-availability
//! probe (`toolAvailability`), the daemon's PATH/environment (`env`), macOS
//! `.app` bundle lookup (`findApp`), and a cross-platform catalog of installed
//! editors/terminals (`listInstalledEditors`). They resolve on the daemon host
//! so callers reach the binaries, app bundles, and worktrees that actually live
//! there — answered on both UDS and WSS, like the rest of `host.*`. The `env`
//! probe is secret-safe: it returns only variable *names* plus the non-secret
//! PATH/SHELL/HOME values, never arbitrary variable values. `findApp` /
//! `listInstalledEditors` are secret-safe by construction: only app names + paths
//! cross the wire.
//!
//! Ported from the Electron FE: `packages/cloudlands-fe/src/shared/main/`
//! `find-binary.ts` (the git binary resolver) and `packages/cloudlands-fe/src/`
//! `features/file/main/file.ipc.ts` (the `file:getDirectoryStatus` handler,
//! including `findParentGitDir` + `expandPath`). The auggie resolver is
//! NOT re-ported: it reuses [`intent_services::auggie_discovery`] (a
//! re-export of `intent_context::discovery`), the canonical port of
//! `auggie-path.ts`. The pure operations accept injected resolvers / a `home`
//! root so they unit-test cleanly with a temp directory.

use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use intent_core::path_utils;
use serde_json::{json, Map, Value};

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
/// helper — used by `host.checkGit`.
pub(crate) fn find_binary(name: &str) -> Option<PathBuf> {
    resolve_binary_path(name, &[])
}

/// Locate `name` on the daemon host, mirroring the FE `findBinary` lookup
/// order: PATH (`which`/`where`) → caller-supplied `common_paths` (checked
/// verbatim, like the FE `commonPaths` option) → OS-common directories.
/// Returns the first existing absolute path, or `None`.
pub(crate) fn resolve_binary_path(name: &str, common_paths: &[String]) -> Option<PathBuf> {
    if name.is_empty() {
        return None;
    }
    // 1. PATH which (lookup_in_path uses which/where)
    if let Some(path) = lookup_in_path(name) {
        if path.is_file() || path.is_symlink() {
            return Some(path);
        }
    }
    // 2. Caller-supplied common_paths hints
    for candidate in common_paths {
        let candidate = PathBuf::from(candidate);
        if candidate.is_file() || candidate.is_symlink() {
            return Some(candidate);
        }
    }
    // 3. Enriched tool directories (hardcoded + login-shell PATH)
    for dir in path_utils::enriched_tool_dirs() {
        let candidate = dir.join(binary_filename(name));
        if candidate.is_file() || candidate.is_symlink() {
            return Some(candidate);
        }
    }
    // 4. Common OS directories (fallback)
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
///
/// Enriches the PATH environment variable to include the binary's parent
/// directory plus enhanced path directories, ensuring scripts with shebangs
/// like `#!/usr/bin/env node` can resolve their interpreter.
fn run_version(path: &Path) -> Option<String> {
    let enriched_path = build_enriched_path_for_binary(path);
    run_version_with(path, &enriched_path)
}

/// Run `<path> --version` with an explicit PATH environment variable.
/// Exposed for testing — production code should use `run_version()`.
fn run_version_with(path: &Path, path_env: &OsString) -> Option<String> {
    let mut child = Command::new(path)
        .arg("--version")
        .env("PATH", path_env)
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

/// Build an enriched PATH for executing a binary, with correct precedence:
/// 1. Binary's parent directory (for co-located dependencies like node in nvm)
/// 2. Enriched tool directories (node, nvm, homebrew, volta, asdf, etc.)
/// 3. Inherited PATH (fallback)
fn build_enriched_path_for_binary(binary_path: &Path) -> OsString {
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();

    // 1. Binary's parent directory (highest priority for co-located dependencies)
    if binary_path.is_absolute() {
        if let Some(parent) = binary_path.parent() {
            path_utils::push_dir(&mut dirs, &mut seen, parent.to_path_buf());
        }
    }

    // 2. Enriched tool directories (node, nvm, homebrew, volta, asdf, etc.)
    for dir in path_utils::enriched_tool_dirs() {
        path_utils::push_dir(&mut dirs, &mut seen, dir);
    }

    // 3. Inherited PATH (lowest priority)
    if let Some(path) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path) {
            path_utils::push_dir(&mut dirs, &mut seen, dir);
        }
    }

    // Join paths, fall back to inherited PATH if joining fails
    std::env::join_paths(&dirs).unwrap_or_else(|_| std::env::var_os("PATH").unwrap_or_default())
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

/// Mirror the FE `SAFE_BINARY_NAME` guard (`/^[a-zA-Z0-9._-]+$/`): reject names
/// with path separators, shell metacharacters, or whitespace before they reach
/// `which`/`where`. An unsafe name resolves to `available:false`, never an error.
pub(crate) fn is_safe_binary_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Build the `host.findBinary` result `{ available, path?, version? }`. Unlike
/// `checkGit`/`checkAuggie`, a binary that resolves but does not answer
/// `--version` is still `available:true` (the `version` is best-effort/optional).
fn build_find_result(path: Option<PathBuf>, probe: &dyn VersionProbe) -> Value {
    let Some(path) = path else {
        return json!({ "available": false });
    };
    let mut result = json!({
        "available": true,
        "path": path.to_string_lossy(),
    });
    if let Some(version) = probe.probe(&path) {
        result["version"] = json!(version);
    }
    result
}

/// Production `host.findBinary` — resolve `name` (honouring the optional caller
/// `common_paths`) and best-effort version-probe it. Rejects unsafe names with
/// `available:false`.
pub(crate) fn find_binary_op(name: &str, common_paths: &[String]) -> Value {
    if !is_safe_binary_name(name) {
        return json!({ "available": false });
    }
    build_find_result(resolve_binary_path(name, common_paths), &OsVersionProbe)
}

/// The default tool set probed by `host.toolAvailability` when the caller does
/// not supply an explicit `tools` list: the ACP agent CLIs the FE detects, the
/// `git` binary, and the `code` editor launcher.
pub(crate) const DEFAULT_TOOLS: &[&str] = &[
    "claude",
    "claude-agent-acp",
    "opencode",
    "codex",
    "codex-acp",
    "cortex",
    "grok",
    "git",
    "code",
];

/// Tool-specific `common_paths` hints for [`find_binary_op`]. grok's native
/// installer puts the binary at `~/.grok/bin/grok` without necessarily adding
/// it to PATH, so the availability probe must check there as a fallback
/// (PATH still wins in `resolve_binary_path`; provider discovery, which
/// spawns the binary, is what prefers the native location — see
/// `intent_providers::discover`).
fn tool_common_paths(name: &str) -> Vec<String> {
    if name != "grok" {
        return Vec::new();
    }
    let bin = home_dir().join(".grok").join("bin");
    let mut paths = vec![bin.join("grok")];
    if cfg!(windows) {
        paths.push(bin.join("grok.exe"));
        paths.push(bin.join("grok.cmd"));
    }
    paths.into_iter().map(|p| p.display().to_string()).collect()
}

/// Build the `host.toolAvailability` result `{ tools: { <name>: { available,
/// path?, version? } } }`. Each tool is resolved via [`find_binary_op`]; an
/// empty / absent `tools` list falls back to [`DEFAULT_TOOLS`].
pub(crate) fn tool_availability_op(tools: Option<Vec<String>>) -> Value {
    let names: Vec<String> = match tools {
        Some(t) if !t.is_empty() => t,
        _ => DEFAULT_TOOLS.iter().map(|s| s.to_string()).collect(),
    };
    let mut map = Map::new();
    for name in names {
        let result = find_binary_op(&name, &tool_common_paths(&name));
        map.insert(name, result);
    }
    json!({ "tools": Value::Object(map) })
}

/// The PATH separator for the host platform.
fn path_separator() -> char {
    if cfg!(windows) {
        ';'
    } else {
        ':'
    }
}

/// OS-common directories that must always be on PATH for binary resolution
/// (mirrors the FE `getEssentialSystemPaths`). Merged into the host PATH to
/// form the "enhanced" PATH the daemon uses to locate tools.
fn essential_system_paths() -> Vec<String> {
    if cfg!(target_os = "windows") {
        let system_root = std::env::var("SystemRoot").unwrap_or_else(|_| r"C:\Windows".to_string());
        let mut paths = vec![
            format!(r"{system_root}\System32"),
            format!(r"{system_root}\System32\wbem"),
            r"C:\Program Files\Git\cmd".to_string(),
            r"C:\Program Files (x86)\Git\cmd".to_string(),
        ];
        if let Ok(local_app_data) = std::env::var("LOCALAPPDATA") {
            paths.push(format!(r"{local_app_data}\Programs\Git\cmd"));
        }
        if let Ok(user_profile) = std::env::var("USERPROFILE") {
            paths.push(format!(r"{user_profile}\scoop\shims"));
        }
        paths.push(r"C:\ProgramData\chocolatey\bin".to_string());
        paths
    } else {
        vec![
            "/bin".to_string(),
            "/usr/bin".to_string(),
            "/usr/local/bin".to_string(),
            "/opt/homebrew/bin".to_string(),
            "/usr/sbin".to_string(),
            "/sbin".to_string(),
        ]
    }
}

/// Compute the daemon's canonical enhanced PATH: inherited entries first, then
/// common tool locations, the login-shell PATH, and platform essentials,
/// de-duplicated in order.
pub(crate) fn enhanced_path_from(current_path: &str) -> String {
    let mut dirs = Vec::new();
    let mut seen = HashSet::new();
    for dir in std::env::split_paths(current_path) {
        path_utils::push_dir(&mut dirs, &mut seen, dir);
    }
    for dir in path_utils::enriched_tool_dirs() {
        path_utils::push_dir(&mut dirs, &mut seen, dir);
    }
    for dir in essential_system_paths() {
        path_utils::push_dir(&mut dirs, &mut seen, PathBuf::from(dir));
    }
    std::env::join_paths(dirs)
        .unwrap_or_else(|_| OsString::from(current_path))
        .to_string_lossy()
        .into_owned()
}

/// Build the `host.env` result. Returns the host PATH (raw + split `entries`),
/// the "enhanced" PATH the daemon uses for binary resolution, the login shell,
/// the home directory, and the SORTED NAMES of every environment variable.
/// SECRET SAFETY: only the PATH / SHELL / HOME values (non-secret by
/// construction) cross the wire verbatim — all other variables are reported by
/// name only, so no secret values are ever exposed.
pub(crate) fn build_env_json(
    raw_path: &str,
    shell: &str,
    home: &Path,
    var_names: Vec<String>,
) -> Value {
    let sep = path_separator();
    let entries: Vec<&str> = raw_path.split(sep).filter(|s| !s.is_empty()).collect();
    json!({
        "path": raw_path,
        "pathEntries": entries,
        "enhancedPath": enhanced_path_from(raw_path),
        "shell": shell,
        "home": home.to_string_lossy(),
        "varNames": var_names,
    })
}

/// Production `host.env` — reads the daemon's actual environment. See
/// [`build_env_json`] for the secret-safety contract (names only, no values).
/// The login shell comes from the shared [`path_utils::login_shell`] resolver
/// (`SHELL` env → user database → `/bin/zsh` on macOS), so the reported shell
/// always matches the one login-shell PATH enrichment uses. A set `SHELL` env
/// var is honoured on every platform, including Windows (e.g. MSYS/Git Bash),
/// as it was before the resolvers were unified; when no shell can be resolved
/// (Windows without `SHELL`, or non-macOS unix without a user-db entry) the
/// field is empty and enrichment is skipped.
pub(crate) fn env_probe() -> Value {
    let raw_path = std::env::var("PATH").unwrap_or_default();
    let shell = path_utils::login_shell().unwrap_or_default();
    let home = home_dir();
    let mut var_names: Vec<String> = std::env::vars_os()
        .filter_map(|(k, _)| k.into_string().ok())
        .collect();
    var_names.sort();
    var_names.dedup();
    build_env_json(&raw_path, &shell, &home, var_names)
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

/// A known editor / terminal / file-manager the `host.listInstalledEditors`
/// probe enumerates. Mirrors the FE `EDITOR_REGISTRY` (only the fields the BE
/// needs for detection — display strings stay on the FE, keyed by `id`).
pub(crate) struct KnownEditor {
    pub id: &'static str,
    /// macOS `<name>.app` bundle name (e.g. `Visual Studio Code`).
    pub app_name: &'static str,
    /// Linux native binary names (e.g. `code`, `code-oss`).
    pub linux_binaries: &'static [&'static str],
    /// Linux flatpak application IDs (e.g. `com.visualstudio.code`).
    pub flatpak_ids: &'static [&'static str],
    /// Windows binaries on PATH (e.g. `code`, `code.cmd`).
    pub win_binaries: &'static [&'static str],
    /// `true` ⇒ skip on non-macOS hosts.
    pub macos_only: bool,
    /// `true` ⇒ skip on non-Windows hosts.
    pub win32_only: bool,
}

/// Built-in catalog of editors / terminals / file-managers the host can detect.
/// Stays in sync with `cloudlands-fe/src/shared/editors/editor-registry.ts` —
/// when an editor is added on the FE, mirror only the detection fields here.
pub(crate) const KNOWN_EDITORS: &[KnownEditor] = &[
    KnownEditor {
        id: "finder",
        app_name: "Finder",
        linux_binaries: &["nautilus", "dolphin", "thunar", "nemo", "pcmanfm"],
        flatpak_ids: &[],
        win_binaries: &["explorer"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "vscode",
        app_name: "Visual Studio Code",
        linux_binaries: &["code", "code-oss"],
        flatpak_ids: &["com.visualstudio.code", "com.visualstudio.code.oss"],
        win_binaries: &["code", "code.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "cursor",
        app_name: "Cursor",
        linux_binaries: &["cursor"],
        flatpak_ids: &["com.cursor.Cursor"],
        win_binaries: &["cursor", "cursor.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "zed",
        app_name: "Zed",
        linux_binaries: &["zed", "zeditor"],
        flatpak_ids: &["dev.zed.Zed"],
        win_binaries: &["zed", "zed.exe"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "xcode",
        app_name: "Xcode",
        linux_binaries: &[],
        flatpak_ids: &[],
        win_binaries: &[],
        macos_only: true,
        win32_only: false,
    },
    KnownEditor {
        id: "intellij",
        app_name: "IntelliJ IDEA",
        linux_binaries: &["idea", "intellij-idea-ultimate"],
        flatpak_ids: &["com.jetbrains.IntelliJ-IDEA-Ultimate"],
        win_binaries: &["idea64.exe", "idea.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "intellij-ce",
        app_name: "IntelliJ IDEA CE",
        linux_binaries: &["idea-ce", "intellij-idea-community"],
        flatpak_ids: &["com.jetbrains.IntelliJ-IDEA-Community"],
        win_binaries: &["idea64.exe", "idea.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "webstorm",
        app_name: "WebStorm",
        linux_binaries: &["webstorm"],
        flatpak_ids: &["com.jetbrains.WebStorm"],
        win_binaries: &["webstorm64.exe", "webstorm.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "pycharm",
        app_name: "PyCharm",
        linux_binaries: &["pycharm", "pycharm-professional"],
        flatpak_ids: &["com.jetbrains.PyCharm-Professional"],
        win_binaries: &["pycharm64.exe", "pycharm.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "pycharm-ce",
        app_name: "PyCharm CE",
        linux_binaries: &["pycharm-community"],
        flatpak_ids: &["com.jetbrains.PyCharm-Community"],
        win_binaries: &["pycharm64.exe", "pycharm.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "goland",
        app_name: "GoLand",
        linux_binaries: &["goland"],
        flatpak_ids: &["com.jetbrains.GoLand"],
        win_binaries: &["goland64.exe", "goland.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "phpstorm",
        app_name: "PhpStorm",
        linux_binaries: &["phpstorm"],
        flatpak_ids: &["com.jetbrains.PhpStorm"],
        win_binaries: &["phpstorm64.exe", "phpstorm.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "rubymine",
        app_name: "RubyMine",
        linux_binaries: &["rubymine"],
        flatpak_ids: &["com.jetbrains.RubyMine"],
        win_binaries: &["rubymine64.exe", "rubymine.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "clion",
        app_name: "CLion",
        linux_binaries: &["clion"],
        flatpak_ids: &["com.jetbrains.CLion"],
        win_binaries: &["clion64.exe", "clion.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "rider",
        app_name: "Rider",
        linux_binaries: &["rider"],
        flatpak_ids: &["com.jetbrains.Rider"],
        win_binaries: &["rider64.exe", "rider.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "datagrip",
        app_name: "DataGrip",
        linux_binaries: &["datagrip"],
        flatpak_ids: &["com.jetbrains.DataGrip"],
        win_binaries: &["datagrip64.exe", "datagrip.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "android-studio",
        app_name: "Android Studio",
        linux_binaries: &["android-studio", "studio"],
        flatpak_ids: &["com.google.AndroidStudio"],
        win_binaries: &["studio64.exe", "studio.cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "sublime",
        app_name: "Sublime Text",
        linux_binaries: &["subl", "sublime_text"],
        flatpak_ids: &["com.sublimetext.three"],
        win_binaries: &["subl", "subl.exe"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "nova",
        app_name: "Nova",
        linux_binaries: &[],
        flatpak_ids: &[],
        win_binaries: &[],
        macos_only: true,
        win32_only: false,
    },
    KnownEditor {
        id: "textmate",
        app_name: "TextMate",
        linux_binaries: &[],
        flatpak_ids: &[],
        win_binaries: &[],
        macos_only: true,
        win32_only: false,
    },
    KnownEditor {
        id: "warp",
        app_name: "Warp",
        linux_binaries: &["warp-terminal", "warp"],
        flatpak_ids: &[],
        win_binaries: &["warp", "warp.exe"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "ghostty",
        app_name: "Ghostty",
        linux_binaries: &["ghostty"],
        flatpak_ids: &["com.mitchellh.ghostty"],
        win_binaries: &[],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "iterm",
        app_name: "iTerm",
        linux_binaries: &[],
        flatpak_ids: &[],
        win_binaries: &[],
        macos_only: true,
        win32_only: false,
    },
    KnownEditor {
        id: "kitty",
        app_name: "kitty",
        linux_binaries: &["kitty"],
        flatpak_ids: &["net.kovidgoyal.kitty"],
        win_binaries: &[],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "alacritty",
        app_name: "Alacritty",
        linux_binaries: &["alacritty"],
        flatpak_ids: &["org.alacritty.Alacritty"],
        win_binaries: &["alacritty", "alacritty.exe"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "hyper",
        app_name: "Hyper",
        linux_binaries: &["hyper"],
        flatpak_ids: &["co.zeit.Hyper"],
        win_binaries: &["hyper", "hyper.exe"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "terminal",
        app_name: "Terminal",
        linux_binaries: &["gnome-terminal", "konsole", "xfce4-terminal", "xterm"],
        flatpak_ids: &[],
        win_binaries: &["cmd"],
        macos_only: false,
        win32_only: false,
    },
    KnownEditor {
        id: "powershell",
        app_name: "PowerShell",
        linux_binaries: &[],
        flatpak_ids: &[],
        win_binaries: &["pwsh", "powershell"],
        macos_only: false,
        win32_only: true,
    },
    KnownEditor {
        id: "windows-terminal",
        app_name: "Windows Terminal",
        linux_binaries: &[],
        flatpak_ids: &[],
        win_binaries: &["wt"],
        macos_only: false,
        win32_only: true,
    },
];

/// Probes for installed Flatpak applications. Injected so detection is
/// unit-testable without spawning `flatpak`.
pub(crate) trait FlatpakProbe: Send + Sync {
    /// Return the set of installed flatpak application IDs (lowercased), or
    /// `None` when flatpak is unavailable / errors out.
    fn list_installed(&self) -> Option<std::collections::HashSet<String>>;
}

/// Production flatpak probe: runs `flatpak list --app --columns=application`
/// with a short timeout and returns the parsed set of installed app IDs.
pub(crate) struct OsFlatpakProbe;

impl FlatpakProbe for OsFlatpakProbe {
    fn list_installed(&self) -> Option<std::collections::HashSet<String>> {
        run_flatpak_list()
    }
}

/// Run `flatpak list --app --columns=application` with a 5s wall timeout and
/// return the parsed set of app IDs. `None` when flatpak is missing/errors.
fn run_flatpak_list() -> Option<std::collections::HashSet<String>> {
    let mut child = Command::new("flatpak")
        .args(["list", "--app", "--columns=application"])
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
    Some(
        buf.lines()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
    )
}

/// Guard for the `host.findApp` `name` parameter. Allows spaces (macOS app
/// names like "Visual Studio Code" contain them) but rejects path separators,
/// `..`, leading dots, and NULs so the value can't escape its parent directory.
pub(crate) fn is_safe_app_name(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('/')
        && !name.contains('\\')
        && !name.contains("..")
        && !name.starts_with('.')
        && !name.contains('\0')
}

/// macOS application search directories: `/Applications` then `~/Applications`,
/// mirroring the FE `isAppInstalledMacOS` lookup.
pub(crate) fn macos_app_dirs(home: &Path) -> Vec<PathBuf> {
    vec![PathBuf::from("/Applications"), home.join("Applications")]
}

/// Probe macOS `.app` bundles in `dirs` for `<name>.app`. Returns the first
/// existing bundle path, or `None`. Pure — the dirs are injected so tests can
/// fixture a fake `/Applications`.
pub(crate) fn find_macos_app_bundle(name: &str, dirs: &[PathBuf]) -> Option<PathBuf> {
    if !is_safe_app_name(name) {
        return None;
    }
    let leaf = format!("{name}.app");
    for dir in dirs {
        let candidate = dir.join(&leaf);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

/// Build the `host.findApp` result. On macOS searches the `.app` bundle dirs;
/// elsewhere returns `installed:false` (cross-platform editor detection lives
/// on `host.listInstalledEditors`). An unsafe `name` is also `installed:false`
/// (never an RPC error). Pure: injected dirs + platform flag.
pub(crate) fn find_app_with(name: &str, macos_dirs: &[PathBuf], is_macos: bool) -> Value {
    if !is_macos || !is_safe_app_name(name) {
        return json!({ "installed": false });
    }
    match find_macos_app_bundle(name, macos_dirs) {
        Some(path) => json!({
            "installed": true,
            "path": path.to_string_lossy(),
            "source": "macAppBundle",
        }),
        None => json!({ "installed": false }),
    }
}

/// Production `host.findApp` — resolves `home` + the platform from the
/// environment; honours the same macOS `.app` bundle lookup the FE used.
pub(crate) fn find_app_op(name: &str) -> Value {
    let home = home_dir();
    find_app_with(name, &macos_app_dirs(&home), cfg!(target_os = "macos"))
}

/// Detect a single editor's install state on macOS (`.app` bundle lookup).
fn detect_editor_macos(editor: &KnownEditor, dirs: &[PathBuf]) -> Value {
    match find_macos_app_bundle(editor.app_name, dirs) {
        Some(path) => json!({
            "id": editor.id,
            "installed": true,
            "path": path.to_string_lossy(),
            "source": "macAppBundle",
        }),
        None => json!({ "id": editor.id, "installed": false }),
    }
}

/// Detect a single editor's install state on Linux: native binary first, then
/// flatpak ID. `binary_resolver` is reused so injected stubs work in tests.
fn detect_editor_linux(
    editor: &KnownEditor,
    binary_resolver: &dyn BinaryResolver,
    flatpak_installed: &std::collections::HashSet<String>,
) -> Value {
    for binary in editor.linux_binaries {
        if let Some(path) = binary_resolver.find(binary) {
            return json!({
                "id": editor.id,
                "installed": true,
                "path": path.to_string_lossy(),
                "source": "binary",
            });
        }
    }
    for flatpak_id in editor.flatpak_ids {
        if flatpak_installed.contains(*flatpak_id) {
            return json!({
                "id": editor.id,
                "installed": true,
                "flatpakId": flatpak_id,
                "source": "flatpak",
            });
        }
    }
    json!({ "id": editor.id, "installed": false })
}

/// Detect a single editor's install state on Windows: probe each candidate
/// binary in turn via the resolver.
fn detect_editor_windows(editor: &KnownEditor, binary_resolver: &dyn BinaryResolver) -> Value {
    for binary in editor.win_binaries {
        if let Some(path) = binary_resolver.find(binary) {
            return json!({
                "id": editor.id,
                "installed": true,
                "path": path.to_string_lossy(),
                "source": "binary",
            });
        }
    }
    json!({ "id": editor.id, "installed": false })
}

/// Which platform `list_installed_editors_with` is detecting against. Carried
/// as an enum so tests can pin a platform regardless of the runtime host.
#[derive(Clone, Copy)]
pub(crate) enum EditorPlatform {
    Macos,
    Linux,
    Windows,
}

impl EditorPlatform {
    pub(crate) fn current() -> Self {
        if cfg!(target_os = "macos") {
            EditorPlatform::Macos
        } else if cfg!(target_os = "windows") {
            EditorPlatform::Windows
        } else {
            EditorPlatform::Linux
        }
    }
}

/// Build the `host.listInstalledEditors` result for an injected platform +
/// resolvers. `macos_only`/`win32_only` editors are skipped on other platforms;
/// every remaining entry reports its detection result.
pub(crate) fn list_installed_editors_with(
    platform: EditorPlatform,
    macos_dirs: &[PathBuf],
    binary_resolver: &dyn BinaryResolver,
    flatpak: &dyn FlatpakProbe,
) -> Value {
    let flatpak_installed = match platform {
        EditorPlatform::Linux => flatpak.list_installed().unwrap_or_default(),
        _ => std::collections::HashSet::new(),
    };
    let mut editors: Vec<Value> = Vec::with_capacity(KNOWN_EDITORS.len());
    for editor in KNOWN_EDITORS {
        let applies = match platform {
            EditorPlatform::Macos => !editor.win32_only,
            EditorPlatform::Windows => !editor.macos_only,
            EditorPlatform::Linux => !editor.macos_only && !editor.win32_only,
        };
        if !applies {
            continue;
        }
        let entry = match platform {
            EditorPlatform::Macos => detect_editor_macos(editor, macos_dirs),
            EditorPlatform::Linux => {
                detect_editor_linux(editor, binary_resolver, &flatpak_installed)
            }
            EditorPlatform::Windows => detect_editor_windows(editor, binary_resolver),
        };
        editors.push(entry);
    }
    json!({ "editors": editors })
}

/// Production `host.listInstalledEditors` — wires in the real platform +
/// resolvers + `home`-derived macOS app dirs.
pub(crate) fn list_installed_editors_op() -> Value {
    let home = home_dir();
    list_installed_editors_with(
        EditorPlatform::current(),
        &macos_app_dirs(&home),
        &OsBinaryResolver,
        &OsFlatpakProbe,
    )
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

/// Probe provider discovery: returns configured providers + npx availability.
/// Runs on a blocking thread to keep the async runtime free. Delegates to
/// intent-services to preserve layering (intent-transport depends only on
/// intent-services, never on intent-providers directly).
pub(crate) fn provider_discovery_op() -> Value {
    intent_services::discover_providers_with_npx()
}

#[cfg(test)]
mod tests;
