//! Auggie binary discovery + enhanced PATH construction (§8.2).
//!
//! Ports `auggie-path.ts` (`getEnhancedPath`, `findAuggiePathAsync`) and the
//! exec-PATH helper from `execute-auggie-command.ts` (`getAuggieExecPATH`).
//! GUI/daemon-launched processes inherit a minimal PATH (on macOS just
//! `/usr/bin:/bin:/usr/sbin:/sbin`); the enhanced PATH adds the common
//! node/npm/nvm locations so the `auggie` binary — and the `node` its shebang
//! needs — are discoverable.

use std::collections::HashSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use directories::BaseDirs;
use intent_core::path_utils;
use intent_core::path_utils::{
    has_windows_exec_extension, is_executable_file, is_executable_file_for, WINDOWS_EXEC_EXTENSIONS,
};

/// Candidate auggie file names for the current platform. POSIX uses the bare
/// name; Windows probes only runnable entry points (`.exe`/`.cmd`/`.bat`) in
/// preference order and never the bare extensionless name — `CreateProcess`
/// cannot run it, and the npm shim pair (`auggie` next to `auggie.cmd`) must
/// resolve the `.cmd` shim.
fn candidate_names() -> &'static [&'static str] {
    if cfg!(windows) {
        &["auggie.exe", "auggie.cmd", "auggie.bat"]
    } else {
        &["auggie"]
    }
}

fn home_dir() -> Option<PathBuf> {
    BaseDirs::new().map(|b| b.home_dir().to_path_buf())
}

/// The auggie-managed binary path (`~/.augment/bin/auggie[.exe]`), highest
/// priority in auggie discovery. This is auggie's own install location, not a
/// generic Intent-managed binary tier.
pub fn managed_binary_path() -> Option<PathBuf> {
    managed_binary_path_with_home(home_dir().as_deref())
}

/// Variant of [`managed_binary_path`] with the home directory injected instead
/// of resolved from the environment (see `enriched_tool_dirs_with_home`).
fn managed_binary_path_with_home(home: Option<&Path>) -> Option<PathBuf> {
    let name = if cfg!(windows) {
        "auggie.exe"
    } else {
        "auggie"
    };
    Some(home?.join(".augment").join("bin").join(name))
}

/// Build the ordered, de-duplicated list of directories to search (port of
/// `getEnhancedPath`): the current PATH first, then common node/npm locations,
/// then each nvm-managed node version's `bin`.
///
/// Re-exported from `intent_core::path_utils` for backward compatibility.
pub fn enhanced_path_dirs() -> Vec<PathBuf> {
    path_utils::enhanced_path_dirs()
}

/// The enhanced PATH joined into a single `OsString` (for a child's `PATH` env).
pub fn enhanced_path() -> OsString {
    std::env::join_paths(enhanced_path_dirs()).unwrap_or_default()
}

/// Find the first auggie executable across `dirs` (first dir, first candidate
/// name wins).
pub fn find_in_dirs(dirs: &[PathBuf]) -> Option<PathBuf> {
    for dir in dirs {
        for name in candidate_names() {
            let candidate = dir.join(name);
            if is_executable_file(&candidate) {
                return Some(candidate);
            }
        }
    }
    None
}

/// Read the auggie binary path recorded by auggie's own installer in
/// `~/.augment/auggie-path` (a single line holding an absolute path). The first
/// non-blank line is used, so a marker that grows extra lines still resolves.
///
/// Returns `None` — silently — when the marker is missing, unreadable, empty,
/// relative, or stale (points at something that is no longer an executable
/// file), so a leftover marker never shadows a working install.
fn marker_file_path_with_home(home: Option<&Path>) -> Option<PathBuf> {
    marker_file_path_with_home_for(home, cfg!(windows))
}

/// [`marker_file_path_with_home`] parametrized on the platform (test seam —
/// Windows CI is disabled, so the Windows arm is unit-tested on POSIX).
fn marker_file_path_with_home_for(home: Option<&Path>, is_windows: bool) -> Option<PathBuf> {
    let marker = home?.join(".augment").join("auggie-path");
    let contents = std::fs::read_to_string(&marker).ok()?;
    let recorded = PathBuf::from(contents.lines().map(str::trim).find(|l| !l.is_empty())?);
    if !recorded.is_absolute() {
        return None;
    }
    resolve_runnable(&recorded, is_windows)
}

/// Resolve `recorded` to a runnable executable under the platform policy. A
/// path that is already executable is returned as-is. On Windows a bare
/// extensionless record (auggie's installer writes the marker as the bare
/// `…\npm\auggie` even though only `auggie.cmd` is runnable there) is resolved
/// to its runnable sibling by probing `.exe`/`.cmd`/`.bat` in the same
/// directory, so the authoritative marker still works when that directory is
/// off the daemon's minimal PATH.
///
/// Deliberate asymmetry with provider PATH scanning: marker resolution
/// REPLACES a non-runnable extension (`with_extension` — the marker records
/// one concrete install path, so `…\auggie.ps1` probes `…\auggie.exe`),
/// while provider PATH-scan candidates APPEND runnable suffixes to the
/// command as given (`name_candidates_for` in `intent_providers::discover` —
/// `foo.py` probes `foo.py.exe`), matching how `cmd.exe` resolves PATHEXT.
fn resolve_runnable(recorded: &Path, is_windows: bool) -> Option<PathBuf> {
    if is_executable_file_for(recorded, is_windows) {
        return Some(recorded.to_path_buf());
    }
    if is_windows && !has_windows_exec_extension(recorded) {
        for ext in WINDOWS_EXEC_EXTENSIONS {
            let candidate = recorded.with_extension(ext);
            if is_executable_file_for(&candidate, is_windows) {
                return Some(candidate);
            }
        }
    }
    None
}

/// Discover the auggie binary (focused port of `findAuggiePathAsync`): the
/// auggie-managed binary first, then the `~/.augment/auggie-path` marker, then
/// a scan of the enhanced PATH. The marker beats the PATH scan because it is
/// auggie's authoritative record of where it installed itself — daemon-launched
/// processes inherit a minimal PATH that often misses that directory entirely.
pub fn find_auggie() -> Option<PathBuf> {
    find_auggie_with_home(home_dir().as_deref(), &enhanced_path_dirs())
}

/// Variant of [`find_auggie`] with the home directory and search dirs injected
/// instead of resolved from the environment, so the tier precedence is testable
/// without mutating process-global `HOME` or `PATH`.
fn find_auggie_with_home(home: Option<&Path>, path_dirs: &[PathBuf]) -> Option<PathBuf> {
    if let Some(managed) = managed_binary_path_with_home(home) {
        if is_executable_file(&managed) {
            return Some(managed);
        }
    }
    if let Some(recorded) = marker_file_path_with_home(home) {
        return Some(recorded);
    }
    find_in_dirs(path_dirs)
}

/// Build the PATH for *executing* auggie (port of `getAuggieExecPATH`): prepend
/// the binary's own directory so its co-located `node` resolves, then the
/// enhanced PATH.
pub fn exec_path(auggie_path: &Path) -> OsString {
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();
    if auggie_path.is_absolute() {
        if let Some(parent) = auggie_path.parent() {
            path_utils::push_dir(&mut dirs, &mut seen, parent.to_path_buf());
        }
    }
    for dir in enhanced_path_dirs() {
        path_utils::push_dir(&mut dirs, &mut seen, dir);
    }
    std::env::join_paths(dirs).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh RAII temp directory for `tag` under the system temp root. The
    /// returned guard removes the dir on drop (including on panic); set
    /// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
    fn unique_temp_dir(tag: &str) -> tempfile::TempDir {
        let mut dir = tempfile::Builder::new()
            .prefix(&format!("intent-ctx-{tag}-"))
            .tempdir()
            .expect("create test temp dir");
        if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
            dir.disable_cleanup(true);
        }
        dir
    }

    #[test]
    fn find_in_dirs_returns_none_when_absent() {
        let dir = unique_temp_dir("absent");
        assert_eq!(find_in_dirs(&[dir.path().to_path_buf()]), None);
    }

    #[cfg(unix)]
    #[test]
    fn find_in_dirs_finds_executable() {
        use std::os::unix::fs::PermissionsExt;
        let dir = unique_temp_dir("found");
        let bin = dir.path().join("auggie");
        std::fs::write(&bin, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(find_in_dirs(&[dir.path().to_path_buf()]), Some(bin));
    }

    #[cfg(unix)]
    #[test]
    fn find_in_dirs_skips_non_executable() {
        let dir = unique_temp_dir("nonexec");
        let bin = dir.path().join("auggie");
        std::fs::write(&bin, "not executable").unwrap();
        assert_eq!(find_in_dirs(&[dir.path().to_path_buf()]), None);
    }

    /// Create an executable stub file at `path` (parents must exist).
    #[cfg(unix)]
    fn write_executable(path: &Path) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(path, "#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// Write `~/.augment/auggie-path` under `home` with the given contents.
    fn write_marker(home: &Path, contents: &str) {
        let augment = home.join(".augment");
        std::fs::create_dir_all(&augment).unwrap();
        std::fs::write(augment.join("auggie-path"), contents).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn marker_file_resolves_recorded_executable() {
        let dir = unique_temp_dir("marker-ok");
        let bin = dir.path().join("auggie");
        write_executable(&bin);
        write_marker(dir.path(), bin.to_str().unwrap());
        assert_eq!(marker_file_path_with_home(Some(dir.path())), Some(bin));
    }

    #[cfg(unix)]
    #[test]
    fn marker_file_trims_surrounding_whitespace() {
        let dir = unique_temp_dir("marker-trim");
        let bin = dir.path().join("auggie");
        write_executable(&bin);
        write_marker(dir.path(), &format!("  {}\n\n", bin.to_str().unwrap()));
        assert_eq!(marker_file_path_with_home(Some(dir.path())), Some(bin));
    }

    #[test]
    fn marker_file_absent_returns_none() {
        let dir = unique_temp_dir("marker-absent");
        assert_eq!(marker_file_path_with_home(Some(dir.path())), None);
        assert_eq!(marker_file_path_with_home(None), None);
    }

    #[test]
    fn marker_file_stale_path_returns_none() {
        let dir = unique_temp_dir("marker-stale");
        let bin = dir.path().join("gone").join("auggie");
        write_marker(dir.path(), bin.to_str().unwrap());
        assert_eq!(marker_file_path_with_home(Some(dir.path())), None);
    }

    #[cfg(unix)]
    #[test]
    fn marker_file_non_executable_returns_none() {
        let dir = unique_temp_dir("marker-nonexec");
        let bin = dir.path().join("auggie");
        std::fs::write(&bin, "not executable").unwrap();
        write_marker(dir.path(), bin.to_str().unwrap());
        assert_eq!(marker_file_path_with_home(Some(dir.path())), None);
    }

    #[cfg(unix)]
    #[test]
    fn marker_file_relative_path_returns_none() {
        let dir = unique_temp_dir("marker-relative");
        let bin = dir.path().join("auggie");
        write_executable(&bin);
        write_marker(dir.path(), "auggie");
        assert_eq!(marker_file_path_with_home(Some(dir.path())), None);
    }

    #[test]
    fn marker_file_empty_returns_none() {
        let dir = unique_temp_dir("marker-empty");
        write_marker(dir.path(), "\n  \n");
        assert_eq!(marker_file_path_with_home(Some(dir.path())), None);
    }

    #[cfg(unix)]
    #[test]
    fn marker_file_uses_first_non_blank_line() {
        let dir = unique_temp_dir("marker-multiline");
        let bin = dir.path().join("auggie");
        write_executable(&bin);
        write_marker(
            dir.path(),
            &format!("\n{}\n# trailing junk\n", bin.to_str().unwrap()),
        );
        assert_eq!(marker_file_path_with_home(Some(dir.path())), Some(bin));
    }

    #[cfg(unix)]
    #[test]
    fn find_auggie_prefers_managed_binary_over_marker_and_path() {
        let dir = unique_temp_dir("tier-managed");
        let managed = dir.path().join(".augment").join("bin").join("auggie");
        std::fs::create_dir_all(managed.parent().unwrap()).unwrap();
        write_executable(&managed);
        let marked = dir.path().join("marked-auggie");
        write_executable(&marked);
        write_marker(dir.path(), marked.to_str().unwrap());
        let path_dir = unique_temp_dir("tier-managed-path");
        write_executable(&path_dir.path().join("auggie"));
        assert_eq!(
            find_auggie_with_home(Some(dir.path()), &[path_dir.path().to_path_buf()]),
            Some(managed)
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_auggie_prefers_marker_over_path_scan() {
        let dir = unique_temp_dir("tier-marker");
        let marked = dir.path().join("marked-auggie");
        write_executable(&marked);
        write_marker(dir.path(), marked.to_str().unwrap());
        let path_dir = unique_temp_dir("tier-marker-path");
        write_executable(&path_dir.path().join("auggie"));
        assert_eq!(
            find_auggie_with_home(Some(dir.path()), &[path_dir.path().to_path_buf()]),
            Some(marked)
        );
    }

    #[cfg(unix)]
    #[test]
    fn find_auggie_falls_back_to_path_scan_without_marker() {
        let dir = unique_temp_dir("tier-path");
        let path_dir = unique_temp_dir("tier-path-dirs");
        let bin = path_dir.path().join("auggie");
        write_executable(&bin);
        assert_eq!(
            find_auggie_with_home(Some(dir.path()), &[path_dir.path().to_path_buf()]),
            Some(bin)
        );
    }

    #[test]
    fn enhanced_path_includes_current_path_entries() {
        let dirs = enhanced_path_dirs();
        assert!(!dirs.is_empty());
    }

    #[test]
    fn has_windows_exec_extension_only_matches_runnable_exts() {
        assert!(has_windows_exec_extension(Path::new("auggie.exe")));
        assert!(has_windows_exec_extension(Path::new("auggie.cmd")));
        assert!(has_windows_exec_extension(Path::new("auggie.CMD")));
        assert!(has_windows_exec_extension(Path::new("auggie.bat")));
        assert!(!has_windows_exec_extension(Path::new("auggie")));
        assert!(!has_windows_exec_extension(Path::new("auggie.ps1")));
    }

    #[test]
    fn is_executable_file_for_windows_requires_runnable_extension() {
        let dir = unique_temp_dir("win-exec");
        let bare = dir.path().join("auggie");
        std::fs::write(&bare, "posix shim").unwrap();
        let cmd = dir.path().join("auggie.cmd");
        std::fs::write(&cmd, "@echo off").unwrap();
        // Windows policy: a bare extensionless file is not runnable; `.cmd` is.
        assert!(!is_executable_file_for(&bare, true));
        assert!(is_executable_file_for(&cmd, true));
        // A path that does not exist is never runnable.
        assert!(!is_executable_file_for(&dir.path().join("nope.cmd"), true));
    }

    #[test]
    fn resolve_runnable_windows_resolves_bare_marker_to_cmd_sibling() {
        let dir = unique_temp_dir("resolve-cmd");
        // npm layout: the bare POSIX shim next to the runnable `.cmd` shim.
        let bare = dir.path().join("auggie");
        std::fs::write(&bare, "posix shim").unwrap();
        let cmd = dir.path().join("auggie.cmd");
        std::fs::write(&cmd, "@echo off").unwrap();
        assert_eq!(resolve_runnable(&bare, true), Some(cmd));
    }

    #[test]
    fn resolve_runnable_windows_none_when_no_runnable_sibling() {
        let dir = unique_temp_dir("resolve-none");
        let bare = dir.path().join("auggie");
        std::fs::write(&bare, "posix shim").unwrap();
        assert_eq!(resolve_runnable(&bare, true), None);
    }

    #[test]
    fn resolve_runnable_returns_existing_runnable_as_is() {
        let dir = unique_temp_dir("resolve-asis");
        let cmd = dir.path().join("auggie.cmd");
        std::fs::write(&cmd, "@echo off").unwrap();
        assert_eq!(resolve_runnable(&cmd, true), Some(cmd.clone()));
    }

    #[test]
    fn marker_file_windows_resolves_bare_record_to_runnable_sibling() {
        let dir = unique_temp_dir("marker-win-bare");
        let npm = dir.path().join("npm");
        std::fs::create_dir_all(&npm).unwrap();
        let bare = npm.join("auggie");
        std::fs::write(&bare, "posix shim").unwrap();
        let cmd = npm.join("auggie.cmd");
        std::fs::write(&cmd, "@echo off").unwrap();
        write_marker(dir.path(), bare.to_str().unwrap());
        assert_eq!(
            marker_file_path_with_home_for(Some(dir.path()), true),
            Some(cmd)
        );
    }

    #[cfg(windows)]
    #[test]
    fn find_in_dirs_windows_prefers_cmd_over_bare_shim() {
        let dir = unique_temp_dir("win-find");
        std::fs::write(dir.path().join("auggie"), "posix shim").unwrap();
        std::fs::write(dir.path().join("auggie.cmd"), "@echo off").unwrap();
        let got = find_in_dirs(&[dir.path().to_path_buf()]).expect("resolves cmd");
        assert_eq!(got.file_name().unwrap(), std::ffi::OsStr::new("auggie.cmd"));
    }
}
