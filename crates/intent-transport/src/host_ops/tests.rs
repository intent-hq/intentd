//! Unit tests for the `host.*` host-services pure operations.

use std::path::PathBuf;

use super::*;

/// A fresh RAII temp directory for `tag` under the system temp root. The
/// returned guard removes the dir on drop (including on panic); set
/// `INTENTD_TEST_KEEP_TMP` (non-empty) to keep it around for debugging.
fn unique_temp_dir(tag: &str) -> tempfile::TempDir {
    let mut dir = tempfile::Builder::new()
        .prefix(&format!("intent-host-{tag}-"))
        .tempdir()
        .expect("create test temp dir");
    if std::env::var_os("INTENTD_TEST_KEEP_TMP").is_some_and(|v| !v.is_empty()) {
        dir.disable_cleanup(true);
    }
    dir
}

struct StubResolver(Option<PathBuf>);
impl BinaryResolver for StubResolver {
    fn find(&self, _name: &str) -> Option<PathBuf> {
        self.0.clone()
    }
}

struct StubProbe(Option<String>);
impl VersionProbe for StubProbe {
    fn probe(&self, _path: &Path) -> Option<String> {
        self.0.clone()
    }
}

#[test]
fn check_git_reports_unavailable_when_not_found() {
    let v = check_git_with(&StubResolver(None), &StubProbe(None));
    assert_eq!(v["available"], false);
    assert!(v.get("version").is_none());
    assert!(v.get("path").is_none());
}

#[test]
fn check_git_reports_available_when_resolver_and_probe_succeed() {
    let v = check_git_with(
        &StubResolver(Some(PathBuf::from("/usr/bin/git"))),
        &StubProbe(Some("git version 2.45.0".to_string())),
    );
    assert_eq!(v["available"], true);
    assert_eq!(v["version"], "git version 2.45.0");
    assert_eq!(v["path"], "/usr/bin/git");
}

#[test]
fn check_git_unavailable_when_probe_fails_even_if_resolver_finds() {
    let v = check_git_with(
        &StubResolver(Some(PathBuf::from("/usr/bin/git"))),
        &StubProbe(None),
    );
    assert_eq!(v["available"], false);
}

#[test]
fn check_node_reports_unavailable_when_not_found() {
    let v = check_node_with(&StubResolver(None), &StubProbe(None));
    assert_eq!(v["available"], false);
    assert!(v.get("version").is_none());
    assert!(v.get("path").is_none());
}

#[test]
fn check_node_reports_available_when_resolver_and_probe_succeed() {
    let v = check_node_with(
        &StubResolver(Some(PathBuf::from("/usr/local/bin/node"))),
        &StubProbe(Some("v24.1.0".to_string())),
    );
    assert_eq!(v["available"], true);
    assert_eq!(v["version"], "v24.1.0");
    assert_eq!(v["path"], "/usr/local/bin/node");
}

#[test]
fn check_node_unavailable_when_probe_fails_even_if_resolver_finds() {
    let v = check_node_with(
        &StubResolver(Some(PathBuf::from("/usr/local/bin/node"))),
        &StubProbe(None),
    );
    assert_eq!(v["available"], false);
}

#[test]
fn check_gh_reports_unavailable_when_not_found() {
    let v = check_gh_with(&StubResolver(None), &StubProbe(None));
    assert_eq!(v["available"], false);
    assert!(v.get("version").is_none());
    assert!(v.get("path").is_none());
}

#[test]
fn check_gh_reports_available_when_resolver_and_probe_succeed() {
    let v = check_gh_with(
        &StubResolver(Some(PathBuf::from("/opt/homebrew/bin/gh"))),
        &StubProbe(Some("gh version 2.62.0 (2024-11-14)".to_string())),
    );
    assert_eq!(v["available"], true);
    assert_eq!(v["version"], "gh version 2.62.0 (2024-11-14)");
    assert_eq!(v["path"], "/opt/homebrew/bin/gh");
}

#[test]
fn check_gh_unavailable_when_probe_fails_even_if_resolver_finds() {
    let v = check_gh_with(
        &StubResolver(Some(PathBuf::from("/opt/homebrew/bin/gh"))),
        &StubProbe(None),
    );
    assert_eq!(v["available"], false);
}

#[test]
fn check_auggie_reports_unavailable_when_unresolved() {
    let v = check_auggie_with(None);
    assert_eq!(v["available"], false);
    assert!(v.get("path").is_none());
    assert!(v.get("version").is_none());
}

/// Resolution-only: a resolved path is `available:true` with no version probe
/// (and no `version` field) — the auggie `--version` spawn is retired.
#[test]
fn check_auggie_is_resolution_only() {
    let v = check_auggie_with(Some(PathBuf::from("/opt/intent/auggie")));
    assert_eq!(v["available"], true);
    assert_eq!(v["path"], "/opt/intent/auggie");
    assert!(v.get("version").is_none());
}

#[cfg(unix)]
#[test]
fn resolve_auggie_prefers_configured_path_when_it_exists() {
    use std::os::unix::fs::PermissionsExt;
    let dir = unique_temp_dir("auggie-cfg");
    let dir = dir.path();
    let configured = dir.join("auggie-custom");
    std::fs::write(&configured, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&configured, std::fs::Permissions::from_mode(0o755)).unwrap();
    let resolved = resolve_auggie_path(Some(configured.to_str().unwrap()));
    assert_eq!(resolved.as_deref(), Some(configured.as_path()));
}

#[test]
fn resolve_auggie_ignores_blank_configured_path() {
    // Empty / whitespace-only configured path falls through to discovery; we
    // can't predict the discovery result on every host, so only assert that we
    // do NOT spuriously return the blank string itself.
    let resolved = resolve_auggie_path(Some("   "));
    if let Some(p) = resolved {
        assert!(!p.as_os_str().is_empty());
    }
}

#[test]
fn resolve_auggie_ignores_nonexistent_configured_path() {
    let bogus = std::env::temp_dir().join("intent-host-auggie-does-not-exist-xyzzy");
    // Even though `bogus` does not exist, resolve falls through to discovery;
    // we only assert it doesn't return the bogus path verbatim.
    let resolved = resolve_auggie_path(Some(bogus.to_str().unwrap()));
    assert_ne!(resolved.as_deref(), Some(bogus.as_path()));
}

#[test]
fn expand_path_handles_tilde_root_and_subpath() {
    let home = PathBuf::from("/home/me");
    assert_eq!(expand_path("~", &home), home);
    assert_eq!(expand_path("~/projects", &home), home.join("projects"));
    assert_eq!(expand_path("/abs/path", &home), PathBuf::from("/abs/path"));
    // Extra leading separators must stay under `home` (monorepo#832): before
    // delegating to `intent_core::expand_tilde_with`, `~//tmp` joined the
    // absolute `/tmp` and escaped `home` entirely.
    assert_eq!(expand_path("~//tmp", &home), home.join("tmp"));
    assert_eq!(expand_path("~//", &home), home);
}

#[test]
fn list_directory_with_returns_home_when_path_is_empty_or_missing() {
    let home = unique_temp_dir("ls-empty");
    let home = home.path();
    std::fs::create_dir_all(home.join("Documents")).unwrap();
    std::fs::write(home.join("readme.txt"), "hi").unwrap();
    let v = list_directory_with(None, home).unwrap();
    assert_eq!(v["path"], home.to_string_lossy().into_owned());
    assert_eq!(v["home"], home.to_string_lossy().into_owned());
    let entries = v["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    // Dirs first, then by name.
    assert_eq!(entries[0]["name"], "Documents");
    assert_eq!(entries[0]["isDirectory"], true);
    assert_eq!(entries[1]["name"], "readme.txt");
    assert_eq!(entries[1]["isDirectory"], false);
}

#[test]
fn list_directory_with_marks_nested_git_repo() {
    let home = unique_temp_dir("ls-git");
    let home = home.path();
    let repo = home.join("repo");
    std::fs::create_dir_all(repo.join(".git")).unwrap();
    let plain = home.join("plain");
    std::fs::create_dir_all(&plain).unwrap();
    let v = list_directory_with(Some(home.to_str().unwrap()), home).unwrap();
    let entries = v["entries"].as_array().unwrap();
    let repo_entry = entries.iter().find(|e| e["name"] == "repo").unwrap();
    assert_eq!(repo_entry["isGitRepo"], true);
    let plain_entry = entries.iter().find(|e| e["name"] == "plain").unwrap();
    assert_eq!(plain_entry["isGitRepo"], false);
}

#[test]
fn list_directory_with_marks_worktree_dot_git_file_as_git_repo() {
    let home = unique_temp_dir("ls-worktree");
    let home = home.path();
    let wt = home.join("wt");
    std::fs::create_dir_all(&wt).unwrap();
    std::fs::write(wt.join(".git"), "gitdir: /elsewhere/.git/worktrees/wt\n").unwrap();
    let v = list_directory_with(Some(home.to_str().unwrap()), home).unwrap();
    let entries = v["entries"].as_array().unwrap();
    let wt_entry = entries.iter().find(|e| e["name"] == "wt").unwrap();
    assert_eq!(wt_entry["isGitRepo"], true);
}

#[test]
fn list_directory_with_expands_tilde() {
    let home = unique_temp_dir("ls-tilde");
    let home = home.path();
    std::fs::create_dir_all(home.join("inside")).unwrap();
    let v = list_directory_with(Some("~"), home).unwrap();
    assert_eq!(v["path"], home.to_string_lossy().into_owned());
    let entries = v["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["name"], "inside");
}

#[test]
fn list_directory_with_expands_tilde_subpath() {
    // Regression coverage for monorepo#824: a typed `~/sub` path must resolve
    // under `home`, and the returned `path`/`parent` must be fully expanded so
    // the FE picker can navigate from the listing.
    let home = unique_temp_dir("ls-tilde-sub");
    let home = home.path();
    let sub = home.join("sub");
    std::fs::create_dir_all(sub.join("nested")).unwrap();
    let v = list_directory_with(Some("~/sub"), home).unwrap();
    assert_eq!(v["path"], sub.to_string_lossy().into_owned());
    assert_eq!(v["parent"], home.to_string_lossy().into_owned());
    assert_eq!(v["home"], home.to_string_lossy().into_owned());
    let entries = v["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["name"], "nested");
    assert_eq!(
        entries[0]["path"],
        sub.join("nested").to_string_lossy().into_owned()
    );
    assert_eq!(entries[0]["isDirectory"], true);
}

#[test]
fn list_directory_with_keeps_double_slash_tilde_under_home() {
    // Regression coverage for monorepo#832: `~//sub` must resolve under `home`
    // like `~/sub` does — before delegating to the shared tilde helper it
    // expanded to the absolute `//sub` and listed outside `home`.
    let home = unique_temp_dir("ls-tilde-dslash");
    let home = home.path();
    let sub = home.join("sub");
    std::fs::create_dir_all(sub.join("nested")).unwrap();
    let v = list_directory_with(Some("~//sub"), home).unwrap();
    assert_eq!(v["path"], sub.to_string_lossy().into_owned());
    assert_eq!(v["parent"], home.to_string_lossy().into_owned());
    let entries = v["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["name"], "nested");
}

#[test]
fn list_directory_with_returns_error_for_missing_path() {
    let home = unique_temp_dir("ls-missing");
    let home = home.path();
    let missing = home.join("nope");
    let err = list_directory_with(Some(missing.to_str().unwrap()), home).unwrap_err();
    assert!(err.contains("nope"));
}

/// Collect `(id, path)` pairs from a favorites array for compact assertions.
fn favorite_pairs(favs: &[Value]) -> Vec<(String, String)> {
    favs.iter()
        .map(|f| {
            (
                f["id"].as_str().unwrap().to_string(),
                f["path"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[test]
fn favorites_include_only_existing_standard_dirs() {
    let home = unique_temp_dir("fav-exist");
    let home = home.path();
    std::fs::create_dir_all(home.join("Desktop")).unwrap();
    std::fs::create_dir_all(home.join("Downloads")).unwrap();
    // No Documents — it must be excluded.
    let pairs = favorite_pairs(&favorites_with(home));
    assert_eq!(
        pairs,
        vec![
            ("home".into(), home.to_string_lossy().into_owned()),
            (
                "desktop".into(),
                home.join("Desktop").to_string_lossy().into_owned()
            ),
            (
                "downloads".into(),
                home.join("Downloads").to_string_lossy().into_owned()
            ),
        ]
    );
}

#[test]
fn favorites_always_include_home_even_when_nothing_else_exists() {
    let home = unique_temp_dir("fav-home-only");
    let home = home.path();
    let pairs = favorite_pairs(&favorites_with(home));
    assert_eq!(
        pairs,
        vec![("home".into(), home.to_string_lossy().into_owned())]
    );
}

#[test]
fn favorites_resolve_xdg_user_dirs_overrides() {
    // Relocated + localized dirs via `~/.config/user-dirs.dirs` must resolve:
    // `$HOME/...` values, absolute values, a `$HOME` "disabled" entry (falls
    // back to the conventional name), plus comments and malformed lines.
    let home = unique_temp_dir("fav-xdg");
    let home = home.path();
    let outside = unique_temp_dir("fav-xdg-outside");
    let outside = outside.path();
    std::fs::create_dir_all(home.join(".config")).unwrap();
    std::fs::create_dir_all(home.join("T\u{e9}l\u{e9}chargements")).unwrap();
    std::fs::create_dir_all(outside.join("Docs")).unwrap();
    std::fs::write(
        home.join(".config").join("user-dirs.dirs"),
        format!(
            "# This file is written by xdg-user-dirs-update\n\
             XDG_DESKTOP_DIR=\"$HOME\"\n\
             XDG_DOCUMENTS_DIR=\"{}\"\n\
             XDG_DOWNLOAD_DIR=\"$HOME/T\u{e9}l\u{e9}chargements\"\n\
             not a key value line\n",
            outside.join("Docs").display()
        ),
    )
    .unwrap();
    let pairs = favorite_pairs(&favorites_with(home));
    // Desktop is "disabled" ($HOME) and the conventional ~/Desktop does not
    // exist, so it is excluded; documents resolves to the absolute override;
    // downloads resolves to the localized $HOME-relative override.
    assert_eq!(
        pairs,
        vec![
            ("home".into(), home.to_string_lossy().into_owned()),
            (
                "documents".into(),
                outside.join("Docs").to_string_lossy().into_owned()
            ),
            (
                "downloads".into(),
                home.join("T\u{e9}l\u{e9}chargements")
                    .to_string_lossy()
                    .into_owned()
            ),
        ]
    );
}

#[test]
fn favorites_unescape_shell_escapes_in_xdg_values() {
    // `user-dirs.dirs` values are shell-format: `xdg-user-dirs-update` writes
    // backslash escapes for shell-special characters (e.g. `\$`, `\"`, `\\`),
    // which must be unescaped so the on-disk name resolves.
    let home = unique_temp_dir("fav-xdg-esc");
    let home = home.path();
    std::fs::create_dir_all(home.join(".config")).unwrap();
    std::fs::create_dir_all(home.join("Archive$2026")).unwrap();
    std::fs::create_dir_all(home.join("a\"b\\c")).unwrap();
    std::fs::write(
        home.join(".config").join("user-dirs.dirs"),
        "XDG_DOCUMENTS_DIR=\"$HOME/Archive\\$2026\"\n\
         XDG_DOWNLOAD_DIR=\"$HOME/a\\\"b\\\\c\"\n",
    )
    .unwrap();
    let pairs = favorite_pairs(&favorites_with(home));
    assert_eq!(
        pairs,
        vec![
            ("home".into(), home.to_string_lossy().into_owned()),
            (
                "documents".into(),
                home.join("Archive$2026").to_string_lossy().into_owned()
            ),
            (
                "downloads".into(),
                home.join("a\"b\\c").to_string_lossy().into_owned()
            ),
        ]
    );
}

#[test]
fn favorites_fall_back_to_conventional_on_invalid_xdg_values() {
    // Unquoted / relative XDG values are invalid per the spec and must be
    // ignored — the conventional home-joined name is used instead. A
    // slash-less `$HOME`-prefixed value (`$HOMEfoo`) is not the documented
    // `$HOME/` form and must be skipped, not resolved as home-relative.
    let home = unique_temp_dir("fav-xdg-invalid");
    let home = home.path();
    std::fs::create_dir_all(home.join(".config")).unwrap();
    std::fs::create_dir_all(home.join("Documents")).unwrap();
    std::fs::create_dir_all(home.join("Downloads")).unwrap();
    std::fs::create_dir_all(home.join("Desktop")).unwrap();
    std::fs::create_dir_all(home.join("foo")).unwrap();
    std::fs::write(
        home.join(".config").join("user-dirs.dirs"),
        "XDG_DOCUMENTS_DIR=$HOME/Unquoted\nXDG_DOWNLOAD_DIR=\"relative/path\"\nXDG_DESKTOP_DIR=\"$HOMEfoo\"\n",
    )
    .unwrap();
    let pairs = favorite_pairs(&favorites_with(home));
    // Desktop falls back to the conventional ~/Desktop (the `$HOMEfoo` value
    // is skipped, never resolved to ~/foo), documents and downloads to theirs.
    assert_eq!(
        pairs,
        vec![
            ("home".into(), home.to_string_lossy().into_owned()),
            (
                "desktop".into(),
                home.join("Desktop").to_string_lossy().into_owned()
            ),
            (
                "documents".into(),
                home.join("Documents").to_string_lossy().into_owned()
            ),
            (
                "downloads".into(),
                home.join("Downloads").to_string_lossy().into_owned()
            ),
        ]
    );
}

#[test]
fn list_directory_with_carries_favorites() {
    // The favorites ride the listing result regardless of the listed path,
    // resolved against `home` (not the listed directory).
    let home = unique_temp_dir("ls-favs");
    let home = home.path();
    std::fs::create_dir_all(home.join("Desktop")).unwrap();
    std::fs::create_dir_all(home.join("sub")).unwrap();
    let v = list_directory_with(Some("~/sub"), home).unwrap();
    let favs = v["favorites"].as_array().unwrap();
    let pairs = favorite_pairs(favs);
    assert_eq!(
        pairs,
        vec![
            ("home".into(), home.to_string_lossy().into_owned()),
            (
                "desktop".into(),
                home.join("Desktop").to_string_lossy().into_owned()
            ),
        ]
    );
}

#[test]
fn create_directory_with_creates_directory() {
    let home = unique_temp_dir("mkdir-basic");
    let home = home.path();
    let target = home.join("newdir");
    let v = create_directory_with(target.to_str().unwrap(), home).unwrap();
    assert!(target.is_dir());
    assert_eq!(v["path"], target.to_string_lossy().into_owned());
}

#[test]
fn create_directory_with_creates_nested_parents() {
    let home = unique_temp_dir("mkdir-nested");
    let home = home.path();
    let target = home.join("a").join("b").join("c");
    let v = create_directory_with(target.to_str().unwrap(), home).unwrap();
    assert!(target.is_dir());
    assert_eq!(v["path"], target.to_string_lossy().into_owned());
}

#[test]
fn create_directory_with_succeeds_when_already_exists() {
    let home = unique_temp_dir("mkdir-exists");
    let home = home.path();
    let target = home.join("existing");
    std::fs::create_dir_all(&target).unwrap();
    let v = create_directory_with(target.to_str().unwrap(), home).unwrap();
    assert!(target.is_dir());
    assert_eq!(v["path"], target.to_string_lossy().into_owned());
}

#[test]
fn create_directory_with_expands_tilde() {
    let home = unique_temp_dir("mkdir-tilde");
    let home = home.path();
    let v = create_directory_with("~/projects/new", home).unwrap();
    let expected = home.join("projects").join("new");
    assert!(expected.is_dir());
    assert_eq!(v["path"], expected.to_string_lossy().into_owned());
}

#[test]
fn create_directory_with_returns_error_when_path_is_a_file() {
    let home = unique_temp_dir("mkdir-file");
    let home = home.path();
    let file = home.join("occupied");
    std::fs::write(&file, "hi").unwrap();
    let err = create_directory_with(file.to_str().unwrap(), home).unwrap_err();
    assert!(err.contains("occupied"));
}

#[test]
fn directory_status_with_reports_existing_git_repo() {
    let home = unique_temp_dir("ds-repo");
    let home = home.path();
    let repo = home.join("repo");
    std::fs::create_dir_all(repo.join(".git")).unwrap();
    let v = directory_status_with(repo.to_str().unwrap(), home);
    assert_eq!(v["exists"], true);
    assert_eq!(v["isDirectory"], true);
    assert_eq!(v["isGitRepo"], true);
    assert_eq!(v["isSubdirectoryOfGitRepo"], false);
    assert!(v.get("parentGitRoot").is_none());
}

#[test]
fn directory_status_with_reports_subdirectory_of_git_repo() {
    let home = unique_temp_dir("ds-sub");
    let home = home.path();
    let repo = home.join("repo");
    std::fs::create_dir_all(repo.join(".git")).unwrap();
    let sub = repo.join("src").join("inner");
    std::fs::create_dir_all(&sub).unwrap();
    let v = directory_status_with(sub.to_str().unwrap(), home);
    assert_eq!(v["exists"], true);
    assert_eq!(v["isDirectory"], true);
    assert_eq!(v["isGitRepo"], false);
    assert_eq!(v["isSubdirectoryOfGitRepo"], true);
    assert_eq!(v["parentGitRoot"], repo.to_string_lossy().into_owned());
    assert_eq!(v["relativePathFromGitRoot"], "src/inner");
}

#[test]
fn directory_status_with_reports_nonexistent_path() {
    let home = unique_temp_dir("ds-nope");
    let home = home.path();
    let missing = home.join("absent");
    let v = directory_status_with(missing.to_str().unwrap(), home);
    assert_eq!(v["exists"], false);
    assert_eq!(v["isDirectory"], false);
    assert_eq!(v["isGitRepo"], false);
    assert_eq!(v["isSubdirectoryOfGitRepo"], false);
}

#[test]
fn directory_status_with_handles_worktree_dot_git_file() {
    let home = unique_temp_dir("ds-worktree");
    let home = home.path();
    let wt = home.join("wt");
    std::fs::create_dir_all(&wt).unwrap();
    std::fs::write(wt.join(".git"), "gitdir: /elsewhere/.git/worktrees/wt\n").unwrap();
    let v = directory_status_with(wt.to_str().unwrap(), home);
    assert_eq!(v["isGitRepo"], true);
    assert_eq!(v["isSubdirectoryOfGitRepo"], false);
}

#[test]
fn directory_status_with_treats_empty_dir_as_empty() {
    let home = unique_temp_dir("ds-empty");
    let home = home.path();
    let empty = home.join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    let v = directory_status_with(empty.to_str().unwrap(), home);
    assert_eq!(v["exists"], true);
    assert_eq!(v["isEmpty"], true);
}

#[test]
fn is_safe_binary_name_accepts_plain_names_and_rejects_metacharacters() {
    assert!(is_safe_binary_name("git"));
    assert!(is_safe_binary_name("claude-agent-acp"));
    assert!(is_safe_binary_name("node.exe"));
    assert!(is_safe_binary_name("a_b.c-1"));
    assert!(!is_safe_binary_name(""));
    assert!(!is_safe_binary_name("git rm"));
    assert!(!is_safe_binary_name("../evil"));
    assert!(!is_safe_binary_name("a/b"));
    assert!(!is_safe_binary_name("a;b"));
    assert!(!is_safe_binary_name("$(whoami)"));
}

#[test]
fn build_find_result_unavailable_when_path_is_none() {
    let v = build_find_result(None, &StubProbe(None));
    assert_eq!(v["available"], false);
    assert!(v.get("path").is_none());
    assert!(v.get("version").is_none());
}

#[test]
fn build_find_result_available_without_version_when_probe_fails() {
    let v = build_find_result(Some(PathBuf::from("/usr/bin/code")), &StubProbe(None));
    assert_eq!(v["available"], true);
    assert_eq!(v["path"], "/usr/bin/code");
    assert!(
        v.get("version").is_none(),
        "version is best-effort: omitted when the probe fails"
    );
}

#[test]
fn build_find_result_includes_version_when_probe_succeeds() {
    let v = build_find_result(
        Some(PathBuf::from("/usr/bin/git")),
        &StubProbe(Some("git version 2.45.0".to_string())),
    );
    assert_eq!(v["available"], true);
    assert_eq!(v["path"], "/usr/bin/git");
    assert_eq!(v["version"], "git version 2.45.0");
}

#[test]
fn find_binary_op_rejects_unsafe_names_without_spawning() {
    let v = find_binary_op("../../bin/sh", &[]);
    assert_eq!(v["available"], false);
}

#[cfg(unix)]
#[test]
fn resolve_binary_path_finds_caller_common_path() {
    use std::os::unix::fs::PermissionsExt;
    let dir = unique_temp_dir("resolve-common");
    let dir = dir.path();
    let bin = dir.join("totally-bogus-binary-xyzzy");
    std::fs::write(&bin, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    // The name is not on PATH; resolution must fall through to common_paths.
    let resolved = resolve_binary_path(
        "totally-bogus-binary-xyzzy",
        &[bin.to_string_lossy().into_owned()],
    );
    assert_eq!(resolved.as_deref(), Some(bin.as_path()));
}

#[cfg(unix)]
#[test]
fn resolve_binary_path_searches_enriched_tool_dirs() {
    use std::os::unix::fs::PermissionsExt;

    // Smoke test: verify that binary resolution searches enriched_tool_dirs
    // (which include ~/.local/bin) when PATH doesn't find the binary. The
    // scratch home is injected via resolve_binary_path_with_home instead of
    // mutating process-global HOME, which would race parallel tests.
    let test_dir = unique_temp_dir("enriched-home");
    let test_dir = test_dir.path();
    let local_bin = test_dir.join(".local").join("bin");
    std::fs::create_dir_all(&local_bin).unwrap();

    let test_bin_name = format!(
        "test-enriched-binary-{}",
        test_dir.file_name().unwrap().to_string_lossy()
    );
    let bin = local_bin.join(&test_bin_name);
    std::fs::write(&bin, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();

    let resolved = resolve_binary_path_with_home(&test_bin_name, &[], test_dir);

    assert_eq!(resolved.as_deref(), Some(bin.as_path()));
}

#[cfg(unix)]
#[test]
fn resolve_binary_path_prefers_newest_nvm_node_version() {
    use std::os::unix::fs::PermissionsExt;

    let home = unique_temp_dir("nvm-multi-version-home");
    let v20_bin = home.path().join(".nvm/versions/node/v20.19.0/bin");
    let v24_bin = home.path().join(".nvm/versions/node/v24.5.0/bin");
    std::fs::create_dir_all(&v20_bin).unwrap();
    std::fs::create_dir_all(&v24_bin).unwrap();
    let older_binary = v20_bin.join("node");
    std::fs::write(&older_binary, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&older_binary, std::fs::Permissions::from_mode(0o755)).unwrap();
    let binary = v24_bin.join("node");
    std::fs::write(&binary, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();

    let tool_dirs = path_utils::enriched_tool_dirs_with_home(Some(home.path()));
    let resolved = resolve_binary_path_with_tool_dirs_and_lookup("node", &[], &tool_dirs, |_| None);

    assert_eq!(resolved.as_deref(), Some(binary.as_path()));
}

#[cfg(unix)]
#[test]
fn resolve_binary_path_prefers_active_path_node_over_newer_nvm_install() {
    use std::os::unix::fs::PermissionsExt;

    let home = unique_temp_dir("nvm-path-precedence-home");
    let nvm_bin = home.path().join(".nvm/versions/node/v26.8.1/bin");
    let path_bin = home.path().join("active-node/bin");
    std::fs::create_dir_all(&nvm_bin).unwrap();
    std::fs::create_dir_all(&path_bin).unwrap();
    let installed_node = nvm_bin.join("node");
    std::fs::write(&installed_node, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&installed_node, std::fs::Permissions::from_mode(0o755)).unwrap();
    let active_node = path_bin.join("node");
    std::fs::write(&active_node, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&active_node, std::fs::Permissions::from_mode(0o755)).unwrap();

    let resolved = resolve_binary_path_with_tool_dirs_and_lookup("node", &[], &[nvm_bin], |_| {
        Some(active_node.clone())
    });

    assert_eq!(resolved.as_deref(), Some(active_node.as_path()));
}

#[cfg(unix)]
#[test]
fn resolve_binary_path_skips_non_executable_nvm_node_for_path_node() {
    use std::os::unix::fs::PermissionsExt;

    let home = unique_temp_dir("nvm-non-executable-home");
    let nvm_bin = home.path().join(".nvm/versions/node/v24.5.0/bin");
    let path_bin = home.path().join("path-bin");
    std::fs::create_dir_all(&nvm_bin).unwrap();
    std::fs::create_dir_all(&path_bin).unwrap();
    let non_executable = nvm_bin.join("node");
    std::fs::write(&non_executable, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&non_executable, std::fs::Permissions::from_mode(0o644)).unwrap();
    let path_node = path_bin.join("node");
    std::fs::write(&path_node, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&path_node, std::fs::Permissions::from_mode(0o755)).unwrap();
    let tool_dirs = vec![nvm_bin];

    let resolved = resolve_binary_path_with_tool_dirs_and_lookup("node", &[], &tool_dirs, |_| {
        Some(path_node.clone())
    });

    assert_eq!(resolved.as_deref(), Some(path_node.as_path()));
}

/// `find_binary_op` caches a POSITIVE resolution: removing the resolved
/// binary after the first call must not flip a second, cached call to
/// unavailable within the TTL.
#[cfg(unix)]
#[test]
fn find_binary_op_caches_positive_result_across_removal() {
    use std::os::unix::fs::PermissionsExt;
    let dir = unique_temp_dir("cache-positive");
    let dir = dir.path();
    let name = format!(
        "intent-cache-pos-{}",
        dir.file_name().unwrap().to_string_lossy()
    );
    let bin = dir.join(&name);
    std::fs::write(&bin, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let common_paths = vec![bin.to_string_lossy().into_owned()];

    let first = find_binary_op(&name, &common_paths);
    assert_eq!(first["available"], true, "{first}");

    std::fs::remove_file(&bin).unwrap();

    let second = find_binary_op(&name, &common_paths);
    assert_eq!(
        second["available"], true,
        "a cached positive must survive the binary's removal within the TTL: {second}"
    );
}

/// `find_binary_op` never caches a NEGATIVE resolution: installing the binary
/// right after an unavailable call must be visible on the very next call.
#[cfg(unix)]
#[test]
fn find_binary_op_does_not_cache_negative_result() {
    use std::os::unix::fs::PermissionsExt;
    let dir = unique_temp_dir("cache-negative");
    let dir = dir.path();
    let name = format!(
        "intent-cache-neg-{}",
        dir.file_name().unwrap().to_string_lossy()
    );
    let bin = dir.join(&name);
    let common_paths = vec![bin.to_string_lossy().into_owned()];

    let first = find_binary_op(&name, &common_paths);
    assert_eq!(first["available"], false, "{first}");

    std::fs::write(&bin, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();

    let second = find_binary_op(&name, &common_paths);
    assert_eq!(
        second["available"], true,
        "a not-found result must never be cached, so an install is picked up \
         immediately: {second}"
    );
}

/// Distinct `common_paths` hint lists must not share a cache entry — a
/// resolution found via one hint set must not leak into a call with a
/// different (and here, empty/non-matching) hint set for the same name.
#[cfg(unix)]
#[test]
fn find_binary_op_cache_key_includes_common_paths() {
    use std::os::unix::fs::PermissionsExt;
    let dir = unique_temp_dir("cache-key-paths");
    let dir = dir.path();
    let name = format!(
        "intent-cache-key-{}",
        dir.file_name().unwrap().to_string_lossy()
    );
    let bin = dir.join(&name);
    std::fs::write(&bin, "#!/bin/sh\nexit 0\n").unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let common_paths = vec![bin.to_string_lossy().into_owned()];

    let hinted = find_binary_op(&name, &common_paths);
    assert_eq!(hinted["available"], true, "{hinted}");

    // No hint at all for this never-on-PATH name: must resolve independently
    // (unavailable), never reusing the hinted call's cached entry.
    let unhinted = find_binary_op(&name, &[]);
    assert_eq!(
        unhinted["available"], false,
        "a different common_paths hint list must not share the cache entry: {unhinted}"
    );
}

#[test]
fn tool_availability_op_defaults_to_canonical_tool_set() {
    let v = tool_availability_op(None);
    let tools = v["tools"].as_object().unwrap();
    for name in DEFAULT_TOOLS {
        let entry = tools.get(*name).unwrap_or_else(|| panic!("missing {name}"));
        assert!(
            entry["available"].is_boolean(),
            "{name}.available is always present"
        );
    }
}

#[test]
fn tool_availability_op_honours_explicit_tool_list() {
    let v = tool_availability_op(Some(vec![
        "git".to_string(),
        "definitely-not-installed-xyzzy".to_string(),
    ]));
    let tools = v["tools"].as_object().unwrap();
    assert_eq!(tools.len(), 2);
    assert!(tools["git"]["available"].is_boolean());
    assert_eq!(tools["definitely-not-installed-xyzzy"]["available"], false);
}

#[test]
fn enhanced_path_dedups_and_appends_canonical_tool_dirs() {
    let custom = if cfg!(target_os = "windows") {
        r"C:\intent-test-tools"
    } else {
        "/opt/tools"
    };
    let input = std::env::join_paths([custom, custom])
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let enhanced = enhanced_path_from(&input);
    let parts: Vec<PathBuf> = std::env::split_paths(&enhanced).collect();
    // The caller's entry comes first and appears exactly once (de-duplicated).
    assert_eq!(parts[0], PathBuf::from(custom));
    assert_eq!(
        parts
            .iter()
            .filter(|p| p.as_path() == Path::new(custom))
            .count(),
        1
    );
    // Every platform-specific essential system path is merged in.
    for essential in essential_system_paths() {
        assert!(
            parts.contains(&PathBuf::from(&essential)),
            "missing essential {essential}"
        );
    }
}

#[test]
fn build_env_json_is_secret_safe_and_well_shaped() {
    let home = PathBuf::from("/home/me");
    let raw_path = format!("/usr/bin{0}/bin", path_separator());
    let v = build_env_json(
        &raw_path,
        "/bin/zsh",
        &home,
        &["PATH".to_string(), "SECRET_TOKEN".to_string()],
    );
    assert_eq!(v["path"], raw_path);
    assert_eq!(v["shell"], "/bin/zsh");
    assert_eq!(v["home"], "/home/me");
    let entries = v["pathEntries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0], "/usr/bin");
    assert!(v["enhancedPath"].is_string());
    let names = v["varNames"].as_array().unwrap();
    // Names are reported, but no variable VALUE other than path/shell/home leaks.
    assert!(names.iter().any(|n| n == "SECRET_TOKEN"));
    assert!(
        !v.to_string().contains("secret-value"),
        "build_env_json never carries arbitrary variable values"
    );
}

#[test]
fn env_probe_reports_path_entries_and_var_names() {
    let v = env_probe();
    assert!(v["path"].is_string());
    assert!(v["pathEntries"].is_array());
    assert!(v["enhancedPath"].is_string());
    assert!(v["varNames"].is_array());
}

/// Stub flatpak probe that returns a fixed set of installed app IDs (or `None`
/// to model an unavailable flatpak install).
struct StubFlatpak(Option<std::collections::HashSet<String>>);

impl FlatpakProbe for StubFlatpak {
    fn list_installed(&self) -> Option<std::collections::HashSet<String>> {
        self.0.clone()
    }
}

/// Stub binary resolver that resolves a fixed mapping of `name -> path`.
struct MapResolver(std::collections::HashMap<&'static str, PathBuf>);

impl BinaryResolver for MapResolver {
    fn find(&self, name: &str) -> Option<PathBuf> {
        self.0.get(name).cloned()
    }
}

#[test]
fn is_safe_app_name_accepts_spaces_and_rejects_traversal() {
    assert!(is_safe_app_name("Visual Studio Code"));
    assert!(is_safe_app_name("IntelliJ IDEA"));
    assert!(is_safe_app_name("kitty"));
    assert!(!is_safe_app_name(""));
    assert!(!is_safe_app_name("../Finder"));
    assert!(!is_safe_app_name(".hidden"));
    assert!(!is_safe_app_name("foo/bar"));
    assert!(!is_safe_app_name("foo\\bar"));
    assert!(!is_safe_app_name("foo\0bar"));
}

#[test]
fn find_macos_app_bundle_finds_first_existing_dir() {
    let root = unique_temp_dir("find-app");
    let root = root.path();
    let apps = root.join("Applications");
    std::fs::create_dir_all(apps.join("Cursor.app")).unwrap();
    let user_apps = root.join("UserApplications");
    std::fs::create_dir_all(&user_apps).unwrap();
    let dirs = vec![user_apps, apps.clone()];
    let found = find_macos_app_bundle("Cursor", &dirs).expect("Cursor.app resolves");
    assert_eq!(found, apps.join("Cursor.app"));
}

#[test]
fn find_macos_app_bundle_returns_none_when_missing() {
    let root = unique_temp_dir("find-app-missing");
    let root = root.path();
    let apps = root.join("Applications");
    std::fs::create_dir_all(&apps).unwrap();
    let dirs = vec![apps];
    assert!(find_macos_app_bundle("DoesNotExist", &dirs).is_none());
}

#[test]
fn find_macos_app_bundle_rejects_unsafe_names() {
    let root = unique_temp_dir("find-app-unsafe");
    let root = root.path();
    let apps = root.join("Applications");
    std::fs::create_dir_all(&apps).unwrap();
    assert!(find_macos_app_bundle("../escape", &[apps]).is_none());
}

#[test]
fn find_app_with_returns_installed_with_source_on_macos() {
    let root = unique_temp_dir("find-app-with");
    let root = root.path();
    let apps = root.join("Applications");
    std::fs::create_dir_all(apps.join("Zed.app")).unwrap();
    let v = find_app_with("Zed", std::slice::from_ref(&apps), true);
    assert_eq!(v["installed"], true);
    assert_eq!(
        v["path"],
        apps.join("Zed.app").to_string_lossy().into_owned()
    );
    assert_eq!(v["source"], "macAppBundle");
}

#[test]
fn find_app_with_returns_uninstalled_when_not_macos() {
    let root = unique_temp_dir("find-app-not-mac");
    let root = root.path();
    let apps = root.join("Applications");
    std::fs::create_dir_all(apps.join("Zed.app")).unwrap();
    let v = find_app_with("Zed", &[apps], false);
    assert_eq!(v["installed"], false);
    assert!(v.get("path").is_none());
}

#[test]
fn find_app_with_returns_uninstalled_for_unsafe_name() {
    let root = unique_temp_dir("find-app-unsafe-name");
    let root = root.path();
    let apps = root.join("Applications");
    std::fs::create_dir_all(&apps).unwrap();
    let v = find_app_with("../../etc/passwd", &[apps], true);
    assert_eq!(v["installed"], false);
}

#[test]
fn list_installed_editors_with_detects_macos_app_bundles() {
    let root = unique_temp_dir("list-mac");
    let root = root.path();
    let apps = root.join("Applications");
    std::fs::create_dir_all(apps.join("Visual Studio Code.app")).unwrap();
    std::fs::create_dir_all(apps.join("Cursor.app")).unwrap();
    let v = list_installed_editors_with(
        EditorPlatform::Macos,
        std::slice::from_ref(&apps),
        &StubResolver(None),
        &StubFlatpak(None),
    );
    let editors = v["editors"].as_array().expect("editors array");
    let vscode = editors
        .iter()
        .find(|e| e["id"] == "vscode")
        .expect("vscode entry");
    assert_eq!(vscode["installed"], true);
    assert_eq!(vscode["source"], "macAppBundle");
    assert_eq!(
        vscode["path"],
        apps.join("Visual Studio Code.app")
            .to_string_lossy()
            .into_owned()
    );
    let cursor = editors
        .iter()
        .find(|e| e["id"] == "cursor")
        .expect("cursor entry");
    assert_eq!(cursor["installed"], true);
    let zed = editors
        .iter()
        .find(|e| e["id"] == "zed")
        .expect("zed entry");
    assert_eq!(zed["installed"], false);
}

#[test]
fn list_installed_editors_with_skips_windows_only_editors_on_macos() {
    let root = unique_temp_dir("list-mac-skip-win");
    let root = root.path();
    let apps = root.join("Applications");
    std::fs::create_dir_all(&apps).unwrap();
    let v = list_installed_editors_with(
        EditorPlatform::Macos,
        &[apps],
        &StubResolver(None),
        &StubFlatpak(None),
    );
    let editors = v["editors"].as_array().unwrap();
    assert!(
        editors
            .iter()
            .all(|e| e["id"] != "powershell" && e["id"] != "windows-terminal"),
        "windows-only editors are filtered out on macOS"
    );
    assert!(editors.iter().any(|e| e["id"] == "xcode"));
}

#[test]
fn list_installed_editors_with_reports_finder_always_installed_on_macos() {
    let root = unique_temp_dir("list-mac-finder");
    let root = root.path();
    let apps = root.join("Applications");
    std::fs::create_dir_all(&apps).unwrap();
    let v = list_installed_editors_with(
        EditorPlatform::Macos,
        &[apps],
        &StubResolver(None),
        &StubFlatpak(None),
    );
    let editors = v["editors"].as_array().unwrap();
    let finder = editors
        .iter()
        .find(|e| e["id"] == "finder")
        .expect("finder entry");
    assert_eq!(finder["installed"], true, "Finder ships with macOS");
    assert_eq!(finder["source"], "macAppBundle");
    assert_eq!(finder["path"], "/System/Library/CoreServices/Finder.app");
}

#[test]
fn list_installed_editors_with_prefers_probed_finder_bundle_over_builtin_path() {
    let root = unique_temp_dir("list-mac-finder-probe");
    let root = root.path();
    let apps = root.join("Applications");
    std::fs::create_dir_all(apps.join("Finder.app")).unwrap();
    let v = list_installed_editors_with(
        EditorPlatform::Macos,
        std::slice::from_ref(&apps),
        &StubResolver(None),
        &StubFlatpak(None),
    );
    let editors = v["editors"].as_array().unwrap();
    let finder = editors
        .iter()
        .find(|e| e["id"] == "finder")
        .expect("finder entry");
    assert_eq!(finder["installed"], true);
    assert_eq!(
        finder["path"],
        apps.join("Finder.app").to_string_lossy().into_owned(),
        "a probe hit still wins over the built-in CoreServices fallback"
    );
}

#[test]
fn list_installed_editors_with_reports_explorer_always_installed_on_windows() {
    let v = list_installed_editors_with(
        EditorPlatform::Windows,
        &[],
        &StubResolver(None),
        &StubFlatpak(None),
    );
    let editors = v["editors"].as_array().unwrap();
    let finder = editors
        .iter()
        .find(|e| e["id"] == "finder")
        .expect("finder entry");
    assert_eq!(finder["installed"], true, "Explorer ships with Windows");
    assert_eq!(finder["source"], "binary");
    assert_eq!(finder["path"], "explorer");
}

#[test]
fn list_installed_editors_with_keeps_linux_file_manager_binary_probe() {
    let v = list_installed_editors_with(
        EditorPlatform::Linux,
        &[],
        &StubResolver(None),
        &StubFlatpak(None),
    );
    let editors = v["editors"].as_array().unwrap();
    let finder = editors.iter().find(|e| e["id"] == "finder").unwrap();
    assert_eq!(
        finder["installed"], false,
        "no guaranteed file manager on Linux — the binary probe still decides"
    );
}

#[test]
fn list_installed_editors_with_detects_linux_binary_then_flatpak() {
    let resolver = MapResolver(
        [("code", PathBuf::from("/usr/bin/code"))]
            .into_iter()
            .collect(),
    );
    let mut flatpak = std::collections::HashSet::new();
    flatpak.insert("dev.zed.Zed".to_string());
    let v = list_installed_editors_with(
        EditorPlatform::Linux,
        &[],
        &resolver,
        &StubFlatpak(Some(flatpak)),
    );
    let editors = v["editors"].as_array().unwrap();
    let vscode = editors.iter().find(|e| e["id"] == "vscode").unwrap();
    assert_eq!(vscode["installed"], true);
    assert_eq!(vscode["source"], "binary");
    assert_eq!(vscode["path"], "/usr/bin/code");
    let zed = editors.iter().find(|e| e["id"] == "zed").unwrap();
    assert_eq!(zed["installed"], true);
    assert_eq!(zed["source"], "flatpak");
    assert_eq!(zed["flatpakId"], "dev.zed.Zed");
    // macOS-only editors are filtered out on Linux.
    assert!(editors.iter().all(|e| e["id"] != "xcode"));
    assert!(editors.iter().all(|e| e["id"] != "iterm"));
}

#[test]
fn list_installed_editors_with_handles_missing_flatpak_on_linux() {
    let v = list_installed_editors_with(
        EditorPlatform::Linux,
        &[],
        &StubResolver(None),
        &StubFlatpak(None),
    );
    let editors = v["editors"].as_array().unwrap();
    for entry in editors {
        assert_eq!(
            entry["installed"], false,
            "no binary + no flatpak ⇒ uninstalled: {entry}"
        );
    }
}

#[test]
fn list_installed_editors_with_windows_uses_binary_resolver() {
    let resolver = MapResolver(
        [("code.cmd", PathBuf::from(r"C:\Program Files\Code\code.cmd"))]
            .into_iter()
            .collect(),
    );
    let v =
        list_installed_editors_with(EditorPlatform::Windows, &[], &resolver, &StubFlatpak(None));
    let editors = v["editors"].as_array().unwrap();
    let vscode = editors.iter().find(|e| e["id"] == "vscode").unwrap();
    assert_eq!(vscode["installed"], true);
    assert_eq!(vscode["source"], "binary");
    // macOS-only editors are filtered out on Windows.
    assert!(editors.iter().all(|e| e["id"] != "xcode"));
    // Windows-only editors are visible on Windows.
    assert!(editors.iter().any(|e| e["id"] == "powershell"));
}

/// Regression test: `run_version_with()` enriches PATH with binary's parent dir,
/// enabling scripts that invoke co-located dependencies via PATH lookup to succeed.
///
/// Mirrors the real nvm layout where 'node' is co-located with npm-installed
/// binaries like 'auggie'. A fake script invokes a co-located fake 'node' via
/// the shell PATH. The minimal PATH does NOT include the script directory, so
/// without parent-dir enrichment the node lookup fails.
#[cfg(unix)]
#[test]
fn run_version_enriches_path_with_binary_parent_dir_for_env_shebangs() {
    use std::os::unix::fs::PermissionsExt;

    let dir = unique_temp_dir("run-version-env-shebang");
    let dir = dir.path();

    // Create fake 'node' interpreter that responds to --version
    let fake_node = dir.join("node");
    std::fs::write(&fake_node, "#!/bin/sh\necho \"fake-auggie 2.0.0\"\n").unwrap();
    std::fs::set_permissions(&fake_node, std::fs::Permissions::from_mode(0o755)).unwrap();

    // Create fake 'auggie' script that invokes co-located 'node' via PATH lookup
    // This mirrors real npm-installed binaries that have #!/usr/bin/env node shebangs
    let fake_auggie = dir.join("auggie");
    std::fs::write(
        &fake_auggie,
        "#!/bin/sh\n# Simulate #!/usr/bin/env node shebang behavior\nif [ \"$1\" = \"--version\" ]; then\n  exec node\nfi\nexit 1\n",
    )
    .unwrap();
    std::fs::set_permissions(&fake_auggie, std::fs::Permissions::from_mode(0o755)).unwrap();

    // Build enriched PATH with parent dir prepended (the fix)
    let enriched_path = super::build_enriched_path_for_binary(&fake_auggie);

    // Probe should succeed with enriched PATH (node resolves via parent dir)
    let version = super::run_version_with(&fake_auggie, &enriched_path);
    assert!(
        version.is_some(),
        "run_version_with enriched PATH should succeed when node is co-located; enriched_path={}",
        enriched_path.to_string_lossy()
    );
    assert_eq!(version.unwrap(), "fake-auggie 2.0.0");

    // Prove regression coverage: probe FAILS with PATH that cannot possibly contain node
    // Use a non-existent directory to ensure deterministic failure (not /usr/bin which may have node)
    let impossible_path = std::ffi::OsString::from("/nonexistent/impossible/directory");
    let version_minimal = super::run_version_with(&fake_auggie, &impossible_path);
    assert!(
        version_minimal.is_none(),
        "run_version_with impossible PATH should fail when node is NOT on PATH (proves fix is needed)"
    );
}

// ---------------------------------------------------------------------------
// Windows runnable-extension resolution (mirrors intent_providers::discover).
//
// These use the platform-parametrized `_for` seams so both arms run on the
// POSIX CI host; POSIX behavior stays byte-identical (bare name only).
// ---------------------------------------------------------------------------

#[test]
fn has_windows_exec_extension_is_case_insensitive() {
    assert!(has_windows_exec_extension(Path::new("pi.cmd")));
    assert!(has_windows_exec_extension(Path::new("pi.EXE")));
    assert!(has_windows_exec_extension(Path::new(r"C:\x\tool.Bat")));
    assert!(!has_windows_exec_extension(Path::new("pi")));
    assert!(!has_windows_exec_extension(Path::new("pi.ps1")));
    assert!(!has_windows_exec_extension(Path::new("foo.py")));
}

#[test]
fn name_candidates_posix_is_bare_name_only() {
    assert_eq!(name_candidates_for("pi", false), vec!["pi"]);
    assert_eq!(name_candidates_for("pi.exe", false), vec!["pi.exe"]);
}

#[test]
fn name_candidates_windows_prefers_executable_extensions_over_bare_name() {
    assert_eq!(
        name_candidates_for("pi", true),
        vec!["pi.exe", "pi.cmd", "pi.bat"],
        "the bare extensionless name must not be a candidate on Windows"
    );
}

#[test]
fn name_candidates_windows_keeps_command_carrying_executable_extension() {
    assert_eq!(name_candidates_for("pi.cmd", true), vec!["pi.cmd"]);
    // Case-insensitive, matching Windows filename semantics.
    assert_eq!(name_candidates_for("PI.EXE", true), vec!["PI.EXE"]);
}

#[test]
fn select_lookup_line_posix_takes_first_line() {
    // `which` prints a single line; the first non-empty line wins verbatim,
    // even one without an extension (POSIX behavior is unchanged).
    assert_eq!(
        select_path_lookup_line("pi", &["/usr/local/bin/pi"], false),
        Some("/usr/local/bin/pi".to_string())
    );
}

#[test]
fn select_lookup_line_windows_npm_shim_pair_prefers_cmd() {
    // Regression: `where pi` lists the bare POSIX shim ahead of the runnable
    // `pi.cmd`; the `.cmd` line must win (bare used to be taken first).
    let lines = [
        r"C:\Users\me\AppData\Roaming\npm\pi",
        r"C:\Users\me\AppData\Roaming\npm\pi.cmd",
    ];
    assert_eq!(
        select_path_lookup_line("pi", &lines, true),
        Some(r"C:\Users\me\AppData\Roaming\npm\pi.cmd".to_string())
    );
}

#[test]
fn select_lookup_line_windows_prefers_exe_over_cmd_and_bat() {
    let lines = [r"C:\tools\pi.bat", r"C:\tools\pi.cmd", r"C:\tools\pi.exe"];
    assert_eq!(
        select_path_lookup_line("pi", &lines, true),
        Some(r"C:\tools\pi.exe".to_string()),
        "exe outranks cmd/bat regardless of where output order"
    );
}

#[test]
fn select_lookup_line_windows_bare_only_does_not_resolve() {
    // Only an extensionless shim on PATH: not CreateProcess-runnable, so PATH
    // resolution fails here (falls through to dir scans / common paths).
    let lines = [r"C:\Users\me\AppData\Roaming\npm\pi"];
    assert_eq!(select_path_lookup_line("pi", &lines, true), None);
}

#[test]
fn select_lookup_line_windows_keeps_first_line_when_name_has_exec_extension() {
    // The caller asked for `pi.cmd` explicitly: keep the first match as-is.
    let lines = [r"C:\a\pi.cmd", r"C:\b\pi.cmd"];
    assert_eq!(
        select_path_lookup_line("pi.cmd", &lines, true),
        Some(r"C:\a\pi.cmd".to_string())
    );
}

#[test]
fn select_lookup_line_empty_is_none() {
    assert_eq!(select_path_lookup_line("pi", &[], false), None);
    assert_eq!(select_path_lookup_line("pi", &[], true), None);
}

/// `find_in_dir_candidates` uses the host platform's candidate set. On POSIX
/// that is the bare name, so a bare executable resolves — proving the dir-scan
/// path still works for Unix after the refactor away from `binary_filename`.
#[cfg(unix)]
#[test]
fn find_in_dir_candidates_posix_resolves_bare_name() {
    let dir = unique_temp_dir("dir-candidates-posix");
    let bin = dir.path().join("some-bare-tool");
    std::fs::write(&bin, "#!/bin/sh\nexit 0\n").unwrap();
    assert_eq!(
        find_in_dir_candidates("some-bare-tool", &[dir.path().to_path_buf()]),
        Some(bin)
    );
}
