//! Unit tests for the `host.*` host-services pure operations.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

fn unique_temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("intent-host-{tag}-{pid}-{nanos}-{n}"));
    std::fs::create_dir_all(&dir).unwrap();
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
fn check_auggie_reports_unavailable_when_unresolved() {
    let v = check_auggie_with(None, &StubProbe(None));
    assert_eq!(v["available"], false);
}

#[test]
fn check_auggie_reports_available_with_version_and_path() {
    let v = check_auggie_with(
        Some(PathBuf::from("/opt/intent/auggie")),
        &StubProbe(Some("auggie 0.42.0".to_string())),
    );
    assert_eq!(v["available"], true);
    assert_eq!(v["version"], "auggie 0.42.0");
    assert_eq!(v["path"], "/opt/intent/auggie");
}

#[cfg(unix)]
#[test]
fn resolve_auggie_prefers_configured_path_when_it_exists() {
    use std::os::unix::fs::PermissionsExt;
    let dir = unique_temp_dir("auggie-cfg");
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
}

#[test]
fn list_directory_with_returns_home_when_path_is_empty_or_missing() {
    let home = unique_temp_dir("ls-empty");
    std::fs::create_dir_all(home.join("Documents")).unwrap();
    std::fs::write(home.join("readme.txt"), "hi").unwrap();
    let v = list_directory_with(None, &home).unwrap();
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
    let repo = home.join("repo");
    std::fs::create_dir_all(repo.join(".git")).unwrap();
    let plain = home.join("plain");
    std::fs::create_dir_all(&plain).unwrap();
    let v = list_directory_with(Some(home.to_str().unwrap()), &home).unwrap();
    let entries = v["entries"].as_array().unwrap();
    let repo_entry = entries.iter().find(|e| e["name"] == "repo").unwrap();
    assert_eq!(repo_entry["isGitRepo"], true);
    let plain_entry = entries.iter().find(|e| e["name"] == "plain").unwrap();
    assert_eq!(plain_entry["isGitRepo"], false);
}

#[test]
fn list_directory_with_marks_worktree_dot_git_file_as_git_repo() {
    let home = unique_temp_dir("ls-worktree");
    let wt = home.join("wt");
    std::fs::create_dir_all(&wt).unwrap();
    std::fs::write(wt.join(".git"), "gitdir: /elsewhere/.git/worktrees/wt\n").unwrap();
    let v = list_directory_with(Some(home.to_str().unwrap()), &home).unwrap();
    let entries = v["entries"].as_array().unwrap();
    let wt_entry = entries.iter().find(|e| e["name"] == "wt").unwrap();
    assert_eq!(wt_entry["isGitRepo"], true);
}

#[test]
fn list_directory_with_expands_tilde() {
    let home = unique_temp_dir("ls-tilde");
    std::fs::create_dir_all(home.join("inside")).unwrap();
    let v = list_directory_with(Some("~"), &home).unwrap();
    assert_eq!(v["path"], home.to_string_lossy().into_owned());
    let entries = v["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["name"], "inside");
}

#[test]
fn list_directory_with_returns_error_for_missing_path() {
    let home = unique_temp_dir("ls-missing");
    let missing = home.join("nope");
    let err = list_directory_with(Some(missing.to_str().unwrap()), &home).unwrap_err();
    assert!(err.contains("nope"));
}

#[test]
fn directory_status_with_reports_existing_git_repo() {
    let home = unique_temp_dir("ds-repo");
    let repo = home.join("repo");
    std::fs::create_dir_all(repo.join(".git")).unwrap();
    let v = directory_status_with(repo.to_str().unwrap(), &home);
    assert_eq!(v["exists"], true);
    assert_eq!(v["isDirectory"], true);
    assert_eq!(v["isGitRepo"], true);
    assert_eq!(v["isSubdirectoryOfGitRepo"], false);
    assert!(v.get("parentGitRoot").is_none());
}

#[test]
fn directory_status_with_reports_subdirectory_of_git_repo() {
    let home = unique_temp_dir("ds-sub");
    let repo = home.join("repo");
    std::fs::create_dir_all(repo.join(".git")).unwrap();
    let sub = repo.join("src").join("inner");
    std::fs::create_dir_all(&sub).unwrap();
    let v = directory_status_with(sub.to_str().unwrap(), &home);
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
    let missing = home.join("absent");
    let v = directory_status_with(missing.to_str().unwrap(), &home);
    assert_eq!(v["exists"], false);
    assert_eq!(v["isDirectory"], false);
    assert_eq!(v["isGitRepo"], false);
    assert_eq!(v["isSubdirectoryOfGitRepo"], false);
}

#[test]
fn directory_status_with_handles_worktree_dot_git_file() {
    let home = unique_temp_dir("ds-worktree");
    let wt = home.join("wt");
    std::fs::create_dir_all(&wt).unwrap();
    std::fs::write(wt.join(".git"), "gitdir: /elsewhere/.git/worktrees/wt\n").unwrap();
    let v = directory_status_with(wt.to_str().unwrap(), &home);
    assert_eq!(v["isGitRepo"], true);
    assert_eq!(v["isSubdirectoryOfGitRepo"], false);
}

#[test]
fn directory_status_with_treats_empty_dir_as_empty() {
    let home = unique_temp_dir("ds-empty");
    let empty = home.join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    let v = directory_status_with(empty.to_str().unwrap(), &home);
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
fn enhanced_path_dedups_and_appends_essentials_order_preserving() {
    let sep = path_separator();
    let custom = "/opt/tools";
    let input = format!("{custom}{sep}{custom}");
    let enhanced = enhanced_path_from(&input);
    let parts: Vec<&str> = enhanced.split(sep).collect();
    // The caller's entry comes first and appears exactly once (de-duplicated).
    assert_eq!(parts[0], custom);
    assert_eq!(parts.iter().filter(|p| **p == custom).count(), 1);
    // Every essential system path is merged in.
    for essential in essential_system_paths() {
        assert!(
            parts.iter().any(|p| *p == essential),
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
        vec!["PATH".to_string(), "SECRET_TOKEN".to_string()],
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
