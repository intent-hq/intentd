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
    let apps = root.join("Applications");
    std::fs::create_dir_all(&apps).unwrap();
    let dirs = vec![apps];
    assert!(find_macos_app_bundle("DoesNotExist", &dirs).is_none());
}

#[test]
fn find_macos_app_bundle_rejects_unsafe_names() {
    let root = unique_temp_dir("find-app-unsafe");
    let apps = root.join("Applications");
    std::fs::create_dir_all(&apps).unwrap();
    assert!(find_macos_app_bundle("../escape", &[apps]).is_none());
}

#[test]
fn find_app_with_returns_installed_with_source_on_macos() {
    let root = unique_temp_dir("find-app-with");
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
    let apps = root.join("Applications");
    std::fs::create_dir_all(apps.join("Zed.app")).unwrap();
    let v = find_app_with("Zed", &[apps], false);
    assert_eq!(v["installed"], false);
    assert!(v.get("path").is_none());
}

#[test]
fn find_app_with_returns_uninstalled_for_unsafe_name() {
    let root = unique_temp_dir("find-app-unsafe-name");
    let apps = root.join("Applications");
    std::fs::create_dir_all(&apps).unwrap();
    let v = find_app_with("../../etc/passwd", &[apps], true);
    assert_eq!(v["installed"], false);
}

#[test]
fn list_installed_editors_with_detects_macos_app_bundles() {
    let root = unique_temp_dir("list-mac");
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

/// Regression test: run_version_with() enriches PATH with binary's parent dir,
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

    // Prove regression coverage: probe FAILS with minimal PATH that lacks the script dir
    let minimal_path = std::ffi::OsString::from("/usr/bin:/bin");
    let version_minimal = super::run_version_with(&fake_auggie, &minimal_path);
    assert!(
        version_minimal.is_none(),
        "run_version_with minimal PATH should fail when node is NOT on PATH (proves fix is needed)"
    );
}

/// Test that enhanced_path() includes enriched directories in correct precedence.
#[test]
fn enhanced_path_includes_enriched_dirs_with_correct_precedence() {
    use intent_core::path_utils;
    use intent_providers::args::enhanced_path;

    let fake_provider_bin = std::path::PathBuf::from("/fake/provider/bin/auggie");
    let path = enhanced_path(Some(&fake_provider_bin));

    // Split path and check precedence
    let sep = if cfg!(windows) { ';' } else { ':' };
    let parts: Vec<&str> = path.split(sep).collect();

    // 1. Provider binary's parent dir should be first
    assert!(
        parts
            .first()
            .map(|p| p.contains("/fake/provider/bin"))
            .unwrap_or(false),
        "Provider binary parent dir should be first in PATH"
    );

    // 2. Should include ~/.augment/bin early
    let has_augment_bin = parts.iter().any(|p| p.contains(".augment/bin"));
    assert!(has_augment_bin, "PATH should include ~/.augment/bin");

    // 3. Should include at least some enhanced dirs (can't test all as they're system-dependent)
    // Just verify path is longer than minimal (more than just provider + augment)
    assert!(
        parts.len() >= 3,
        "Enhanced path should include multiple directories, got {} parts",
        parts.len()
    );

    // 4. Verify deduplication: get enriched dirs and check no directory appears twice
    let enriched_dirs = path_utils::enhanced_path_dirs();
    let dir_strings: Vec<String> = enriched_dirs
        .iter()
        .map(|d| d.to_string_lossy().to_string())
        .collect();
    let unique_count = dir_strings
        .iter()
        .collect::<std::collections::HashSet<_>>()
        .len();
    assert_eq!(
        dir_strings.len(),
        unique_count,
        "enhanced_path_dirs should not contain duplicates"
    );
}
