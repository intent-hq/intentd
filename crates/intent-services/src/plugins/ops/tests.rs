use super::*;
use crate::plugins::{MANIFEST_FILE, PLUGIN_SCHEMA};

fn write(path: &Path, body: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, body).unwrap();
}

fn source(dir: &Path, name: &str, version: &str) {
    write(
        &dir.join(MANIFEST_FILE),
        &json!({ "$schema": PLUGIN_SCHEMA, "name": name, "version": version }).to_string(),
    );
    write(
        &dir.join("skills/review/SKILL.md"),
        "---\nname: review\ndescription: Reviews.\n---\n",
    );
    write(&dir.join(".git/HEAD"), "ref: refs/heads/main\n");
}

fn setup() -> (tempfile::TempDir, PluginPaths, PathBuf) {
    let tmp = tempfile::tempdir().unwrap();
    let paths = PluginPaths::new(tmp.path().join(".intent"));
    let src = tmp.path().join("src/acme");
    source(&src, "acme", "1.0.0");
    (tmp, paths, src)
}

#[tokio::test]
async fn install_copies_validates_and_records_the_source() {
    let (_tmp, paths, src) = setup();
    let out = install_at(&paths, src.to_str().unwrap()).await.unwrap();
    let plugin = &out["plugin"];
    assert_eq!(plugin["name"], "acme");
    assert_eq!(plugin["enabled"], true);
    assert_eq!(plugin["status"], "ok");
    assert_eq!(plugin["sourceKind"], "path");
    assert_eq!(plugin["skills"], json!(["review"]));
    assert!(paths
        .plugin_dir("acme")
        .join("skills/review/SKILL.md")
        .is_file());
    assert!(!paths.plugin_dir("acme").join(".git").exists());

    let again = install_at(&paths, src.to_str().unwrap()).await.unwrap_err();
    assert!(matches!(again, Error::InvalidParams(_)), "{again:?}");
}

#[tokio::test]
async fn invalid_sources_leave_nothing_behind() {
    let (tmp, paths, _src) = setup();
    let bad = tmp.path().join("src/bad");
    write(&bad.join(MANIFEST_FILE), "{}");
    for source in [
        bad.to_str().unwrap(),
        "relative/path",
        "",
        "/does/not/exist",
    ] {
        let err = install_at(&paths, source).await.unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)), "{source}: {err:?}");
    }
    let leftovers: Vec<_> = std::fs::read_dir(paths.plugins_dir())
        .map(|d| {
            d.filter_map(std::result::Result::ok)
                .map(|e| e.file_name())
                .collect()
        })
        .unwrap_or_default();
    assert!(leftovers.is_empty(), "{leftovers:?}");
}

#[tokio::test]
async fn update_refreshes_from_source_and_keeps_data_and_state() {
    let (_tmp, paths, src) = setup();
    install_at(&paths, src.to_str().unwrap()).await.unwrap();
    toggle_at(&paths, "acme", false).await.unwrap();
    write(&paths.data_dir("acme").join("db"), "state");
    source(&src, "acme", "2.0.0");

    let out = update_at(&paths, "acme").await.unwrap();
    assert_eq!(out["plugin"]["version"], "2.0.0");
    assert_eq!(out["plugin"]["enabled"], false);
    assert!(paths.data_dir("acme").join("db").is_file());

    source(&src, "renamed", "3.0.0");
    let err = update_at(&paths, "acme").await.unwrap_err();
    assert!(matches!(err, Error::InvalidParams(_)), "{err:?}");
    assert_eq!(list_value(&paths)["plugins"][0]["version"], "2.0.0");
}

#[tokio::test]
async fn uninstall_removes_plugin_and_optionally_data() {
    let (_tmp, paths, src) = setup();
    install_at(&paths, src.to_str().unwrap()).await.unwrap();
    write(&paths.data_dir("acme").join("db"), "state");
    uninstall_at(&paths, "acme", true).await.unwrap();
    assert!(!paths.plugin_dir("acme").exists());
    assert!(paths.data_dir("acme").exists());
    assert!(paths.read_registry().plugins.is_empty());

    install_at(&paths, src.to_str().unwrap()).await.unwrap();
    uninstall_at(&paths, "acme", false).await.unwrap();
    assert!(!paths.data_dir("acme").exists());

    for name in ["acme", "../x", ".staging"] {
        let err = uninstall_at(&paths, name, false).await.unwrap_err();
        assert!(matches!(err, Error::NotFound(_)), "{name}: {err:?}");
    }
}

#[tokio::test]
async fn hand_placed_folders_are_listed_and_toggleable() {
    let (_tmp, paths, src) = setup();
    copy_tree(&src, &paths.plugin_dir("acme")).unwrap();
    write(&paths.plugin_dir("broken").join(MANIFEST_FILE), "{");
    let listed = list_value(&paths);
    assert_eq!(listed["plugins"][0]["name"], "acme");
    assert_eq!(listed["plugins"][0]["enabled"], true);
    assert!(listed["plugins"][0].get("source").is_none());
    assert_eq!(listed["plugins"][1]["status"], "invalid");

    let out = toggle_at(&paths, "acme", false).await.unwrap();
    assert_eq!(out["plugin"]["enabled"], false);
    let err = update_at(&paths, "acme").await.unwrap_err();
    assert!(matches!(err, Error::InvalidParams(_)), "{err:?}");
}
