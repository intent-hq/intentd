use super::*;
use crate::transfer_submodules::test_fixture::{
    git as run_git, init_repo, local_commit, superproject_with_submodule,
};
use std::fs;

fn inputs(root: &Path) -> Vec<RepositoryInput> {
    let mut paths = vec![".".to_string()];
    paths.extend(
        crate::transfer_submodules::checkpoint_submodules(root)
            .unwrap()
            .into_iter()
            .map(|s| s.path),
    );
    paths
        .into_iter()
        .enumerate()
        .map(|(n, path)| RepositoryInput {
            repo_key: format!("repo-{n}"),
            fork_base: run_git(&root.join(&path), &["rev-parse", "HEAD"]),
            path,
            inherited: None,
            exclusions: CaptureOptions::default(),
        })
        .collect()
}

fn index_bytes(root: &Path) -> Vec<u8> {
    let repo = git2::Repository::open(root).unwrap();
    fs::read(repo.index().unwrap().path().unwrap()).unwrap()
}

fn manifest(repos: Vec<RepositoryEntry>) -> Manifest {
    Manifest {
        format_version: 1,
        checkpoint_id: uuid::Uuid::new_v4().to_string(),
        workspace_id: "ws-1".into(),
        agent_id: "agent-1".into(),
        lease_id: "lease-1".into(),
        incarnation: uuid::Uuid::new_v4().to_string(),
        run_id: uuid::Uuid::new_v4().to_string(),
        assignment_epoch: "3".into(),
        capture_revision: "8".into(),
        captured_at: "2026-09-28T09:00:00Z".into(),
        journal_seq: "42".into(),
        repos,
        session: session::Session::history("codex", "42").unwrap(),
        attachments: Vec::new(),
    }
}

#[test]
fn checkpoint_nested_dirty_unpublished_roundtrip_keeps_original_gitlinks() {
    let temp = tempfile::tempdir().unwrap();
    let (root, _) = superproject_with_submodule(temp.path());
    let sub = root.join("sub");
    let inner_origin = temp.path().join("inner-src");
    init_repo(&inner_origin);
    run_git(
        &sub,
        &[
            "submodule",
            "add",
            "-q",
            inner_origin.to_str().unwrap(),
            "inner",
        ],
    );
    run_git(&sub, &["commit", "-qm", "add nested"]);
    let inner = sub.join("inner");
    let unpublished = local_commit(&inner, "unpublished");
    fs::write(inner.join("README.md"), "staged").unwrap();
    run_git(&inner, &["add", "README.md"]);
    fs::write(inner.join("README.md"), "unstaged").unwrap();
    fs::write(inner.join("binary"), [0, 255, 4]).unwrap();
    fs::write(sub.join("parent-dirt"), "dirty").unwrap();
    fs::write(root.join("root-dirt"), "dirty").unwrap();
    let before: Vec<_> = [&root, &sub, &inner]
        .iter()
        .map(|p| {
            (
                index_bytes(p),
                run_git(p, &["rev-parse", "HEAD"]),
                run_git(p, &["status", "--porcelain"]),
            )
        })
        .collect();
    let requests = inputs(&root);
    let repos = capture_repositories(&root, &requests).unwrap();
    assert_eq!(repos.len(), 3);
    assert_eq!(repos[2].snapshot.head, unpublished);
    assert!(repos.iter().all(|r| r.snapshot.wip.is_some()));
    let wire = manifest(repos.clone());
    wire.validate().unwrap();
    let encoded = serde_json::to_value(&wire).unwrap();
    assert!(
        encoded["repos"][0].get("head").is_some(),
        "snapshot is flattened"
    );
    assert_eq!(serde_json::from_value::<Manifest>(encoded).unwrap(), wire);
    let sources: HashMap<_, _> = requests
        .iter()
        .map(|r| (r.repo_key.clone(), root.join(&r.path)))
        .collect();
    let destination = temp.path().join("restore");
    restore_repositories(&repos, &sources, &destination).unwrap();
    for (i, rel) in [".", "sub", "sub/inner"].iter().enumerate() {
        let path = root.join(rel);
        assert_eq!(index_bytes(&path), before[i].0);
        assert_eq!(run_git(&path, &["rev-parse", "HEAD"]), before[i].1);
        assert_eq!(run_git(&path, &["status", "--porcelain"]), before[i].2);
        assert_eq!(
            run_git(&destination.join(rel), &["rev-parse", "HEAD"]),
            before[i].1
        );
        assert_eq!(
            run_git(&destination.join(rel), &["status", "--porcelain"]),
            before[i].2
        );
        assert_eq!(
            run_git(&destination.join(rel), &["write-tree"]),
            run_git(&path, &["write-tree"])
        );
    }
    assert_eq!(
        fs::read(destination.join("sub/inner/binary")).unwrap(),
        [0, 255, 4]
    );
    assert_eq!(
        fs::read_to_string(destination.join("sub/inner/README.md")).unwrap(),
        "unstaged"
    );
}

#[test]
fn checkpoint_partial_capture_and_restore_never_publish_partial_tree() {
    let temp = tempfile::tempdir().unwrap();
    let (root, _) = superproject_with_submodule(temp.path());
    let mut requests = inputs(&root);
    fs::write(root.join("dirty"), "must remain").unwrap();
    let before = index_bytes(&root);
    requests[1]
        .exclusions
        .excluded_paths
        .push(PathBuf::from("../invalid"));
    assert!(capture_repositories(&root, &requests).is_err());
    assert_eq!(index_bytes(&root), before);
    requests[1].exclusions.excluded_paths.clear();
    let repos = capture_repositories(&root, &requests).unwrap();
    let sources = HashMap::from([(repos[0].repo_key.clone(), root.clone())]);
    let dst = temp.path().join("partial");
    assert!(
        restore_repositories(&repos, &sources, &dst).is_err(),
        "missing child objects"
    );
    assert!(!dst.exists(), "partial root checkout removed");
    assert_eq!(index_bytes(&root), before);
    assert_eq!(
        fs::read_to_string(root.join("dirty")).unwrap(),
        "must remain"
    );
}

#[test]
fn checkpoint_manifest_rejects_version_paths_edges_watermarks_and_preserves_inherited() {
    let temp = tempfile::tempdir().unwrap();
    let (root, _) = superproject_with_submodule(temp.path());
    let mut repos = capture_repositories(&root, &inputs(&root)).unwrap();
    repos[0].inherited = Some(Inherited {
        source_agent_id: "parent".into(),
        checkpoint_id: uuid::Uuid::new_v4().to_string(),
        execution_base: repos[0].snapshot.head.clone(),
    });
    let good = manifest(repos);
    good.validate().unwrap();
    let json = serde_json::to_string(&good).unwrap();
    assert_eq!(serde_json::from_str::<Manifest>(&json).unwrap(), good);
    let mut bad = good.clone();
    bad.format_version = 2;
    assert!(bad.validate().is_err());
    let mut bad = good.clone();
    bad.repos[1].path = "../escape".into();
    assert!(bad.validate().is_err());
    let mut bad = good.clone();
    bad.repos[1].repo_key = bad.repos[0].repo_key.clone();
    assert!(bad.validate().is_err());
    let mut bad = good.clone();
    bad.repos[0].submodules.clear();
    assert!(bad.validate().is_err());
    let mut bad = good.clone();
    bad.session.through_seq = "41".into();
    assert!(bad.validate().is_err());
    let mut bad = good.clone();
    bad.capture_revision = "18446744073709551616".into();
    assert!(bad.validate().is_err());
    let mut bad = good.clone();
    bad.capture_revision = "08".into();
    assert!(bad.validate().is_err());
    let mut bad = good;
    bad.repos.swap(0, 1);
    assert!(bad.validate().is_err());
}

#[test]
fn checkpoint_uninitialized_child_is_gitlink_only_and_missing_grants_fail() {
    let temp = tempfile::tempdir().unwrap();
    let (root, _) = superproject_with_submodule(temp.path());
    let requests = inputs(&root);
    assert!(capture_repositories(&root, &requests[..1]).is_err());
    run_git(&root, &["submodule", "deinit", "-f", "sub"]);
    let requests = inputs(&root);
    assert_eq!(requests.len(), 1);
    let repos = capture_repositories(&root, &requests).unwrap();
    assert!(repos[0].submodules.is_empty());
    assert!(repos[0].snapshot.wip.is_none());
}

#[cfg(unix)]
#[test]
fn checkpoint_restore_rejects_submodule_symlink_escape_without_touching_outside() {
    let temp = tempfile::tempdir().unwrap();
    let (root, _) = superproject_with_submodule(temp.path());
    let outside = temp.path().join("outside");
    fs::create_dir_all(outside.join("sub")).unwrap();
    std::os::unix::fs::symlink(&outside, root.join("escape")).unwrap();
    let requests = inputs(&root);
    let mut repos = capture_repositories(&root, &requests).unwrap();
    repos[0].submodules[0].path = "escape/sub".into();
    repos[1].path = "escape/sub".into();
    let sources: HashMap<_, _> = requests
        .iter()
        .map(|r| (r.repo_key.clone(), root.join(&r.path)))
        .collect();
    let destination = temp.path().join("restore");
    assert!(restore_repositories(&repos, &sources, &destination).is_err());
    assert!(!destination.exists());
    assert!(
        outside.join("sub").is_dir(),
        "even empty external directories remain intact"
    );
    fs::create_dir(&destination).unwrap();
    fs::write(destination.join("owned-by-user"), "keep").unwrap();
    assert!(restore_repositories(&repos, &sources, &destination).is_err());
    assert_eq!(
        fs::read_to_string(destination.join("owned-by-user")).unwrap(),
        "keep"
    );
}

#[test]
fn checkpoint_deleted_submodule_roundtrip_preserves_staging() {
    for staged in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (root, _) = superproject_with_submodule(temp.path());
        fs::remove_dir_all(root.join("sub")).unwrap();
        if staged {
            run_git(&root, &["rm", "--cached", "sub"]);
        }
        let before = (index_bytes(&root), run_git(&root, &["show-ref"]));
        let requests = inputs(&root);
        assert_eq!(requests.len(), 1);
        let repos = capture_repositories(&root, &requests).unwrap();
        assert!(repos[0].snapshot.wip.is_some());
        assert!(repos[0].submodules.is_empty());
        let sources = HashMap::from([(repos[0].repo_key.clone(), root.clone())]);
        let dst = temp.path().join("restore");
        restore_repositories(&repos, &sources, &dst).unwrap();
        assert!(!dst.join("sub").exists());
        let staged_tree = |path| {
            git2::Repository::open(path)
                .unwrap()
                .index()
                .unwrap()
                .write_tree()
                .unwrap()
        };
        assert_eq!(staged_tree(&dst), staged_tree(&root));
        assert_eq!(
            run_git(&dst, &["--no-optional-locks", "status", "--porcelain"]),
            run_git(&root, &["--no-optional-locks", "status", "--porcelain"])
        );
        assert_eq!((index_bytes(&root), run_git(&root, &["show-ref"])), before);
    }
}

#[test]
fn checkpoint_inventory_rejects_submodule_file_replacement_without_mutation() {
    for staged in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (root, _) = superproject_with_submodule(temp.path());
        let requests = inputs(&root);
        fs::remove_dir_all(root.join("sub")).unwrap();
        fs::write(root.join("sub"), [0, 255, 4]).unwrap();
        if staged {
            run_git(&root, &["add", "sub"]);
        }
        let before = (index_bytes(&root), run_git(&root, &["show-ref"]));
        // Inventory deliberately fails closed on non-directory child paths;
        // the standalone Git codec can still roundtrip the replacement.
        assert!(capture_repositories(&root, &requests[..1]).is_err());
        assert_eq!((index_bytes(&root), run_git(&root, &["show-ref"])), before);
        assert_eq!(fs::read(root.join("sub")).unwrap(), [0, 255, 4]);
    }
}

#[cfg(unix)]
#[test]
fn checkpoint_inventory_rejects_submodule_symlink_without_mutation() {
    for staged in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (root, _) = superproject_with_submodule(temp.path());
        let requests = inputs(&root);
        fs::remove_dir_all(root.join("sub")).unwrap();
        std::os::unix::fs::symlink("README.md", root.join("sub")).unwrap();
        if staged {
            run_git(&root, &["add", "sub"]);
        }
        let before = (index_bytes(&root), run_git(&root, &["show-ref"]));
        let err = capture_repositories(&root, &requests[..1]).unwrap_err();
        assert!(err.to_string().contains("symlink"));
        assert_eq!((index_bytes(&root), run_git(&root, &["show-ref"])), before);
        assert_eq!(
            fs::read_link(root.join("sub")).unwrap(),
            Path::new("README.md")
        );
    }
}
