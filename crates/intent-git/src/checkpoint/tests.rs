use super::*;
use crate::testutil::{commit_file, init_repo, write_file};
use std::fs;

fn stage(path: &Path, file: &str) {
    let repo = Repository::open(path).unwrap();
    let mut index = repo.index().unwrap();
    index.add_path(Path::new(file)).unwrap();
    index.write().unwrap();
}

type Fingerprint = (String, Vec<u8>, Vec<(Vec<u8>, u32)>);

fn fingerprint(path: &Path) -> Fingerprint {
    let repo = Repository::open(path).unwrap();
    let mut options = git2::StatusOptions::new();
    options.include_untracked(true).recurse_untracked_dirs(true);
    let status = repo
        .statuses(Some(&mut options))
        .unwrap()
        .iter()
        .map(|e| (e.path_bytes().to_vec(), e.status().bits()))
        .collect();
    let head = repo.head().unwrap().target().unwrap().to_string();
    (
        head,
        fs::read(repo.index().unwrap().path().unwrap()).unwrap(),
        status,
    )
}

#[test]
fn checkpoint_roundtrip_preserves_index_worktree_binary_deletions_and_untracked() {
    let dir = init_repo("checkpoint");
    let src = dir.path();
    for name in ["mixed", "deleted", "recreated", "unstaged-delete", "binary"] {
        commit_file(src, name, "base\n");
    }
    write_file(src, "mixed", "staged\n");
    stage(src, "mixed");
    write_file(src, "mixed", "unstaged\n");
    fs::write(src.join("binary"), [0, 255, 8, 7]).unwrap();
    stage(src, "binary");
    fs::write(src.join("binary"), [255, 0, 7, 8]).unwrap();
    for name in ["deleted", "recreated"] {
        fs::remove_file(src.join(name)).unwrap();
        let repo = Repository::open(src).unwrap();
        let mut index = repo.index().unwrap();
        index.remove_path(Path::new(name)).unwrap();
        index.write().unwrap();
    }
    fs::remove_file(src.join("unstaged-delete")).unwrap();
    write_file(src, "recreated", "untracked after staged delete\n");
    write_file(src, "new", "untracked\n");
    let before = fingerprint(src);
    let snapshot = capture(src, &CaptureOptions::default()).unwrap();
    assert_eq!(fingerprint(src), before, "capture changed live state");
    let temp = tempfile::tempdir().unwrap();
    let dst = temp.path().join("restored");
    restore(src, &snapshot, &dst).unwrap();
    let after = fingerprint(&dst);
    assert_eq!(after.0, before.0);
    assert_eq!(after.2, before.2);
    for name in ["mixed", "binary", "recreated", "new"] {
        assert_eq!(
            fs::read(dst.join(name)).unwrap(),
            fs::read(src.join(name)).unwrap()
        );
    }
    for name in ["deleted", "unstaged-delete"] {
        assert!(!dst.join(name).exists());
    }
    let source = Repository::open(src).unwrap();
    let target = Repository::open(&dst).unwrap();
    assert_eq!(
        source.index().unwrap().write_tree().unwrap(),
        target.index().unwrap().write_tree().unwrap()
    );
    assert_eq!(fingerprint(src), before);
}

#[test]
fn checkpoint_interruption_and_detected_races_leave_source_intact() {
    let dir = init_repo("checkpoint-interrupt");
    commit_file(dir.path(), "file", "base");
    write_file(dir.path(), "file", "dirty");
    let before = fingerprint(dir.path());
    assert!(
        capture_checked(dir.path(), &CaptureOptions::default(), || Err(invalid(
            "interrupted"
        )))
        .is_err()
    );
    assert_eq!(fingerprint(dir.path()), before);
    let err = capture_checked(dir.path(), &CaptureOptions::default(), || {
        write_file(dir.path(), "file", "external writer");
        Ok(())
    })
    .unwrap_err();
    assert!(err.to_string().contains("source changed"), "{err}");
    assert_eq!(fingerprint(dir.path()).0, before.0);
    assert_eq!(fingerprint(dir.path()).1, before.1);
    assert_eq!(
        fs::read_to_string(dir.path().join("file")).unwrap(),
        "external writer"
    );
}

#[test]
fn checkpoint_excludes_runtime_secrets_ignored_and_nested_repositories() {
    let dir = init_repo("checkpoint-exclusions");
    commit_file(dir.path(), ".gitignore", "ignored\n");
    for name in [
        ".codex/auth.json",
        ".claude/.credentials.json",
        ".intent/secrets/spawn",
        ".intent/attachments/a",
        "tool-outputs/t",
        "runtime/token",
        "ignored",
    ] {
        write_file(dir.path(), name, "SECRET_FIXTURE");
    }
    let nested = dir.path().join("nested");
    Repository::init(&nested).unwrap();
    write_file(&nested, "private", "SECRET_FIXTURE");
    write_file(dir.path(), "ordinary", "included");
    let options = CaptureOptions {
        excluded_paths: vec![PathBuf::from("runtime")],
    };
    let before = fingerprint(dir.path());
    let snapshot = capture(dir.path(), &options).unwrap();
    let repo = Repository::open(dir.path()).unwrap();
    let tree = repo
        .find_commit(oid(snapshot.wip.as_ref().unwrap()).unwrap())
        .unwrap()
        .tree()
        .unwrap();
    assert_eq!(tree.len(), 2);
    assert!(tree.get_name("ordinary").is_some());
    assert_eq!(fingerprint(dir.path()), before);
    stage(dir.path(), ".codex/auth.json");
    let before = fingerprint(dir.path());
    assert!(capture(dir.path(), &options)
        .unwrap_err()
        .to_string()
        .contains("runtime-owned"));
    assert_eq!(fingerprint(dir.path()), before);
}

#[test]
fn checkpoint_clean_detached_and_invalid_restore() {
    let dir = init_repo("checkpoint-clean");
    commit_file(dir.path(), "file", "base");
    let repo = Repository::open(dir.path()).unwrap();
    repo.set_head_detached(repo.head().unwrap().target().unwrap())
        .unwrap();
    let before = fingerprint(dir.path());
    let mut snapshot = capture(dir.path(), &CaptureOptions::default()).unwrap();
    assert!(snapshot.wip.is_none() && snapshot.branch.is_none());
    let tmp = tempfile::tempdir().unwrap();
    let dst = tmp.path().join("restore");
    restore(dir.path(), &snapshot, &dst).unwrap();
    assert!(Repository::open(&dst).unwrap().head_detached().unwrap());
    assert!(
        restore(dir.path(), &snapshot, &dst).is_err(),
        "never overwrite existing destination"
    );
    snapshot.object_format = "sha256".into();
    assert!(restore(dir.path(), &snapshot, &tmp.path().join("invalid")).is_err());
    assert!(!tmp.path().join("invalid").exists());
    assert_eq!(fingerprint(dir.path()), before);
}

#[test]
fn checkpoint_rejects_unborn_and_conflicted_indices() {
    let dir = init_repo("checkpoint-conflict");
    assert!(capture(dir.path(), &CaptureOptions::default()).is_err());
    commit_file(dir.path(), "file", "base");
    let repo = Repository::open(dir.path()).unwrap();
    let mut index = repo.index().unwrap();
    let mut entry = index.get_path(Path::new("file"), 0).unwrap();
    entry.flags = (entry.flags & !0x3000) | 0x1000;
    index.add(&entry).unwrap();
    index.write().unwrap();
    let before = fs::read(index.path().unwrap()).unwrap();
    assert!(capture(dir.path(), &CaptureOptions::default())
        .unwrap_err()
        .to_string()
        .contains("conflict"));
    assert_eq!(fs::read(index.path().unwrap()).unwrap(), before);
}

#[cfg(unix)]
#[test]
fn checkpoint_symlink_and_executable_roundtrip_without_following_links() {
    use std::os::unix::fs::{symlink, PermissionsExt};
    let dir = init_repo("checkpoint-links");
    commit_file(dir.path(), "file", "base");
    let outside = tempfile::tempdir().unwrap();
    fs::write(outside.path().join("secret"), "SECRET_FIXTURE").unwrap();
    symlink(outside.path().join("secret"), dir.path().join("link")).unwrap();
    write_file(dir.path(), "run", "#!/bin/sh\n");
    fs::set_permissions(dir.path().join("run"), fs::Permissions::from_mode(0o755)).unwrap();
    let socket = std::os::unix::net::UnixListener::bind(dir.path().join("socket")).unwrap();
    let before = fingerprint(dir.path());
    let snapshot = capture(dir.path(), &CaptureOptions::default()).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let dst = tmp.path().join("restore");
    restore(dir.path(), &snapshot, &dst).unwrap();
    assert_eq!(
        fs::read_link(dst.join("link")).unwrap(),
        outside.path().join("secret")
    );
    assert_ne!(
        fs::metadata(dst.join("run")).unwrap().permissions().mode() & 0o111,
        0
    );
    assert!(!dst.join("socket").exists());
    assert_eq!(fingerprint(dir.path()), before);
    drop(socket);
}

#[test]
fn checkpoint_paths_reject_cross_platform_escapes() {
    for path in [
        "",
        ".",
        "..",
        "/root",
        "a/../b",
        "a//b",
        "a/.git/config",
        "C:/root",
        "a\\b",
    ] {
        assert!(!safe_relative_path(path), "{path}");
    }
    assert!(safe_relative_path("packages/lib"));
}

#[test]
fn checkpoint_preserves_raw_crlf_worktree_bytes_and_provider_project_config() {
    let dir = init_repo("checkpoint-raw");
    commit_file(dir.path(), ".gitattributes", "*.txt text eol=lf\n");
    commit_file(dir.path(), "file.txt", "base\n");
    commit_file(dir.path(), ".claude/CLAUDE.md", "project instructions\n");
    fs::write(dir.path().join("file.txt"), b"changed\r\n").unwrap();
    let before = fingerprint(dir.path());
    let snapshot = capture(dir.path(), &CaptureOptions::default()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let dst = temp.path().join("restored");
    restore(dir.path(), &snapshot, &dst).unwrap();
    assert_eq!(fs::read(dst.join("file.txt")).unwrap(), b"changed\r\n");
    assert_eq!(
        fs::read(dst.join(".claude/CLAUDE.md")).unwrap(),
        b"project instructions\n"
    );
    assert_eq!(fingerprint(dir.path()), before);
}

#[cfg(unix)]
#[test]
fn checkpoint_non_utf8_filename_roundtrip() {
    use std::os::unix::ffi::OsStringExt;
    let dir = init_repo("checkpoint-non-utf8");
    commit_file(dir.path(), "file", "base");
    let name = std::ffi::OsString::from_vec(vec![b'x', 0xff]);
    fs::write(dir.path().join(&name), [1, 0, 255]).unwrap();
    let snapshot = capture(dir.path(), &CaptureOptions::default()).unwrap();
    let temp = tempfile::tempdir().unwrap();
    let dst = temp.path().join("restored");
    restore(dir.path(), &snapshot, &dst).unwrap();
    assert_eq!(fs::read(dst.join(name)).unwrap(), [1, 0, 255]);
}
