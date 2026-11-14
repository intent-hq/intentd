use super::*;

#[test]
fn file_git_and_search_calls_are_routed_but_hub_and_upload_calls_are_not() {
    for method in [
        "file.write",
        "file.read",
        "git.status",
        "git.agentCommit",
        "git.pull",
        "git.fetch",
        "file-tracking.getChanges",
        "search.inFiles",
        "file.getAttachmentInfo",
    ] {
        assert!(routed(method), "{method}");
    }
    for method in [
        "git.push",
        "git.clone",
        "git.getConfig",
        "git.getRemoteUrl",
        "git.removeLockFile",
        "file.placeAttachment",
        "file.attachmentUpload.begin",
        "file.attachmentUpload.chunk",
        "file.attachmentUpload.commit",
        "file.attachmentUpload.abort",
        "host.exec",
        "terminal.create",
    ] {
        assert!(!routed(method), "{method}");
    }
}

#[test]
fn absolute_paths_under_the_worktree_become_checkout_paths() {
    let dir = tempfile::tempdir().unwrap();
    let worktree = dir.path().join("work");
    std::fs::create_dir_all(worktree.join("src")).unwrap();
    let abs = |rel: &str| worktree.join(rel).to_string_lossy().into_owned();
    assert_eq!(
        checkout_relative(&worktree, "src/a.rs").as_deref(),
        Some("src/a.rs")
    );
    assert_eq!(
        checkout_relative(&worktree, &abs("src/a.rs")).as_deref(),
        Some("src/a.rs")
    );
    // A file that does not exist yet (a write makes it).
    assert_eq!(
        checkout_relative(&worktree, &abs("new/deeper/b.txt")).as_deref(),
        Some("new/deeper/b.txt")
    );
    assert_eq!(
        checkout_relative(&worktree, &worktree.to_string_lossy()).as_deref(),
        Some("")
    );
    // Elsewhere, a sibling sharing the prefix, or climbing out.
    assert_eq!(checkout_relative(&worktree, "/etc/passwd"), None);
    let sibling = dir.path().join("work2").join("x");
    assert_eq!(
        checkout_relative(&worktree, &sibling.to_string_lossy()),
        None
    );
    assert_eq!(checkout_relative(&worktree, &abs("src/../../x")), None);
}

#[cfg(unix)]
#[test]
fn a_worktree_reached_through_a_link_maps_too() {
    let dir = tempfile::tempdir().unwrap();
    let real = dir.path().join("real");
    std::fs::create_dir_all(real.join("src")).unwrap();
    let link = dir.path().join("link");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    let through_link = link.join("src").join("a.rs");
    assert_eq!(
        checkout_relative(&real, &through_link.to_string_lossy()).as_deref(),
        Some("src/a.rs")
    );
    let real_path = real.join("src").join("a.rs");
    assert_eq!(
        checkout_relative(&link, &real_path.to_string_lossy()).as_deref(),
        Some("src/a.rs")
    );
}

#[test]
fn params_are_mapped_and_outside_paths_refused() {
    let dir = tempfile::tempdir().unwrap();
    let worktree = dir.path().join("work");
    std::fs::create_dir_all(&worktree).unwrap();
    let abs = |rel: &str| worktree.join(rel).to_string_lossy().into_owned();
    let params = json!({
        "workspaceId": "ws",
        "path": abs("a.txt"),
        "oldPath": "b.txt",
        "paths": [abs("c.txt"), "d.txt"],
        "files": format!("{}, e.txt", abs("f.txt")),
        "repoPath": abs(""),
        "content": abs("not/a/path/param"),
    });
    let Value::Object(params) = params else {
        unreachable!()
    };
    let mapped = home_params(&worktree, params).unwrap();
    assert_eq!(mapped["path"], "a.txt");
    assert_eq!(mapped["oldPath"], "b.txt");
    assert_eq!(mapped["paths"], json!(["c.txt", "d.txt"]));
    assert_eq!(mapped["files"], "f.txt,e.txt");
    assert_eq!(mapped["repoPath"], "");
    assert_eq!(
        mapped["content"],
        abs("not/a/path/param"),
        "contents are left alone"
    );
    assert_eq!(mapped["workspaceId"], "ws");

    for (param, value) in [
        ("path", json!("/etc/passwd")),
        ("paths", json!(["ok.txt", "/etc/passwd"])),
        ("files", json!("ok.txt,/etc/passwd")),
        ("filePath", json!("/tmp/elsewhere")),
    ] {
        let mut params = Map::new();
        params.insert(param.to_owned(), value);
        let error = home_params(&worktree, params).unwrap_err();
        assert!(
            matches!(&error, Error::InvalidParams(message) if message.contains("outside the workspace's checkout")),
            "{param}: {error:?}"
        );
    }
}

#[test]
fn a_path_based_call_for_another_repository_runs_here() {
    let dir = tempfile::tempdir().unwrap();
    let worktree = dir.path().join("work");
    std::fs::create_dir_all(&worktree).unwrap();
    let with = |repo: Option<&str>| {
        let mut params = Map::new();
        if let Some(repo) = repo {
            params.insert("repoPath".into(), json!(repo));
        }
        params
    };
    assert!(names_checkout(
        &worktree,
        &with(Some(&worktree.to_string_lossy()))
    ));
    assert!(names_checkout(&worktree, &with(None)));
    assert!(!names_checkout(&worktree, &with(Some("/some/other/repo"))));
    assert!(!names_checkout(&worktree, &with(Some("relative"))));
}

#[tokio::test]
async fn payload_pieces_cover_the_bytes_in_order() {
    let bytes: Vec<u8> = (0..(HOME_PIECE_BYTES * 2 + 17))
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("payload.bin");
    std::fs::write(&file, &bytes).unwrap();
    let expected = sha2::Sha256::digest(&bytes)
        .iter()
        .fold(String::new(), |mut hex, b| {
            use std::fmt::Write as _;
            let _ = write!(hex, "{b:02x}");
            hex
        });
    for source in [
        crate::file_ops::AttachmentSource::Bytes(&bytes),
        crate::file_ops::AttachmentSource::CopyFrom(&file),
    ] {
        let payload = Payload::open(&source).await.unwrap();
        assert_eq!(payload.size, bytes.len() as u64);
        let mut joined = Vec::new();
        for seq in 0..3 {
            let piece = payload.piece(seq).await.unwrap();
            assert!(piece.len() <= HOME_PIECE_BYTES);
            joined.extend(piece);
        }
        assert_eq!(joined, bytes);
        assert_eq!(payload.sha256().await.unwrap(), expected);
    }
    let missing = dir.path().join("missing.bin");
    let source = crate::file_ops::AttachmentSource::CopyFrom(&missing);
    assert!(matches!(
        Payload::open(&source).await,
        Err(Error::InvalidParams(message)) if message.contains("does not exist")
    ));
}
