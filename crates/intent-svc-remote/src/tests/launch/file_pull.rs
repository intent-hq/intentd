//! `ws.file.pull` through head: a lease agent's `file.pullData` frames are
//! served from a local agent's checkout on head, or read on the source
//! agent's machine with `checkoutRead` and relayed; refused for unknown
//! agents, agents of another workspace, paths outside the checkout and a
//! machine on a link older than nodeProtocol 16.

use base64::Engine as _;
use intent_core::node_link::{
    CheckoutEntryKind, CheckoutPiece, CheckoutReadOutcome, CheckoutReadReply,
    CHECKOUT_FILES_PROTOCOL,
};
use intent_core::remote_launch::workspace_api::{
    MAX_LINK_ATTACHMENT_CHUNK_BYTES, PULL_DATA_METHOD,
};

use super::*;

/// A local (backend) agent of `workspace_id`.
async fn local_agent(f: &Fixture, workspace_id: &WorkspaceId) -> AgentId {
    let agent = AgentId::new();
    let now = intent_core::now_iso();
    sqlx::query("INSERT INTO agent_session (id, workspace_id, name, status, provider, created_at, updated_at) VALUES (?, ?, 'local', 'idle', 'mock', ?, ?)")
        .bind(agent.as_str())
        .bind(workspace_id.as_str())
        .bind(&now)
        .bind(&now)
        .execute(f.services.store().write_pool())
        .await
        .unwrap();
    agent
}

fn args(from: &AgentId, path: &str) -> Value {
    json!({ "fromAgentId": from, "path": path, "offset": 0, "length": 1024 })
}

fn piece(value: Value) -> CheckoutPiece {
    serde_json::from_value(value).unwrap()
}

#[tokio::test]
async fn head_serves_pull_pieces_from_local_and_remote_checkouts() {
    let (f, mut link, _machine) = fixture().await;
    let (caller, _) = f.placed().await;
    let (source, _) = f.placed().await;
    let local = local_agent(&f, &workspace()).await;
    let now = intent_core::now_iso();
    f.services
        .store()
        .insert_workspace(&Workspace {
            id: WorkspaceId::from("ws-other"),
            title: "Other".into(),
            branch: "main".into(),
            created_at: now.clone(),
            updated_at: now,
            ..Default::default()
        })
        .await
        .unwrap();
    let stranger = local_agent(&f, &WorkspaceId::from("ws-other")).await;
    std::fs::create_dir_all(f.repo.path().join("out")).unwrap();
    std::fs::write(f.repo.path().join("out/report.txt"), "report").unwrap();

    // A local source: read in head's checkout of the workspace.
    let got = piece(answered(
        forward(
            &link,
            &caller,
            PULL_DATA_METHOD,
            args(&local, "out/report.txt"),
        )
        .await,
    ));
    assert_eq!(got.kind, CheckoutEntryKind::File);
    assert_eq!(got.size, 6);
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(&got.data)
            .unwrap(),
        b"report"
    );
    assert!(got.sha256.is_some());
    assert!(!got.transfer_id.is_empty());
    for (from, path, code) in [
        (&local, "../outside", "file-pull-outside-checkout"),
        (&local, ".git/config", "file-pull-outside-checkout"),
        (&local, "out/missing.txt", "file-pull-not-found"),
        (&stranger, "out/report.txt", "file-pull-unknown-agent"),
        (&AgentId::new(), "out/report.txt", "file-pull-unknown-agent"),
    ] {
        let error = refused(forward(&link, &caller, PULL_DATA_METHOD, args(from, path)).await);
        assert!(error.message.starts_with(code), "{path}: {error:?}");
        assert_eq!(error.code, -32602, "{error:?}");
    }
    for bad in [
        json!({ "fromAgentId": local }),
        json!({ "fromAgentId": local, "path": "out/report.txt", "offset": 0, "length": 0 }),
        json!({ "fromAgentId": local, "path": "out/report.txt", "offset": 0, "length": MAX_LINK_ATTACHMENT_CHUNK_BYTES + 1 }),
        json!({ "fromAgentId": local, "path": "out/report.txt", "offset": 4, "length": 1024 }),
    ] {
        let malformed = refused(forward(&link, &caller, PULL_DATA_METHOD, bad.clone()).await);
        assert_eq!(malformed.code, -32602, "{bad}: {malformed:?}");
    }

    // A remote source: head reads on its machine and relays the answer;
    // the transfer it opens answers only this caller, source and path.
    let relayed = CheckoutPiece {
        kind: CheckoutEntryKind::Directory,
        size: 10,
        offset: 0,
        data: base64::engine::general_purpose::STANDARD.encode(b"tar"),
        sha256: Some("00".into()),
        transfer_id: "t-machine".into(),
        skipped: Some(1),
    };
    for answer in [Ok(relayed.clone()), Err("file-pull-not-found: out/x")] {
        let call = {
            let node_link = link.link.clone();
            let caller = caller.clone();
            let args = json!({
                "fromAgentId": source, "path": "out/x", "offset": 0,
                "length": MAX_LINK_ATTACHMENT_CHUNK_BYTES,
            });
            tokio::spawn(async move {
                node_link
                    .call(
                        caller,
                        intent_core::remote_launch::workspace_api::CALL_METHOD,
                        json!({ "method": PULL_DATA_METHOD, "args": args }),
                        wait(),
                    )
                    .await
                    .unwrap()
            })
        };
        let read = link
            .expect(|frame| match frame {
                LinkFrame::CheckoutRead(read) => Some(read),
                _ => None,
            })
            .await;
        assert_eq!(read.agent_id, source);
        assert_eq!(read.path, "out/x");
        assert_eq!(read.offset, 0);
        assert_eq!(read.length, MAX_LINK_ATTACHMENT_CHUNK_BYTES);
        assert!(read.transfer_id.is_none());
        let reply = match &answer {
            Ok(piece) => CheckoutReadReply {
                request_id: read.request_id,
                agent_id: source.clone(),
                outcome: CheckoutReadOutcome::Ok,
                piece: Some(piece.clone()),
                error: None,
            },
            Err(error) => CheckoutReadReply {
                request_id: read.request_id,
                agent_id: source.clone(),
                outcome: CheckoutReadOutcome::Failed,
                piece: None,
                error: Some((*error).to_owned()),
            },
        };
        link.link
            .send(&LinkFrame::CheckoutReadReply(reply))
            .await
            .unwrap();
        match (answer, call.await.unwrap()) {
            (Ok(expected), LinkOutcome::Ok(value)) => assert_eq!(piece(value), expected),
            (Err(expected), LinkOutcome::Err(error)) => {
                assert_eq!(error.message, expected);
                assert_eq!(error.code, -32602);
            }
            (expected, got) => panic!("expected {expected:?}, got {got:?}"),
        }
    }
    let later = |path: &str| {
        json!({
            "fromAgentId": source, "path": path, "offset": 3, "length": 1024,
            "transferId": "t-machine",
        })
    };
    for (who, path) in [(&source, "out/x"), (&caller, "out/y")] {
        let expired = refused(forward(&link, who, PULL_DATA_METHOD, later(path)).await);
        assert!(
            expired.message.starts_with("file-pull-transfer-expired"),
            "{path}: {expired:?}"
        );
    }

    // A machine on an older link is refused at once, without a frame.
    f.node
        .shared_ref()
        .set_protocol_for_test(CHECKOUT_FILES_PROTOCOL - 1);
    let older = refused(forward(&link, &caller, PULL_DATA_METHOD, args(&source, "out/x")).await);
    assert!(
        older.message.starts_with("file-pull-unsupported")
            && older.message.contains("update Intent"),
        "{older:?}"
    );
    // A local source does not depend on the machine's link.
    answered(
        forward(
            &link,
            &caller,
            PULL_DATA_METHOD,
            args(&local, "out/report.txt"),
        )
        .await,
    );
    f.node.shutdown();
}

/// Answer head's next `checkoutRead` for `agent` with `reply`, returning
/// the read.
async fn answer_read(
    link: &mut NodeEnd,
    agent: &AgentId,
    reply: impl FnOnce(&intent_core::node_link::CheckoutRead) -> (Option<CheckoutPiece>, Option<String>),
) -> intent_core::node_link::CheckoutRead {
    let read = link
        .expect(|frame| match frame {
            LinkFrame::CheckoutRead(read) => Some(read),
            _ => None,
        })
        .await;
    let (piece, error) = reply(&read);
    link.link
        .send(&LinkFrame::CheckoutReadReply(CheckoutReadReply {
            request_id: read.request_id,
            agent_id: agent.clone(),
            outcome: if piece.is_some() {
                CheckoutReadOutcome::Ok
            } else {
                CheckoutReadOutcome::Failed
            },
            piece,
            error,
        }))
        .await
        .unwrap();
    read
}

/// A local agent pulls from a remote agent: head drives the pieces with
/// `checkoutRead` on the source's machine and writes its own checkout,
/// keeping the copy only whole; a source that fails mid-transfer leaves
/// nothing behind.
#[tokio::test]
async fn a_local_agent_pulls_from_a_remote_agents_machine() {
    use intent_core::WorkspaceApi as _;
    use sha2::Digest as _;
    let (f, mut link, _machine) = fixture().await;
    let (source, _) = f.placed().await;
    let local = local_agent(&f, &workspace()).await;
    let bytes = b"0123456789";
    let sha256 = sha2::Sha256::digest(bytes)
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = std::fmt::Write::write_fmt(&mut hex, format_args!("{byte:02x}"));
            hex
        });
    let piece = |offset: usize, end: usize| CheckoutPiece {
        kind: CheckoutEntryKind::File,
        size: 10,
        offset: offset as u64,
        data: base64::engine::general_purpose::STANDARD.encode(&bytes[offset..end]),
        sha256: (offset == 0).then(|| sha256.clone()),
        transfer_id: "t-file".into(),
        skipped: None,
    };
    let start = |to: &'static str| {
        let (services, local, source) = (f.services.clone(), local.clone(), source.clone());
        tokio::spawn(intent_core::with_caller(
            intent_core::Caller::Agent {
                agent_id: local.clone(),
            },
            async move {
                services
                    .file_pull(
                        workspace(),
                        Some(local),
                        source,
                        "out/data.bin".into(),
                        Some(to.into()),
                    )
                    .await
            },
        ))
    };

    let pull = start("in/data.bin");
    let first = answer_read(&mut link, &source, |_| (Some(piece(0, 6)), None)).await;
    assert_eq!((first.offset, first.transfer_id.as_deref()), (0, None));
    let second = answer_read(&mut link, &source, |_| (Some(piece(6, 10)), None)).await;
    assert_eq!(second.offset, 6);
    assert_eq!(second.transfer_id.as_deref(), Some("t-file"));
    let result = pull.await.unwrap().unwrap();
    assert_eq!(result["path"], "in/data.bin", "{result}");
    assert_eq!(result["sha256"], sha256.as_str(), "{result}");
    assert_eq!(
        std::fs::read(f.repo.path().join("in/data.bin")).unwrap(),
        bytes
    );

    let pull = start("lost/data.bin");
    answer_read(&mut link, &source, |_| (Some(piece(0, 6)), None)).await;
    answer_read(&mut link, &source, |_| {
        (
            None,
            Some("file-pull-source-unavailable: the machine went away".into()),
        )
    })
    .await;
    let error = pull.await.unwrap().unwrap_err().to_string();
    assert!(error.contains("file-pull-source-unavailable"), "{error}");
    assert!(!f.repo.path().join("lost").exists(), "no parent made");
    let parts: Vec<_> = std::fs::read_dir(f.repo.path().join(".intent/pulls"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert_eq!(parts, [".gitignore"], "no part file left");
    f.node.shutdown();
}
