//! A remote agent's `workspace_api` calls through its node-side server:
//! local calls on its worktree, forwarded calls over a real link (the test
//! plays head), and the namespaces a node refuses.

use std::time::Duration;

use serde_json::{json, Value};

use intent_core::node_link::{LinkOutcome, LinkRpcError};
use intent_transport::node_link::{DialedLink, LinkEvent};

use super::*;

fn wait() -> Duration {
    intentd_test_support::test_timeout(Duration::from_secs(10))
}

fn agent() -> AgentId {
    AgentId::from("agent-remote")
}

fn workspace() -> WorkspaceId {
    WorkspaceId::from("ws-remote")
}

/// A top-level agent with every feature on.
fn gates() -> Gates {
    Gates {
        agent_features: serde_json::to_value(AgentFeaturesSettings::default()).unwrap(),
        is_sub_agent: false,
        compact_tool_descriptions: false,
        mcp_proxies: Vec::new(),
        harness_version: None,
        git_identity: None,
    }
}

async fn call(server: &WorkspaceMcpServer, code: &str) -> (bool, String) {
    let response = server
        .handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "workspace_api", "arguments": { "code": code, "summary": "test" } },
        }))
        .await
        .expect("a tools/call response");
    let result = &response["result"];
    (
        result["isError"] == json!(true),
        result["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
    )
}

fn ok_json(outcome: (bool, String)) -> Value {
    assert!(!outcome.0, "call failed: {}", outcome.1);
    // `JSON.stringify` results arrive as a JSON string of JSON.
    match serde_json::from_str(&outcome.1) {
        Ok(Value::String(inner)) => serde_json::from_str(&inner).unwrap_or(Value::String(inner)),
        Ok(value) => value,
        Err(_) => Value::String(outcome.1),
    }
}

fn git(dir: &Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "git {args:?}: {output:?}");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

#[tokio::test]
async fn file_calls_run_on_the_agents_worktree_and_stay_inside_it() {
    let dir = tempfile::tempdir().unwrap();
    let server = server(
        &agent(),
        &workspace(),
        dir.path(),
        &gates(),
        HeadLink::default(),
    )
    .unwrap();

    ok_json(
        call(
            &server,
            r#"return await ws.file.write("src/a.txt", "hello");"#,
        )
        .await,
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("src/a.txt")).unwrap(),
        "hello"
    );
    let read = ok_json(
        call(
            &server,
            r#"return JSON.stringify(await ws.file.read("src/a.txt"));"#,
        )
        .await,
    );
    assert_eq!(read, json!("hello"));
    let listed = call(&server, r#"return await ws.file.list("src");"#).await;
    assert!(!listed.0 && listed.1.contains("a.txt"), "{listed:?}");
    ok_json(
        call(
            &server,
            r#"return await ws.file.rename("src/a.txt", "b.txt");"#,
        )
        .await,
    );
    assert!(dir.path().join("b.txt").exists());
    ok_json(
        call(
            &server,
            r#"return JSON.stringify(await ws.file.mkdir("made"));"#,
        )
        .await,
    );
    assert!(dir.path().join("made").is_dir());
    ok_json(
        call(
            &server,
            r#"return JSON.stringify(await ws.file.delete("b.txt"));"#,
        )
        .await,
    );
    assert!(!dir.path().join("b.txt").exists());

    let escape = call(&server, r#"return await ws.file.read("../outside.txt");"#).await;
    assert!(escape.0, "{escape:?}");
}

#[tokio::test]
async fn host_exec_runs_in_the_agents_worktree() {
    let dir = tempfile::tempdir().unwrap();
    let server = server(
        &agent(),
        &workspace(),
        dir.path(),
        &gates(),
        HeadLink::default(),
    )
    .unwrap();
    let ran = ok_json(
        call(
            &server,
            r#"return JSON.stringify(await ws.host.exec({ command: "pwd" }));"#,
        )
        .await,
    );
    assert_eq!(ran["exitCode"], json!(0), "{ran}");
    let printed = std::fs::canonicalize(ran["stdout"].as_str().unwrap().trim()).unwrap();
    assert_eq!(printed, std::fs::canonicalize(dir.path()).unwrap());
    let outside = call(
        &server,
        r#"return await ws.host.exec({ command: "pwd", cwd: "/" });"#,
    )
    .await;
    assert!(outside.0, "{outside:?}");
}

#[tokio::test]
async fn git_commit_commits_the_agents_work_in_its_worktree() {
    if intent_providers::resolve_on_path("git").is_none() {
        eprintln!("skipping: git not on PATH");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "--quiet"]);
    std::fs::write(dir.path().join("one.txt"), "1").unwrap();
    std::fs::write(dir.path().join("two.txt"), "2").unwrap();
    let server = server(
        &agent(),
        &workspace(),
        dir.path(),
        &gates(),
        HeadLink::default(),
    )
    .unwrap();

    let first = ok_json(
        call(
            &server,
            r#"return JSON.stringify(await ws.git.commit("one", { files: ["one.txt"], userRequested: true }));"#,
        )
        .await,
    );
    assert_eq!(first["files"], json!(["one.txt"]), "{first}");
    assert_eq!(
        first["hash"],
        json!(git(dir.path(), &["rev-parse", "HEAD"]))
    );
    let second = ok_json(
        call(
            &server,
            r#"return JSON.stringify(await ws.git.commit("the rest", { userRequested: true }));"#,
        )
        .await,
    );
    assert_eq!(second["files"], json!(["two.txt"]), "{second}");
    assert_eq!(git(dir.path(), &["log", "--format=%s"]), "the rest\none");

    let empty = call(
        &server,
        r#"return await ws.git.commit("nothing", { userRequested: true });"#,
    )
    .await;
    assert!(
        empty.0 && empty.1.contains("nothing to commit"),
        "{empty:?}"
    );
}

/// Hide the host's global and system git config and any inherited identity
/// env, so only the repo's own config and the launch decide. Nextest runs
/// each test in its own process.
fn isolate_git_identity() {
    std::env::set_var("GIT_CONFIG_GLOBAL", "/dev/null");
    std::env::set_var("GIT_CONFIG_NOSYSTEM", "1");
    for key in intent_git::identity::GIT_IDENTITY_ENV_VARS {
        std::env::remove_var(key);
    }
}

fn identity_of_head(dir: &Path) -> String {
    git(dir, &["log", "-1", "--format=%an <%ae>|%cn <%ce>"])
}

async fn commit_file(server: &WorkspaceMcpServer, dir: &Path, name: &str) {
    std::fs::write(dir.join(name), name).unwrap();
    ok_json(
        call(
            server,
            &format!(
                r#"return JSON.stringify(await ws.git.commit("{name}", {{ userRequested: true }}));"#
            ),
        )
        .await,
    );
}

/// A remote agent's `ws.git.commit` commits as the backend's identity from
/// the launch, as the agent's own `git commit` does (intentd#3165), over
/// the node's git config too.
#[tokio::test]
async fn git_commit_uses_the_launch_identity_of_the_backend() {
    if intent_providers::resolve_on_path("git").is_none() {
        eprintln!("skipping: git not on PATH");
        return;
    }
    isolate_git_identity();
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "--quiet"]);
    let gates = Gates {
        git_identity: Some(intent_core::node_link::GitIdentity {
            name: "Backend User".into(),
            email: "backend@example.com".into(),
        }),
        ..gates()
    };
    let server = server(
        &agent(),
        &workspace(),
        dir.path(),
        &gates,
        HeadLink::default(),
    )
    .unwrap();

    commit_file(&server, dir.path(), "one.txt").await;
    assert_eq!(
        identity_of_head(dir.path()),
        "Backend User <backend@example.com>|Backend User <backend@example.com>"
    );

    git(dir.path(), &["config", "user.name", "Node User"]);
    git(dir.path(), &["config", "user.email", "node@example.com"]);
    commit_file(&server, dir.path(), "two.txt").await;
    assert_eq!(
        identity_of_head(dir.path()),
        "Backend User <backend@example.com>|Backend User <backend@example.com>"
    );
}

/// Without a launch identity the node's git config decides, and with
/// neither the commit uses the fixed agent identity.
#[tokio::test]
async fn git_commit_without_a_launch_identity_uses_config_then_the_placeholder() {
    if intent_providers::resolve_on_path("git").is_none() {
        eprintln!("skipping: git not on PATH");
        return;
    }
    isolate_git_identity();
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "--quiet"]);
    let gates = Gates {
        git_identity: Some(intent_core::node_link::GitIdentity {
            name: String::new(),
            email: "half@example.com".into(),
        }),
        ..gates()
    };
    let server = server(
        &agent(),
        &workspace(),
        dir.path(),
        &gates,
        HeadLink::default(),
    )
    .unwrap();

    commit_file(&server, dir.path(), "one.txt").await;
    assert_eq!(
        identity_of_head(dir.path()),
        "Intent remote agent <remote-agent@intent.invalid>|Intent remote agent <remote-agent@intent.invalid>"
    );

    git(dir.path(), &["config", "user.name", "Node User"]);
    git(dir.path(), &["config", "user.email", "node@example.com"]);
    commit_file(&server, dir.path(), "two.txt").await;
    assert_eq!(
        identity_of_head(dir.path()),
        "Node User <node@example.com>|Node User <node@example.com>"
    );
}

#[tokio::test]
async fn namespaces_a_node_cannot_serve_are_refused_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let server = server(
        &agent(),
        &workspace(),
        dir.path(),
        &gates(),
        HeadLink::default(),
    )
    .unwrap();
    for code in [r#"return await host({ method: "secret.provide", args: {} });"#] {
        let (failed, text) = call(&server, code).await;
        assert!(failed, "{code}: {text}");
        assert!(
            text.contains("is not supported on remote agents yet"),
            "{code}: {text}"
        );
    }
    // Hooks, questions, the browser, the desktop and ws.mcp go to head, so
    // with no link they fail for the link, not by name.
    for code in [
        "return await ws.mcp.listServers();",
        "return await ws.hook.list();",
        "return await ws.desktop.listDisplay();",
        "return await ws.browser.listTabs();",
        r#"return await ws.app.question.ask({ header: "h", question: "q", options: [{ label: "a" }, { label: "b" }] });"#,
    ] {
        let (failed, text) = call(&server, code).await;
        assert!(failed, "{code}: {text}");
        assert!(text.contains("node-link-unavailable"), "{code}: {text}");
    }
    for code in [
        r#"return await ws.git.registerRoot("sub");"#,
        r#"return await ws.git.unregisterRoot("sub");"#,
        r#"return await ws.git.commit("x", { gitRootId: "root-1", userRequested: true });"#,
    ] {
        let (failed, text) = call(&server, code).await;
        assert!(failed, "{code}: {text}");
        assert!(
            text.contains(workspace_api::GIT_ROOTS_MESSAGE),
            "{code}: {text}"
        );
    }
    let help = call(&server, r#"return ws.help("note");"#).await;
    assert!(!help.0, "{help:?}");
}

/// `ws.workspace.info` is answered on the node with the agent's own
/// checkout there; `details` goes to head.
#[tokio::test]
async fn workspace_info_names_the_agents_checkout_on_the_node() {
    let dir = tempfile::tempdir().unwrap();
    let server = server(
        &agent(),
        &workspace(),
        dir.path(),
        &gates(),
        HeadLink::default(),
    )
    .unwrap();
    let info = ok_json(call(&server, "return JSON.stringify(await ws.workspace.info());").await);
    assert_eq!(
        info,
        json!({ "id": workspace().as_str(), "path": dir.path().to_string_lossy() })
    );
    let (failed, text) = call(&server, "return await ws.workspace.details();").await;
    assert!(failed && text.contains("node-link-unavailable"), "{text}");
}

#[tokio::test]
async fn a_forwarded_call_without_a_link_fails_clearly() {
    let dir = tempfile::tempdir().unwrap();
    let server = server(
        &agent(),
        &workspace(),
        dir.path(),
        &gates(),
        HeadLink::default(),
    )
    .unwrap();
    let (failed, text) = call(&server, r#"return await ws.note.read("n-1");"#).await;
    assert!(failed, "{text}");
    assert!(text.contains("node-link-unavailable"), "{text}");
}

/// Remote parity S6: `ws.file.getAttachment` runs on the machine. The bytes
/// come from head with the internal `file.attachmentData` frame, and the copy
/// lands in the agent's checkout, git-ignored, with the local result shape.
/// Head's refusal (an unknown id, or an older backend) is the call's error.
#[tokio::test]
async fn file_get_attachment_copies_heads_bytes_into_the_checkout() {
    use base64::Engine as _;
    use intent_core::remote_launch::workspace_api::ATTACHMENT_DATA_METHOD;
    let dir = tempfile::tempdir().unwrap();
    let (node_end, mut head_end, _served) = bound_link(dir.path()).await;
    let head = HeadLink::default();
    head.set(node_end);
    let server = server(&agent(), &workspace(), dir.path(), &gates(), head).unwrap();
    let (asked_tx, mut asked) = tokio::sync::mpsc::unbounded_channel();
    let answering = tokio::spawn(async move {
        while let Some(event) = head_end.events.recv().await {
            let LinkEvent::Request(request) = event else {
                continue;
            };
            let frame = request.request().clone();
            asked_tx.send(frame.params.clone()).unwrap();
            let outcome = match frame.params["args"]["attachmentId"].as_str() {
                Some("att-1") => LinkOutcome::Ok(json!({
                    "fileName": "notes.txt",
                    "mimeType": "text/plain",
                    "size": 5,
                    "uploadedAt": "2026-11-05T00:00:00Z",
                    "data": base64::engine::general_purpose::STANDARD.encode(b"hello"),
                })),
                _ => LinkOutcome::Err(LinkRpcError {
                    code: -32000,
                    message: "unknown attachment id: att-missing".into(),
                    data: None,
                }),
            };
            request.respond(outcome).await.unwrap();
        }
    });

    let copied = ok_json(
        call(
            &server,
            r#"return JSON.stringify(await ws.file.getAttachment("att-1"));"#,
        )
        .await,
    );
    assert_eq!(
        copied,
        json!({
            "path": ".intent/attachments/notes.txt",
            "fileName": "notes.txt",
            "mimeType": "text/plain",
            "size": 5,
            "uploadedAt": "2026-11-05T00:00:00Z",
        })
    );
    assert_eq!(
        std::fs::read(dir.path().join(".intent/attachments/notes.txt")).unwrap(),
        b"hello"
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join(".intent/attachments/.gitignore")).unwrap(),
        "*\n"
    );
    let params = asked.try_recv().unwrap();
    assert_eq!(params["method"], json!(ATTACHMENT_DATA_METHOD));
    assert_eq!(
        params["args"],
        json!({
            "attachmentId": "att-1",
            "offset": 0,
            "length": intent_core::remote_launch::workspace_api::MAX_LINK_ATTACHMENT_CHUNK_BYTES,
        })
    );

    let into = ok_json(
        call(
            &server,
            r#"return JSON.stringify(await ws.file.getAttachment("att-1", "inbox"));"#,
        )
        .await,
    );
    assert_eq!(into["path"], json!("inbox/notes.txt"), "{into}");
    assert!(dir.path().join("inbox/notes.txt").is_file());

    let unknown = call(
        &server,
        r#"return await ws.file.getAttachment("att-missing");"#,
    )
    .await;
    assert!(
        unknown.0 && unknown.1.contains("unknown attachment id"),
        "{unknown:?}"
    );
    while asked.try_recv().is_ok() {}
    let escape = call(
        &server,
        r#"return await ws.file.getAttachment("att-1", "../outside");"#,
    )
    .await;
    assert!(escape.0, "{escape:?}");
    assert!(!dir.path().parent().unwrap().join("outside").exists());
    assert!(
        asked.try_recv().is_err(),
        "a destDir outside the checkout is refused before head is asked"
    );
    answering.abort();
}

/// A file larger than one frame comes from head in pieces: the machine asks
/// for `offset`/`length` ranges, writes them as they come and keeps the copy
/// only when its size and sha256 match head's. An older backend's answers
/// (the whole file, or its `attachment-too-large-for-link` refusal) work as
/// before; see [`file_get_attachment_copies_heads_bytes_into_the_checkout`].
#[tokio::test]
async fn file_get_attachment_fetches_a_large_file_in_pieces() {
    use base64::Engine as _;
    use intent_core::remote_launch::workspace_api::MAX_LINK_ATTACHMENT_CHUNK_BYTES;
    use sha2::{Digest as _, Sha256};
    let chunk = usize::try_from(MAX_LINK_ATTACHMENT_CHUNK_BYTES).unwrap();
    let file: Vec<u8> = (0..2 * chunk + 123)
        .map(|i| u8::try_from(i % 251).unwrap())
        .collect();
    let digest = Sha256::digest(&file)
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = std::fmt::Write::write_fmt(&mut hex, format_args!("{byte:02x}"));
            hex
        });
    let dir = tempfile::tempdir().unwrap();
    let (node_end, mut head_end, _served) = bound_link(dir.path()).await;
    let head = HeadLink::default();
    head.set(node_end);
    let server = server(&agent(), &workspace(), dir.path(), &gates(), head).unwrap();
    let (asked_tx, mut asked) = tokio::sync::mpsc::unbounded_channel();
    let source = file.clone();
    let answering = tokio::spawn(async move {
        while let Some(event) = head_end.events.recv().await {
            let LinkEvent::Request(request) = event else {
                continue;
            };
            let args = request.request().params["args"].clone();
            asked_tx.send(args.clone()).unwrap();
            let id = args["attachmentId"].as_str().unwrap_or_default().to_owned();
            let offset = usize::try_from(args["offset"].as_u64().unwrap()).unwrap();
            let length = usize::try_from(args["length"].as_u64().unwrap()).unwrap();
            let end = source.len().min(offset + length.min(chunk));
            let mut data = source[offset..end].to_vec();
            if id == "att-corrupt" && offset > 0 {
                data[0] ^= 1;
            }
            let name = if id == "att-long" {
                format!("{}.bin", "n".repeat(240))
            } else {
                format!("{id}.bin")
            };
            let mut answer = json!({
                "fileName": name,
                "mimeType": "application/octet-stream",
                "size": source.len(),
                "uploadedAt": "2026-11-06T00:00:00Z",
                "offset": offset,
                "data": base64::engine::general_purpose::STANDARD.encode(&data),
            });
            if offset == 0 {
                answer["sha256"] = json!(digest);
            }
            request.respond(LinkOutcome::Ok(answer)).await.unwrap();
        }
    });

    let copied = ok_json(
        call(
            &server,
            r#"return JSON.stringify(await ws.file.getAttachment("att-big"));"#,
        )
        .await,
    );
    assert_eq!(
        copied,
        json!({
            "path": ".intent/attachments/att-big.bin",
            "fileName": "att-big.bin",
            "mimeType": "application/octet-stream",
            "size": file.len(),
            "uploadedAt": "2026-11-06T00:00:00Z",
        })
    );
    assert!(std::fs::read(dir.path().join(".intent/attachments/att-big.bin")).unwrap() == file);
    let offsets: Vec<u64> = std::iter::from_fn(|| asked.try_recv().ok())
        .map(|args| args["offset"].as_u64().unwrap())
        .collect();
    assert_eq!(
        offsets,
        [
            0,
            MAX_LINK_ATTACHMENT_CHUNK_BYTES,
            2 * MAX_LINK_ATTACHMENT_CHUNK_BYTES
        ]
    );

    let long = call(
        &server,
        r#"return JSON.stringify(await ws.file.getAttachment("att-long"));"#,
    )
    .await;
    assert!(
        !long.0,
        "a name near the file-system limit still copies: {long:?}"
    );
    let long_name = format!("{}.bin", "n".repeat(240));
    assert!(dir
        .path()
        .join(".intent/attachments")
        .join(&long_name)
        .is_file());
    std::fs::remove_file(dir.path().join(".intent/attachments").join(&long_name)).unwrap();
    while asked.try_recv().is_ok() {}

    let corrupt = call(
        &server,
        r#"return await ws.file.getAttachment("att-corrupt");"#,
    )
    .await;
    assert!(
        corrupt.0 && corrupt.1.contains("attachment-copy-mismatch"),
        "{corrupt:?}"
    );
    let mut left: Vec<_> = std::fs::read_dir(dir.path().join(".intent/attachments"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    left.sort();
    assert_eq!(
        left,
        [".gitignore", "att-big.bin"],
        "no partial copy is kept"
    );
    answering.abort();
}

/// The machine copies a file of exactly the 200 MiB cap and refuses a larger
/// one from head's first answer (an older backend still serves it), with
/// `attachment-exceeds-cap` naming the size and the cap: it asks for no more pieces and writes nothing.
#[tokio::test]
async fn file_get_attachment_enforces_the_size_cap() {
    use base64::Engine as _;
    use intent_core::remote_launch::workspace_api::MAX_LINK_ATTACHMENT_CHUNK_BYTES;
    use intent_services::feature_api::file_ops::ATTACHMENT_COPY_MAX_BYTES as CAP;
    use sha2::{Digest as _, Sha256};
    let chunk = usize::try_from(MAX_LINK_ATTACHMENT_CHUNK_BYTES).unwrap();
    let zeros = base64::engine::general_purpose::STANDARD.encode(vec![0u8; chunk]);
    let mut hasher = Sha256::new();
    let block = vec![0u8; chunk];
    for _ in 0..CAP / MAX_LINK_ATTACHMENT_CHUNK_BYTES {
        hasher.update(&block);
    }
    hasher.update(&block[..usize::try_from(CAP % MAX_LINK_ATTACHMENT_CHUNK_BYTES).unwrap()]);
    let digest = hasher
        .finalize()
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = std::fmt::Write::write_fmt(&mut hex, format_args!("{byte:02x}"));
            hex
        });
    let dir = tempfile::tempdir().unwrap();
    let (node_end, mut head_end, _served) = bound_link(dir.path()).await;
    let head = HeadLink::default();
    head.set(node_end);
    let server = server(&agent(), &workspace(), dir.path(), &gates(), head).unwrap();
    let (asked_tx, mut asked) = tokio::sync::mpsc::unbounded_channel();
    let answering = tokio::spawn(async move {
        while let Some(event) = head_end.events.recv().await {
            let LinkEvent::Request(request) = event else {
                continue;
            };
            let args = request.request().params["args"].clone();
            asked_tx.send(args.clone()).unwrap();
            let id = args["attachmentId"].as_str().unwrap_or_default().to_owned();
            let size = if id == "att-over" { CAP + 1 } else { CAP };
            let offset = args["offset"].as_u64().unwrap();
            let length = (size - offset).min(MAX_LINK_ATTACHMENT_CHUNK_BYTES);
            let data = if length == MAX_LINK_ATTACHMENT_CHUNK_BYTES {
                zeros.clone()
            } else {
                base64::engine::general_purpose::STANDARD.encode(vec![
                    0u8;
                    usize::try_from(length)
                        .unwrap()
                ])
            };
            let mut answer = json!({
                "fileName": format!("{id}.bin"),
                "size": size,
                "uploadedAt": "2026-11-09T00:00:00Z",
                "offset": offset,
                "data": data,
            });
            if offset == 0 {
                answer["sha256"] = json!(digest);
            }
            request.respond(LinkOutcome::Ok(answer)).await.unwrap();
        }
    });

    let copied = ok_json(
        call(
            &server,
            r#"return JSON.stringify(await ws.file.getAttachment("att-cap"));"#,
        )
        .await,
    );
    assert_eq!(copied["size"], json!(CAP));
    assert_eq!(
        std::fs::metadata(dir.path().join(".intent/attachments/att-cap.bin"))
            .unwrap()
            .len(),
        CAP
    );
    while asked.try_recv().is_ok() {}

    let refused = call(
        &server,
        r#"return await ws.file.getAttachment("att-over", "over");"#,
    )
    .await;
    assert!(
        refused.0
            && refused.1.contains(&format!(
                "attachment-exceeds-cap: {} bytes; limit 209715200",
                CAP + 1
            )),
        "{refused:?}"
    );
    let offsets: Vec<u64> = std::iter::from_fn(|| asked.try_recv().ok())
        .map(|args| args["offset"].as_u64().unwrap())
        .collect();
    assert_eq!(offsets, [0], "no piece is asked for past the first");
    assert!(
        !dir.path().join("over").exists(),
        "an over-cap copy writes nothing"
    );
    answering.abort();
}

/// An older backend refuses a file over 5 MiB with today's error, which the
/// call reports unchanged.
#[tokio::test]
async fn file_get_attachment_reports_an_older_backends_size_refusal() {
    let dir = tempfile::tempdir().unwrap();
    let (node_end, mut head_end, _served) = bound_link(dir.path()).await;
    let head = HeadLink::default();
    head.set(node_end);
    let server = server(&agent(), &workspace(), dir.path(), &gates(), head).unwrap();
    let message = "attachment-too-large-for-link: \"big.bin\" is 6000000 bytes; a remote agent can copy attachments of at most 5242880 bytes. Continue without this file, or ask the user to share it another way.";
    let answering = tokio::spawn(async move {
        while let Some(event) = head_end.events.recv().await {
            let LinkEvent::Request(request) = event else {
                continue;
            };
            request
                .respond(LinkOutcome::Err(LinkRpcError {
                    code: -32602,
                    message: message.into(),
                    data: None,
                }))
                .await
                .unwrap();
        }
    });
    let refused = call(&server, r#"return await ws.file.getAttachment("att-big");"#).await;
    assert!(refused.0 && refused.1.contains(message), "{refused:?}");
    assert!(!dir.path().join(".intent/attachments/big.bin").exists());
    answering.abort();
}

/// A bound link: the machine's end and the backend's (the test's) end.
async fn bound_link(dir: &Path) -> (NodeLink, DialedLink, crate::executor::test_link::Served) {
    crate::executor::test_link::bound_link(dir).await
}

/// Forwarded frames reach head as one `workspace.call` request for the
/// agent, carrying the frame; head's answer (or refusal) is the call's.
#[tokio::test]
async fn control_plane_calls_are_forwarded_to_head_for_the_agent() {
    let dir = tempfile::tempdir().unwrap();
    let (node_end, mut head_end, _served) = bound_link(dir.path()).await;
    let head = HeadLink::default();
    head.set(node_end);
    let server = server(&agent(), &workspace(), dir.path(), &gates(), head).unwrap();
    let answering = tokio::spawn(async move {
        let mut seen = Vec::new();
        while let Some(event) = head_end.events.recv().await {
            let LinkEvent::Request(request) = event else {
                continue;
            };
            let frame = request.request().clone();
            let outcome = if frame.params["method"] == "task.updateStatus" {
                LinkOutcome::Err(LinkRpcError {
                    code: -32000,
                    message: "task not found on head".into(),
                    data: None,
                })
            } else {
                LinkOutcome::Ok(json!({ "answered": frame.params["method"] }))
            };
            seen.push(frame);
            request.respond(outcome).await.unwrap();
            if seen.len() == 2 {
                return (seen, head_end);
            }
        }
        panic!("the link closed");
    });

    let read = ok_json(
        call(
            &server,
            r#"return JSON.stringify(await ws.note.read("n-1"));"#,
        )
        .await,
    );
    assert_eq!(read, json!({ "answered": "note.read" }));
    let failed = call(
        &server,
        r#"return await ws.task.updateStatus("t-1", "x", "done");"#,
    )
    .await;
    assert!(
        failed.0 && failed.1.contains("task not found on head"),
        "{failed:?}"
    );

    let (seen, _head_end) = tokio::time::timeout(wait(), answering)
        .await
        .unwrap()
        .unwrap();
    assert!(seen
        .iter()
        .all(|request| request.agent_id == agent() && request.method == CALL_METHOD));
    assert_eq!(seen[0].params["method"], json!("note.read"));
    assert_eq!(seen[0].params["args"]["id"], json!("n-1"));
}

/// Saved scripts are forwarded to head only on a link that negotiated
/// nodeProtocol 8; on 7 the machine refuses every `ws.script.*` itself.
#[tokio::test]
async fn script_calls_reach_head_only_on_a_protocol_8_link() {
    for (max, forwarded) in [(8, true), (7, false)] {
        let dir = tempfile::tempdir().unwrap();
        let (node_end, mut head_end, _served) = crate::executor::test_link::bound_link_offering(
            dir.path(),
            intent_core::node_link::ProtocolRange { min: 7, max },
        )
        .await;
        assert_eq!(node_end.protocol(), max);
        let head = HeadLink::default();
        head.set(node_end);
        let server = server(&agent(), &workspace(), dir.path(), &gates(), head).unwrap();
        let answering = tokio::spawn(async move {
            while let Some(event) = head_end.events.recv().await {
                if let LinkEvent::Request(request) = event {
                    let method = request.request().params["method"].clone();
                    request
                        .respond(LinkOutcome::Ok(json!({ "answered": method })))
                        .await
                        .unwrap();
                }
            }
        });
        let (failed, text) = call(&server, "return JSON.stringify(await ws.script.list());").await;
        if forwarded {
            assert!(!failed, "{text}");
            assert!(text.contains("script.list"), "{text}");
        } else {
            assert!(failed, "{text}");
            assert!(text.contains("remote-agent-unsupported"), "{text}");
        }
        answering.abort();
    }
}

/// intent-hq/intent#8593: the machine forwards the workspace setters and
/// `proposeSibling` to head for the agent; the other workspace calls it
/// still refuses itself.
#[tokio::test]
async fn workspace_setters_are_forwarded_to_head() {
    let dir = tempfile::tempdir().unwrap();
    let (node_end, mut head_end, _served) = bound_link(dir.path()).await;
    let head = HeadLink::default();
    head.set(node_end);
    let server = server(&agent(), &workspace(), dir.path(), &gates(), head).unwrap();
    let answering = tokio::spawn(async move {
        while let Some(event) = head_end.events.recv().await {
            if let LinkEvent::Request(request) = event {
                let frame = request.request().clone();
                assert_eq!(frame.agent_id, agent());
                request
                    .respond(LinkOutcome::Ok(json!({
                        "answered": frame.params["method"],
                        "args": frame.params["args"],
                    })))
                    .await
                    .unwrap();
            }
        }
    });
    for (code, method, args) in [
        (
            r#"ws.workspace.setTitle("Remote")"#,
            "workspace.setTitle",
            json!({ "title": "Remote" }),
        ),
        (
            r#"ws.workspace.setAgentName("Namer")"#,
            "workspace.setAgentName",
            json!({ "name": "Namer" }),
        ),
        (
            r#"ws.workspace.setStatusMessage("Busy")"#,
            "workspace.setStatusMessage",
            json!({ "statusMessage": "Busy" }),
        ),
        (
            "ws.workspace.setStatusImage(null)",
            "workspace.setStatusImage",
            json!({ "image": null }),
        ),
        (
            r#"ws.workspace.proposeSibling({ title: "Next", initialPrompt: "Go" })"#,
            "workspace.proposeSibling",
            json!({ "title": "Next", "initialPrompt": "Go" }),
        ),
    ] {
        let answer = ok_json(call(&server, &format!("return JSON.stringify(await {code});")).await);
        assert_eq!(
            answer,
            json!({ "answered": method, "args": args }),
            "{method}"
        );
    }
    answering.abort();
}

/// Remote parity S2: the backend-data calls go to head for the agent, as
/// one `workspace.call` each.
#[tokio::test]
async fn backend_data_calls_are_forwarded_to_head() {
    let dir = tempfile::tempdir().unwrap();
    let (node_end, mut head_end, _served) = bound_link(dir.path()).await;
    let head = HeadLink::default();
    head.set(node_end);
    let server = server(&agent(), &workspace(), dir.path(), &gates(), head).unwrap();
    let answering = tokio::spawn(async move {
        while let Some(event) = head_end.events.recv().await {
            if let LinkEvent::Request(request) = event {
                let frame = request.request().clone();
                assert_eq!(frame.agent_id, agent());
                request
                    .respond(LinkOutcome::Ok(
                        json!({ "answered": frame.params["method"] }),
                    ))
                    .await
                    .unwrap();
            }
        }
    });
    for (code, method) in [
        (
            r#"ws.primitive.addCli("n-1", "make check", "Gate")"#,
            "primitive.addCli",
        ),
        ("ws.discussion.conversations()", "discussion.conversations"),
        (
            "ws.crossWorkspace.listSiblings()",
            "crossWorkspace.listSiblings",
        ),
        ("ws.git.listRoots()", "git.listRoots"),
        ("ws.terminal.list()", "terminal.list"),
        (r#"ws.terminal.readOutput("t-1")"#, "terminal.readOutput"),
        ("ws.workspace.archive()", "workspace.archive"),
        ("ws.workspace.unarchive()", "workspace.unarchive"),
        (
            r#"ws.workspace.applyProposal("p-1", { userRequested: true })"#,
            "workspace.applyProposal",
        ),
        (
            r#"host({ method: "workspace.timeline", args: {} })"#,
            "workspace.timeline",
        ),
    ] {
        let answer = ok_json(call(&server, &format!("return JSON.stringify(await {code});")).await);
        assert_eq!(answer, json!({ "answered": method }), "{method}");
    }
    let (failed, text) = call(
        &server,
        r#"return await host({ method: "app.agents.list", args: {} });"#,
    )
    .await;
    assert!(
        failed && text.contains("is for the Assistant workspace"),
        "{text}"
    );
    answering.abort();
}

/// Remote parity G1: the machine asks head for the placement text when it
/// builds the agent's server and for `ws.help("agent")`, and adds it to the
/// `workspace_api` description and the help; a backend that refuses the
/// frame (older) or no link leaves it out.
#[tokio::test]
async fn placement_help_comes_from_head() {
    use intent_core::machine_selection::MACHINE_MODELS_SIGNATURE;
    use intent_core::remote_launch::workspace_api::PLACEMENT_HELP_METHOD;
    const HELP: &str = "Remote placement (test): machine `node-2`.";
    let lookup_help = format!("{HELP} Confirm with `{MACHINE_MODELS_SIGNATURE}`.");
    // An older backend's text (no lookup), a backend that serves the lookup,
    // and one that refuses the frame.
    for (answer, lookup) in [
        (Some(HELP.to_owned()), false),
        (Some(lookup_help), true),
        (None, false),
    ] {
        let answers = answer.is_some();
        let dir = tempfile::tempdir().unwrap();
        let (node_end, mut head_end, _served) = bound_link(dir.path()).await;
        let head = HeadLink::default();
        head.set(node_end);
        let (asked_tx, mut asked) = tokio::sync::mpsc::unbounded_channel();
        let answering = tokio::spawn(async move {
            while let Some(event) = head_end.events.recv().await {
                if let LinkEvent::Request(request) = event {
                    let frame = request.request().clone();
                    assert_eq!(frame.agent_id, agent());
                    asked_tx.send(frame.params["method"].clone()).unwrap();
                    let outcome = if let Some(help) = &answer {
                        LinkOutcome::Ok(json!({ "help": help }))
                    } else {
                        LinkOutcome::Err(LinkRpcError {
                            code: -32003,
                            message: format!("remote-agent-unsupported: `{PLACEMENT_HELP_METHOD}`"),
                            data: None,
                        })
                    };
                    request.respond(outcome).await.unwrap();
                }
            }
        });
        let server = described_server(
            &agent(),
            &workspace(),
            dir.path(),
            (&gates(), None),
            (head, None),
        )
        .await
        .unwrap();
        assert_eq!(asked.try_recv().unwrap(), json!(PLACEMENT_HELP_METHOD));
        let description = server.full_workspace_api_description();
        assert_eq!(description.contains(HELP), answers, "{description}");
        let help = ok_json(call(&server, r#"return ws.help("agent");"#).await);
        let help = help.as_str().unwrap_or_default();
        assert_eq!(help.contains(HELP), answers, "{help}");
        assert!(help.contains("ws.agent.create"), "{help}");
        let bound = ok_json(call(&server, "return 'machineModels' in ws.agent;").await);
        assert_eq!(
            bound,
            json!(lookup),
            "machineModels bound only when head serves it"
        );
        answering.abort();
    }

    let dir = tempfile::tempdir().unwrap();
    let unlinked = described_server(
        &agent(),
        &workspace(),
        dir.path(),
        (&gates(), None),
        (HeadLink::default(), None),
    )
    .await
    .unwrap();
    assert!(!unlinked
        .full_workspace_api_description()
        .contains("Remote placement"));
}

/// intent-hq/intent#8593: a call's cards (two proposals here) go to head as
/// one stamped batch before the call answers, whether the agent's code
/// returns the card or discards it, as a local agent's call registers them.
#[tokio::test]
async fn a_calls_cards_are_published_to_head_as_one_batch() {
    use intent_core::remote_launch::workspace_api::ATTACHMENTS_METHOD;
    let dir = tempfile::tempdir().unwrap();
    let (node_end, mut head_end, _served) = bound_link(dir.path()).await;
    let head = HeadLink::default();
    head.set(node_end);
    let server = server(&agent(), &workspace(), dir.path(), &gates(), head).unwrap();
    let (published_tx, mut published) = tokio::sync::mpsc::unbounded_channel();
    let answering = tokio::spawn(async move {
        let mut proposals = 0;
        while let Some(event) = head_end.events.recv().await {
            let LinkEvent::Request(request) = event else {
                continue;
            };
            let frame = request.request().clone();
            let outcome = if frame.params["method"] == ATTACHMENTS_METHOD {
                published_tx.send(frame.params["args"].clone()).unwrap();
                json!({})
            } else {
                proposals += 1;
                let card = json!({ "kind": "workspace-create", "applyToolCallId": format!("workspace-create-{proposals}") });
                json!({
                    "ok": true,
                    "proposal": card,
                    "__mcpContentItems": [
                        { "type": "text", "text": json!({ "ok": true, "proposal": card }).to_string() },
                        { "type": "resource", "resource": {
                            "uri": format!("intent-proposal://workspace-create/{proposals}"),
                            "name": "Create workspace",
                            "mimeType": "application/vnd.intent.proposal+json",
                            "text": card.to_string(),
                        } },
                    ],
                })
            };
            request.respond(LinkOutcome::Ok(outcome)).await.unwrap();
        }
    });

    let returned = call(
        &server,
        r#"const a = await ws.workspace.proposeSibling({ title: "A", initialPrompt: "Go" }); await ws.workspace.proposeSibling({ title: "B", initialPrompt: "Go" }); return a;"#,
    )
    .await;
    assert!(!returned.0, "{returned:?}");
    let batch = published
        .try_recv()
        .expect("published before the call answered");
    let cards = batch["attachments"].as_array().unwrap();
    assert_eq!(cards.len(), 2, "{batch}");
    for card in cards {
        let id = card["id"].as_str().unwrap();
        assert!(
            intent_core::turn_attachments::is_attachment_id(id),
            "{card}"
        );
        assert_eq!(
            serde_json::from_str::<Value>(card["text"].as_str().unwrap()).unwrap()["attachmentId"],
            id,
            "{card}"
        );
    }
    assert!(
        returned.1.contains(cards[0]["id"].as_str().unwrap()),
        "the output carries the batch's nonce: {}",
        returned.1
    );
    assert!(published.try_recv().is_err(), "one batch per call");

    let discarded = call(
        &server,
        r#"await ws.workspace.proposeSibling({ title: "C", initialPrompt: "Go" }); return "done";"#,
    )
    .await;
    assert!(!discarded.0, "{discarded:?}");
    let batch = published
        .try_recv()
        .expect("a discarded card is published too");
    assert_eq!(batch["attachments"].as_array().unwrap().len(), 1, "{batch}");
    answering.abort();
}

/// Review of intentd#2786: an agent-initiated commit passes the same
/// auto-commit gate a local one does, with the policy read from head; with
/// no answer from head it is refused, and `userRequested` bypasses the gate.
#[tokio::test]
async fn git_commit_honors_the_workspaces_auto_commit_setting_from_head() {
    if intent_providers::resolve_on_path("git").is_none() {
        eprintln!("skipping: git not on PATH");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let repo = dir.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("one.txt"), "1").unwrap();

    let unlinked = server(&agent(), &workspace(), &repo, &gates(), HeadLink::default()).unwrap();
    let (failed, text) = call(&unlinked, r#"return await ws.git.commit("one");"#).await;
    assert!(failed && text.contains("auto-commit setting"), "{text}");

    let (node_end, mut head_end, _served) = bound_link(dir.path()).await;
    let head = HeadLink::default();
    head.set(node_end);
    let server = server(&agent(), &workspace(), &repo, &gates(), head).unwrap();
    let answering = tokio::spawn(async move {
        let mut policies = Vec::new();
        for enabled in [false, true] {
            loop {
                let Some(LinkEvent::Request(request)) = head_end.events.recv().await else {
                    continue;
                };
                policies.push(request.request().params.clone());
                request
                    .respond(LinkOutcome::Ok(json!({ "autoCommit": enabled })))
                    .await
                    .unwrap();
                break;
            }
        }
        (policies, head_end)
    });
    let (failed, text) = call(&server, r#"return await ws.git.commit("one");"#).await;
    assert!(failed && text.contains("Auto-commit is disabled"), "{text}");
    let head_commit = std::process::Command::new("git")
        .arg("-C")
        .arg(&repo)
        .args(["rev-parse", "--verify", "--quiet", "HEAD"])
        .output()
        .unwrap();
    assert!(!head_commit.status.success(), "nothing was committed");
    let allowed = ok_json(
        call(
            &server,
            r#"return JSON.stringify(await ws.git.commit("one"));"#,
        )
        .await,
    );
    assert_eq!(allowed["files"], json!(["one.txt"]), "{allowed}");
    let (policies, _head_end) = tokio::time::timeout(wait(), answering)
        .await
        .unwrap()
        .unwrap();
    assert!(policies
        .iter()
        .all(|params| params["method"] == json!(COMMIT_POLICY_METHOD)));
}

/// Review of intentd#2786: the session's captured features gate the node's
/// local calls exactly as a local bridge's do, and a snapshot the node cannot
/// read starts no server.
#[tokio::test]
async fn the_sessions_feature_snapshot_gates_local_calls() {
    let dir = tempfile::tempdir().unwrap();
    let features = AgentFeaturesSettings {
        host_exec: false,
        ..AgentFeaturesSettings::default()
    };
    let gated = Gates {
        agent_features: serde_json::to_value(features).unwrap(),
        is_sub_agent: true,
        ..gates()
    };
    let server = server(
        &agent(),
        &workspace(),
        dir.path(),
        &gated,
        HeadLink::default(),
    )
    .unwrap();
    let (failed, text) = call(
        &server,
        r#"return await host({ method: "host.exec", args: { command: "pwd" } });"#,
    )
    .await;
    assert!(failed && text.contains("hostExec = false"), "{text}");
    let missing = call(&server, "return typeof ws.host;").await;
    assert_eq!(ok_json(missing), json!("undefined"));
    ok_json(
        call(
            &server,
            r#"return JSON.stringify(await ws.file.write("a.txt", "x"));"#,
        )
        .await,
    );

    let unreadable = Gates {
        agent_features: json!({ "noSuchFeature": true }),
        ..gates()
    };
    assert!(server_fails(dir.path(), &unreadable));
}

fn server_fails(dir: &Path, gates: &Gates) -> bool {
    server(&agent(), &workspace(), dir, gates, HeadLink::default()).is_err()
}

/// A provider that truncates long tool descriptions gets the compact
/// `workspace_api` description on the machine, as a local bridge serves
/// it: an index pointing at the reference head puts in the instructions.
#[tokio::test]
async fn a_truncating_provider_gets_the_compact_tool_description() {
    let dir = tempfile::tempdir().unwrap();
    let mut described = Vec::new();
    for compact in [false, true] {
        let gates = Gates {
            compact_tool_descriptions: compact,
            ..gates()
        };
        let server = server(
            &agent(),
            &workspace(),
            dir.path(),
            &gates,
            HeadLink::default(),
        )
        .unwrap();
        let listed = server
            .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
            .await
            .unwrap();
        described.push(
            listed["result"]["tools"][0]["description"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    let (full, compact) = (&described[0], &described[1]);
    assert!(full.contains("ws.note.read("), "{full}");
    assert!(
        compact.contains("system-prompt \"Workspace API Reference\""),
        "{compact}"
    );
    assert!(!compact.contains("ws.note.read("), "{compact}");
    assert!(compact.len() < full.len());
}

async fn listed_description(server: &WorkspaceMcpServer) -> String {
    let listed = server
        .handle_message(&json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/list" }))
        .await
        .unwrap();
    listed["result"]["tools"][0]["description"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// The launch's `harnessVersion` picks the `workspace_api` description the
/// machine serves, in both forms, matching that version's docs: a frozen
/// pre-3.10 stamp and an older backend's launch (no field) get the legacy
/// text, and every registered stamp the docs its row resolves to (which
/// version maps to which row is pinned in intent-services' harness tests).
/// Version strings come from the constants so a bump never breaks this.
#[tokio::test]
async fn the_tool_description_follows_the_launch_harness_version() {
    use intent_acp::{WorkspaceApiDocs, WORKSPACE_API_DOCS_V1};
    use intent_services::feature_api::harness::launch_workspace_api_docs;
    let dir = tempfile::tempdir().unwrap();
    let mut cases: Vec<(Option<&str>, &'static dyn WorkspaceApiDocs)> = vec![
        (None, &WORKSPACE_API_DOCS_V1),
        (Some("not-a-version"), &WORKSPACE_API_DOCS_V1),
        (Some("3.8"), &WORKSPACE_API_DOCS_V1),
    ];
    cases.extend(
        intent_core::REPOSITORY_GUIDANCE_HARNESS_VERSIONS
            .iter()
            .map(|v| (Some(*v), launch_workspace_api_docs(Some(v)))),
    );
    for compact in [false, true] {
        for &(version, docs) in &cases {
            let launched = Gates {
                compact_tool_descriptions: compact,
                harness_version: version.map(str::to_owned),
                ..gates()
            };
            let remote = server(
                &agent(),
                &workspace(),
                dir.path(),
                &launched,
                HeadLink::default(),
            )
            .unwrap();
            let reference = server(
                &agent(),
                &workspace(),
                dir.path(),
                &Gates {
                    harness_version: None,
                    ..launched
                },
                HeadLink::default(),
            )
            .unwrap()
            .with_workspace_api_docs(docs);
            assert_eq!(
                listed_description(&remote).await,
                listed_description(&reference).await,
                "{version:?} compact={compact}"
            );
        }
    }
}

/// nodeProtocol 11: each user MCP server head names becomes one more
/// `session/new` entry after `workspace-mcp` (a reserved or repeated name is
/// skipped), running `intentd mcp-bridge` against its own authenticated
/// listener; a provider's `tools/call` there reaches head as the agent's
/// `mcp.proxy.callTool` frame with the proxy's longer link budget.
#[tokio::test]
async fn proxied_user_mcp_servers_ride_session_new_and_call_head() {
    use intent_acp::session::McpServer;
    use intent_core::remote_launch::workspace_api::MCP_PROXY_CALL_TOOL_METHOD;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let dir = tempfile::tempdir().unwrap();
    let (node_end, mut head_end, _served) = bound_link(dir.path()).await;
    let head = HeadLink::default();
    head.set(node_end);
    let proxy = |name: &str, id: &str| McpProxy {
        name: name.into(),
        server_id: id.into(),
    };
    let gates = Gates {
        mcp_proxies: vec![
            proxy("Linear", "srv-1"),
            proxy("workspace-mcp", "srv-2"),
            proxy("Linear", "srv-3"),
        ],
        ..gates()
    };
    let answering = tokio::spawn(async move {
        while let Some(event) = head_end.events.recv().await {
            let LinkEvent::Request(request) = event else {
                continue;
            };
            let frame = request.request().clone();
            if frame.params["method"] != MCP_PROXY_CALL_TOOL_METHOD {
                request
                    .respond(LinkOutcome::Err(LinkRpcError {
                        code: -32601,
                        message: "not here".into(),
                        data: None,
                    }))
                    .await
                    .unwrap();
                continue;
            }
            request
                .respond(LinkOutcome::Ok(json!({
                    "content": [{ "type": "text", "text": "from head" }],
                })))
                .await
                .unwrap();
            return frame;
        }
        panic!("the link closed");
    });
    let (_bridges, entries) = serve(
        &agent(),
        &workspace(),
        dir.path(),
        (&gates, None),
        (head, Arc::new(|_| Box::pin(async { false }))),
        Path::new("/opt/intent/intentd"),
    )
    .await
    .unwrap();
    let stdio: Vec<_> = entries
        .iter()
        .map(|entry| match entry {
            McpServer::Stdio(stdio) => stdio.clone(),
            other => panic!("not stdio: {other:?}"),
        })
        .collect();
    let names: Vec<&str> = stdio.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["workspace-mcp", "Linear"]);
    for entry in &stdio {
        let gh_token = entry.env.iter().find(|var| var.name == "GH_TOKEN");
        assert_eq!(
            gh_token.map(|var| var.value.as_str()),
            Some(""),
            "{} blanks GH_TOKEN",
            entry.name
        );
    }
    let linear = &stdio[1];
    assert_eq!(linear.args[0], "mcp-bridge");
    assert_ne!(linear.args[2], stdio[0].args[2], "its own listener");
    let token = linear
        .env
        .iter()
        .find(|var| var.name == intent_acp::BRIDGE_TOKEN_ENV)
        .map(|var| var.value.clone())
        .unwrap();

    let mut stream = tokio::net::TcpStream::connect(&linear.args[2])
        .await
        .unwrap();
    stream
        .write_all(intent_acp::bridge_auth_line(&token).as_bytes())
        .await
        .unwrap();
    let request = json!({ "jsonrpc": "2.0", "id": 7, "method": "tools/call",
                          "params": { "name": "echo", "arguments": { "input": "x" } } });
    stream
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let mut lines = BufReader::new(stream).lines();
    let reply: Value = serde_json::from_str(
        &tokio::time::timeout(wait(), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(reply["id"], 7, "{reply}");
    assert_eq!(
        reply["result"]["content"][0]["text"], "from head",
        "{reply}"
    );

    let frame = tokio::time::timeout(wait(), answering)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.agent_id, agent());
    assert_eq!(frame.method, CALL_METHOD);
    assert_eq!(
        frame.params["args"],
        json!({ "serverId": "srv-1", "name": "echo", "arguments": { "input": "x" } })
    );
    assert_eq!(
        u128::from(frame.timeout_ms),
        MCP_PROXY_LINK_TIMEOUT.as_millis()
    );
}

/// [`gates`] with secret requests on (they default to off).
fn secret_gates() -> Gates {
    let features = AgentFeaturesSettings {
        secret_requests: true,
        host_exec: true,
        ..AgentFeaturesSettings::default()
    };
    Gates {
        agent_features: serde_json::to_value(features).unwrap(),
        ..gates()
    }
}

const SECRET_EXEC: &str = r#"return JSON.stringify(await ws.host.exec({ command: "sh", args: ["-c", "printf 'k=%s' \"$API_KEY\""], secretEnv: ["API_KEY"] }));"#;

/// nodeProtocol 14: a remote agent's `secretEnv` resolves from the secrets
/// it holds on this machine, with the value redacted from the output, and
/// another agent on the same machine never reaches them.
#[tokio::test]
async fn secret_env_uses_only_the_agents_own_secrets_on_the_machine() {
    let dir = tempfile::tempdir().unwrap();
    let (node_end, _head_end, _served) = bound_link(dir.path()).await;
    let head = HeadLink::default();
    head.set(node_end);
    head.secrets().put(
        &agent(),
        "API_KEY".into(),
        zeroize::Zeroizing::new("canary-machine-secret".into()),
    );
    let owner = server(
        &agent(),
        &workspace(),
        dir.path(),
        &secret_gates(),
        head.clone(),
    )
    .unwrap();
    let ran = ok_json(call(&owner, SECRET_EXEC).await);
    assert_eq!(ran["exitCode"], json!(0), "{ran}");
    assert_eq!(ran["stdout"], "k=[secret:API_KEY]", "{ran}");

    let other = AgentId::from("agent-other");
    let other_server = server(
        &other,
        &workspace(),
        dir.path(),
        &secret_gates(),
        head.clone(),
    )
    .unwrap();
    let refused = call(&other_server, SECRET_EXEC).await;
    assert!(refused.0, "{refused:?}");
    assert!(refused.1.contains("secret-unavailable"), "{refused:?}");
    assert!(!refused.1.contains("canary-machine-secret"), "{refused:?}");
    assert_eq!(head.secrets().names(&other), Vec::<String>::new());
}

/// On a link below nodeProtocol 14 a remote agent has no `secretEnv`: the
/// call fails before anything runs, even if a value were held.
#[tokio::test]
async fn secret_env_is_refused_on_an_older_link() {
    let dir = tempfile::tempdir().unwrap();
    let (node_end, _head_end, _served) = crate::executor::test_link::bound_link_offering(
        dir.path(),
        intent_core::node_link::ProtocolRange { min: 7, max: 13 },
    )
    .await;
    let head = HeadLink::default();
    head.set(node_end);
    head.secrets().put(
        &agent(),
        "API_KEY".into(),
        zeroize::Zeroizing::new("canary-machine-secret".into()),
    );
    let server = server(&agent(), &workspace(), dir.path(), &secret_gates(), head).unwrap();
    let refused = call(&server, SECRET_EXEC).await;
    assert!(refused.0, "{refused:?}");
    assert!(
        refused.1.contains("secretEnv is unavailable"),
        "{refused:?}"
    );
    assert!(!refused.1.contains("canary-machine-secret"), "{refused:?}");
}

/// Head lists a remote agent's git roots relative to its checkout; the
/// machine makes each one absolute in the agent's checkout and reads its
/// branch there, and leaves out one not checked out on the machine. A row
/// head already made absolute (an older backend) passes through
/// (intent-hq/intent#8692).
#[tokio::test]
async fn git_roots_from_head_are_resolved_in_the_agents_checkout() {
    let dir = tempfile::tempdir().unwrap();
    let work = dir.path().join("work");
    let module = work.join("a/b");
    std::fs::create_dir_all(&module).unwrap();
    git(&module, &["init", "--quiet", "-b", "topic"]);
    git(
        &module,
        &[
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            "b",
        ],
    );
    let (node_end, mut head_end, _served) = bound_link(dir.path()).await;
    let head = HeadLink::default();
    head.set(node_end);
    let server = server(&agent(), &workspace(), &work, &gates(), head).unwrap();
    let answering = tokio::spawn(async move {
        while let Some(event) = head_end.events.recv().await {
            if let LinkEvent::Request(request) = event {
                request
                    .respond(LinkOutcome::Ok(json!([
                        { "id": "r-1", "path": "a/b", "source": "auto" },
                        { "id": "r-2", "path": "/backend/ws/c", "branch": "main" },
                        { "id": "r-3", "path": "missing", "source": "auto" },
                    ])))
                    .await
                    .unwrap();
            }
        }
    });
    let roots = ok_json(call(&server, "return JSON.stringify(await ws.git.listRoots());").await);
    assert_eq!(
        roots,
        json!([
            {
                "id": "r-1",
                "path": module.to_string_lossy(),
                "source": "auto",
                "branch": "topic",
            },
            { "id": "r-2", "path": "/backend/ws/c", "branch": "main" },
        ])
    );
    answering.abort();
}
