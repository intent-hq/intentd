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
    for code in [
        "return await ws.terminal.list();",
        "return await ws.hook.list();",
        "return await ws.desktop.listDisplay();",
        "return await ws.browser.listTabs();",
        "return await ws.workspace.archive();",
        r#"return await ws.file.getAttachment("att-1");"#,
        r#"return await ws.git.registerRoot("sub");"#,
    ] {
        let (failed, text) = call(&server, code).await;
        assert!(failed, "{code}: {text}");
        assert!(
            text.contains("is not supported on remote agents yet"),
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
    let (failed, text) = call(&server, "return await ws.workspace.archive();").await;
    assert!(
        failed && text.contains("remote-agent-unsupported"),
        "{text}"
    );
    answering.abort();
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
        is_sub_agent: false,
    };
    assert!(server_fails(dir.path(), &unreadable));
}

fn server_fails(dir: &Path, gates: &Gates) -> bool {
    server(&agent(), &workspace(), dir, gates, HeadLink::default()).is_err()
}
