//! Real-WSS coverage for importing an existing provider session
//! (`agent.importableSessions.list` / `agent.importSession`, PROTOCOL §5.5).
//!
//! The mock ACP agent keeps a Claude-style store (`MOCK_AGENT_SESSION_STORE=
//! claude`, `behavior.sessionImport`) under a fixture `~/.claude`, seeded with
//! a transcript shaped like a real Claude Code session. The list must return
//! it with its title and folder; the import must copy it into the checkout's
//! project folder under a new id with `sessionId` / `cwd` rewritten, leave the
//! source byte-identical, and give the new agent the replayed history. The
//! agent's first turn then resumes the copy with `session/load`.

#![cfg(unix)]

use crate::common;

use intentd_test_support::GuardedChild;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio_tungstenite::tungstenite::Message;

const SESSION_ID: &str = "6eca79e3-7963-44ad-b89e-e781d28df2b9";
const SOURCE_CWD: &str = "/home/dev/projects/ember-cloudflare";

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

struct Daemon {
    child: GuardedChild,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill_tree();
    }
}

#[derive(Debug)]
struct PinnedVerifier {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let actual = Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if actual == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("fingerprint mismatch".into()))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_config(fingerprint: &str) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fingerprint.to_string(),
            provider,
        }))
        .with_no_client_auth();
    Arc::new(config)
}

fn mock_agent_script() -> Option<String> {
    let script = format!(
        "{}/tests/fixtures/mock-acp-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    if intent_providers::resolve_on_path("node").is_none() || !Path::new(&script).exists() {
        eprintln!("skipping WSS session import e2e: mock agent unavailable");
        return None;
    }
    Some(script)
}

fn claude_slug(path: &str) -> String {
    path.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

/// Write a transcript under the fixture home with a fixed modification time.
fn store_session(home: &Path, cwd: &str, id: &str, body: &[u8], modified: SystemTime) -> PathBuf {
    let dir = home.join(".claude/projects").join(claude_slug(cwd));
    std::fs::create_dir_all(&dir).expect("mkdir project");
    let path = dir.join(format!("{id}.jsonl"));
    std::fs::write(&path, body).expect("write session");
    std::fs::File::options()
        .write(true)
        .open(&path)
        .and_then(|f| f.set_modified(modified))
        .expect("set mtime");
    path
}

fn at(seconds: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

fn spawn_serve(
    data_dir: &Path,
    home: &Path,
    script: &str,
    session_log: &Path,
    rpc_log: &Path,
    behavior: &Value,
    extra_env: &[(&str, String)],
) -> Daemon {
    common::enable_ws_api(data_dir);
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir workspaces dir");
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("daemon log");
    let mut command = common::hermetic_serve_command(data_dir);
    command
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("HOME", home)
        .env_remove("CLAUDE_CONFIG_DIR")
        .env("MOCK_AGENT_SCRIPT_PATH", script)
        .env("MOCK_AGENT_BEHAVIOR", behavior.to_string())
        .env("MOCK_AGENT_SESSION_STORE", "claude")
        .env("MOCK_AGENT_SESSION_LOG", session_log)
        .env("MOCK_AGENT_RPC_LOG", rpc_log)
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (key, value) in extra_env {
        command.env(key, value);
    }
    common::hermetic_fixture_identity(&mut command, data_dir);
    let child = GuardedChild::spawn(&mut command).expect("spawn intentd serve");
    Daemon { child }
}

async fn seed_workspace(data_dir: &Path, worktree: &Path) -> String {
    use intent_core::{now_iso, Workspace, WorkspaceId};
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let id = WorkspaceId::new();
    let timestamp = now_iso();
    let workspace = Workspace {
        id: id.clone(),
        title: "Session Import E2E".into(),
        branch: "main".into(),
        worktree_path: Some(worktree.to_string_lossy().into_owned()),
        created_at: timestamp.clone(),
        updated_at: timestamp,
        ..Default::default()
    };
    store
        .insert_workspace(&workspace)
        .await
        .expect("insert workspace");
    id.0
}

async fn rpc_envelope(ws: &mut common::TlsWs, id: i64, method: &str, params: Value) -> Value {
    ws.send(Message::Text(
        json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
            .to_string()
            .into(),
    ))
    .await
    .expect("send RPC");
    loop {
        let frame = tokio::time::timeout(common::rpc_read_timeout(), ws.next())
            .await
            .expect("RPC timed out");
        match frame {
            Some(Ok(Message::Text(text))) => {
                let value: Value = serde_json::from_str(&text).expect("JSON response");
                if value["id"] == json!(id) {
                    return value;
                }
            }
            Some(Ok(Message::Ping(payload))) => {
                ws.send(Message::Pong(payload)).await.expect("pong");
            }
            Some(Ok(_)) => {}
            other => panic!("expected WSS frame, got {other:?}"),
        }
    }
}

fn texts(blocks: &Value, kind: &str) -> Vec<String> {
    blocks
        .as_array()
        .expect("blocks")
        .iter()
        .filter(|b| b["type"] == kind)
        .map(|b| b["text"].as_str().unwrap_or_default().to_string())
        .collect()
}

#[tokio::test]
async fn lists_and_imports_a_claude_session_over_real_wss() {
    let Some(script) = mock_agent_script() else {
        return;
    };
    let data_dir_guard = common::test_tempdir_in("/tmp", "itd-wss-import-");
    let data_dir = data_dir_guard.path().to_path_buf();
    let home = data_dir.join("home");
    let worktree = data_dir.join("worktree");
    std::fs::create_dir_all(&worktree).expect("mkdir worktree");
    let session_log = data_dir.join("sessions.log");
    let rpc_log = data_dir.join("rpc.log");

    let fixture = std::fs::read(format!(
        "{}/tests/fixtures/session-import/claude-session.jsonl",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("read fixture");
    let source = store_session(&home, SOURCE_CWD, SESSION_ID, &fixture, at(1_793_000_000));
    let older = br#"{"type":"user","cwd":"/home/dev/old","sessionId":"older-1","message":{"role":"user","content":"Old work"}}
"#;
    store_session(&home, "/home/dev/old", "older-1", older, at(1_792_000_000));
    let newest = br#"{"type":"user","cwd":"/home/dev/new","sessionId":"newest-1","message":{"role":"user","content":"New work"}}
{"type":"custom-title","customTitle":"Newest session","sessionId":"newest-1"}
"#;
    store_session(
        &home,
        "/home/dev/new",
        "newest-1",
        newest,
        at(1_794_000_000),
    );

    // A session started in a subfolder of a git repo lists the repo's
    // top-level as `repoRoot`; the sessions above (folders that do not
    // exist on this host) have none.
    let repo = data_dir.join("repo");
    let repo_sub = repo.join("apps/web");
    std::fs::create_dir_all(&repo_sub).expect("mkdir repo subfolder");
    let init = std::process::Command::new("git")
        .args(["init", "-q"])
        .arg(&repo)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .status()
        .expect("run git init");
    assert!(init.success(), "git init failed");
    let repo_root = repo
        .canonicalize()
        .expect("canonical repo")
        .to_string_lossy()
        .into_owned();
    let repo_cwd = repo_sub.to_string_lossy().into_owned();
    let in_repo = format!(
        "{{\"type\":\"user\",\"cwd\":{cwd},\"sessionId\":\"repo-1\",\"message\":{{\"role\":\"user\",\"content\":\"Repo work\"}}}}\n",
        cwd = json!(repo_cwd)
    );
    store_session(
        &home,
        &repo_cwd,
        "repo-1",
        in_repo.as_bytes(),
        at(1_791_000_000),
    );

    let workspace_id = seed_workspace(&data_dir, &worktree).await;
    let behavior = json!({ "sessionImport": { "pageSize": 2 }, "response": "resumed" });
    let _daemon = spawn_serve(
        &data_dir,
        &home,
        &script,
        &session_log,
        &rpc_log,
        &behavior,
        &[],
    );
    let status = common::await_wss_status_logged(
        &data_dir.join("intentd.sock"),
        &data_dir.join("daemon.log"),
    )
    .await;
    let port = u16::try_from(status["result"]["port"].as_u64().expect("WSS port"))
        .expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint");
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let mut ws = common::wss_connect_with_retry(port, client_config(fingerprint), &url).await;

    // Support detection: `sessionImport: 1` on system.capabilities.
    let caps = rpc_envelope(&mut ws, 0, "system.capabilities", json!({})).await;
    assert_eq!(caps["result"]["sessionImport"], 1, "{caps}");

    // List: merged across the adapter's pages, newest first, exact shape.
    let listed = rpc_envelope(
        &mut ws,
        1,
        "agent.importableSessions.list",
        json!({ "provider": "mock" }),
    )
    .await;
    assert_eq!(
        listed,
        json!({ "jsonrpc": "2.0", "id": 1, "result": {
            "sessions": [
                { "provider": "mock", "sessionId": "newest-1", "title": "Newest session",
                  "cwd": "/home/dev/new", "updatedAt": "2026-11-06T21:20:00.000Z" },
                { "provider": "mock", "sessionId": SESSION_ID,
                  "title": "Cloudflare account installation", "cwd": SOURCE_CWD,
                  "updatedAt": "2026-10-26T07:33:20.000Z" },
                { "provider": "mock", "sessionId": "older-1", "title": "older-1",
                  "cwd": "/home/dev/old", "updatedAt": "2026-10-14T17:46:40.000Z" },
                { "provider": "mock", "sessionId": "repo-1", "title": "repo-1",
                  "cwd": repo_cwd, "repoRoot": repo_root,
                  "updatedAt": "2026-10-03T04:00:00.000Z" },
            ],
            "providers": [{ "id": "mock", "label": "Mock (E2E)", "status": "ok" }],
        }})
    );

    // Paging and search.
    let page = rpc_envelope(
        &mut ws,
        2,
        "agent.importableSessions.list",
        json!({ "provider": "mock", "limit": 1, "cursor": "o:1" }),
    )
    .await;
    assert_eq!(page["result"]["sessions"][0]["sessionId"], SESSION_ID);
    assert_eq!(page["result"]["nextCursor"], "o:2");
    let found = rpc_envelope(
        &mut ws,
        3,
        "agent.importableSessions.list",
        json!({ "provider": "mock", "query": "CLOUDFLARE" }),
    )
    .await;
    let found = found["result"]["sessions"].as_array().expect("sessions");
    assert_eq!(found.len(), 1);
    assert_eq!(found[0]["sessionId"], SESSION_ID);

    // Unknown provider and missing session.
    let unknown = rpc_envelope(
        &mut ws,
        4,
        "agent.importableSessions.list",
        json!({ "provider": "nope" }),
    )
    .await;
    assert_eq!(unknown["error"]["code"], -32602, "{unknown}");
    let missing = rpc_envelope(
        &mut ws,
        5,
        "agent.importSession",
        json!({ "workspaceId": workspace_id, "provider": "mock", "sessionId": "missing-1" }),
    )
    .await;
    assert_eq!(missing["error"]["code"], -32602, "{missing}");
    assert_eq!(missing["error"]["data"]["code"], "not-found", "{missing}");

    // Import.
    let imported = rpc_envelope(
        &mut ws,
        6,
        "agent.importSession",
        json!({ "workspaceId": workspace_id, "provider": "mock", "sessionId": SESSION_ID }),
    )
    .await;
    assert!(imported.get("error").is_none(), "import: {imported}");
    let result = &imported["result"];
    assert_eq!(result["forked"], true);
    assert_eq!(result["messageCount"], 4);
    let agent = &result["agent"];
    assert_eq!(agent["name"], "Cloudflare account installation");
    assert_eq!(agent["provider"], "mock");
    assert_eq!(agent["workspaceId"], workspace_id);
    let new_id = result["sessionId"].as_str().expect("new session id");
    assert_ne!(new_id, SESSION_ID);
    let agent_id = agent["id"].as_str().expect("agent id").to_string();

    // The source is untouched; the copy lives under the checkout's slug
    // with `sessionId` and the `cwd`s under the source folder rewritten.
    assert_eq!(std::fs::read(&source).expect("read source"), fixture);
    let worktree_str = worktree.to_string_lossy().into_owned();
    let forked = home
        .join(".claude/projects")
        .join(claude_slug(&worktree_str))
        .join(format!("{new_id}.jsonl"));
    let rows: Vec<Value> = std::fs::read_to_string(&forked)
        .expect("forked transcript")
        .lines()
        .map(|l| serde_json::from_str(l).expect("JSON row"))
        .collect();
    assert_eq!(rows.len(), 14);
    for row in &rows {
        if let Some(id) = row.get("sessionId") {
            assert_eq!(id, new_id, "{row}");
        }
        if let Some(cwd) = row.get("cwd").and_then(Value::as_str) {
            assert!(cwd.starts_with(&worktree_str), "{row}");
        }
    }
    assert!(rows
        .iter()
        .any(|r| r["cwd"] == format!("{worktree_str}/apps/worker")));

    // The agent's history is the replayed conversation.
    let convo = rpc_envelope(
        &mut ws,
        7,
        "agent.getConversation",
        json!({ "agentId": agent_id, "workspaceId": workspace_id }),
    )
    .await;
    let messages = convo["result"]["messages"].as_array().expect("messages");
    let roles: Vec<&str> = messages
        .iter()
        .map(|m| m["role"].as_str().unwrap_or_default())
        .collect();
    assert_eq!(roles, ["user", "assistant", "user", "assistant"], "{convo}");
    let blocks = |i: usize| {
        messages[i]
            .get("contentBlocks")
            .or_else(|| messages[i].get("content"))
            .cloned()
            .unwrap_or(Value::Null)
    };
    assert_eq!(
        texts(&blocks(0), "text"),
        ["Install the Cloudflare account in the worker config"]
    );
    assert_eq!(
        texts(&blocks(1), "text"),
        [
            "I will read the worker config.",
            "Added account_id to wrangler.toml."
        ]
    );
    let first_reply = blocks(1);
    let tool = first_reply
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["type"] == "tool_use")
        .expect("tool_use block");
    assert_eq!(tool["toolCallId"], "toolu_01");
    assert_eq!(tool["metadata"]["status"], "completed");
    assert!(first_reply
        .as_array()
        .unwrap()
        .iter()
        .any(|b| b["type"] == "tool_result" && b["tool_use_id"] == "toolu_01"));
    assert_eq!(texts(&blocks(2), "text"), ["Thanks, that works"]);
    assert_eq!(texts(&blocks(3), "text"), ["Glad it works."]);

    // The first turn resumes the copy with `session/load` in the checkout.
    let sent = rpc_envelope(
        &mut ws,
        8,
        "agent.sendMessage",
        json!({ "workspaceId": workspace_id, "agentId": agent_id, "content": "continue" }),
    )
    .await;
    assert_eq!(sent["result"]["success"], true, "send: {sent}");
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    loop {
        let loads: Vec<Value> = std::fs::read_to_string(&session_log)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter(|e| e["method"] == "session/load" && e["sessionId"] == new_id)
            .collect();
        // One load for the import's replay, one for the turn's resume.
        if loads.len() >= 2 {
            assert!(loads.iter().all(|e| e["cwd"] == worktree_str.as_str()));
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "turn never resumed the imported session: {loads:?}"
        );
        // timing-guard: polls a file the mock child appends to; no event marks it.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // The first prompt after the import carries one hidden <system> block
    // with the system prompt (the mock is a FirstTurnPrepend provider whose
    // resumed session never got it) and the import note; the next does not.
    let first = wait_for_prompts(&rpc_log, 1).await;
    assert!(first[0].starts_with("<system>"), "{}", first[0]);
    assert_eq!(first[0].matches("<system>").count(), 1, "{}", first[0]);
    assert!(
        first[0].contains("has been imported into Intent"),
        "{}",
        first[0]
    );
    assert!(
        first[0].contains(&format!("under {SOURCE_CWD} now live under {worktree_str}")),
        "{}",
        first[0]
    );
    assert!(first[0].contains("Session Import E2E"), "{}", first[0]);
    let system_prompt = rpc_envelope(
        &mut ws,
        9,
        "agent.getSession",
        json!({ "agentId": agent_id, "workspaceId": workspace_id }),
    )
    .await;
    let system_prompt = system_prompt["result"]["systemPrompt"]
        .as_str()
        .or_else(|| system_prompt["result"]["session"]["systemPrompt"].as_str())
        .unwrap_or_default()
        .to_string();
    assert!(
        !system_prompt.is_empty(),
        "the turn persisted a system prompt"
    );
    assert!(
        first[0].contains(system_prompt.trim()),
        "first prompt carries the system prompt"
    );
    let sent = rpc_envelope(
        &mut ws,
        10,
        "agent.sendMessage",
        json!({ "workspaceId": workspace_id, "agentId": agent_id, "content": "and again" }),
    )
    .await;
    assert_eq!(sent["result"]["success"], true, "send: {sent}");
    let prompts = wait_for_prompts(&rpc_log, 2).await;
    assert!(!prompts[1].contains("<system>"), "{}", prompts[1]);
    assert!(
        !prompts[1].contains("imported into Intent"),
        "{}",
        prompts[1]
    );

    // The user-visible rows never show the hidden block.
    let convo = rpc_envelope(
        &mut ws,
        11,
        "agent.getConversation",
        json!({ "agentId": agent_id, "workspaceId": workspace_id }),
    )
    .await;
    let rendered = convo["result"]["messages"].to_string();
    assert!(!rendered.contains("<system>"), "{convo}");
    assert!(rendered.contains("\"continue\""), "{convo}");
}

/// The `method` of every request the mock adapters received, in order.
fn adapter_methods(rpc_log: &Path) -> Vec<String> {
    std::fs::read_to_string(rpc_log)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .filter_map(|e| e["method"].as_str().map(str::to_owned))
        .collect()
}

fn count(methods: &[String], method: &str) -> usize {
    methods.iter().filter(|m| *m == method).count()
}

/// Regression for intent-hq/intent#9232: an adapter whose `session/list`
/// cannot page through a huge store in time. The list reads only the pages
/// the requested window needs, keeps the rows read before the deadline when
/// the adapter stops answering (with `truncated` and a WARN), and serves
/// later calls from the same listing instead of starting another adapter.
#[tokio::test]
async fn list_reads_only_needed_pages_and_keeps_rows_when_the_adapter_stalls() {
    let Some(script) = mock_agent_script() else {
        return;
    };
    let data_dir_guard = common::test_tempdir_in("/tmp", "itd-wss-import-stall-");
    let data_dir = data_dir_guard.path().to_path_buf();
    let home = data_dir.join("home");
    let session_log = data_dir.join("sessions.log");
    let rpc_log = data_dir.join("rpc.log");
    for n in 1..=5u64 {
        let id = format!("stall-{n}");
        let cwd = format!("/home/dev/stall/{n}");
        let row = json!({ "type": "user", "cwd": cwd, "sessionId": id,
            "message": { "role": "user", "content": "work" } });
        store_session(
            &home,
            &cwd,
            &id,
            format!("{row}\n").as_bytes(),
            at(1_790_000_000 + n * 1000),
        );
    }

    // Two sessions per page; the second page never answers.
    let behavior = json!({ "sessionImport": { "pageSize": 2, "stallAfterPages": 1 } });
    let budget = common::test_timeout(Duration::from_secs(5));
    let _daemon = spawn_serve(
        &data_dir,
        &home,
        &script,
        &session_log,
        &rpc_log,
        &behavior,
        &[(
            "INTENTD_SESSION_IMPORT_TIMEOUT_MS",
            budget.as_millis().to_string(),
        )],
    );
    let status = common::await_wss_status_logged(
        &data_dir.join("intentd.sock"),
        &data_dir.join("daemon.log"),
    )
    .await;
    let port = u16::try_from(status["result"]["port"].as_u64().expect("WSS port"))
        .expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint");
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    let mut ws = common::wss_connect_with_retry(port, client_config(fingerprint), &url).await;
    let row = |n: u64, updated: &str| {
        json!({ "provider": "mock", "sessionId": format!("stall-{n}"),
            "title": format!("stall-{n}"), "cwd": format!("/home/dev/stall/{n}"),
            "updatedAt": updated })
    };
    let newest = row(5, "2026-09-21T15:36:40.000Z");
    let second = row(4, "2026-09-21T15:20:00.000Z");

    // The first window fits in the first page: one `session/list`, and a
    // cursor because the adapter has more.
    let first = rpc_envelope(
        &mut ws,
        1,
        "agent.importableSessions.list",
        json!({ "provider": "mock", "limit": 2 }),
    )
    .await;
    assert_eq!(
        first,
        json!({ "jsonrpc": "2.0", "id": 1, "result": {
            "sessions": [newest, second],
            "providers": [{ "id": "mock", "label": "Mock (E2E)", "status": "ok" }],
            "nextCursor": "o:2",
        }})
    );
    let methods = adapter_methods(&rpc_log);
    assert_eq!(count(&methods, "initialize"), 1, "{methods:?}");
    assert_eq!(count(&methods, "session/list"), 1, "{methods:?}");

    // A search needs every page: the second one stalls, so the call keeps
    // the first page's rows and marks the provider truncated.
    let searched = rpc_envelope(
        &mut ws,
        2,
        "agent.importableSessions.list",
        json!({ "provider": "mock", "query": "/home/dev/stall" }),
    )
    .await;
    assert_eq!(
        searched,
        json!({ "jsonrpc": "2.0", "id": 2, "result": {
            "sessions": [newest, second],
            "providers": [{ "id": "mock", "label": "Mock (E2E)", "status": "ok",
                "truncated": true }],
        }})
    );
    let methods = adapter_methods(&rpc_log);
    assert_eq!(count(&methods, "initialize"), 2, "{methods:?}");
    assert_eq!(count(&methods, "session/list"), 3, "{methods:?}");
    let log = std::fs::read_to_string(data_dir.join("daemon.log")).unwrap_or_default();
    assert!(
        log.lines()
            .any(|l| l.contains("WARN") && l.contains("timed out") && l.contains("mock")),
        "no WARN for the provider timeout:\n{log}"
    );

    // Paging and searching again reuse that listing: no new adapter.
    let again = rpc_envelope(
        &mut ws,
        3,
        "agent.importableSessions.list",
        json!({ "provider": "mock", "query": "stall-4" }),
    )
    .await;
    assert_eq!(again["result"]["sessions"], json!([second]), "{again}");
    let paged = rpc_envelope(
        &mut ws,
        4,
        "agent.importableSessions.list",
        json!({ "provider": "mock", "limit": 1, "cursor": "o:1" }),
    )
    .await;
    assert_eq!(paged["result"]["sessions"], json!([second]), "{paged}");
    assert!(paged["result"].get("nextCursor").is_none(), "{paged}");
    let methods = adapter_methods(&rpc_log);
    assert_eq!(count(&methods, "initialize"), 2, "{methods:?}");
    assert_eq!(count(&methods, "session/list"), 3, "{methods:?}");
}

/// The text of every `session/prompt` the mock received, in order, once at
/// least `count` arrived.
async fn wait_for_prompts(rpc_log: &Path, count: usize) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(60));
    loop {
        let prompts: Vec<String> = std::fs::read_to_string(rpc_log)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter(|e| e["method"] == "session/prompt")
            .map(|e| {
                e["params"]["prompt"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|b| b["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .collect();
        if prompts.len() >= count {
            return prompts;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "expected {count} prompts, got {prompts:?}"
        );
        // timing-guard: polls a file the mock child appends to; no event marks it.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
