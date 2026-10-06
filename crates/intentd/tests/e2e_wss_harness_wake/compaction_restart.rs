//! Real provider activity, durable queue admission, process termination and
//! automatic recovery over WSS. Only the empty workspace is seeded in storage.
use super::*;
use intentd_test_support::GuardedChild;
use sqlx::Connection;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixListener;

type Ws = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;
const CONTEXT: &str = "warmup context: preserve cobalt-lighthouse-731";
const OWNED_PROMPT: &str = "continue investigation: native-compaction-owned-518";
const PARTIAL: &str = "pre-restart partial finding: silver-bridge-624";
const QUEUED: &str = "queued user instruction: deliver amber-otter-942 once";

struct RestartFixture {
    child: Option<GuardedChild>,
    root: tempfile::TempDir,
    script: String,
    generation: usize,
    load_session: bool,
}

impl RestartFixture {
    fn start(&mut self, gate: Option<&Path>) {
        self.generation += 1;
        let root = self.root.path();
        common::enable_ws_api(root);
        let workspaces = root.join("workspaces");
        std::fs::create_dir_all(&workspaces).unwrap();
        let log =
            std::fs::File::create(root.join(format!("daemon-{}.log", self.generation))).unwrap();
        let mut cmd = common::hermetic_serve_command(root);
        cmd.env("INTENTD_WORKSPACES_DIR", workspaces)
            .env("INTENTD_AUTH_TOKEN", TOKEN)
            .env("MOCK_AGENT_SCRIPT_PATH", &self.script)
            .env(
                "MOCK_AGENT_BEHAVIOR",
                json!({"response": "provider completed response", "advertiseLoadSession": self.load_session,
                    "parkIfPromptEndsWith": OWNED_PROMPT,
                    "parkedRawUpdates": [
                        {"sessionUpdate":"tool_call", "toolCallId":"native-compaction", "kind":"think", "title":"Compact conversation", "status":"in_progress"},
                        {"sessionUpdate":"agent_message_chunk", "content":{"type":"text", "text":PARTIAL}}
                    ]}).to_string(),
            )
            .env("MOCK_AGENT_WAKE_TRIGGER_FILE", root.join("wake.jsonl"))
            .env("MOCK_AGENT_WAKE_JSON", "1")
            .env("MOCK_AGENT_PROMPT_LOG", root.join("prompts.jsonl"))
            .env("MOCK_AGENT_SESSION_LOG", root.join("sessions.jsonl"))
            .env("MOCK_AGENT_CHECKPOINT_FILE", root.join("checkpoint.json"))
            .env("MOCK_AGENT_PROMPT_RESULT_GATE_FILE", root.join("finish-warmup"))
            .stdout(Stdio::null())
            .stderr(Stdio::from(log));
        if let Some(gate) = gate {
            cmd.env("INTENTD_TEST_STARTUP_RESUME_GATE", gate);
        }
        self.child = Some(GuardedChild::spawn(&mut cmd).unwrap());
    }

    async fn connect(&self) -> Ws {
        let socket = self.root.path().join("intentd.sock");
        assert!(await_uds(&socket).await, "isolated daemon must start");
        let status = common::await_wss_status(&socket).await;
        let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
        connect_ws(
            port,
            client_config(status["result"]["fingerprint"].as_str().unwrap()),
        )
        .await
    }

    async fn stop(&mut self, graceful: bool) {
        let child = self.child.as_mut().unwrap();
        if graceful {
            child.signal(nix::sys::signal::Signal::SIGTERM).unwrap();
        } else {
            child.kill().unwrap();
        }
        timeout(common::daemon_startup_timeout(), async {
            loop {
                if let Some(status) = child.try_wait().unwrap() {
                    if graceful {
                        assert!(status.success(), "graceful shutdown: {status}");
                    }
                    break;
                }
                // timing-guard: wait for this isolated child to exit, bounded above
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("isolated daemon exits");
        self.child.take();
        // A stale socket can make await_uds observe the previous listener.
        let _ = std::fs::remove_file(self.root.path().join("intentd.sock"));
    }

    async fn durable_content(&self, agent: &str) -> Vec<String> {
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(self.root.path().join("intentd.db"))
            .read_only(true);
        let mut connection = sqlx::SqliteConnection::connect_with(&options)
            .await
            .unwrap();
        let content =
            sqlx::query_scalar("SELECT content FROM agent_message WHERE agent_id = ? ORDER BY seq")
                .bind(agent)
                .fetch_all(&mut connection)
                .await
                .unwrap();
        connection.close().await.unwrap();
        content
    }

    fn prompts(&self) -> Vec<Value> {
        std::fs::read_to_string(self.root.path().join("prompts.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

impl Drop for RestartFixture {
    fn drop(&mut self) {
        self.child.take();
        for generation in 1..=self.generation {
            let path = self.root.path().join(format!("daemon-{generation}.log"));
            eprintln!(
                "=== {} ===\n{}",
                path.display(),
                std::fs::read_to_string(&path).unwrap_or_default()
            );
        }
    }
}

async fn subscribe(fixture: &RestartFixture, workspace: &str) -> Ws {
    let mut sub = fixture.connect().await;
    let response = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({
            "workspaceId": workspace, "eventTypes": ["agent:*", "chat:stream:delta"]
        }),
    )
    .await;
    assert!(response["subscriptionId"].is_string());
    sub
}

async fn next_agent_event(sub: &mut Ws, agent: &str) -> Value {
    timeout(common::rpc_read_timeout(), async {
        loop {
            let frame = wss_event(sub, 60).await;
            let event = &frame["params"]["event"];
            if event["data"]["agentId"] == agent {
                assert_ne!(event["type"], "agent:failed", "{event}");
                return event.clone();
            }
        }
    })
    .await
    .expect("agent event")
}

async fn finish_turns(sub: &mut Ws, agent: &str, expected: usize) {
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    timeout(common::rpc_read_timeout(), async {
        loop {
            let event = next_agent_event(sub, agent).await;
            match event["type"].as_str() {
                Some("chat:stream:delta") => {
                    let id = event["data"]["messageId"].clone();
                    if !starts.contains(&id) {
                        starts.push(id);
                    }
                }
                Some("agent:stream:end") => ends.push(event["data"]["messageId"].clone()),
                Some("agent:idle") if ends.len() == expected => break,
                _ => {}
            }
        }
    })
    .await
    .expect("automatic turns complete without manual send");
    assert_eq!(starts.len(), expected, "one streamed response per turn");
    assert_eq!(starts, ends, "every streamed response ends once, in order");
}

async fn active_compaction(
    load_session: bool,
    queued: bool,
    prompt_owned: bool,
) -> (RestartFixture, String, String, Ws, Ws, Option<String>) {
    let script = gate("native compaction restart").expect("node and pinned mock required");
    let root = common::test_tempdir_in("/tmp", "itd-compaction-restart-");
    let workspace = seed_workspace_only(root.path()).await;
    let mut fixture = RestartFixture {
        child: None,
        root,
        script,
        generation: 0,
        load_session,
    };
    fixture.start(None);
    let mut rpc = fixture.connect().await;
    wss_rpc(
        &mut rpc,
        2,
        "settings.update",
        json!({"changes": [
            {"path": "agents.resumeInterruptedOnStart", "value": "on"}
        ]}),
    )
    .await;
    let mut sub = subscribe(&fixture, &workspace).await;
    let created = wss_rpc(&mut rpc, 3, "agent.create", json!({
        "workspaceId": workspace, "name": "Compaction restart", "provider": "mock", "model": "default"
    })).await;
    let agent = created["agent"]["id"].as_str().unwrap().to_owned();
    let sent = wss_rpc(
        &mut rpc,
        4,
        "agent.sendMessage",
        json!({
            "workspaceId": workspace, "agentId": agent, "content": CONTEXT
        }),
    )
    .await;
    assert_eq!(sent["success"], true);
    // The first prompt stays open at the fixture barrier. Queue and edit via
    // WSS while that normal prompt owns the busy slot; no native-wake race.
    let mut queued_id = if queued && !prompt_owned {
        let response = wss_rpc(
            &mut rpc,
            5,
            "agent.queueMessage",
            json!({
                "agentId": agent, "content": QUEUED
            }),
        )
        .await;
        assert_eq!(response["success"], true);
        let id = response["queuedMessage"]["id"].as_str().unwrap().to_owned();
        let edit = wss_rpc(
            &mut rpc,
            6,
            "agent.editQueuedMessage",
            json!({
                "agentId": agent, "messageId": id, "content": QUEUED, "editing": true
            }),
        )
        .await;
        assert_eq!(edit["queuedMessage"]["editing"], true);
        Some(id)
    } else {
        None
    };
    std::fs::write(fixture.root.path().join("finish-warmup"), "release").unwrap();
    finish_turns(&mut sub, &agent, 1).await;
    assert_eq!(fixture.prompts().len(), 1);

    if prompt_owned {
        let sent = wss_rpc(
            &mut rpc,
            7,
            "agent.sendMessage",
            json!({
                "workspaceId": workspace, "agentId": agent, "content": OWNED_PROMPT
            }),
        )
        .await;
        assert_eq!(sent["success"], true);
    } else {
        write_wake_trigger(
            &fixture.root.path().join("wake.jsonl"),
            &json!({
                "sessionUpdate": "tool_call", "toolCallId": "native-compaction",
                "kind": "think", "title": "Compact conversation", "status": "in_progress"
            })
            .to_string(),
        );
    }
    let mut saw_tool = false;
    let mut saw_partial = false;
    loop {
        let event = next_agent_event(&mut sub, &agent).await;
        saw_tool |= event["type"] == "agent:tool:call";
        saw_partial |=
            event["type"] == "chat:stream:delta" && event["data"].to_string().contains(PARTIAL);
        if saw_tool && (!prompt_owned || saw_partial) {
            break;
        }
    }
    if prompt_owned && queued {
        let response = wss_rpc(
            &mut rpc,
            8,
            "agent.queueMessage",
            json!({
                "agentId": agent, "content": QUEUED
            }),
        )
        .await;
        assert_eq!(response["success"], true);
        assert_ne!(
            response["queuedMessage"]["editing"], true,
            "ordinary input, no edit hold"
        );
        queued_id = Some(response["queuedMessage"]["id"].as_str().unwrap().to_owned());
    }
    if !prompt_owned {
        std::fs::remove_file(fixture.root.path().join("wake.jsonl")).unwrap();
    }
    (fixture, workspace, agent, rpc, sub, queued_id)
}

enum CompactionOwner {
    Prompt,
    Harness,
}

async fn run_restart_case(
    graceful: bool,
    queued: bool,
    load_session: bool,
    owner: CompactionOwner,
) {
    let prompt_owned = matches!(owner, CompactionOwner::Prompt);
    let (mut fixture, workspace, agent, _initial_rpc, mut sub, queued_id) =
        active_compaction(load_session, queued, prompt_owned).await;
    let resume_index = 1 + usize::from(prompt_owned);
    let expected_prompts = resume_index + 1 + usize::from(queued);
    // Observe across three production quiet windows: unfinished native tools
    // must not settle or drain ordinary input behind a prompt owner,
    // or input held under edit behind an unsolicited wake. This is a negative event
    // assertion with a deadline, not a sleep used to order the test.
    let premature = timeout(Duration::from_secs(6), async {
        loop {
            let event = next_agent_event(&mut sub, &agent).await;
            assert_ne!(
                event["type"], "agent:idle",
                "unfinished compaction became idle"
            );
            if event["type"] == "agent:stream:end" {
                return event;
            }
        }
    })
    .await;
    assert!(
        premature.is_err(),
        "unfinished compaction settled: {premature:?}"
    );
    assert_eq!(
        fixture.prompts().len(),
        resume_index,
        "queue must not preempt compaction"
    );
    // Read-only evidence distinguishes durable context from a live WSS preview.
    // Never seed recovery rows or assume that SIGKILL ran the graceful flush.
    let before_stop = fixture.durable_content(&agent).await;
    let partial_was_durable = before_stop.iter().any(|row| row.contains(PARTIAL));
    assert!(before_stop.iter().any(|row| row.contains(CONTEXT)));
    if prompt_owned {
        assert!(before_stop.iter().any(|row| row.contains(OWNED_PROMPT)));
    }
    fixture.stop(graceful).await;
    let after_stop = fixture.durable_content(&agent).await;
    for row in &before_stop {
        assert!(after_stop.contains(row), "termination lost durable context");
    }
    if prompt_owned && graceful {
        assert!(after_stop.iter().any(|row| row.contains(PARTIAL)));
    }
    eprintln!("restart evidence: graceful={graceful}, prompt_owned={prompt_owned}, partial durable before={partial_was_durable}, after={}", after_stop.iter().any(|row| row.contains(PARTIAL)));

    for generation in 2..=3 {
        let gate_path = fixture
            .root
            .path()
            .join(format!("resume-{generation}.sock"));
        let gate = UnixListener::bind(&gate_path).unwrap();
        fixture.start(Some(&gate_path));
        let (mut release, _) = timeout(common::daemon_startup_timeout(), gate.accept())
            .await
            .expect("startup sweep reaches gate")
            .unwrap();
        let mut rpc = fixture.connect().await;
        sub = subscribe(&fixture, &workspace).await;
        let queue = wss_rpc(&mut rpc, 10, "agent.getQueue", json!({"agentId": agent})).await;
        let entries = queue["queue"].as_array().unwrap();
        if generation == 2 && queued {
            assert_eq!(entries.len(), 1, "queue survives real restart: {queue}");
            assert_eq!(entries[0]["id"].as_str(), queued_id.as_deref());
            assert_eq!(entries[0]["content"], QUEUED);
        } else {
            assert!(entries.is_empty(), "{queue}");
        }
        release.write_u8(1).await.unwrap();
        if generation == 2 {
            finish_turns(&mut sub, &agent, if queued { 2 } else { 1 }).await;
            let prompts = fixture.prompts();
            assert_eq!(prompts.len(), expected_prompts, "{prompts:?}");
            let resumed = prompts[resume_index]["text"].as_str().unwrap();
            assert!(
                resumed.contains("You were interrupted for about"),
                "automatic continuation reaches provider: {resumed}"
            );
            assert_eq!(prompts[resume_index]["sessionFromLoad"], load_session);
            let context = if load_session {
                prompts[resume_index]["checkpointContext"].to_string()
            } else {
                resumed.to_owned()
            };
            let sessions: Vec<Value> =
                std::fs::read_to_string(fixture.root.path().join("sessions.jsonl"))
                    .unwrap()
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
            assert_eq!(sessions.len(), 2);
            assert_eq!(
                sessions[1]["method"],
                if load_session {
                    "session/load"
                } else {
                    "session/new"
                }
            );
            assert_ne!(
                sessions[0]["pid"], sessions[1]["pid"],
                "new provider process after restart"
            );
            if load_session {
                assert_eq!(sessions[0]["sessionId"], sessions[1]["sessionId"]);
            }
            assert!(
                context.contains(CONTEXT),
                "model sees restored user context: {resumed}"
            );
            assert!(
                context.contains("provider completed response"),
                "model sees prior assistant response: {resumed}"
            );
            if prompt_owned {
                assert!(
                    resumed.contains(OWNED_PROMPT),
                    "interrupted user request reaches model: {resumed}"
                );
                // Graceful shutdown flushes LiveTurn into the durable
                // transcript. SIGKILL cannot flush the in-memory partial;
                // crash recovery must retain all available durable context.
                if graceful || partial_was_durable {
                    assert!(
                        resumed.contains(PARTIAL),
                        "checkpointed assistant tail reaches model: {resumed}"
                    );
                }
            }
            if queued {
                assert!(
                    !resumed.contains(QUEUED),
                    "queued input remains a separate turn"
                );
                assert_eq!(
                    prompts[resume_index + 1]["text"]
                        .as_str()
                        .unwrap()
                        .matches(QUEUED)
                        .count(),
                    1
                );
            }
        }
        // Wait for the actual sweep completion, including the empty second
        // restart, before asserting that no further prompt was admitted.
        timeout(common::rpc_read_timeout(), async {
            loop {
                let log = std::fs::read_to_string(
                    fixture.root.path().join(format!("daemon-{generation}.log")),
                )
                .unwrap();
                if log.contains("resume-on-start: auto-resume sweep complete") {
                    break;
                }
                // timing-guard: poll the observable startup-sweep completion
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("startup sweep completes");
        let queue = wss_rpc(&mut rpc, 11, "agent.getQueue", json!({"agentId": agent})).await;
        assert_eq!(queue["queue"], json!([]));
        assert_eq!(
            fixture.prompts().len(),
            expected_prompts,
            "second restart must not redeliver"
        );
        let conversation = wss_rpc(
            &mut rpc,
            12,
            "agent.getConversation",
            json!({
                "workspaceId": workspace, "agentId": agent
            }),
        )
        .await;
        let rows = conversation["messages"]
            .as_array()
            .expect("conversation messages");
        assert_eq!(rows.iter().filter(|row| row["role"] == "user").count(), expected_prompts, "one original user row, one automatic continuation, and optional queued input: {rows:?}");
        assert_eq!(
            rows.iter()
                .filter(|row| row["role"] == "user" && blocks_text(row).contains(QUEUED))
                .count(),
            usize::from(queued)
        );
        fixture.stop(true).await;
    }
}

#[tokio::test]
async fn native_compaction_graceful_restart_without_queue() {
    run_restart_case(true, false, false, CompactionOwner::Harness).await;
}
#[tokio::test]
async fn unsolicited_compaction_graceful_restart_with_edit_hold() {
    run_restart_case(true, true, false, CompactionOwner::Harness).await;
}
#[tokio::test]
async fn native_compaction_crash_restart_without_queue() {
    run_restart_case(false, false, false, CompactionOwner::Harness).await;
}
#[tokio::test]
async fn unsolicited_compaction_crash_restart_with_edit_hold() {
    run_restart_case(false, true, false, CompactionOwner::Harness).await;
}

#[tokio::test]
async fn native_compaction_graceful_load_without_queue() {
    run_restart_case(true, false, true, CompactionOwner::Harness).await;
}
#[tokio::test]
async fn unsolicited_compaction_graceful_load_with_edit_hold() {
    run_restart_case(true, true, true, CompactionOwner::Harness).await;
}
#[tokio::test]
async fn native_compaction_crash_load_without_queue() {
    run_restart_case(false, false, true, CompactionOwner::Harness).await;
}
#[tokio::test]
async fn unsolicited_compaction_crash_load_with_edit_hold() {
    run_restart_case(false, true, true, CompactionOwner::Harness).await;
}

/// Ready-to-send input retains the existing preemption contract. This is
/// deliberately separate from input held under edit across the restart cases.
#[tokio::test]
async fn native_compaction_ready_send_preempts_without_restart() {
    let (mut fixture, workspace, agent, mut rpc, mut sub, _) =
        active_compaction(false, false, false).await;
    let sent = wss_rpc(
        &mut rpc,
        5,
        "agent.sendMessage",
        json!({
            "workspaceId": workspace, "agentId": agent, "content": QUEUED
        }),
    )
    .await;
    assert_eq!(sent["success"], true);
    assert_eq!(sent["queued"], true);
    let mut wake_ended = false;
    let mut prompt_started = false;
    let mut prompt_ended = false;
    timeout(common::rpc_read_timeout(), async {
        loop {
            let event = next_agent_event(&mut sub, &agent).await;
            match event["type"].as_str() {
                Some("agent:stream:end") if !wake_ended => wake_ended = true,
                Some("chat:stream:delta") => {
                    assert!(wake_ended, "wake releases ownership before prompt starts");
                    prompt_started = true;
                }
                Some("agent:stream:end") => {
                    assert!(prompt_started);
                    assert!(!prompt_ended);
                    prompt_ended = true;
                }
                Some("agent:idle") => {
                    assert!(prompt_ended, "no successful wake idle before queued send");
                    break;
                }
                _ => {}
            }
        }
    })
    .await
    .expect("ready input preempts unfinished native tool");
    let prompts = fixture.prompts();
    assert_eq!(prompts.len(), 2);
    assert_eq!(
        prompts[1]["text"].as_str().unwrap().matches(QUEUED).count(),
        1
    );
    fixture.stop(true).await;
}

#[tokio::test]
async fn prompt_owned_compaction_graceful_restart_with_ordinary_queue() {
    run_restart_case(true, true, false, CompactionOwner::Prompt).await;
}
#[tokio::test]
async fn prompt_owned_compaction_crash_restart_with_ordinary_queue() {
    run_restart_case(false, true, false, CompactionOwner::Prompt).await;
}
#[tokio::test]
async fn prompt_owned_compaction_graceful_load_with_ordinary_queue() {
    run_restart_case(true, true, true, CompactionOwner::Prompt).await;
}
#[tokio::test]
async fn prompt_owned_compaction_crash_load_with_ordinary_queue() {
    run_restart_case(false, true, true, CompactionOwner::Prompt).await;
}
