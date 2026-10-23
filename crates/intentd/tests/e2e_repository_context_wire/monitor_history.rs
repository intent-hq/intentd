//! Cold native history from an actual admitted ACP producer database.
//! Explicit fixture run only: no imported/public metadata may mint evidence.
use super::*;
use std::sync::Mutex;
use tokio::io::AsyncReadExt;
#[path = "monitor_handoff.rs"]
mod handoff;
#[path = "monitor_lifecycle.rs"]
mod lifecycle;
#[path = "monitor_live_hold.rs"]
mod live_hold;

struct OriginalAccount {
    endpoint: String,
    paths: Arc<Mutex<Vec<String>>>,
    review_state: Arc<Mutex<&'static str>>,
    draft: Arc<Mutex<bool>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for OriginalAccount {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl OriginalAccount {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let paths = Arc::new(Mutex::new(Vec::new()));
        let captured = paths.clone();
        let review_state = Arc::new(Mutex::new("opened"));
        let captured_state = review_state.clone();
        let draft = Arc::new(Mutex::new(false));
        let captured_draft = draft.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let paths = captured.clone();
                let review_state = captured_state.clone();
                let draft = captured_draft.clone();
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    let mut block = [0; 4096];
                    loop {
                        let n = socket.read(&mut block).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&block[..n]);
                        if bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                        assert!(bytes.len() < 16384);
                    }
                    let request = String::from_utf8(bytes).unwrap();
                    let path = request.split_whitespace().nth(1).unwrap().to_string();
                    paths.lock().unwrap().push(path.clone());
                    let (status, body) = if path == "/api/v4/user" {
                        let id = if request.contains("pat-second") {
                            43
                        } else {
                            42
                        };
                        (200, json!({"id":id,"username":"fixture","name":"Fixture"}))
                    } else if path == "/api/v4/projects/group%2Fproject" {
                        (
                            200,
                            json!({"id":42,"path_with_namespace":"group/project","only_allow_merge_if_pipeline_succeeds":false,"only_allow_merge_if_all_discussions_are_resolved":false}),
                        )
                    } else if path == "/api/v4/projects/group%2Fproject/merge_requests/4" {
                        (
                            200,
                            json!({"iid":4,"web_url":"https://gitlab.test/forge/group/project/-/merge_requests/4","title":"actual daemon review","state":*review_state.lock().unwrap(),"draft":*draft.lock().unwrap(),"source_branch":"feature","target_branch":"main","source_project_id":42,"target_project_id":42,"created_at":"2026-09-27T00:00:00Z","updated_at":"2026-09-27T00:00:00Z"}),
                        )
                    } else if path.ends_with("/approvals") {
                        (
                            200,
                            json!({"approvals_required":0,"approvals_left":0,"approved_by":[]}),
                        )
                    } else if path.contains("/discussions") {
                        (200, json!([]))
                    } else {
                        (404, json!({}))
                    };
                    let body = body.to_string();
                    let response=format!("HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                    socket.write_all(response.as_bytes()).await.unwrap();
                });
            }
        });
        Self {
            endpoint,
            paths,
            review_state,
            draft,
            task,
        }
    }
}
async fn connect(h: &Harness, wss: bool) -> Client {
    if wss {
        h.wss(TOKEN).await
    } else {
        h.uds().await
    }
}
async fn authorize(client: &mut Client, token: &str) {
    let response=client.rpc("sourceControl.connect",json!({"provider":"gitlab","method":"pat","token":token,"instanceBaseUrl":"https://gitlab.test/forge"})).await;
    success(&response);
}
async fn push(client: &mut Client, id: &Value) -> Value {
    loop {
        if let Some(pos) = client.notices.iter().position(|v| {
            v["method"] == "subscription.push" && &v["params"]["subscriptionId"] == id
        }) {
            return client.notices.remove(pos);
        }
        let frame = client.next().await;
        client.notices.push(frame);
    }
}
async fn complete_history(client: &mut Client, agent: &intent_core::AgentId, mode: &str) -> Value {
    let subscribed = client
        .rpc(
            "chat.subscribe",
            json!({"agentId":agent,"historyDelivery":mode,"replaceGroup":"cold-history"}),
        )
        .await;
    let id = success(&subscribed)["subscriptionId"].clone();
    let first = push(client, &id).await;
    let mut complete =
        mode == "snapshot" || first["params"]["snapshot"]["initialHistory"]["complete"] == true;
    let mut frames = vec![first];
    while !complete {
        let frame = push(client, &id).await;
        assert_eq!(frame["params"]["seq"], frames.len());
        assert_eq!(frame["params"]["kind"], "history");
        complete = frame["params"]["history"]["complete"] == true;
        frames.push(frame);
    }
    let unsubscribed = client
        .rpc("chat.unsubscribe", json!({"subscriptionId":id}))
        .await;
    assert_eq!(success(&unsubscribed)["success"], true);
    json!({"mode":mode,"subscribe":subscribed,"pushes":frames,"unsubscribe":unsubscribed})
}
async fn denied_surfaces(
    client: &mut Client,
    query: &Value,
    agent: &intent_core::AgentId,
    message_id: &str,
    text: &str,
) -> Value {
    let conversation = client.rpc("agent.getConversation", query.clone()).await;
    let index = client.rpc("agent.listUserMessages", query.clone()).await;
    for response in [&conversation, &index] {
        assert!(!success(response).to_string().contains(message_id));
        assert!(success(response)
            .to_string()
            .contains("ordinary-cold-control"));
    }
    let mut block_query = query.clone();
    block_query["messageId"] = json!(message_id);
    block_query["blockId"] = json!(format!("{message_id}:0"));
    let block = client.rpc("agent.getMessageBlock", block_query).await;
    assert!(
        !block.to_string().contains(text),
        "denied block exposed original text: {block}"
    );
    assert!(block.get("error").is_some() || block["result"]["block"].is_null());
    let mut chat = Vec::new();
    for mode in ["snapshot", "progressive"] {
        let history = complete_history(client, agent, mode).await;
        assert!(!history.to_string().contains(message_id));
        assert!(history.to_string().contains("ordinary-cold-control"));
        chat.push(history);
    }
    json!({"conversation":conversation,"index":index,"block":block,"chat":chat})
}
async fn check_cold_history(wss: bool) {
    let input = PathBuf::from(
        std::env::var_os("INTENT_MONITOR_HISTORY_DATABASE_FIXTURE")
            .expect("actual ACP database fixture required"),
    );
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(input.with_extension("json")).unwrap()).unwrap();
    let account = OriginalAccount::new().await;
    let h = Harness::boot_with_inputs(None, true, Some((&input, &account.endpoint))).await;
    let store = Store::open(&h.dir.path().join("intentd.db")).await.unwrap();
    let agent = intent_core::AgentId::from(manifest["agentId"].as_str().unwrap());
    let message_id = manifest["messageId"].as_str().unwrap();
    let original = store
        .get_monitor_message_provenance(&agent, message_id)
        .await
        .unwrap()
        .expect("original prepared write survives cold boot");
    store
        .append_agent_message_with_id(
            &agent,
            "ordinary-cold-control",
            "user",
            &json!([{"type":"text","text":"Ordinary cold receiver control"}]),
            None,
            &intent_core::now_iso(),
        )
        .await
        .unwrap();
    let rows = store
        .list_qualified_pr_monitors_by_agent(&agent)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.monitor().clone(), r.target().clone()))
        .collect::<Vec<_>>();
    let query = json!({"agentId":agent,"workspaceId":manifest["workspaceId"]});
    let mut ordinary = connect(&h, wss).await;
    let hello = ordinary
        .rpc("client.hello", json!({"clientId":"cold-history-desktop"}))
        .await;
    assert_eq!(
        success(&hello)["server"]["capabilities"]["reviewMonitoring"],
        1
    );
    let mut doc = connect(&h, wss).await;
    let attachment=doc.rpc("client.hello",json!({"clientId":success(&hello)["clientId"],"connectionRole":"review-monitor","capabilities":{"reviewMonitoring":1}})).await;
    assert_eq!(success(&attachment)["connectionRole"], "review-monitor");
    let held = denied_surfaces(
        &mut doc,
        &query,
        &agent,
        message_id,
        original.message.content[0]["text"].as_str().unwrap(),
    )
    .await;
    assert!(
        account.paths.lock().unwrap().is_empty(),
        "missing account must not perform a provider read"
    );
    authorize(&mut ordinary, "stored-pat").await;
    let reads = account.paths.lock().unwrap().len();
    let conversation = doc.rpc("agent.getConversation", query.clone()).await;
    assert!(
        success(&conversation)["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["id"] == message_id),
        "cold original account history missing: {conversation}"
    );
    let index = doc.rpc("agent.listUserMessages", query.clone()).await;
    assert!(success(&index)["items"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["id"] == message_id));
    let mut block_query = query.clone();
    block_query["messageId"] = json!(message_id);
    block_query["blockId"] = json!(format!("{message_id}:0"));
    let block = doc.rpc("agent.getMessageBlock", block_query).await;
    assert_eq!(
        success(&block)["block"]["text"],
        original.message.content[0]["text"]
    );
    let mut snapshots = Vec::new();
    for mode in ["snapshot", "progressive"] {
        let history = complete_history(&mut doc, &agent, mode).await;
        assert!(
            history.to_string().contains(message_id),
            "native chat lost original history: {history}"
        );
        assert!(history.to_string().contains("ordinary-cold-control"));
        if mode == "progressive" {
            assert_eq!(
                history["pushes"][0]["params"]["snapshot"]["messages"][0]["id"],
                "ordinary-cold-control"
            );
            assert!(
                history["pushes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|frame| frame["params"]["history"]["message"]["id"] == message_id),
                "monitor must be read in progressive tail"
            );
        }
        snapshots.push(history);
    }
    let legacy = denied_surfaces(
        &mut ordinary,
        &query,
        &agent,
        message_id,
        original.message.content[0]["text"].as_str().unwrap(),
    )
    .await;
    assert_eq!(
        account.paths.lock().unwrap().len(),
        reads,
        "history must not re-resolve through a provider request"
    );
    authorize(&mut ordinary, "pat-second").await;
    let other = denied_surfaces(
        &mut doc,
        &query,
        &agent,
        message_id,
        original.message.content[0]["text"].as_str().unwrap(),
    )
    .await;
    authorize(&mut ordinary, "stored-pat").await;
    let fresh = doc.rpc("agent.getConversation", query).await;
    assert!(
        success(&fresh).to_string().contains(message_id),
        "fresh original-account admission can recover history"
    );
    assert_eq!(
        store
            .get_agent_message_by_id(&agent, message_id)
            .await
            .unwrap()
            .unwrap(),
        original.message
    );
    assert_eq!(
        store
            .list_qualified_pr_monitors_by_agent(&agent)
            .await
            .unwrap()
            .into_iter()
            .map(|r| (r.monitor().clone(), r.target().clone()))
            .collect::<Vec<_>>(),
        rows,
        "history cannot consume original monitor obligations"
    );
    let fixture = json!({"scope":"cold daemon from actual ACP database; fresh original-account admission; no delivery acknowledgement","wss":wss,"ordinaryHello":hello,"attachmentHello":attachment,"held":held,"conversation":conversation,"index":index,"block":block,"chat":snapshots,"legacy":legacy,"otherAccount":other,"freshOriginalAccount":fresh});
    for forbidden in [
        "monitorBinding",
        "authorityScopeId",
        "connectionGeneration",
        "stored-pat",
        "pat-second",
    ] {
        assert!(!fixture.to_string().contains(forbidden));
    }
    if let Some(dir) = std::env::var_os("INTENT_MONITOR_FIXTURE_DIR") {
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            Path::new(&dir).join(if wss {
                "producer-cold-history-wss.json"
            } else {
                "producer-cold-history-uds.json"
            }),
            serde_json::to_vec_pretty(&fixture).unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires the actual admitted ACP database fixture captured by the Services producer test"]
async fn monitor_history_real_uds_cold_original_account_and_chat() {
    check_cold_history(false).await;
}
#[tokio::test]
#[ignore = "requires the actual admitted ACP database fixture captured by the Services producer test"]
async fn monitor_history_real_wss_cold_original_account_and_chat() {
    check_cold_history(true).await;
}

async fn wait_marker(path: &Path) {
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(30)),
        async {
            while !path.exists() {
                // timing-guard: wait for the selected daemon writer barrier
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
    )
    .await
    .unwrap();
}

async fn held_socket_history(wss: bool, replace_source: bool, unchanged: bool) {
    let input = PathBuf::from(
        std::env::var_os("INTENT_MONITOR_HISTORY_DATABASE_FIXTURE")
            .expect("actual ACP database fixture required"),
    );
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(input.with_extension("json")).unwrap()).unwrap();
    let account = OriginalAccount::new().await;
    let h =
        Harness::boot_with_writer_hold(None, true, Some((&input, &account.endpoint)), true).await;
    let store = Store::open(&h.dir.path().join("intentd.db")).await.unwrap();
    let agent = intent_core::AgentId::from(manifest["agentId"].as_str().unwrap());
    let message_id = manifest["messageId"].as_str().unwrap();
    let original = store
        .get_agent_message_by_id(&agent, message_id)
        .await
        .unwrap()
        .unwrap();
    let mut control = connect(&h, wss).await;
    let hello = control
        .rpc("client.hello", json!({"clientId":"held-history-control"}))
        .await;
    let client_id = success(&hello)["clientId"].clone();
    authorize(&mut control, "stored-pat").await;
    let mut doc = connect(&h, wss).await;
    success(&doc.rpc("client.hello",json!({"clientId":client_id,"connectionRole":"review-monitor","capabilities":{"reviewMonitoring":1}})).await);
    success(
        &doc.rpc(
            "events.subscribe",
            json!({"eventTypes":["settings:changed"],"replaceGroup":"later-public"}),
        )
        .await,
    );
    let barrier = h.dir.path().join("private-write-hold");
    std::fs::write(barrier.join("arm"), message_id).unwrap();
    let subscribed = doc
        .rpc(
            "chat.subscribe",
            json!({"agentId":agent,"historyDelivery":"snapshot","replaceGroup":"held-original"}),
        )
        .await;
    let subscription_id = success(&subscribed)["subscriptionId"].clone();
    wait_marker(&barrier.join("entered")).await;
    assert_eq!(
        std::fs::read_to_string(barrier.join("entered")).unwrap(),
        if wss {
            "wss-before-flush"
        } else {
            "uds-before-write"
        }
    );
    assert!(
        !doc.notices
            .iter()
            .any(|frame| frame.to_string().contains(message_id)),
        "held private frame transferred before mutation"
    );
    if unchanged {
        // Positive control: the same held output remains admitted.
    } else if replace_source {
        let response=control.rpc("agent.replaceMessages",json!({"agentId":agent,"workspaceId":manifest["workspaceId"],"messages":[{"role":"user","contentBlocks":[{"type":"text","text":"replacement ordinary history"}],"timestamp":original.created_at}]})).await;
        success(&response);
    } else {
        authorize(&mut control, "pat-second").await;
        authorize(&mut control, "stored-pat").await;
        assert_eq!(
            store
                .get_agent_message_by_id(&agent, message_id)
                .await
                .unwrap()
                .unwrap(),
            original
        );
    }
    // A real public event is generated while the original private writer is
    // held. The refusal path must discard its queue rather than flush it.
    success(
        &control
            .rpc(
                "settings.update",
                json!({"changes":[{"path":"git.autoCommit","value":true}]}),
            )
            .await,
    );
    std::fs::write(barrier.join("release"), b"release original").unwrap();
    if unchanged {
        let private = push(&mut doc, &subscription_id).await;
        assert!(private.to_string().contains(message_id));
        success(
            &doc.rpc("settings.get", json!({"path":"git.autoCommit"}))
                .await,
        );
        while !doc
            .notices
            .iter()
            .any(|frame| frame.to_string().contains("settings:changed"))
        {
            let frame = doc.next().await;
            doc.notices.push(frame);
        }
        assert!(!barrier.join("discarded-public.json").exists());
        assert_eq!(
            store
                .get_agent_message_by_id(&agent, message_id)
                .await
                .unwrap()
                .unwrap(),
            original
        );
        if let Some(dir) = std::env::var_os("INTENT_MONITOR_FIXTURE_DIR") {
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(Path::new(&dir).join(format!("held-history-{}-unchanged.json",if wss {"wss"} else {"uds"})),serde_json::to_vec_pretty(&json!({"wss":wss,"private":private,"laterPublic":doc.notices,"connectionStillUsable":true})).unwrap()).unwrap();
        }
        return;
    }
    let mut received = Vec::new();
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(15)),
        async {
            match &mut doc.socket {
                Socket::Uds(stream) => {
                    stream.read_to_end(&mut received).await.unwrap();
                }
                Socket::Ws(socket) => {
                    while let Some(frame) = socket.next().await {
                        match frame {
                            Ok(Message::Text(text)) => received.extend_from_slice(text.as_bytes()),
                            Ok(Message::Close(_)) | Err(_) => break,
                            _ => {}
                        }
                    }
                }
            }
        },
    )
    .await
    .unwrap();
    assert!(
        received.is_empty(),
        "retired pending frame or later public data escaped: {}",
        String::from_utf8_lossy(&received)
    );
    wait_marker(&barrier.join("discarded-public.json")).await;
    let discarded: Vec<String> =
        serde_json::from_slice(&std::fs::read(barrier.join("discarded-public.json")).unwrap())
            .unwrap();
    assert!(
        discarded
            .iter()
            .any(|frame| frame.contains("settings:changed")),
        "later public event was not observed in the actual discarded queue: {discarded:?}"
    );
    let mut fresh = connect(&h, wss).await;
    success(&fresh.rpc("client.hello",json!({"clientId":client_id,"connectionRole":"review-monitor","capabilities":{"reviewMonitoring":1}})).await);
    let fresh_history = complete_history(&mut fresh, &agent, "snapshot").await;
    assert_ne!(
        fresh_history["subscribe"]["result"]["subscriptionId"],
        subscription_id
    );
    if replace_source {
        assert!(fresh_history
            .to_string()
            .contains("replacement ordinary history"));
        assert!(!fresh_history.to_string().contains(message_id));
    } else {
        assert!(
            fresh_history.to_string().contains(message_id),
            "fresh original-account history unavailable"
        );
    }
    if let Some(dir) = std::env::var_os("INTENT_MONITOR_FIXTURE_DIR") {
        std::fs::create_dir_all(&dir).unwrap();
        let receipt = json!({"scope":"real daemon socket writer held before UDS write or WSS flush; no recall claim for previously transferred bytes","wss":wss,"replaceSource":replace_source,"subscribed":subscribed,"postRetirementBytes":received.len(),"discardedPublic":discarded,"fresh":fresh_history});
        std::fs::write(
            Path::new(&dir).join(format!(
                "held-history-{}-{}.json",
                if wss { "wss" } else { "uds" },
                if replace_source { "source" } else { "account" }
            )),
            serde_json::to_vec_pretty(&receipt).unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test]
#[ignore = "requires the actual admitted ACP database fixture"]
async fn monitor_history_real_uds_held_account_aba() {
    held_socket_history(false, false, false).await;
}
#[tokio::test]
#[ignore = "requires the actual admitted ACP database fixture"]
async fn monitor_history_real_wss_held_account_aba() {
    held_socket_history(true, false, false).await;
}
#[tokio::test]
#[ignore = "requires the actual admitted ACP database fixture"]
async fn monitor_history_real_uds_held_source_replacement() {
    held_socket_history(false, true, false).await;
}
#[tokio::test]
#[ignore = "requires the actual admitted ACP database fixture"]
async fn monitor_history_real_wss_held_source_replacement() {
    held_socket_history(true, true, false).await;
}

#[tokio::test]
#[ignore = "requires the actual admitted ACP database fixture"]
async fn monitor_history_real_uds_held_unchanged() {
    held_socket_history(false, false, true).await;
}
#[tokio::test]
#[ignore = "requires the actual admitted ACP database fixture"]
async fn monitor_history_real_wss_held_unchanged() {
    held_socket_history(true, false, true).await;
}

pub(super) fn configure_native_daemon(root: &Path, command: &mut std::process::Command) {
    use std::os::unix::fs::PermissionsExt as _;
    let runtime = PathBuf::from(
        std::env::var_os("INTENT_REVIEW_INSTALLED_ROOT")
            .expect("explicit verified installed bundle"),
    );
    let native = root.join("native-runtime");
    std::fs::create_dir(&native).unwrap();
    let peer = native.join("adapter.mjs");
    let diagnostic = r#"import cp from 'node:child_process';
import {syncBuiltinESMExports} from 'node:module';
import {appendFileSync,writeFileSync as writeDiagnostic} from 'node:fs';
const originalSpawn=cp.spawn;
const nativeProcesses=[];
cp.spawn=function(...args){
 const child=originalSpawn(...args);
 child.stderr?.on('data',data=>appendFileSync(process.argv[3]+'/native-stderr.log',data));
 if(String(args[0])===process.argv[4]){
  const entry={pid:child.pid,closed:false};nativeProcesses.push(entry);
  const save=()=>writeDiagnostic(process.argv[3]+'/native-processes.json',JSON.stringify(nativeProcesses));save();
  child.once('close',(code,signal)=>{Object.assign(entry,{closed:true,code,signal});save();});
 }
 return child;
};
syncBuiltinESMExports();
"#;
    std::fs::write(
        &peer,
        format!("{diagnostic}{}", include_str!("native_daemon_peer.mjs")),
    )
    .unwrap();
    let launcher = native.join("launcher");
    let quote = |path: &Path| format!("'{}'", path.to_str().unwrap().replace('\'', "'\\''"));
    std::fs::write(
        &launcher,
        format!(
            "#!/bin/sh\nset -eu\nexec {} {} {} {} {}\n",
            quote(&runtime.join("node/bin/node")),
            quote(&peer),
            quote(&runtime.join("runtime")),
            quote(&native),
            quote(&runtime.join("bin/review-containment"))
        ),
    )
    .unwrap();
    std::fs::set_permissions(&launcher, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = root.join("config.toml");
    let text = std::fs::read_to_string(&config)
        .unwrap()
        .replace("enabled = {}", "enabled = { claude-code = true }");
    std::fs::write(
        config,
        format!("{text}\n[prMonitor]\npollSeconds = 10\ndebounceSeconds = 10\n"),
    )
    .unwrap();
    // Exercise the normal installed-CLI name discovery in this disposable PATH.
    std::os::unix::fs::symlink(
        runtime.join("runtime/node_modules/@anthropic-ai/claude-agent-sdk-linux-x64/claude"),
        root.join("bin/claude"),
    )
    .unwrap();
    command.env("INTENTD_TEST_REVIEW_LAUNCHER", launcher);
}

#[tokio::test]
#[ignore = "requires explicit verified installed native bundle and admitted database fixture"]
async fn monitor_native_daemon_original_startup_and_callback() {
    native_daemon_case(false, false, false, false, false, false, None).await;
}

#[tokio::test]
#[ignore = "requires explicit verified installed native bundle and admitted database fixture"]
async fn monitor_native_daemon_model_registers_original_monitor() {
    native_daemon_case(true, false, false, false, false, false, None).await;
}

#[tokio::test]
#[ignore = "requires explicit verified installed native bundle and admitted database fixture"]
async fn monitor_native_daemon_scheduler_delivers_original_monitor() {
    native_daemon_case(true, true, false, false, false, false, None).await;
}

#[tokio::test]
#[ignore = "requires explicit verified installed native bundle and admitted database fixture"]
async fn monitor_native_daemon_account_replacement_joins_original_allocation() {
    native_daemon_case(true, true, true, false, false, false, None).await;
}

#[tokio::test]
#[ignore = "requires explicit verified installed native bundle and admitted database fixture"]
async fn monitor_native_daemon_restart_replays_original_history() {
    native_daemon_case(true, true, false, true, false, false, None).await;
}

#[tokio::test]
#[ignore = "requires explicit verified installed native bundle and admitted database fixture"]
async fn monitor_native_daemon_restart_refuses_replacement_and_recovers_original() {
    native_daemon_case(true, true, false, true, true, false, None).await;
}

#[tokio::test]
#[ignore = "requires explicit verified installed native bundle and admitted database fixture"]
async fn monitor_native_daemon_abrupt_restart_replays_original_history() {
    native_daemon_case(true, true, false, true, false, true, None).await;
}

#[tokio::test]
#[ignore = "requires explicit verified installed native bundle and admitted database fixture"]
async fn monitor_native_daemon_abrupt_restart_refuses_replacement_and_recovers_original() {
    native_daemon_case(true, true, false, true, true, true, None).await;
}

async fn restart_native_daemon(h: &mut Harness, endpoint: &str) {
    restart_native_daemon_mode(h, endpoint, false).await;
}

async fn restart_native_daemon_mode(h: &mut Harness, endpoint: &str, abrupt: bool) {
    let original_live = if abrupt {
        let processes: Value = serde_json::from_slice(
            &std::fs::read(h.dir.path().join("native-runtime/native-processes.json")).unwrap(),
        )
        .unwrap();
        let originals = processes.as_array().unwrap();
        assert!(
            !originals.is_empty(),
            "abrupt recovery requires a live original allocation"
        );
        for original in originals {
            assert_eq!(
                original["closed"], false,
                "original already closed before daemon kill"
            );
            assert!(
                Path::new(&format!("/proc/{}", original["pid"].as_u64().unwrap())).exists(),
                "original process absent before daemon kill"
            );
        }
        processes
    } else {
        Value::Null
    };
    if abrupt {
        h.child.kill().unwrap();
    } else {
        let mut control = h.uds().await;
        success(&control.rpc("system.shutdown", json!({})).await);
    }
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(30)),
        async {
            loop {
                if let Some(status) = h.child.try_wait().unwrap() {
                    if abrupt {
                        use std::os::unix::process::ExitStatusExt;
                        assert_eq!(
                            status.signal(),
                            Some(9),
                            "original daemon was not killed: {status}"
                        );
                    } else {
                        assert!(
                            status.success(),
                            "original daemon shutdown failed: {status}"
                        );
                    }
                    break;
                }
                // timing-guard: observe real daemon shutdown and native join
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
    )
    .await
    .expect("original daemon did not finish shutdown");
    if abrupt {
        timeout(
            intentd_test_support::test_timeout(Duration::from_secs(30)),
            async {
                loop {
                    let bytes =
                        std::fs::read(h.dir.path().join("native-runtime/native-processes.json"))
                            .unwrap();
                    if let Ok(processes) = serde_json::from_slice::<Value>(&bytes) {
                        if processes.as_array().unwrap().iter().all(|p| {
                            p["closed"] == true
                                && !Path::new(&format!("/proc/{}", p["pid"].as_u64().unwrap()))
                                    .exists()
                        }) {
                            break;
                        }
                    }
                    // timing-guard: observe original native child containment after daemon SIGKILL
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            },
        )
        .await
        .expect("original native child survived abrupt daemon loss");
    }
    let closed: Value = serde_json::from_slice(
        &std::fs::read(h.dir.path().join("native-runtime/native-processes.json")).unwrap(),
    )
    .unwrap();
    assert!(
        closed
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p["closed"] == true),
        "native pipes must close before daemon exit: {closed}"
    );
    for process in closed.as_array().unwrap() {
        assert!(!Path::new(&format!("/proc/{}", process["pid"].as_u64().unwrap())).exists());
    }
    if abrupt {
        let before_ids: Vec<_> = original_live
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["pid"].clone())
            .collect();
        let after_ids: Vec<_> = closed
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["pid"].clone())
            .collect();
        assert_eq!(
            before_ids, after_ids,
            "post-kill observations changed the original allocation set"
        );
        std::fs::write(
            h.dir.path().join("abrupt-original-transition.json"),
            serde_json::to_vec(&json!({"before":original_live,"after":closed,"daemonSignal":9}))
                .unwrap(),
        )
        .unwrap();
    }
    let log = std::fs::File::create(h.dir.path().join("daemon-restarted.log")).unwrap();
    let mut command = common::hermetic_serve_command(h.dir.path());
    command
        .env(
            "INTENTD_TEST_REVIEW_LAUNCHER",
            h.dir.path().join("native-runtime/launcher"),
        )
        .env("INTENTD_REVIEW_MONITOR_TEST_SUPPORT", "1")
        .env(
            "INTENTD_REPOSITORY_TEST_TRANSPORTS",
            json!([["https://gitlab.test/forge", endpoint]]).to_string(),
        )
        .env_remove("INTENT_TEST_PRIVATE_WRITE_HOLD")
        .env("INTENTD_DATA_DIR", h.dir.path())
        .env("INTENTD_WORKSPACES_DIR", h.dir.path().join("workspaces"))
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_SPECIALISTS_DIR", h.dir.path().join("specialists"))
        .env(
            "RUST_LOG",
            "warn,intentd=info,intent_services::repository_native_wire=debug",
        )
        .env_remove("GITLAB_TOKEN")
        .env("PATH", h.dir.path().join("bin"))
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    h.child = GuardedChild::spawn(&mut command).unwrap();
    let socket = h.dir.path().join("intentd.sock");
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(&socket).await.is_ok() {
                break;
            }
            // timing-guard: restarted daemon socket readiness
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    let status = common::await_wss_status(&socket).await;
    h.port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    h.cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if std::fs::read_to_string(h.dir.path().join("daemon-restarted.log"))
                .unwrap()
                .contains("config.toml live-reload watcher ready")
            {
                break;
            }
            // timing-guard: restarted configuration watcher ready before source admission
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}

async fn native_daemon_case(
    register_monitor: bool,
    deliver_monitor: bool,
    retire_monitor: bool,
    restart: bool,
    recovery: bool,
    abrupt: bool,
    live_hold: Option<(bool, bool, bool)>,
) {
    let input = PathBuf::from(std::env::var_os("INTENT_MONITOR_HISTORY_DATABASE_FIXTURE").unwrap());
    let account = OriginalAccount::new().await;
    let mut h = Harness::boot_case(
        None,
        true,
        Some((&input, &account.endpoint)),
        live_hold.is_some(),
        true,
    )
    .await;
    if register_monitor {
        let instruction = json!({"summary":"Register the original review monitor","code":"return await ws.pr.monitor(4, {target:{repository:{provider:'gitlab',instanceBaseUrl:'https://gitlab.test/forge',projectPath:'group/project'},kind:'merge-request',number:4}});"});
        std::fs::write(
            h.dir.path().join("native-runtime/monitor-request.json"),
            serde_json::to_vec(&instruction).unwrap(),
        )
        .unwrap();
    }
    let mut client = h.wss(TOKEN).await;
    authorize(&mut client, "stored-pat").await;
    let created=client.rpc("agent.create",json!({"workspaceId":h.workspace,"name":"Native daemon owner","model":"claude-sonnet-4-6","provider":"claude-code"})).await;
    let agent = intent_core::AgentId::from(success(&created)["agent"]["id"].as_str().unwrap());
    success(&client.rpc("agent.sendMessage",json!({"workspaceId":h.workspace,"agentId":agent,"content":"Ordinary daemon startup control"})).await);
    let store = Store::open(&h.dir.path().join("intentd.db")).await.unwrap();
    let model = h.dir.path().join("native-runtime/model.json");
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(45)),
        async {
            loop {
                let session = store.get_agent_session(&agent).await.unwrap();
                if model.exists() && session.status == intent_core::AgentStatus::RuntimeIdle {
                    break;
                }
                if session.status == intent_core::AgentStatus::Error {
                    panic!(
                        "native daemon startup failed: {:?}\n{}",
                        session.status,
                        std::fs::read_to_string(h.dir.path().join("daemon.log")).unwrap()
                    );
                }
                // timing-guard: wait for actual daemon provider turn completion
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        },
    )
    .await
    .expect("native daemon turn did not complete");
    let session = store.get_agent_session(&agent).await.unwrap();
    let session_id = session
        .acp_session_id
        .as_ref()
        .expect("original ACP session persisted");
    assert!(
        store
            .get_review_context_enrollment(&h.workspace, &agent, session_id)
            .await
            .unwrap()
            .is_some(),
        "actual daemon startup must retain original private native enrollment"
    );
    let mut requests: Value = serde_json::from_slice(&std::fs::read(&model).unwrap()).unwrap();
    assert!(requests
        .to_string()
        .contains("Ordinary daemon startup control"));
    assert!(!requests
        .to_string()
        .contains("Intent review monitor observation (not a user instruction)."));
    let mut retirement = Value::Null;
    if register_monitor {
        assert_eq!(
            requests.as_array().unwrap().len(),
            2,
            "actual callback result must reach the next model request: {requests}"
        );
        let rows = store
            .list_active_pr_monitors_by_agent(&agent)
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "model callback must register its original monitor: {requests}"
        );
        assert_eq!(rows[0].agent_id, agent);
        assert_eq!(rows[0].workspace_id, h.workspace);
        assert!(account
            .paths
            .lock()
            .unwrap()
            .iter()
            .any(|p| p == "/api/v4/projects/group%2Fproject/merge_requests/4"));
        let mut live_chat = if deliver_monitor && !retire_monitor && !restart {
            let hello = client
                .rpc("client.hello", json!({"clientId":"native-live-chat"}))
                .await;
            let mut capable = connect(&h, live_hold.map(|case| case.0).unwrap_or(true)).await;
            success(&capable.rpc("client.hello", json!({"clientId":success(&hello)["clientId"],"connectionRole":"review-monitor","capabilities":{"reviewMonitoring":1}})).await);
            let subscribed = capable
                .rpc(
                    "chat.subscribe",
                    json!({"agentId":agent,"historyDelivery":"snapshot"}),
                )
                .await;
            let id = success(&subscribed)["subscriptionId"].clone();
            let initial = push(&mut capable, &id).await;
            assert_eq!(initial["params"]["seq"], 0);
            assert!(initial
                .to_string()
                .contains("Ordinary daemon startup control"));
            assert!(!initial.to_string().contains("pr_monitor_wake"));
            if live_hold.is_some() {
                success(&capable.rpc("events.subscribe", json!({"eventTypes":["settings:changed"],"replaceGroup":"later-public"})).await);
                std::fs::write(
                    h.dir.path().join("private-write-hold/arm"),
                    "pr_monitor_wake",
                )
                .unwrap();
            }
            Some((capable, id))
        } else {
            None
        };
        if deliver_monitor {
            *account.review_state.lock().unwrap() = "merged";
            timeout(
                intentd_test_support::test_timeout(Duration::from_secs(90)),
                async {
                    loop {
                        let current: Value =
                            serde_json::from_slice(&std::fs::read(&model).unwrap()).unwrap();
                        let row = store.get_pr_monitor(&rows[0].monitor_id).await.unwrap();
                        let session = store.get_agent_session(&agent).await.unwrap();
                        if current.as_array().unwrap().len() >= 3
                            && row.pending_changes.is_empty()
                            && session.status == intent_core::AgentStatus::RuntimeIdle
                        {
                            requests = current;
                            break;
                        }
                        // timing-guard: observe actual scheduled forge poll and native delivery completion
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                },
            )
            .await
            .expect("original scheduled monitor did not reach native model");
            if let Some(case) = live_hold {
                let (capable, id) = live_chat.as_mut().unwrap();
                live_hold::exercise(&h, &store, &agent, &mut client, (capable, id), case).await;
                return;
            }
            if let Some((capable, id)) = live_chat.as_mut() {
                let frames = timeout(
                    intentd_test_support::test_timeout(Duration::from_secs(5)),
                    async {
                        let mut frames = Vec::new();
                        loop {
                            let frame = push(capable, id).await;
                            let found = frame.to_string().contains("pr_monitor_wake");
                            frames.push(frame);
                            if found {
                                return frames;
                            }
                        }
                    },
                )
                .await
                .expect("qualified live chat did not receive persisted monitor wake");
                let update = frames.last().unwrap();
                assert_eq!(update["params"]["subscriptionId"], *id);
                assert_eq!(update["params"]["seq"], 1);
                assert_eq!(update["params"]["kind"], "snapshot");
                let private_rows: Vec<_> = update["params"]["snapshot"]["messages"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|message| message["metadata"]["type"] == "pr_monitor_wake")
                    .collect();
                assert_eq!(private_rows.len(), 1);
                let original = private_rows[0];
                assert_eq!(original["role"], "user");
                assert_eq!(original["agentId"], agent.as_str());
                assert_eq!(original["seq"], 2);
                assert_eq!(
                    original["metadata"]["monitorId"],
                    rows[0].monitor_id.as_str()
                );
                assert_eq!(
                    original["metadata"]["resource"],
                    json!({
                        "repository":{"provider":"gitlab","instanceBaseUrl":"https://gitlab.test/forge","projectPath":"group/project"},
                        "kind":"merge-request","number":4
                    })
                );
                let persisted =
                    serde_json::to_value(store.get_agent_messages(&agent, None).await.unwrap())
                        .unwrap();
                let same = persisted
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|message| message["id"] == original["id"])
                    .unwrap();
                assert_eq!(same["role"], original["role"]);
                assert_eq!(same["seq"], original["seq"]);
                if let Some(dir) = std::env::var_os("INTENT_MONITOR_FIXTURE_DIR") {
                    std::fs::create_dir_all(&dir).unwrap();
                    std::fs::write(
                        PathBuf::from(dir).join("native-live-chat.json"),
                        serde_json::to_vec_pretty(&frames).unwrap(),
                    )
                    .unwrap();
                }
            }
            let last = requests.as_array().unwrap().last().unwrap();
            assert!(last
                .to_string()
                .contains("Intent review monitor observation (not a user instruction)."));
            assert!(last.to_string().contains("merged"));
            let row = store.get_pr_monitor(&rows[0].monitor_id).await.unwrap();
            assert_eq!(row.state, intent_core::PrMonitorState::Completed);
            assert!(row.pending_changes.is_empty());
            if retire_monitor {
                let processes = h.dir.path().join("native-runtime/native-processes.json");
                let before: Value =
                    serde_json::from_slice(&std::fs::read(&processes).unwrap()).unwrap();
                assert_eq!(before.as_array().unwrap().len(), 1);
                assert_eq!(before[0]["closed"], false);
                let original_messages =
                    serde_json::to_value(store.get_agent_messages(&agent, None).await.unwrap())
                        .unwrap();
                authorize(&mut client, "pat-second").await;
                let after: Value =
                    serde_json::from_slice(&std::fs::read(&processes).unwrap()).unwrap();
                assert_eq!(after[0]["pid"], before[0]["pid"]);
                assert_eq!(
                    after[0]["closed"], true,
                    "account replacement must join original child and its pipes before success"
                );
                assert!(
                    !Path::new(&format!("/proc/{}", after[0]["pid"].as_u64().unwrap())).exists()
                );
                assert_eq!(
                    serde_json::to_value(store.get_agent_messages(&agent, None).await.unwrap())
                        .unwrap(),
                    original_messages
                );
                assert_eq!(store.get_pr_monitor(&row.monitor_id).await.unwrap(), row);
                assert_eq!(
                    serde_json::from_slice::<Value>(&std::fs::read(&model).unwrap()).unwrap(),
                    requests
                );
                retirement = json!({"before":before,"after":after,"messagesPreserved":true,"monitorPreserved":true});
            }
        }
    }
    let mut restarted = Value::Null;
    if restart {
        let original_pid = h.child.id();
        let original_enrollment = store
            .get_review_context_enrollment(&h.workspace, &agent, session_id)
            .await
            .unwrap()
            .unwrap()
            .owner_key()
            .to_owned();
        let original_messages =
            serde_json::to_value(store.get_agent_messages(&agent, None).await.unwrap()).unwrap();
        std::fs::remove_file(h.dir.path().join("native-runtime/monitor-request.json")).unwrap();
        restart_native_daemon_mode(&mut h, &account.endpoint, abrupt).await;
        assert_ne!(h.child.id(), original_pid);
        assert_eq!(
            store
                .get_review_context_enrollment(&h.workspace, &agent, session_id)
                .await
                .unwrap()
                .unwrap()
                .owner_key(),
            original_enrollment
        );
        assert_eq!(
            serde_json::to_value(store.get_agent_messages(&agent, None).await.unwrap()).unwrap(),
            original_messages
        );
        let mut fresh = h.wss(TOKEN).await;
        // The restarted adapter owns a new capture; remove only the old fixture output.
        std::fs::remove_file(&model).unwrap();
        let mut refusal = Value::Null;
        if recovery {
            authorize(&mut fresh, "pat-second").await;
            let response=fresh.rpc("agent.sendMessage",json!({"workspaceId":h.workspace,"agentId":agent,"content":"Replacement account must not consume private history"})).await;
            if response.get("error").is_none() {
                timeout(
                    intentd_test_support::test_timeout(Duration::from_secs(45)),
                    async {
                        loop {
                            let current = store.get_agent_session(&agent).await.unwrap();
                            if current.status == intent_core::AgentStatus::Error {
                                break;
                            }
                            assert!(!model.exists(), "replacement account reached native model");
                            // timing-guard: wait for original private admission refusal through manager
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                    },
                )
                .await
                .expect("replacement account did not receive a private admission refusal");
            }
            assert!(
                !model.exists(),
                "refused replacement must not consume native history"
            );
            assert_eq!(
                store
                    .get_agent_session(&agent)
                    .await
                    .unwrap()
                    .acp_session_id
                    .as_deref(),
                Some(session_id.as_str())
            );
            assert_eq!(
                store
                    .get_review_context_enrollment(&h.workspace, &agent, session_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .owner_key(),
                original_enrollment
            );
            let after = serde_json::to_value(store.get_agent_messages(&agent, None).await.unwrap())
                .unwrap();
            for message in original_messages.as_array().unwrap() {
                assert!(
                    after.as_array().unwrap().contains(message),
                    "original history changed during refusal"
                );
            }
            refusal =
                json!({"response":response,"modelRequests":0,"originalMessagesPreserved":true});
        }
        authorize(&mut fresh, "stored-pat").await;
        success(&fresh.rpc("agent.sendMessage",json!({"workspaceId":h.workspace,"agentId":agent,"content":"Ordinary followup after cold daemon restart"})).await);
        timeout(
            intentd_test_support::test_timeout(Duration::from_secs(45)),
            async {
                loop {
                    let current = store.get_agent_session(&agent).await.unwrap();
                    if model.exists() && current.status == intent_core::AgentStatus::RuntimeIdle {
                        break;
                    }
                    assert_ne!(
                        current.status,
                        intent_core::AgentStatus::Error,
                        "restarted private LOAD failed: {}",
                        std::fs::read_to_string(h.dir.path().join("daemon-restarted.log")).unwrap()
                    );
                    // timing-guard: actual restarted daemon LOAD and ordinary turn completion
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            },
        )
        .await
        .expect("restarted private LOAD did not complete");
        let replay: Value = serde_json::from_slice(&std::fs::read(&model).unwrap()).unwrap();
        assert_eq!(
            replay.as_array().unwrap().len(),
            if recovery { 2 } else { 1 }
        );
        assert!(replay
            .to_string()
            .contains("Intent review monitor observation (not a user instruction)."));
        // Native request normalization may coalesce adjacent user messages. Select
        // the original ordinary block, not the entire combined history vector.
        let ordinary: Vec<_> = replay[0]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["role"] == "user")
            .flat_map(|m| m["content"].as_array().unwrap())
            .filter(|block| {
                block
                    .to_string()
                    .contains("Ordinary followup after cold daemon restart")
            })
            .collect();
        assert_eq!(ordinary.len(), 1);
        if recovery {
            assert_eq!(ordinary[0]["type"], "text");
            let text = ordinary[0]["text"].as_str().unwrap();
            let (snapshot, content) = text.split_once("\n\n").expect("queued recovery snapshot");
            let snapshot: Value = serde_json::from_str(
                snapshot
                    .strip_prefix("current ws.agent.snapshot() => ")
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(snapshot["queuedMessages"], 1);
            assert_eq!(content, "Ordinary followup after cold daemon restart");
            assert!(!replay[0]
                .to_string()
                .contains("Replacement account must not consume private history"));
            let recovered: Vec<_> = replay[1]["messages"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|m| m["role"] == "user")
                .flat_map(|m| m["content"].as_array().unwrap())
                .filter(|block| {
                    block
                        .to_string()
                        .contains("Replacement account must not consume private history")
                })
                .collect();
            assert_eq!(recovered.len(), 1);
            assert_eq!(
                *recovered[0],
                json!({"type":"text","text":"Replacement account must not consume private history"})
            );
        } else {
            assert_eq!(
                *ordinary[0],
                json!({"type":"text","text":"Ordinary followup after cold daemon restart"})
            );
        }
        assert_eq!(
            store
                .get_agent_session(&agent)
                .await
                .unwrap()
                .acp_session_id
                .as_deref(),
            Some(session_id.as_str())
        );
        let processes = h.dir.path().join("native-runtime/native-processes.json");
        let before: Value = serde_json::from_slice(&std::fs::read(&processes).unwrap()).unwrap();
        assert_eq!(before[0]["closed"], false);
        let messages_before_retirement =
            serde_json::to_value(store.get_agent_messages(&agent, None).await.unwrap()).unwrap();
        authorize(&mut fresh, "pat-second").await;
        let after: Value = serde_json::from_slice(&std::fs::read(&processes).unwrap()).unwrap();
        assert_eq!(after[0]["pid"], before[0]["pid"]);
        assert_eq!(after[0]["closed"], true);
        assert!(!Path::new(&format!("/proc/{}", after[0]["pid"].as_u64().unwrap())).exists());
        assert_eq!(
            serde_json::from_slice::<Value>(&std::fs::read(&model).unwrap()).unwrap(),
            replay
        );
        assert_eq!(
            serde_json::to_value(store.get_agent_messages(&agent, None).await.unwrap()).unwrap(),
            messages_before_retirement
        );
        let original_transition = if abrupt {
            serde_json::from_slice::<Value>(
                &std::fs::read(h.dir.path().join("abrupt-original-transition.json")).unwrap(),
            )
            .unwrap()
        } else {
            Value::Null
        };
        restarted = json!({"abrupt":abrupt,"originalTransition":original_transition,"daemonPids":[original_pid,h.child.id()],"requests":replay,"nativeBefore":before,"nativeAfter":after,"refusal":refusal});
    }
    if let Some(dir) = std::env::var_os("INTENT_MONITOR_FIXTURE_DIR") {
        std::fs::create_dir_all(&dir).unwrap();
        let name = if abrupt && recovery {
            "daemon-native-abrupt-restart-recovery.json"
        } else if abrupt {
            "daemon-native-abrupt-restart.json"
        } else if recovery {
            "daemon-native-restart-recovery.json"
        } else if restart {
            "daemon-native-restart.json"
        } else if retire_monitor {
            "daemon-native-account-retirement.json"
        } else if deliver_monitor {
            "daemon-native-scheduler.json"
        } else if register_monitor {
            "daemon-native-monitor-register.json"
        } else {
            "daemon-native-startup.json"
        };
        std::fs::write(Path::new(&dir).join(name),serde_json::to_vec_pretty(&json!({"agentId":agent,"sessionId":session_id,"requests":requests,"retirement":retirement,"restarted":restarted})).unwrap()).unwrap();
    }
}
