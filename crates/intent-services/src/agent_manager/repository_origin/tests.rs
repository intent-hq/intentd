//! Disposable physical handles and actual ACP frames; no provider process.

use super::super::{AgentManager, BusEventSink, EventBus};
use super::*;
use crate::repository_admission::request_context::RepositoryCapturedRequest;
use crate::repository_admission::{
    begin_repository_stage, capture_repository_operation, revalidate_repository_stage,
    AdmissionError, AdmissionResult, OriginalRepositoryCaller, RepositoryAuthorityFacts,
    RepositoryAuthorityProvenance, RepositoryAuthoritySource, RepositoryEntry,
    RepositoryOperationFacts, RepositoryOperationSource, RepositoryRetirement,
};
use crate::repository_credentials::authority::{
    RepositoryAuthorityFence, RepositoryCredentialTransport,
};
use crate::repository_credentials::{RepositoryAuthorityRequest, RepositoryCredentialUse};
use intent_acp::{Connection, ConnectionHooks, EventSink, IncomingNotification};
use intent_core::caller::{with_caller, Caller};
use intent_core::{chief_workspace, BoxFuture, NativeReviewPreparation, NativeReviewStage};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream};
use tokio::sync::{mpsc, Mutex as TokioMutex};

pub(super) struct Fixture {
    pub(super) dir: tempfile::TempDir,
    pub(super) manager: Arc<AgentManager>,
    pub(super) row: AgentSession,
}

impl Fixture {
    pub(super) async fn new() -> Self {
        Self::with_provider("claude-code").await
    }

    pub(super) async fn with_provider(provider: &str) -> Self {
        Self::with_initial_stamp(provider, None).await
    }

    pub(super) async fn with_initial_stamp(provider: &str, stamp: Option<&str>) -> Self {
        let dir = crate::test_support::test_tempdir("manager-repository-origin");
        let store = Store::open(&dir.path().join("store.db")).await.unwrap();
        let mut workspace = chief_workspace();
        workspace.id = WorkspaceId::new();
        store.insert_workspace(&workspace).await.unwrap();
        let mut value = json!({
            "id":AgentId::new(),"workspaceId":workspace.id,"name":"original owner",
            "provider":provider,"status":"idle",
            "createdAt":"2026-09-27T00:00:00Z","updatedAt":"2026-09-27T00:00:00Z"
        });
        if let Some(stamp) = stamp {
            value["harnessVersion"] = json!(stamp);
        }
        let row: AgentSession = serde_json::from_value(value).unwrap();
        store.insert_agent_session(&row).await.unwrap();
        let bus = EventBus::new(store.clone());
        let services = Services::new(store).with_event_bus(bus.clone());
        let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus));
        Self {
            dir,
            manager: Arc::new(AgentManager::new(services, sink, 4)),
            row,
        }
    }

    pub(super) fn caller(&self) -> Caller {
        Caller::Agent {
            agent_id: self.row.id.clone(),
        }
    }

    pub(super) async fn origin(&self) -> Arc<RepositoryOrigin> {
        RepositoryOrigin::allocate(&self.manager.services, &self.row).await
    }
}

pub(super) fn handle(
    origin: Arc<RepositoryOrigin>,
) -> (AgentHandle, BufReader<DuplexStream>, DuplexStream) {
    let (client_write, peer_read) = tokio::io::duplex(4096);
    let (peer_write, client_read) = tokio::io::duplex(4096);
    let (notes, receiver) = mpsc::unbounded_channel::<IncomingNotification>();
    let connection = Arc::new(Connection::new(
        client_write,
        client_read,
        None,
        ConnectionHooks {
            notifications: Some(notes),
            ..ConnectionHooks::default()
        },
    ));
    (
        AgentHandle {
            repository_origin: origin,
            connection,
            notifications: Arc::new(TokioMutex::new(receiver)),
            serve_task: tokio::spawn(std::future::pending()),
            child: None,
            child_pid: None,
            _mcp_bridge: None,
            _mcp_config: None,
            _rules_config: None,
            _pi_extension: None,
            npx_launch_dir: None,
            antigravity_profile: None,
            session_mcp_servers: vec![],
            spawned_model: None,
            spawned_provider: "claude-agent-acp".into(),
            thought_level: None,
            confirmed_effort: None,
            wake_gate: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            wake_listener: None,
        },
        BufReader::new(peer_read),
        peer_write,
    )
}

async fn reply_new(reader: &mut BufReader<DuplexStream>, writer: &mut DuplexStream, id: &str) {
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    let request: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(request["method"], "session/new");
    let response = json!({"jsonrpc":"2.0","id":request["id"],"result":{"sessionId":id}});
    writer
        .write_all(format!("{response}\n").as_bytes())
        .await
        .unwrap();
}

async fn confirm(
    fixture: &Fixture,
    handle: &AgentHandle,
    reader: &mut BufReader<DuplexStream>,
    writer: &mut DuplexStream,
) -> RepositoryCallbackContext {
    let (creator, attempt) = handle
        .repository_origin
        .begin_session(RepositoryCreationIntent::FirstSet)
        .unwrap();
    let operation = creator.initialize(&fixture.manager.services.store, || async {
        let response =
            intent_acp::session::new_session(&handle.connection, fixture.dir.path(), vec![], None)
                .await
                .map_err(|_| AdmissionError::Unavailable)?;
        Ok(response.session_id.0.to_string())
    });
    let (owner, ()) = tokio::join!(operation, reply_new(reader, writer, "original-acp"));
    let owner = owner.unwrap();
    let fresh = owner.callback();
    assert!(handle.repository_origin.install(attempt, owner));
    fresh
}

pub(super) async fn current(request: &RepositoryCapturedRequest, caller: Caller) -> bool {
    with_caller(caller, async { request.source_lifetime().is_ok() }).await
}

struct ScriptedSession {
    task: tokio::task::JoinHandle<()>,
    calls: Arc<Mutex<Vec<Value>>>,
}

impl Drop for ScriptedSession {
    fn drop(&mut self) {
        self.task.abort();
    }
}

type SessionReply = std::result::Result<Value, Value>;

fn session_reply(request: &Value, load: bool, session_id: &str) -> Value {
    match request["method"].as_str().unwrap() {
        "initialize" => json!({
            "protocolVersion":1,"agentCapabilities":{"loadSession":load}
        }),
        "session/new" => json!({"sessionId":session_id}),
        "session/set_config_option" => json!({"configOptions":[{
            "id":"model","name":"Model","category":"model","type":"select",
            "currentValue":request["params"]["value"],
            "options":[{"value":"model-a","name":"Model A"}]
        }]}),
        _ => json!({}),
    }
}

fn scripted_replies(
    mut reader: BufReader<DuplexStream>,
    mut writer: DuplexStream,
    mut reply: impl FnMut(Value) -> BoxFuture<'static, SessionReply> + Send + 'static,
) -> ScriptedSession {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let task = tokio::spawn(async move {
        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                return;
            }
            let request: Value = serde_json::from_str(&line).unwrap();
            recorded.lock().unwrap().push(request.clone());
            let Some(id) = request.get("id").cloned() else {
                continue;
            };
            let response = match reply(request).await {
                Ok(result) => json!({"jsonrpc":"2.0","id":id,"result":result}),
                Err(error) => json!({"jsonrpc":"2.0","id":id,"error":error}),
            };
            if writer
                .write_all(format!("{response}\n").as_bytes())
                .await
                .is_err()
            {
                return;
            }
        }
    });
    ScriptedSession { task, calls }
}

fn scripted_session(
    reader: BufReader<DuplexStream>,
    writer: DuplexStream,
    load: bool,
    session_id: &'static str,
) -> ScriptedSession {
    scripted_replies(reader, writer, move |request| {
        Box::pin(async move { Ok(session_reply(&request, load, session_id)) })
    })
}

struct PausedSession {
    peer: ScriptedSession,
    arrived: tokio::sync::oneshot::Receiver<Value>,
    release: tokio::sync::oneshot::Sender<SessionReply>,
}

fn paused_session(
    reader: BufReader<DuplexStream>,
    writer: DuplexStream,
    load: bool,
    method: &'static str,
) -> PausedSession {
    let (entered, arrived) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    let mut pause = Some((entered, released));
    let peer = scripted_replies(reader, writer, move |request| {
        let gate = (request["method"] == method)
            .then(|| pause.take())
            .flatten();
        Box::pin(async move {
            if let Some((entered, released)) = gate {
                entered.send(request).unwrap();
                released
                    .await
                    .unwrap_or_else(|_| Err(json!({"code":-32603,"message":"cancelled fixture"})))
            } else {
                Ok(session_reply(&request, load, "candidate"))
            }
        })
    });
    PausedSession {
        peer,
        arrived,
        release,
    }
}

async fn arrived(receiver: tokio::sync::oneshot::Receiver<Value>) -> Value {
    tokio::time::timeout(std::time::Duration::from_secs(10), receiver)
        .await
        .expect("actual ACP request arrived")
        .unwrap()
}

pub(super) fn callback(origin: &RepositoryOrigin) -> Option<RepositoryCallbackContext> {
    origin
        .state
        .lock()
        .unwrap()
        .owner
        .as_ref()
        .map(RepositoryPhysicalOwner::callback)
}

impl Fixture {
    pub(super) async fn seed_session(
        &mut self,
        id: Option<&str>,
        provider: &str,
        model: Option<&str>,
    ) {
        self.row.acp_session_id = id.map(str::to_owned);
        self.row.provider = Some(provider.into());
        self.row.model = model.map(str::to_owned);
        self.manager
            .services
            .store
            .update_agent_session(&self.row.workspace_id, &self.row)
            .await
            .unwrap();
    }

    async fn mount(&self) -> (Arc<RepositoryOrigin>, BufReader<DuplexStream>, DuplexStream) {
        let origin = self.origin().await;
        let (handle, reader, writer) = handle(origin.clone());
        self.manager
            .handles
            .lock()
            .unwrap()
            .insert(self.row.id.clone(), handle);
        (origin, reader, writer)
    }

    pub(super) async fn start(&self, provider: &str) -> intent_core::Result<String> {
        self.manager
            .start_session(
                &self.row.id,
                self.dir.path().to_path_buf(),
                intent_providers::provider_config(provider),
            )
            .await
    }

    pub(super) async fn stored(&self) -> AgentSession {
        self.manager
            .services
            .store
            .get_agent_session(&self.row.id)
            .await
            .unwrap()
    }

    pub(super) async fn count_writes(&self) {
        sqlx::query("CREATE TABLE observed_acp_writes (id TEXT)")
            .execute(self.manager.services.store.write_pool())
            .await
            .unwrap();
        sqlx::query("CREATE TRIGGER observe_acp_write AFTER UPDATE OF acp_session_id ON agent_session BEGIN INSERT INTO observed_acp_writes VALUES (NEW.id); END")
            .execute(self.manager.services.store.write_pool()).await.unwrap();
    }

    pub(super) async fn writes(&self) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM observed_acp_writes")
            .fetch_one(self.manager.services.store.read_pool())
            .await
            .unwrap()
    }

    pub(super) async fn usage(&self, input: u64) {
        self.manager
            .services
            .store
            .set_agent_session_token_usage(
                &self.row.workspace_id,
                &self.row.id,
                &intent_core::TokenUsageTotals {
                    input_tokens: input,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }

    pub(super) async fn accounting(&self) -> (Option<Value>, Option<Value>) {
        let (current, baseline): (Option<String>, Option<String>) = sqlx::query_as(
            "SELECT token_usage, token_usage_baseline FROM agent_session WHERE id = ?",
        )
        .bind(&self.row.id.0)
        .fetch_one(self.manager.services.store.read_pool())
        .await
        .unwrap();
        (
            current.map(|s| serde_json::from_str(&s).unwrap()),
            baseline.map(|s| serde_json::from_str(&s).unwrap()),
        )
    }
}

#[tokio::test]
async fn actual_start_session_retains_original_owner_through_metadata_and_idle() {
    let f = Fixture::new().await;
    let origin = f.origin().await;
    let pending = origin.pending_callback().unwrap();
    let early = pending.capture();
    // This is the same narrow write performed after endpoint exposure in
    // create_agent. It must not be skipped or mistaken for a binding change.
    f.manager
        .services
        .store
        .set_agent_session_system_prompt(&f.row.workspace_id, &f.row.id, "assembled prompt")
        .await
        .unwrap();
    let (handle, reader, writer) = handle(origin.clone());
    let peer = scripted_session(reader, writer, false, "manager-created");
    f.manager
        .handles
        .lock()
        .unwrap()
        .insert(f.row.id.clone(), handle);
    let session = f
        .manager
        .start_session(
            &f.row.id,
            f.dir.path().to_path_buf(),
            intent_providers::provider_config("claude-code"),
        )
        .await
        .unwrap();
    assert_eq!(session, "manager-created");
    assert_eq!(
        peer.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call["method"] == "session/new")
            .count(),
        1,
    );
    let fresh = origin
        .state
        .lock()
        .unwrap()
        .owner
        .as_ref()
        .map(RepositoryPhysicalOwner::callback);
    let fresh = fresh.expect("the actual start_session must retain its original confirmed owner");
    assert!(!current(&early, f.caller()).await);
    assert!(!current(&pending.capture(), f.caller()).await);
    assert!(current(&fresh.capture(), f.caller()).await);
    let mut stored = f
        .manager
        .services
        .store
        .get_agent_session(&f.row.id)
        .await
        .unwrap();
    assert_eq!(stored.system_prompt.as_deref(), Some("assembled prompt"));
    for status in [
        intent_core::AgentStatus::Active,
        intent_core::AgentStatus::RuntimeIdle,
    ] {
        stored.status = status;
        f.manager
            .services
            .store
            .update_agent_session(&f.row.workspace_id, &stored)
            .await
            .unwrap();
        assert!(current(&fresh.capture(), f.caller()).await);
    }
    f.manager
        .services
        .store
        .set_agent_session_model(
            &f.row.workspace_id,
            &f.row.id,
            "different-model",
            Some("claude-code"),
            "2026-09-27T21:00:00Z",
        )
        .await
        .unwrap();
    assert!(!current(&fresh.capture(), f.caller()).await);
}

#[tokio::test]
async fn actual_acp_confirmation_never_upgrades_the_pending_endpoint() {
    let f = Fixture::new().await;
    let origin = f.origin().await;
    let pending = origin.pending_callback().unwrap();
    let early = pending.capture();
    let (handle, mut reader, mut writer) = handle(origin);
    let fresh = confirm(&f, &handle, &mut reader, &mut writer).await;
    assert!(!current(&early, f.caller()).await);
    assert!(!current(&pending.capture(), f.caller()).await);
    assert!(current(&fresh.capture(), f.caller()).await);
    drop(handle);
    assert!(!current(&fresh.capture(), f.caller()).await);
}

#[tokio::test]
async fn late_kill_for_an_old_handle_cannot_remove_a_same_id_replacement() {
    let f = Fixture::new().await;
    let original = f.origin().await;
    let (first, _reader, _writer) = handle(original.clone());
    f.manager
        .handles
        .lock()
        .unwrap()
        .insert(f.row.id.clone(), first);
    let late = f.manager.make_kill(f.row.id.clone());
    let old = take(&f.manager.handles, &f.row.id, &original, None).unwrap();
    let replacement = RepositoryOrigin::unavailable();
    let (next, _next_reader, _next_writer) = handle(replacement.clone());
    f.manager
        .handles
        .lock()
        .unwrap()
        .insert(f.row.id.clone(), next);
    late().await;
    assert!(Arc::ptr_eq(
        &capture(&f.manager.handles, &f.row.id).unwrap(),
        &replacement,
    ));
    drop(old);
    assert!(!replacement.state.lock().unwrap().retired);
}

#[tokio::test]
async fn retirement_before_late_confirmation_rejects_the_new_owner() {
    let f = Fixture::new().await;
    let original = f.origin().await;
    let (creator, _) = original
        .begin_session(RepositoryCreationIntent::FirstSet)
        .unwrap();
    let (handle, mut reader, mut writer) = handle(original.clone());
    original.retire();
    let operation = creator.initialize(&f.manager.services.store, || async {
        let response =
            intent_acp::session::new_session(&handle.connection, f.dir.path(), vec![], None)
                .await
                .map_err(|_| AdmissionError::Unavailable)?;
        Ok(response.session_id.0.to_string())
    });
    let (owner, ()) = tokio::join!(operation, reply_new(&mut reader, &mut writer, "late-acp"));
    assert!(
        owner.is_err(),
        "retiring the actual handle must stop its escaped pending initialization"
    );
    assert!(original.pending_callback().is_none());
    assert!(original
        .begin_session(RepositoryCreationIntent::FirstSet)
        .is_none());
}

#[tokio::test]
async fn soft_cancel_retires_requests_but_idle_keeps_the_physical_owner() {
    let f = Fixture::new().await;
    let original = f.origin().await;
    let (handle, mut reader, mut writer) = handle(original.clone());
    let callback = confirm(&f, &handle, &mut reader, &mut writer).await;
    let before = callback.capture();
    original.interrupt_requests();
    assert!(!current(&before, f.caller()).await);
    assert!(current(&callback.capture(), f.caller()).await);
    f.manager.registry.mark_idle_slot_held(&f.row.id);
    assert!(current(&callback.capture(), f.caller()).await);
    original.retire();
    assert!(!current(&callback.capture(), f.caller()).await);
}

#[tokio::test]
async fn pending_initialization_cancellation_keeps_the_original_callback_unavailable() {
    let f = Fixture::new().await;
    let origin = f.origin().await;
    let pending = origin.pending_callback().unwrap();
    let (creator, _) = origin
        .begin_session(RepositoryCreationIntent::FirstSet)
        .unwrap();
    let (handle, mut reader, _writer) = handle(origin.clone());
    let mut operation = Box::pin(creator.initialize(&f.manager.services.store, || async {
        let response =
            intent_acp::session::new_session(&handle.connection, f.dir.path(), vec![], None)
                .await
                .map_err(|_| AdmissionError::Unavailable)?;
        Ok(response.session_id.0.to_string())
    }));
    let mut line = String::new();
    tokio::select! {
        _ = &mut operation => panic!("ACP operation completed without its response"),
        result = reader.read_line(&mut line) => { assert!(result.unwrap() > 0); },
    }
    origin.retire();
    drop(operation);
    assert!(!current(&pending.capture(), f.caller()).await);
    assert!(f
        .manager
        .services
        .store
        .get_agent_session(&f.row.id)
        .await
        .unwrap()
        .acp_session_id
        .is_none());
}

struct CheckRetirementOnDrop {
    leaf: RepositoryRetirement,
    observed: Option<tokio::sync::oneshot::Sender<bool>>,
}

impl Drop for CheckRetirementOnDrop {
    fn drop(&mut self) {
        if let Some(observed) = self.observed.take() {
            let _ = observed.send(self.leaf.check_current() == Err(AdmissionError::Retired));
        }
    }
}

#[tokio::test]
async fn actual_teardown_retires_the_source_leaf_before_aborting_work() {
    for action in ["drop", "detach", "shutdown", "kill"] {
        let f = Fixture::new().await;
        let origin = f.origin().await;
        let (mut handle, mut reader, mut writer) = handle(origin.clone());
        let callback = confirm(&f, &handle, &mut reader, &mut writer).await;
        let captured = callback.capture();
        let leaf = with_caller(f.caller(), async {
            captured.source_lifetime().unwrap().retirement()
        })
        .await;
        let (observed, result) = tokio::sync::oneshot::channel();
        let guard = CheckRetirementOnDrop {
            leaf,
            observed: Some(observed),
        };
        let task = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        if action == "drop" || action == "kill" {
            handle.serve_task.abort();
            handle.serve_task = task;
        } else {
            f.manager
                .workers
                .lock()
                .unwrap()
                .insert(f.row.id.clone(), task);
        }
        if action == "drop" {
            drop(handle);
        } else {
            f.manager
                .handles
                .lock()
                .unwrap()
                .insert(f.row.id.clone(), handle);
            match action {
                "detach" => {
                    f.manager.detach(&f.row.id).await;
                }
                "shutdown" => {
                    f.manager.busy.lock().unwrap().insert(f.row.id.clone());
                    f.manager
                        .agent_ws
                        .lock()
                        .unwrap()
                        .insert(f.row.id.clone(), f.row.workspace_id.clone());
                    f.manager.shutdown().await;
                }
                "kill" => {
                    f.manager.make_kill(f.row.id.clone())().await;
                }
                _ => unreachable!(),
            }
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(5), result)
                .await
                .unwrap()
                .unwrap(),
            "{action}"
        );
        assert!(!current(&callback.capture(), f.caller()).await, "{action}");
    }
}

// Permission and repository observations here are explicit fixtures. The
// original ACP-created owner, captured leaf and consumed R dispatch fence are
// real; this checks manager lock ordering without exposing private fence APIs.
struct FenceSource {
    facts: RepositoryOperationFacts,
    authority: RepositoryAuthorityFacts,
}

impl RepositoryAuthoritySource for FenceSource {
    fn read<'a>(
        &'a self,
        _original: &'a OriginalRepositoryCaller,
        _workspace: &'a WorkspaceId,
    ) -> BoxFuture<'a, AdmissionResult<RepositoryAuthorityFacts>> {
        Box::pin(async { Ok(self.authority.clone()) })
    }
}

impl RepositoryOperationSource for FenceSource {
    fn observe<'a>(
        &'a self,
        _original: &'a RepositoryOperationFacts,
    ) -> BoxFuture<'a, AdmissionResult<RepositoryOperationFacts>> {
        Box::pin(async { Ok(self.facts.clone()) })
    }
}

pub(super) async fn held_stage_fence(
    fixture: &Fixture,
    leaf: RepositoryRetirement,
) -> (
    crate::repository_admission::RepositoryDispatchStamp,
    Box<dyn RepositoryAuthorityFence>,
) {
    let json: Value = serde_json::from_str(include_str!(
        "../../../../intent-core/tests/fixtures/native_review_v1.json"
    ))
    .unwrap();
    let mut preparation: NativeReviewPreparation =
        serde_json::from_value(json["prepare"]["reviewPreparation"].clone()).unwrap();
    preparation.root.workspace_id = fixture.row.workspace_id.clone();
    let destinations = vec!["https://git.example:8443/gitlab/team/sub/app.git".to_owned()];
    let request = RepositoryAuthorityRequest {
        execution: preparation.scope.clone(),
        target: preparation.source.repository.clone(),
        connection: preparation.source.connection.clone().unwrap(),
        use_kind: RepositoryCredentialUse::NativePush,
        allowed_transport: RepositoryCredentialTransport::GitHttps(destinations.clone()),
    };
    let facts = RepositoryOperationFacts {
        preparation,
        worktree_path: fixture.dir.path().to_path_buf(),
        git_dir: fixture.dir.path().join("fixture-git"),
        common_dir: fixture.dir.path().join("fixture-git"),
        source_ref: "refs/heads/feature".into(),
        staging_fingerprint: None,
        fetch_destinations: destinations.clone(),
        push_destinations: destinations,
        credential_requests: vec![request],
    };
    let source = Arc::new(FenceSource {
        facts: facts.clone(),
        authority: RepositoryAuthorityFacts {
            caller: fixture.caller(),
            workspace: fixture.row.workspace_id.clone(),
            workspace_exists: true,
            primary_principal_id: None,
            workspace_role: None,
            credential: None,
            provenance: RepositoryAuthorityProvenance::Injected(1),
            internal_stages: vec![NativeReviewStage::Push],
        },
    });
    with_caller(fixture.caller(), async {
        let original = OriginalRepositoryCaller::capture(RepositoryEntry::AgentCallback).unwrap();
        let operation = capture_repository_operation(
            original,
            "manager-lock-order".into(),
            facts,
            vec![NativeReviewStage::Push],
            source,
            leaf,
        )
        .await
        .unwrap();
        let checked = revalidate_repository_stage(&operation, NativeReviewStage::Push)
            .await
            .unwrap();
        let stamp = begin_repository_stage(checked).unwrap();
        let (request, authority) = stamp.credential_authority().unwrap();
        let fence = authority.revalidate(&request).await.unwrap();
        (stamp, fence)
    })
    .await
}

#[tokio::test]
async fn removal_drains_retirement_without_holding_the_handles_map() {
    let f = Fixture::new().await;
    let origin = f.origin().await;
    let (handle, mut reader, mut writer) = handle(origin.clone());
    let callback = confirm(&f, &handle, &mut reader, &mut writer).await;
    let captured = callback.capture();
    let leaf = with_caller(f.caller(), async {
        captured.source_lifetime().unwrap().retirement()
    })
    .await;
    f.manager
        .handles
        .lock()
        .unwrap()
        .insert(f.row.id.clone(), handle);
    let (entered, inside) = std::sync::mpsc::channel();
    let (release, hold) = std::sync::mpsc::channel();
    let (_stage, fence) = held_stage_fence(&f, leaf).await;
    let dispatch = std::thread::spawn(move || {
        fence.dispatch(&mut move || {
            entered.send(()).unwrap();
            hold.recv().unwrap();
            Ok(())
        })
    });
    inside
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let handles = f.manager.handles.clone();
    let id = f.row.id.clone();
    let observed_origin = origin.clone();
    let removed = std::thread::spawn(move || take(&handles, &id, &origin, None));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !observed_origin.state.lock().unwrap().retired {
        assert!(
            std::time::Instant::now() < deadline,
            "removal did not begin retirement"
        );
        std::thread::yield_now();
    }
    // The R fence is held above. The removal must not occupy the map while
    // waiting for that already admitted original stage to finish.
    assert!(f.manager.handles.try_lock().is_ok());
    release.send(()).unwrap();
    assert_eq!(dispatch.join().unwrap(), Ok(()));
    drop(removed.join().unwrap().unwrap());
    assert!(!current(&callback.capture(), f.caller()).await);
}

#[tokio::test]
async fn actual_load_preserves_response_without_rewriting_or_adopting_a_later_binding() {
    for replaced in [false, true] {
        let mut f = Fixture::new().await;
        f.seed_session(Some("original-load"), "claude-code", None)
            .await;
        f.count_writes().await;
        let (origin, reader, writer) = f.mount().await;
        let pending = origin.pending_callback().unwrap();
        let PausedSession {
            peer,
            arrived: entered,
            release,
        } = paused_session(reader, writer, true, "session/load");
        let opening = f.start("claude-code");
        let control = async {
            let request = arrived(entered).await;
            assert_eq!(request["params"]["sessionId"], "original-load");
            if replaced {
                f.manager
                    .services
                    .store
                    .replace_acp_session_id(
                        &f.row.workspace_id,
                        &f.row.id,
                        "original-load",
                        "new-winner",
                    )
                    .await
                    .unwrap();
            }
            release.send(Ok(json!({}))).unwrap();
        };
        let (response, ()) = tokio::join!(opening, control);
        assert_eq!(response.unwrap(), "original-load");
        assert_eq!(f.writes().await, i64::from(replaced));
        assert_eq!(
            f.stored().await.acp_session_id.as_deref(),
            Some(if replaced {
                "new-winner"
            } else {
                "original-load"
            })
        );
        assert_eq!(
            peer.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r["method"] == "session/load")
                .count(),
            1
        );
        assert!(!peer
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|r| r["method"] == "session/new"));
        assert!(!current(&pending.capture(), f.caller()).await);
        if replaced {
            assert!(callback(&origin).is_none());
        } else {
            assert!(current(&callback(&origin).unwrap().capture(), f.caller()).await);
        }
    }
}

#[tokio::test]
async fn actual_recreate_runs_one_legacy_transaction_without_borrowing_a_winner() {
    for scenario in ["replace", "same-id", "cleared", "lost-cas"] {
        let mut f = Fixture::new().await;
        f.seed_session(Some("old-session"), "claude-code", None)
            .await;
        f.usage(100).await;
        f.count_writes().await;
        let (origin, reader, writer) = f.mount().await;
        let PausedSession {
            peer,
            arrived: entered,
            release,
        } = paused_session(reader, writer, false, "session/new");
        let opening = f.start("claude-code");
        let control = async {
            arrived(entered).await;
            if scenario == "cleared" {
                // A real same-ID row recreation is the current-NULL race;
                // the ordinary full-row updater correctly forbids clearing ID.
                let mut row = f.stored().await;
                assert!(f
                    .manager
                    .services
                    .store
                    .delete_agent_session(&f.row.workspace_id, &f.row.id)
                    .await
                    .unwrap());
                row.acp_session_id = None;
                f.manager
                    .services
                    .store
                    .insert_agent_session(&row)
                    .await
                    .unwrap();
                f.usage(100).await;
            } else if scenario == "lost-cas" {
                f.manager
                    .services
                    .store
                    .replace_acp_session_id(&f.row.workspace_id, &f.row.id, "old-session", "winner")
                    .await
                    .unwrap();
                f.usage(200).await;
                f.manager
                    .services
                    .store
                    .set_agent_effort_levels(
                        &f.row.workspace_id,
                        &f.row.id,
                        Some(&["winner-level".into()]),
                        "2026-09-27T21:20:00Z",
                    )
                    .await
                    .unwrap();
            }
            let before = f.writes().await;
            release.send(Ok(json!({"sessionId":if scenario == "same-id" {"old-session"} else {"candidate"}}))).unwrap();
            before
        };
        let (response, before) = tokio::join!(opening, control);
        let expected = match scenario {
            "same-id" => "old-session",
            "lost-cas" => "winner",
            _ => "candidate",
        };
        assert_eq!(response.unwrap(), expected, "{scenario}");
        assert_eq!(f.stored().await.acp_session_id.as_deref(), Some(expected));
        assert_eq!(
            f.writes().await - before,
            i64::from(scenario != "lost-cas"),
            "one original ACP persistence operation: {scenario}"
        );
        let (usage, baseline) = f.accounting().await;
        assert_eq!(baseline.unwrap()["inputTokens"], 100);
        if scenario == "lost-cas" {
            assert_eq!(usage.unwrap()["inputTokens"], 200);
            assert_eq!(
                f.stored().await.effort_levels,
                Some(vec!["winner-level".into()])
            );
        } else {
            assert!(usage.is_none());
        }
        assert_eq!(
            peer.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r["method"] == "session/new")
                .count(),
            1
        );
        if scenario == "replace" {
            assert!(current(&callback(&origin).unwrap().capture(), f.caller()).await);
        } else {
            assert!(
                callback(&origin).is_none(),
                "compatibility effects cannot supply strict ownership: {scenario}"
            );
        }
    }
}

#[tokio::test]
async fn actual_failed_or_cancelled_create_never_persists_or_upgrades_pending_callbacks() {
    for cancel in [false, true] {
        let f = Fixture::new().await;
        f.count_writes().await;
        let (origin, reader, writer) = f.mount().await;
        let pending = origin.pending_callback().unwrap();
        let PausedSession {
            peer,
            arrived: entered,
            release,
        } = paused_session(reader, writer, false, "session/new");
        let mut opening = Box::pin(f.start("claude-code"));
        tokio::select! {
            _ = arrived(entered) => {},
            result = &mut opening => panic!("session completed before controlled reply: {result:?}"),
        }
        if cancel {
            drop(opening);
            release
                .send(Ok(json!({"sessionId":"late-cancelled"})))
                .unwrap();
        } else {
            release
                .send(Err(
                    json!({"code":-32603,"message":"original create failure"}),
                ))
                .unwrap();
            let error = opening.await.unwrap_err().to_string();
            assert!(error.contains("original create failure"));
        }
        assert_eq!(f.writes().await, 0);
        assert!(f.stored().await.acp_session_id.is_none());
        assert!(callback(&origin).is_none());
        assert!(!current(&pending.capture(), f.caller()).await);
        assert_eq!(
            peer.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|r| r["method"] == "session/new")
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn actual_retired_create_keeps_its_successful_result_without_an_owner() {
    let f = Fixture::new().await;
    f.count_writes().await;
    let (origin, reader, writer) = f.mount().await;
    let pending = origin.pending_callback().unwrap();
    let PausedSession {
        peer,
        arrived: entered,
        release,
    } = paused_session(reader, writer, false, "session/new");
    let control = async {
        arrived(entered).await;
        origin.retire();
        release
            .send(Ok(json!({"sessionId":"committed-after-retirement"})))
            .unwrap();
    };
    let (response, ()) = tokio::join!(f.start("claude-code"), control);
    assert_eq!(response.unwrap(), "committed-after-retirement");
    assert_eq!(
        f.stored().await.acp_session_id.as_deref(),
        Some("committed-after-retirement")
    );
    assert_eq!(f.writes().await, 1);
    assert!(callback(&origin).is_none());
    assert!(!current(&pending.capture(), f.caller()).await);
    assert_eq!(
        peer.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r["method"] == "session/new")
            .count(),
        1
    );
}

#[tokio::test]
async fn actual_antigravity_confirms_setup_before_its_single_commit() {
    for scenario in ["replace", "first", "rejected", "lost-cas"] {
        let mut f = Fixture::with_provider("antigravity").await;
        let original = (scenario != "first").then_some("old-session");
        f.seed_session(original, "antigravity", Some("model-a"))
            .await;
        f.count_writes().await;
        let (origin, reader, writer) = f.mount().await;
        let PausedSession {
            peer,
            arrived: entered,
            release,
        } = paused_session(reader, writer, false, "session/set_config_option");
        let control = async {
            let request = arrived(entered).await;
            assert_eq!(request["params"]["sessionId"], "candidate");
            assert_eq!(request["params"]["value"], "model-a");
            assert_eq!(f.writes().await, 0);
            assert_eq!(f.stored().await.acp_session_id.as_deref(), original);
            assert!(callback(&origin).is_none());
            if scenario == "lost-cas" {
                f.manager
                    .services
                    .store
                    .replace_acp_session_id(&f.row.workspace_id, &f.row.id, "old-session", "winner")
                    .await
                    .unwrap();
            }
            if scenario == "rejected" {
                release
                    .send(Err(json!({"code":-32602,"message":"model was rejected"})))
                    .unwrap();
            } else {
                release
                    .send(Ok(session_reply(&request, false, "candidate")))
                    .unwrap();
            }
        };
        let (response, ()) = tokio::join!(f.start("antigravity"), control);
        match scenario {
            "rejected" => {
                assert!(response.is_err());
                assert_eq!(f.writes().await, 0);
                assert_eq!(f.stored().await.acp_session_id.as_deref(), original);
            }
            "lost-cas" => {
                assert!(matches!(response, Err(intent_core::Error::Conflict { .. })));
                assert_eq!(f.writes().await, 1);
                assert_eq!(f.stored().await.acp_session_id.as_deref(), Some("winner"));
            }
            _ => {
                assert_eq!(response.unwrap(), "candidate");
                assert_eq!(f.writes().await, 1);
                assert_eq!(
                    f.stored().await.acp_session_id.as_deref(),
                    Some("candidate")
                );
            }
        }
        let calls = peer.calls.lock().unwrap().clone();
        let methods: Vec<_> = calls.iter().filter_map(|r| r["method"].as_str()).collect();
        assert_eq!(methods.iter().filter(|m| **m == "session/new").count(), 1);
        let new = methods.iter().position(|m| *m == "session/new").unwrap();
        let mode = methods
            .iter()
            .position(|m| *m == "session/set_mode")
            .unwrap();
        let model = methods
            .iter()
            .position(|m| *m == "session/set_config_option")
            .unwrap();
        assert!(new < mode && mode < model);
        if scenario == "replace" {
            assert!(current(&callback(&origin).unwrap().capture(), f.caller()).await);
        } else {
            assert!(callback(&origin).is_none());
        }
    }
}

#[tokio::test]
async fn actual_owner_scope_keeps_source_cleanup_separate_from_preparation_and_cancellation() {
    use crate::repository_admission::request_context::current_source_lifetime;
    use intent_acp::mcp_server::request_context::McpRequestContext;
    use intent_store::RepositoryLifecycleKey;

    let f = Fixture::new().await;
    let (origin, reader, writer) = f.mount().await;
    let _peer = scripted_session(reader, writer, false, "original-source");
    f.start("claude-code").await.unwrap();
    // This fresh fixture callback is distinguishable from the installed pending
    // endpoint. It does not claim that an actual adapter can receive it yet.
    let fresh = callback(&origin).unwrap();
    let scope = McpRequestContext::capture(&fresh);
    let mut original_response = None;
    with_caller(
        f.caller(),
        scope.scope(Box::pin(async {
            let source = current_source_lifetime().unwrap();
            let cleanup = source
                .subscribe(
                    &f.manager.services.store,
                    &f.caller(),
                    &[
                        RepositoryLifecycleKey::Database,
                        RepositoryLifecycleKey::Agent(f.row.id.clone()),
                    ],
                )
                .unwrap();
            let child = source.retirement();
            assert!(child.check_current().is_ok());
            original_response = Some(json!({"operation":"original","sha":"remote-A"}));
            drop(cleanup);
            assert_eq!(child.check_current(), Err(AdmissionError::Retired));
        })),
    )
    .await;
    let mut preparation = false;
    with_caller(
        f.caller(),
        scope.scope(Box::pin(async {
            preparation = current_source_lifetime().is_ok();
        })),
    )
    .await;
    assert!(
        preparation,
        "normal source cleanup must keep the SAME request usable"
    );
    assert_eq!(
        original_response,
        Some(json!({"operation":"original","sha":"remote-A"}))
    );
    let cancelled = McpRequestContext::capture(&fresh);
    drop(cancelled.scope(Box::pin(std::future::pending())));
    with_caller(
        f.caller(),
        cancelled.scope(Box::pin(async {
            assert!(current_source_lifetime().is_err());
        })),
    )
    .await;
    assert!(current(&fresh.capture(), f.caller()).await);
    origin.interrupt_requests();
    let mut ordinary_runs = 0;
    with_caller(
        f.caller(),
        scope.scope(Box::pin(async {
            ordinary_runs += 1;
            assert!(current_source_lifetime().is_err());
        })),
    )
    .await;
    assert_eq!(ordinary_runs, 1);
    let queued = McpRequestContext::capture(&fresh);
    f.manager
        .services
        .store
        .set_agent_session_model(
            &f.row.workspace_id,
            &f.row.id,
            "replacement-model",
            Some("claude-code"),
            "2026-09-27T21:20:00Z",
        )
        .await
        .unwrap();
    with_caller(
        f.caller(),
        queued.scope(Box::pin(async {
            assert!(current_source_lifetime().is_err());
        })),
    )
    .await;
    assert!(!current(&fresh.capture(), f.caller()).await);
}

async fn metadata_completion_ordering(replace_handle: bool, install_before_original: bool) {
    let f = Fixture::new().await;
    f.manager
        .services
        .store
        .set_agent_effort_levels(
            &f.row.workspace_id,
            &f.row.id,
            Some(&["previous".into()]),
            "2026-09-27T21:30:00Z",
        )
        .await
        .unwrap();
    let store = &f.manager.services.store;
    // Only actual post-persistence metadata updates fire these markers. The
    // original ACP-ID transaction has committed before either can be reached.
    for sql in [
        "CREATE TABLE observed_model_metadata (id TEXT)",
        "CREATE TABLE observed_effort_metadata (id TEXT)",
        "CREATE TABLE observed_acp_writes (value TEXT)",
        "CREATE TRIGGER pause_model_metadata AFTER UPDATE OF resolved_model ON agent_session BEGIN INSERT INTO observed_model_metadata VALUES (NEW.id); END",
        "CREATE TRIGGER pause_effort_metadata AFTER UPDATE OF effort_levels ON agent_session BEGIN INSERT INTO observed_effort_metadata VALUES (NEW.id); END",
        "CREATE TRIGGER observe_acp_write AFTER UPDATE OF acp_session_id ON agent_session BEGIN INSERT INTO observed_acp_writes VALUES (NEW.acp_session_id); END",
    ] {
        sqlx::query(sql).execute(store.write_pool()).await.unwrap();
    }
    let (model_entered, model_wait) = tokio::sync::oneshot::channel();
    let (model_release, model_resume) = std::sync::mpsc::channel();
    let (effort_entered, effort_wait) = tokio::sync::oneshot::channel();
    let (effort_release, effort_resume) = std::sync::mpsc::channel();
    let mut model_pause = Some((model_entered, model_resume));
    let mut effort_pause = Some((effort_entered, effort_resume));
    let mut writer = store.write_pool().acquire().await.unwrap();
    writer
        .lock_handle()
        .await
        .unwrap()
        .set_update_hook(move |change| {
            let gate = match change.table {
                "observed_model_metadata" => model_pause.take(),
                "observed_effort_metadata" => effort_pause.take(),
                _ => None,
            };
            if let Some((entered, resume)) = gate {
                let _ = entered.send(());
                // Never panic across SQLite's callback; test-side timeouts report
                // a failed ordering and dropping the sender releases this worker.
                let _ = resume.recv_timeout(std::time::Duration::from_secs(10));
            }
        });
    drop(writer);
    let (original, reader, writer) = f.mount().await;
    let pending_capture = original.pending_callback().unwrap().capture();
    let mut creations = 0;
    let original_peer = scripted_replies(reader, writer, move |request| {
        if request["method"] == "session/new" {
            creations += 1;
        }
        let id = if creations <= 1 {
            "original-A"
        } else {
            "replacement-B"
        };
        Box::pin(async move { Ok(session_reply(&request, false, id)) })
    });
    let original_connection = f.manager.handles.lock().unwrap()[&f.row.id]
        .connection
        .clone();
    let control = async {
        tokio::time::timeout(std::time::Duration::from_secs(10), model_wait)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            f.stored().await.acp_session_id.as_deref(),
            Some("original-A")
        );
        assert!(
            callback(&original).is_none(),
            "owner is still inside the metadata-awaiting result"
        );
        let (replacement, connection, peer) = if replace_handle {
            let row = f.stored().await;
            let removed = take(
                &f.manager.handles,
                &f.row.id,
                &original,
                Some(&f.manager.registry),
            )
            .unwrap();
            drop(removed);
            let replacement = RepositoryOrigin::allocate(&f.manager.services, &row).await;
            let (handle, reader, writer) = handle(replacement.clone());
            let connection = handle.connection.clone();
            let peer = scripted_session(reader, writer, false, "replacement-B");
            f.manager
                .handles
                .lock()
                .unwrap()
                .insert(f.row.id.clone(), handle);
            (replacement, connection, Some(peer))
        } else {
            (original.clone(), original_connection, None)
        };
        let (creation, attempt) = replacement
            .begin_session(RepositoryCreationIntent::Replace {
                expected: Some("original-A".into()),
            })
            .unwrap();
        let (producer_done, observed_producer) = tokio::sync::oneshot::channel();
        let mut initializing = Box::pin(creation.initialize_compatible(|| async {
            let response =
                intent_acp::session::new_session(&connection, f.dir.path(), vec![], None).await?;
            producer_done.send(()).unwrap();
            Ok::<_, intent_acp::AcpError>((response.session_id.0.to_string(), response))
        }));
        tokio::select! {
            biased;
            result = &mut initializing => panic!("replacement persisted while metadata holds the sole writer: {:?}", result.producer),
            completed = observed_producer => completed.unwrap(),
        }
        // The second original Store transaction is now queued for the sole
        // writer ahead of the first completion's next effort metadata write.
        // These are actual ACP frames and the original compatible transaction,
        // not an injected successful owner or a second persistence operation.
        model_release.send(()).unwrap();
        let outcome = initializing.await;
        assert_eq!(
            outcome.producer.unwrap().session_id.0.as_ref(),
            "replacement-B"
        );
        assert_eq!(
            crate::agent_session::compatibility_session_id(outcome.result.unwrap()),
            "replacement-B"
        );
        let owner = outcome.owner.unwrap();
        let fresh = owner.callback();
        let pending_install = if install_before_original {
            assert!(replacement.install(attempt, owner));
            None
        } else {
            Some((attempt, owner))
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), effort_wait)
            .await
            .unwrap()
            .unwrap();
        assert!(current(&fresh.capture(), f.caller()).await);
        effort_release.send(()).unwrap();
        (replacement, fresh, peer, pending_install)
    };
    let (result, (replacement, fresh, replacement_peer, pending_install)) =
        tokio::join!(f.start("claude-code"), control);
    assert_eq!(
        result.unwrap(),
        "original-A",
        "the completed producer keeps its own result"
    );
    assert_eq!(
        f.stored().await.acp_session_id.as_deref(),
        Some("replacement-B")
    );
    assert!(is_current(&f.manager.handles, &f.row.id, &replacement));
    assert!(
        current(&fresh.capture(), f.caller()).await,
        "late original completion must not retire the installed replacement owner"
    );
    if let Some((attempt, owner)) = pending_install {
        assert!(
            callback(&replacement).is_none(),
            "obsolete completion must be rejected before the new owner is installed"
        );
        assert!(replacement.install(attempt, owner));
    }
    assert!(current(&callback(&replacement).unwrap().capture(), f.caller()).await);
    assert!(!current(&pending_capture, f.caller()).await);
    let committed: Vec<String> =
        sqlx::query_scalar("SELECT value FROM observed_acp_writes ORDER BY rowid")
            .fetch_all(store.read_pool())
            .await
            .unwrap();
    assert_eq!(committed, ["original-A", "replacement-B"]);
    let count_new = |peer: &ScriptedSession| {
        peer.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request["method"] == "session/new")
            .count()
    };
    assert_eq!(
        count_new(&original_peer) + replacement_peer.as_ref().map_or(0, count_new),
        2,
        "each original producer runs once"
    );
    let mut writer = store.write_pool().acquire().await.unwrap();
    writer.lock_handle().await.unwrap().remove_update_hook();
}

#[tokio::test]
async fn same_handle_late_metadata_completion_preserves_new_session_owner() {
    metadata_completion_ordering(false, true).await;
}

#[tokio::test]
async fn replaced_handle_late_metadata_completion_preserves_new_physical_owner() {
    metadata_completion_ordering(true, true).await;
}

#[tokio::test]
async fn same_handle_obsolete_metadata_completion_cannot_install_before_new_owner() {
    metadata_completion_ordering(false, false).await;
}
