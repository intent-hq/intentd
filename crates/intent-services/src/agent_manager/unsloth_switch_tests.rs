//! The managed Unsloth server holds one model. A user send that would make it
//! load another model while other agents still use it is refused until the
//! user confirms (PROTOCOL §5.5, `unsloth-model-switch-confirmation-required`).

use super::dead_child_respawn_tests::install_fake_handle;
use super::role_reminder_tests::{session, workspace};
use super::*;
use crate::unsloth_switch::CONFIRM_METADATA_KEY;
use intent_core::{MessageOrigin, WorkspaceApi};

const MODEL_A: &str = "unsloth/Qwen3-4B-GGUF";
const MODEL_B: &str = "unsloth/gemma-3-27b-it-GGUF";

struct Fixture {
    mgr: Arc<AgentManager>,
    ws: WorkspaceId,
    _db: tempfile::TempDir,
    pipes: Vec<(tokio::io::DuplexStream, tokio::io::DuplexStream)>,
}

impl Fixture {
    async fn new() -> Self {
        let (mgr, _agent, db) = super::role_reminder_tests::manager_with(None, None).await;
        let mgr = Arc::new(mgr);
        mgr.services.attach_agent_manager(&mgr);
        let ws = WorkspaceId::from("ws-unsloth");
        let mut row = workspace(&ws);
        row.title = "Dark mode".to_string();
        mgr.services.store.insert_workspace(&row).await.unwrap();
        Self {
            mgr,
            ws,
            _db: db,
            pipes: Vec::new(),
        }
    }

    async fn agent(&self, id: &str, name: &str, model: &str) -> AgentId {
        let agent = AgentId::from(id);
        let mut s = session(&agent, &self.ws, None);
        s.name = name.to_string();
        s.provider = Some("unsloth".to_string());
        s.model = Some(model.to_string());
        self.mgr
            .services
            .store
            .insert_agent_session(&s)
            .await
            .unwrap();
        agent
    }

    /// Mark `agent` as running on the server, with the identity a real
    /// unsloth spawn records: the `opencode` command under the `unsloth`
    /// selection.
    fn live(&mut self, agent: &AgentId, model: &str) {
        self.pipes.push(install_fake_handle(&self.mgr, agent, None));
        let mut handles = self.mgr.handles.lock().unwrap();
        let handle = handles.get_mut(agent).expect("handle");
        handle.spawned_provider = "opencode".to_string();
        handle.spawned_selection = "unsloth".to_string();
        handle.spawned_model = Some(model.to_string());
    }

    async fn admit(
        &self,
        agent: &AgentId,
        origin: MessageOrigin,
        metadata: Option<Value>,
    ) -> Result<Option<Value>> {
        self.mgr
            .services
            .admit_unsloth_model_switch(agent, origin, metadata)
            .await
    }
}

fn refused(result: Result<Option<Value>>) -> Value {
    let err = result.expect_err("switch must need confirmation");
    let Error::ConfirmationRequired(failure) = &err else {
        panic!("expected a confirmation refusal, got {err:?}");
    };
    assert_eq!(err.code(), -32602);
    assert_eq!(
        failure.data["code"],
        "unsloth-model-switch-confirmation-required"
    );
    failure.data.clone()
}

#[tokio::test]
async fn unsloth_agents_are_counted_by_selection_not_command() {
    let mut fx = Fixture::new().await;
    let a = fx.agent("agent-a", "Planner", MODEL_A).await;
    fx.live(&a, MODEL_A);
    let plain = AgentId::from("agent-opencode");
    fx.pipes.push(install_fake_handle(&fx.mgr, &plain, None));
    {
        let mut handles = fx.mgr.handles.lock().unwrap();
        let handle = handles.get_mut(&plain).unwrap();
        handle.spawned_provider = "opencode".to_string();
        handle.spawned_selection = "opencode".to_string();
    }
    assert_eq!(fx.mgr.count_agents_with_provider("unsloth"), 1);
    assert_eq!(fx.mgr.count_agents_with_provider("opencode"), 1);
}

#[tokio::test]
async fn a_switch_that_would_unload_other_agents_lists_them() {
    let mut fx = Fixture::new().await;
    let a = fx.agent("agent-a", "Planner", MODEL_A).await;
    let b = fx.agent("agent-b", "Reviewer", MODEL_B).await;
    fx.live(&a, MODEL_A);
    fx.mgr.unsloth_manager().set_owned_server_for_test(MODEL_A);

    let data = refused(fx.admit(&b, MessageOrigin::User, None).await);
    assert_eq!(data["currentModel"], MODEL_A);
    assert_eq!(data["requestedModel"], MODEL_B);
    assert_eq!(
        data["agents"],
        json!([{
            "agentId": "agent-a",
            "agentName": "Planner",
            "workspaceId": "ws-unsloth",
            "workspaceTitle": "Dark mode",
        }])
    );
}

#[tokio::test]
async fn the_wire_send_is_refused_before_anything_persists() {
    let mut fx = Fixture::new().await;
    let a = fx.agent("agent-a", "Planner", MODEL_A).await;
    let b = fx.agent("agent-b", "Reviewer", MODEL_B).await;
    fx.live(&a, MODEL_A);
    fx.mgr.unsloth_manager().set_owned_server_for_test(MODEL_A);

    let err = intent_core::with_caller(
        intent_core::Caller::Daemon,
        fx.mgr.services.agent_send_message(
            fx.ws.clone(),
            b.clone(),
            "hello".into(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            MessageOrigin::User,
        ),
    )
    .await
    .expect_err("refused");
    assert!(matches!(err, Error::ConfirmationRequired(_)), "{err:?}");
    let transcript = fx
        .mgr
        .services
        .store
        .get_agent_messages(&b, None)
        .await
        .unwrap();
    assert!(transcript.is_empty(), "nothing persisted: {transcript:?}");
}

#[tokio::test]
async fn a_confirmed_switch_passes_and_the_flag_is_not_persisted() {
    let mut fx = Fixture::new().await;
    let a = fx.agent("agent-a", "Planner", MODEL_A).await;
    let b = fx.agent("agent-b", "Reviewer", MODEL_B).await;
    fx.live(&a, MODEL_A);
    fx.mgr.unsloth_manager().set_owned_server_for_test(MODEL_A);

    let metadata = json!({ "source": "chat", CONFIRM_METADATA_KEY: true });
    let admitted = fx
        .admit(&b, MessageOrigin::User, Some(metadata))
        .await
        .unwrap();
    assert_eq!(admitted, Some(json!({ "source": "chat" })));

    let only_flag = json!({ CONFIRM_METADATA_KEY: true });
    assert_eq!(
        fx.admit(&b, MessageOrigin::User, Some(only_flag))
            .await
            .unwrap(),
        None
    );
}

#[tokio::test]
async fn sends_that_unload_no_one_pass() {
    let mut fx = Fixture::new().await;
    let a = fx.agent("agent-a", "Planner", MODEL_A).await;
    let b = fx.agent("agent-b", "Reviewer", MODEL_B).await;
    let same = fx.agent("agent-same", "Writer", MODEL_A).await;

    // No server running.
    assert!(fx.admit(&b, MessageOrigin::User, None).await.is_ok());

    fx.mgr.unsloth_manager().set_owned_server_for_test(MODEL_A);
    // Server running, but no other agent uses it.
    assert!(fx.admit(&b, MessageOrigin::User, None).await.is_ok());

    fx.live(&a, MODEL_A);
    // Same model as the server.
    assert!(fx.admit(&same, MessageOrigin::User, None).await.is_ok());
    // The only attached agent is the sender itself.
    assert!(fx.admit(&a, MessageOrigin::User, None).await.is_ok());
    // Automatic deliveries keep the daemon's restart warning instead.
    assert!(fx.admit(&b, MessageOrigin::Automatic, None).await.is_ok());

    // A server Intent adopted is never stopped by a switch.
    fx.mgr
        .unsloth_manager()
        .set_adopted_server_for_test(MODEL_A);
    assert!(fx.admit(&b, MessageOrigin::User, None).await.is_ok());
}

#[tokio::test]
async fn a_live_agent_on_another_model_still_needs_confirmation() {
    let mut fx = Fixture::new().await;
    let a = fx.agent("agent-a", "Planner", MODEL_A).await;
    let b = fx.agent("agent-b", "Reviewer", MODEL_B).await;
    fx.live(&a, MODEL_A);
    // A live process does not prove the next turn reuses it (a model,
    // policy or recreation change respawns it), so the gate never exempts it.
    fx.live(&b, MODEL_B);
    fx.mgr.unsloth_manager().set_owned_server_for_test(MODEL_A);
    let data = refused(fx.admit(&b, MessageOrigin::User, None).await);
    assert_eq!(data["requestedModel"], MODEL_B);

    // After `agent.setModel` the next turn respawns, so it switches.
    fx.mgr
        .services
        .store
        .set_agent_session_model(
            &fx.ws,
            &b,
            "unsloth/Llama-3.2-3B-GGUF",
            Some("unsloth"),
            &now_iso(),
        )
        .await
        .unwrap();
    let data = refused(fx.admit(&b, MessageOrigin::User, None).await);
    assert_eq!(data["requestedModel"], "unsloth/Llama-3.2-3B-GGUF");
}

#[tokio::test]
async fn agents_already_off_the_served_model_are_not_listed() {
    let mut fx = Fixture::new().await;
    let a = fx.agent("agent-a", "Planner", MODEL_A).await;
    let stale = fx.agent("agent-stale", "Writer", MODEL_B).await;
    let b = fx.agent("agent-b", "Reviewer", MODEL_B).await;
    fx.live(&a, MODEL_A);
    // `stale` still runs a process spawned on MODEL_B from before the server
    // switched to MODEL_A: switching back loses it nothing.
    fx.live(&stale, MODEL_B);
    fx.mgr.unsloth_manager().set_owned_server_for_test(MODEL_A);

    let data = refused(fx.admit(&b, MessageOrigin::User, None).await);
    let listed: Vec<&str> = data["agents"]
        .as_array()
        .unwrap()
        .iter()
        .map(|agent| agent["agentId"].as_str().unwrap())
        .collect();
    assert_eq!(listed, ["agent-a"]);
}

#[tokio::test]
async fn a_remote_agent_never_uses_the_local_server() {
    let mut fx = Fixture::new().await;
    let a = fx.agent("agent-a", "Planner", MODEL_A).await;
    let remote = fx.agent("agent-remote", "Reviewer", MODEL_B).await;
    fx.live(&a, MODEL_A);
    fx.mgr.unsloth_manager().set_owned_server_for_test(MODEL_A);

    let placement = json!({
        "nodeId": "node-1",
        "leaseId": "lease-1",
        "nodeName": "Mac Studio",
        "placement": {},
        "effectiveIsolation": "pending",
        "baseCommit": "abc123",
    });
    let key = intent_core::remote_launch::REMOTE_PLACEMENT_METADATA_KEY;
    fx.mgr
        .services
        .store
        .set_agent_session_metadata_key_json(
            &fx.ws,
            &remote,
            key,
            &placement.to_string(),
            &now_iso(),
        )
        .await
        .unwrap();
    let stored = fx
        .mgr
        .services
        .store
        .get_agent_session_summary(&remote)
        .await
        .unwrap();
    assert!(
        intent_core::remote_launch::RemotePlacementView::from_metadata(stored.metadata.as_ref())
            .is_some(),
        "fixture must read back as a remote placement"
    );

    assert!(fx.admit(&remote, MessageOrigin::User, None).await.is_ok());
}

#[tokio::test]
async fn a_dead_process_on_its_model_still_needs_confirmation() {
    let mut fx = Fixture::new().await;
    let a = fx.agent("agent-a", "Planner", MODEL_A).await;
    let b = fx.agent("agent-b", "Reviewer", MODEL_B).await;
    fx.live(&a, MODEL_A);
    fx.mgr.unsloth_manager().set_owned_server_for_test(MODEL_A);

    // `b` has a handle on MODEL_B whose child already exited: its next turn
    // respawns and asks the server for MODEL_B.
    let mut child = tokio::process::Command::new("true").spawn().unwrap();
    child.wait().await.unwrap();
    fx.pipes.push(install_fake_handle(&fx.mgr, &b, Some(child)));
    {
        let mut handles = fx.mgr.handles.lock().unwrap();
        let handle = handles.get_mut(&b).unwrap();
        handle.spawned_selection = "unsloth".to_string();
        handle.spawned_model = Some(MODEL_B.to_string());
    }

    let data = refused(fx.admit(&b, MessageOrigin::User, None).await);
    assert_eq!(data["requestedModel"], MODEL_B);
}
