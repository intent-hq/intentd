//! A background hook evaluates its script under `Caller::Daemon`, but every
//! `ws.*` frame it makes acts for the hook's owning agent: the host built by
//! [`make_workspace_host_for_bridge`] binds `Caller::Agent { owner }` around
//! each frame, so agent-only service checks (the last-top-level retire guard,
//! self-queue redaction, repository admission, …) hold from a hook exactly as
//! they do in a live `workspace_api` turn. No per-method allowlist: the
//! method list below is read from the live preludes, so a new binding is
//! covered without editing this test.

use super::*;
use intent_core::{BoxFuture, Caller};
use std::collections::BTreeSet;

/// Records the ambient caller at the service calls every frame makes before
/// its binding parses arguments: the dispatch's retired-caller read (inline,
/// or first step of the spawned send for `agent.send` / `agent.sendToTask`)
/// and the retired-workspace read.
#[derive(Default)]
struct ProbeApi {
    seen: Mutex<Vec<Option<Caller>>>,
}

impl ProbeApi {
    fn record(&self) {
        self.seen
            .lock()
            .unwrap()
            .push(intent_core::current_caller());
    }

    fn take(&self) -> Vec<Option<Caller>> {
        std::mem::take(&mut *self.seen.lock().unwrap())
    }
}

impl WorkspaceApi for ProbeApi {
    fn agent_is_retired(&self, _agent_id: AgentId) -> BoxFuture<'_, bool> {
        self.record();
        Box::pin(async { false })
    }

    fn retired_workspace_for(
        &self,
        _workspace_id: Option<WorkspaceId>,
        _agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, intent_core::Result<Option<WorkspaceId>>> {
        self.record();
        Box::pin(async { Ok(None) })
    }
}

/// Every feature on (including the default-off `secretRequests`) so every
/// binding's installer is in the prelude.
fn all_features() -> AgentFeaturesSettings {
    AgentFeaturesSettings {
        secret_requests: true,
        ..AgentFeaturesSettings::default()
    }
}

/// Every `host({ method })` name a hook's prelude can call, with the
/// workspace that installs it (the `app.*` chief-only bindings need the
/// chief workspace).
fn every_binding_method() -> Vec<(String, WorkspaceId)> {
    let features = all_features();
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for ws in [WorkspaceId::from("ws-hook"), WorkspaceId::chief()] {
        let prelude = super::super::bindings::prelude_for_workspace(&features, false, &ws, true);
        for chunk in prelude.split("method: '").skip(1) {
            let method = chunk.split('\'').next().unwrap_or_default().to_string();
            if seen.insert(method.clone()) {
                out.push((method, ws.clone()));
            }
        }
    }
    out
}

#[tokio::test]
async fn every_hook_frame_reaches_the_service_as_the_owning_agent() {
    let probe = Arc::new(ProbeApi::default());
    let api: Arc<dyn WorkspaceApi> = probe.clone();
    let owner = AgentId::from("agent-hook-owner");
    let expected = Some(Caller::Agent {
        agent_id: owner.clone(),
    });
    let methods = every_binding_method();
    assert!(
        methods.len() > 100,
        "prelude scan found only {} methods",
        methods.len()
    );
    // Enough for the send bindings to reach their spawned retired-caller
    // read; every other binding is probed before it reads its arguments.
    let args = json!({ "agentId": "agent-other", "taskNoteId": "task-1", "message": "m" });
    let mut unobserved = Vec::new();
    let mut wrong = Vec::new();
    for (method, ws) in &methods {
        let host = make_workspace_host_for_bridge(
            api.clone(),
            ws.clone(),
            owner.clone(),
            None,
            all_features(),
            false,
            Duration::from_secs(5),
        );
        let frame = json!({ "method": method, "args": args });
        let _ = intent_core::with_caller(Caller::Daemon, host(frame)).await;
        let seen = probe.take();
        if seen.is_empty() {
            unobserved.push(method.clone());
        } else if seen.iter().any(|caller| caller != &expected) {
            wrong.push(format!("{method}: {seen:?}"));
        }
    }
    assert!(
        wrong.is_empty(),
        "hook frames reached the service without the owner's identity:\n{}",
        wrong.join("\n")
    );
    assert!(
        unobserved.is_empty(),
        "no service call observed for: {unobserved:?}"
    );
}

/// The binding only ever sees the owner: a frame dispatched with NO ambient
/// caller (an unbound spawn) still acts as the owner rather than being
/// refused as unbound or, worse, inheriting someone else's identity.
#[tokio::test]
async fn owner_binding_does_not_depend_on_the_enclosing_scope() {
    let probe = Arc::new(ProbeApi::default());
    let api: Arc<dyn WorkspaceApi> = probe.clone();
    let owner = AgentId::from("agent-hook-owner");
    let host = make_workspace_host_for_bridge(
        api,
        WorkspaceId::from("ws-hook"),
        owner.clone(),
        None,
        all_features(),
        false,
        Duration::from_secs(5),
    );
    let frame = json!({ "method": "note.list", "args": {} });
    let _ = host(frame.clone()).await;
    let other = Caller::Agent {
        agent_id: AgentId::from("agent-someone-else"),
    };
    let _ = intent_core::with_caller(other, host(frame)).await;
    let seen = probe.take();
    let expected = Some(Caller::Agent { agent_id: owner });
    assert!(!seen.is_empty());
    assert!(seen.iter().all(|caller| caller == &expected), "{seen:?}");
}
