//! Desktop authority is volatile and bound to one admitted execution connection.
//! Durable consent and terminal reports never restore execution after restart.
use super::Services;
use base64::Engine;
use intent_core::desktop::{
    DesktopConnection, DesktopError, DesktopResult, DesktopState, PENDING_HINT, RELEASE_HINT,
    STOP_HINT,
};
use intent_core::events::{
    DESKTOP_PERMISSION_CHANGED, DESKTOP_PERMISSION_REQUESTED, DESKTOP_PERMISSION_RESOLVED,
    DESKTOP_SESSION_CHANGED,
};
use intent_core::{AgentId, Caller, PrincipalId, WorkspaceId};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Default)]
pub(crate) struct Runtime {
    live: Mutex<HashMap<AgentId, Live>>,
    candidates: Mutex<HashMap<String, Vec<Binding>>>,
    original_candidates: Mutex<HashMap<String, Vec<Binding>>>,
    assignment_generations: Mutex<HashMap<WorkspaceId, String>>,
    revoked: Mutex<std::collections::HashSet<String>>,
    outbox_gate: tokio::sync::Mutex<()>,
    pub(crate) changed: tokio::sync::Notify,
    watchers: Mutex<HashMap<AgentId, String>>,
    gates: Mutex<HashMap<AgentId, Arc<tokio::sync::Mutex<()>>>>,
    #[cfg(test)]
    decision_barrier: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
    #[cfg(test)]
    action_barrier: Mutex<Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Binding {
    workspace_id: WorkspaceId,
    agent_id: AgentId,
    agent_name: String,
    #[serde(flatten)]
    connection: DesktopConnection,
    computer_id: String,
    computer_name: String,
}
#[derive(Clone)]
struct Live {
    binding: Binding,
    phase: Phase,
}
#[derive(Clone)]
enum Phase {
    Pending {
        request_id: String,
        expires: Instant,
        expires_at: String,
        accepted: bool,
        claim_generation: Option<String>,
    },
    Active {
        session_id: String,
        sequence: u64,
    },
}
impl Live {
    fn key(&self) -> &str {
        match &self.phase {
            Phase::Pending { request_id, .. } => request_id,
            Phase::Active { session_id, .. } => session_id,
        }
    }

    fn state(&self) -> DesktopState {
        match &self.phase {
            Phase::Pending {
                request_id,
                claim_generation,
                ..
            } => DesktopState::PendingPermission {
                request_id: request_id.clone(),
                computer_name: claim_generation
                    .is_none()
                    .then(|| self.binding.computer_name.clone()),
            },
            Phase::Active { session_id, .. } => DesktopState::Active {
                session_id: session_id.clone(),
                computer_name: self.binding.computer_name.clone(),
                hint: RELEASE_HINT.into(),
            },
        }
    }
    fn pending(&self) -> Option<Value> {
        match &self.phase {
            Phase::Pending {
                request_id,
                expires_at,
                claim_generation,
                ..
            } => Some(
                json!({"requestId":request_id,"workspaceId":self.binding.workspace_id,"agentId":self.binding.agent_id,"agentName":self.binding.agent_name,"computerId":self.binding.computer_id,"computerName":self.binding.computer_name,"expiresAt":expires_at,"claimsPrimary":claim_generation.is_some(),"options":[{"id":"allow_once","label":"Allow once"},{"id":"allow_future","label":"Allow future sessions for this agent"},{"id":"deny","label":"Deny"}]}),
            ),
            Phase::Active { .. } => None,
        }
    }
}
fn error(code: &str, detail: &str) -> DesktopError {
    DesktopError::new(code, detail)
}
fn id() -> String {
    uuid::Uuid::new_v4().to_string()
}
fn hash(token: &str) -> String {
    use std::fmt::Write;
    Sha256::digest(token.as_bytes())
        .iter()
        .fold(String::new(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}
fn value<T: Serialize>(data: &T) -> Value {
    serde_json::to_value(data).expect("desktop serializable types")
}
fn string<'a>(args: &'a Value, key: &str) -> DesktopResult<&'a str> {
    args[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| error("invalid-params", "Missing desktop string argument"))
}
impl Runtime {
    fn assignment_generation(&self, workspace: &WorkspaceId) -> String {
        self.assignment_generations
            .lock()
            .expect("desktop assignment generations")
            .entry(workspace.clone())
            .or_insert_with(id)
            .clone()
    }
    pub(crate) fn assignment_changed(&self, workspace: &WorkspaceId) {
        self.assignment_generations
            .lock()
            .expect("desktop assignment generations")
            .insert(workspace.clone(), id());
        self.changed.notify_waiters();
    }
    pub(crate) fn active_client(&self, workspace: &WorkspaceId) -> Option<intent_core::ClientId> {
        self.live
            .lock()
            .expect("desktop state")
            .values()
            .find(|live| {
                &live.binding.workspace_id == workspace
                    && matches!(live.phase, Phase::Active { .. })
            })
            .map(|live| live.binding.connection.client_id.clone())
    }

    fn candidates(&self, request: &str) -> Vec<Binding> {
        self.candidates
            .lock()
            .expect("desktop candidates")
            .get(request)
            .cloned()
            .unwrap_or_default()
    }
    /// Commit against the current request and cohort together. Invalidation
    /// can remove them while a responder awaits storage; never insert a stale
    /// snapshot back into either map. False is a nonterminal candidate denial.
    fn commit_decision(
        &self,
        live: &mut Live,
        request: &str,
        decision: &str,
    ) -> DesktopResult<bool> {
        let stale = || {
            error(
                "desktop-stale-request",
                "Desktop request is stale or already answered",
            )
        };
        let mut states = self.live.lock().expect("desktop states");
        let current = states.get_mut(&live.binding.agent_id).ok_or_else(stale)?;
        match &current.phase {
            Phase::Pending {
                request_id,
                expires,
                accepted: false,
                claim_generation,
                ..
            } if request_id == request
                && *expires > Instant::now()
                && claim_generation.as_ref().is_none_or(|generation| {
                    generation == &self.assignment_generation(&live.binding.workspace_id)
                }) => {}
            _ => return Err(stale()),
        }
        let mut cohorts = self.candidates.lock().expect("desktop candidates");
        let candidates = cohorts.get_mut(request).ok_or_else(stale)?;
        if !candidates
            .iter()
            .any(|candidate| candidate.connection == live.binding.connection)
        {
            return Err(stale());
        }
        if decision == "deny" && candidates.len() > 1 {
            candidates.retain(|candidate| candidate.connection != live.binding.connection);
            current.binding = candidates[0].clone();
            return Ok(false);
        }
        current.binding = live.binding.clone();
        if let Phase::Pending { accepted, .. } = &mut current.phase {
            *accepted = true;
        }
        *live = current.clone();
        if decision == "deny" {
            states.remove(&live.binding.agent_id);
        }
        Ok(true)
    }
    fn cancel_decision(&self, agent: &AgentId, request: &str) {
        if let Some(current) = self.live.lock().expect("desktop states").get_mut(agent) {
            if let Phase::Pending {
                request_id,
                accepted,
                ..
            } = &mut current.phase
            {
                if request_id == request {
                    *accepted = false;
                }
            }
        }
    }
    fn get(&self, agent: &AgentId) -> Option<Live> {
        self.live.lock().expect("desktop state").get(agent).cloned()
    }
    /// Reserve only on the current session. Stop can remove authority while
    /// the caller awaits validation; a cloned Live must never restore it.
    fn reserve_command(&self, observed: &Live) -> DesktopResult<(String, u64)> {
        let inactive = || error("desktop-not-active", "Desktop control is not active");
        let Phase::Active {
            session_id: expected,
            ..
        } = &observed.phase
        else {
            return Err(inactive());
        };
        // Match revocation's lock order so removal and reservation are atomic.
        let revoked = self.revoked.lock().expect("desktop revocations");
        if revoked.contains(expected) {
            return Err(inactive());
        }
        let mut states = self.live.lock().expect("desktop state");
        let current = states
            .get_mut(&observed.binding.agent_id)
            .ok_or_else(inactive)?;
        if current.binding.connection != observed.binding.connection {
            return Err(inactive());
        }
        let Phase::Active {
            session_id,
            sequence,
        } = &mut current.phase
        else {
            return Err(inactive());
        };
        if session_id != expected {
            return Err(inactive());
        }
        if *sequence >= 9_007_199_254_740_991 {
            return Err(error(
                "desktop-stale-command",
                "Desktop sequence exhausted; end control",
            ));
        }
        *sequence += 1;
        Ok((session_id.clone(), *sequence))
    }
    fn put(&self, live: Live) {
        if matches!(live.phase, Phase::Active { .. }) {
            self.assignment_changed(&live.binding.workspace_id);
        }
        self.live
            .lock()
            .expect("desktop state")
            .insert(live.binding.agent_id.clone(), live);
    }
    fn remove(&self, agent: &AgentId) -> Option<Live> {
        let live = self.live.lock().expect("desktop state").remove(agent);
        if let Some(live) = &live {
            if matches!(live.phase, Phase::Active { .. }) {
                self.assignment_changed(&live.binding.workspace_id);
            }
        }
        live
    }
    fn gate(&self, agent: &AgentId) -> Arc<tokio::sync::Mutex<()>> {
        self.gates
            .lock()
            .expect("desktop gates")
            .entry(agent.clone())
            .or_default()
            .clone()
    }
    pub(crate) fn state(&self, agent: &AgentId) -> DesktopState {
        self.get(agent)
            .map_or(DesktopState::Inactive, |s| s.state())
    }
}
impl Binding {
    fn params(&self, operation: &str) -> Value {
        json!({"operation":operation,"workspaceId":self.workspace_id,"agentId":self.agent_id,"principalId":self.connection.principal_id,"connectionEpoch":self.connection.connection_epoch,"computerId":self.computer_id})
    }
}
impl Services {
    async fn desktop_validate_live(&self, live: &Live) -> DesktopResult<()> {
        if let Phase::Pending {
            request_id,
            accepted: false,
            claim_generation,
            ..
        } = &live.phase
        {
            if claim_generation.as_ref().is_some_and(|generation| {
                generation
                    != &self
                        .desktop
                        .assignment_generation(&live.binding.workspace_id)
            }) {
                return Err(error(
                    "desktop-stale-request",
                    "Workspace primary assignment changed",
                ));
            }
            let mut invalid = Vec::new();
            for candidate in self.desktop.candidates(request_id) {
                if self.desktop_validate(&candidate).await.is_err() {
                    invalid.push(candidate.connection);
                }
            }
            // Remove only the invalid incarnations observed in this pass.
            // A concurrent response may already have narrowed or removed the
            // live cohort; never restore it from the validation snapshot. Keep
            // original_candidates intact for the final correlated event.
            let viable = self
                .desktop
                .candidates
                .lock()
                .expect("desktop candidates")
                .get_mut(request_id)
                .is_some_and(|candidates| {
                    candidates.retain(|candidate| !invalid.contains(&candidate.connection));
                    !candidates.is_empty()
                });
            if viable {
                return Ok(());
            }
            return Err(error(
                "desktop-not-active",
                "No consent candidate remains eligible",
            ));
        }
        self.desktop_validate(&live.binding).await
    }
    async fn desktop_owner(
        &self,
        workspace: &WorkspaceId,
        agent: &AgentId,
    ) -> DesktopResult<(PrincipalId, String)> {
        self.require_member(workspace).await?;
        let session = self.store.get_agent_session_summary(agent).await?;
        if &session.workspace_id != workspace {
            return Err(error("not-found", "Desktop agent not found"));
        }
        self.store.get_workspace(workspace).await?;
        let owner = self
            .store
            .get_workspace_owner_principal_id(workspace)
            .await?
            .ok_or_else(|| error("forbidden", "Workspace has no owning principal"))?;
        Ok((owner, session.name))
    }
    async fn desktop_connection(
        &self,
        workspace: &WorkspaceId,
        principal: &PrincipalId,
    ) -> DesktopResult<DesktopConnection> {
        let dispatch = self
            .reverse_dispatch
            .as_ref()
            .ok_or_else(|| error("desktop-offline", "No primary desktop connected"))?;
        let target = self.driving_client_target(workspace).await?;
        dispatch.desktop_resolve(&target, principal)
    }
    async fn desktop_reverse(&self, binding: &Binding, params: Value) -> DesktopResult<Value> {
        self.reverse_dispatch
            .as_ref()
            .ok_or_else(|| error("desktop-offline", "No desktop connected"))?
            .desktop_dispatch(binding.connection.clone(), params)
            .await
    }
    async fn desktop_validate(&self, binding: &Binding) -> DesktopResult<()> {
        let (principal, _) = self
            .desktop_owner(&binding.workspace_id, &binding.agent_id)
            .await?;
        let target = self.driving_client_target(&binding.workspace_id).await?;
        let target = if matches!(target, intent_core::ReverseTarget::Default) {
            intent_core::ReverseTarget::Client(binding.connection.client_id.clone())
        } else {
            target
        };
        let resolved = self
            .reverse_dispatch
            .as_ref()
            .ok_or_else(|| error("desktop-offline", "No desktop connected"))?
            .desktop_resolve(&target, &principal)?;
        if principal != binding.connection.principal_id || resolved != binding.connection {
            return Err(error(
                "desktop-not-active",
                "Workspace primary desktop changed",
            ));
        }
        let session = self
            .store
            .get_agent_session_summary(&binding.agent_id)
            .await?;
        if matches!(
            session.status,
            intent_core::AgentStatus::Completed
                | intent_core::AgentStatus::Deleted
                | intent_core::AgentStatus::Error
        ) || session.retired_at.is_some()
        {
            return Err(error("desktop-not-active", "Agent has terminated"));
        }
        Ok(())
    }
    async fn desktop_prepare(
        &self,
        workspace: &WorkspaceId,
        agent: &AgentId,
    ) -> DesktopResult<Binding> {
        let (principal, agent_name) = self.desktop_owner(workspace, agent).await?;
        let connection = self.desktop_connection(workspace, &principal).await?;
        self.desktop_prepare_connection(workspace, agent, agent_name, connection)
            .await
    }
    async fn desktop_prepare_connection(
        &self,
        workspace: &WorkspaceId,
        agent: &AgentId,
        agent_name: String,
        connection: DesktopConnection,
    ) -> DesktopResult<Binding> {
        let mut binding = Binding {
            workspace_id: workspace.clone(),
            agent_id: agent.clone(),
            agent_name,
            connection,
            computer_id: String::new(),
            computer_name: String::new(),
        };
        let mut params = binding.params("prepare");
        params.as_object_mut().unwrap().remove("computerId");
        let prepared = self.desktop_reverse(&binding, params).await?;
        if !matches!(prepared["platform"].as_str(), Some("macos" | "windows")) {
            return Err(error(
                "desktop-unsupported",
                "Primary desktop platform is unsupported",
            ));
        }
        binding.computer_id = string(&prepared, "computerId")?.into();
        binding.computer_name = string(&prepared, "computerName")?.into();
        self.desktop_validate(&binding).await?;
        Ok(binding)
    }
    pub(crate) async fn desktop_agent_op(
        &self,
        workspace: WorkspaceId,
        method: String,
        args: Value,
    ) -> DesktopResult<Value> {
        let Some(Caller::Agent { agent_id }) = intent_core::current_caller() else {
            return Err(error(
                "forbidden",
                "Desktop methods require authenticated agent context",
            ));
        };
        let action = intent_core::desktop::validate_action(&method, &args)?;
        self.require_member(&workspace).await?;
        let session = self.store.get_agent_session_summary(&agent_id).await?;
        if session.workspace_id != workspace {
            return Err(error("not-found", "Desktop agent not found"));
        }
        if method != "endControl" && !self.session_agent_features(&session).desktop_control {
            return Err(error(
                "forbidden",
                "Desktop method is disabled in settings (agentFeatures.desktopControl = false)",
            ));
        }
        let gate = self.desktop.gate(&agent_id);
        let _guard = gate.lock().await;
        match method.as_str() {
            "startControl" => self.desktop_start(&workspace, &agent_id).await,
            "endControl" => self.desktop_end(&agent_id, "agent_end", false).await,
            _ => self
                .desktop_action(&workspace, &agent_id, action)
                .await
                .map_err(|mut e| {
                    if e.execution.is_none() {
                        e.execution = Some("not_started".into());
                    }
                    e
                }),
        }
    }
    async fn desktop_start(
        &self,
        workspace: &WorkspaceId,
        agent: &AgentId,
    ) -> DesktopResult<Value> {
        if let Some(live) = self.desktop.get(agent) {
            if self.desktop_validate_live(&live).await.is_ok() {
                match &live.phase {
                    Phase::Active { session_id, .. } => {
                        return Ok(
                            json!({"status":"active","sessionId":session_id,"alreadyGranted":true,"computerName":live.binding.computer_name,"message":"Control is already granted","hint":RELEASE_HINT}),
                        )
                    }
                    Phase::Pending {
                        request_id,
                        expires,
                        ..
                    } if *expires > Instant::now() => {
                        return Ok(
                            json!({"status":"pending_permission","requestId":request_id,"message":PENDING_HINT}),
                        )
                    }
                    Phase::Pending { .. } => {}
                }
            }
            self.desktop_end(agent, "primary_changed", true).await?;
        }
        let generation = self.desktop.assignment_generation(workspace);
        let unassigned = matches!(
            self.driving_client_target(workspace).await?,
            intent_core::ReverseTarget::Default
        );
        let mut candidates = Vec::new();
        if unassigned {
            let (owner, name) = self.desktop_owner(workspace, agent).await?;
            let connections = self
                .reverse_dispatch
                .as_ref()
                .ok_or_else(|| error("desktop-offline", "No desktop connected"))?
                .desktop_candidates(&owner);
            let mut prepares = tokio::task::JoinSet::new();
            for connection in connections {
                let services = self.clone();
                let workspace = workspace.clone();
                let agent = agent.clone();
                let name = name.clone();
                prepares.spawn(intent_core::with_caller(Caller::Daemon, async move {
                    services
                        .desktop_prepare_connection(&workspace, &agent, name, connection)
                        .await
                }));
            }
            while let Some(result) = prepares.join_next().await {
                if let Ok(Ok(binding)) = result {
                    candidates.push(binding);
                }
            }
        } else {
            candidates.push(self.desktop_prepare(workspace, agent).await?);
        }
        let binding = candidates
            .first()
            .cloned()
            .ok_or_else(|| error("desktop-offline", "No eligible desktop connected"))?;
        if !unassigned
            && self
                .store
                .desktop_permission(
                    &binding.connection.principal_id,
                    workspace,
                    agent,
                    &binding.computer_id,
                )
                .await?
        {
            return self.desktop_activate(binding, None).await;
        }
        let request_id = id();
        let _pin = self.browser_client_pin_gate.lock().await;
        let _tabs = self.browser_tab_gate.lock().await;
        if generation != self.desktop.assignment_generation(workspace) {
            return Err(error(
                "desktop-stale-request",
                "Workspace primary changed during preparation",
            ));
        }
        let mut viable = Vec::new();
        for candidate in candidates {
            if self.desktop_validate(&candidate).await.is_ok() {
                viable.push(candidate);
            }
        }
        let candidates = viable;
        let binding = candidates.first().cloned().ok_or_else(|| {
            error(
                "desktop-offline",
                "Consent candidates disconnected during preparation",
            )
        })?;
        let expires_at = (chrono::Utc::now() + chrono::Duration::minutes(5))
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let mut request_binding = value(&binding);
        request_binding["expiresAt"] = expires_at.clone().into();
        request_binding["assignmentGeneration"] = generation.clone().into();
        self.store
            .desktop_insert_request(&request_id, workspace, agent, &request_binding)
            .await?;
        let live = Live {
            binding,
            phase: Phase::Pending {
                request_id: request_id.clone(),
                expires: Instant::now() + Duration::from_secs(300),
                expires_at,
                accepted: false,
                claim_generation: unassigned.then_some(generation),
            },
        };
        self.desktop.put(live.clone());
        self.desktop
            .candidates
            .lock()
            .expect("desktop candidates")
            .insert(request_id.clone(), candidates.clone());
        self.desktop
            .original_candidates
            .lock()
            .expect("desktop candidates")
            .insert(request_id.clone(), candidates.clone());
        for binding in candidates {
            let candidate = Live {
                binding,
                phase: live.phase.clone(),
            };
            self.desktop_event(
                &candidate.binding,
                DESKTOP_PERMISSION_REQUESTED,
                candidate.pending().unwrap(),
            )
            .await?;
        }
        self.desktop_watch(agent.clone());
        Ok(json!({"status":"pending_permission","requestId":request_id,"message":PENDING_HINT}))
    }
    async fn desktop_activate(
        &self,
        binding: Binding,
        request: Option<&str>,
    ) -> DesktopResult<Value> {
        self.desktop_validate(&binding).await?;
        if let Some(request) = request {
            if !self.desktop.get(&binding.agent_id).is_some_and(|l| matches!(l.phase,Phase::Pending { ref request_id,expires,.. } if request_id == request && expires > Instant::now())) {
                return Err(error("desktop-stale-request", "Desktop request was withdrawn or expired"));
            }
        }
        let session_id = id();
        let mut entropy = Vec::with_capacity(32);
        entropy.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
        entropy.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
        entropy.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
        let entropy = Sha256::digest(entropy);
        let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(entropy);
        let mut journal_binding = value(&binding);
        if let Some(request) = request {
            journal_binding["requestId"] = request.into();
        }
        self.store
            .desktop_insert_terminal(
                &session_id,
                &binding.workspace_id,
                &binding.agent_id,
                &journal_binding,
                &hash(&token),
            )
            .await?;
        let mut params = binding.params("startControl");
        params["sessionId"] = session_id.clone().into();
        params["agentName"] = binding.agent_name.clone().into();
        params["leaseMs"] = 15000.into();
        params["stopReportToken"] = token.into();
        let ready = self.desktop_reverse(&binding, params).await;
        let result = match ready {
            Ok(ready)
                if ready["ready"] == true
                    && ready["sessionId"] == session_id
                    && ready["computerId"] == binding.computer_id =>
            {
                self.desktop_validate(&binding).await.and_then(|()| {
                    if request.is_some_and(|request| !self.desktop.get(&binding.agent_id).is_some_and(|l| matches!(l.phase,Phase::Pending { ref request_id,expires,.. } if request_id == request && expires > Instant::now()))) {
                        Err(error("desktop-stale-request", "Desktop request was withdrawn or expired"))
                    } else { Ok(()) }
                })
            }
            Ok(_) => Err(error(
                "desktop-execution-failed",
                "Invalid desktop readiness acknowledgement",
            )),
            Err(e) => Err(e),
        };
        if let Err(e) = result {
            let mut params = binding.params("endControl");
            params["sessionId"] = session_id.clone().into();
            let _ = self.desktop_reverse(&binding, params).await;
            self.store
                .desktop_record_end(&session_id, "executor_failed", None, None)
                .await?;
            return Err(e);
        }
        let live = Live {
            binding: binding.clone(),
            phase: Phase::Active {
                session_id: session_id.clone(),
                sequence: 0,
            },
        };
        {
            let revoked = self.desktop.revoked.lock().expect("desktop revocations");
            if revoked.contains(&session_id) {
                return Err(error(
                    "desktop-not-active",
                    "Desktop session stopped during activation",
                ));
            }
            self.desktop.put(live.clone());
        }
        self.desktop_event(&binding,DESKTOP_SESSION_CHANGED,json!({"workspaceId":binding.workspace_id,"agentId":binding.agent_id,"sessionId":session_id,"computerId":binding.computer_id,"computerName":binding.computer_name,"status":"active"})).await?;
        if let Some(request) = request {
            self.desktop_outcome(&live, request, "granted", None, None)
                .await?;
        } else {
            self.desktop_watch(binding.agent_id.clone());
        }
        Ok(
            json!({"status":"active","sessionId":session_id,"alreadyGranted":false,"computerName":binding.computer_name,"message":"Desktop control is active","hint":RELEASE_HINT}),
        )
    }
    async fn desktop_action(
        &self,
        _workspace: &WorkspaceId,
        agent: &AgentId,
        action: Value,
    ) -> DesktopResult<Value> {
        let live = self
            .desktop
            .get(agent)
            .ok_or_else(|| error("desktop-not-active", "Desktop control is not active"))?;
        self.desktop_validate(&live.binding).await?;
        #[cfg(test)]
        {
            let barrier = self.desktop.action_barrier.lock().unwrap().take();
            if let Some((seen, resume)) = barrier {
                seen.notify_one();
                resume.notified().await;
            }
        }
        let (session_id, sequence) = self.desktop.reserve_command(&live)?;
        let start = tokio::time::Instant::now();
        let deadline = start + Duration::from_secs(10);
        let command_id = id();
        let mut prepare = live.binding.params("prepareCommand");
        prepare["sessionId"] = session_id.clone().into();
        prepare["commandId"] = command_id.clone().into();
        prepare["sequence"] = sequence.into();
        let result_action = action.clone();
        prepare["action"] = action;
        let ticket =
            tokio::time::timeout_at(deadline, self.desktop_reverse(&live.binding, prepare))
                .await
                .map_err(|_| {
                    let mut e = error(
                        "desktop-command-expired",
                        "Desktop command preparation timed out",
                    );
                    e.execution = Some("not_started".into());
                    e
                })??;
        if ticket["commandId"] != command_id
            || ticket["sequence"] != sequence
            || ticket["expiresInMs"] != 10000
        {
            return Err(error("desktop-execution-failed", "Invalid command ticket"));
        }
        self.desktop_validate(&live.binding).await?;
        if !matches!(self.desktop.state(agent), DesktopState::Active { session_id: ref current,.. } if current == &session_id)
        {
            return Err(error("desktop-not-active", "Desktop session ended"));
        }
        let mut execute = live.binding.params("execute");
        execute["sessionId"] = session_id.clone().into();
        execute["commandId"] = command_id.clone().into();
        execute["sequence"] = sequence.into();
        execute["deadlineId"] = string(&ticket, "deadlineId")?.into();
        if tokio::time::Instant::now() >= deadline {
            return Err(error(
                "desktop-command-expired",
                "Desktop action budget expired before execution",
            ));
        }
        let response =
            tokio::time::timeout_at(deadline, self.desktop_reverse(&live.binding, execute)).await;
        let result = match response {
            Ok(Ok(response))
                if response["commandId"] == command_id && response["sequence"] == sequence =>
            {
                validate_result(
                    &response["result"],
                    &result_action,
                    &live.binding.workspace_id,
                )
                .map(|()| response["result"].clone())
            }
            Ok(Err(e)) => Err(e),
            _ => {
                let mut e = error(
                    "desktop-outcome-unknown",
                    "Desktop execution could not be confirmed; do not retry",
                );
                e.execution = Some("unknown".into());
                Err(e)
            }
        };
        if let Err(e) = result {
            if e.code == "desktop-outcome-unknown"
                || matches!(e.execution.as_deref(), Some("partial" | "unknown"))
            {
                self.desktop.remove(agent);
                let services = self.clone();
                let ended = live.clone();
                intent_core::spawn_daemon(async move {
                    let _ = services
                        .desktop_finish_end(ended, "outcome_unknown", true)
                        .await;
                    services.desktop_flush_outbox().await;
                });
            }
            return Err(e);
        }
        self.desktop_validate(&live.binding)
            .await
            .map_err(|mut e| {
                e.execution = Some("unknown".into());
                e
            })?;
        if !matches!(self.desktop.state(agent),DesktopState::Active { session_id:ref current,.. } if current == &session_id)
        {
            let mut e = error(
                "desktop-not-active",
                "Desktop session ended before result delivery",
            );
            e.execution = Some("unknown".into());
            return Err(e);
        }
        result
    }
    async fn desktop_outcome(
        &self,
        live: &Live,
        request: &str,
        outcome: &str,
        failure: Option<&DesktopError>,
        reason: Option<&str>,
    ) -> DesktopResult<()> {
        let state = if outcome == "granted" {
            live.state()
        } else {
            DesktopState::Inactive
        };
        let message = match (outcome, failure) {
            ("granted", _) => RELEASE_HINT.to_string(),
            ("failed", Some(failure)) => format!(
                "Desktop permission failed; control is not active. Request {request}: {failure}"
            ),
            _ => format!("Desktop permission {outcome}; control is not active."),
        };
        let mut payload = json!({"type":"desktop_control","requestId":request,"outcome":outcome,"state":state,"message":message});
        if let Some(reason) = reason {
            annotate_wake_reason(&mut payload, reason);
        }
        if let Phase::Active { session_id, .. } = &live.phase {
            payload["sessionId"] = session_id.clone().into();
        }
        if let Some(e) = failure {
            payload["error"] = value(e);
        }
        if self
            .store
            .desktop_resolve_request(
                request,
                &live.binding.workspace_id,
                &live.binding.agent_id,
                outcome,
                &payload,
            )
            .await?
        {
            let mut event = json!({"workspaceId":live.binding.workspace_id,"agentId":live.binding.agent_id,"requestId":request,"outcome":outcome,"state":state});
            if let Some(e) = failure {
                event["error"] = value(e);
            }
            self.desktop
                .candidates
                .lock()
                .expect("desktop candidates")
                .remove(request);
            let candidates = self
                .desktop
                .original_candidates
                .lock()
                .expect("desktop candidates")
                .remove(request)
                .unwrap_or_else(|| vec![live.binding.clone()]);
            for binding in candidates {
                self.desktop_event(&binding, DESKTOP_PERMISSION_RESOLVED, event.clone())
                    .await?;
            }
        }
        Ok(())
    }
    async fn desktop_end(&self, agent: &AgentId, reason: &str, wake: bool) -> DesktopResult<Value> {
        let Some(live) = self.desktop.get(agent) else {
            return Ok(json!({"ended":false,"withdrawn":false}));
        };
        self.desktop_end_live(live, reason, wake).await
    }
    async fn desktop_end_live(&self, live: Live, reason: &str, wake: bool) -> DesktopResult<Value> {
        let agent = &live.binding.agent_id;
        {
            let mut states = self.desktop.live.lock().expect("desktop states");
            if states
                .get(agent)
                .is_none_or(|current| current.key() != live.key())
            {
                return Ok(json!({"ended":false,"withdrawn":false}));
            }
            states.remove(agent);
        }
        if matches!(live.phase, Phase::Active { .. }) {
            self.desktop.assignment_changed(&live.binding.workspace_id);
        }
        self.desktop_finish_end(live, reason, wake).await
    }
    async fn desktop_finish_end(
        &self,
        live: Live,
        reason: &str,
        wake: bool,
    ) -> DesktopResult<Value> {
        let agent = &live.binding.agent_id;
        match &live.phase {
            Phase::Pending { request_id, .. } => {
                self.desktop_outcome(
                    &live,
                    request_id,
                    if reason == "agent_end" {
                        "withdrawn"
                    } else if reason == "expired" {
                        "expired"
                    } else {
                        "invalidated"
                    },
                    None,
                    Some(reason),
                )
                .await?;
                Ok(json!({"ended":false,"withdrawn":true}))
            }
            Phase::Active { session_id, .. } => {
                let mut payload = wake.then(|| json!({"type":"desktop_control","sessionId":session_id,"outcome":"revoked","state":{"status":"inactive"},"message":"Desktop control ended; control is not active. Do not automatically restart."}));
                if let Some(payload) = &mut payload {
                    annotate_wake_reason(payload, reason);
                    if let Ok(Some(record)) = self.store.desktop_terminal(session_id).await {
                        if let Some(request) = record["requestId"].as_str() {
                            payload["requestId"] = request.into();
                        }
                    }
                }
                self.store
                    .desktop_record_end(session_id, reason, None, payload.as_ref())
                    .await?;
                self.desktop_event(&live.binding,DESKTOP_SESSION_CHANGED,json!({"workspaceId":live.binding.workspace_id,"agentId":agent,"sessionId":session_id,"computerId":live.binding.computer_id,"computerName":live.binding.computer_name,"status":"ended","reason":reason})).await?;
                let mut params = live.binding.params("endControl");
                params["sessionId"] = session_id.clone().into();
                let result = self.desktop_reverse(&live.binding, params).await?;
                if result["sessionId"] != *session_id || !result["ended"].is_boolean() {
                    return Err(error(
                        "desktop-execution-failed",
                        "Desktop teardown was not confirmed",
                    ));
                }
                Ok(json!({"ended":true,"withdrawn":false}))
            }
        }
    }
}
impl Services {
    pub(crate) async fn desktop_client_op(
        &self,
        method: String,
        args: Value,
        connection: DesktopConnection,
    ) -> DesktopResult<Value> {
        let Some(Caller::Wire { principal_id, .. }) = intent_core::current_caller() else {
            return Err(error(
                "forbidden",
                "Desktop decisions require an authenticated desktop client",
            ));
        };
        if principal_id != connection.principal_id {
            return Err(error("forbidden", "Desktop caller mismatch"));
        }
        let workspace = WorkspaceId::from(string(&args, "workspaceId")?);
        self.require_member(&workspace).await?;
        let allowed: &[&str] = match method.as_str() {
            "getState" => &["workspaceId", "agentId"],
            "setPermission" => &["workspaceId", "agentId", "computerId", "allowed"],
            "respondPermission" => &["workspaceId", "requestId", "decision"],
            "revoke" => &["workspaceId", "sessionId", "reason", "stopReport"],
            _ => return Err(error("invalid-params", "Unknown desktop control method")),
        };
        if args
            .as_object()
            .is_none_or(|o| o.keys().any(|k| !allowed.contains(&k.as_str())))
        {
            return Err(error("invalid-params", "Unknown desktop argument"));
        }
        if method == "revoke" {
            return self.desktop_revoke(&workspace, &args, &connection).await;
        }
        let target = self.driving_client_target(&workspace).await?;
        let unassigned = matches!(target, intent_core::ReverseTarget::Default);
        let candidate_target = if unassigned {
            intent_core::ReverseTarget::Client(connection.client_id.clone())
        } else {
            target
        };
        let selected = self
            .reverse_dispatch
            .as_ref()
            .ok_or_else(|| error("desktop-offline", "No desktop connected"))?
            .desktop_resolve(&candidate_target, &principal_id)?;
        if selected != connection {
            return Err(error(
                if method == "respondPermission" {
                    "desktop-stale-request"
                } else {
                    "forbidden"
                },
                "Only the selected primary can grant desktop control",
            ));
        }
        if method == "respondPermission" {
            let request = string(&args, "requestId")?.to_string();
            let decision = string(&args, "decision")?.to_string();
            if !["allow_once", "allow_future", "deny"].contains(&decision.as_str()) {
                return Err(error(
                    "invalid-params",
                    "Unknown desktop permission decision",
                ));
            }
            let live = self.desktop.live.lock().expect("desktop state").values().find(|l| l.binding.workspace_id == workspace && matches!(&l.phase,Phase::Pending { request_id,.. } if request_id==&request)).cloned().ok_or_else(|| error("desktop-stale-request", "Desktop request is no longer pending"))?;
            let gate = self.desktop.gate(&live.binding.agent_id);
            let _guard = gate.lock().await;
            let _pin = self.browser_client_pin_gate.lock().await;
            let _tabs = self.browser_tab_gate.lock().await;
            let mut live = self
                .desktop
                .get(&live.binding.agent_id)
                .ok_or_else(|| error("desktop-stale-request", "Desktop request was withdrawn"))?;
            self.desktop_validate_live(&live).await?;
            let candidates = self.desktop.candidates(&request);
            let binding = candidates
                .iter()
                .find(|binding| binding.connection == connection)
                .cloned()
                .ok_or_else(|| {
                    error(
                        "desktop-stale-request",
                        "Desktop request belongs to another connection or was dismissed",
                    )
                })?;
            self.desktop_validate(&binding).await?;
            if !self
                .session_agent_features(
                    &self
                        .store
                        .get_agent_session_summary(&live.binding.agent_id)
                        .await?,
                )
                .desktop_control
            {
                return Err(error(
                    "forbidden",
                    "Desktop method is disabled in settings (agentFeatures.desktopControl = false)",
                ));
            }
            #[cfg(test)]
            {
                let barrier = self.desktop.decision_barrier.lock().unwrap().take();
                if let Some((seen, resume)) = barrier {
                    seen.notify_one();
                    resume.notified().await;
                }
            }
            live.binding = binding;
            let claim_generation = match &live.phase {
                Phase::Pending {
                    claim_generation, ..
                } => claim_generation.clone(),
                Phase::Active { .. } => None,
            };
            let claims_primary = claim_generation.is_some();
            if !self
                .desktop
                .commit_decision(&mut live, &request, &decision)?
            {
                return Ok(json!({"accepted":true,"requestId":request}));
            }
            if decision != "deny" && claims_primary {
                // Acceptance is reserved in the current runtime request before
                // awaiting the durable claim. A failed write may release that
                // reservation, but must never restore an invalidated request.
                let claimed: DesktopResult<()> = async {
                    self.desktop_validate(&live.binding).await?;
                    if claim_generation.as_ref()
                        != Some(&self.desktop.assignment_generation(&workspace))
                        || !matches!(
                            self.driving_client_target(&workspace).await?,
                            intent_core::ReverseTarget::Default
                        )
                        || !self
                            .store
                            .desktop_claim_primary(
                                &request,
                                &value(&live.binding),
                                claim_generation.as_deref().unwrap(),
                                decision == "allow_future",
                            )
                            .await?
                    {
                        return Err(error(
                            "desktop-stale-request",
                            "Workspace primary was assigned before this approval",
                        ));
                    }
                    Ok(())
                }
                .await;
                if let Err(error) = claimed {
                    self.desktop
                        .cancel_decision(&live.binding.agent_id, &request);
                    return Err(error);
                }
                self.desktop.assignment_changed(&workspace);
                crate::publish_event(
                    self.event_bus.as_ref(),
                    crate::workspace_updated_event(
                        &workspace,
                        &json!({"browserClientId":connection.client_id}),
                    ),
                )
                .await;
            }
            let services = self.clone();
            let gate = gate.clone();
            intent_core::spawn_daemon(async move {
                let _guard = gate.lock().await;
                let current = services.desktop.get(&live.binding.agent_id);
                if decision != "deny" && !current.is_some_and(|l| matches!(l.phase,Phase::Pending { ref request_id,.. } if request_id==&request)) { return; }
                let result: DesktopResult<()> = async {
                    if decision == "deny" {
                        services
                            .desktop_outcome(&live, &request, "denied", None, None)
                            .await?;
                    } else {
                        if decision == "allow_future" && !claims_primary {
                            services
                                .store
                                .desktop_set_permission(
                                    &principal_id,
                                    &workspace,
                                    &live.binding.agent_id,
                                    &live.binding.computer_id,
                                    true,
                                )
                                .await?;
                        }
                        services
                            .desktop_activate(live.binding.clone(), Some(&request))
                            .await?;
                    }
                    Ok(())
                }
                .await;
                if let Err(e) = result {
                    if decision != "deny" {
                        services.desktop.remove(&live.binding.agent_id);
                    }
                    let _ = services
                        .desktop_outcome(&live, &request, "failed", Some(&e), None)
                        .await;
                }
                services.desktop_flush_outbox().await;
            });
            return Ok(json!({"accepted":true,"requestId":args["requestId"]}));
        }
        let agent = AgentId::from(string(&args, "agentId")?);
        let (owner, _) = self.desktop_owner(&workspace, &agent).await?;
        if owner != principal_id {
            return Err(error(
                "forbidden",
                "Only the owning principal can grant consent",
            ));
        }
        let binding = self
            .desktop_prepare_connection(
                &workspace,
                &agent,
                self.store.get_agent_session_summary(&agent).await?.name,
                connection.clone(),
            )
            .await?;
        if binding.connection != connection {
            return Err(error(
                "forbidden",
                "Primary connection changed during preparation",
            ));
        }
        let gate = self.desktop.gate(&agent);
        let _guard = gate.lock().await;
        if method == "setPermission" {
            if string(&args, "computerId")? != binding.computer_id {
                return Err(error(
                    "desktop-stale-request",
                    "Permission menu refers to another computer",
                ));
            }
            let allowed = args["allowed"]
                .as_bool()
                .ok_or_else(|| error("invalid-params", "allowed must be a boolean"))?;
            self.store
                .desktop_set_permission(
                    &principal_id,
                    &workspace,
                    &agent,
                    &binding.computer_id,
                    allowed,
                )
                .await?;
        }
        let allowed = self
            .store
            .desktop_permission(&principal_id, &workspace, &agent, &binding.computer_id)
            .await?;
        let permission = json!({"computerId":binding.computer_id,"computerName":binding.computer_name,"allowed":allowed});
        if method == "setPermission" {
            self.desktop_event(
                &binding,
                DESKTOP_PERMISSION_CHANGED,
                json!({"workspaceId":workspace,"agentId":agent,"permission":permission}),
            )
            .await?;
            return Ok(json!({"permission":permission}));
        }
        let mut result =
            json!({"state":self.desktop_current_state(&agent).await,"permission":permission});
        if let Some(mut live) = self.desktop.get(&agent) {
            if let Phase::Pending { ref request_id, .. } = live.phase {
                if let Some(candidate) = self
                    .desktop
                    .candidates(request_id)
                    .into_iter()
                    .find(|binding| binding.connection == connection)
                {
                    live.binding = candidate;
                    result["pending"] = live.pending().unwrap();
                }
            }
        }
        Ok(result)
    }
    async fn desktop_revoke(
        &self,
        workspace: &WorkspaceId,
        args: &Value,
        connection: &DesktopConnection,
    ) -> DesktopResult<Value> {
        let session = string(args, "sessionId")?;
        let reason = string(args, "reason")?;
        if ![
            "user_stop",
            "screen_locked",
            "os_permission_lost",
            "lease_expired",
            "executor_failed",
            "unsupported_environment",
        ]
        .contains(&reason)
        {
            return Err(error("invalid-params", "Unknown desktop revocation reason"));
        }
        let record = self
            .store
            .desktop_terminal(session)
            .await?
            .ok_or_else(|| error("not-found", "Desktop session not found"))?;
        let binding: Binding = serde_json::from_value(record.clone()).map_err(|_| {
            error(
                "desktop-execution-failed",
                "Invalid terminal desktop record",
            )
        })?;
        if &binding.workspace_id != workspace {
            return Err(error("not-found", "Desktop session not found"));
        }
        if binding.connection.principal_id != connection.principal_id {
            return Err(error(
                "forbidden",
                "Desktop session belongs to another principal",
            ));
        }
        let report = if reason == "user_stop" {
            let report = &args["stopReport"];
            if report.as_object().is_none_or(|o| o.len() != 4)
                || string(report, "computerId")? != binding.computer_id
                || string(report, "connectionEpoch")? != binding.connection.connection_epoch
                || hash(string(report, "stopReportToken")?) != string(&record, "tokenHash")?
            {
                return Err(error("forbidden", "Invalid desktop Stop credential"));
            }
            let report_id = string(report, "reportId")?;
            if uuid::Uuid::parse_str(report_id).is_err() {
                return Err(error("invalid-params", "Stop report ID must be a UUID"));
            }
            Some(report_id)
        } else {
            if args.get("stopReport").is_some() {
                return Err(error(
                    "invalid-params",
                    "Only user Stop carries a report credential",
                ));
            }
            if binding.connection != *connection {
                return Err(error(
                    "forbidden",
                    "Revocation requires the bound executor connection",
                ));
            }
            None
        };
        let active = {
            let mut revoked = self.desktop.revoked.lock().expect("desktop revocations");
            revoked.insert(session.to_string());
            let mut states = self.desktop.live.lock().expect("desktop states");
            let active = states.get(&binding.agent_id).is_some_and(
                |live| matches!(&live.phase,Phase::Active { session_id,.. } if session_id==session),
            );
            if active {
                states.remove(&binding.agent_id);
            }
            active
        };
        let successor = matches!(self.desktop.state(&binding.agent_id),DesktopState::Active { session_id: ref current,.. } if current != session);
        let message = if report.is_some() {
            if successor {
                format!("{STOP_HINT} Stopped session {session}; the newer explicit session is unchanged.")
            } else {
                STOP_HINT.to_string()
            }
        } else {
            "Desktop control ended; control is not active. Do not automatically restart.".into()
        };
        let mut payload = json!({"type":"desktop_control","sessionId":session,"outcome":"revoked","state":{"status":"inactive"},"message":message});
        annotate_wake_reason(&mut payload, reason);
        if let Some(request) = record["requestId"].as_str() {
            payload["requestId"] = request.into();
        }
        if let Some(report) = report {
            payload["reportId"] = report.into();
        }
        let changed = self
            .store
            .desktop_record_end(session, reason, report, Some(&payload))
            .await?;
        if changed {
            if let Some(request) = record["requestId"].as_str() {
                if let Some(pending) = self.desktop.get(&binding.agent_id).filter(|live| matches!(&live.phase,Phase::Pending { request_id,.. } if request_id==request)) {
                    self.desktop.remove(&binding.agent_id);
                    self.desktop_event(&pending.binding, DESKTOP_PERMISSION_RESOLVED, json!({"workspaceId":workspace,"agentId":binding.agent_id,"requestId":request,"outcome":"invalidated","state":{"status":"inactive"}})).await?;
                }
            }
            let mut event = json!({"workspaceId":workspace,"agentId":binding.agent_id,"sessionId":session,"computerId":binding.computer_id,"computerName":binding.computer_name,"status":"ended","reason":reason});
            if let Some(report) = report {
                event["reportId"] = report.into();
            }
            self.desktop_event(&binding, DESKTOP_SESSION_CHANGED, event)
                .await?;
        }
        self.desktop_flush_outbox().await;
        Ok(json!({"revoked":active && changed,"reported":report.is_some() && changed}))
    }
    async fn desktop_event(
        &self,
        binding: &Binding,
        event_type: &str,
        data: Value,
    ) -> DesktopResult<()> {
        if let Some(bus) = &self.event_bus {
            bus.publish(&intent_store::NewEvent { workspace_id:binding.workspace_id.clone(),timestamp:intent_core::now_iso(),event_type:event_type.into(),actor:intent_core::EventActor { actor_type:intent_core::ActorType::System,id:None,name:None,..Default::default() },session_id:None,correlation_id:None,parent_event_id:None,metadata:Some(json!({"desktopPrincipalId":binding.connection.principal_id,"desktopConnectionEpoch":if event_type==DESKTOP_SESSION_CHANGED {Value::Null} else {binding.connection.connection_epoch.clone().into()}})),data }).await?;
        }
        Ok(())
    }
    fn desktop_watch(&self, agent: AgentId) {
        let watch = id();
        self.desktop
            .watchers
            .lock()
            .expect("desktop watchers")
            .insert(agent.clone(), watch.clone());
        let services = self.clone();
        intent_core::spawn_daemon(async move {
            let mut timer = tokio::time::interval(Duration::from_secs(5));
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                if services
                    .desktop
                    .watchers
                    .lock()
                    .expect("desktop watchers")
                    .get(&agent)
                    != Some(&watch)
                {
                    break;
                }
                let change = services
                    .reverse_dispatch
                    .as_ref()
                    .and_then(|dispatch| dispatch.desktop_changes());
                tokio::select! {
                    _ = timer.tick() => {},
                    () = services.desktop.changed.notified() => {},
                    () = async { if let Some(change) = change { change.notified().await; } else { std::future::pending::<()>().await; } } => {},
                }
                services.desktop_flush_outbox().await;
                let Some(live) = services.desktop.get(&agent) else {
                    break;
                };
                if services.desktop_validate_live(&live).await.is_err() {
                    let _ = services
                        .desktop_end_live(
                            live.clone(),
                            services.desktop_invalid_reason(&live.binding).await,
                            true,
                        )
                        .await;
                    continue;
                }
                match &live.phase {
                    Phase::Pending {
                        request_id,
                        expires,
                        ..
                    } if *expires <= Instant::now() => {
                        let gate = services.desktop.gate(&agent);
                        let _guard = gate.lock().await;
                        if services.desktop.get(&agent).is_some_and(|l| matches!(l.phase,Phase::Pending { request_id:ref r,.. } if r==request_id)) {
                            services.desktop.remove(&agent);
                            let _ = services.desktop_outcome(&live,request_id,"expired",None,Some("expired")).await;
                        }
                    }
                    Phase::Active { session_id, .. } => {
                        let mut params = live.binding.params("renew");
                        params["sessionId"] = session_id.clone().into();
                        params["leaseMs"] = 15000.into();
                        let renewed = services.desktop_reverse(&live.binding, params).await;
                        if !renewed
                            .is_ok_and(|v| v["renewed"] == true && v["sessionId"] == *session_id)
                        {
                            let _ = services
                                .desktop_end_live(live.clone(), "disconnected", true)
                                .await;
                        }
                    }
                    Phase::Pending { .. } => {}
                }
            }
            services.desktop_flush_outbox().await;
        });
    }
    async fn desktop_flush_outbox(&self) {
        let _guard = self.desktop.outbox_gate.lock().await;
        let Ok(entries) = self.store.desktop_outbox().await else {
            return;
        };
        for (id, workspace, agent, mut payload) in entries {
            match self.store.desktop_wake_exists(&agent, &id).await {
                Ok(true) => {
                    let _ = self.store.desktop_outbox_delivered(&id).await;
                    continue;
                }
                Err(_) => continue,
                Ok(false) => {}
            }
            // Generic wake delivery may fall back to a memory-only queue after
            // a write failure. Adopt that entry on retries, including while a
            // drain owns it, instead of enqueueing another copy.
            let pending_in_memory = {
                let draining = self.draining_queue_entries.lock().expect("draining queue");
                let queues = self.agent_queues.lock().expect("agent queues");
                draining
                    .get(&agent)
                    .into_iter()
                    .flatten()
                    .chain(queues.get(&agent).into_iter().flatten())
                    .any(|entry| {
                        entry
                            .message_metadata
                            .as_ref()
                            .is_some_and(|metadata| metadata["desktopWakeId"] == id)
                    })
            };
            if pending_in_memory {
                self.persist_queue_snapshot(&agent).await;
                if self
                    .store
                    .desktop_wake_exists(&agent, &id)
                    .await
                    .is_ok_and(|exists| exists)
                {
                    let _ = self.store.desktop_outbox_delivered(&id).await;
                }
                continue;
            }
            if payload["outcome"] == "revoked" {
                let changed = {
                    let mut queues = self.agent_queues.lock().expect("agent queues");
                    if let Some(queue) = queues.get_mut(&agent) {
                        let before = queue.len();
                        queue.retain(|entry| {
                            !entry.message_metadata.as_ref().is_some_and(|m| {
                                m["type"] == "desktop_control"
                                    && m["sessionId"] == payload["sessionId"]
                            })
                        });
                        before != queue.len()
                    } else {
                        false
                    }
                };
                if changed {
                    self.persist_queue_snapshot(&agent).await;
                    self.publish_queue_updated(&agent).await;
                }
            }
            if payload["outcome"] == "granted"
                && !matches!(self.desktop.state(&agent),DesktopState::Active { ref session_id,.. } if payload["sessionId"] == *session_id)
            {
                payload["outcome"] = "invalidated".into();
                payload["state"] = json!({"status":"inactive"});
                payload["message"] =
                    "Desktop control is no longer active. Do not automatically restart.".into();
                if let Some(session) = payload["sessionId"].as_str() {
                    if let Ok(Some(record)) = self.store.desktop_terminal(session).await {
                        if let Some(reason) = record["reason"].as_str() {
                            annotate_wake_reason(&mut payload, reason);
                        }
                    }
                }
            }
            let message = payload["message"]
                .as_str()
                .unwrap_or("Desktop control is not active.")
                .to_string();
            payload["desktopWakeId"] = id.clone().into();
            let terminal = self
                .store
                .get_agent_session_summary(&agent)
                .await
                .is_ok_and(|session| {
                    session.retired_at.is_some()
                        || matches!(
                            session.status,
                            intent_core::AgentStatus::Completed
                                | intent_core::AgentStatus::Deleted
                                | intent_core::AgentStatus::Error
                        )
                });
            if terminal {
                if self
                    .store
                    .append_agent_message_with_metadata(
                        &agent,
                        "system",
                        &json!(message),
                        Some(&payload),
                        &intent_core::now_iso(),
                    )
                    .await
                    .is_ok()
                {
                    let _ = self.store.desktop_outbox_delivered(&id).await;
                }
                continue;
            }
            if self
                .deliver_wake_message(&workspace, &agent, &message, Some(&payload))
                .await
                .is_ok()
            {
                self.persist_queue_snapshot(&agent).await;
                if self
                    .store
                    .desktop_wake_exists(&agent, &id)
                    .await
                    .is_ok_and(|exists| exists)
                {
                    let _ = self.store.desktop_outbox_delivered(&id).await;
                }
            }
        }
    }
}
impl Services {
    /// Run after agent queues are rehydrated; active desktop authority starts empty.
    ///
    /// # Errors
    /// Returns the store error if durable restart reconciliation cannot commit.
    pub async fn desktop_recover(&self) -> intent_core::Result<()> {
        self.store.desktop_invalidate_restart().await?;
        self.desktop_flush_outbox().await;
        let services = self.clone();
        intent_core::spawn_daemon(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if services
                    .draining_shutdown
                    .load(std::sync::atomic::Ordering::Acquire)
                {
                    break;
                }
                services.desktop_flush_outbox().await;
            }
        });
        Ok(())
    }
    pub(crate) async fn desktop_refresh_wake(
        &self,
        agent: &AgentId,
        entry: &mut crate::agent_ops::QueuedMessage,
    ) {
        let Some(payload) = entry
            .message_metadata
            .as_mut()
            .filter(|p| p["type"] == "desktop_control")
        else {
            return;
        };
        let Some(session) = payload["sessionId"].as_str().map(str::to_string) else {
            return;
        };
        if payload["outcome"] == "granted"
            && !matches!(self.desktop.state(agent),DesktopState::Active {ref session_id,..} if session_id==&session)
        {
            payload["outcome"] = "invalidated".into();
            payload["state"] = json!({"status":"inactive"});
            payload["message"] =
                "Desktop control is no longer active. Do not automatically restart.".into();
        }
        if let Ok(Some(record)) = self.store.desktop_terminal(&session).await {
            if payload["outcome"] == "invalidated" || payload["outcome"] == "revoked" {
                if let Some(reason) = record["reason"].as_str() {
                    annotate_wake_reason(payload, reason);
                }
            }
            if record["reason"] == "user_stop" {
                payload["reason"] = "user_stop".into();
                payload["outcome"] = "revoked".into();
                payload["state"] = json!({"status":"inactive"});
                payload["reportId"] = record["reportId"].clone();
                let successor = matches!(self.desktop.state(agent),DesktopState::Active {ref session_id,..} if session_id!=&session);
                payload["message"] = if successor {format!("{STOP_HINT} Stopped session {session}; the newer explicit session is unchanged.")} else {STOP_HINT.into()}.into();
            }
        }
        entry.content = payload["message"]
            .as_str()
            .unwrap_or("Desktop control is not active.")
            .into();
    }
    pub(crate) async fn desktop_terminate_agent(&self, agent: &AgentId) {
        let _ = self.desktop_end(agent, "agent_terminated", false).await;
    }
}
#[cfg(test)]
pub(crate) mod tests;

impl Services {
    pub(crate) async fn desktop_current_state(&self, agent: &AgentId) -> DesktopState {
        if let Some(live) = self.desktop.get(agent) {
            if matches!(live.phase, Phase::Pending {expires,..} if expires <= Instant::now()) {
                let _ = self.desktop_end_live(live.clone(), "expired", true).await;
            } else if self.desktop_validate_live(&live).await.is_err() {
                let reason = self.desktop_invalid_reason(&live.binding).await;
                let _ = self.desktop_end_live(live, reason, true).await;
            }
        }
        self.desktop.state(agent)
    }
}

impl Services {
    async fn desktop_invalid_reason(&self, binding: &Binding) -> &'static str {
        if self
            .store
            .get_workspace_owner_principal_id(&binding.workspace_id)
            .await
            .is_ok_and(|owner| owner.as_ref() != Some(&binding.connection.principal_id))
        {
            return "owner_changed";
        }
        if self
            .store
            .get_agent_session_summary(&binding.agent_id)
            .await
            .is_ok_and(|session| {
                session.retired_at.is_some()
                    || matches!(
                        session.status,
                        intent_core::AgentStatus::Completed
                            | intent_core::AgentStatus::Deleted
                            | intent_core::AgentStatus::Error
                    )
            })
        {
            return "agent_terminated";
        }
        if self
            .desktop_connection(&binding.workspace_id, &binding.connection.principal_id)
            .await
            .is_err_and(|e| e.code == "desktop-offline")
        {
            return "disconnected";
        }
        "primary_changed"
    }
}

fn validate_result(result: &Value, action: &Value, workspace: &WorkspaceId) -> DesktopResult<()> {
    let invalid = || {
        let mut e = error("desktop-execution-failed", "Invalid native desktop result");
        e.execution = Some("unknown".into());
        e
    };
    let screenshot = action["kind"] == "screenshot";
    let enumerate = action["kind"] == "listDisplay";
    if !screenshot && !enumerate {
        return if result == &json!({"ok":true}) {
            Ok(())
        } else {
            Err(invalid())
        };
    }
    if result
        .as_object()
        .is_none_or(|o| o.len() != if screenshot { 3 } else { 2 })
        || string(result, "layoutId").is_err()
        || (screenshot
            && !result["capturedAt"].as_str().is_some_and(|s| {
                chrono::DateTime::parse_from_rfc3339(s)
                    .is_ok_and(|t| t.offset().local_minus_utc() == 0)
            }))
    {
        return Err(invalid());
    }
    let displays = result["displays"].as_array().ok_or_else(invalid)?;
    if screenshot
        && (displays.len() != 1
            || action
                .get("layoutId")
                .is_some_and(|layout| layout != &result["layoutId"])
            || action
                .get("displayId")
                .is_some_and(|id| id != &displays[0]["displayId"]))
    {
        return Err(invalid());
    }
    let mut ids = std::collections::HashSet::new();
    for display in displays {
        if display
            .as_object()
            .is_none_or(|o| o.len() != if screenshot { 9 } else { 6 })
            || string(display, "displayId").is_err()
            || !ids.insert(display["displayId"].as_str())
            || !["width", "height"]
                .iter()
                .all(|key| display[key].as_u64().is_some_and(|n| n > 0))
            || !["originX", "originY"]
                .iter()
                .all(|key| display[key].as_i64().is_some())
            || !display["scaleFactor"]
                .as_f64()
                .is_some_and(|n| n.is_finite() && n > 0.0)
            || (screenshot && display["mimeType"] != "image/png")
        {
            return Err(invalid());
        }
        if screenshot {
            let asset = string(display, "assetId").map_err(|_| invalid())?;
            if display["url"] != format!("workspace-asset://{workspace}/{asset}") {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

// Only stable public reason codes belong in model-visible diagnostics. Journal
// contents never authorize a new session, and unknown values stay private.
fn annotate_wake_reason(payload: &mut Value, reason: &str) {
    if ![
        "user_stop",
        "screen_locked",
        "os_permission_lost",
        "lease_expired",
        "executor_failed",
        "outcome_unknown",
        "unsupported_environment",
        "owner_changed",
        "agent_terminated",
        "disconnected",
        "primary_changed",
        "agent_end",
        "expired",
    ]
    .contains(&reason)
    {
        return;
    }
    payload["reason"] = reason.into();
    if reason != "user_stop" {
        let suffix = format!(" Reason: {reason}.");
        let message = payload["message"]
            .as_str()
            .unwrap_or("Desktop control is not active.");
        if !message.ends_with(&suffix) {
            payload["message"] = format!("{message}{suffix}").into();
        }
    }
}
