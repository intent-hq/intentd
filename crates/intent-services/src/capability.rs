//! Capability matrix and membership enforcement (multiplayer w3).
//!
//! Every front door — the wire router, the agent-facing `ws.*` bindings and
//! the hook runner — converges on the [`WorkspaceApi`](intent_core::WorkspaceApi)
//! implementation, so the matrix is enforced here, keyed on the
//! [`Caller`] bound to the request:
//!
//! - `Caller::Wire { is_administrator: false }` — a collaborator connection.
//!   The only caller class the matrix constrains: it sees and touches only
//!   the workspaces it is a member of, may not call Owner-only operations,
//!   and never reaches administrator-only ones.
//! - `Caller::Wire { is_administrator: true }` — the primary user, who owns
//!   every workspace in v1 (no transfer RPC) and administers the daemon.
//! - `Caller::Agent` / `Caller::Daemon` — act with the owner's capabilities
//!   (decided: an agent steered by a collaborator still runs `ws.host.exec`).
//! - An unbound request (`current_caller() == None`) is **refused**
//!   (`Forbidden`, the w3 brief AC) by every protected gate and every
//!   membership-narrowing read. `Caller` is a `tokio::task_local!`, so every
//!   `tokio::spawn` drops the binding: daemon-internal work that reaches a
//!   gate (event fan-out, git status refresher, PR-monitor flush, the
//!   scheduled-delete timer, export finalisers, startup) binds
//!   `Caller::Daemon` explicitly, and the wire subscription snapshot / delta
//!   readers re-bind the *subscriber's* caller so a collaborator's channel
//!   never receives rows outside its membership. An unbound refusal also
//!   logs one `tracing::warn!` per gate, and under the
//!   [`ASSERT_BOUND_CALLER_ENV`] test seam (armed for every daemon the
//!   intentd integration suite spawns) it aborts the process, so a missed
//!   binding — even on a detached task nothing awaits — fails the e2e suite
//!   instead of degrading a background path silently.
//!
//! Classes: *Member+* (Read / Steer & edit) → [`Services::require_member`];
//! *Owner-only* (`workspace.delete` / `archive` / `export.*`,
//! `workspace.members.remove`, `agent.delete`, `hook.runNow` / `cancel`,
//! `prMonitor.cancel` / `flush`, settings writes) →
//! [`Services::require_owner`]; *administrator-only* (`mcp.servers.*`,
//! `host.listDirectory` / `env`, `git.clone`, `github.*` / `linear.*` /
//! `sentry.*`, provider credentials) → [`Services::require_administrator`].
//! A non-member is answered `NotFound` (membership is not disclosed); a
//! member lacking the capability gets [`Error::Forbidden`] (`-32003`, the
//! same code as the transport's default-deny allowlist).
//!
//! Cost: every guard returns before touching the store unless the caller is
//! a collaborator, so the administrator / agent / hook paths pay nothing.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

use intent_core::{
    current_caller, lift_from_principal_id, AgentId, Caller, Error, PrincipalId, Result, Workspace,
    WorkspaceId, WorkspaceRole,
};
use serde_json::{json, Value};

use crate::Services;

/// The log line an unbound gate evaluation emits (once per gate per process).
pub const UNBOUND_GATE_LOG: &str = "capability gate evaluated without a bound Caller; refusing";

/// Test seam (same shape as `INTENTD_ASSERT_HERMETIC_ROOT`): when set, an
/// unbound gate evaluation aborts the process instead of merely refusing,
/// so a missed spawn binding fails the e2e suite loudly rather than
/// degrading a background path into a swallowed `Forbidden`. An abort, not
/// a panic: a panic only unwinds the evaluating Tokio task, so a miss on a
/// detached task (a dropped `JoinHandle`) would leave the daemon running
/// and the test green. The integration-test `common` module sets it for
/// every daemon it spawns; production binaries never see it.
pub const ASSERT_BOUND_CALLER_ENV: &str = "INTENTD_ASSERT_BOUND_CALLER";

/// The bound collaborator principal, or `None` when the caller is not
/// constrained by the matrix (administrator, agent, daemon). An unbound
/// request is refused: `Forbidden`, logged once per `gate` per process so a
/// missed spawn binding shows up in the daemon log — and, under
/// [`ASSERT_BOUND_CALLER_ENV`], fatal to the process.
pub(crate) fn gated_collaborator_caller(gate: &str) -> Result<Option<PrincipalId>> {
    match current_caller() {
        Some(Caller::Wire {
            principal_id,
            is_administrator: false,
        }) => Ok(Some(principal_id)),
        Some(Caller::Wire { .. } | Caller::Agent { .. } | Caller::Daemon) => Ok(None),
        None => {
            static WARNED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
            let first = WARNED
                .get_or_init(Mutex::default)
                .lock()
                .is_ok_and(|mut seen| seen.insert(gate.to_string()));
            if first {
                tracing::warn!(gate, "{UNBOUND_GATE_LOG}");
            }
            if std::env::var_os(ASSERT_BOUND_CALLER_ENV).is_some() {
                // stderr, not tracing: the abort below never flushes a
                // non-blocking log writer.
                eprintln!(
                    "{ASSERT_BOUND_CALLER_ENV}: capability gate `{gate}` evaluated without a \
                     bound Caller — bind the entry point (`with_caller` / `spawn_daemon`); \
                     aborting"
                );
                std::process::abort();
            }
            Err(Error::Forbidden(format!("{gate}: no caller is bound")))
        }
    }
}

/// [`gated_collaborator_caller`] for the membership-narrowing reads that
/// have no `what` of their own.
pub(crate) fn collaborator_caller() -> Result<Option<PrincipalId>> {
    gated_collaborator_caller("member")
}

fn not_a_member(workspace_id: &WorkspaceId) -> Error {
    Error::NotFound(format!("workspace {workspace_id}"))
}

/// The owner tag a search registers its cancel token under: the collaborator
/// principal, or `None` for an unconstrained caller. `search.cancel` from a
/// collaborator flips only tokens registered under its own tag; an
/// unconstrained caller cancels any (`CancelRegistry::cancel_as`). An
/// unbound caller is refused like every other gate.
pub(crate) fn search_owner() -> Result<Option<String>> {
    Ok(collaborator_caller()?.map(|principal_id| principal_id.0))
}

/// The `workspace.update` fields a member (collaborator) may set: the
/// workspace-card metadata the catalog promises. Every other field is
/// owner-only. `changes` is the serialised (camelCase) `WorkspaceUpdate`
/// delta, so a field is "touched" iff its key is present.
pub(crate) fn collaborator_editable_update(changes: &Value) -> bool {
    const EDITABLE: [&str; 4] = ["title", "tags", "statusMessage", "statusImageAssetId"];
    match changes.as_object() {
        Some(fields) => fields.keys().all(|k| EDITABLE.contains(&k.as_str())),
        None => false,
    }
}

/// The `agent.update` fields a member (collaborator) may set: the display
/// metadata the catalog promises (name, background flag). Every other field
/// — lifecycle (`status` / `isActive`), session ids, model / provider /
/// system prompt, task linkage, completion report, delegation depth, initial
/// message and attachment / context state — is owner-only.
pub(crate) fn collaborator_editable_agent_update(changes: &Value) -> bool {
    const EDITABLE: [&str; 3] = ["name", "nameExplicitlySet", "isBackground"];
    match changes.as_object() {
        Some(fields) => fields.keys().all(|k| EDITABLE.contains(&k.as_str())),
        None => false,
    }
}

impl Services {
    /// The collaborator caller's role in `workspace_id`; `Ok(None)` when the
    /// caller is not constrained by the matrix.
    async fn collaborator_role(
        &self,
        workspace_id: &WorkspaceId,
        gate: &str,
    ) -> Result<Option<(PrincipalId, Option<WorkspaceRole>)>> {
        let Some(principal_id) = gated_collaborator_caller(gate)? else {
            return Ok(None);
        };
        let role = self
            .store
            .get_workspace_member_role(workspace_id, &principal_id)
            .await?;
        Ok(Some((principal_id, role)))
    }

    /// Member+ gate for a workspace-scoped read / steer / edit. A
    /// collaborator who is not a member gets `NotFound`.
    pub(crate) async fn require_member(&self, workspace_id: &WorkspaceId) -> Result<()> {
        match self.collaborator_role(workspace_id, "member").await? {
            None | Some((_, Some(_))) => Ok(()),
            Some((_, None)) => Err(not_a_member(workspace_id)),
        }
    }

    /// Owner-only gate. A collaborator member gets `Forbidden`; a non-member
    /// `NotFound`.
    pub(crate) async fn require_owner(&self, workspace_id: &WorkspaceId, what: &str) -> Result<()> {
        match self.collaborator_role(workspace_id, what).await? {
            None | Some((_, Some(WorkspaceRole::Owner))) => Ok(()),
            Some((_, Some(WorkspaceRole::Collaborator))) => Err(Error::Forbidden(format!(
                "{what} requires the workspace owner"
            ))),
            Some((_, None)) => Err(not_a_member(workspace_id)),
        }
    }

    /// Administrator-only gate: a per-principal (collaborator) wire caller is
    /// refused regardless of workspace roles.
    pub(crate) fn require_administrator(what: &str) -> Result<()> {
        match gated_collaborator_caller(what)? {
            None => Ok(()),
            Some(_) => Err(Error::Forbidden(format!(
                "{what} requires the daemon administrator"
            ))),
        }
    }

    /// The agent's workspace for a membership check: the metadata-only
    /// session row (no transcript hydration — monorepo#958); an unknown
    /// agent is `NotFound`.
    async fn agent_workspace(&self, agent_id: &AgentId) -> Result<WorkspaceId> {
        Ok(self
            .store
            .get_agent_session_summary(agent_id)
            .await?
            .workspace_id)
    }

    /// Member+ gate keyed by agent: resolves the agent's workspace (one point
    /// read, collaborator callers only) and requires membership there. An
    /// unknown agent is `NotFound` either way.
    pub(crate) async fn require_agent_member(&self, agent_id: &AgentId) -> Result<()> {
        if gated_collaborator_caller("member")?.is_none() {
            return Ok(());
        }
        let workspace_id = self.agent_workspace(agent_id).await?;
        self.require_member(&workspace_id).await
    }

    /// [`Self::require_agent_member`] for a call that also names the
    /// workspace the turn runs against (`agent.sendMessage`,
    /// `agent.sendQueuedMessageNow`, `agent.editAndRegenerate`): a
    /// collaborator's caller-supplied `workspace_id` must be the agent's own
    /// workspace, otherwise the agent is `NotFound` there — the turn's side
    /// effects (archived check, `try_begin`, events, spawn cwd) are keyed by
    /// that id, so a member of A must not run A's agent "in" workspace B.
    pub(crate) async fn require_agent_member_in(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Result<()> {
        if gated_collaborator_caller("member")?.is_none() {
            return Ok(());
        }
        let agent_workspace = self.agent_workspace(agent_id).await?;
        if agent_workspace != *workspace_id {
            return Err(Error::NotFound(format!("agent {agent_id}")));
        }
        self.require_member(&agent_workspace).await
    }

    /// Owner-only gate keyed by agent (see [`Self::require_agent_member`]).
    pub(crate) async fn require_agent_owner(&self, agent_id: &AgentId, what: &str) -> Result<()> {
        if gated_collaborator_caller(what)?.is_none() {
            return Ok(());
        }
        let workspace_id = self.agent_workspace(agent_id).await?;
        self.require_owner(&workspace_id, what).await
    }

    /// Path-based `git.*` gate (`git.getBranches` / `branchStatus` / `pull` /
    /// `getRemoteUrl` take a caller-supplied `repoPath`): a collaborator may
    /// only name a path that is the worktree, repository path or a registered
    /// git root of one of its member workspaces; anything else is `Forbidden`
    /// before the path is even validated (no existence disclosure). The
    /// pre-registration workspace-create flow keeps arbitrary paths for the
    /// administrator / agent / daemon callers.
    pub(crate) async fn require_member_repo_path(&self, repo_path: &str, what: &str) -> Result<()> {
        let Some(visible) = self.visible_workspace_ids().await? else {
            return Ok(());
        };
        let forbidden = || {
            Error::Forbidden(format!(
                "{what}: repoPath is not a member workspace checkout"
            ))
        };
        let wanted = std::path::Path::new(repo_path);
        let wanted = wanted
            .canonicalize()
            .unwrap_or_else(|_| wanted.to_path_buf());
        for workspace_id in &visible {
            let Ok(ws) = self.store.get_workspace(workspace_id).await else {
                continue;
            };
            let mut candidates: Vec<String> =
                [ws.worktree_path.as_deref(), ws.repository_path.as_deref()]
                    .into_iter()
                    .flatten()
                    .filter(|p| !p.is_empty())
                    .map(str::to_string)
                    .collect();
            candidates.extend(
                self.store
                    .list_workspace_git_roots(workspace_id)
                    .await?
                    .into_iter()
                    .map(|root| root.path),
            );
            for candidate in candidates {
                let candidate = std::path::Path::new(&candidate);
                let candidate = candidate
                    .canonicalize()
                    .unwrap_or_else(|_| candidate.to_path_buf());
                if candidate == wanted {
                    return Ok(());
                }
            }
        }
        Err(forbidden())
    }

    /// The workspace ids a collaborator caller may see, or `None` when the
    /// caller is unconstrained. One query, independent of the list length.
    pub(crate) async fn visible_workspace_ids(&self) -> Result<Option<HashSet<WorkspaceId>>> {
        let Some(principal_id) = collaborator_caller()? else {
            return Ok(None);
        };
        Ok(Some(
            self.store
                .list_principal_memberships(&principal_id)
                .await?
                .into_iter()
                .map(|m| m.workspace_id)
                .collect(),
        ))
    }

    /// The workspace ids a collaborator caller *owns*, or `None` when the
    /// caller is unconstrained.
    pub(crate) async fn owned_workspace_ids(&self) -> Result<Option<HashSet<WorkspaceId>>> {
        let Some(principal_id) = collaborator_caller()? else {
            return Ok(None);
        };
        Ok(Some(
            self.store
                .list_principal_memberships(&principal_id)
                .await?
                .into_iter()
                .filter(|m| m.role == WorkspaceRole::Owner)
                .map(|m| m.workspace_id)
                .collect(),
        ))
    }

    /// `agent.pendingPermissions` (unfiltered) for a collaborator: keep only
    /// the prompts of agents in workspaces the caller owns — the same
    /// owner-only boundary as `agent:permission:*` event delivery. One
    /// metadata-only session read per distinct prompting agent (pending
    /// prompts are few); an agent that no longer resolves is dropped.
    pub(crate) async fn retain_owned_agent_prompts(
        &self,
        requests: &mut Vec<intent_acp::PermissionRequestData>,
    ) -> Result<()> {
        let Some(owned) = self.owned_workspace_ids().await? else {
            return Ok(());
        };
        let mut by_agent: HashMap<String, bool> = HashMap::new();
        for request in requests.iter() {
            if by_agent.contains_key(&request.session_id) {
                continue;
            }
            let allowed = match self
                .agent_workspace(&AgentId::from(request.session_id.as_str()))
                .await
            {
                Ok(workspace_id) => owned.contains(&workspace_id),
                Err(Error::NotFound(_)) => false,
                Err(e) => return Err(e),
            };
            by_agent.insert(request.session_id.clone(), allowed);
        }
        requests.retain(|r| by_agent.get(&r.session_id).copied().unwrap_or(false));
        Ok(())
    }

    /// Membership filter for `workspace.list` rows (and the workspace channel
    /// snapshot): a collaborator keeps only the workspaces it is a member of.
    pub(crate) async fn retain_member_workspaces(&self, list: &mut Vec<Workspace>) -> Result<()> {
        if let Some(visible) = self.visible_workspace_ids().await? {
            list.retain(|ws| visible.contains(&ws.id));
        }
        Ok(())
    }

    /// `workspace.members.list`: see
    /// [`intent_core::WorkspaceApi::workspace_members_list`].
    pub(crate) async fn workspace_members_list_op(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Value> {
        self.require_member(workspace_id).await?;
        self.store.get_workspace(workspace_id).await?;
        let members = self.store.list_workspace_members(workspace_id).await?;
        let principals: HashMap<PrincipalId, intent_core::Principal> = self
            .store
            .list_principals()
            .await?
            .into_iter()
            .map(|p| (p.id.clone(), p))
            .collect();
        let rows: Vec<Value> = members
            .iter()
            .map(|m| {
                let p = principals.get(&m.principal_id);
                json!({
                    "principalId": m.principal_id,
                    "login": p.and_then(|p| p.login.clone()),
                    "displayName": p.and_then(|p| p.display_name.clone()),
                    "avatarUrl": p.and_then(|p| p.avatar_url.clone()),
                    "role": m.role,
                    "addedAt": m.added_at,
                })
            })
            .collect();
        Ok(json!({ "members": rows }))
    }

    /// `workspace.members.remove`: see
    /// [`intent_core::WorkspaceApi::workspace_members_remove`]. Owner-only.
    /// Removing the owner is `InvalidParams`; removing a non-member is a
    /// no-op (`removed: false`). On removal the member's queued user
    /// messages on the workspace's agents are dropped and a
    /// `workspace:updated { changes: { members: true, removedPrincipalId } }`
    /// event is published: the removed member's workspace-channel forwarder
    /// (which runs under that member's caller) maps it to a `removedIds`
    /// delta, and every later re-read of the workspace by that member is
    /// `NotFound`, so its workspace-scoped channels deliver nothing further.
    pub(crate) async fn workspace_members_remove_op(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
    ) -> Result<Value> {
        self.require_owner(workspace_id, "workspace.members.remove")
            .await?;
        self.store.get_workspace(workspace_id).await?;
        match self
            .store
            .get_workspace_member_role(workspace_id, principal_id)
            .await?
        {
            None => return Ok(json!({ "removed": false })),
            Some(WorkspaceRole::Owner) => {
                return Err(Error::InvalidParams(format!(
                    "principal {principal_id} owns workspace {workspace_id} and cannot be removed"
                )));
            }
            Some(WorkspaceRole::Collaborator) => {}
        }
        let removed = self.detach_collaborator(workspace_id, principal_id).await?;
        Ok(json!({ "removed": removed }))
    }

    /// Shared teardown of a collaborator membership (`members.remove`,
    /// `members.leave`, `principal.revokeSelf`): delete the row, drop the
    /// member's queued messages and publish the `removedPrincipalId`
    /// `workspace:updated`. Returns whether a row was deleted.
    pub(crate) async fn detach_collaborator(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
    ) -> Result<bool> {
        let removed = self
            .store
            .remove_workspace_member(workspace_id, principal_id)
            .await?;
        if removed {
            self.drop_queued_messages_from(workspace_id, principal_id)
                .await;
            let member_count = self.member_count(workspace_id).await?;
            crate::publish_event(
                self.event_bus.as_ref(),
                crate::workspace_updated_event(
                    workspace_id,
                    &json!({
                        "members": true,
                        "removedPrincipalId": principal_id,
                        "memberCount": member_count,
                    }),
                ),
            )
            .await;
        }
        Ok(removed)
    }

    /// Drop every queued entry stamped with `principal_id` on the agents of
    /// `workspace_id`, republishing `agent:queue:updated` for each queue that
    /// changed (which also persists the shrunk snapshot).
    async fn drop_queued_messages_from(
        &self,
        workspace_id: &WorkspaceId,
        principal_id: &PrincipalId,
    ) {
        let sessions = match self.store.list_agent_sessions(workspace_id).await {
            Ok(sessions) => sessions,
            Err(e) => {
                tracing::warn!(error = %e, workspace = %workspace_id, "members.remove: agent list failed; queued messages kept");
                return;
            }
        };
        let mut changed = Vec::new();
        {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            for session in &sessions {
                let Some(queue) = guard.get_mut(&session.id) else {
                    continue;
                };
                let before = queue.len();
                queue.retain(|m| {
                    lift_from_principal_id(m.message_metadata.as_ref()).as_ref()
                        != Some(principal_id)
                });
                if queue.len() != before {
                    changed.push(session.id.clone());
                }
            }
        }
        for agent_id in changed {
            self.publish_queue_updated_for(&agent_id, workspace_id)
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{workspace, TempDb};
    use intent_core::{now_iso, with_caller, Principal, WorkspaceApi};
    use intent_store::Store;
    use std::future::Future;

    /// The caller roles of the matrix, one column each.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Role {
        Administrator,
        Owner,
        Collaborator,
        NonMember,
        Agent,
        Daemon,
    }

    const ROLES: [Role; 6] = [
        Role::Administrator,
        Role::Owner,
        Role::Collaborator,
        Role::NonMember,
        Role::Agent,
        Role::Daemon,
    ];

    /// One workspace with a non-administrator **owner** (the primary is
    /// demoted so the one-owner index admits the promotion), a collaborator
    /// member, and a principal with no membership.
    struct Fixture {
        services: Services,
        ws: WorkspaceId,
        other_ws: WorkspaceId,
        primary: PrincipalId,
        owner: PrincipalId,
        collaborator: PrincipalId,
        outsider: PrincipalId,
    }

    fn principal(login: &str) -> Principal {
        Principal {
            id: PrincipalId::new(),
            github_user_id: None,
            login: Some(login.to_string()),
            display_name: Some(format!("{login} name")),
            avatar_url: None,
            is_primary: false,
            created_at: now_iso(),
            updated_at: now_iso(),
        }
    }

    async fn fixture(tmp: &TempDb) -> Fixture {
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let other_ws = WorkspaceId::new();
        store
            .insert_workspace(&workspace(&other_ws))
            .await
            .expect("other ws");
        let primary = store.get_primary_principal().await.expect("primary").id;
        let (owner, collaborator, outsider) = (
            principal("owner"),
            principal("collab"),
            principal("outsider"),
        );
        for p in [&owner, &collaborator, &outsider] {
            store.upsert_principal(p).await.expect("principal");
        }
        store
            .set_workspace_member_role(&ws, &primary, WorkspaceRole::Collaborator)
            .await
            .expect("demote primary");
        store
            .add_workspace_member(&ws, &owner.id, WorkspaceRole::Owner)
            .await
            .expect("owner");
        store
            .add_workspace_member(&ws, &collaborator.id, WorkspaceRole::Collaborator)
            .await
            .expect("collaborator");
        let workspaces_root = tmp.path.parent().expect("temp db dir").join("workspaces");
        Fixture {
            services: Services::new(store).with_workspaces_root(workspaces_root),
            ws,
            other_ws,
            primary,
            owner: owner.id,
            collaborator: collaborator.id,
            outsider: outsider.id,
        }
    }

    impl Fixture {
        fn caller(&self, role: Role) -> Caller {
            let wire = |principal_id: &PrincipalId, is_administrator| Caller::Wire {
                principal_id: principal_id.clone(),
                is_administrator,
            };
            match role {
                Role::Administrator => wire(&self.primary, true),
                Role::Owner => wire(&self.owner, false),
                Role::Collaborator => wire(&self.collaborator, false),
                Role::NonMember => wire(&self.outsider, false),
                Role::Agent => Caller::Agent {
                    agent_id: AgentId::new(),
                },
                Role::Daemon => Caller::Daemon,
            }
        }

        async fn run<T, F>(&self, role: Role, f: F) -> Result<T>
        where
            F: Future<Output = Result<T>>,
        {
            with_caller(self.caller(role), f).await
        }
    }

    /// Collapse a guarded call's outcome to the matrix cell it should land
    /// on: `ok`, `forbidden` (`-32003`) or `not-found` (membership hidden).
    fn cell<T>(r: &Result<T>) -> &'static str {
        match r {
            Ok(_) => "ok",
            Err(Error::Forbidden(_)) => "forbidden",
            Err(Error::NotFound(_)) => "not-found",
            Err(e) => panic!("unexpected error class: {e}"),
        }
    }

    /// Member+ (Read / Steer & edit): every member and every bound
    /// unconstrained caller passes; a non-member is `NotFound`.
    #[tokio::test]
    async fn member_class_by_role() {
        let tmp = TempDb::new();
        let f = fixture(&tmp).await;
        for role in ROLES {
            let expected = match role {
                Role::NonMember => "not-found",
                _ => "ok",
            };
            let guard = f.run(role, f.services.require_member(&f.ws)).await;
            assert_eq!(cell(&guard), expected, "require_member as {role:?}");
            let read = f.run(role, f.services.get_workspace(f.ws.clone())).await;
            assert_eq!(cell(&read), expected, "workspace.get as {role:?}");
            let roster = f
                .run(role, f.services.workspace_members_list(f.ws.clone()))
                .await;
            assert_eq!(
                cell(&roster),
                expected,
                "workspace.members.list as {role:?}"
            );
            let steer = f.run(role, f.services.mark_seen(f.ws.clone())).await;
            assert_eq!(cell(&steer), expected, "workspace.markSeen as {role:?}");
        }
        // A non-member's refusal is indistinguishable from a missing id.
        let missing = f
            .run(
                Role::NonMember,
                f.services.get_workspace(WorkspaceId::new()),
            )
            .await;
        assert_eq!(cell(&missing), "not-found");
    }

    /// Owner-only: the owner and every bound unconstrained caller pass; a
    /// collaborator member is `Forbidden`; a non-member stays `NotFound`.
    #[tokio::test]
    async fn owner_class_by_role() {
        let tmp = TempDb::new();
        let f = fixture(&tmp).await;
        for role in ROLES {
            let expected = match role {
                Role::Collaborator => "forbidden",
                Role::NonMember => "not-found",
                _ => "ok",
            };
            let guard = f
                .run(role, f.services.require_owner(&f.ws, "workspace.delete"))
                .await;
            assert_eq!(cell(&guard), expected, "require_owner as {role:?}");
            // Removing a principal that is not a member is a no-op, so the
            // fixture survives every column.
            let remove = f
                .run(
                    role,
                    f.services
                        .workspace_members_remove(f.ws.clone(), f.outsider.clone()),
                )
                .await;
            assert_eq!(
                cell(&remove),
                expected,
                "workspace.members.remove as {role:?}"
            );
        }
        let refused = f
            .run(
                Role::Collaborator,
                f.services.require_owner(&f.ws, "workspace.delete"),
            )
            .await
            .unwrap_err();
        assert_eq!(refused.code(), -32003);
        assert!(
            refused.to_string().contains("workspace.delete"),
            "{refused}"
        );
    }

    /// Administrator-only: every per-principal (collaborator) wire caller is
    /// `Forbidden` regardless of workspace role; the administrator, agents
    /// and the daemon pass.
    #[tokio::test]
    async fn administrator_class_by_role() {
        let tmp = TempDb::new();
        let f = fixture(&tmp).await;
        for role in ROLES {
            let expected = match role {
                Role::Owner | Role::Collaborator | Role::NonMember => "forbidden",
                Role::Administrator | Role::Agent | Role::Daemon => "ok",
            };
            let guard = f
                .run(role, async { Services::require_administrator("git.clone") })
                .await;
            assert_eq!(cell(&guard), expected, "require_administrator as {role:?}");
            // `host.exec` through the trait: a collaborator never reaches the
            // runner; an agent (steered by anyone) and the daemon do — the
            // empty args fail on params, past the guard.
            let exec = f
                .run(role, f.services.host_exec(f.ws.clone(), json!({})))
                .await;
            match expected {
                "forbidden" => assert!(
                    matches!(exec, Err(Error::Forbidden(_))),
                    "host.exec as {role:?}: {exec:?}"
                ),
                _ => assert!(
                    matches!(exec, Err(Error::InvalidParams(_))),
                    "host.exec as {role:?}: {exec:?}"
                ),
            }
        }
    }

    /// Brief AC (multiplayer w3): an unbound context — no entry point bound a
    /// `Caller` — is `Forbidden` (`-32003`) on every gate class and every
    /// membership-narrowing read, never treated as the primary user. The
    /// same calls pass as the daemon, so the refusal is the missing binding
    /// and not the fixture.
    #[tokio::test]
    async fn unbound_caller_is_forbidden_on_every_gate() {
        // Unarmed: the production behaviour under test is the refusal, not
        // the test seam's abort.
        assert!(
            std::env::var_os(ASSERT_BOUND_CALLER_ENV).is_none(),
            "{ASSERT_BOUND_CALLER_ENV} must be unset for this test"
        );
        let tmp = TempDb::new();
        let f = fixture(&tmp).await;
        assert_eq!(intent_core::current_caller(), None);
        let agent = AgentId::new();
        let unbound: Vec<(&str, Result<()>)> = vec![
            ("require_member", f.services.require_member(&f.ws).await),
            (
                "require_owner",
                f.services.require_owner(&f.ws, "workspace.delete").await,
            ),
            (
                "require_administrator",
                Services::require_administrator("git.clone"),
            ),
            (
                "require_agent_member",
                f.services.require_agent_member(&agent).await,
            ),
            (
                "require_agent_owner",
                f.services.require_agent_owner(&agent, "agent.delete").await,
            ),
            (
                "require_member_repo_path",
                f.services.require_member_repo_path("/", "git.pull").await,
            ),
            (
                "visible_workspace_ids",
                f.services.visible_workspace_ids().await.map(drop),
            ),
            (
                "owned_workspace_ids",
                f.services.owned_workspace_ids().await.map(drop),
            ),
            (
                "workspace.get",
                f.services.get_workspace(f.ws.clone()).await.map(drop),
            ),
            (
                "workspace.list",
                f.services.list_workspaces(true).await.map(drop),
            ),
            (
                "workspace.delete",
                f.services.delete_workspace(f.ws.clone()).await.map(drop),
            ),
            (
                "workspace.archive",
                f.services
                    .archive_workspace(f.ws.clone(), None)
                    .await
                    .map(drop),
            ),
            (
                "workspace.members.remove",
                f.services
                    .workspace_members_remove(f.ws.clone(), f.outsider.clone())
                    .await
                    .map(drop),
            ),
            (
                "host.exec",
                f.services
                    .host_exec(f.ws.clone(), json!({}))
                    .await
                    .map(drop),
            ),
        ];
        for (gate, outcome) in unbound {
            assert_eq!(cell(&outcome), "forbidden", "{gate} unbound: {outcome:?}");
            assert_eq!(outcome.unwrap_err().code(), -32003, "{gate} unbound");
        }
        // The fixture itself is fine: the daemon passes the same gates.
        assert_eq!(
            cell(
                &f.run(
                    Role::Daemon,
                    f.services.require_owner(&f.ws, "workspace.delete")
                )
                .await
            ),
            "ok"
        );
        assert!(f
            .run(Role::Daemon, f.services.visible_workspace_ids())
            .await
            .expect("visible ids")
            .is_none());
    }

    /// Set by [`armed_seam_aborts_on_detached_unbound_gate`] when it
    /// re-executes this test binary; [`detached_unbound_probe_child`] is a
    /// no-op without it.
    const PROBE_CHILD_ENV: &str = "INTENTD_CALLER_PROBE_CHILD";

    /// Child half of the abort probe: evaluates a gate unbound on a
    /// *detached* task — the `JoinHandle` is dropped, nothing awaits it —
    /// and then reports that the process is still alive. Unarmed, the task's
    /// `Forbidden` is swallowed and the child exits 0, which is exactly the
    /// silent degradation the seam exists to catch.
    #[tokio::test]
    async fn detached_unbound_probe_child() {
        if std::env::var_os(PROBE_CHILD_ENV).is_none() {
            return;
        }
        let tmp = TempDb::new();
        let f = fixture(&tmp).await;
        let services = f.services.clone();
        let (tx, rx) = tokio::sync::oneshot::channel();
        drop(tokio::spawn(async move {
            let outcome = services.list_workspaces(true).await.map(|w| w.len());
            let _ = tx.send(format!("{outcome:?}"));
        }));
        let outcome = rx.await.expect("detached task reported");
        println!("probe-alive: {outcome}");
    }

    /// Armed, the seam aborts the whole process on an unbound gate even when
    /// the miss happens on a detached task: a panic would only unwind that
    /// task and leave the daemon running (and an e2e suite green). Runs the
    /// probe child twice — unarmed as the positive control (alive, plain
    /// `Forbidden`), then armed (dies by `SIGABRT` naming the seam).
    #[test]
    fn armed_seam_aborts_on_detached_unbound_gate() {
        let exe = std::env::current_exe().expect("test binary");
        // An aborted child never sweeps its `TempDb`; root it under a dir
        // the parent sweeps.
        let scratch = crate::test_support::test_tempdir("intentd-caller-probe-");
        let run = |armed: bool| {
            let mut cmd = std::process::Command::new(&exe);
            cmd.args([
                "--exact",
                "capability::tests::detached_unbound_probe_child",
                "--nocapture",
            ])
            .env(PROBE_CHILD_ENV, "1")
            .env("TMPDIR", scratch.path())
            .env_remove(ASSERT_BOUND_CALLER_ENV);
            if armed {
                cmd.env(ASSERT_BOUND_CALLER_ENV, "1");
            }
            cmd.output().expect("run probe child")
        };

        let unarmed = run(false);
        let stdout = String::from_utf8_lossy(&unarmed.stdout);
        assert!(
            unarmed.status.success(),
            "unarmed probe: {:?}\n{stdout}\n{}",
            unarmed.status,
            String::from_utf8_lossy(&unarmed.stderr)
        );
        assert!(
            stdout.contains("probe-alive: Err(Forbidden("),
            "unarmed probe stdout: {stdout}"
        );

        let armed = run(true);
        let stderr = String::from_utf8_lossy(&armed.stderr);
        assert!(!armed.status.success(), "armed probe survived: {stderr}");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                armed.status.signal(),
                Some(libc::SIGABRT),
                "armed probe: {:?}\n{stderr}",
                armed.status
            );
        }
        assert!(
            !String::from_utf8_lossy(&armed.stdout).contains("probe-alive"),
            "armed probe reported alive"
        );
        assert!(
            stderr.contains(ASSERT_BOUND_CALLER_ENV) && stderr.contains("aborting"),
            "armed probe stderr: {stderr}"
        );
    }

    /// Cross-workspace visibility: a collaborator caller sees exactly its
    /// member workspaces on `workspace.list`; every other caller sees all.
    #[tokio::test]
    async fn visibility_by_role() {
        let tmp = TempDb::new();
        let f = fixture(&tmp).await;
        for role in ROLES {
            let listed: HashSet<WorkspaceId> = f
                .run(role, f.services.list_workspaces(true))
                .await
                .expect("workspace.list")
                .into_iter()
                .map(|w| w.id)
                .collect();
            let sees_member = listed.contains(&f.ws);
            let sees_other = listed.contains(&f.other_ws);
            match role {
                Role::Owner | Role::Collaborator => {
                    assert!(sees_member && !sees_other, "{role:?}: {listed:?}");
                }
                Role::NonMember => assert!(listed.is_empty(), "{role:?}: {listed:?}"),
                Role::Administrator | Role::Agent | Role::Daemon => {
                    assert!(sees_member && sees_other, "{role:?}: {listed:?}");
                }
            }
        }
        let ids = f
            .run(Role::Collaborator, f.services.visible_workspace_ids())
            .await
            .expect("visible ids")
            .expect("constrained");
        assert_eq!(ids, HashSet::from([f.ws.clone()]));
        assert!(f
            .run(Role::Administrator, f.services.visible_workspace_ids())
            .await
            .expect("visible ids")
            .is_none());
    }

    /// Unshare: removal by the owner drops the member's role and answers its
    /// next read with `NotFound`; the owner row itself cannot be removed.
    #[tokio::test]
    async fn remove_member_revokes_visibility() {
        let tmp = TempDb::new();
        let f = fixture(&tmp).await;
        let removed = f
            .run(
                Role::Owner,
                f.services
                    .workspace_members_remove(f.ws.clone(), f.collaborator.clone()),
            )
            .await
            .expect("remove");
        assert_eq!(removed["removed"], true);
        let after = f
            .run(Role::Collaborator, f.services.get_workspace(f.ws.clone()))
            .await;
        assert_eq!(cell(&after), "not-found");
        let again = f
            .run(
                Role::Owner,
                f.services
                    .workspace_members_remove(f.ws.clone(), f.collaborator.clone()),
            )
            .await
            .expect("remove again");
        assert_eq!(again["removed"], false);
        let self_remove = f
            .run(
                Role::Owner,
                f.services
                    .workspace_members_remove(f.ws.clone(), f.owner.clone()),
            )
            .await;
        assert!(
            matches!(self_remove, Err(Error::InvalidParams(_))),
            "{self_remove:?}"
        );
    }
}
