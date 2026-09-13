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
//! - An unbound request is not a collaborator (the transport binds every
//!   wire caller; agents and hooks never enter through it), matching the
//!   transport's `is_non_administrator_caller` and the event narrowing.
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

use intent_core::{
    current_caller, lift_from_principal_id, AgentId, Caller, Error, PrincipalId, Result, Workspace,
    WorkspaceId, WorkspaceRole,
};
use serde_json::{json, Value};

use crate::Services;

/// The bound collaborator principal, or `None` when the caller is not
/// constrained by the matrix (administrator, agent, daemon, unbound).
pub(crate) fn collaborator_caller() -> Option<PrincipalId> {
    match current_caller() {
        Some(Caller::Wire {
            principal_id,
            is_administrator: false,
        }) => Some(principal_id),
        Some(Caller::Wire { .. } | Caller::Agent { .. } | Caller::Daemon) | None => None,
    }
}

fn not_a_member(workspace_id: &WorkspaceId) -> Error {
    Error::NotFound(format!("workspace {workspace_id}"))
}

impl Services {
    /// The collaborator caller's role in `workspace_id`; `Ok(None)` when the
    /// caller is not constrained by the matrix.
    async fn collaborator_role(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<Option<(PrincipalId, Option<WorkspaceRole>)>> {
        let Some(principal_id) = collaborator_caller() else {
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
        match self.collaborator_role(workspace_id).await? {
            None | Some((_, Some(_))) => Ok(()),
            Some((_, None)) => Err(not_a_member(workspace_id)),
        }
    }

    /// Owner-only gate. A collaborator member gets `Forbidden`; a non-member
    /// `NotFound`.
    pub(crate) async fn require_owner(&self, workspace_id: &WorkspaceId, what: &str) -> Result<()> {
        match self.collaborator_role(workspace_id).await? {
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
        match collaborator_caller() {
            None => Ok(()),
            Some(_) => Err(Error::Forbidden(format!(
                "{what} requires the daemon administrator"
            ))),
        }
    }

    /// Member+ gate keyed by agent: resolves the agent's workspace (one point
    /// read, collaborator callers only) and requires membership there. An
    /// unknown agent is `NotFound` either way.
    pub(crate) async fn require_agent_member(&self, agent_id: &AgentId) -> Result<()> {
        if collaborator_caller().is_none() {
            return Ok(());
        }
        let session = self.store.get_agent_session(agent_id).await?;
        self.require_member(&session.workspace_id).await
    }

    /// Owner-only gate keyed by agent (see [`Self::require_agent_member`]).
    pub(crate) async fn require_agent_owner(&self, agent_id: &AgentId, what: &str) -> Result<()> {
        if collaborator_caller().is_none() {
            return Ok(());
        }
        let session = self.store.get_agent_session(agent_id).await?;
        self.require_owner(&session.workspace_id, what).await
    }

    /// The workspace ids a collaborator caller may see, or `None` when the
    /// caller is unconstrained. One query, independent of the list length.
    pub(crate) async fn visible_workspace_ids(&self) -> Result<Option<HashSet<WorkspaceId>>> {
        let Some(principal_id) = collaborator_caller() else {
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
        let removed = self
            .store
            .remove_workspace_member(workspace_id, principal_id)
            .await?;
        if removed {
            self.drop_queued_messages_from(workspace_id, principal_id)
                .await;
            crate::publish_event(
                self.event_bus.as_ref(),
                crate::workspace_updated_event(
                    workspace_id,
                    &json!({ "members": true, "removedPrincipalId": principal_id }),
                ),
            )
            .await;
        }
        Ok(json!({ "removed": removed }))
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
        Fixture {
            services: Services::new(store),
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

    /// Member+ (Read / Steer & edit): every member and every unconstrained
    /// caller passes; a non-member is `NotFound`.
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

    /// Owner-only: the owner and every unconstrained caller pass; a
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
        // An unbound context is not a collaborator (matches the transport's
        // `is_non_administrator_caller`).
        assert_eq!(cell(&Services::require_administrator("git.clone")), "ok");
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
