//! Capability-matrix fixtures shared by the capability tests in core and
//! the runtime feature's capability tests.

use std::future::Future;

use intent_core::{
    now_iso, with_caller, AgentId, Caller, Principal, PrincipalId, Result, WorkspaceId,
    WorkspaceRole,
};
use intent_store::Store;

use crate::test_support::fixtures::{workspace, TempDb};
use crate::Services;

/// The caller roles of the matrix, one column each.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Administrator,
    Owner,
    Collaborator,
    NonMember,
    Agent,
    Daemon,
}

/// One workspace with a non-administrator **owner** (the primary is
/// demoted so the one-owner index admits the promotion), a collaborator
/// member, and a principal with no membership.
pub struct Fixture {
    pub services: Services,
    pub ws: WorkspaceId,
    pub other_ws: WorkspaceId,
    pub primary: PrincipalId,
    pub owner: PrincipalId,
    pub collaborator: PrincipalId,
    pub outsider: PrincipalId,
}

fn principal(login: &str) -> Principal {
    Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: None,
        login: Some(login.to_string()),
        display_name: Some(format!("{login} name")),
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    }
}

pub async fn fixture(tmp: &TempDb) -> Fixture {
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
        services: Services::new_isolated(store).with_workspaces_root(workspaces_root),
        ws,
        other_ws,
        primary,
        owner: owner.id,
        collaborator: collaborator.id,
        outsider: outsider.id,
    }
}

impl Fixture {
    #[must_use]
    pub fn caller(&self, role: Role) -> Caller {
        let wire = |principal_id: &PrincipalId, is_administrator| Caller::Wire {
            principal_id: principal_id.clone(),
            host_role: if is_administrator {
                intent_core::HostRole::Owner
            } else {
                intent_core::HostRole::Guest
            },
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

    pub async fn run<T, F>(&self, role: Role, f: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        with_caller(self.caller(role), f).await
    }
}

/// Host-role fixtures: the primary owner plus one host member. Test-only:
/// seeding the host member needs `sqlx`, a dev-dependency.
#[cfg(test)]
pub mod host_role {
    use intent_core::{now_iso, Caller, Principal, PrincipalId};
    use intent_store::Store;

    use crate::test_support::fixtures::TempDb;
    use crate::Services;

    pub async fn fixture(tmp: &TempDb) -> (Services, PrincipalId, PrincipalId) {
        let store = Store::open(&tmp.path).await.unwrap();
        let owner = store.get_primary_principal().await.unwrap();
        assert!(owner.identity_key().is_none(), "no forge account is needed");
        let member = Principal {
            id: PrincipalId::new(),
            identity: None,
            github_user_id: None,
            login: None,
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.upsert_principal(&member).await.unwrap();
        sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
            .bind(&member.id.0)
            .bind(now_iso())
            .execute(store.write_pool())
            .await
            .unwrap();
        let root = tmp.path.parent().unwrap().join("workspaces");
        (
            Services::new_isolated(store).with_workspaces_root(root),
            owner.id,
            member.id,
        )
    }

    #[must_use]
    pub fn caller(id: &PrincipalId) -> Caller {
        Caller::Wire {
            principal_id: id.clone(),
            host_role: intent_core::HostRole::Member,
        }
    }
}
