//! Caller authorization of the transfer RPCs: transfer handles stay owned by
//! the principal that opened them, and gated callers cannot export.

use intent_core::discussion::{DiscussionAuthor, DiscussionTarget};
use intent_core::{
    now_iso, with_caller, AgentId, Caller, Error, HostRole, Principal, PrincipalId, WorkspaceApi,
    WorkspaceId, WorkspaceRole,
};
use intent_services::feature_api::tests::{workspace, TempDb};
use intent_services::feature_api::Services;
use intent_store::Store;
use serde_json::json;

/// A `Services` with the transfer feature registered, the primary owner,
/// and one host member.
async fn fixture(tmp: &TempDb) -> (Services, PrincipalId, PrincipalId) {
    let store = Store::open(&tmp.path).await.unwrap();
    let owner = store.get_primary_principal().await.unwrap();
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
        crate::register(Services::new_isolated(store)).with_workspaces_root(root),
        owner.id,
        member.id,
    )
}

fn caller(id: &PrincipalId) -> Caller {
    Caller::Wire {
        principal_id: id.clone(),
        host_role: HostRole::Member,
    }
}

fn human(id: &PrincipalId) -> Caller {
    Caller::Wire {
        principal_id: id.clone(),
        host_role: HostRole::Guest,
    }
}

async fn conversation_fixture() -> (TempDb, Services, WorkspaceId, Vec<Principal>) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let primary = store.get_primary_principal().await.unwrap();
    let mut people = vec![primary.clone()];
    for name in ["alice", "bob", "carol", "outsider"] {
        let mut p = primary.clone();
        p.id = PrincipalId(name.into());
        p.is_primary = false;
        p.login = Some(name.into());
        p.display_name = Some(format!("Offline {name}"));
        store.upsert_principal(&p).await.unwrap();
        if name != "outsider" {
            store
                .add_workspace_member(&ws, &p.id, WorkspaceRole::Collaborator)
                .await
                .unwrap();
        }
        people.push(p);
    }
    (
        tmp,
        crate::register(Services::new_isolated(store)),
        ws,
        people,
    )
}

fn recipients(ids: &[PrincipalId]) -> DiscussionTarget {
    DiscussionTarget {
        conversation_id: None,
        recipient_principal_ids: Some(ids.to_vec()),
        ..DiscussionTarget::default()
    }
}

#[tokio::test]
async fn member_import_handles_remain_owned_by_the_authenticated_initiator() {
    use base64::Engine as _;
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let manifest = json!({"formatVersion":intent_core::transfer::TRANSFER_FORMAT_VERSION,
        "creatingIntentdVersion":intent_core::transfer::CREATING_INTENTD_VERSION,"workspaceId":"member-import",
        "createdAt":now_iso(),"tables":[],"assets":[],"attachments":[],
        "git":{"hasRepository":false,"dirtyFiles":[],"sandboxBranches":[]}});
    let started = with_caller(
        caller(&member),
        svc.workspace_import_begin(manifest, 3, "0".repeat(64)),
    )
    .await
    .unwrap();
    let id = started["importId"].as_str().unwrap().to_string();
    let data = base64::engine::general_purpose::STANDARD.encode(b"abc");
    let mut second = svc.store().get_primary_principal().await.unwrap();
    second.id = PrincipalId::new();
    second.is_primary = false;
    svc.store().upsert_principal(&second).await.unwrap();
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
        .bind(second.id.as_str())
        .bind(now_iso())
        .execute(svc.store().write_pool())
        .await
        .unwrap();
    with_caller(caller(&second.id), async {
        assert!(matches!(
            svc.workspace_import_chunk(id.clone(), 0, data.clone())
                .await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            svc.workspace_import_commit(id.clone()).await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            svc.workspace_import_abort(id.clone()).await,
            Err(Error::NotFound(_))
        ));
    })
    .await;
    with_caller(caller(&member), async {
        svc.workspace_import_chunk(id.clone(), 0, data.clone())
            .await
            .unwrap();
        svc.workspace_import_abort(id.clone()).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn member_export_handles_remain_owned_by_the_authenticated_initiator() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let ws = WorkspaceId::new();
    svc.store().insert_workspace(&workspace(&ws)).await.unwrap();
    let export_id = "member-export".to_string();
    svc.feature_transfer_port().park_building_export_for_test(
        &export_id,
        &ws,
        Some(member.clone()),
        tmp.path.with_extension("export"),
    );
    let mut other = svc.store().get_principal(&member).await.unwrap();
    other.id = PrincipalId::new();
    svc.store().upsert_principal(&other).await.unwrap();
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
        .bind(other.id.as_str())
        .bind(now_iso())
        .execute(svc.store().write_pool())
        .await
        .unwrap();
    with_caller(caller(&other.id), async {
        assert!(matches!(
            svc.workspace_export_read(export_id.clone(), 0).await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            svc.workspace_export_finalize(export_id.clone(), false, None)
                .await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            svc.workspace_export_abort(export_id.clone()).await,
            Err(Error::NotFound(_))
        ));
    })
    .await;
    with_caller(caller(&member), svc.workspace_export_abort(export_id))
        .await
        .unwrap();
}

#[tokio::test]
async fn discussion_export_cannot_bypass_participant_authorization() {
    let (_tmp, svc, ws, p) = conversation_fixture().await;
    assert!(matches!(
        with_caller(
            human(&p[1].id),
            svc.discussion_send(
                ws.clone(),
                "private".into(),
                "private body".into(),
                recipients(&[p[2].id.clone()]),
            ),
        )
        .await,
        Err(Error::InvalidParams(_))
    ));
    // Seed historical private data through persistence; new private creation is rejected.
    let author = DiscussionAuthor {
        principal_id: Some(p[1].id.clone()),
        agent_id: None,
        login: p[1].login.clone(),
        display_name: p[1].display_name.clone(),
        avatar_url: p[1].avatar_url.clone(),
    };
    svc.store()
        .send_discussion_message(
            &ws,
            "private",
            "private body",
            &author,
            &recipients(&[p[2].id.clone()]),
        )
        .await
        .unwrap();
    for caller in [
        Caller::Wire {
            principal_id: p[0].id.clone(),
            host_role: HostRole::Owner,
        },
        Caller::Agent {
            agent_id: AgentId::new(),
        },
    ] {
        assert!(matches!(
            with_caller(caller, svc.workspace_export_start(ws.clone())).await,
            Err(Error::Unsupported(_))
        ));
    }
    assert!(
        svc.feature_transfer_port().export_count_for_test() == 0,
        "no readable export session"
    );
}
