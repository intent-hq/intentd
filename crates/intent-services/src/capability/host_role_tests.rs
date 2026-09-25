use super::*;
use crate::tests::{workspace, TempDb};
use intent_core::{
    now_iso, with_caller, Principal, WorkspaceApi, WorkspaceCreate, WorkspaceUpdate,
};
use intent_store::Store;

async fn fixture(tmp: &TempDb) -> (Services, PrincipalId, PrincipalId) {
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
        Services::new(store).with_workspaces_root(root),
        owner.id,
        member.id,
    )
}

fn caller(id: &PrincipalId) -> Caller {
    Caller::Wire {
        principal_id: id.clone(),
        host_role: intent_core::HostRole::Member,
    }
}

#[tokio::test]
async fn host_member_reads_current_and_future_workspaces_without_direct_grants() {
    let tmp = TempDb::new();
    let (svc, owner, member) = fixture(&tmp).await;
    with_caller(caller(&member), async {
        assert!(svc.list_workspaces(true).await.unwrap().is_empty());
        for id in ["existing", "future"] {
            let ws = workspace(&WorkspaceId::from(id));
            svc.store.insert_workspace(&ws).await.unwrap();
            let got = serde_json::to_value(svc.get_workspace(ws.id).await.unwrap()).unwrap();
            assert_eq!(got["ownerPrincipalId"], owner.0);
            assert_eq!(got["myRole"], "collaborator");
            assert_eq!(got["canManage"], true);
        }
        for rows in [
            svc.list_workspaces(true).await.unwrap(),
            svc.list_workspaces_lite(true).await.unwrap(),
        ] {
            assert_eq!(rows.len(), 2);
            for row in rows {
                assert_eq!(serde_json::to_value(row).unwrap()["canManage"], true);
            }
        }
    })
    .await;
}

#[tokio::test]
async fn host_member_creates_on_empty_host_preserving_primary_ownership() {
    let tmp = TempDb::new();
    let (svc, owner, member) = fixture(&tmp).await;
    with_caller(caller(&member), async {
        let result = svc
            .create_workspace(
                WorkspaceCreate {
                    title: Some("Member workspace".into()),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap();
        let ws = serde_json::to_value(result.workspace).unwrap();
        assert_eq!(ws["ownerPrincipalId"], owner.0);
        assert_eq!(ws["myRole"], "collaborator");
        assert_eq!(ws["canManage"], true);
    })
    .await;
}

#[tokio::test]
async fn retained_collaborator_row_does_not_reduce_host_management() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let ws = workspace(&WorkspaceId::from("retained-guest"));
    svc.store.insert_workspace(&ws).await.unwrap();
    svc.store
        .add_workspace_member(&ws.id, &member, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    with_caller(caller(&member), async {
        let got = svc
            .update_workspace(
                ws.id,
                WorkspaceUpdate {
                    default_model: Some("member-selection".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(got.default_model.as_deref(), Some("member-selection"));
        assert_eq!(serde_json::to_value(got).unwrap()["canManage"], true);
        assert!(matches!(
            Services::require_administrator("settings.set"),
            Err(Error::Forbidden(_))
        ));
    })
    .await;
}

#[tokio::test]
async fn principal_discovery_uses_durable_role_without_a_forge_profile() {
    let tmp = TempDb::new();
    let (svc, owner, member) = fixture(&tmp).await;
    for (id, role, admin) in [(owner, "owner", true), (member, "member", false)] {
        let me = with_caller(caller(&id), svc.principal_me()).await.unwrap();
        assert_eq!(me["id"], id.0);
        assert_eq!(me["hostRole"], role);
        assert_eq!(me["isAdministrator"], admin);
        assert_eq!(me["hostMembershipRevision"], 1);
    }
}

#[tokio::test]
async fn host_member_lifecycle_uses_existing_archive_and_incremental_delete() {
    let tmp = TempDb::new();
    let (svc, owner, member) = fixture(&tmp).await;
    with_caller(caller(&member), async {
        let created = svc
            .create_workspace(WorkspaceCreate::default(), Some("create-once".into()))
            .await
            .unwrap()
            .workspace;
        let replay = svc
            .create_workspace(WorkspaceCreate::default(), Some("create-once".into()))
            .await
            .unwrap()
            .workspace;
        assert_eq!(created.id, replay.id);
        assert!(replay.membership.unwrap().can_manage);
        let archived = svc
            .archive_workspace(created.id.clone(), None)
            .await
            .unwrap();
        assert!(archived.archived);
        assert!(archived.membership.unwrap().can_manage);
        assert!(svc.list_workspaces(false).await.unwrap().is_empty());
        assert_eq!(svc.list_workspaces(true).await.unwrap().len(), 1);
        let restored = svc.unarchive_workspace(created.id.clone()).await.unwrap();
        assert!(!restored.archived);
        assert!(restored.membership.unwrap().can_manage);
        let duplicate = svc
            .duplicate_workspace(created.id.clone(), Some("Copy".into()))
            .await
            .unwrap();
        let summary = duplicate.membership.unwrap();
        assert_eq!(summary.owner_principal_id, Some(owner));
        assert_eq!(summary.my_role, Some(WorkspaceRole::Collaborator));
        assert!(summary.can_manage);
        svc.delete_workspace(duplicate.id.clone()).await.unwrap();
        assert!(matches!(
            svc.get_workspace(duplicate.id).await,
            Err(Error::NotFound(_))
        ));
        assert!(svc.get_workspace(created.id).await.is_ok());
    })
    .await;
}

#[tokio::test]
async fn workspace_guests_unknown_and_unbound_cannot_gain_host_capabilities() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let shared = WorkspaceId::from("shared");
    let private = WorkspaceId::from("unrelated");
    for id in [&shared, &private] {
        svc.store.insert_workspace(&workspace(id)).await.unwrap();
    }
    svc.store.remove_host_member(&member).await.unwrap();
    svc.store
        .add_workspace_member(&shared, &member, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    // Deliberately retain the old admitted member role: only durable grants count.
    with_caller(caller(&member), async {
        let rows = svc.list_workspaces(true).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, shared);
        assert!(!rows[0].membership.as_ref().unwrap().can_manage);
        assert!(matches!(
            svc.get_workspace(private.clone()).await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            svc.require_workspace_manager(&shared, "workspace.delete")
                .await,
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            svc.require_workspace_creator("workspace.create").await,
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            svc.settings_list().await,
            Err(Error::Forbidden(_))
        ));
        let updated = svc
            .update_workspace(
                shared.clone(),
                WorkspaceUpdate {
                    title: Some("Guest title".into()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(!updated.membership.unwrap().can_manage);
    })
    .await;
    with_caller(caller(&PrincipalId::new()), async {
        assert!(svc.list_workspaces(true).await.is_err());
        assert!(svc.principal_me().await.is_err());
        assert!(svc
            .require_workspace_creator("workspace.create")
            .await
            .is_err());
        assert!(matches!(
            svc.get_workspace(shared.clone()).await,
            Err(Error::NotFound(_))
        ));
    })
    .await;
    assert!(matches!(
        svc.require_workspace_manager(&shared, "workspace.delete")
            .await,
        Err(Error::Forbidden(_))
    ));
    assert!(matches!(
        svc.require_workspace_creator("workspace.create").await,
        Err(Error::Forbidden(_))
    ));
    svc.principal_me()
        .await
        .expect_err("principal discovery requires a bound caller");
}

#[tokio::test]
async fn removed_member_cannot_reuse_admitted_role_or_reconnect_credential() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let ws = WorkspaceId::from("revoked");
    svc.store.insert_workspace(&workspace(&ws)).await.unwrap();
    svc.store
        .add_workspace_member(&ws, &member, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let hash = "b".repeat(64);
    svc.store
        .insert_principal_credential(&member, &hash)
        .await
        .unwrap();
    assert_eq!(
        svc.resolve_principal_credential(hash.clone())
            .await
            .unwrap(),
        Some(member.clone())
    );
    assert_eq!(
        svc.principal_host_role(member.clone()).await.unwrap(),
        HostRole::Member
    );
    with_caller(caller(&member), async {
        assert!(
            svc.get_workspace(ws.clone())
                .await
                .unwrap()
                .membership
                .unwrap()
                .can_manage
        );
        svc.store.remove_host_member(&member).await.unwrap();
        assert!(svc.list_workspaces(true).await.unwrap().is_empty());
        assert!(matches!(
            svc.get_workspace(ws.clone()).await,
            Err(Error::NotFound(_))
        ));
        assert!(svc
            .require_workspace_manager(&ws, "workspace.delete")
            .await
            .is_err());
        assert!(svc
            .require_workspace_creator("workspace.create")
            .await
            .is_err());
        let me = svc.principal_me().await.unwrap();
        assert_eq!(me["hostRole"], "guest");
        assert_eq!(me["isAdministrator"], false);
        assert_eq!(me["hostMembershipRevision"], 2);
    })
    .await;
    assert_eq!(
        svc.resolve_principal_credential(hash.clone())
            .await
            .unwrap(),
        None
    );
    let restarted = Services::new(Store::open(&tmp.path).await.unwrap());
    assert_eq!(
        restarted.resolve_principal_credential(hash).await.unwrap(),
        None
    );
    assert_eq!(
        restarted.principal_host_role(member).await.unwrap(),
        HostRole::Guest
    );
}

#[tokio::test]
async fn member_search_inherits_workspaces_but_excludes_chief() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let ws = WorkspaceId::from("searchable");
    svc.store.insert_workspace(&workspace(&ws)).await.unwrap();
    let chief = WorkspaceId::from(intent_core::CHIEF_WORKSPACE_ID);
    with_caller(Caller::Daemon, async {
        for (id, title) in [
            (ws.clone(), "needle ordinary"),
            (chief.clone(), "needle administration"),
        ] {
            svc.create_note(
                id,
                intent_core::NoteCreate {
                    title: title.into(),
                    ..Default::default()
                },
                None,
                None,
            )
            .await
            .unwrap();
        }
    })
    .await;
    with_caller(caller(&member), async {
        assert_eq!(
            svc.visible_workspace_ids().await.unwrap().unwrap(),
            HashSet::from([ws])
        );
        let result = svc.search_notes("needle".into(), None).await.unwrap();
        let text = result.to_string();
        assert!(text.contains("needle ordinary"), "{result}");
        assert!(!text.contains("needle administration"), "{result}");
        assert!(matches!(
            svc.get_workspace(chief.clone()).await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            svc.require_workspace_manager(&chief, "workspace.update")
                .await,
            Err(Error::NotFound(_))
        ));
        // Prompt/tool owner gates are intentionally left to their assigned task.
        assert!(svc
            .require_owner(&WorkspaceId::from("searchable"), "agent.respondPermission")
            .await
            .is_err());
    })
    .await;
}

#[tokio::test]
async fn host_member_list_authority_cost_does_not_grow_per_workspace() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    svc.store
        .insert_workspace(&workspace(&WorkspaceId::from("first")))
        .await
        .unwrap();
    with_caller(caller(&member), async {
        svc.list_workspaces_lite(true).await.unwrap();
        let (first, small) =
            crate::test_tracing::count_sqlx_statements(svc.list_workspaces_lite(true)).await;
        assert_eq!(first.unwrap().len(), 1);
        for i in 0..40 {
            svc.store
                .insert_workspace(&workspace(&WorkspaceId::from(format!("extra-{i}"))))
                .await
                .unwrap();
        }
        svc.list_workspaces_lite(true).await.unwrap();
        let (many, large) =
            crate::test_tracing::count_sqlx_statements(svc.list_workspaces_lite(true)).await;
        let many = many.unwrap();
        assert_eq!(many.len(), 41);
        assert!(many
            .iter()
            .all(|w| w.membership.as_ref().unwrap().can_manage));
        assert!(
            small > 0 && large <= small + 1,
            "list query count grew with membership rows: {small} -> {large}"
        );
    })
    .await;
}
