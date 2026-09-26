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
async fn member_sender_preamble_uses_bound_person_without_an_explicit_workspace_row() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let ws = WorkspaceId::new();
    svc.store.insert_workspace(&workspace(&ws)).await.unwrap();
    with_caller(caller(&member), async {
        let preamble = svc
            .collaborator_sender_preamble(&ws)
            .await
            .unwrap()
            .expect("member sender preamble");
        assert!(preamble.contains(member.as_str()), "{preamble}");
    })
    .await;
}

#[tokio::test]
async fn member_file_access_cannot_fall_back_from_a_missing_workspace() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let result = with_caller(caller(&member), svc.require_member(&WorkspaceId::new())).await;
    assert!(matches!(result, Err(Error::NotFound(_))));
}

#[tokio::test]
async fn member_tools_manage_another_persons_agent_without_a_workspace_grant() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let ws = WorkspaceId::from("member-tools");
    svc.store.insert_workspace(&workspace(&ws)).await.unwrap();
    let agent = with_caller(
        Caller::Daemon,
        svc.agent_create(
            ws.clone(),
            Some("Owner-created".into()),
            Some("gpt-test".into()),
            None,
            None,
            None,
            intent_core::AgentCreateExtra {
                provider: Some("codex".into()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    let id = AgentId::from(agent["agent"]["id"].as_str().unwrap());
    with_caller(caller(&member), async {
        svc.agent_update(id.clone(), None, json!({"model": "member-choice"}))
            .await
            .unwrap();
        svc.agent_delete(id.clone(), None).await.unwrap();
        assert!(svc.agent_get(id, None).await.is_err());
    })
    .await;
}

#[tokio::test]
async fn member_tools_permission_lists_include_manageable_workspaces() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let ws = WorkspaceId::from("member-prompts");
    svc.store.insert_workspace(&workspace(&ws)).await.unwrap();
    let agent = with_caller(
        Caller::Daemon,
        svc.agent_create(
            ws.clone(),
            None,
            Some("gpt-test".into()),
            None,
            None,
            None,
            intent_core::AgentCreateExtra {
                provider: Some("codex".into()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    let id = AgentId::from(agent["agent"]["id"].as_str().unwrap());
    with_caller(caller(&member), async {
        assert!(svc
            .owned_workspace_ids()
            .await
            .unwrap()
            .unwrap()
            .contains(&ws));
        assert_eq!(
            svc.agent_pending_permissions(Some(id)).await.unwrap(),
            json!({"requests": []})
        );
        assert_eq!(
            svc.agent_pending_permissions(None).await.unwrap(),
            json!({"requests": []})
        );
    })
    .await;
}

#[tokio::test]
async fn member_tools_script_and_terminal_lists_admit_host_members() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let ws = WorkspaceId::from("member-execution");
    svc.store.insert_workspace(&workspace(&ws)).await.unwrap();
    with_caller(caller(&member), async {
        svc.terminal_list(ws.clone()).await.unwrap();
        svc.script_list(ws).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn member_tools_share_configured_mcp_without_admin_authority() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    with_caller(caller(&member), async {
        svc.mcp_list_servers(None).await.unwrap();
        assert!(matches!(
            svc.settings_list().await,
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            svc.agent_replace_messages(AgentId::new(), None, json!([]))
                .await,
            Err(Error::Forbidden(_))
        ));
    })
    .await;
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
    if super::tests::reran_unarmed(
        "capability::host_role_tests::workspace_guests_unknown_and_unbound_cannot_gain_host_capabilities",
    ) {
        return;
    }
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

#[tokio::test]
async fn member_execution_context_is_allowlisted_and_uses_host_policy() {
    use crate::settings::{InMemorySecretStore, SecretStore};
    use std::sync::Arc;
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let registry =
        Arc::new(crate::SettingsRegistry::load(tmp.path.with_extension("toml")).unwrap());
    registry
        .apply(&[
            ("sourceControl.github.tokenSource".into(), json!("explicit")),
            (
                "sourceControl.github.exposeGitCredentialToChildren".into(),
                json!(false),
            ),
            ("model.defaultProvider".into(), json!("codex")),
            ("model.default".into(), json!("host-model")),
            ("providers.paths".into(), json!({"codex":"/host/provider"})),
        ])
        .unwrap();
    let secrets = Arc::new(InMemorySecretStore::default());
    secrets
        .store("sourceControl.github.token", "must-never-leave-the-host")
        .unwrap();
    secrets
        .store("collaboration.github.token", "identity-only")
        .unwrap();
    let svc = svc
        .with_secret_store(secrets.clone())
        .with_settings_registry(registry.clone());
    with_caller(caller(&member), async {
        let context = svc.host_execution_context().await.unwrap();
        let keys: std::collections::BTreeSet<_> = context.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(keys, ["defaultModelId", "defaultProviderId", "gitCredentialPolicy", "repositoryConnections"].into_iter().collect());
        assert_eq!(context["defaultProviderId"], "codex");
        assert_eq!(context["defaultModelId"], "host-model");
        assert_eq!(context["gitCredentialPolicy"], json!({
            "provider":"github", "protocol":"https", "host":"github.com", "managedHelperEnabled":false,
            "setting":"sourceControl.github.exposeGitCredentialToChildren",
        }));
        assert_eq!(context["repositoryConnections"][0], json!({"provider":"github", "host":"github.com", "configured":true}));
        assert!(!context.to_string().contains("must-never-leave"));
        assert!(!context.to_string().contains("identity-only"));
        assert!(!context.to_string().contains("/host/provider"));
        assert_eq!(svc.execution_provider_paths().await.unwrap()["codex"], "/host/provider");
        assert!(matches!(svc.settings_get("providers.paths".into()).await, Err(Error::Forbidden(_))));
        svc.secrets.delete("sourceControl.github.token").await.unwrap();
        assert_eq!(svc.host_execution_context().await.unwrap()["repositoryConnections"][0]["configured"], false,
            "collaboration credentials must never supply execution");
        registry.apply(&[("sourceControl.github.exposeGitCredentialToChildren".into(), json!(true))]).unwrap();
        assert_eq!(svc.host_execution_context().await.unwrap()["gitCredentialPolicy"]["managedHelperEnabled"], true);
    }).await;
    svc.store.remove_host_member(&member).await.unwrap();
    assert!(
        with_caller(caller(&member), svc.host_execution_context())
            .await
            .is_err(),
        "stale admitted member rejected"
    );
    assert!(
        with_caller(caller(&PrincipalId::new()), svc.host_execution_context())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn member_prompt_filter_retains_all_manageable_agents_and_no_hidden_ids() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let mut ids = Vec::new();
    for ws in [
        WorkspaceId::from("prompt-a"),
        WorkspaceId::from("prompt-b"),
        WorkspaceId::chief(),
    ] {
        if svc.store.get_workspace(&ws).await.is_err() {
            svc.store.insert_workspace(&workspace(&ws)).await.unwrap();
        }
        let created = with_caller(
            Caller::Daemon,
            svc.agent_create(
                ws.clone(),
                None,
                Some("gpt-test".into()),
                None,
                None,
                None,
                intent_core::AgentCreateExtra {
                    provider: Some("codex".into()),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
        ids.push(AgentId::from(created["agent"]["id"].as_str().unwrap()));
    }
    svc.store
        .add_workspace_member(
            &WorkspaceId::from("prompt-b"),
            &member,
            WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    let requests: Vec<_> = ids
        .iter()
        .chain(std::iter::once(&AgentId::from("unknown")))
        .enumerate()
        .map(|(i, id)| intent_acp::PermissionRequestData {
            request_id: format!("permission-{i}"),
            session_id: id.0.clone(),
            title: "Approval".into(),
            description: None,
            options: Vec::new(),
            agent_name: "Shared agent".into(),
            risk_level: intent_acp::permission::RiskLevel::Low,
            timestamp: 0,
        })
        .collect();
    with_caller(caller(&member), async {
        let mut visible = requests.clone();
        svc.retain_owned_agent_prompts(&mut visible).await.unwrap();
        assert_eq!(
            visible
                .iter()
                .map(|r| r.session_id.as_str())
                .collect::<Vec<_>>(),
            [&ids[0].0, &ids[1].0]
        );
        for id in &ids[..2] {
            svc.require_agent_owner(id, "agent.respondPermission")
                .await
                .unwrap();
        }
        assert!(svc
            .agent_pending_permissions(Some(ids[2].clone()))
            .await
            .is_err());
        assert!(svc
            .agent_pending_permissions(Some(AgentId::from("unknown")))
            .await
            .is_err());
    })
    .await;
    svc.store.remove_host_member(&member).await.unwrap();
    svc.store
        .add_workspace_member(
            &WorkspaceId::from("prompt-a"),
            &member,
            WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    with_caller(caller(&member), async {
        let mut visible = requests.clone();
        svc.retain_owned_agent_prompts(&mut visible).await.unwrap();
        assert!(
            visible.is_empty(),
            "a guest's readable agent is not an answerable prompt"
        );
        assert!(matches!(
            svc.require_agent_owner(&ids[0], "agent.respondPermission")
                .await,
            Err(Error::Forbidden(_))
        ));
    })
    .await;
}

#[cfg(unix)]
#[tokio::test]
async fn member_terminal_ids_resolve_the_workspace_and_recheck_revocation() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let ws = WorkspaceId::from("member-terminal");
    svc.store.insert_workspace(&workspace(&ws)).await.unwrap();
    let spawned = svc
        .pty
        .spawn(intent_pty::SpawnSpec::new(ws.as_str(), "/bin/cat"))
        .unwrap();
    let id = spawned.to_string();
    with_caller(caller(&member), async {
        svc.terminal_write(
            id.clone(),
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, "shared\n"),
        )
        .await
        .unwrap();
        svc.terminal_resize(id.clone(), 100, 30).await.unwrap();
        svc.terminal_get_buffer(id.clone(), None).await.unwrap();
        assert!(svc
            .terminal_get_buffer("unknown".into(), None)
            .await
            .is_err());
        assert!(svc
            .terminal_read_output(WorkspaceId::chief(), id.clone(), None, None, None)
            .await
            .is_err());
    })
    .await;
    svc.store.remove_host_member(&member).await.unwrap();
    assert!(with_caller(
        caller(&member),
        svc.terminal_write(id.clone(), "denied".into())
    )
    .await
    .is_err());
    with_caller(Caller::Daemon, svc.terminal_kill(id))
        .await
        .unwrap();
}

#[tokio::test]
async fn member_execution_errors_are_classified_and_sanitized_without_changing_owner_errors() {
    use crate::host_execution::{ai_authorization_error, forge_error, git_error};
    use intent_core::execution::ExecutionAuthorizationReason as Reason;
    use intent_sourcecontrol::Error as ScError;
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    with_caller(
        caller(&member),
        svc.execution_call(async {
            for (error, reason) in [
                (
                    ScError::NotConfigured("secret-missing".into()),
                    Reason::Missing,
                ),
                (ScError::Auth("secret-rejected".into()), Reason::Rejected),
                (
                    ScError::Auth("insufficient_scope secret-scope".into()),
                    Reason::InsufficientScope,
                ),
            ] {
                let error = forge_error(error);
                let auth = error.execution_authorization().unwrap();
                assert_eq!(auth.reason, reason);
                assert_eq!(auth.recovery.actor, "host-owner");
                assert!(error.to_string().contains("connected host"));
                assert!(!error.to_string().contains("secret-"));
                assert!(!json!(auth).to_string().contains("secret-"));
            }
            for error in [
                ScError::Api("network".into()),
                ScError::RateLimited("limit".into()),
                ScError::NotFound("repo".into()),
            ] {
                assert!(forge_error(error).execution_authorization().is_none());
            }
            for (url, hint) in [
                ("https://github.com/org/repo", true),
                ("git@github.com:org/repo", false),
                ("https://gitlab.com/org/repo", false),
                ("/tmp/local", false),
            ] {
                let error = git_error(Error::GitAuthorization("provider-secret".into()), Some(url));
                let auth = error.execution_authorization().unwrap();
                assert_eq!(auth.recovery.setting.is_some(), hint, "{url}");
                assert!(!json!(auth).to_string().contains("org/repo"));
            }
            assert!(git_error(
                Error::Internal("fatal: authentication failed".into()),
                Some("https://github.com/org/repo")
            )
            .execution_authorization()
            .is_none());
            let error = ai_authorization_error(
                Error::InvalidParams("private provider error".into()),
                "codex",
                Reason::Rejected,
            );
            let auth = error.execution_authorization().unwrap();
            assert_eq!(auth.host, None);
            assert_eq!(auth.recovery.setting, None);
            assert_eq!(auth.recovery.action, "check-ai-authorization");
            assert!(!error.to_string().contains("private provider error"));
            Ok(())
        }),
    )
    .await
    .unwrap();
    with_caller(Caller::Daemon,svc.execution_call(async {
        assert!(matches!(forge_error(ScError::Auth("original".into())), Error::Internal(s) if s=="source control auth error: original"));
        assert!(matches!(ai_authorization_error(Error::InvalidParams("original".into()),"codex",Reason::Missing),Error::InvalidParams(s) if s=="original"));
        Ok(())
    })).await.unwrap();
}

#[tokio::test]
async fn member_execution_context_events_cover_policy_setup_and_unchanged_configured_rejection() {
    use crate::events::{EventBus, SubscriptionFilter};
    use intent_core::events::HOST_EXECUTION_CONTEXT_CHANGED;
    use std::{sync::Arc, time::Duration};
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let registry =
        Arc::new(crate::SettingsRegistry::load(tmp.path.with_extension("toml")).unwrap());
    registry
        .apply(&[("sourceControl.github.tokenSource".into(), json!("explicit"))])
        .unwrap();
    let bus = EventBus::new(svc.store.clone());
    let svc = svc
        .with_settings_registry(registry)
        .with_event_bus(bus.clone());
    let worker = svc.spawn_execution_context_loop();
    let mut events = bus.subscribe(SubscriptionFilter {
        event_types: vec![HOST_EXECUTION_CONTEXT_CHANGED.into()],
        ..Default::default()
    });
    let before = svc.execution_context_snapshot().await.unwrap();
    with_caller(
        caller(&member),
        svc.execution_call(async {
            let _ = crate::host_execution::forge_error(intent_sourcecontrol::Error::Auth(
                "private response".into(),
            ));
            Ok(())
        }),
    )
    .await
    .unwrap();
    let batch = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        batch[0].data,
        json!(before),
        "rejection invalidates despite unchanged configured flags"
    );
    with_caller(
        Caller::Daemon,
        svc.settings_update(
            json!([{"path":"sourceControl.github.exposeGitCredentialToChildren","value":false}]),
        ),
    )
    .await
    .unwrap();
    let batch = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        batch.last().unwrap().data["gitCredentialPolicy"]["managedHelperEnabled"],
        false
    );
    with_caller(
        caller(&member),
        svc.observe_execution_readiness(
            json!({"providers":[{"id":"codex","authenticated":false,"secret":"no-leak"}]}),
        ),
    )
    .await
    .unwrap();
    let batch = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(!batch[0].data.to_string().contains("no-leak"));
    worker.abort();
    let _ = worker.await;
}

#[tokio::test]
async fn member_import_handles_remain_owned_by_the_authenticated_initiator() {
    use base64::Engine as _;
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let manifest = json!({"formatVersion":intent_core::transfer::TRANSFER_FORMAT_VERSION,
        "creatingIntentdVersion":env!("CARGO_PKG_VERSION"),"workspaceId":"member-import",
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
    let mut second = svc.store.get_primary_principal().await.unwrap();
    second.id = PrincipalId::new();
    second.is_primary = false;
    svc.store.upsert_principal(&second).await.unwrap();
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
        .bind(second.id.as_str())
        .bind(now_iso())
        .execute(svc.store.write_pool())
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
    use crate::transfer_export::{ExportSession, ExportState};
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let ws = WorkspaceId::new();
    svc.store.insert_workspace(&workspace(&ws)).await.unwrap();
    let export_id = "member-export".to_string();
    svc.transfer_exports.lock().unwrap().insert(
        export_id.clone(),
        ExportSession {
            initiator: Some(member.clone()),
            workspace_id: ws,
            staging_dir: tmp.path.with_extension("export"),
            state: ExportState::Building { aborted: false },
            wip_paths: vec![],
            max_chunk_bytes: 16,
        },
    );
    let mut other = svc.store.get_principal(&member).await.unwrap();
    other.id = PrincipalId::new();
    svc.store.upsert_principal(&other).await.unwrap();
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
        .bind(other.id.as_str())
        .bind(now_iso())
        .execute(svc.store.write_pool())
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
async fn member_mcp_toggle_is_workspace_scoped_and_global_setup_stays_owner_only() {
    use std::sync::Arc;
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let registry =
        Arc::new(crate::SettingsRegistry::load(tmp.path.with_extension("toml")).unwrap());
    let svc = svc
        .with_settings_registry(registry)
        .with_secret_store(Arc::new(crate::settings::InMemorySecretStore::default()));
    let ws = WorkspaceId::new();
    svc.store.insert_workspace(&workspace(&ws)).await.unwrap();
    with_caller(
        Caller::Daemon,
        svc.mcp_servers_create(
            json!({"id":"shared-tool","transport":"stdio","command":"unused","enabled":false}),
        ),
    )
    .await
    .unwrap();
    with_caller(caller(&member), async {
        for enabled in [false, true] {
            let result = svc
                .mcp_servers_toggle("shared-tool".into(), enabled, Some(ws.clone()))
                .await
                .unwrap();
            assert_eq!(result["workspaceDisabled"], !enabled);
        }
        assert!(matches!(
            svc.mcp_servers_toggle("shared-tool".into(), true, None)
                .await,
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            svc.mcp_servers_delete("shared-tool".into()).await,
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            svc.mcp_oauth_list().await,
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            svc.repo_remove("/ignored".into()).await,
            Err(Error::Forbidden(_))
        ));
    })
    .await;
}
