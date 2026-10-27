use crate::test_runtime::capability_fixture as fixture;
use intent_core::{
    AgentCreateExtra, Error, ScriptCreateParams, ScriptMode, WorkspaceApi, WorkspaceRole,
};
use intent_services::feature_api::test_support::capability_fixtures::Role;
use intent_services::feature_api::test_support::fixtures::TempDb;
use serde_json::json;

#[tokio::test]
async fn guest_roles_are_scoped_live_and_do_not_grant_owner_authority() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let svc = &f.services;
    let set = |role| svc.workspace_members_set_role(f.ws.clone(), f.collaborator.clone(), role);
    assert!(matches!(
        f.run(Role::Collaborator, set(WorkspaceRole::Manager)).await,
        Err(Error::Forbidden(_))
    ));
    // Inherited host management does not make an ordinary host member the
    // workspace owner for assigning guest roles.
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES(?,?)")
        .bind(f.outsider.as_str())
        .bind(intent_core::now_iso())
        .execute(svc.store().write_pool())
        .await
        .unwrap();
    assert!(matches!(
        f.run(Role::NonMember, set(WorkspaceRole::Manager)).await,
        Err(Error::NotFound(_))
    ));
    let first = f
        .run(Role::Owner, set(WorkspaceRole::Manager))
        .await
        .unwrap();
    assert_eq!(first["changed"], true);
    assert_eq!(
        f.run(Role::Owner, set(WorkspaceRole::Manager))
            .await
            .unwrap()["changed"],
        false
    );
    assert!(f
        .run(
            Role::Collaborator,
            svc.require_workspace_operator(&f.ws, "agent.create")
        )
        .await
        .unwrap());
    assert!(matches!(
        f.run(
            Role::Collaborator,
            svc.require_workspace_operator(&f.other_ws, "agent.create")
        )
        .await,
        Err(Error::NotFound(_))
    ));
    for what in [
        "terminal.create",
        "workspace.delete",
        "workspace.members.add",
        "script.create",
        "script.run",
    ] {
        assert!(
            matches!(
                f.run(
                    Role::Collaborator,
                    svc.require_workspace_manager(&f.ws, what)
                )
                .await,
                Err(Error::Forbidden(_))
            ),
            "{what}"
        );
    }
    assert!(matches!(
        f.run(Role::Collaborator, svc.require_host_execution("host.exec"))
            .await,
        Err(Error::Forbidden(_))
    ));
    for (target, role) in [
        (&f.owner, WorkspaceRole::Manager),
        (&f.collaborator, WorkspaceRole::Owner),
    ] {
        assert!(matches!(
            f.run(
                Role::Owner,
                svc.workspace_members_set_role(f.ws.clone(), target.clone(), role)
            )
            .await,
            Err(Error::InvalidParams(_))
        ));
    }
    let view = f
        .run(Role::Collaborator, svc.get_workspace(f.ws.clone()))
        .await
        .unwrap();
    let membership = view.membership.unwrap();
    assert_eq!(membership.my_role, Some(WorkspaceRole::Manager));
    assert!(!membership.can_manage);
    let roster = f
        .run(Role::Collaborator, svc.workspace_members_list(f.ws.clone()))
        .await
        .unwrap();
    assert_eq!(
        roster["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["principalId"] == f.collaborator.as_str())
            .unwrap()["role"],
        "manager"
    );
    let script = f
        .run(
            Role::Owner,
            svc.script_create(
                f.ws.clone(),
                ScriptCreateParams {
                    name: "saved".into(),
                    command: "true".into(),
                    mode: ScriptMode::Command,
                    purpose: Some(intent_core::ScriptPurpose::Saved),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
    let id = script["id"].as_str().unwrap().to_owned();
    assert!(f
        .run(Role::Collaborator, svc.script_list(f.ws.clone()))
        .await
        .is_ok());
    assert!(f
        .run(
            Role::Collaborator,
            svc.script_status(f.ws.clone(), id.clone())
        )
        .await
        .is_ok());
    assert!(f
        .run(Role::Collaborator, svc.script_get(f.ws.clone(), id.clone()))
        .await
        .is_ok());
    assert!(matches!(
        f.run(
            Role::Collaborator,
            svc.script_create(f.ws.clone(), ScriptCreateParams::default())
        )
        .await,
        Err(Error::Forbidden(_))
    ));
    // Running a saved command is permitted; creating one-off commands is not.
    let one_off = f
        .run(
            Role::Owner,
            svc.script_create(
                f.ws.clone(),
                ScriptCreateParams {
                    name: "private command".into(),
                    command: "true".into(),
                    mode: ScriptMode::Command,
                    purpose: Some(intent_core::ScriptPurpose::OneOff),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
    let one_off_id = one_off["id"].as_str().unwrap().to_owned();
    for result in [
        f.run(
            Role::Collaborator,
            svc.script_get(f.ws.clone(), one_off_id.clone()),
        )
        .await,
        f.run(
            Role::Collaborator,
            svc.script_run(f.ws.clone(), one_off_id, None, None),
        )
        .await,
    ] {
        assert!(matches!(result, Err(Error::Forbidden(_))));
    }
    for options in [json!({}), json!({"view":"summary","limit":1})] {
        let page = f
            .run(
                Role::Collaborator,
                svc.script_list_view(f.ws.clone(), options),
            )
            .await
            .unwrap();
        assert_eq!(page["scripts"].as_array().unwrap().len(), 1);
        assert_eq!(page["scripts"][0]["id"], id);
        assert!(page.get("nextCursor").is_none());
    }
    for extra in [
        AgentCreateExtra {
            metadata: Some(json!({"permissionMode":"bypassPermissions"})),
            ..Default::default()
        },
        AgentCreateExtra {
            workspace_path: Some("/".into()),
            ..Default::default()
        },
        AgentCreateExtra {
            agent_type: Some("chief".into()),
            ..Default::default()
        },
    ] {
        assert!(matches!(
            f.run(
                Role::Collaborator,
                svc.agent_create(f.ws.clone(), None, None, None, None, None, extra)
            )
            .await,
            Err(Error::Forbidden(_))
        ));
    }
    f.run(Role::Owner, set(WorkspaceRole::Collaborator))
        .await
        .unwrap();
    assert!(matches!(
        f.run(Role::Collaborator, svc.script_status(f.ws.clone(), id))
            .await,
        Err(Error::Forbidden(_))
    ));
    assert!(matches!(
        f.run(
            Role::Collaborator,
            svc.agent_create(
                f.ws.clone(),
                None,
                None,
                None,
                None,
                None,
                AgentCreateExtra::default()
            )
        )
        .await,
        Err(Error::Forbidden(_))
    ));
}

#[tokio::test]
async fn guest_roles_script_list_cannot_import_repository_commands() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let repo = tmp.path.parent().unwrap().join("untrusted-repo");
    std::fs::create_dir_all(repo.join(".intent")).unwrap();
    std::fs::write(
        repo.join(".intent/config.json"),
        r#"{"scripts":[{"name":"unapproved","command":"true","mode":"command"}]}"#,
    )
    .unwrap();
    sqlx::query("UPDATE workspace SET repository_path=? WHERE id=?")
        .bind(repo.to_str().unwrap())
        .bind(f.ws.as_str())
        .execute(f.services.store().write_pool())
        .await
        .unwrap();
    f.run(
        Role::Owner,
        f.services.workspace_members_set_role(
            f.ws.clone(),
            f.collaborator.clone(),
            WorkspaceRole::Manager,
        ),
    )
    .await
    .unwrap();
    let list = f
        .run(Role::Collaborator, f.services.script_list(f.ws.clone()))
        .await
        .unwrap();
    assert_eq!(list["scripts"], json!([]));
    assert!(!f
        .services
        .store()
        .workspace_scripts_initialized(&f.ws)
        .await
        .unwrap());
    let list = f
        .run(Role::Owner, f.services.script_list(f.ws.clone()))
        .await
        .unwrap();
    assert_eq!(list["scripts"].as_array().unwrap().len(), 1);
    let list = f
        .run(Role::Collaborator, f.services.script_list(f.ws.clone()))
        .await
        .unwrap();
    assert_eq!(list["scripts"].as_array().unwrap().len(), 1);
}
