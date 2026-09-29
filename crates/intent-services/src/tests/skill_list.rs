//! Repository-free discovery must preserve workspace authorization and avoid provisioning.
use super::*;
use intent_core::{with_caller, Caller, HostRole, PrincipalId, WorkspaceRole};
use serde_json::json;

#[tokio::test]
async fn skill_list_repo_less_is_empty_without_user_skills_and_does_not_provision() {
    let (_tmp, svc, id, _) = setup("").await;
    let root = WorkspacesRoot::new();
    let svc = svc.with_workspaces_root(root.path().to_path_buf());
    let before = svc.store.get_workspace(&id).await.unwrap();
    let skills = with_caller(Caller::Daemon, svc.skill_list(id.clone()))
        .await
        .expect("authorized repo-less workspace must support skill discovery");
    assert_eq!(skills, json!([]));
    let after = svc.store.get_workspace(&id).await.unwrap();
    assert_eq!(
        serde_json::to_value(after).unwrap(),
        serde_json::to_value(before).unwrap()
    );
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn skill_list_preserves_missing_and_denied_errors() {
    let (_tmp, svc, id, _) = setup("").await;
    let missing = WorkspaceId::new();
    assert!(matches!(
        with_caller(Caller::Daemon, svc.skill_list(missing.clone())).await,
        Err(Error::NotFound(_))
    ));
    let mut guest = svc.store.get_primary_principal().await.unwrap();
    guest.id = PrincipalId::new();
    guest.is_primary = false;
    svc.store.upsert_principal(&guest).await.unwrap();
    let caller = Caller::Wire {
        principal_id: guest.id.clone(),
        host_role: HostRole::Guest,
    };
    for workspace_id in [&id, &missing] {
        assert!(matches!(
            with_caller(caller.clone(), svc.skill_list(workspace_id.clone())).await,
            Err(Error::NotFound(_))
        ));
    }
    svc.store
        .add_workspace_member(&id, &guest.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    assert_eq!(
        with_caller(caller, svc.skill_list(id)).await.unwrap(),
        json!([])
    );
}

#[tokio::test]
async fn skill_list_configured_workspace_keeps_precedence_and_sorting() {
    let (_tmp, svc, id, _) = setup("").await;
    let root = test_tempdir("skill-list-project-");
    for (tier, name, description) in [
        (".agents", "zebra", "base"),
        (".agents", "alpha", "base"),
        (".intent", "alpha", "override"),
    ] {
        let dir = root.path().join(tier).join("skills").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("SKILL.md"),
            format!("---\nname: {name}\ndescription: {description}\n---\nSkill body\n"),
        )
        .unwrap();
    }
    let mut row = svc.store.get_workspace(&id).await.unwrap();
    row.repository_path = Some(root.path().to_string_lossy().into_owned());
    svc.store.update_workspace(&row).await.unwrap();
    let skills = with_caller(Caller::Daemon, svc.skill_list(id))
        .await
        .unwrap();
    assert_eq!(skills.as_array().unwrap().len(), 2);
    assert_eq!(skills[0]["name"], "alpha");
    assert_eq!(skills[0]["description"], "override");
    assert_eq!(skills[0]["scope"], "project");
    assert_eq!(skills[1]["name"], "zebra");
}
