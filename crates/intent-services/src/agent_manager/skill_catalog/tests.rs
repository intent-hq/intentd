use super::*;
use crate::agent_manager::role_reminder_tests::manager_with;

async fn seed_catalog_session(mgr: &AgentManager, agent: &AgentId, repo: &Path) {
    let mut session = mgr.services.store.get_agent_session(agent).await.unwrap();
    session.provider = Some("codex".into());
    session.acp_session_id = Some("retained-codex-session".into());
    session.metadata = Some(json!({"unrelatedPreference": "keep"}));
    mgr.services
        .store
        .update_agent_session(&session.workspace_id, &session)
        .await
        .unwrap();
    let mut workspace = mgr
        .services
        .store
        .get_workspace(&session.workspace_id)
        .await
        .unwrap();
    workspace.worktree_path = Some(repo.to_string_lossy().into_owned());
    mgr.services
        .store
        .update_workspace(&workspace)
        .await
        .unwrap();
}

fn write_skill(repo: &Path, description: &str) -> PathBuf {
    let path = repo.join(".agents/skills/example/SKILL.md");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        &path,
        format!("---\nname: example\ndescription: {description}\n---\nRead this file.\n"),
    )
    .unwrap();
    path
}

async fn prompt(mgr: &AgentManager, agent: &AgentId) -> String {
    let blocks = mgr
        .build_turn_prompt(
            agent,
            &WorkspaceId::from("ws-1"),
            "normal turn",
            &TurnOptions::default(),
        )
        .await;
    serde_json::to_value(blocks).unwrap()[0]["text"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn delivered(mgr: &AgentManager, agent: &AgentId) {
    let fingerprint = mgr.skill_catalog_pending.lock().unwrap().remove(agent);
    mgr.acknowledge_skill_catalog(agent, &WorkspaceId::from("ws-1"), fingerprint)
        .await;
}

#[tokio::test]
async fn fresh_catalog_is_sent_once_and_recreated_session_keeps_full_prompt() {
    let repo = crate::tests::test_tempdir("intentd-catalog-");
    let skill_path = write_skill(repo.path(), "Original description");
    let (mgr, agent, _db) = manager_with(None, None).await;
    seed_catalog_session(&mgr, &agent, repo.path()).await;
    let workspace = mgr
        .services
        .store
        .get_workspace(&WorkspaceId::from("ws-1"))
        .await
        .unwrap();
    let catalog = crate::rules::skill_catalog_for_workspace(&workspace).await;
    mgr.services
        .store
        .set_agent_session_system_prompt(
            &workspace.id,
            &agent,
            &format!("Other instructions\n\n{catalog}"),
        )
        .await
        .unwrap();
    mgr.arm_first_turn_prepend(&agent, intent_providers::find_provider("codex").unwrap());
    let first = prompt(&mgr, &agent).await;
    assert!(first.contains("Other instructions"));
    assert!(first.contains(skill_path.to_str().unwrap()));
    assert_eq!(first.matches("<available_skills>").count(), 1);
    assert!(!first.contains("Intent skill catalog update"));
    delivered(&mgr, &agent).await;
    assert_eq!(prompt(&mgr, &agent).await, "normal turn");
    mgr.arm_first_turn_prepend(&agent, intent_providers::find_provider("codex").unwrap());
    let recreated = prompt(&mgr, &agent).await;
    assert!(recreated.contains("Other instructions"));
    assert_eq!(recreated.matches("<available_skills>").count(), 1);
}

#[tokio::test]
async fn retained_catalog_refreshes_changed_missing_and_removed_skills_without_history_replay() {
    let repo = crate::tests::test_tempdir("intentd-catalog-");
    let (mgr, agent, _db) = manager_with(None, None).await;
    seed_catalog_session(&mgr, &agent, repo.path()).await;
    mgr.services
        .store
        .set_agent_session_system_prompt(
            &WorkspaceId::from("ws-1"),
            &agent,
            "Old system prompt must not replay",
        )
        .await
        .unwrap();
    mgr.services
        .store
        .append_agent_message(
            &agent,
            "assistant",
            &json!([{"type":"text", "text":"Retained history"}]),
            &now_iso(),
        )
        .await
        .unwrap();
    let before = mgr
        .services
        .store
        .get_agent_messages(&agent, None)
        .await
        .unwrap();

    // Legacy loaded session has no delivery fingerprint and no skills yet.
    let empty = prompt(&mgr, &agent).await;
    assert!(empty.contains("<available_skills>\n</available_skills>"));
    assert!(!empty.contains("Old system prompt"));
    assert!(!empty.contains("Retained history"));
    assert!(!empty.contains("<supervisor>"));
    delivered(&mgr, &agent).await;
    assert_eq!(prompt(&mgr, &agent).await, "normal turn");

    let path = write_skill(repo.path(), "New skill");
    let added = prompt(&mgr, &agent).await;
    assert!(added.contains("<description>New skill</description>"));
    assert!(added.contains(path.to_str().unwrap()));
    delivered(&mgr, &agent).await;

    write_skill(repo.path(), "Updated description");
    let changed = prompt(&mgr, &agent).await;
    assert!(changed.contains("Updated description"));
    assert!(!changed.contains("<description>New skill</description>"));
    delivered(&mgr, &agent).await;
    assert_eq!(prompt(&mgr, &agent).await, "normal turn");

    std::fs::remove_file(path).unwrap();
    assert!(prompt(&mgr, &agent)
        .await
        .contains("<available_skills>\n</available_skills>"));
    delivered(&mgr, &agent).await;
    assert_eq!(prompt(&mgr, &agent).await, "normal turn");
    let after = mgr
        .services
        .store
        .get_agent_messages(&agent, None)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
    let session = mgr.services.store.get_agent_session(&agent).await.unwrap();
    assert_eq!(
        session.acp_session_id.as_deref(),
        Some("retained-codex-session")
    );
    assert_eq!(session.metadata.unwrap()["unrelatedPreference"], "keep");
}

#[tokio::test]
async fn catalog_delivery_survives_manager_restart_but_unacknowledged_updates_retry() {
    let repo = crate::tests::test_tempdir("intentd-catalog-");
    write_skill(repo.path(), "Before restart");
    let (mgr, agent, _db) = manager_with(None, None).await;
    seed_catalog_session(&mgr, &agent, repo.path()).await;
    let first = prompt(&mgr, &agent).await;
    assert!(first.contains("Intent skill catalog update"));
    // A failed/cancelled prompt never acknowledges its staged fingerprint.
    assert_eq!(prompt(&mgr, &agent).await, first);
    delivered(&mgr, &agent).await;

    let restarted = AgentManager::new(mgr.services.clone(), mgr.sink.clone(), 4);
    drop(mgr);
    assert_eq!(prompt(&restarted, &agent).await, "normal turn");
    write_skill(repo.path(), "After restart");
    assert!(prompt(&restarted, &agent).await.contains("After restart"));
}

#[tokio::test]
async fn native_prompt_providers_do_not_receive_catalog_updates() {
    let repo = crate::tests::test_tempdir("intentd-catalog-");
    write_skill(repo.path(), "Must use native prompt");
    let (mgr, agent, _db) = manager_with(None, None).await;
    seed_catalog_session(&mgr, &agent, repo.path()).await;
    let mut session = mgr.services.store.get_agent_session(&agent).await.unwrap();
    session.provider = Some("claude-code".into());
    mgr.services
        .store
        .update_agent_session(&session.workspace_id, &session)
        .await
        .unwrap();
    assert_eq!(prompt(&mgr, &agent).await, "normal turn");
    assert!(mgr.skill_catalog_pending.lock().unwrap().is_empty());
}
