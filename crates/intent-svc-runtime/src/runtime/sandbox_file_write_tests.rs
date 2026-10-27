//! Sandboxed agent file writes stay inside the sandbox (moved from
//! `tests.rs`).

use intent_core::{now_iso, WorkspaceApi, WorkspaceId};
use intent_store::Store;

use intent_services::feature_api::test_support::fixtures::{workspace, TempDb};
use intent_services::feature_api::Services;

#[expect(clippy::similar_names)] // deliberate parallel naming across the scenario's instances
/// Containment integration test: delegate an agent with isolation=cow, perform a
/// file write through the agent-scoped ops path (`caller_agent_id` → `resolve_root`),
/// and assert the write landed in the sandbox and the user's directory is untouched.
#[tokio::test]
async fn file_write_via_sandboxed_agent_is_contained() {
    use crate::sandbox_ops::{provision_sandbox, ProvisionConfig};
    use intent_core::{AgentId, AgentSession, AgentStatus};
    use intent_git::{cow_probe, CowSupport};
    use std::fs;

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();

    // Set up test directories under target/ (same volume)
    let workspace_root = std::env::current_dir()
        .unwrap()
        .ancestors()
        .nth(2) // packages/intentd
        .unwrap()
        .to_path_buf();
    let test_root = workspace_root
        .join("target")
        .join(format!("test-containment-{}", uuid::Uuid::new_v4()));
    let user_dir = test_root.join("user-workspace");
    let workspaces_root = test_root.join("workspaces");

    // Initialize a git repo in the user directory
    fs::create_dir_all(&user_dir).unwrap();
    let repo = git2::Repository::init(&user_dir).unwrap();
    let sig = git2::Signature::now("Test", "test@example.com").unwrap();
    let tree_id = {
        let mut index = repo.index().unwrap();
        index.write_tree().unwrap()
    };
    let tree = repo.find_tree(tree_id).unwrap();
    repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
        .unwrap();

    // Early probe check - skip test if CoW not available
    fs::create_dir_all(&workspaces_root).unwrap();
    let probe = cow_probe(&user_dir, &workspaces_root).unwrap();
    if probe == CowSupport::Unsupported {
        eprintln!("SKIP test (CoW not supported): {user_dir:?} → {workspaces_root:?}");
        let _ = fs::remove_dir_all(&test_root);
        return;
    }

    // Create workspace pointing at user directory
    let mut ws = workspace(&ws_id);
    ws.repository_path = Some(user_dir.to_string_lossy().to_string());
    ws.skip_worktree = true; // direct mode
    store.insert_workspace(&ws).await.expect("insert ws");

    // Create agent
    let agent_id = AgentId::new();
    let agent = AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: agent_id.clone(),
        workspace_id: ws_id.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Sandboxed Agent".to_string(),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        system_prompt: None,
        specialist: None,
        status: AgentStatus::Active,
        is_active: true,
        messages: vec![],
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        attention_request_kind: None,
        attention_request_reason: None,
        attention_request_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        file_blocks: None,
        is_background: false,
        metadata: None,
        created_at: now_iso(),
        updated_at: now_iso(),
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
        notifications_muted: false,
    };
    store
        .insert_agent_session(&agent)
        .await
        .expect("insert agent");

    // Create Services before provisioning so it has access to the store
    let svc = crate::test_runtime::with_runtime(Services::new(store.clone()));

    // Provision sandbox
    let config = ProvisionConfig { workspaces_root };
    let outcome = provision_sandbox(&store, &ws_id, &agent_id, &config)
        .await
        .expect("provision");
    let crate::sandbox_ops::ProvisionOutcome::Supported {
        path: sandbox_path, ..
    } = outcome
    else {
        panic!("Expected Supported after probe confirmed CoW available");
    };

    // Update agent session with sandbox path (simulates delegate flow)
    let mut updated_agent = store.get_agent_session(&agent_id).await.unwrap();
    updated_agent.sandbox_path = Some(sandbox_path.to_string_lossy().to_string());
    updated_agent.updated_at = now_iso();
    store
        .update_agent_session(&ws_id, &updated_agent)
        .await
        .unwrap();

    // Write a file via the sandboxed agent (caller_agent_id triggers resolve_root to use sandbox_path)
    let written = svc
        .file_write(
            ws_id.clone(),
            "contained.txt".to_string(),
            "sandboxed write".to_string(),
            Some(agent_id.clone()),
        )
        .await
        .expect("write via sandbox agent");

    assert_eq!(
        written,
        serde_json::json!({ "ok": true, "path": "contained.txt", "size": 15 })
    );

    // Assert the write landed in the sandbox
    let sandbox_file = sandbox_path.join("contained.txt");
    assert!(
        sandbox_file.exists(),
        "Write must land in sandbox: {sandbox_file:?}"
    );
    let sandbox_content = fs::read_to_string(&sandbox_file).unwrap();
    assert_eq!(sandbox_content, "sandboxed write");

    // Assert the user's directory is completely untouched
    let user_file = user_dir.join("contained.txt");
    assert!(
        !user_file.exists(),
        "User directory must remain untouched: {user_file:?}"
    );

    // ws.file.getAttachment for a SANDBOXED caller: the source is the
    // CANONICAL store (attachments never exist in the clone), the copy
    // lands in the caller's sandbox, and the canonical checkout gains no
    // new copy (PROTOCOL §6.8).
    {
        use base64::Engine as _;
        let placed = svc
            .file_place_attachment(
                ws_id.clone(),
                "brief.md".to_string(),
                Some(base64::engine::general_purpose::STANDARD.encode(b"# brief")),
                None,
                None,
                None,
            )
            .await
            .expect("place attachment");
        let att_id = placed["attachmentId"].as_str().unwrap().to_string();
        // The placement landed canonically, not in the sandbox.
        assert!(user_dir.join(".intent/attachments/brief.md").exists());
        assert!(!sandbox_path.join(".intent/attachments/brief.md").exists());

        let got = svc
            .file_get_attachment(ws_id.clone(), att_id, Some(agent_id.clone()), None)
            .await
            .expect("get attachment via sandboxed caller");
        assert_eq!(
            got["path"],
            serde_json::json!(".intent/attachments/brief.md")
        );
        let sandbox_copy = sandbox_path.join(".intent/attachments/brief.md");
        assert!(
            sandbox_copy.exists(),
            "copy must land in the sandbox: {sandbox_copy:?}"
        );
        assert_eq!(fs::read(&sandbox_copy).unwrap(), b"# brief");
    }

    // Clean up
    let _ = fs::remove_dir_all(&test_root);
}
