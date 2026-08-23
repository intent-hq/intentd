//! Integration tests for sandbox merge-back on agent completion (completion-interception path).
//!
//! These tests exercise the `handle_completion_event` → `handle_sandbox_merge_on_completion` wiring:
//! - (a) Clean merge → completion propagates with merged status; sandbox:cow:merged event; commits in canonical
//! - (b) Conflict → completion NOT delivered; bounce message queued; canonical pristine; `retry_count` incremented
//! - (c) Retry cap exhausted → completion propagates with merge-pending
//! - (d) Bounce refreshes sandbox with canonical HEAD without re-provisioning

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use git2::Repository;
    use intent_core::{
        now_iso, ActorType, AgentId, AgentSession, AgentStatus, Event, EventActor, WorkspaceId,
        WorkspaceStatus,
    };
    use intent_git::{cow_probe, CowSupport};
    use intent_store::{SandboxStatus, Store};
    use serde_json::json;

    use crate::events::{EventBus, SubscriptionFilter};
    use crate::sandbox_ops::{provision_sandbox, ProvisionConfig, ProvisionOutcome};
    use crate::Services;

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("completion-test-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ = fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
        }
    }

    async fn temp_store() -> (Store, TempDb) {
        let db = TempDb::new();
        let store = Store::open(&db.path).await.unwrap();
        (store, db)
    }

    /// Create a test repo under target/ for same-volume `CoW`.
    fn temp_repo_in_target(name: &str) -> (PathBuf, PathBuf) {
        let workspace_root = std::env::current_dir()
            .unwrap()
            .ancestors()
            .nth(2) // packages/intentd
            .unwrap()
            .to_path_buf();
        let test_root = workspace_root
            .join("target")
            .join(format!("test-sandbox-{}", uuid::Uuid::new_v4()));
        let repo_path = test_root.join(name);
        init_test_repo(&repo_path);
        (test_root, repo_path)
    }

    fn init_test_repo(repo_path: &Path) {
        fs::create_dir_all(repo_path).unwrap();
        let repo = Repository::init(repo_path).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let tree_id = {
            let mut index = repo.index().unwrap();
            index.write_tree().unwrap()
        };
        let tree = repo.find_tree(tree_id).unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
            .unwrap();
    }

    fn workspace_for_repo(repo_path: &Path) -> intent_core::Workspace {
        let now = now_iso();
        intent_core::Workspace {
            id: WorkspaceId::new(),
            title: "Test WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: intent_core::WorkspaceActivity::Idle,
            attention: intent_core::WorkspaceAttention::None,
            created_at: now.clone(),
            updated_at: now,
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: Some(repo_path.to_string_lossy().to_string()),
            repository_owner: None,
            repository_name: Some("test-repo".to_string()),
            worktree_path: None,
            scope: None,
            skip_worktree: true,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            execution_environment: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    async fn create_agent_session(
        store: &Store,
        ws_id: &WorkspaceId,
        agent_id: &AgentId,
        parent_id: Option<&AgentId>,
        sandbox_path: Option<String>,
    ) {
        let agent = AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            parent_agent_id: parent_id.cloned(),
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
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
            sandbox_path: sandbox_path.clone(),
            sandbox_branch: sandbox_path.as_ref().map(|_| format!("sb/{}", agent_id.0)),
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
        };
        store.insert_agent_session(&agent).await.unwrap();
    }

    /// Build an agent:idle completion event for the given agent.
    fn completion_event(ws_id: &WorkspaceId, agent_id: &AgentId) -> Event {
        Event {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: ws_id.clone(),
            timestamp: now_iso(),
            event_type: "agent:idle".to_string(),
            actor: EventActor {
                actor_type: ActorType::Agent,
                id: Some(agent_id.0.clone()),
                name: None,
                email: None,
                metadata: None,
                model: None,
            },
            session_id: Some(agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({
                "agentId": agent_id.0,
            }),
        }
    }

    /// Subscribe to sandbox:cow:merged events.
    fn subscribe_to_sandbox_merged(
        bus: &EventBus,
        ws_id: &WorkspaceId,
    ) -> crate::events::Subscription {
        let filter = SubscriptionFilter {
            event_types: vec!["sandbox:cow:merged".to_string()],
            workspace_id: Some(ws_id.0.clone()),
            batch_window: None,
            ..Default::default()
        };
        bus.subscribe(filter)
    }

    #[tokio::test]
    async fn test_clean_merge_propagates_completion_and_emits_event() {
        // Scenario (a): Clean merge → completion propagates with merged status;
        // sandbox:cow:merged event emitted; agent commits present in canonical.

        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("clean-merge");
        let workspaces_root = test_root.join("workspaces");
        fs::create_dir_all(&workspaces_root).unwrap();

        // Probe CoW support
        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        // Create workspace and agents (child + parent coordinator)
        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        let child_id = AgentId::from("agent-child");
        let parent_id = AgentId::from("agent-parent");

        // Provision sandbox for child
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        create_agent_session(&store, &ws.id, &child_id, Some(&parent_id), None).await;
        let outcome = provision_sandbox(&store, &ws.id, &child_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };

        // Update agent session with sandbox_path
        let mut session = store.get_agent_session(&child_id).await.unwrap();
        session.sandbox_path = Some(sandbox_path.to_string_lossy().to_string());
        session.sandbox_branch = Some(format!("sb/{}", child_id.0));
        store.update_agent_session(&ws.id, &session).await.unwrap();

        // Create parent agent session
        create_agent_session(&store, &ws.id, &parent_id, None, None).await;

        // Make a clean commit in the sandbox
        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        fs::write(sandbox_path.join("new_file.txt"), "content").unwrap();
        let mut index = sandbox_repo.index().unwrap();
        index.add_path(Path::new("new_file.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = sandbox_repo.find_tree(tree_oid).unwrap();
        let parent_commit = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Agent work",
                &tree,
                &[&parent_commit],
            )
            .unwrap();

        // Wire up Services with EventBus
        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_workspaces_root(workspaces_root.clone());

        // Register a completion watch: parent watches child
        services
            .register_completion_watch(
                &ws.id,
                &ws.id,
                parent_id.clone(),
                "Parent".to_string(),
                child_id.clone(),
                None, // no group
            )
            .expect("register watch");

        // Subscribe to sandbox:cow:merged events
        let mut merged_sub = subscribe_to_sandbox_merged(&bus, &ws.id);

        // Trigger the completion-interception path
        let event = completion_event(&ws.id, &child_id);
        services.handle_completion_event(&event).await;

        // Assert: completion WAS delivered to parent (clean merge allows propagation)
        // The parent should have received a wake message
        let parent_messages = store.get_agent_messages(&parent_id, None).await.unwrap();
        let parent_user_messages: Vec<_> = parent_messages
            .iter()
            .filter(|m| m.role == "user")
            .collect();
        assert!(
            !parent_user_messages.is_empty(),
            "Parent should have received a wake message on clean merge"
        );
        assert!(
            parent_user_messages.iter().any(|m| m
                .content
                .to_string()
                .contains("Sandbox merged into the workspace repo")),
            "Wake must carry the merged-sandbox annotation: {:?}",
            parent_user_messages
                .iter()
                .map(|m| m.content.to_string())
                .collect::<Vec<_>>()
        );

        // Assert: sandbox:cow:merged event was emitted
        let merged_event =
            tokio::time::timeout(std::time::Duration::from_secs(1), merged_sub.recv()).await;
        assert!(
            merged_event.is_ok(),
            "sandbox:cow:merged event should be emitted"
        );
        let merged_batch = merged_event.unwrap().unwrap();
        assert_eq!(merged_batch.len(), 1);
        assert_eq!(merged_batch[0].event_type, "sandbox:cow:merged");

        // Assert: sandbox persists (agent-lifetime lifecycle) with status
        // Merged and the merged tip recorded for the next incremental merge.
        let sandbox = store.get_sandbox(&ws.id, &child_id).await.unwrap();
        let sandbox = sandbox.expect("Sandbox must persist after merge");
        assert_eq!(sandbox.status, SandboxStatus::Merged);
        assert!(
            sandbox.last_merged_commit_sha.is_some(),
            "Merged tip must be recorded for incremental repeat merges"
        );
        assert!(
            sandbox_path.exists(),
            "Sandbox directory must persist after merge"
        );

        // Assert: session sandbox linkage kept — the agent's next turn reuses
        // the same sandbox.
        let session = store.get_agent_session(&child_id).await.unwrap();
        assert!(
            session.sandbox_path.is_some() && session.sandbox_branch.is_some(),
            "Session sandbox fields must persist after merge"
        );

        // Assert: agent commits are in canonical
        let _canonical_repo = Repository::open(&repo_path).unwrap();
        assert!(
            repo_path.join("new_file.txt").exists(),
            "New file should be in canonical"
        );

        // Assert: no bounce message queued
        let messages = store.get_agent_messages(&child_id, None).await.unwrap();
        let user_messages: Vec<_> = messages.iter().filter(|m| m.role == "user").collect();
        assert_eq!(
            user_messages.len(),
            0,
            "No bounce message should be queued on clean merge"
        );

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_top_level_agent_sandbox_merges_on_turn_end() {
        // Uniform per-agent isolation (executionEnvironment=cow): a TOP-LEVEL
        // agent — no parent, no completion watch — with a sandbox still gets
        // the turn-end merge-back: its commit lands in the canonical checkout,
        // the sandbox transitions to Merged, and sandbox:cow:merged fires.

        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("top-level-merge");
        let workspaces_root = test_root.join("workspaces");
        fs::create_dir_all(&workspaces_root).unwrap();

        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        let mut ws = workspace_for_repo(&repo_path);
        ws.execution_environment = Some(intent_core::SandboxType::Cow);
        store.insert_workspace(&ws).await.unwrap();

        let agent_id = AgentId::from("agent-top-level");
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        create_agent_session(&store, &ws.id, &agent_id, None, None).await;
        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };

        let mut session = store.get_agent_session(&agent_id).await.unwrap();
        session.sandbox_path = Some(sandbox_path.to_string_lossy().to_string());
        session.sandbox_branch = Some(format!("sb/{}", agent_id.0));
        store.update_agent_session(&ws.id, &session).await.unwrap();

        // Clean commit in the sandbox
        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        fs::write(sandbox_path.join("top_level.txt"), "top-level work").unwrap();
        let mut index = sandbox_repo.index().unwrap();
        index.add_path(Path::new("top_level.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = sandbox_repo.find_tree(tree_oid).unwrap();
        let parent_commit = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Top-level work",
                &tree,
                &[&parent_commit],
            )
            .unwrap();

        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_workspaces_root(workspaces_root.clone());

        let mut merged_sub = subscribe_to_sandbox_merged(&bus, &ws.id);

        // No completion watch registered: the top-level agent has no parent.
        let event = completion_event(&ws.id, &agent_id);
        services.handle_completion_event(&event).await;

        assert!(
            repo_path.join("top_level.txt").exists(),
            "Top-level agent's commit must land in the canonical checkout"
        );
        let sandbox = store
            .get_sandbox(&ws.id, &agent_id)
            .await
            .unwrap()
            .expect("Sandbox must persist after merge");
        assert_eq!(sandbox.status, SandboxStatus::Merged);
        let merged_event =
            tokio::time::timeout(std::time::Duration::from_secs(1), merged_sub.recv()).await;
        assert!(
            merged_event.is_ok(),
            "sandbox:cow:merged event should be emitted for a top-level agent"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_conflict_suppresses_completion_and_bounces_agent() {
        // Scenario (b): Conflict → completion NOT delivered to parent;
        // agent has queued bounce message with conflicting paths;
        // canonical pristine; sandbox status conflict_bounced; retry_count incremented.

        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("conflict");
        let workspaces_root = test_root.join("workspaces");
        fs::create_dir_all(&workspaces_root).unwrap();

        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        // Create workspace and agents
        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        let child_id = AgentId::from("agent-child");
        let parent_id = AgentId::from("agent-parent");

        // Provision sandbox
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        create_agent_session(&store, &ws.id, &child_id, Some(&parent_id), None).await;
        let outcome = provision_sandbox(&store, &ws.id, &child_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };

        // Update agent session with sandbox_path
        let mut session = store.get_agent_session(&child_id).await.unwrap();
        session.sandbox_path = Some(sandbox_path.to_string_lossy().to_string());
        session.sandbox_branch = Some(format!("sb/{}", child_id.0));
        store.update_agent_session(&ws.id, &session).await.unwrap();

        create_agent_session(&store, &ws.id, &parent_id, None, None).await;

        // Create conflicting changes: both canonical and sandbox modify same file
        let canonical_repo = Repository::open(&repo_path).unwrap();
        fs::write(repo_path.join("conflict.txt"), "canonical version").unwrap();
        let mut canonical_index = canonical_repo.index().unwrap();
        canonical_index.add_path(Path::new("conflict.txt")).unwrap();
        canonical_index.write().unwrap();
        let canonical_tree_oid = canonical_index.write_tree().unwrap();
        let canonical_tree = canonical_repo.find_tree(canonical_tree_oid).unwrap();
        let canonical_parent = canonical_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        canonical_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Canonical change",
                &canonical_tree,
                &[&canonical_parent],
            )
            .unwrap();

        // Sandbox modifies the same file differently
        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        fs::write(sandbox_path.join("conflict.txt"), "sandbox version").unwrap();
        let mut sandbox_index = sandbox_repo.index().unwrap();
        sandbox_index.add_path(Path::new("conflict.txt")).unwrap();
        sandbox_index.write().unwrap();
        let sandbox_tree_oid = sandbox_index.write_tree().unwrap();
        let sandbox_tree = sandbox_repo.find_tree(sandbox_tree_oid).unwrap();
        let sandbox_parent = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Sandbox change",
                &sandbox_tree,
                &[&sandbox_parent],
            )
            .unwrap();

        // Wire up Services with EventBus
        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_workspaces_root(workspaces_root.clone());

        // Register a completion watch: parent watches child
        services
            .register_completion_watch(
                &ws.id,
                &ws.id,
                parent_id.clone(),
                "Parent".to_string(),
                child_id.clone(),
                None,
            )
            .expect("register watch");

        // Count parent messages before
        let parent_messages_before = store.get_agent_messages(&parent_id, None).await.unwrap();
        let parent_user_msg_count_before = parent_messages_before
            .iter()
            .filter(|m| m.role == "user")
            .count();

        // Trigger the completion-interception path
        let event = completion_event(&ws.id, &child_id);
        services.handle_completion_event(&event).await;

        // Assert: completion was NOT delivered (conflict suppresses propagation)
        // Parent should NOT have received a new wake message
        let parent_messages_after = store.get_agent_messages(&parent_id, None).await.unwrap();
        let parent_user_msg_count_after = parent_messages_after
            .iter()
            .filter(|m| m.role == "user")
            .count();
        assert_eq!(
            parent_user_msg_count_after, parent_user_msg_count_before,
            "Parent should NOT receive wake on conflict"
        );

        // Assert: bounce message was queued for the agent
        let messages = store.get_agent_messages(&child_id, None).await.unwrap();
        let user_messages: Vec<_> = messages.iter().filter(|m| m.role == "user").collect();
        assert!(!user_messages.is_empty(), "Bounce message should be queued");
        let last_msg = user_messages.last().unwrap();
        let content_str = last_msg.content.to_string();
        assert!(
            content_str.contains("Merge conflict detected"),
            "Bounce message should mention conflict"
        );
        assert!(
            content_str.contains("conflict.txt"),
            "Bounce message should list conflicting file"
        );

        // Assert: canonical is pristine (unchanged by merge attempt)
        let canonical_content = fs::read_to_string(repo_path.join("conflict.txt")).unwrap();
        assert_eq!(
            canonical_content, "canonical version",
            "Canonical should be pristine"
        );

        // Assert: sandbox status is ConflictBounced with the conflicting
        // paths persisted on the row (pollers see WHY it bounced)
        let sandbox = store.get_sandbox(&ws.id, &child_id).await.unwrap().unwrap();
        assert_eq!(sandbox.status, SandboxStatus::ConflictBounced);
        assert_eq!(
            sandbox.conflicting_paths,
            vec!["conflict.txt".to_string()],
            "Conflicting paths persisted on the bounced row"
        );

        // Assert: retry_count was incremented
        assert_eq!(
            sandbox.retry_count, 1,
            "Retry count should be 1 after first bounce"
        );

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    /// Wait for an `agent:status-changed` event carrying `status` for
    /// `agent_id` (mirrors the DELIV-1 helper in `agent_ops::tests`).
    async fn expect_status(
        sub: &mut crate::events::Subscription,
        agent_id: &AgentId,
        status: &str,
        within: std::time::Duration,
    ) -> bool {
        let deadline = std::time::Instant::now() + within;
        loop {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            match tokio::time::timeout(remaining, sub.recv()).await {
                Ok(Some(batch)) => {
                    for ev in batch {
                        if ev.event_type == "agent:status-changed"
                            && ev.data.get("agentId").and_then(serde_json::Value::as_str)
                                == Some(agent_id.0.as_str())
                            && ev.data.get("status").and_then(serde_json::Value::as_str)
                                == Some(status)
                        {
                            return true;
                        }
                    }
                }
                _ => return false,
            }
        }
    }

    #[tokio::test]
    async fn test_conflict_bounce_resumes_agent_via_runtime() {
        // Regression (dev-seat merge-verification round): a conflict bounce
        // must RESUME the bounced agent, not merely append a transcript row.
        // With the runtime AgentManager attached, bounce delivery must route
        // through the manager (slot claim + worker spawn) — proven by the
        // `agent:status-changed[active]` event the `try_begin` claim emits.
        // The pre-fix path called the store-only `agent_send_message_op`, so
        // the conflict instructions sat unread and the sandbox stayed
        // `conflict_bounced` until the sweep cap exhausted.

        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("bounce-resume");
        let workspaces_root = test_root.join("workspaces");
        fs::create_dir_all(&workspaces_root).unwrap();

        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        let child_id = AgentId::from("agent-child");
        let parent_id = AgentId::from("agent-parent");

        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        create_agent_session(&store, &ws.id, &child_id, Some(&parent_id), None).await;
        let outcome = provision_sandbox(&store, &ws.id, &child_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };

        let mut session = store.get_agent_session(&child_id).await.unwrap();
        session.sandbox_path = Some(sandbox_path.to_string_lossy().to_string());
        session.sandbox_branch = Some(format!("sb/{}", child_id.0));
        store.update_agent_session(&ws.id, &session).await.unwrap();

        create_agent_session(&store, &ws.id, &parent_id, None, None).await;

        // Conflicting commits: canonical and sandbox modify the same file.
        let canonical_repo = Repository::open(&repo_path).unwrap();
        fs::write(repo_path.join("conflict.txt"), "canonical version").unwrap();
        let mut canonical_index = canonical_repo.index().unwrap();
        canonical_index.add_path(Path::new("conflict.txt")).unwrap();
        canonical_index.write().unwrap();
        let canonical_tree_oid = canonical_index.write_tree().unwrap();
        let canonical_tree = canonical_repo.find_tree(canonical_tree_oid).unwrap();
        let canonical_parent = canonical_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        canonical_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Canonical change",
                &canonical_tree,
                &[&canonical_parent],
            )
            .unwrap();

        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        fs::write(sandbox_path.join("conflict.txt"), "sandbox version").unwrap();
        let mut sandbox_index = sandbox_repo.index().unwrap();
        sandbox_index.add_path(Path::new("conflict.txt")).unwrap();
        sandbox_index.write().unwrap();
        let sandbox_tree_oid = sandbox_index.write_tree().unwrap();
        let sandbox_tree = sandbox_repo.find_tree(sandbox_tree_oid).unwrap();
        let sandbox_parent = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Sandbox change",
                &sandbox_tree,
                &[&sandbox_parent],
            )
            .unwrap();

        // Wire Services WITH the runtime AgentManager attached.
        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_workspaces_root(workspaces_root.clone());
        let sink: Arc<dyn intent_acp::EventSink> = Arc::new(crate::BusEventSink::new(bus.clone()));
        let manager = Arc::new(crate::agent_manager::AgentManager::new(
            services.clone(),
            sink,
            4,
        ));
        services.attach_agent_manager(&manager);

        // Subscribe BEFORE the op so the live-only broadcast is captured.
        let mut status_sub = bus.subscribe(SubscriptionFilter {
            event_types: vec!["agent:status-changed".to_string()],
            ..Default::default()
        });

        let event = completion_event(&ws.id, &child_id);
        services.handle_completion_event(&event).await;

        // The bounce landed: status conflict_bounced, retry consumed.
        let sandbox = store.get_sandbox(&ws.id, &child_id).await.unwrap().unwrap();
        assert_eq!(sandbox.status, SandboxStatus::ConflictBounced);
        assert_eq!(sandbox.retry_count, 1);

        // The bounced agent MUST be resumed: the runtime slot claim emits
        // `agent:status-changed[active]` for the child.
        assert!(
            expect_status(
                &mut status_sub,
                &child_id,
                "active",
                std::time::Duration::from_secs(3)
            )
            .await,
            "conflict bounce must drive a turn via the runtime AgentManager"
        );

        manager.stop(&child_id).await;
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_retry_cap_exhausted_propagates_with_terminal_conflict() {
        // Scenario (c): Retry cap exhausted (retry_count >= 2) →
        // completion DOES propagate, and the conflict is TERMINAL: status
        // `conflict` with conflictingPaths persisted and the sandbox's
        // commits preserved on its sb/<agentId> branch in canonical.

        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("retry-cap");
        let workspaces_root = test_root.join("workspaces");
        fs::create_dir_all(&workspaces_root).unwrap();

        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        // Create workspace and agents
        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        let child_id = AgentId::from("agent-child");
        let parent_id = AgentId::from("agent-parent");

        // Provision sandbox
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        create_agent_session(&store, &ws.id, &child_id, Some(&parent_id), None).await;
        let outcome = provision_sandbox(&store, &ws.id, &child_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };

        let mut session = store.get_agent_session(&child_id).await.unwrap();
        session.sandbox_path = Some(sandbox_path.to_string_lossy().to_string());
        session.sandbox_branch = Some(format!("sb/{}", child_id.0));
        store.update_agent_session(&ws.id, &session).await.unwrap();

        create_agent_session(&store, &ws.id, &parent_id, None, None).await;

        // Set retry_count to 2 (at cap) manually in the store
        store
            .increment_sandbox_retry_count(&ws.id, &child_id)
            .await
            .unwrap();
        store
            .increment_sandbox_retry_count(&ws.id, &child_id)
            .await
            .unwrap();

        // Create conflicting changes
        let canonical_repo = Repository::open(&repo_path).unwrap();
        fs::write(repo_path.join("file.txt"), "canonical").unwrap();
        let mut canonical_index = canonical_repo.index().unwrap();
        canonical_index.add_path(Path::new("file.txt")).unwrap();
        canonical_index.write().unwrap();
        let canonical_tree_oid = canonical_index.write_tree().unwrap();
        let canonical_tree = canonical_repo.find_tree(canonical_tree_oid).unwrap();
        let canonical_parent = canonical_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        canonical_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Canonical",
                &canonical_tree,
                &[&canonical_parent],
            )
            .unwrap();

        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        fs::write(sandbox_path.join("file.txt"), "sandbox").unwrap();
        let mut sandbox_index = sandbox_repo.index().unwrap();
        sandbox_index.add_path(Path::new("file.txt")).unwrap();
        sandbox_index.write().unwrap();
        let sandbox_tree_oid = sandbox_index.write_tree().unwrap();
        let sandbox_tree = sandbox_repo.find_tree(sandbox_tree_oid).unwrap();
        let sandbox_parent = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Sandbox",
                &sandbox_tree,
                &[&sandbox_parent],
            )
            .unwrap();

        // Wire up Services
        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_workspaces_root(workspaces_root.clone());

        // Register a completion watch: parent watches child
        services
            .register_completion_watch(
                &ws.id,
                &ws.id,
                parent_id.clone(),
                "Parent".to_string(),
                child_id.clone(),
                None,
            )
            .expect("register watch");

        // Trigger completion
        let event = completion_event(&ws.id, &child_id);
        services.handle_completion_event(&event).await;

        // Assert: completion WAS delivered (retry cap forces propagation)
        // The parent should have received a wake message
        let parent_messages = store.get_agent_messages(&parent_id, None).await.unwrap();
        let parent_user_messages: Vec<_> = parent_messages
            .iter()
            .filter(|m| m.role == "user")
            .collect();
        assert!(
            !parent_user_messages.is_empty(),
            "Parent should receive wake when retry cap exhausted"
        );

        // Assert: terminal conflict with paths persisted
        let sandbox = store.get_sandbox(&ws.id, &child_id).await.unwrap().unwrap();
        assert_eq!(sandbox.status, SandboxStatus::Conflict);
        assert_eq!(sandbox.conflicting_paths, vec!["file.txt".to_string()]);

        // Assert: the sandbox's commits were preserved in canonical on a
        // sb/<agentId>-recovery-<timestamp> branch (canonical worktree
        // untouched).
        let canonical_repo = Repository::open(&repo_path).unwrap();
        let recovery_prefix = format!("sb/{}-recovery-", child_id.0);
        let recovery_tip = canonical_repo
            .branches(Some(git2::BranchType::Local))
            .unwrap()
            .flatten()
            .find_map(|(b, _)| {
                let name = b.name().ok()??.to_string();
                name.starts_with(&recovery_prefix)
                    .then(|| b.get().peel_to_commit().ok())
                    .flatten()
            })
            .expect("recovery branch must exist in canonical");
        assert_eq!(recovery_tip.message().unwrap_or(""), "Sandbox");

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_bounce_refreshes_sandbox_without_reprovisioning() {
        // Scenario (d): Bounce fetches canonical HEAD into sandbox
        // without re-provisioning (same sandbox path; canonical commit visible).

        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("refresh");
        let workspaces_root = test_root.join("workspaces");
        fs::create_dir_all(&workspaces_root).unwrap();

        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        let child_id = AgentId::from("agent-child");
        let parent_id = AgentId::from("agent-parent");

        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        create_agent_session(&store, &ws.id, &child_id, Some(&parent_id), None).await;
        let outcome = provision_sandbox(&store, &ws.id, &child_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };

        let mut session = store.get_agent_session(&child_id).await.unwrap();
        session.sandbox_path = Some(sandbox_path.to_string_lossy().to_string());
        session.sandbox_branch = Some(format!("sb/{}", child_id.0));
        store.update_agent_session(&ws.id, &session).await.unwrap();

        create_agent_session(&store, &ws.id, &parent_id, None, None).await;

        // Make a new commit in canonical AFTER sandbox was provisioned
        let canonical_repo = Repository::open(&repo_path).unwrap();
        fs::write(repo_path.join("new_canonical.txt"), "new canonical content").unwrap();
        let mut canonical_index = canonical_repo.index().unwrap();
        canonical_index
            .add_path(Path::new("new_canonical.txt"))
            .unwrap();
        canonical_index.write().unwrap();
        let canonical_tree_oid = canonical_index.write_tree().unwrap();
        let canonical_tree = canonical_repo.find_tree(canonical_tree_oid).unwrap();
        let canonical_parent = canonical_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let new_canonical_commit = canonical_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "New canonical commit",
                &canonical_tree,
                &[&canonical_parent],
            )
            .unwrap();

        // Create conflicting sandbox commit
        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        fs::write(
            sandbox_path.join("new_canonical.txt"),
            "conflicting sandbox content",
        )
        .unwrap();
        let mut sandbox_index = sandbox_repo.index().unwrap();
        sandbox_index
            .add_path(Path::new("new_canonical.txt"))
            .unwrap();
        sandbox_index.write().unwrap();
        let sandbox_tree_oid = sandbox_index.write_tree().unwrap();
        let sandbox_tree = sandbox_repo.find_tree(sandbox_tree_oid).unwrap();
        let sandbox_parent = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Sandbox conflict",
                &sandbox_tree,
                &[&sandbox_parent],
            )
            .unwrap();

        // Wire up Services
        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_workspaces_root(workspaces_root.clone());

        // Trigger completion (will conflict and bounce)
        let event = completion_event(&ws.id, &child_id);
        services.handle_completion_event(&event).await;

        // Assert: sandbox path still exists (not re-provisioned)
        assert!(sandbox_path.exists(), "Sandbox path should still exist");

        // Assert: canonical HEAD is visible in sandbox as refs/remotes/canonical/HEAD
        let sandbox_repo_after = Repository::open(&sandbox_path).unwrap();
        let canonical_remote = sandbox_repo_after
            .find_reference("refs/remotes/canonical/HEAD")
            .expect("Canonical HEAD should be fetched into sandbox");
        let fetched_commit = canonical_remote.peel_to_commit().unwrap().id();
        assert_eq!(
            fetched_commit, new_canonical_commit,
            "Sandbox should have fetched new canonical commit"
        );

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_bounce_fetches_canonical_tip_from_merge_target_repo() {
        // Regression (dev-seat merge-verification round): the bounce's
        // canonical fetch must come from the SAME repository the merge
        // targets (`resolve_user_directory` — the workspace checkout for
        // CoW/Direct checkout modes), and the bounce message must name the
        // fetched commit. The pre-fix path fetched `repository_path`
        // unconditionally, so a checkout-mode workspace bounced its agent
        // against a stale tip and the conflict never converged.

        let (store, _db) = temp_store().await;
        let (test_root, origin_path) = temp_repo_in_target("bounce-fetch-origin");
        let workspaces_root = test_root.join("workspaces");
        fs::create_dir_all(&workspaces_root).unwrap();

        // The workspace CHECKOUT is a clone of origin: this is the canonical
        // repo sandboxes merge into (worktree_path, checkoutMode=cow).
        let checkout_path = test_root.join("checkout");
        let clone_out = std::process::Command::new("git")
            .arg("clone")
            .arg("--quiet")
            .arg(&origin_path)
            .arg(&checkout_path)
            .output()
            .unwrap();
        assert!(clone_out.status.success(), "git clone must succeed");

        let probe = cow_probe(&checkout_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        let mut ws = workspace_for_repo(&origin_path);
        ws.worktree_path = Some(checkout_path.to_string_lossy().to_string());
        ws.checkout_mode = Some(intent_core::CheckoutMode::Cow);
        store.insert_workspace(&ws).await.unwrap();

        let child_id = AgentId::from("agent-child");
        let parent_id = AgentId::from("agent-parent");

        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        create_agent_session(&store, &ws.id, &child_id, Some(&parent_id), None).await;
        let outcome = provision_sandbox(&store, &ws.id, &child_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };

        let mut session = store.get_agent_session(&child_id).await.unwrap();
        session.sandbox_path = Some(sandbox_path.to_string_lossy().to_string());
        session.sandbox_branch = Some(format!("sb/{}", child_id.0));
        store.update_agent_session(&ws.id, &session).await.unwrap();

        create_agent_session(&store, &ws.id, &parent_id, None, None).await;

        // Conflicting commit lands in the CHECKOUT only — origin stays at
        // the initial commit (the stale tip the pre-fix fetch would grab).
        let checkout_repo = Repository::open(&checkout_path).unwrap();
        fs::write(checkout_path.join("conflict.txt"), "canonical version").unwrap();
        let mut checkout_index = checkout_repo.index().unwrap();
        checkout_index.add_path(Path::new("conflict.txt")).unwrap();
        checkout_index.write().unwrap();
        let checkout_tree_oid = checkout_index.write_tree().unwrap();
        let checkout_tree = checkout_repo.find_tree(checkout_tree_oid).unwrap();
        let checkout_parent = checkout_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let checkout_tip = checkout_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Checkout change",
                &checkout_tree,
                &[&checkout_parent],
            )
            .unwrap();

        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        fs::write(sandbox_path.join("conflict.txt"), "sandbox version").unwrap();
        let mut sandbox_index = sandbox_repo.index().unwrap();
        sandbox_index.add_path(Path::new("conflict.txt")).unwrap();
        sandbox_index.write().unwrap();
        let sandbox_tree_oid = sandbox_index.write_tree().unwrap();
        let sandbox_tree = sandbox_repo.find_tree(sandbox_tree_oid).unwrap();
        let sandbox_parent = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Sandbox change",
                &sandbox_tree,
                &[&sandbox_parent],
            )
            .unwrap();

        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_workspaces_root(workspaces_root.clone());

        let event = completion_event(&ws.id, &child_id);
        services.handle_completion_event(&event).await;

        // Bounced, not merge_pending: the fetch must have succeeded.
        let sandbox = store.get_sandbox(&ws.id, &child_id).await.unwrap().unwrap();
        assert_eq!(sandbox.status, SandboxStatus::ConflictBounced);

        // The fetched tip is the CHECKOUT's head (the merge target), not
        // origin's stale head.
        let sandbox_repo_after = Repository::open(&sandbox_path).unwrap();
        let fetched = sandbox_repo_after
            .find_reference("refs/remotes/canonical/HEAD")
            .expect("canonical HEAD must be fetched into the sandbox")
            .peel_to_commit()
            .unwrap()
            .id();
        assert_eq!(
            fetched, checkout_tip,
            "bounce must fetch the canonical tip from the merge-target repo (workspace checkout)"
        );

        // The bounce message names the exact fetched commit.
        let messages = store.get_agent_messages(&child_id, None).await.unwrap();
        let bounce_text = messages
            .iter()
            .rfind(|m| m.role == "user")
            .expect("bounce message must be delivered")
            .content
            .to_string();
        assert!(
            bounce_text.contains(&checkout_tip.to_string()),
            "bounce message must reference the fetched canonical commit: {bounce_text}"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    /// Provision a sandbox with one clean commit and strand it `merge_pending`,
    /// returning `(test_root, repo_path, sandbox_path, ws, services, bus)`.
    /// Returns `None` when `CoW` is unsupported (test should skip).
    #[allow(clippy::type_complexity)]
    async fn setup_merge_pending_sandbox(
        store: &Store,
        name: &str,
        agent_id: &AgentId,
    ) -> Option<(
        PathBuf,
        PathBuf,
        PathBuf,
        intent_core::Workspace,
        Services,
        EventBus,
    )> {
        let (test_root, repo_path) = temp_repo_in_target(name);
        let workspaces_root = test_root.join("workspaces");
        fs::create_dir_all(&workspaces_root).unwrap();

        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return None;
        }

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        create_agent_session(store, &ws.id, agent_id, None, None).await;
        let outcome = provision_sandbox(store, &ws.id, agent_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };

        let mut session = store.get_agent_session(agent_id).await.unwrap();
        session.sandbox_path = Some(sandbox_path.to_string_lossy().to_string());
        session.sandbox_branch = Some(format!("sb/{}", agent_id.0));
        store.update_agent_session(&ws.id, &session).await.unwrap();

        // Clean commit in the sandbox
        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        fs::write(sandbox_path.join("swept.txt"), "swept content").unwrap();
        let mut index = sandbox_repo.index().unwrap();
        index.add_path(Path::new("swept.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = sandbox_repo.find_tree(tree_oid).unwrap();
        let parent_commit = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Swept work",
                &tree,
                &[&parent_commit],
            )
            .unwrap();

        // Strand the sandbox merge_pending (as the pre-#592 fetch bug did)
        store
            .update_sandbox_status(&ws.id, agent_id, SandboxStatus::MergePending, &now_iso())
            .await
            .unwrap();

        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_workspaces_root(workspaces_root);

        Some((test_root, repo_path, sandbox_path, ws, services, bus))
    }

    #[tokio::test]
    async fn test_sweep_merges_stranded_merge_pending_sandbox() {
        // The sweep finds a merge_pending sandbox with clean work, merges it
        // into canonical, discards it, and emits sandbox:cow:merged — identical
        // bookkeeping to the completion / RPC paths.

        let (store, _db) = temp_store().await;
        let agent_id = AgentId::from("agent-swept");
        let Some((test_root, repo_path, _sandbox_path, ws, services, bus)) =
            setup_merge_pending_sandbox(&store, "sweep-merge", &agent_id).await
        else {
            return;
        };

        let mut merged_sub = subscribe_to_sandbox_merged(&bus, &ws.id);

        let summary = services.sweep_merge_pending_sandboxes().await;
        assert_eq!(summary.merged, 1, "Sweep should merge the stranded sandbox");
        assert_eq!(summary.errors, 0);
        assert_eq!(summary.conflicts, 0);

        // Sandbox persists as merged, commit landed in canonical, event emitted
        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap();
        let sandbox = sandbox.expect("Sandbox must persist after merge");
        assert_eq!(sandbox.status, SandboxStatus::Merged);
        assert!(
            repo_path.join("swept.txt").exists(),
            "Swept commit should land in canonical"
        );
        let merged_event =
            tokio::time::timeout(std::time::Duration::from_secs(1), merged_sub.recv()).await;
        assert!(
            merged_event.is_ok(),
            "sandbox:cow:merged event should be emitted by the sweep"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sweep_wedged_sandbox_does_not_block_other_merges() {
        // Merge-lane independence regression: sandbox X wedged (claimed
        // `merging` with a live-looking timestamp — the watchdog must not
        // touch it, and it never settles) must not stop sandbox Y's clean
        // merge_pending work from landing in the same sweep pass.

        let (store, _db) = temp_store().await;
        let agent_x = AgentId::from("agent-wedged");
        let Some((test_root, repo_path, _sandbox_x, ws, services, _bus)) =
            setup_merge_pending_sandbox(&store, "sweep-lanes", &agent_x).await
        else {
            return;
        };

        // Wedge X: freshly-claimed `merging` (not stale) — skipped by both
        // the watchdog and the merge_pending listing.
        store
            .update_sandbox_status(&ws.id, &agent_x, SandboxStatus::Merging, &now_iso())
            .await
            .unwrap();

        // Second sandbox Y in the SAME workspace with clean, mergeable work.
        let agent_y = AgentId::from("agent-behind");
        let config = ProvisionConfig {
            workspaces_root: test_root.join("workspaces"),
        };
        create_agent_session(&store, &ws.id, &agent_y, None, None).await;
        let outcome = provision_sandbox(&store, &ws.id, &agent_y, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_y, ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };
        let mut session = store.get_agent_session(&agent_y).await.unwrap();
        session.sandbox_path = Some(sandbox_y.to_string_lossy().to_string());
        session.sandbox_branch = Some(format!("sb/{}", agent_y.0));
        store.update_agent_session(&ws.id, &session).await.unwrap();

        let sandbox_repo = Repository::open(&sandbox_y).unwrap();
        fs::write(sandbox_y.join("behind.txt"), "queued work").unwrap();
        let mut index = sandbox_repo.index().unwrap();
        index.add_path(Path::new("behind.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = sandbox_repo.find_tree(tree_oid).unwrap();
        let parent_commit = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Behind work",
                &tree,
                &[&parent_commit],
            )
            .unwrap();
        store
            .update_sandbox_status(&ws.id, &agent_y, SandboxStatus::MergePending, &now_iso())
            .await
            .unwrap();

        let summary = services.sweep_merge_pending_sandboxes().await;
        assert_eq!(summary.merged, 1, "Y must land despite X being wedged");
        assert!(
            repo_path.join("behind.txt").exists(),
            "Y's commit must reach canonical"
        );
        let x = store.get_sandbox(&ws.id, &agent_x).await.unwrap().unwrap();
        assert_eq!(
            x.status,
            SandboxStatus::Merging,
            "Wedged X untouched (fresh claim; watchdog must not reset it)"
        );
        let y = store.get_sandbox(&ws.id, &agent_y).await.unwrap().unwrap();
        assert_eq!(y.status, SandboxStatus::Merged);

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sweep_budget_aborts_wedged_lane_and_completes_pass() {
        // Regression (dev-seat sweep stall): one wedged merge (hung fetch —
        // simulated via the sweep-delay test seam) must not pin the whole
        // sweep pass. The budget aborts the stuck lane (its claim guard
        // resets the row `merging → merge_pending`), the other workspace's
        // lane still lands, and the sweep RETURNS — so the daemon's ticker
        // re-arms instead of stalling forever behind the wedge.

        let (store, _db) = temp_store().await;
        let agent_wedged = AgentId::from("agent-wedged-budget");
        let Some((root_a, _repo_a, _sb_a, ws_a, services, _bus_a)) =
            setup_merge_pending_sandbox(&store, "sweep-budget-a", &agent_wedged).await
        else {
            return;
        };
        let agent_clean = AgentId::from("agent-clean-budget");
        let Some((root_b, repo_b, _sb_b, ws_b, _services_b, _bus_b)) =
            setup_merge_pending_sandbox(&store, "sweep-budget-b", &agent_clean).await
        else {
            let _ = fs::remove_dir_all(&root_a);
            return;
        };

        // Wedge A's merge attempt well past the budget.
        services.set_test_sweep_delay(&agent_wedged, std::time::Duration::from_secs(30));

        let started = std::time::Instant::now();
        let summary = services
            .sweep_merge_pending_sandboxes_with_budget(std::time::Duration::from_millis(1500))
            .await;

        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "sweep must return promptly after the budget, not wait out the wedge"
        );
        assert_eq!(
            summary.merged, 1,
            "the clean lane must land despite the wedge"
        );
        assert_eq!(
            summary.timed_out_lanes, 1,
            "the wedged lane must be aborted"
        );
        assert!(repo_b.join("swept.txt").exists());
        let clean = store
            .get_sandbox(&ws_b.id, &agent_clean)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(clean.status, SandboxStatus::Merged);

        // The aborted lane's claim guard resets the wedged row so the next
        // tick can reclaim it (the reset is spawned from Drop; allow it a
        // moment to run).
        let mut wedged_status = SandboxStatus::Merging;
        for _ in 0..40 {
            wedged_status = store
                .get_sandbox(&ws_a.id, &agent_wedged)
                .await
                .unwrap()
                .unwrap()
                .status;
            if wedged_status == SandboxStatus::MergePending {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(
            wedged_status,
            SandboxStatus::MergePending,
            "claim guard must reset the aborted lane's claim for the next tick"
        );

        let _ = fs::remove_dir_all(&root_a);
        let _ = fs::remove_dir_all(&root_b);
    }

    #[tokio::test]
    async fn test_sweep_selects_retry_count_zero_rows() {
        // Regression (dev-seat merge-verification round): a fresh
        // `merge_pending` row with retry_count=0 must be selected and merged
        // by the sweep — the retry cap only excludes rows AT the cap, and a
        // capped sibling in the same pass must not shadow the fresh row.

        let (store, _db) = temp_store().await;
        let agent_fresh = AgentId::from("agent-retry-zero");
        let Some((test_root, repo_path, _sandbox_path, ws, services, _bus)) =
            setup_merge_pending_sandbox(&store, "sweep-zero", &agent_fresh).await
        else {
            return;
        };
        let fresh = store
            .get_sandbox(&ws.id, &agent_fresh)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            fresh.retry_count, 0,
            "precondition: fresh row at zero retries"
        );

        // Capped sibling in the same workspace lane.
        let agent_capped = AgentId::from("agent-retry-capped");
        let config = ProvisionConfig {
            workspaces_root: test_root.join("workspaces"),
        };
        create_agent_session(&store, &ws.id, &agent_capped, None, None).await;
        let outcome = provision_sandbox(&store, &ws.id, &agent_capped, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported { .. } = outcome else {
            panic!("Expected Supported outcome");
        };
        store
            .update_sandbox_status(
                &ws.id,
                &agent_capped,
                SandboxStatus::MergePending,
                &now_iso(),
            )
            .await
            .unwrap();
        for _ in 0..crate::SANDBOX_MERGE_SWEEP_RETRY_CAP {
            store
                .increment_sandbox_retry_count(&ws.id, &agent_capped)
                .await
                .unwrap();
        }

        let summary = services.sweep_merge_pending_sandboxes().await;
        assert_eq!(summary.merged, 1, "retry_count=0 row must be merged");
        assert_eq!(summary.skipped_capped, 1, "capped sibling skipped");
        assert!(repo_path.join("swept.txt").exists());
        let merged = store
            .get_sandbox(&ws.id, &agent_fresh)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(merged.status, SandboxStatus::Merged);

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sweep_skips_sandbox_at_retry_cap() {
        // A sandbox that already burned the retry cap stays merge_pending for
        // manual sandbox.cow.merge / sandbox.cow.discard — the sweep must not touch it.

        let (store, _db) = temp_store().await;
        let agent_id = AgentId::from("agent-capped");
        let Some((test_root, _repo_path, sandbox_path, ws, services, _bus)) =
            setup_merge_pending_sandbox(&store, "sweep-cap", &agent_id).await
        else {
            return;
        };

        for _ in 0..crate::SANDBOX_MERGE_SWEEP_RETRY_CAP {
            store
                .increment_sandbox_retry_count(&ws.id, &agent_id)
                .await
                .unwrap();
        }

        let summary = services.sweep_merge_pending_sandboxes().await;
        assert_eq!(summary.skipped_capped, 1, "Capped sandbox must be skipped");
        assert_eq!(summary.merged, 0);

        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap().unwrap();
        assert_eq!(
            sandbox.status,
            SandboxStatus::MergePending,
            "Capped sandbox stays merge_pending for manual handling"
        );
        assert!(
            sandbox_path.exists(),
            "Capped sandbox must not be discarded"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sweep_skips_busy_agent() {
        // A sandbox whose agent is mid-turn is skipped — no merge under an
        // active worker; the sandbox stays merge_pending for the next sweep.

        let (store, _db) = temp_store().await;
        let agent_id = AgentId::from("agent-busy");
        let Some((test_root, repo_path, _sandbox_path, ws, services, _bus)) =
            setup_merge_pending_sandbox(&store, "sweep-busy", &agent_id).await
        else {
            return;
        };

        services.set_test_busy(&agent_id, true);

        let summary = services.sweep_merge_pending_sandboxes().await;
        assert_eq!(summary.skipped_busy, 1, "Busy agent must be skipped");
        assert_eq!(summary.merged, 0);

        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap().unwrap();
        assert_eq!(sandbox.status, SandboxStatus::MergePending);
        assert!(
            !repo_path.join("swept.txt").exists(),
            "Canonical must be untouched while the agent is busy"
        );

        // Once the agent goes idle, the next sweep merges it.
        services.set_test_busy(&agent_id, false);
        let summary = services.sweep_merge_pending_sandboxes().await;
        assert_eq!(
            summary.merged, 1,
            "Idle agent's sandbox merges on next sweep"
        );
        assert!(repo_path.join("swept.txt").exists());

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sweep_conflict_lands_terminal_conflict_status() {
        // A conflicting sweep merge lands the TERMINAL `conflict` status
        // immediately (no live agent turn to bounce, and conflicts are
        // deterministic — retrying without canonical changing is useless):
        // conflictingPaths persisted, commits preserved on the sb/<agentId>
        // recovery branch, and the row leaves the retry queue.

        let (store, _db) = temp_store().await;
        let agent_id = AgentId::from("agent-conflicted");
        let Some((test_root, repo_path, sandbox_path, ws, services, _bus)) =
            setup_merge_pending_sandbox(&store, "sweep-conflict", &agent_id).await
        else {
            return;
        };

        // Conflicting canonical commit on the same file the sandbox touched
        let canonical_repo = Repository::open(&repo_path).unwrap();
        fs::write(repo_path.join("swept.txt"), "canonical version").unwrap();
        let mut index = canonical_repo.index().unwrap();
        index.add_path(Path::new("swept.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = canonical_repo.find_tree(tree_oid).unwrap();
        let parent = canonical_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        canonical_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Canonical clash",
                &tree,
                &[&parent],
            )
            .unwrap();

        let summary = services.sweep_merge_pending_sandboxes().await;
        assert_eq!(summary.conflicts, 1, "Conflict should be tallied");
        assert_eq!(summary.merged, 0);

        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap().unwrap();
        assert_eq!(
            sandbox.status,
            SandboxStatus::Conflict,
            "Sweep conflict is terminal"
        );
        assert_eq!(
            sandbox.conflicting_paths,
            vec!["swept.txt".to_string()],
            "Conflicting paths persisted on the row"
        );
        assert!(sandbox_path.exists(), "Sandbox must not be discarded");
        let canonical_content = fs::read_to_string(repo_path.join("swept.txt")).unwrap();
        assert_eq!(
            canonical_content, "canonical version",
            "Canonical must stay pristine on conflict"
        );
        // Work preserved: sb/<agentId>-recovery-<timestamp> branch in
        // canonical carries the sandbox tip.
        let recovery_prefix = format!("sb/{}-recovery-", agent_id.0);
        let recovery_found = canonical_repo
            .branches(Some(git2::BranchType::Local))
            .unwrap()
            .flatten()
            .any(|(b, _)| {
                b.name()
                    .ok()
                    .flatten()
                    .is_some_and(|n| n.starts_with(&recovery_prefix))
                    && b.get().peel_to_commit().is_ok()
            });
        assert!(recovery_found, "recovery branch must exist in canonical");

        // Terminal rows leave the retry queue: the next sweep does not touch it.
        let summary2 = services.sweep_merge_pending_sandboxes().await;
        assert!(
            summary2.is_empty(),
            "Terminal conflict must not be re-swept: {summary2:?}"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sweep_blocked_does_not_consume_retry() {
        // A Blocked outcome (dirty canonical overlapping the sandbox's
        // changes) returns the sandbox to merge_pending WITHOUT consuming a
        // retry — blocked-ness resolves externally and must not burn the cap.

        let (store, _db) = temp_store().await;
        let agent_id = AgentId::from("agent-blocked");
        let Some((test_root, repo_path, sandbox_path, ws, services, _bus)) =
            setup_merge_pending_sandbox(&store, "sweep-blocked", &agent_id).await
        else {
            return;
        };

        // Uncommitted canonical change to the same file the sandbox touched
        // → dirty-overlap Blocked.
        fs::write(repo_path.join("swept.txt"), "uncommitted canonical edit").unwrap();

        let summary = services.sweep_merge_pending_sandboxes().await;
        assert_eq!(summary.blocked, 1, "Blocked should be tallied");
        assert_eq!(summary.merged, 0);
        assert_eq!(summary.conflicts, 0);

        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap().unwrap();
        assert_eq!(
            sandbox.status,
            SandboxStatus::MergePending,
            "Blocked sandbox returns to merge_pending"
        );
        assert_eq!(sandbox.retry_count, 0, "Blocked must NOT consume a retry");
        assert!(sandbox_path.exists(), "Sandbox must not be discarded");

        let _ = fs::remove_dir_all(&test_root);
    }

    /// Provision a sandbox for an agent whose session metadata opts out of
    /// turn-end merges (`mergeOnTurnEnd: false` — the delegate/create path
    /// stamps this), with one clean commit in the sandbox. Returns `None`
    /// when `CoW` is unsupported (test should skip).
    #[allow(clippy::type_complexity)]
    async fn setup_no_merge_sandbox(
        store: &Store,
        name: &str,
        child_id: &AgentId,
        parent_id: &AgentId,
    ) -> Option<(
        PathBuf,
        PathBuf,
        PathBuf,
        intent_core::Workspace,
        Services,
        EventBus,
    )> {
        let (test_root, repo_path) = temp_repo_in_target(name);
        let workspaces_root = test_root.join("workspaces");
        fs::create_dir_all(&workspaces_root).unwrap();

        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return None;
        }

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        // Child session carries the delegate-stamped opt-out metadata BEFORE
        // provisioning so provision_sandbox picks it up.
        create_agent_session(store, &ws.id, child_id, Some(parent_id), None).await;
        let mut session = store.get_agent_session(child_id).await.unwrap();
        session.metadata = Some(json!({ "mergeOnTurnEnd": false }));
        store.update_agent_session(&ws.id, &session).await.unwrap();

        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        let outcome = provision_sandbox(store, &ws.id, child_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };

        let mut session = store.get_agent_session(child_id).await.unwrap();
        session.sandbox_path = Some(sandbox_path.to_string_lossy().to_string());
        session.sandbox_branch = Some(format!("sb/{}", child_id.0));
        store.update_agent_session(&ws.id, &session).await.unwrap();

        create_agent_session(store, &ws.id, parent_id, None, None).await;

        // Clean commit in the sandbox
        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        fs::write(sandbox_path.join("kept.txt"), "kept content").unwrap();
        let mut index = sandbox_repo.index().unwrap();
        index.add_path(Path::new("kept.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = sandbox_repo.find_tree(tree_oid).unwrap();
        let parent_commit = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Kept work",
                &tree,
                &[&parent_commit],
            )
            .unwrap();

        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_workspaces_root(workspaces_root);

        Some((test_root, repo_path, sandbox_path, ws, services, bus))
    }

    #[tokio::test]
    async fn test_merge_on_turn_end_false_skips_completion_merge() {
        // mergeOnTurnEnd=false: completion propagates normally, but NO merge
        // runs — canonical untouched, sandbox intact in its current status,
        // no sandbox:cow:merged event, no bounce message.

        let (store, _db) = temp_store().await;
        let child_id = AgentId::from("agent-nomerge");
        let parent_id = AgentId::from("agent-parent");
        let Some((test_root, repo_path, sandbox_path, ws, services, bus)) =
            setup_no_merge_sandbox(&store, "no-merge", &child_id, &parent_id).await
        else {
            return;
        };

        // The provisioned record carries the opt-out flag.
        let sandbox = store.get_sandbox(&ws.id, &child_id).await.unwrap().unwrap();
        assert!(
            !sandbox.merge_on_turn_end,
            "Sandbox record must persist mergeOnTurnEnd=false from session metadata"
        );

        services
            .register_completion_watch(
                &ws.id,
                &ws.id,
                parent_id.clone(),
                "Parent".to_string(),
                child_id.clone(),
                None,
            )
            .expect("register watch");
        let mut merged_sub = subscribe_to_sandbox_merged(&bus, &ws.id);

        let event = completion_event(&ws.id, &child_id);
        services.handle_completion_event(&event).await;

        // Completion propagated to the parent, annotated with the unmerged
        // sandbox outcome (status + path).
        let parent_messages = store.get_agent_messages(&parent_id, None).await.unwrap();
        assert!(
            parent_messages.iter().any(|m| m.role == "user"),
            "Parent should have received a wake message despite the skipped merge"
        );
        assert!(
            parent_messages
                .iter()
                .filter(|m| m.role == "user")
                .any(|m| {
                    let text = m.content.to_string();
                    text.contains("Sandbox left unmerged") && text.contains("ws.agent.mergeSandbox")
                }),
            "Wake must carry the unmerged-sandbox annotation"
        );

        // No merge happened: canonical untouched, sandbox intact & Created.
        assert!(
            !repo_path.join("kept.txt").exists(),
            "Canonical must be untouched when mergeOnTurnEnd=false"
        );
        let sandbox = store.get_sandbox(&ws.id, &child_id).await.unwrap().unwrap();
        assert_eq!(
            sandbox.status,
            SandboxStatus::Created,
            "Sandbox must stay in its current status (no merging transition)"
        );
        assert!(sandbox_path.exists(), "Sandbox directory must stay live");
        let merged_event =
            tokio::time::timeout(std::time::Duration::from_millis(300), merged_sub.recv()).await;
        assert!(
            merged_event.is_err(),
            "No sandbox:cow:merged event when the merge is skipped"
        );

        // No bounce message queued to the child.
        let messages = store.get_agent_messages(&child_id, None).await.unwrap();
        assert!(
            !messages.iter().any(|m| m.role == "user"),
            "No bounce message should be queued when the merge is skipped"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_manual_merge_works_on_merge_on_turn_end_false_sandbox() {
        // The "decide later" story: after a skipped turn-end merge, the
        // manual sandbox.cow.merge RPC still merges the sandbox.

        use intent_core::WorkspaceApi;

        let (store, _db) = temp_store().await;
        let child_id = AgentId::from("agent-later");
        let parent_id = AgentId::from("agent-parent");
        let Some((test_root, repo_path, _sandbox_path, ws, services, _bus)) =
            setup_no_merge_sandbox(&store, "manual-later", &child_id, &parent_id).await
        else {
            return;
        };

        // Completion runs first (merge skipped).
        let event = completion_event(&ws.id, &child_id);
        services.handle_completion_event(&event).await;
        assert!(!repo_path.join("kept.txt").exists());

        // Manual merge via the sandbox.cow.merge RPC path. ASYNC contract:
        // immediate "started" ack, outcome lands on the sandbox row.
        let result = services
            .sandbox_merge(ws.id.clone(), child_id.clone())
            .await
            .expect("manual merge should start");
        assert_eq!(result["status"], json!("started"));
        let status =
            wait_for_sandbox_status(&store, &ws.id, &child_id, SandboxStatus::Merged).await;
        assert_eq!(status, SandboxStatus::Merged);
        assert!(
            repo_path.join("kept.txt").exists(),
            "Manual merge must land the sandbox commit in canonical"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sweep_skips_merge_on_turn_end_false_sandbox() {
        // A merge_pending sandbox with mergeOnTurnEnd=false (e.g. a failed
        // manual merge) must NOT be auto-retried by the sweep.

        let (store, _db) = temp_store().await;
        let agent_id = AgentId::from("agent-optout");
        let Some((test_root, repo_path, sandbox_path, ws, services, _bus)) =
            setup_merge_pending_sandbox(&store, "sweep-optout", &agent_id).await
        else {
            return;
        };

        // Flip the provisioned record to the opt-out flag (re-insert; there
        // is deliberately no runtime mutator for the flag).
        let mut sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap().unwrap();
        store.delete_sandbox(&ws.id, &agent_id).await.unwrap();
        sandbox.merge_on_turn_end = false;
        store.insert_sandbox(&sandbox).await.unwrap();

        let summary = services.sweep_merge_pending_sandboxes().await;
        assert_eq!(
            summary.skipped_manual_merge, 1,
            "Opt-out sandbox must be skipped by the sweep"
        );
        assert_eq!(summary.merged, 0);

        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap().unwrap();
        assert_eq!(sandbox.status, SandboxStatus::MergePending);
        assert!(
            !repo_path.join("swept.txt").exists(),
            "Canonical must stay untouched"
        );
        assert!(sandbox_path.exists(), "Sandbox must not be discarded");

        let _ = fs::remove_dir_all(&test_root);
    }

    /// Poll until the sandbox reaches `expected` or the deadline passes.
    /// The claim guard's drop reset runs on a detached task, so tests must
    /// wait for it rather than assert immediately.
    async fn wait_for_sandbox_status(
        store: &Store,
        ws_id: &WorkspaceId,
        agent_id: &AgentId,
        expected: SandboxStatus,
    ) -> SandboxStatus {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let status = store
                .get_sandbox(ws_id, agent_id)
                .await
                .unwrap()
                .unwrap()
                .status;
            if status == expected || std::time::Instant::now() > deadline {
                return status;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn test_claim_guard_drop_resets_stranded_merging_row() {
        // Regression for the stranded-`merging` incident: a merge path that
        // claimed the row and then died without persisting a terminal status
        // (dropped future / panic) must not leave the row `merging` — the
        // armed guard's Drop resets it to merge_pending.

        let (store, _db) = temp_store().await;
        let agent_id = AgentId::from("agent-guard-drop");
        let Some((test_root, _repo_path, _sandbox_path, ws, _services, _bus)) =
            setup_merge_pending_sandbox(&store, "guard-drop", &agent_id).await
        else {
            return;
        };

        // Claim the row exactly like a merge path does.
        assert!(store
            .try_transition_sandbox_status(
                &ws.id,
                &agent_id,
                SandboxStatus::MergePending,
                SandboxStatus::Merging,
                &now_iso(),
            )
            .await
            .unwrap());
        let guard = crate::sandbox_ops::MergeClaimGuard::armed(
            store.clone(),
            ws.id.clone(),
            agent_id.clone(),
        );

        // The owning scope dies without disarming (cancellation / panic).
        drop(guard);

        let status =
            wait_for_sandbox_status(&store, &ws.id, &agent_id, SandboxStatus::MergePending).await;
        assert_eq!(
            status,
            SandboxStatus::MergePending,
            "dropped armed guard must reset merging → merge_pending"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_claim_guard_disarm_keeps_terminal_status() {
        // Counterpart: after the merge path persists its terminal status and
        // disarms, dropping the guard must NOT touch the row.

        let (store, _db) = temp_store().await;
        let agent_id = AgentId::from("agent-guard-disarm");
        let Some((test_root, _repo_path, _sandbox_path, ws, _services, _bus)) =
            setup_merge_pending_sandbox(&store, "guard-disarm", &agent_id).await
        else {
            return;
        };

        assert!(store
            .try_transition_sandbox_status(
                &ws.id,
                &agent_id,
                SandboxStatus::MergePending,
                SandboxStatus::Merging,
                &now_iso(),
            )
            .await
            .unwrap());
        let mut guard = crate::sandbox_ops::MergeClaimGuard::armed(
            store.clone(),
            ws.id.clone(),
            agent_id.clone(),
        );

        // Merge finished: terminal status persisted, guard disarmed.
        store
            .update_sandbox_status(&ws.id, &agent_id, SandboxStatus::Merged, &now_iso())
            .await
            .unwrap();
        guard.disarm();
        drop(guard);

        // Give a misbehaving guard time to fire before asserting.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap().unwrap();
        assert_eq!(
            sandbox.status,
            SandboxStatus::Merged,
            "disarmed guard must not touch the terminal status"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_manual_merge_on_claimed_row_returns_in_progress() {
        // sandbox.cow.merge on a row already claimed `merging` is an expected
        // state: a structured { status: "in_progress" } result, not an
        // Error::Internal the caller has to string-match.

        use intent_core::WorkspaceApi;

        let (store, _db) = temp_store().await;
        let agent_id = AgentId::from("agent-claimed");
        let Some((test_root, repo_path, _sandbox_path, ws, services, _bus)) =
            setup_merge_pending_sandbox(&store, "claimed-rpc", &agent_id).await
        else {
            return;
        };

        store
            .update_sandbox_status(&ws.id, &agent_id, SandboxStatus::Merging, &now_iso())
            .await
            .unwrap();

        let result = services
            .sandbox_merge(ws.id.clone(), agent_id.clone())
            .await
            .expect("claimed row must be a typed result, not an error");
        assert_eq!(result["status"], json!("in_progress"));
        assert_eq!(result["ok"], json!(true));
        assert!(
            !repo_path.join("swept.txt").exists(),
            "in_progress result must not merge anything"
        );
        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap().unwrap();
        assert_eq!(
            sandbox.status,
            SandboxStatus::Merging,
            "the in-flight claim must be left alone"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sweep_watchdog_resets_stale_merging_row() {
        // A row stranded `merging` in a LIVE daemon (claim owner lost without
        // the guard firing) is reset to merge_pending by the sweep's watchdog
        // once past the stale threshold — and then merged in the same sweep.

        let (store, _db) = temp_store().await;
        let agent_id = AgentId::from("agent-stale");
        let Some((test_root, repo_path, _sandbox_path, ws, services, _bus)) =
            setup_merge_pending_sandbox(&store, "sweep-stale", &agent_id).await
        else {
            return;
        };

        // Strand it `merging` with a claim timestamp older than the threshold.
        let stale = (time::OffsetDateTime::now_utc()
            - (crate::SANDBOX_MERGING_STALE_AFTER + time::Duration::minutes(1)))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
        store
            .update_sandbox_status(&ws.id, &agent_id, SandboxStatus::Merging, &stale)
            .await
            .unwrap();

        let summary = services.sweep_merge_pending_sandboxes().await;
        assert_eq!(
            summary.merged, 1,
            "watchdog resets the stale row and the same sweep merges it"
        );
        assert!(repo_path.join("swept.txt").exists());

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sweep_watchdog_leaves_fresh_merging_row_alone() {
        // A row claimed `merging` moments ago has a live owner; the watchdog
        // must not steal the claim from an in-flight merge.

        let (store, _db) = temp_store().await;
        let agent_id = AgentId::from("agent-fresh");
        let Some((test_root, repo_path, _sandbox_path, ws, services, _bus)) =
            setup_merge_pending_sandbox(&store, "sweep-fresh", &agent_id).await
        else {
            return;
        };

        store
            .update_sandbox_status(&ws.id, &agent_id, SandboxStatus::Merging, &now_iso())
            .await
            .unwrap();

        let summary = services.sweep_merge_pending_sandboxes().await;
        assert!(summary.is_empty(), "fresh merging row must be untouched");
        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap().unwrap();
        assert_eq!(sandbox.status, SandboxStatus::Merging);
        assert!(!repo_path.join("swept.txt").exists());

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_recover_stranded_merging_sandboxes() {
        // A sandbox stranded `merging` by a daemon that died mid-merge is
        // invisible to the sweep; startup recovery resets it to merge_pending
        // and the next sweep then merges it.

        let (store, _db) = temp_store().await;
        let agent_id = AgentId::from("agent-stranded");
        let Some((test_root, repo_path, _sandbox_path, ws, services, _bus)) =
            setup_merge_pending_sandbox(&store, "sweep-recover", &agent_id).await
        else {
            return;
        };

        // Simulate a crash mid-merge: sandbox left `merging`.
        store
            .update_sandbox_status(&ws.id, &agent_id, SandboxStatus::Merging, &now_iso())
            .await
            .unwrap();

        // The sweep alone cannot see it.
        let summary = services.sweep_merge_pending_sandboxes().await;
        assert!(summary.is_empty(), "Sweep must not see a merging sandbox");

        // Startup recovery resets it, then the sweep merges it.
        let recovered = services.recover_stranded_merging_sandboxes().await;
        assert_eq!(recovered, 1, "Stranded merging sandbox should be recovered");
        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap().unwrap();
        assert_eq!(sandbox.status, SandboxStatus::MergePending);

        let summary = services.sweep_merge_pending_sandboxes().await;
        assert_eq!(summary.merged, 1, "Recovered sandbox merges on next sweep");
        assert!(repo_path.join("swept.txt").exists());

        let _ = fs::remove_dir_all(&test_root);
    }

    /// Provision a sandbox whose worktree is left DIRTY (an uncommitted
    /// file), with a parent watching the child. `providers.active` is pinned
    /// off-auggie so the LLM commit-message generation short-circuits and
    /// the deterministic fallback subject is used.
    async fn setup_dirty_sandbox(
        store: &Store,
        name: &str,
        child_id: &AgentId,
        parent_id: &AgentId,
    ) -> Option<(
        PathBuf,
        PathBuf,
        PathBuf,
        intent_core::Workspace,
        Services,
        EventBus,
    )> {
        let (test_root, repo_path) = temp_repo_in_target(name);
        let workspaces_root = test_root.join("workspaces");
        fs::create_dir_all(&workspaces_root).unwrap();

        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return None;
        }

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();
        store
            .set_setting("providers.active", "\"claude\"")
            .await
            .unwrap();

        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        create_agent_session(store, &ws.id, child_id, Some(parent_id), None).await;
        let outcome = provision_sandbox(store, &ws.id, child_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };

        let mut session = store.get_agent_session(child_id).await.unwrap();
        session.sandbox_path = Some(sandbox_path.to_string_lossy().to_string());
        session.sandbox_branch = Some(format!("sb/{}", child_id.0));
        store.update_agent_session(&ws.id, &session).await.unwrap();

        create_agent_session(store, &ws.id, parent_id, None, None).await;

        // Leave the sandbox worktree DIRTY: an uncommitted file.
        fs::write(sandbox_path.join("dirty.txt"), "uncommitted work").unwrap();

        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_workspaces_root(workspaces_root);

        Some((test_root, repo_path, sandbox_path, ws, services, bus))
    }

    #[tokio::test]
    async fn test_dirty_sandbox_auto_commit_on_commits_and_merges() {
        // Auto-commit ON (default): a dirty sandbox at turn end is committed
        // (fallback subject; LLM unavailable) and merged — completion
        // propagates with the merged annotation and canonical has the file.

        let (store, _db) = temp_store().await;
        let child_id = AgentId::from("agent-dirty-on");
        let parent_id = AgentId::from("agent-parent");
        let Some((test_root, repo_path, _sandbox_path, ws, services, bus)) =
            setup_dirty_sandbox(&store, "dirty-ac-on", &child_id, &parent_id).await
        else {
            return;
        };

        services
            .register_completion_watch(
                &ws.id,
                &ws.id,
                parent_id.clone(),
                "Parent".to_string(),
                child_id.clone(),
                None,
            )
            .expect("register watch");
        let mut merged_sub = subscribe_to_sandbox_merged(&bus, &ws.id);

        let event = completion_event(&ws.id, &child_id);
        services.handle_completion_event(&event).await;

        // Merged: canonical has the previously-uncommitted file.
        assert!(
            repo_path.join("dirty.txt").exists(),
            "Dirty sandbox state must be committed and merged when auto-commit is on"
        );
        let merged_event =
            tokio::time::timeout(std::time::Duration::from_secs(1), merged_sub.recv()).await;
        assert!(merged_event.is_ok(), "sandbox:cow:merged must be emitted");

        // Completion propagated with the merged annotation.
        let parent_messages = store.get_agent_messages(&parent_id, None).await.unwrap();
        assert!(
            parent_messages
                .iter()
                .filter(|m| m.role == "user")
                .any(|m| m.content.to_string().contains("Sandbox merged")),
            "Parent wake must carry the merged annotation"
        );

        // No bounce message to the child.
        let messages = store.get_agent_messages(&child_id, None).await.unwrap();
        assert!(
            !messages.iter().any(|m| m.role == "user"),
            "No bounce message on a successful dirty-commit merge"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_dirty_sandbox_auto_commit_off_bounces_agent() {
        // Auto-commit OFF + dirty sandbox at turn end: no snapshot, no merge,
        // and the turn must NOT complete — the child is bounced with commit
        // instructions, the parent hears nothing, canonical stays untouched.

        let (store, _db) = temp_store().await;
        let child_id = AgentId::from("agent-dirty-off");
        let parent_id = AgentId::from("agent-parent");
        let Some((test_root, repo_path, sandbox_path, ws, services, bus)) =
            setup_dirty_sandbox(&store, "dirty-ac-off", &child_id, &parent_id).await
        else {
            return;
        };
        store
            .set_workspace_auto_commit(&ws.id, false)
            .await
            .unwrap();

        services
            .register_completion_watch(
                &ws.id,
                &ws.id,
                parent_id.clone(),
                "Parent".to_string(),
                child_id.clone(),
                None,
            )
            .expect("register watch");
        let mut merged_sub = subscribe_to_sandbox_merged(&bus, &ws.id);

        let sandbox_before = store.get_sandbox(&ws.id, &child_id).await.unwrap().unwrap();
        let event = completion_event(&ws.id, &child_id);
        services.handle_completion_event(&event).await;

        // Nothing was committed or merged.
        assert!(
            !repo_path.join("dirty.txt").exists(),
            "Canonical must stay untouched when auto-commit is off"
        );
        assert!(
            sandbox_path.join("dirty.txt").exists(),
            "Dirty file must remain uncommitted in the sandbox"
        );
        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        let head_message = sandbox_repo
            .head()
            .unwrap()
            .peel_to_commit()
            .unwrap()
            .message()
            .unwrap()
            .to_string();
        assert!(
            !head_message.contains("Auto-commit"),
            "No snapshot commit may be created when auto-commit is off"
        );
        let merged_event =
            tokio::time::timeout(std::time::Duration::from_millis(300), merged_sub.recv()).await;
        assert!(merged_event.is_err(), "No sandbox:cow:merged event");

        // Completion suppressed: parent got nothing.
        let parent_messages = store.get_agent_messages(&parent_id, None).await.unwrap();
        assert!(
            !parent_messages.iter().any(|m| m.role == "user"),
            "Completion must NOT propagate while the sandbox is dirty"
        );

        // The child was bounced with commit instructions.
        let messages = store.get_agent_messages(&child_id, None).await.unwrap();
        let user_messages: Vec<_> = messages.iter().filter(|m| m.role == "user").collect();
        assert_eq!(user_messages.len(), 1, "Exactly one bounce message");
        let text = user_messages[0].content.to_string();
        assert!(
            text.contains("uncommitted changes") && text.contains("dirty.txt"),
            "Bounce must name the dirty paths: {text}"
        );

        // Sandbox returned to its pre-claim status; no conflict retry consumed.
        let sandbox = store.get_sandbox(&ws.id, &child_id).await.unwrap().unwrap();
        assert_eq!(
            sandbox.status, sandbox_before.status,
            "Sandbox must return to its pre-claim status after a dirty bounce"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_manual_merge_reports_dirty_when_auto_commit_off() {
        // The manual sandbox.cow.merge RPC (ws.agent.mergeSandbox) on a dirty
        // sandbox with auto-commit off: status "dirty" with the paths; nothing
        // committed or merged.

        use intent_core::WorkspaceApi;

        let (store, _db) = temp_store().await;
        let child_id = AgentId::from("agent-dirty-rpc");
        let parent_id = AgentId::from("agent-parent");
        let Some((test_root, repo_path, sandbox_path, ws, services, _bus)) =
            setup_dirty_sandbox(&store, "dirty-rpc", &child_id, &parent_id).await
        else {
            return;
        };
        store
            .set_workspace_auto_commit(&ws.id, false)
            .await
            .unwrap();

        let sandbox_before = store.get_sandbox(&ws.id, &child_id).await.unwrap().unwrap();
        let result = services
            .sandbox_merge(ws.id.clone(), child_id.clone())
            .await
            .expect("dirty merge must start, not error");
        assert_eq!(result["status"], json!("started"));

        // Async outcome: the dirty bounce restores the pre-claim status.
        let status =
            wait_for_sandbox_status(&store, &ws.id, &child_id, sandbox_before.status).await;
        assert_eq!(
            status, sandbox_before.status,
            "Sandbox must return to its pre-claim status"
        );
        assert!(!repo_path.join("dirty.txt").exists());
        assert!(sandbox_path.join("dirty.txt").exists());

        let _ = fs::remove_dir_all(&test_root);
    }
}
