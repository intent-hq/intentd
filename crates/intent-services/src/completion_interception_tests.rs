//! Integration tests for sandbox merge-back on agent completion (completion-interception path).
//!
//! These tests exercise the handle_completion_event → handle_sandbox_merge_on_completion wiring:
//! - (a) Clean merge → completion propagates with merged status; sandbox:merged event; commits in canonical
//! - (b) Conflict → completion NOT delivered; bounce message queued; canonical pristine; retry_count incremented
//! - (c) Retry cap exhausted → completion propagates with merge-pending
//! - (d) Bounce refreshes sandbox with canonical HEAD without re-provisioning

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

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

    /// Create a test repo under target/ for same-volume CoW.
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
        fs::create_dir_all(&repo_path).unwrap();
        let repo = Repository::init(&repo_path).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let tree_id = {
            let mut index = repo.index().unwrap();
            index.write_tree().unwrap()
        };
        let tree = repo.find_tree(tree_id).unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
            .unwrap();
    }

    fn workspace_for_repo(repo_path: &PathBuf) -> intent_core::Workspace {
        let now = now_iso();
        intent_core::Workspace {
            id: WorkspaceId::new(),
            title: "Test WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
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
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            parent_agent_id: parent_id.cloned(),
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
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
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: sandbox_path.clone(),
            sandbox_branch: sandbox_path.as_ref().map(|_| format!("sb/{}", agent_id.0)),
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

    /// Subscribe to sandbox:merged events.
    fn subscribe_to_sandbox_merged(
        bus: &EventBus,
        ws_id: &WorkspaceId,
    ) -> crate::events::Subscription {
        let filter = SubscriptionFilter {
            event_types: vec!["sandbox:merged".to_string()],
            workspace_id: Some(ws_id.0.clone()),
            batch_window: None,
            ..Default::default()
        };
        bus.subscribe(filter)
    }

    #[tokio::test]
    async fn test_clean_merge_propagates_completion_and_emits_event() {
        // Scenario (a): Clean merge → completion propagates with merged status;
        // sandbox:merged event emitted; agent commits present in canonical.

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

        // Register a completion watch: parent watches child (oneShot)
        services.register_completion_watch(
            &ws.id,
            parent_id.clone(),
            "Parent".to_string(),
            child_id.clone(),
            true, // oneShot
            None, // no group
        );

        // Subscribe to sandbox:merged events
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
            parent_user_messages.len() > 0,
            "Parent should have received a wake message on clean merge"
        );

        // Assert: sandbox:merged event was emitted
        let merged_event =
            tokio::time::timeout(std::time::Duration::from_secs(1), merged_sub.recv()).await;
        assert!(
            merged_event.is_ok(),
            "sandbox:merged event should be emitted"
        );
        let merged_batch = merged_event.unwrap().unwrap();
        assert_eq!(merged_batch.len(), 1);
        assert_eq!(merged_batch[0].event_type, "sandbox:merged");

        // Assert: sandbox status is Merged
        let sandbox = store.get_sandbox(&ws.id, &child_id).await.unwrap();
        assert!(sandbox.is_none(), "Sandbox should be discarded after merge");

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
        services.register_completion_watch(
            &ws.id,
            parent_id.clone(),
            "Parent".to_string(),
            child_id.clone(),
            true, // oneShot
            None,
        );

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
        assert!(user_messages.len() > 0, "Bounce message should be queued");
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

        // Assert: sandbox status is ConflictBounced
        let sandbox = store.get_sandbox(&ws.id, &child_id).await.unwrap().unwrap();
        assert_eq!(sandbox.status, SandboxStatus::ConflictBounced);

        // Assert: retry_count was incremented
        assert_eq!(
            sandbox.retry_count, 1,
            "Retry count should be 1 after first bounce"
        );

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_retry_cap_exhausted_propagates_with_merge_pending() {
        // Scenario (c): Retry cap exhausted (retry_count >= 2) →
        // completion DOES propagate with merge-pending status.

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
        services.register_completion_watch(
            &ws.id,
            parent_id.clone(),
            "Parent".to_string(),
            child_id.clone(),
            true, // oneShot
            None,
        );

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
            parent_user_messages.len() > 0,
            "Parent should receive wake when retry cap exhausted"
        );

        // Assert: sandbox status is MergePending
        let sandbox = store.get_sandbox(&ws.id, &child_id).await.unwrap().unwrap();
        assert_eq!(sandbox.status, SandboxStatus::MergePending);

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
}
