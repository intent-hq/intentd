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

    use git2::Repository;
    use intent_core::{
        now_iso, ActorType, AgentId, AgentSession, AgentStatus, Event, EventActor, WorkspaceId,
    };
    use intent_git::{cow_probe, CowSupport};
    use intent_store::{SandboxStatus, Store};
    use serde_json::json;

    use crate::events::{EventBus, SubscriptionFilter};
    use crate::sandbox_ops::{provision_sandbox, ProvisionConfig, ProvisionOutcome};
    use crate::Services;

    /// `SQLite` db inside an RAII temp dir; the dir sweep on drop also covers
    /// the `-wal`/`-shm` sidecars.
    struct TempDb {
        path: PathBuf,
        dir: tempfile::TempDir,
    }

    impl TempDb {
        fn new() -> Self {
            let dir = crate::test_support::test_tempdir("completion-test-");
            let path = dir.path().join("completion.db");
            Self { path, dir }
        }
    }

    async fn temp_store() -> (Store, TempDb) {
        let db = TempDb::new();
        let store = Store::open(&db.path).await.unwrap();
        (store, db)
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
            created_at: now.clone(),
            updated_at: now,
            repository_path: Some(repo_path.to_string_lossy().to_string()),
            repository_name: Some("test-repo".to_string()),
            skip_worktree: true,
            ..Default::default()
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
            retired_at: None,
            notifications_muted: false,
        };
        store.insert_agent_session(&agent).await.unwrap();
    }

    #[intent_test_macros::daemon_test]
    async fn merge_sweep_finishes_marked_sandbox_after_caller_abort() {
        use tokio::time::{timeout, Duration};
        let (store, db) = temp_store().await;
        let canonical = db.dir.path().join("canonical");
        let sandbox_path = db.dir.path().join("sandbox");
        init_test_repo(&canonical);
        let agent = AgentId::from("sweep-shutdown");
        let branch = format!("sb/{}", agent.0);
        // A real Git clone and commit exercise merge-back without a native CoW dependency.
        let base = {
            let repo = Repository::clone(canonical.to_str().unwrap(), &sandbox_path).unwrap();
            let parent = repo.head().unwrap().peel_to_commit().unwrap();
            let base = parent.id().to_string();
            repo.branch(&branch, &parent, false).unwrap();
            repo.set_head(&format!("refs/heads/{branch}")).unwrap();
            fs::write(sandbox_path.join("swept.txt"), "retained merge").unwrap();
            let mut index = repo.index().unwrap();
            index.add_path(Path::new("swept.txt")).unwrap();
            index.write().unwrap();
            let oid = index.write_tree().unwrap();
            let tree = repo.find_tree(oid).unwrap();
            let sig = git2::Signature::now("Test", "test@example.com").unwrap();
            repo.commit(Some("HEAD"), &sig, &sig, "Retained work", &tree, &[&parent])
                .unwrap();
            base
        };
        let ws = workspace_for_repo(&canonical);
        store.insert_workspace(&ws).await.unwrap();
        create_agent_session(
            &store,
            &ws.id,
            &agent,
            None,
            Some(sandbox_path.to_string_lossy().into()),
        )
        .await;
        store
            .insert_sandbox(&intent_store::Sandbox {
                id: "shutdown-sandbox".into(),
                workspace_id: ws.id.clone(),
                agent_id: agent.clone(),
                path: sandbox_path.to_string_lossy().into(),
                branch,
                base_commit_sha: base,
                snapshot_commit_sha: None,
                status: SandboxStatus::MergePending,
                retry_count: 0,
                created_at: now_iso(),
                updated_at: now_iso(),
            })
            .await
            .unwrap();
        let bus = EventBus::new(store.clone());
        let services = crate::test_support::fake_runtime::FakeRuntime::install(
            Services::new_with_file_secrets(
                store.clone(),
                intent_core::FileSecretStore::with_path(db.dir.path().join("secrets.json")),
            )
            .with_event_bus(bus.clone()),
        );
        let (release, released) = tokio::sync::oneshot::channel();
        *services.sandbox_merge_finish.lock().unwrap() = Some(released);
        let owner = services.clone();
        let caller =
            intent_core::spawn_daemon(async move { owner.sweep_merge_pending_sandboxes().await });
        timeout(
            intentd_test_support::test_timeout(Duration::from_secs(10)),
            services.sandbox_merge_marked.notified(),
        )
        .await
        .unwrap();
        assert_eq!(
            store
                .get_sandbox(&ws.id, &agent)
                .await
                .unwrap()
                .unwrap()
                .status,
            SandboxStatus::Merged
        );
        // The git merge-back itself is runtime behaviour (sandbox_ops tests);
        // the fake runtime reports a clean merge without touching the checkout.
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let (entered, entering) = tokio::sync::oneshot::channel();
        *services.secrets.writer_drain_pending.lock().unwrap() = Some(entered);
        let owner = services.clone();
        let mut drain =
            intent_core::spawn_daemon(async move { owner.shutdown_store_writers().await });
        let pending = timeout(
            intentd_test_support::test_timeout(Duration::from_secs(5)),
            async {
                tokio::select! {
                    point = entering => { assert_eq!(point.unwrap(), "store-tasks"); true }
                    result = &mut drain => { result.unwrap(); false }
                }
            },
        )
        .await
        .unwrap();
        let _ = release.send(());
        if pending {
            timeout(
                intentd_test_support::test_timeout(Duration::from_secs(5)),
                drain,
            )
            .await
            .unwrap()
            .unwrap();
        }
        assert!(pending, "shutdown discarded marked merge cleanup and event");
        bus.shutdown().await.unwrap();
        store.close().await;
        let reopened = Store::open(&db.path).await.unwrap();
        assert!(reopened
            .get_sandbox(&ws.id, &agent)
            .await
            .unwrap()
            .is_none());
        assert!(!sandbox_path.exists());
        let events = reopened
            .query_events(&intent_store::EventQuery::default())
            .await
            .unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == "sandbox:cow:merged")
                .count(),
            1
        );
        reopened.close().await;
    }

}
