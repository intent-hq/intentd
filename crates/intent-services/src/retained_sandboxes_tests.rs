//! Legacy per-agent `CoW` sandboxes after retirement: the boot recovery
//! notice names every live sandbox's retained path and branch and leaves the
//! rows, folders and branches untouched (`methods/nodes.md` "Landing
//! sequence" step 2).

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use git2::Repository;
    use intent_core::{now_iso, AgentId, AgentSession, AgentStatus, WorkspaceId};
    use intent_store::{SandboxStatus, Store};

    use crate::Services;

    /// `SQLite` db inside an RAII temp dir; the dir sweep on drop also covers
    /// the `-wal`/`-shm` sidecars.
    struct TempDb {
        path: PathBuf,
        dir: tempfile::TempDir,
    }

    impl TempDb {
        fn new() -> Self {
            let dir = crate::test_support::test_tempdir("retained-sandbox-test-");
            let path = dir.path().join("retained.db");
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
            pinned: false,
        };
        store.insert_agent_session(&agent).await.unwrap();
    }

    #[tokio::test]
    async fn boot_reports_live_legacy_sandboxes_without_touching_them() {
        let (store, db) = temp_store().await;
        let canonical = db.dir.path().join("canonical");
        init_test_repo(&canonical);
        let ws = workspace_for_repo(&canonical);
        store.insert_workspace(&ws).await.unwrap();

        let statuses = [
            (SandboxStatus::Created, true),
            (SandboxStatus::Merging, true),
            (SandboxStatus::MergePending, true),
            (SandboxStatus::ConflictBounced, true),
            (SandboxStatus::Merged, false),
            (SandboxStatus::Discarded, false),
        ];
        let mut live = Vec::new();
        for (i, (status, is_live)) in statuses.into_iter().enumerate() {
            let agent = AgentId::from(format!("legacy-{i}").as_str());
            let path = db.dir.path().join(format!("sandboxes/{}/repo", agent.0));
            fs::create_dir_all(&path).unwrap();
            fs::write(path.join("work.txt"), format!("kept {i}")).unwrap();
            let path_s = path.to_string_lossy().to_string();
            create_agent_session(&store, &ws.id, &agent, None, Some(path_s.clone())).await;
            store
                .insert_sandbox(&intent_store::Sandbox {
                    id: format!("sandbox-{i}"),
                    workspace_id: ws.id.clone(),
                    agent_id: agent.clone(),
                    path: path_s.clone(),
                    branch: format!("sb/{}", agent.0),
                    base_commit_sha: "0".repeat(40),
                    snapshot_commit_sha: None,
                    status,
                    retry_count: 1,
                    created_at: now_iso(),
                    updated_at: now_iso(),
                })
                .await
                .unwrap();
            if is_live {
                live.push((agent, path, path_s, status));
            }
        }

        let services = Services::new_with_file_secrets(
            store.clone(),
            intent_core::FileSecretStore::with_path(db.dir.path().join("secrets.json")),
        );
        let mut reported = services.report_retained_sandboxes().await;
        reported.sort_by(|a, b| a.agent_id.0.cmp(&b.agent_id.0));
        assert_eq!(
            reported
                .iter()
                .map(|r| (r.agent_id.0.clone(), r.path.clone(), r.branch.clone()))
                .collect::<Vec<_>>(),
            live.iter()
                .map(|(agent, _, path, _)| (
                    agent.0.clone(),
                    path.clone(),
                    format!("sb/{}", agent.0)
                ))
                .collect::<Vec<_>>(),
            "one notice per live sandbox, naming its path and branch"
        );

        for (agent, path, path_s, status) in &live {
            assert_eq!(
                fs::read_to_string(path.join("work.txt")).unwrap(),
                format!("kept {}", &agent.0["legacy-".len()..]),
                "retained files are untouched"
            );
            let row = store.get_sandbox(&ws.id, agent).await.unwrap().unwrap();
            assert_eq!(row.status, *status, "the row is not transitioned");
            assert_eq!(row.retry_count, 1, "the row is not reset");
            let session = store
                .get_agent_session_without_messages(agent)
                .await
                .unwrap();
            assert_eq!(
                session.sandbox_path.as_deref(),
                Some(path_s.as_str()),
                "the session keeps its sandbox link for the next-turn notice"
            );
        }
        store.close().await;
    }
}
