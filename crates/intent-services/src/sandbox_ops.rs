//! Sandbox provisioning and lifecycle for CoW agent isolation (direct-mode workspaces).

use std::path::PathBuf;

use intent_core::{AgentId, Error, Result, Workspace, WorkspaceId};
use intent_git::{cow_clone, cow_probe, CowSupport};
use intent_store::{Sandbox, SandboxStatus, Store};

use crate::now_iso;

/// Outcome of sandbox provisioning.
#[derive(Debug, Clone)]
pub enum ProvisionOutcome {
    /// CoW is supported; sandbox was created.
    Supported {
        path: PathBuf,
        branch: String,
        base_commit_sha: String,
        snapshot_commit_sha: Option<String>,
    },
    /// CoW is not supported; fallback to shared mode (no bytes copied).
    Unsupported,
}

/// Configuration for sandbox provisioning.
pub struct ProvisionConfig {
    /// Workspaces root directory (from config.workspaces_root).
    pub workspaces_root: PathBuf,
}

/// Provision a sandbox for an agent in a direct-mode workspace.
///
/// 1. Probe CoW support between the user's repository directory and the sandbox parent.
/// 2. If Unsupported, return `ProvisionOutcome::Unsupported` (fallback to shared mode; ZERO bytes copied).
/// 3. If Supported: cow_clone the user's directory to `<workspaces_root>/<workspaceId>/sandboxes/<agentId>/<repo-slug>`.
/// 4. Create branch `sb/<agentId>` in the sandbox.
/// 5. If the source had uncommitted changes, create a snapshot commit of the dirty state.
/// 6. Persist the sandbox record.
/// 7. Return `ProvisionOutcome::Supported` with the sandbox details.
///
/// The user's directory is never modified by this operation.
pub async fn provision_sandbox(
    store: &Store,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
    config: &ProvisionConfig,
) -> Result<ProvisionOutcome> {
    // Load workspace
    let workspace = store.get_workspace(workspace_id).await?;

    // Only direct-mode workspaces (skip_worktree = true OR no worktree_path) are supported
    let user_dir = resolve_user_directory(&workspace)?;

    // Construct sandbox path: <workspaces_root>/<workspaceId>/sandboxes/<agentId>/<repo-slug>
    let repo_slug = repo_slug_from_workspace(&workspace);
    let sandbox_parent = config
        .workspaces_root
        .join(&workspace_id.0)
        .join("sandboxes")
        .join(&agent_id.0);
    let sandbox_path = sandbox_parent.join(&repo_slug);

    // Ensure sandbox parent exists (needed for cow_probe)
    std::fs::create_dir_all(&sandbox_parent)
        .map_err(|e| Error::Internal(format!("create sandbox parent dir failed: {e}")))?;

    // Probe CoW support
    let probe_result = cow_probe(&user_dir, &sandbox_parent)?;
    if probe_result == CowSupport::Unsupported {
        return Ok(ProvisionOutcome::Unsupported);
    }

    // CoW clone the user's directory
    cow_clone(&user_dir, &sandbox_path)?;

    // Open the sandbox repo and record the base commit SHA
    // Scope git2 objects to ensure they're dropped before any await points
    let (base_commit_sha, branch_name, snapshot_commit_sha) = {
        let sandbox_repo = git2::Repository::open(&sandbox_path)
            .map_err(|e| Error::Internal(format!("open sandbox repo failed: {e}")))?;
        let base_commit_sha = sandbox_repo
            .head()
            .ok()
            .and_then(|h| h.target())
            .map(|oid| oid.to_string())
            .ok_or_else(|| Error::Internal("sandbox has no HEAD commit".to_string()))?;

        // Create branch sb/<agentId> in the sandbox
        let branch_name = format!("sb/{}", &agent_id.0);
        let head_commit = sandbox_repo
            .head()
            .map_err(|e| Error::Internal(format!("get HEAD failed: {e}")))?
            .peel_to_commit()
            .map_err(|e| Error::Internal(format!("peel HEAD to commit failed: {e}")))?;
        sandbox_repo
            .branch(&branch_name, &head_commit, false)
            .map_err(|e| Error::Internal(format!("create branch failed: {e}")))?;

        // Check out the new branch
        let refname = format!("refs/heads/{}", branch_name);
        sandbox_repo
            .set_head(&refname)
            .map_err(|e| Error::Internal(format!("set HEAD failed: {e}")))?;

        // Check for dirty state and create a snapshot commit if needed
        let snapshot_commit_sha = if is_dirty(&sandbox_repo)? {
            Some(create_snapshot_commit(&sandbox_repo, agent_id)?)
        } else {
            None
        };

        // Return the values we need, git2 objects will be dropped here
        (base_commit_sha, branch_name, snapshot_commit_sha)
    };

    // Persist the sandbox record
    let now = now_iso();
    let sandbox = Sandbox {
        id: uuid::Uuid::new_v4().to_string(),
        workspace_id: workspace_id.clone(),
        agent_id: agent_id.clone(),
        path: sandbox_path.to_string_lossy().to_string(),
        branch: branch_name.clone(),
        base_commit_sha: base_commit_sha.clone(),
        snapshot_commit_sha: snapshot_commit_sha.clone(),
        status: SandboxStatus::Created,
        created_at: now.clone(),
        updated_at: now,
    };
    store.insert_sandbox(&sandbox).await?;

    Ok(ProvisionOutcome::Supported {
        path: sandbox_path,
        branch: branch_name,
        base_commit_sha,
        snapshot_commit_sha,
    })
}

/// Discard a sandbox: remove the directory and the database record.
pub async fn discard_sandbox(
    store: &Store,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
) -> Result<()> {
    // Load the sandbox record to get the path
    let sandbox = store.get_sandbox(workspace_id, agent_id).await?;
    if let Some(s) = sandbox {
        // Remove the directory
        let path = PathBuf::from(&s.path);
        if path.exists() {
            std::fs::remove_dir_all(&path)
                .map_err(|e| Error::Internal(format!("remove sandbox directory failed: {e}")))?;
        }
    }

    // Delete the record (whether or not the directory existed)
    store.delete_sandbox(workspace_id, agent_id).await?;

    Ok(())
}

/// Garbage-collect orphaned sandboxes: remove sandboxes whose agent no longer exists
/// or whose directory is missing.
pub async fn gc_orphaned_sandboxes(store: &Store) -> Result<()> {
    let all_sandboxes = store.list_all_sandboxes().await?;

    for sandbox in all_sandboxes {
        let mut should_remove = false;

        // Check if the agent session still exists
        let agent_exists = store.get_agent_session(&sandbox.agent_id).await.is_ok();
        if !agent_exists {
            should_remove = true;
        }

        // Check if the directory exists
        let path = PathBuf::from(&sandbox.path);
        if !path.exists() {
            should_remove = true;
        }

        if should_remove {
            // Remove the directory if it exists
            if path.exists() {
                let _ = std::fs::remove_dir_all(&path);
            }
            // Delete the record
            store
                .delete_sandbox(&sandbox.workspace_id, &sandbox.agent_id)
                .await?;
        }
    }

    Ok(())
}

/// Resolve the user's repository directory from a workspace.
/// Returns an error if the workspace is not direct-mode or doesn't have a repository path.
fn resolve_user_directory(workspace: &Workspace) -> Result<PathBuf> {
    // Direct-mode means: skip_worktree = true OR (skip_worktree = false but no worktree provisioned)
    // For now, we require repository_path to be set
    let repo_path = workspace
        .repository_path
        .as_ref()
        .ok_or_else(|| Error::InvalidParams("workspace has no repository_path".to_string()))?;

    let path = PathBuf::from(repo_path);
    if !path.exists() {
        return Err(Error::InvalidParams(format!(
            "repository path does not exist: {}",
            repo_path
        )));
    }

    // Verify it's a git repository
    if !path.join(".git").exists() {
        return Err(Error::InvalidParams(format!(
            "repository path is not a git repository: {}",
            repo_path
        )));
    }

    Ok(path)
}

/// Derive a repository slug from the workspace (repository name, sanitized).
fn repo_slug_from_workspace(workspace: &Workspace) -> String {
    workspace
        .repository_name
        .as_ref()
        .map(|n| slugify(n))
        .unwrap_or_else(|| {
            workspace
                .repository_path
                .as_ref()
                .and_then(|p| {
                    PathBuf::from(p)
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                })
                .unwrap_or_else(|| "repo".to_string())
        })
}

/// Simple slugification: lowercase, replace non-alphanumeric with hyphens.
fn slugify(s: &str) -> String {
    s.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

/// Check if a git repository has uncommitted changes (staged, unstaged, or untracked).
fn is_dirty(repo: &git2::Repository) -> Result<bool> {
    let mut opts = git2::StatusOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(false);
    let statuses = repo
        .statuses(Some(&mut opts))
        .map_err(|e| Error::Internal(format!("git status failed: {e}")))?;
    Ok(!statuses.is_empty())
}

/// Create a snapshot commit of the current dirty state in the repository.
/// Stages all changes (tracked and untracked) and commits them with a snapshot message.
fn create_snapshot_commit(repo: &git2::Repository, agent_id: &AgentId) -> Result<String> {
    // Stage all changes
    let mut index = repo
        .index()
        .map_err(|e| Error::Internal(format!("get index failed: {e}")))?;

    // Add all files (including untracked)
    index
        .add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)
        .map_err(|e| Error::Internal(format!("stage all files failed: {e}")))?;

    index
        .write()
        .map_err(|e| Error::Internal(format!("write index failed: {e}")))?;

    // Create the tree
    let tree_oid = index
        .write_tree()
        .map_err(|e| Error::Internal(format!("write tree failed: {e}")))?;
    let tree = repo
        .find_tree(tree_oid)
        .map_err(|e| Error::Internal(format!("find tree failed: {e}")))?;

    // Get parent commit
    let parent = repo
        .head()
        .ok()
        .and_then(|h| h.target())
        .and_then(|oid| repo.find_commit(oid).ok());

    let sig = repo
        .signature()
        .map_err(|e| Error::Internal(format!("get signature failed: {e}")))?;

    let message = format!("WIP snapshot for {}", agent_id.0);
    let parents: Vec<&git2::Commit> = parent.iter().collect();

    let oid = repo
        .commit(Some("HEAD"), &sig, &sig, &message, &tree, &parents)
        .map_err(|e| Error::Internal(format!("create commit failed: {e}")))?;

    Ok(oid.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::WorkspaceStatus;
    use intent_store::Store;
    use std::fs;
    use std::path::Path;

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("sandbox-test-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ =
                    std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
        }
    }

    async fn temp_store() -> (Store, TempDb) {
        let db = TempDb::new();
        let store = Store::open(&db.path).await.unwrap();
        (store, db)
    }

    fn temp_repo(name: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::TempDir::new().unwrap();
        let repo_path = dir.path().join(name);
        init_test_repo(&repo_path);
        (dir, repo_path)
    }

    /// Create a test repo under a specific parent directory (for same-volume CoW tests)
    /// Uses workspace root's target dir, not crate's target dir, to ensure same volume.
    fn temp_repo_in_target(name: &str) -> (PathBuf, PathBuf) {
        // Navigate to workspace root (up from crates/intent-services)
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

        // Initialize a git repository
        let repo = git2::Repository::init(&repo_path).unwrap();

        // Create an initial commit
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let tree_id = {
            let mut index = repo.index().unwrap();
            index.write_tree().unwrap()
        };
        let tree = repo.find_tree(tree_id).unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
            .unwrap();
    }

    fn workspace_for_repo(repo_path: &PathBuf) -> Workspace {
        let now = now_iso();
        Workspace {
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
        }
    }

    #[tokio::test]
    async fn provision_creates_sandbox_and_leaves_source_untouched() {
        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("source");

        // Use same test_root for workspaces to ensure same volume
        let workspaces_root = test_root.join("workspaces");

        // Early probe check - skip test if CoW not available (e.g., non-CoW filesystem)
        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!(
                "Skipping test: CoW not supported between {:?} and {:?}",
                repo_path, workspaces_root
            );
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        // Add a test file to the source repo
        fs::write(repo_path.join("test.txt"), "hello").unwrap();
        let source_repo = git2::Repository::open(&repo_path).unwrap();
        let mut index = source_repo.index().unwrap();
        index.add_path(std::path::Path::new("test.txt")).unwrap();
        index.write().unwrap();
        let tree_id = index.write_tree().unwrap();
        let tree = source_repo.find_tree(tree_id).unwrap();
        let parent = source_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        source_repo
            .commit(Some("HEAD"), &sig, &sig, "Add test file", &tree, &[&parent])
            .unwrap();

        // Capture state before provisioning
        let source_head_before = source_repo.head().unwrap().target().unwrap().to_string();
        let source_index_before = source_repo.index().unwrap();
        let staged_count_before = source_index_before.len();
        let test_file_content_before = fs::read_to_string(repo_path.join("test.txt")).unwrap();

        // Create workspace
        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        // Create agent
        let agent_id = AgentId::new();
        let agent = intent_core::AgentSession {
            id: agent_id.clone(),
            workspace_id: ws.id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: intent_core::AgentStatus::Active,
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
            sandbox_path: None,
            sandbox_branch: None,
        };
        store.insert_agent_session(&agent).await.unwrap();

        // Provision sandbox (use workspaces_root in same TempDir to ensure same volume)
        fs::create_dir_all(&workspaces_root).unwrap();
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();

        // We probed upfront, so this MUST be Supported
        let ProvisionOutcome::Supported {
            path,
            branch,
            base_commit_sha,
            snapshot_commit_sha,
        } = outcome
        else {
            panic!("Expected Supported after probe confirmed CoW available");
        };

        // Verify sandbox was created
        assert!(path.exists());
        assert!(path.join(".git").exists());
        assert_eq!(branch, format!("sb/{}", agent_id.0));

        // Verify the source repo was COMPLETELY UNTOUCHED
        let source_head_after = source_repo.head().unwrap().target().unwrap().to_string();
        assert_eq!(
            source_head_before, source_head_after,
            "HEAD must not change"
        );

        let source_index_after = source_repo.index().unwrap();
        assert_eq!(
            staged_count_before,
            source_index_after.len(),
            "Index must not change"
        );

        let test_file_content_after = fs::read_to_string(repo_path.join("test.txt")).unwrap();
        assert_eq!(
            test_file_content_before, test_file_content_after,
            "File content must be byte-identical"
        );

        // Verify base commit matches source HEAD
        assert_eq!(base_commit_sha, source_head_before);

        // Clean sandbox has no snapshot commit
        assert_eq!(snapshot_commit_sha, None);

        // Verify sandbox record was persisted
        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap();
        assert!(sandbox.is_some());

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn provision_dirty_state_creates_snapshot_commit() {
        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("source-dirty");

        // Use same test_root for workspaces to ensure same volume
        let workspaces_root = test_root.join("workspaces");

        // Early probe check - skip test if CoW not available
        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!(
                "Skipping test: CoW not supported between {:?} and {:?}",
                repo_path, workspaces_root
            );
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        // Add a committed file
        fs::write(repo_path.join("committed.txt"), "committed").unwrap();
        let source_repo = git2::Repository::open(&repo_path).unwrap();
        let mut index = source_repo.index().unwrap();
        index
            .add_path(std::path::Path::new("committed.txt"))
            .unwrap();
        index.write().unwrap();
        let tree_id = index.write_tree().unwrap();
        let tree = source_repo.find_tree(tree_id).unwrap();
        let parent = source_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        source_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Add committed file",
                &tree,
                &[&parent],
            )
            .unwrap();

        let base_sha = source_repo.head().unwrap().target().unwrap().to_string();

        // Add an uncommitted file (dirty state)
        fs::write(repo_path.join("dirty.txt"), "dirty WIP").unwrap();

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        let agent_id = AgentId::new();
        let agent = intent_core::AgentSession {
            id: agent_id.clone(),
            workspace_id: ws.id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: intent_core::AgentStatus::Active,
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
            sandbox_path: None,
            sandbox_branch: None,
        };
        store.insert_agent_session(&agent).await.unwrap();

        fs::create_dir_all(&workspaces_root).unwrap();
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };

        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();

        // We probed upfront, so this MUST be Supported
        let ProvisionOutcome::Supported {
            path,
            branch,
            base_commit_sha,
            snapshot_commit_sha,
        } = outcome
        else {
            panic!("Expected Supported after probe confirmed CoW available");
        };

        // Verify snapshot commit was created
        let snapshot_sha = snapshot_commit_sha.expect("Snapshot commit must exist for dirty state");

        // Verify base SHA matches source HEAD
        assert_eq!(base_commit_sha, base_sha, "Base SHA must match source HEAD");

        // Verify the sandbox contains the dirty file
        assert!(path.join("dirty.txt").exists());
        let content = fs::read_to_string(path.join("dirty.txt")).unwrap();
        assert_eq!(content, "dirty WIP", "Sandbox must contain the dirty WIP");

        // Open sandbox repo and verify snapshot commit exists on the sandbox branch
        let sandbox_repo = git2::Repository::open(&path).unwrap();
        let snapshot_commit = sandbox_repo
            .find_commit(git2::Oid::from_str(&snapshot_sha).unwrap())
            .unwrap();
        assert_eq!(
            snapshot_commit.message().unwrap(),
            format!("WIP snapshot for {}", agent_id.0),
            "Snapshot commit message must match"
        );

        // Verify snapshot commit is on the sandbox branch
        let sb_branch_ref = sandbox_repo
            .find_branch(&branch, git2::BranchType::Local)
            .unwrap();
        let sb_head = sb_branch_ref.get().target().unwrap().to_string();
        assert_eq!(
            sb_head, snapshot_sha,
            "Sandbox branch must point to snapshot commit"
        );

        // Verify sandbox record has correct SHAs
        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap().unwrap();
        assert_eq!(sandbox.base_commit_sha, base_sha);
        assert_eq!(sandbox.snapshot_commit_sha, Some(snapshot_sha));

        // Verify the source repo still has dirty state (unchanged)
        assert!(repo_path.join("dirty.txt").exists());
        let statuses = source_repo.statuses(None).unwrap();
        assert!(
            !statuses.is_empty(),
            "Source repo must still have dirty state"
        );

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn discard_sandbox_removes_directory_and_record() {
        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("discard-test");

        // Use same test_root for workspaces to ensure same volume
        let workspaces_root = test_root.join("workspaces");

        // Early probe check - skip test if CoW not available
        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!(
                "Skipping test: CoW not supported between {:?} and {:?}",
                repo_path, workspaces_root
            );
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        let agent_id = AgentId::new();
        let agent = intent_core::AgentSession {
            id: agent_id.clone(),
            workspace_id: ws.id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: intent_core::AgentStatus::Active,
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
            sandbox_path: None,
            sandbox_branch: None,
        };
        store.insert_agent_session(&agent).await.unwrap();

        fs::create_dir_all(&workspaces_root).unwrap();
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };

        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();

        // We probed upfront, so this MUST be Supported
        let ProvisionOutcome::Supported { path, .. } = outcome else {
            panic!("Expected Supported after probe confirmed CoW available");
        };

        // Verify sandbox was created
        assert!(path.exists(), "Sandbox directory must exist before discard");
        assert!(
            path.join(".git").exists(),
            "Sandbox .git directory must exist"
        );

        // Verify sandbox record exists
        let sandbox_before = store.get_sandbox(&ws.id, &agent_id).await.unwrap();
        assert!(
            sandbox_before.is_some(),
            "Sandbox record must exist before discard"
        );

        // Discard the sandbox
        discard_sandbox(&store, &ws.id, &agent_id).await.unwrap();

        // Verify directory was COMPLETELY removed
        assert!(
            !path.exists(),
            "Sandbox directory must be completely removed after discard"
        );

        // Verify record was deleted from the database
        let sandbox_after = store.get_sandbox(&ws.id, &agent_id).await.unwrap();
        assert!(
            sandbox_after.is_none(),
            "Sandbox record must be deleted from DB after discard"
        );

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn gc_orphaned_sandboxes_removes_missing_agents() {
        let (store, _db) = temp_store().await;
        let workspaces_root = tempfile::TempDir::new().unwrap();

        // Create workspace first (FK requirement)
        let ws_id = WorkspaceId::new();
        let ws = workspace_for_repo(&PathBuf::from("/tmp/fake"));
        let mut ws_copy = ws.clone();
        ws_copy.id = ws_id.clone();
        store.insert_workspace(&ws_copy).await.unwrap();

        // Create agent temporarily to satisfy FK, then we'll delete it
        let agent_id = AgentId::new();
        let agent = intent_core::AgentSession {
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: intent_core::AgentStatus::Active,
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
            sandbox_path: None,
            sandbox_branch: None,
        };
        store.insert_agent_session(&agent).await.unwrap();

        // Create a sandbox record
        let sandbox = intent_store::Sandbox {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: ws_id.clone(),
            agent_id: agent_id.clone(),
            path: workspaces_root
                .path()
                .join("orphaned")
                .to_string_lossy()
                .to_string(),
            branch: "sb/test".to_string(),
            base_commit_sha: "abc123".to_string(),
            snapshot_commit_sha: None,
            status: intent_store::SandboxStatus::Created,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.insert_sandbox(&sandbox).await.unwrap();

        // Now delete the agent session to make the sandbox orphaned
        store.delete_agent_session(&ws_id, &agent_id).await.unwrap();

        // Run GC
        gc_orphaned_sandboxes(&store).await.unwrap();

        // Verify the orphaned sandbox was removed
        let result = store.get_sandbox(&ws_id, &agent_id).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn provision_returns_unsupported_when_cow_unavailable() {
        let (store, _db) = temp_store().await;
        let (_repo_dir_guard, repo_path) = temp_repo("no-cow");

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        let agent_id = AgentId::new();
        let agent = intent_core::AgentSession {
            id: agent_id.clone(),
            workspace_id: ws.id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: intent_core::AgentStatus::Active,
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
            sandbox_path: None,
            sandbox_branch: None,
        };
        store.insert_agent_session(&agent).await.unwrap();

        // Try to use a cross-volume destination
        // On most systems, /tmp is a different volume, but on this APFS machine it's the same
        let cross_volume_root = std::env::temp_dir().join(format!(
            "intentd-cross-volume-test-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&cross_volume_root).unwrap();

        // Probe first to check if we actually have a cross-volume scenario
        let probe = cow_probe(&repo_path, &cross_volume_root).unwrap();
        if probe == CowSupport::Supported {
            eprintln!("Skipping test: test environment has no cross-volume paths available");
            let _ = fs::remove_dir_all(&cross_volume_root);
            return;
        }

        let config = ProvisionConfig {
            workspaces_root: cross_volume_root.clone(),
        };

        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();

        // When CoW is not supported, we return Unsupported and copy ZERO bytes
        let ProvisionOutcome::Unsupported = outcome else {
            panic!("Expected Unsupported outcome for cross-volume CoW attempt");
        };

        // Verify no sandbox directory was created
        let would_be_path = config
            .workspaces_root
            .join(ws.id.0.to_string())
            .join(agent_id.0.to_string());
        assert!(
            !would_be_path.exists(),
            "No sandbox directory should exist when CoW is unsupported"
        );

        // Verify no sandbox record was created in the database
        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap();
        assert!(
            sandbox.is_none(),
            "No sandbox record should exist when CoW is unsupported"
        );

        // Clean up
        let _ = fs::remove_dir_all(&cross_volume_root);
    }
}
