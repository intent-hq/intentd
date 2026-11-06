//! Regression for intent-hq/intent#7449: a repo-less `ws.pr.monitor` /
//! `ws.pr.unmonitor` in a GitHub workspace (inserted with repository metadata,
//! so its selection is never-saved / Automatic) must use the workspace's own
//! fetch remote.
use super::*;
use crate::repository_admission_source_tests::fixtures::Fixture as GitFixture;
use intent_core::caller::{with_caller, Caller};
use intent_core::WorkspaceApi;

#[tokio::test]
async fn repo_less_monitor_and_unmonitor_use_the_workspace_github_remote() {
    let git = GitFixture::with_repository_metadata("actual", "repository").await;
    git.git(
        &git.path,
        &[
            "remote",
            "add",
            "origin",
            "https://github.com/Actual/Repository.git",
        ],
    );
    let ws = git.workspace.id.clone();
    let owner = AgentId::from("agent-prmon");
    git.store
        .insert_agent_session(&agent(&ws, "agent-prmon"))
        .await
        .expect("agent");
    let forge = StubForge::new();
    let root = tempfile::tempdir().expect("temp workspaces root");
    let services = Services::new_with_file_secrets(
        git.store.clone(),
        intent_core::FileSecretStore::with_path(root.path().join("secrets.json")),
    )
    .with_event_bus(EventBus::new(git.store.clone()))
    .with_workspaces_root(root.path().to_path_buf())
    .with_source_control(Arc::new(forge.clone()));

    with_caller(Caller::Daemon, async {
        let started = services
            .pr_monitor_start(ws.clone(), owner.clone(), 42, None)
            .await
            .expect("repo-less monitor resolves the GitHub fetch remote");
        assert_eq!(started["ok"], true, "{started}");
        assert_eq!(
            services.active_pr_monitor_labels(&owner).await,
            vec!["actual/repository#42".to_string()]
        );
        services
            .pr_monitor_stop(ws.clone(), owner.clone(), 42, None)
            .await
            .expect("repo-less unmonitor resolves the same remote");
        assert!(services.active_pr_monitor_labels(&owner).await.is_empty());
    })
    .await;
}
