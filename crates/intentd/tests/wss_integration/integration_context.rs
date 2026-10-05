//! Routing metadata grants no host privileges and does not replace Git path authorization.
use super::*;
use serde_json::json;

#[tokio::test]
async fn integration_context_retains_integration_and_git_authorization() {
    let srv = start(WsOptions::default()).await;
    let mut guest = Guest::connect(&srv, &"b5".repeat(32)).await;
    let allowed = WorkspaceId::new();
    let mut record = fixture_workspace(&allowed);
    let path = srv.dir.path().join("allowed-repo");
    std::fs::create_dir_all(&path).unwrap();
    git(&path, &["init", "-q", "-b", "main"]);
    git(
        &path,
        &[
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.test",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "--allow-empty",
            "-m",
            "seed",
        ],
    );
    let branch = "main";
    record.path = Some(path.to_string_lossy().into_owned());
    record.worktree_path = record.path.clone();
    srv.store.insert_workspace(&record).await.unwrap();
    let primary = srv.store.get_primary_principal().await.unwrap();
    srv.store
        .set_workspace_member_role(
            &allowed,
            &primary.id,
            intent_core::WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    srv.store
        .add_workspace_member(
            &allowed,
            &guest.principal.id,
            intent_core::WorkspaceRole::Owner,
        )
        .await
        .unwrap();

    // Even ownership of the routing workspace does not permit host integration use.
    for method in [
        "github.authStatus",
        "github.getUser",
        "sourceControl.authStatus",
        "sourceControl.getUser",
        "github.repos.list",
        "github.repos.search",
        "github.repos.get",
        "github.branches.list",
        "github.branches.listCached",
        "github.repoConfig.get",
        "github.relatedRepos.list",
        "github.users.search",
        "github.pulls.create",
        "github.pulls.get",
        "github.pulls.list",
        "github.pulls.search",
        "github.pulls.merge",
        "github.pulls.updateBranch",
        "github.issues.get",
        "github.issues.list",
        "github.issues.search",
        "github.listReviewComments",
        "github.replyReviewComment",
        "github.getReviewThreads",
        "github.resolveThread",
        "github.unresolveThread",
        "linear.authStatus",
        "linear.listIssues",
        "linear.searchIssues",
        "linear.getIssue",
        "linear.viewer",
        "linear.listTeams",
        "linear.listWorkflowStates",
        "linear.listProjects",
        "linear.listLabels",
        "linear.createIssue",
        "linear.updateIssue",
        "sentry.authStatus",
        "sentry.listIssues",
        "sentry.searchIssues",
        "sentry.listProjects",
        "sentry.getIssue",
        "sentry.resolveIssue",
        "sentry.ignoreIssue",
        "sentry.assignIssue",
    ] {
        for context in [None, Some(&allowed)] {
            let mut params = json!({"provider":"github","owner":"o","repo":"r","number":7,"title":"Title","body":"Body","head":"feature","base":"main","query":"q","id":"issue","issueId":"issue","teamId":"team","commentId":9,"threadId":"thread"});
            if let Some(context) = context {
                params["workspaceId"] = json!(context);
            }
            let v = guest.call(method, params).await;
            assert_eq!(v["error"]["code"], -32003, "{method}: {v}");
        }
    }
    let forbidden_path = srv.dir.path().join("private-repo");
    std::fs::create_dir_all(&forbidden_path).unwrap();
    git(&forbidden_path, &["init", "-q"]);
    for method in [
        "git.getBranches",
        "git.getRemoteUrl",
        "git.branchStatus",
        "git.pull",
    ] {
        for context in [None, Some(&allowed)] {
            let mut params = json!({"repoPath":forbidden_path,"branchName":branch});
            if let Some(context) = context {
                params["workspaceId"] = json!(context);
            }
            let v = guest.call(method, params).await;
            assert_eq!(v["error"]["code"], -32003, "{method}: {v}");
        }
    }
    // Conversely, an unrelated route cannot replace permission on the actual path.
    for method in ["git.getBranches", "git.getRemoteUrl", "git.branchStatus"] {
        let params = json!({"repoPath":path,"branchName":branch});
        let direct = guest.call(method, params.clone()).await;
        assert!(direct.get("error").is_none(), "{method}: {direct}");
        let mut routed = params;
        routed["workspaceId"] = json!("unrelated-route");
        let mut routed = guest.call(method, routed).await;
        routed["id"] = direct["id"].clone();
        assert_eq!(direct, routed, "{method}");
    }
    srv.ws.stop().await;
}

fn git(path: &Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
