use super::*;

/// The retired keyed RPC must fail before touching Git or writing a receipt.
/// Human commits still use the existing staged-index agentCommit path.
#[tokio::test]
async fn removed_git_commit_has_no_git_effect_over_wss() {
    let srv = start(WsOptions::default()).await;
    let repo_dir = test_tempdir("intentd-wss-removed-commit-");
    let repo = repo_dir.path();
    let git = |args: &[&str]| -> String {
        let output = std::process::Command::new("git")
            .current_dir(repo)
            .args(args)
            .output()
            .expect("run fixture git");
        assert!(output.status.success(), "git {args:?}: {output:?}");
        String::from_utf8(output.stdout).unwrap().trim().to_string()
    };
    git(&["init", "-q"]);
    git(&["config", "user.name", "Test"]);
    git(&["config", "user.email", "test@example.com"]);
    git(&["config", "commit.gpgsign", "false"]);
    std::fs::write(repo.join("seed.txt"), "seed\n").unwrap();
    git(&["add", "seed.txt"]);
    git(&["commit", "-q", "-m", "seed"]);
    let created = wss_call(
        srv.port,
        srv.cfg.clone(),
        &serde_json::json!({
            "jsonrpc":"2.0", "id":1, "method":"workspace.create",
            "params":{"title":"Removed commit RPC", "worktreePath":repo, "path":repo}
        })
        .to_string(),
    )
    .await;
    let workspace_id = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id");
    std::fs::write(repo.join("staged.txt"), "staged\n").unwrap();
    git(&["add", "staged.txt"]);
    std::fs::write(repo.join("seed.txt"), "unstaged\n").unwrap();
    let head_before = git(&["rev-parse", "HEAD"]);
    let index_before = git(&["write-tree"]);
    let status_before = git(&["status", "--porcelain"]);

    for id in 2..=3 {
        let response = wss_call(srv.port, srv.cfg.clone(), &serde_json::json!({
            "jsonrpc":"2.0", "id":id, "method":"git.commit",
            "params":{"workspaceId":workspace_id, "message":"retired commit", "idempotencyKey":"removed-key"}
        }).to_string()).await;
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], id);
        assert_eq!(response["error"]["code"], -32601, "{response}");
        assert!(response.get("result").is_none());
        assert!(srv
            .store
            .events_by_type(&WorkspaceId::from(workspace_id), "git:commit", 10)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(git(&["rev-parse", "HEAD"]), head_before);
        assert_eq!(git(&["write-tree"]), index_before);
        assert_eq!(git(&["status", "--porcelain"]), status_before);
        assert!(srv
            .store
            .get_idempotent(workspace_id, "removed-key")
            .await
            .unwrap()
            .is_none());
    }

    let response = wss_call(srv.port, srv.cfg.clone(), &serde_json::json!({
        "jsonrpc":"2.0", "id":4, "method":"git.agentCommit",
        "params":{"workspaceId":workspace_id, "message":"supported commit", "userRequested":true}
    }).to_string()).await;
    assert_eq!(response["result"]["ok"], true, "{response}");
    assert_eq!(
        response["result"]["files"],
        serde_json::json!(["staged.txt"])
    );
    assert_eq!(response["result"]["fileCount"], 1);
    assert_eq!(response["result"]["hash"], git(&["rev-parse", "HEAD"]));
    assert_eq!(git(&["rev-parse", "HEAD^"]), head_before);
    assert_eq!(git(&["rev-parse", "HEAD^{tree}"]), index_before);
    assert_eq!(
        std::fs::read_to_string(repo.join("seed.txt")).unwrap(),
        "unstaged\n"
    );
    assert_eq!(git(&["diff", "--name-only"]), "seed.txt");
    assert!(git(&["diff", "--cached", "--name-only"]).is_empty());
    srv.ws.stop().await;
}
