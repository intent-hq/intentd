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

    let mut guest = Guest::connect(&srv, &"bc".repeat(32)).await;
    srv.store
        .add_workspace_member(
            &WorkspaceId::from(workspace_id),
            &guest.principal.id,
            intent_core::WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    let mut member = Guest::connect(&srv, &"cd".repeat(32)).await;
    sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
        .bind(&member.principal.id.0)
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        guest.call("principal.me", serde_json::json!({})).await["result"]["hostRole"],
        "guest"
    );
    assert_eq!(
        member.call("principal.me", serde_json::json!({})).await["result"]["hostRole"],
        "member"
    );
    let mut events_before = Vec::new();
    for kind in ["git:commit", "changes:git-status"] {
        let events = srv
            .store
            .events_by_type(&WorkspaceId::from(workspace_id), kind, 10)
            .await
            .unwrap();
        events_before.push((kind, serde_json::to_value(events).unwrap()));
    }

    // Non-administrators fail at the connection allowlist; the administrator
    // reaches dispatch and gets Method not found. Neither path can touch Git.
    for (role, code) in [
        ("administrator", -32601),
        ("guest", -32003),
        ("member", -32003),
    ] {
        for id in 2..=3 {
            let params = serde_json::json!({"workspaceId":workspace_id, "message":"retired commit", "idempotencyKey":"removed-key"});
            let response = match role {
                "guest" => guest.call("git.commit", params).await,
                "member" => member.call("git.commit", params).await,
                _ => {
                    wss_call(
                        srv.port,
                        srv.cfg.clone(),
                        &serde_json::json!({
                            "jsonrpc":"2.0", "id":id, "method":"git.commit", "params":params
                        })
                        .to_string(),
                    )
                    .await
                }
            };
            assert_eq!(response["jsonrpc"], "2.0");
            assert_eq!(response["error"]["code"], code, "{role}: {response}");
            assert!(response.get("result").is_none());
            for (kind, before) in &events_before {
                let events = srv
                    .store
                    .events_by_type(&WorkspaceId::from(workspace_id), kind, 10)
                    .await
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(events).unwrap(),
                    *before,
                    "{role}: {kind}"
                );
            }
            assert_eq!(git(&["rev-parse", "HEAD"]), head_before, "{role}");
            assert_eq!(git(&["write-tree"]), index_before, "{role}");
            assert_eq!(git(&["status", "--porcelain"]), status_before, "{role}");
            assert_eq!(
                std::fs::read_to_string(repo.join("seed.txt")).unwrap(),
                "unstaged\n"
            );
            assert_eq!(
                std::fs::read_to_string(repo.join("staged.txt")).unwrap(),
                "staged\n"
            );
            assert!(srv
                .store
                .get_idempotent(workspace_id, "removed-key")
                .await
                .unwrap()
                .is_none());
        }
    }

    // A host member still uses the supported human staged-index commit path.
    let response = member
        .call(
            "git.agentCommit",
            serde_json::json!({
                "workspaceId":workspace_id, "message":"supported commit", "userRequested":true
            }),
        )
        .await;
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
    drop(guest);
    drop(member);
    srv.ws.stop().await;
}
