use super::*;
use intent_core::WorkspaceApi;

async fn seed_link(svc: &Services, forge: &StubForge, ws: &WorkspaceId) {
    let pr = forge.get_pr(&RepoRef::new("o", "r"), 42).await.unwrap();
    let info = pr_ops::build_pr_info(&pr);
    let mut row = svc.store().get_workspace(ws).await.unwrap();
    row.pr_number = Some(42);
    row.pr_url = Some(info.url.clone());
    row.pr_status = Some(info.status);
    row.active_pull_request = Some(info.clone());
    row.pull_requests = Some(vec![info]);
    svc.store().update_workspace_pr_linkage(&row).await.unwrap();
}

async fn assert_queue(svc: &Services, ws: &WorkspaceId, queued: bool) {
    let row = svc.store().get_workspace(ws).await.unwrap();
    let expected = queued.then_some(true);
    assert_eq!(row.active_pull_request.unwrap().is_in_merge_queue, expected);
    assert_eq!(row.pull_requests.unwrap()[0].is_in_merge_queue, expected);
    let rows = svc.list_workspaces(false).await.unwrap();
    let row = rows.iter().find(|row| &row.id == ws).unwrap();
    assert_eq!(
        row.pull_requests.as_ref().unwrap()[0].is_in_merge_queue,
        expected
    );
}

#[intent_test_macros::daemon_test]
async fn registration_projects_queue_without_a_hover() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    seed_link(&svc, &forge, &ws).await;
    forge.edit_quiet(|s| s.in_merge_queue = Some(true));
    let before = forge.fetches();
    register(&svc, &ws, &owner).await;
    assert_queue(&svc, &ws, true).await;
    assert_eq!(forge.fetches() - before, 1);
}

#[intent_test_macros::daemon_test]
async fn poll_and_explicit_check_project_queue_to_sibling_workspaces() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    forge.edit_quiet(|s| s.folded = Some(FoldedRead::default()));
    let svc = svc.with_pr_monitor_debounce_seconds(3600);
    let (sibling_ws, sibling) = sibling_workspace(&svc, "agent-home-sibling").await;
    for id in [&ws, &sibling_ws] {
        seed_link(&svc, &forge, id).await;
    }
    let monitor = register(&svc, &ws, &owner).await;
    register(&svc, &sibling_ws, &sibling).await;
    for queued in [true, false] {
        forge.edit_quiet(|s| s.in_merge_queue = Some(queued));
        let before = forge.sub_fetches("pr_observation");
        let rules_before = forge.sub_fetches("branch_rules");
        svc.poll_pr_monitors().await;
        assert_eq!(
            forge.sub_fetches("branch_rules"),
            rules_before,
            "queue changes reuse cached rules"
        );
        for id in [&ws, &sibling_ws] {
            assert_queue(&svc, id, queued).await;
            let events = svc
                .store()
                .events_by_type(id, "pr:updated", 10)
                .await
                .unwrap();
            assert_eq!(
                events[0].data["pullRequests"][0]["isInMergeQueue"],
                if queued { json!(true) } else { Value::Null }
            );
        }
        let (preview, fetched) = svc.serve_pr(&RepoRef::new("o", "r"), 42).await.unwrap();
        assert!(!fetched, "preview reuses the same observation");
        assert_eq!(
            preview.snapshot.requirements.is_in_merge_queue,
            queued.then_some(true)
        );
        assert_eq!(
            forge.sub_fetches("pr_observation") - before,
            1,
            "siblings share one observation"
        );
        let before_events = svc
            .store()
            .events_by_type(&ws, "pr:updated", 100)
            .await
            .unwrap()
            .len();
        svc.poll_pr_monitors().await;
        assert_eq!(
            svc.store()
                .events_by_type(&ws, "pr:updated", 100)
                .await
                .unwrap()
                .len(),
            before_events,
            "an unchanged observation emits no redundant PR update"
        );
    }
    forge.edit_quiet(|s| s.in_merge_queue = Some(true));
    svc.pr_monitor_check_and_flush(&ws, &monitor.monitor_id)
        .await
        .unwrap();
    assert_queue(&svc, &ws, true).await;
    assert_queue(&svc, &sibling_ws, true).await;
}

#[intent_test_macros::daemon_test]
async fn monitor_only_home_rows_carry_queue_and_clear_it_on_exit() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    forge.edit_quiet(|s| s.folded = Some(FoldedRead::default()));
    let svc = svc.with_pr_monitor_debounce_seconds(3600);
    forge.edit_quiet(|s| s.in_merge_queue = Some(true));
    register(&svc, &ws, &owner).await;
    for queued in [true, false] {
        forge.edit_quiet(|s| s.in_merge_queue = Some(queued));
        svc.poll_pr_monitors().await;
        let rows = svc.list_workspaces(false).await.unwrap();
        let row = rows.iter().find(|row| row.id == ws).unwrap();
        assert_eq!(
            row.pull_requests.as_ref().unwrap()[0].is_in_merge_queue,
            queued.then_some(true)
        );
        assert!(
            svc.store()
                .get_workspace(&ws)
                .await
                .unwrap()
                .pull_requests
                .is_none(),
            "monitor-only PRs remain an emit projection, not persisted linkage"
        );
    }
}

#[intent_test_macros::daemon_test]
async fn polling_projects_git_root_queue_without_touching_another_repo_or_head() {
    let (_db, root_dir, svc, forge, ws, owner) = setup().await;
    forge.edit_quiet(|s| s.folded = Some(FoldedRead::default()));
    let svc = svc.with_pr_monitor_debounce_seconds(3600);
    seed_link(&svc, &forge, &ws).await;
    let pr = forge.get_pr(&RepoRef::new("o", "r"), 42).await.unwrap();
    let info = pr_ops::build_pr_info(&pr);
    let (root_ws, _) = sibling_workspace(&svc, "agent-home-root").await;
    let root = intent_core::WorkspaceGitRoot {
        id: intent_core::WorkspaceGitRootId::new(),
        workspace_id: root_ws.clone(),
        path: root_dir.path().to_string_lossy().into_owned(),
        source: intent_core::WorkspaceGitRootSource::Agent,
        repo_owner: Some("o".into()),
        repo_name: Some("r".into()),
        registered_by_agent_ids: vec![],
        registered_commit_sha: None,
        pr_number: Some(42),
        pr_url: Some(info.url.clone()),
        pr_status: Some(info.status),
        pull_requests: Some(vec![info]),
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    svc.store().upsert_workspace_git_root(&root).await.unwrap();
    let (foreign_ws, _) = sibling_workspace(&svc, "agent-home-other-repo").await;
    seed_link(&svc, &forge, &foreign_ws).await;
    let mut foreign = svc.store().get_workspace(&foreign_ws).await.unwrap();
    foreign.pr_url = Some("https://github.com/other/repo/pull/42".into());
    foreign.active_pull_request.as_mut().unwrap().url = foreign.pr_url.clone().unwrap();
    foreign.pull_requests.as_mut().unwrap()[0].url = foreign.pr_url.clone().unwrap();
    svc.store()
        .update_workspace_pr_linkage(&foreign)
        .await
        .unwrap();
    register(&svc, &ws, &owner).await;

    // The workspace copy moved on while this monitor still observes its old head.
    let mut moved = svc.store().get_workspace(&ws).await.unwrap();
    moved.active_pull_request.as_mut().unwrap().head_sha = Some("newer-head".into());
    moved.pull_requests.as_mut().unwrap()[0].head_sha = Some("newer-head".into());
    svc.store()
        .update_workspace_pr_linkage(&moved)
        .await
        .unwrap();
    forge.edit_quiet(|s| s.in_merge_queue = Some(true));
    svc.poll_pr_monitors().await;
    let list = svc.list_workspaces(false).await.unwrap();
    let root_row = list.iter().find(|row| row.id == root_ws).unwrap();
    assert_eq!(
        root_row.pull_requests.as_ref().unwrap()[0].is_in_merge_queue,
        Some(true)
    );
    assert_eq!(
        svc.store()
            .events_by_type(&root_ws, "gitRoot:updated", 10)
            .await
            .unwrap()
            .len(),
        1
    );
    for id in [&ws, &foreign_ws] {
        assert_queue(&svc, id, false).await;
    }
}

#[intent_test_macros::daemon_test]
async fn monitor_only_terminal_and_draft_rows_never_claim_to_be_queued() {
    for state in [PrState::Merged, PrState::Closed, PrState::Open] {
        let (_db, _root, svc, forge, ws, owner) = setup().await;
        forge.edit_quiet(|s| {
            s.in_merge_queue = Some(true);
            s.pr_state = state;
            s.draft = state == PrState::Open;
        });
        register(&svc, &ws, &owner).await;
        let rows = svc.list_workspaces(false).await.unwrap();
        let row = rows.iter().find(|row| row.id == ws).unwrap();
        assert_eq!(
            row.pull_requests.as_ref().unwrap()[0].is_in_merge_queue,
            None
        );
    }
}
