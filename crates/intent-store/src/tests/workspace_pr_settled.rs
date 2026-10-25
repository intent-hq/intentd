//! `workspaces_with_settled_pull_requests`: the settled-PR projection behind
//! the slow automatic-check tier for workspaces whose PRs are all finished.

use intent_core::{
    now_iso, PullRequestInfo, PullRequestStatus, WorkspaceGitRoot, WorkspaceGitRootId,
    WorkspaceGitRootSource, WorkspaceId,
};

use super::{sample_workspace, TempDb};
use crate::Store;

fn pr(number: u64, status: PullRequestStatus) -> PullRequestInfo {
    PullRequestInfo {
        id: format!("pr-{number}"),
        number,
        url: format!("https://github.com/intent-hq/intentd/pull/{number}"),
        title: "Change".to_string(),
        status,
        created_at: now_iso(),
        updated_at: now_iso(),
        base_ref: Some("main".to_string()),
        head_ref: Some("feature".to_string()),
        head_sha: None,
        author: None,
        mergeable: None,
        mergeable_state: None,
        is_draft: None,
        is_in_merge_queue: None,
    }
}

fn root(ws: &WorkspaceId, prs: Option<Vec<PullRequestInfo>>) -> WorkspaceGitRoot {
    let ts = now_iso();
    WorkspaceGitRoot {
        id: WorkspaceGitRootId::new(),
        workspace_id: ws.clone(),
        path: format!("/tmp/settled-root-{}", ws.0),
        source: WorkspaceGitRootSource::Auto,
        repo_owner: Some("intent-hq".to_string()),
        repo_name: Some("intentd".to_string()),
        registered_by_agent_ids: vec![],
        registered_commit_sha: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: prs,
        created_at: ts.clone(),
        updated_at: ts,
    }
}

/// (id, link status, active PR, pool, git-root pool, expected settled).
type Case = (
    &'static str,
    Option<PullRequestStatus>,
    Option<PullRequestStatus>,
    Option<Vec<PullRequestStatus>>,
    Option<Vec<PullRequestStatus>>,
    bool,
);

#[tokio::test]
async fn settled_workspaces_need_a_pr_and_no_open_or_unknown_one() {
    use PullRequestStatus::{Closed, Draft, Merged, Open};
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    assert!(store
        .workspaces_with_settled_pull_requests(&[])
        .await
        .unwrap()
        .is_empty());

    #[rustfmt::skip]
    let cases: Vec<Case> = vec![
        ("no-prs", None, None, None, None, false),
        ("empty-pool", None, None, Some(vec![]), None, false),
        ("merged", Some(Merged), Some(Merged), Some(vec![Merged]), None, true),
        ("closed-pool", None, None, Some(vec![Closed, Merged]), None, true),
        ("open-pool", None, None, Some(vec![Merged, Open]), None, false),
        ("draft-pool", None, None, Some(vec![Draft]), None, false),
        ("open-link", Some(Open), None, Some(vec![Merged]), None, false),
        ("open-active", None, Some(Open), Some(vec![Merged]), None, false),
        ("root-merged", None, None, None, Some(vec![Merged]), true),
        ("root-open", Some(Merged), None, None, Some(vec![Open]), false),
    ];
    let mut ids = Vec::new();
    for (id, link, active, pool, root_pool, _) in &cases {
        let id = WorkspaceId::from(*id);
        let mut ws = sample_workspace(&id, &id.0, false);
        if let Some(status) = link {
            ws.pr_number = Some(1);
            ws.pr_url = Some("https://github.com/intent-hq/intentd/pull/1".into());
            ws.pr_status = Some(*status);
        }
        ws.active_pull_request = active.map(|status| pr(1, status));
        ws.pull_requests = pool.as_ref().map(|statuses| {
            statuses
                .iter()
                .enumerate()
                .map(|(i, status)| pr(10 + i as u64, *status))
                .collect()
        });
        store.insert_workspace(&ws).await.unwrap();
        if let Some(statuses) = root_pool {
            let prs = statuses
                .iter()
                .enumerate()
                .map(|(i, status)| pr(100 + i as u64, *status))
                .collect();
            store
                .upsert_workspace_git_root(&root(&id, Some(prs)))
                .await
                .unwrap();
        }
        ids.push(id);
    }
    let excluded = WorkspaceId::from("excluded-merged");
    let mut ws = sample_workspace(&excluded, "excluded", false);
    ws.pull_requests = Some(vec![pr(5, Merged)]);
    store.insert_workspace(&ws).await.unwrap();

    // A link without a recorded status is unknown, not settled.
    let unknown = WorkspaceId::from("unknown-link");
    let mut ws = sample_workspace(&unknown, "unknown", false);
    ws.pr_url = Some("https://github.com/intent-hq/intentd/pull/2".into());
    ws.pull_requests = Some(vec![pr(6, Merged)]);
    store.insert_workspace(&ws).await.unwrap();
    ids.push(unknown);

    ids.push(WorkspaceId::from("missing"));
    ids.push(ids[2].clone());
    let settled = store
        .workspaces_with_settled_pull_requests(&ids)
        .await
        .unwrap();
    for (id, .., expected) in &cases {
        assert_eq!(
            settled.contains(&WorkspaceId::from(*id)),
            *expected,
            "workspace {id}"
        );
    }
    assert!(!settled.contains(&WorkspaceId::from("unknown-link")));
    assert!(!settled.contains(&excluded));
    assert_eq!(settled.len(), 3);
}

#[tokio::test]
async fn malformed_pr_snapshots_are_not_settled() {
    let db = TempDb::new();
    let store = Store::open(&db.path).await.unwrap();
    let id = WorkspaceId::from("malformed");
    let mut ws = sample_workspace(&id, "malformed", false);
    ws.pull_requests = Some(vec![pr(1, PullRequestStatus::Merged)]);
    store.insert_workspace(&ws).await.unwrap();
    let ids = [id.clone()];
    assert!(store
        .workspaces_with_settled_pull_requests(&ids)
        .await
        .unwrap()
        .contains(&id));
    for column in ["active_pull_request", "pull_requests"] {
        sqlx::query(&format!(
            "UPDATE workspace SET {column} = 'not json' WHERE id = ?"
        ))
        .bind(&id.0)
        .execute(store.write_pool())
        .await
        .unwrap();
        assert!(
            store
                .workspaces_with_settled_pull_requests(&ids)
                .await
                .unwrap()
                .is_empty(),
            "{column}"
        );
        sqlx::query(&format!(
            "UPDATE workspace SET {column} = NULL WHERE id = ?"
        ))
        .bind(&id.0)
        .execute(store.write_pool())
        .await
        .unwrap();
    }
}
