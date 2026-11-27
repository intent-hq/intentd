//! Paged `workspace.list` read cost: each page's store reads stay scoped to
//! the page's own workspaces, plus a manual latency baseline over a
//! host-shaped store. Timings are diagnostic only: no wall-clock thresholds
//! run in CI.

use std::time::{Duration, Instant};

use intent_core::{
    AgentId, AgentSession, AgentStatus, PrMonitor, PrMonitorId, PrMonitorState, PullRequestInfo,
    PullRequestStatus, WorkspaceApi, WorkspaceGitRoot, WorkspaceGitRootId, WorkspaceGitRootSource,
    WorkspaceId, WorkspaceListFilter,
};
use intent_store::Store;
use serde_json::json;

use super::{workspace, TempDb};
use crate::test_tracing::{capture_sqlx_queries, warm_sqlx_pool};
use crate::Services;

#[derive(Clone, Copy)]
struct Shape {
    active: usize,
    archived: usize,
    agents_per_workspace: usize,
    completed_monitors_per_workspace: usize,
    system_prompt_bytes: usize,
    metadata_prompt_bytes: usize,
    snapshot_bytes: usize,
}

fn stamp(i: usize) -> String {
    format!(
        "2026-01-01T{:02}:{:02}:{:02}Z",
        i / 3600,
        (i / 60) % 60,
        i % 60
    )
}

fn pr(number: u64) -> PullRequestInfo {
    PullRequestInfo {
        id: number.to_string(),
        number,
        url: format!("https://github.com/o/r/pull/{number}"),
        title: format!("Root PR {number}"),
        status: PullRequestStatus::Open,
        created_at: "2026-01-01T00:00:00Z".into(),
        updated_at: "2026-01-01T00:00:00Z".into(),
        base_ref: None,
        head_ref: None,
        head_sha: None,
        author: None,
        mergeable: None,
        mergeable_state: None,
        is_draft: None,
        is_in_merge_queue: None,
    }
}

fn snapshot(number: u64, pad: usize) -> String {
    json!({
        "title": format!("Monitored PR {number}"),
        "url": format!("https://github.com/o/r/pull/{number}"),
        "headSha": "abc123",
        "prUpdatedAt": "2026-01-01T12:00:00Z",
        "requirements": { "state": "merged", "isDraft": false, "mergeable": true },
        "pad": "s".repeat(pad),
    })
    .to_string()
}

/// Seed `shape` and return the active workspace ids in list order. Every
/// workspace owns agents, PR monitors (one active plus completed history)
/// and a PR-bearing git root, so an unscoped bulk read returns rows for
/// workspaces outside the requested page.
async fn seed(store: &Store, shape: Shape) -> Vec<WorkspaceId> {
    let mut active = Vec::new();
    let mut pr_number = 0u64;
    for i in 0..shape.active + shape.archived {
        let id = WorkspaceId::from(format!("ws-latency-{i:04}"));
        let mut ws = workspace(&id);
        ws.created_at = stamp(i);
        ws.updated_at = stamp(i);
        if i >= shape.active {
            ws.archived = true;
            ws.status = intent_core::WorkspaceStatus::Archived;
        } else {
            active.push(id.clone());
        }
        store.insert_workspace(&ws).await.unwrap();
        for a in 0..shape.agents_per_workspace {
            store
                .insert_agent_session(&AgentSession {
                    id: AgentId::from(format!("agent-{i:04}-{a}")),
                    workspace_id: id.clone(),
                    name: format!("Agent {a}"),
                    name_explicitly_set: true,
                    status: AgentStatus::Idle,
                    specialist: Some("developer".into()),
                    created_at: stamp(i * 10 + a),
                    updated_at: stamp(i * 10 + a),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        let owner = AgentId::from(format!("agent-{i:04}-0"));
        for m in 0..=shape.completed_monitors_per_workspace {
            pr_number += 1;
            let state = if m == 0 {
                PrMonitorState::Active
            } else {
                PrMonitorState::Completed
            };
            let snap = snapshot(pr_number, shape.snapshot_bytes);
            store
                .insert_pr_monitor(&PrMonitor {
                    monitor_id: PrMonitorId::new(),
                    workspace_id: id.clone(),
                    agent_id: owner.clone(),
                    repo_owner: "o".into(),
                    repo_name: "r".into(),
                    pr_number: i64::try_from(pr_number).unwrap(),
                    state,
                    last_snapshot: Some(snap.clone()),
                    baseline_snapshot: Some(snap),
                    pending_changes: vec![],
                    pending_since: None,
                    last_change_at: None,
                    last_polled_at: None,
                    last_error: None,
                    created_at: stamp(m),
                    updated_at: stamp(m),
                })
                .await
                .unwrap();
        }
        pr_number += 1;
        store
            .upsert_workspace_git_root(&WorkspaceGitRoot {
                id: WorkspaceGitRootId::new(),
                workspace_id: id.clone(),
                path: format!("/tmp/latency-root-{i}"),
                source: WorkspaceGitRootSource::Agent,
                repo_owner: Some("o".into()),
                repo_name: Some("r".into()),
                registered_by_agent_ids: vec![],
                registered_commit_sha: None,
                pr_number: None,
                pr_url: None,
                pr_status: None,
                pull_requests: Some(vec![pr(pr_number)]),
                created_at: stamp(i),
                updated_at: stamp(i),
            })
            .await
            .unwrap();
    }
    // Direct fixture write (never production data): the heavy agent columns
    // the host carries — a large system prompt stored mid-row and prompt
    // copies inside `metadata` — next to the small keys the list reads.
    let body = |c: &str| c.repeat(shape.metadata_prompt_bytes);
    let metadata = json!({
        "behaviorPrompt": body("b"),
        "specialistGeneratedBehaviorPrompt": body("g"),
        "initialMessage": body("i"),
        "specialistRoleReminder": "Stay in role.",
        "specialistName": "Developer",
        "lastSeenMessageId": "msg-1",
    })
    .to_string();
    sqlx::query("UPDATE agent_session SET system_prompt = ?, metadata = ?")
        .bind("p".repeat(shape.system_prompt_bytes))
        .bind(metadata)
        .execute(store.write_pool())
        .await
        .unwrap();
    active
}

async fn walk(svc: &Services, filter: WorkspaceListFilter) -> Vec<(Duration, usize, usize)> {
    let mut pages = Vec::new();
    let mut token = None;
    loop {
        let start = Instant::now();
        let page = svc
            .list_workspaces_page(filter, false, None, token.take())
            .await
            .expect("page");
        let elapsed = start.elapsed();
        let bytes = serde_json::to_vec(&page.workspaces).unwrap().len();
        pages.push((elapsed, page.workspaces.len(), bytes));
        match page.next_token {
            Some(next) => token = Some(next),
            None => return pages,
        }
    }
}

/// A page's store reads are O(rows of the page's workspaces), never
/// O(every workspace's history): with two workspaces per page and every
/// other workspace owning agents, monitors and git-root PRs, no statement
/// returns more rows than the page's two workspaces own. An unscoped bulk
/// read (all monitors, all git roots) returns rows for all 24 workspaces.
#[intent_test_macros::daemon_test]
async fn workspace_list_page_reads_scale_with_the_page() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let shape = Shape {
        active: 12,
        archived: 12,
        agents_per_workspace: 3,
        completed_monitors_per_workspace: 3,
        system_prompt_bytes: 16,
        metadata_prompt_bytes: 16,
        snapshot_bytes: 16,
    };
    let active = seed(&store, shape).await;
    let svc = Services::new_isolated(store.clone());
    warm_sqlx_pool(store.read_pool()).await;
    // Two workspaces own at most 2 × 4 monitor rows; the page read itself
    // fetches one look-ahead row past the page.
    let per_page_bound = 2 * (shape.completed_monitors_per_workspace + 1);
    for filter in [
        WorkspaceListFilter::ACTIVE,
        WorkspaceListFilter::WITH_ARCHIVED,
    ] {
        let (page, queries) =
            capture_sqlx_queries(svc.list_workspaces_page(filter, false, Some(2), None)).await;
        let page = page.expect("page");
        assert_eq!(
            page.workspaces.iter().map(|w| &w.id).collect::<Vec<_>>(),
            active.iter().take(2).collect::<Vec<_>>()
        );
        let row = page.workspaces[0].clone();
        assert_eq!(row.agent_summary.as_ref().map(|s| s.count), Some(3));
        let prs = row.pull_requests.as_ref().expect("merged pull requests");
        assert_eq!(
            prs.len(),
            shape.completed_monitors_per_workspace + 2,
            "git-root PR plus one PR per non-cancelled monitor"
        );
        let over: Vec<_> = queries
            .iter()
            .filter(|q| usize::try_from(q.rows_returned).unwrap() > per_page_bound)
            .map(|q| {
                (
                    q.rows_returned,
                    q.sql
                        .split_whitespace()
                        .take(12)
                        .collect::<Vec<_>>()
                        .join(" "),
                )
            })
            .collect();
        assert!(
            over.is_empty(),
            "statements returning more than the page's {per_page_bound} rows: {over:#?}"
        );
    }
}

/// Manual latency baseline for the paged list. `host` mirrors the daemon
/// behind the 36 s pages (771 workspaces, 22 active, ~4.5 agents each with
/// ~70 KB system prompts and ~7 KB prompt copies in metadata, ~3 monitors
/// each); `small` is ~20 workspaces with ~60 agents. Each shape is walked
/// by one reader, then by three concurrent readers (the reconnect burst
/// where three desktop windows re-read the list at once).
#[intent_test_macros::daemon_test(flavor = "multi_thread")]
#[ignore = "manual workspace.list page latency baseline; run with --ignored --nocapture"]
async fn workspace_list_page_latency_baseline() {
    for (label, filter, shape) in [
        (
            "small",
            WorkspaceListFilter::ACTIVE,
            Shape {
                active: 20,
                archived: 0,
                agents_per_workspace: 3,
                completed_monitors_per_workspace: 3,
                system_prompt_bytes: 70_000,
                metadata_prompt_bytes: 2_300,
                snapshot_bytes: 5_000,
            },
        ),
        (
            "host",
            WorkspaceListFilter::WITH_ARCHIVED,
            Shape {
                active: 22,
                archived: 750,
                agents_per_workspace: 4,
                completed_monitors_per_workspace: 2,
                system_prompt_bytes: 70_000,
                metadata_prompt_bytes: 2_300,
                snapshot_bytes: 5_000,
            },
        ),
    ] {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let seeded = Instant::now();
        seed(&store, shape).await;
        eprintln!(
            "WORKSPACE_LIST shape={label} workspaces={} agents={} seed_ms={}",
            shape.active + shape.archived,
            (shape.active + shape.archived) * shape.agents_per_workspace,
            seeded.elapsed().as_millis()
        );
        let svc = Services::new_isolated(store.clone());
        walk(&svc, filter).await;
        let (_, queries) =
            capture_sqlx_queries(svc.list_workspaces_page(filter, false, None, None)).await;
        let rows: u64 = queries.iter().map(|q| q.rows_returned).sum();
        eprintln!(
            "WORKSPACE_LIST shape={label} first_page_statements={} first_page_rows={rows}",
            queries.len()
        );
        for readers in [1usize, 3] {
            let start = Instant::now();
            let handles: Vec<_> = (0..readers)
                .map(|_| {
                    let svc = svc.clone();
                    tokio::spawn(intent_core::with_caller(
                        intent_core::Caller::Daemon,
                        Box::pin(async move { walk(&svc, filter).await }),
                    ))
                })
                .collect();
            let mut pages = Vec::new();
            for handle in handles {
                pages.extend(handle.await.unwrap());
            }
            let wall = start.elapsed();
            pages.sort_by_key(|(elapsed, _, _)| *elapsed);
            let p50 = pages[pages.len() / 2].0;
            let max = pages.last().unwrap().0;
            let rows: usize = pages.iter().map(|(_, n, _)| n).sum::<usize>() / readers;
            let bytes = pages.iter().map(|(_, _, b)| b).max().unwrap();
            eprintln!(
                "WORKSPACE_LIST shape={label} readers={readers} pages={} rows_per_walk={rows} \
                 page_p50_ms={} page_max_ms={} max_page_bytes={bytes} wall_ms={}",
                pages.len() / readers,
                p50.as_millis(),
                max.as_millis(),
                wall.as_millis()
            );
        }
    }
}
