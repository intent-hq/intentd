//! PR monitors on both backends: legacy and qualified reads, the poll and
//! lifecycle writes with the rate-limit pause composition, target
//! qualification, the legacy re-arm and the prepared writes.
//!
//! Ties: `m-a` / `m-Z` share a `created_at` and are inserted in the order
//! opposite to their byte order, so rowid-ordered reads (`ORDER BY
//! created_at`) and id-ordered reads (`created_at, monitor_id`) differ, and
//! `COLLATE "C"` decides the latter. `m-done2` / `m-done1` share the latest
//! completed `updated_at`, and `m-b0` shares its `created_at` with them.
//!
//! Decoy rule: each body ends by resetting the decoy's monitors to their
//! seeded state (active rows every filter selects, completed and cancelled
//! history, pause-eligible `m-lgw`) and giving it rows the subject probes
//! as absent (`m-missing`, `m-ghost`) or creates (`m-new`). Titles, errors
//! and pending changes carry the tenant, so a leaked row never looks right.

use intent_core::{
    AgentId, AgentSession, Error, Hook, HookId, HookState, PrMonitor, PrMonitorId, PrMonitorState,
    RepositoryProvider, RepositoryResourceKind, RepositoryTarget, ReviewTarget, Workspace,
    WorkspaceId,
};
use intent_store::{
    pr_monitor_pause_error, MonitorQualificationOutcome, MonitorTargetProvenance,
    PersistedMonitorTarget, PrMonitorListEntry, PrMonitorPollUpdate, PreparedMonitorWrite, Store,
    SubscriptionAgentProjection,
};
use serde_json::{json, Value};

use crate::harness::{parity_test, TestDb};

const T0: &str = "2026-01-01T00:00:00.000Z";
const T1: &str = "2026-01-02T00:00:00.000Z";
const T2: &str = "2026-01-03T00:00:00.000Z";
const T3: &str = "2026-01-04T00:00:00.000Z";

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

fn aid(id: &str) -> AgentId {
    AgentId(id.into())
}

fn mid(id: &str) -> PrMonitorId {
    PrMonitorId(id.into())
}

fn ids(rows: &[PrMonitor]) -> Vec<&str> {
    rows.iter().map(|m| m.monitor_id.0.as_str()).collect()
}

fn qids(rows: &[intent_store::QualifiedPrMonitor]) -> Vec<&str> {
    rows.iter()
        .map(|m| m.monitor().monitor_id.0.as_str())
        .collect()
}

fn entry_keys(rows: &[PrMonitorListEntry]) -> Vec<(String, i64)> {
    rows.iter()
        .map(|e| (e.workspace_id.0.clone(), e.pr_number))
        .collect()
}

fn keys(pairs: &[(&str, i64)]) -> Vec<(String, i64)> {
    pairs.iter().map(|(w, n)| ((*w).to_owned(), *n)).collect()
}

fn not_found<T>(r: &intent_core::Result<T>) -> bool {
    matches!(r, Err(Error::NotFound(_)))
}

fn invalid<T>(r: &intent_core::Result<T>) -> bool {
    matches!(r, Err(Error::InvalidParams(_)))
}

fn internal<T>(r: &intent_core::Result<T>) -> bool {
    matches!(r, Err(Error::Internal(_)))
}

fn target(
    provider: RepositoryProvider,
    project: &str,
    kind: RepositoryResourceKind,
    number: u64,
) -> ReviewTarget {
    let instance = match provider {
        RepositoryProvider::Github => "https://github.com",
        RepositoryProvider::Gitlab => "https://git.example:8443/forge",
    };
    ReviewTarget {
        repository: RepositoryTarget {
            provider,
            instance_base_url: instance.into(),
            project_path: project.into(),
        },
        kind,
        number,
    }
}

fn gh(project: &str, number: u64) -> ReviewTarget {
    target(
        RepositoryProvider::Github,
        project,
        RepositoryResourceKind::PullRequest,
        number,
    )
}

fn gl(project: &str, number: u64) -> ReviewTarget {
    target(
        RepositoryProvider::Gitlab,
        project,
        RepositoryResourceKind::MergeRequest,
        number,
    )
}

/// A valid snapshot carrying the list-entry scalars.
fn snapshot(tenant: &str) -> String {
    serde_json::json!({
        "url": "https://github.com/Org/Repo/pull/1",
        "title": format!("{tenant} one"),
        "headSha": "abc123",
        "requirements": {
            "state": "open",
            "isDraft": false,
            "mergeable": true,
            "isInMergeQueue": false,
        },
        "prUpdatedAt": "2026-01-02T03:04:05Z",
    })
    .to_string()
}

/// `(id, agent, workspace, owner, name, number, state, created_at, updated_at)`.
type Spec = (
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    i64,
    PrMonitorState,
    &'static str,
    &'static str,
);

/// Legacy rows, in insertion (rowid) order.
const LEGACY: &[Spec] = &[
    (
        "m-a",
        "agent-a",
        "ws-a",
        "org",
        "repo",
        2,
        PrMonitorState::Active,
        T1,
        T1,
    ),
    (
        "m-Z",
        "agent-a",
        "ws-a",
        "Org",
        "Repo",
        1,
        PrMonitorState::Active,
        T1,
        T1,
    ),
    (
        "m-b0",
        "agent-b",
        "ws-a",
        "Org",
        "Repo",
        3,
        PrMonitorState::Active,
        T0,
        T0,
    ),
    (
        "m-done2",
        "agent-b",
        "ws-a",
        "Org",
        "Repo",
        4,
        PrMonitorState::Completed,
        T0,
        T2,
    ),
    (
        "m-done1",
        "agent-b",
        "ws-a",
        "Org",
        "Repo",
        5,
        PrMonitorState::Completed,
        T0,
        T2,
    ),
    (
        "m-old",
        "agent-b",
        "ws-a",
        "Org",
        "Repo",
        6,
        PrMonitorState::Completed,
        T0,
        T0,
    ),
    (
        "m-cancel",
        "agent-a",
        "ws-a",
        "Org",
        "Repo",
        1,
        PrMonitorState::Cancelled,
        T0,
        T0,
    ),
];

fn monitor(spec: &Spec, tenant: &str) -> PrMonitor {
    let (id, agent, ws, owner, name, number, state, created_at, updated_at) = *spec;
    PrMonitor {
        monitor_id: mid(id),
        workspace_id: wid(ws),
        agent_id: aid(agent),
        repo_owner: owner.into(),
        repo_name: name.into(),
        pr_number: number,
        state,
        last_snapshot: Some(if id == "m-a" {
            format!("not json {tenant}")
        } else {
            snapshot(tenant)
        }),
        baseline_snapshot: Some(format!("{{\"baseline\":\"{tenant}\"}}")),
        pending_changes: vec![format!("{tenant} change")],
        pending_since: Some(T0.into()),
        last_change_at: Some(T0.into()),
        last_polled_at: Some(T0.into()),
        last_error: Some(format!("{tenant} boom")),
        created_at: created_at.into(),
        updated_at: updated_at.into(),
    }
}

fn spec(id: &str) -> &'static Spec {
    LEGACY
        .iter()
        .chain(QUALIFIED.iter().map(|(s, _)| s))
        .chain(LATE.iter())
        .find(|s| s.0 == id)
        .expect("known monitor")
}

/// Qualified rows (after [`LEGACY`]) with their target and provenance.
const QUALIFIED: &[(Spec, &str)] = &[
    (
        (
            "m-c",
            "agent-c",
            "ws-b",
            "Grp",
            "Proj",
            9,
            PrMonitorState::Active,
            T0,
            T0,
        ),
        "gl",
    ),
    (
        (
            "m-q",
            "agent-a",
            "ws-a",
            "Org",
            "Repo2",
            7,
            PrMonitorState::Active,
            T2,
            T2,
        ),
        "captured",
    ),
    (
        (
            "m-lgw",
            "agent-a",
            "ws-a",
            "Org",
            "Repo3",
            8,
            PrMonitorState::Active,
            T2,
            T2,
        ),
        "writer",
    ),
];

/// Legacy rows inserted last: `m-z` in the archived workspace and `m-u9`,
/// an unresolved claim on `m-c`'s identity.
const LATE: &[Spec] = &[
    (
        "m-z",
        "agent-z",
        "ws-z",
        "Org",
        "Repo",
        1,
        PrMonitorState::Active,
        T0,
        T0,
    ),
    (
        "m-u9",
        "agent-c",
        "ws-b",
        "Grp",
        "Proj",
        9,
        PrMonitorState::Active,
        T0,
        T0,
    ),
];

fn qualified_target(id: &str) -> (ReviewTarget, MonitorTargetProvenance) {
    match id {
        "m-c" => (gl("Grp/Proj", 9), MonitorTargetProvenance::CapturedRequest),
        "m-q" => (gh("Org/Repo2", 7), MonitorTargetProvenance::CapturedRequest),
        _ => (
            gh("Org/Repo3", 8),
            MonitorTargetProvenance::LegacyGithubWriter,
        ),
    }
}

async fn seed(store: &Store, tenant: &str) {
    for (ws, archived) in [("ws-a", false), ("ws-b", false), ("ws-z", true)] {
        store
            .insert_workspace(&Workspace {
                id: wid(ws),
                title: format!("{tenant} {ws}"),
                branch: "main".into(),
                archived,
                created_at: T0.into(),
                updated_at: T0.into(),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    for (agent, ws) in [
        ("agent-a", "ws-a"),
        ("agent-b", "ws-a"),
        ("agent-c", "ws-b"),
        ("agent-z", "ws-z"),
    ] {
        store
            .insert_agent_session(&AgentSession {
                id: aid(agent),
                workspace_id: wid(ws),
                name: format!("{agent} of {tenant}"),
                created_at: T0.into(),
                updated_at: T0.into(),
                ..AgentSession::default()
            })
            .await
            .unwrap();
    }
    seed_monitors(store, tenant).await;
}

async fn seed_monitors(store: &Store, tenant: &str) {
    for s in LEGACY {
        assert!(store.insert_pr_monitor(&monitor(s, tenant)).await.unwrap());
    }
    for (s, _) in QUALIFIED {
        let (target, provenance) = qualified_target(s.0);
        assert!(store
            .insert_qualified_pr_monitor(&monitor(s, tenant), &target, provenance)
            .await
            .unwrap());
    }
    for s in LATE {
        assert!(store.insert_pr_monitor(&monitor(s, tenant)).await.unwrap());
    }
}

const NEW: Spec = (
    "m-new",
    "agent-c",
    "ws-b",
    "Grp",
    "Other",
    11,
    PrMonitorState::Active,
    T3,
    T3,
);

/// Reset the decoy's monitors to their seeded state and add the rows the
/// subject probes as absent or creates.
async fn decoy_rows(store: &Store, tenant: &str, db: Option<&TestDb>) {
    if tenant != "decoy" {
        return;
    }
    let db = db.expect("the decoy tenant runs only on Postgres");
    sqlx::query("DELETE FROM pr_monitor WHERE tenant_id = 'decoy'")
        .execute(&db.admin)
        .await
        .unwrap();
    seed_monitors(store, tenant).await;
    for s in [
        (
            "m-missing",
            "agent-a",
            "ws-a",
            "Ghost",
            "Missing",
            50,
            PrMonitorState::Active,
            T1,
            T1,
        ),
        (
            "m-ghost",
            "agent-a",
            "ws-a",
            "nobody",
            "x",
            99,
            PrMonitorState::Active,
            T1,
            T1,
        ),
    ] {
        assert!(store.insert_pr_monitor(&monitor(&s, tenant)).await.unwrap());
    }
    assert!(store
        .insert_qualified_pr_monitor(
            &monitor(&NEW, tenant),
            &gh("Grp/Other", 11),
            MonitorTargetProvenance::CapturedRequest
        )
        .await
        .unwrap());
}

parity_test!(
    pr_monitor_reads_filter_order_and_project,
    |store, tenant, db| {
        seed(&store, tenant).await;
        let (ws_a, ws_b) = (wid("ws-a"), wid("ws-b"));

        assert_eq!(
            store.get_pr_monitor(&mid("m-Z")).await.unwrap(),
            monitor(spec("m-Z"), tenant)
        );
        assert!(not_found(&store.get_pr_monitor(&mid("m-missing")).await));
        assert!(not_found(
            &store.get_qualified_pr_monitor(&mid("m-missing")).await
        ));

        let found = store
            .find_active_pr_monitor(&aid("agent-a"), "ORG", "repo", 1)
            .await
            .unwrap();
        assert_eq!(found.map(|m| m.monitor_id), Some(mid("m-Z")));
        let found = store
            .find_active_pr_monitor(&aid("agent-a"), "org", "REPO2", 7)
            .await
            .unwrap();
        assert_eq!(
            found.map(|m| m.monitor_id),
            Some(mid("m-q")),
            "github.com PR"
        );
        assert!(store
            .find_active_pr_monitor(&aid("agent-c"), "Grp", "Proj", 9)
            .await
            .unwrap()
            .is_some_and(|m| m.monitor_id == mid("m-u9")));
        assert!(store
            .find_active_pr_monitor(&aid("agent-a"), "nobody", "x", 99)
            .await
            .unwrap()
            .is_none());
        let found = store
            .find_active_pr_monitor_in_workspace(&ws_a, "org", "Repo", 3)
            .await
            .unwrap();
        assert_eq!(found.map(|m| m.monitor_id), Some(mid("m-b0")));
        assert!(store
            .find_active_pr_monitor_in_workspace(&ws_a, "nobody", "x", 99)
            .await
            .unwrap()
            .is_none());
        assert!(
            store
                .find_active_pr_monitor_in_workspace(&ws_a, "org", "repo", 4)
                .await
                .unwrap()
                .is_none(),
            "completed"
        );

        assert_eq!(
            ids(&store
                .list_pr_monitors_by_agent(&aid("agent-a"))
                .await
                .unwrap()),
            ["m-cancel", "m-a", "m-Z", "m-q", "m-lgw"]
        );
        assert_eq!(
            ids(&store.list_pr_monitors_by_workspace(&ws_a).await.unwrap()),
            ["m-b0", "m-done2", "m-done1", "m-old", "m-cancel", "m-a", "m-Z", "m-q", "m-lgw"]
        );
        assert_eq!(
            ids(&store
                .list_active_pr_monitors_by_agent(&aid("agent-b"))
                .await
                .unwrap()),
            ["m-b0"]
        );
        assert_eq!(
            ids(&store
                .list_active_pr_monitors_by_workspace(&ws_a)
                .await
                .unwrap()),
            ["m-b0", "m-a", "m-Z", "m-q", "m-lgw"]
        );
        assert!(store
            .list_pr_monitors_by_agent(&aid("agent-none"))
            .await
            .unwrap()
            .is_empty());

        let display = ["m-b0", "m-done2", "m-a", "m-Z", "m-q", "m-lgw"];
        assert_eq!(
            ids(&store
                .list_display_status_pr_monitors_by_workspace(&ws_a)
                .await
                .unwrap()),
            display
        );
        let batch = store
            .list_display_status_pr_monitors_by_workspaces(&[
                ws_b.clone(),
                wid("ws-none"),
                ws_a.clone(),
            ])
            .await
            .unwrap();
        let mut expected = display.to_vec();
        expected.extend(["m-c", "m-u9"]);
        assert_eq!(ids(&batch), expected);
        assert!(store
            .list_display_status_pr_monitors_by_workspaces(&[])
            .await
            .unwrap()
            .is_empty());

        assert_eq!(
            ids(&store.load_active_pr_monitors().await.unwrap()),
            ["m-b0", "m-c", "m-z", "m-u9", "m-a", "m-Z", "m-q", "m-lgw"]
        );

        let all = store
            .load_non_cancelled_pr_monitor_list_entries(true)
            .await
            .unwrap();
        assert_eq!(
            entry_keys(&all),
            keys(&[
                ("ws-a", 3),
                ("ws-a", 4),
                ("ws-a", 5),
                ("ws-a", 6),
                ("ws-b", 9),
                ("ws-z", 1),
                ("ws-b", 9),
                ("ws-a", 2),
                ("ws-a", 1),
                ("ws-a", 7),
                ("ws-a", 8),
            ])
        );
        let live = store
            .load_non_cancelled_pr_monitor_list_entries(false)
            .await
            .unwrap();
        assert!(live.iter().all(|e| e.workspace_id != wid("ws-z")));
        assert_eq!(live.len(), all.len() - 1);
        let one = all
            .iter()
            .find(|e| e.pr_number == 1 && e.workspace_id == ws_a)
            .unwrap();
        assert_eq!(
            (
                one.snapshot_url.as_deref(),
                one.snapshot_title.clone(),
                one.snapshot_head_sha.as_deref(),
                one.snapshot_state.as_deref(),
                one.snapshot_is_draft,
                one.snapshot_mergeable,
                one.snapshot_is_in_merge_queue,
                one.snapshot_pr_updated_at.as_deref(),
                one.state,
            ),
            (
                Some("https://github.com/Org/Repo/pull/1"),
                Some(format!("{tenant} one")),
                Some("abc123"),
                Some("open"),
                Some(false),
                Some(true),
                Some(false),
                Some("2026-01-02T03:04:05Z"),
                PrMonitorState::Active,
            )
        );
        let bad = all.iter().find(|e| e.pr_number == 2).unwrap();
        assert!(
            bad.snapshot_url.is_none() && bad.snapshot_is_draft.is_none(),
            "invalid JSON"
        );

        let reads = store.load_workspace_pr_monitor_reads(&ws_a).await.unwrap();
        assert_eq!(
            entry_keys(&reads.list_entries),
            keys(&[
                ("ws-a", 3),
                ("ws-a", 4),
                ("ws-a", 5),
                ("ws-a", 6),
                ("ws-a", 2),
                ("ws-a", 1),
                ("ws-a", 7),
                ("ws-a", 8),
            ])
        );
        assert_eq!(ids(&reads.display_rows), display);
        let z = reads
            .display_rows
            .iter()
            .find(|m| m.monitor_id == mid("m-Z"))
            .unwrap();
        assert_eq!(z.last_snapshot.as_deref(), Some(snapshot(tenant).as_str()));
        assert_eq!(
            (
                z.agent_id.clone(),
                z.pending_changes.len(),
                z.last_error.clone()
            ),
            (aid("agent-a"), 0, None)
        );
        assert!(store
            .load_workspace_pr_monitor_reads(&wid("ws-none"))
            .await
            .unwrap()
            .list_entries
            .is_empty());

        let c = store.get_qualified_pr_monitor(&mid("m-c")).await.unwrap();
        assert_eq!(
            c.target(),
            &PersistedMonitorTarget::Resolved {
                target: gl("Grp/Proj", 9),
                provenance: MonitorTargetProvenance::CapturedRequest,
            }
        );
        let a = store.get_qualified_pr_monitor(&mid("m-a")).await.unwrap();
        assert_eq!(a.monitor(), &monitor(spec("m-a"), tenant));
        assert_eq!(
            a.target_evidence(),
            serde_json::json!({
                "provider": null, "instanceBaseUrl": null, "projectPath": null, "kind": null,
                "provenance": "unresolved", "unresolvedReason": "missing-provenance",
            })
        );
        assert_eq!(
            qids(&store.load_active_qualified_pr_monitors().await.unwrap()),
            ["m-b0", "m-c", "m-u9", "m-z", "m-Z", "m-a", "m-lgw", "m-q"]
        );
        assert_eq!(
            qids(
                &store
                    .list_qualified_pr_monitors_by_agent(&aid("agent-a"))
                    .await
                    .unwrap()
            ),
            ["m-cancel", "m-Z", "m-a", "m-lgw", "m-q"]
        );
        assert_eq!(
            qids(
                &store
                    .list_qualified_pr_monitors_by_workspace(&ws_b)
                    .await
                    .unwrap()
            ),
            ["m-c", "m-u9"]
        );

        let q = store
            .find_active_qualified_pr_monitor(&aid("agent-a"), &gh("org/REPO2", 7))
            .await
            .unwrap();
        assert_eq!(q.map(|m| m.monitor().monitor_id.clone()), Some(mid("m-q")));
        let c = store
            .find_active_qualified_pr_monitor(&aid("agent-c"), &gl("Grp/Proj", 9))
            .await
            .unwrap();
        assert_eq!(c.map(|m| m.monitor().monitor_id.clone()), Some(mid("m-c")));
        assert!(
            store
                .find_active_qualified_pr_monitor(&aid("agent-c"), &gl("grp/proj", 9))
                .await
                .unwrap()
                .is_none(),
            "GitLab paths match case-sensitively"
        );
        assert!(store
            .find_active_qualified_pr_monitor(&aid("agent-a"), &gh("Ghost/Missing", 50))
            .await
            .unwrap()
            .is_none());
        let q = store
            .find_active_qualified_pr_monitor_in_workspace(&ws_a, &gh("Org/Repo3", 8))
            .await
            .unwrap();
        assert_eq!(
            q.map(|m| m.monitor().monitor_id.clone()),
            Some(mid("m-lgw"))
        );
        assert!(
            store
                .find_active_qualified_pr_monitor_in_workspace(&ws_a, &gh("Org/Repo", 1))
                .await
                .unwrap()
                .is_none(),
            "unresolved rows are never qualified matches"
        );
        assert!(invalid(
            &store
                .find_active_qualified_pr_monitor(&aid("agent-a"), &gh("Org/Repo2", 0))
                .await
        ));
        assert!(invalid(
            &store
                .find_active_qualified_pr_monitor_in_workspace(&ws_a, &gl("Grp/Proj", 0))
                .await
        ));

        decoy_rows(&store, tenant, db).await;
    }
);

fn poll<'a>(
    last_error: Option<&'a str>,
    updated_at: &'a str,
    expected: &'a str,
    pending: &'a [String],
) -> PrMonitorPollUpdate<'a> {
    PrMonitorPollUpdate {
        last_snapshot: Some("{\"poll\":1}"),
        baseline_snapshot: Some("{\"poll\":0}"),
        pending_changes: pending,
        pending_since: Some(T3),
        last_change_at: None,
        last_polled_at: Some(updated_at),
        last_error,
        updated_at,
        expected_updated_at: expected,
    }
}

async fn last_error(store: &Store, id: &str) -> Option<String> {
    store.get_pr_monitor(&mid(id)).await.unwrap().last_error
}

parity_test!(
    pr_monitor_lifecycle_poll_and_pause_writes,
    |store, tenant, db| {
        seed(&store, tenant).await;
        let boom = format!("{tenant} boom");

        assert!(store
            .update_pr_monitor_state(&mid("m-a"), PrMonitorState::Cancelled, T3)
            .await
            .unwrap());
        assert!(!store
            .update_pr_monitor_state(&mid("m-a"), PrMonitorState::Completed, T3)
            .await
            .unwrap());
        assert!(!store
            .update_pr_monitor_state(&mid("m-missing"), PrMonitorState::Cancelled, T3)
            .await
            .unwrap());
        let a = store.get_pr_monitor(&mid("m-a")).await.unwrap();
        assert_eq!(
            (a.state, a.updated_at.as_str()),
            (PrMonitorState::Cancelled, T3)
        );

        assert!(!store
            .complete_pr_monitor(&mid("m-Z"), T3, T0)
            .await
            .unwrap());
        assert!(!store
            .complete_pr_monitor(&mid("m-done1"), T3, T2)
            .await
            .unwrap());
        assert!(!store
            .complete_pr_monitor(&mid("m-missing"), T3, T1)
            .await
            .unwrap());
        assert!(store
            .complete_pr_monitor(&mid("m-Z"), T3, T1)
            .await
            .unwrap());
        let z = store.get_pr_monitor(&mid("m-Z")).await.unwrap();
        assert_eq!(
            (
                z.state,
                z.pending_changes.len(),
                z.pending_since,
                z.last_change_at,
                z.last_polled_at
            ),
            (
                PrMonitorState::Completed,
                0,
                None,
                None,
                Some(T0.to_owned())
            )
        );

        let pending = vec![format!("{tenant} new change")];
        assert!(!store
            .update_pr_monitor_poll(&mid("m-b0"), poll(None, T3, T1, &pending))
            .await
            .unwrap());
        assert!(!store
            .update_pr_monitor_poll(&mid("m-missing"), poll(None, T3, T1, &pending))
            .await
            .unwrap());
        assert!(store
            .update_pr_monitor_poll(&mid("m-b0"), poll(Some("fetch failed"), T3, T0, &pending))
            .await
            .unwrap());
        let b0 = store.get_pr_monitor(&mid("m-b0")).await.unwrap();
        assert_eq!(
            (
                b0.last_snapshot.as_deref(),
                b0.baseline_snapshot.as_deref(),
                b0.pending_changes.clone(),
                b0.pending_since.as_deref(),
                b0.last_change_at.as_deref(),
                b0.last_polled_at.as_deref(),
                b0.last_error.as_deref(),
                b0.updated_at.as_str(),
            ),
            (
                Some("{\"poll\":1}"),
                Some("{\"poll\":0}"),
                pending.clone(),
                Some(T3),
                None,
                Some(T3),
                Some("fetch failed"),
                T3,
            )
        );
        assert!(store
            .update_pr_monitor_poll(&mid("m-b0"), poll(None, T2, T3, &[]))
            .await
            .unwrap());
        let b0 = store.get_pr_monitor(&mid("m-b0")).await.unwrap();
        assert!(b0.pending_changes.is_empty() && b0.last_error.is_none());

        // The pause touches only active legacy-writer GitHub rows (`m-lgw`).
        let p2030 = pr_monitor_pause_error(Some("2030-01-01T00:00:00Z"));
        let p2029 = pr_monitor_pause_error(Some("2029-01-01T00:00:00Z"));
        let p2031 = pr_monitor_pause_error(Some("2031-01-01T00:00:00Z"));
        assert_eq!(
            store
                .annotate_active_pr_monitors_pause(&p2030)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            last_error(&store, "m-lgw").await,
            Some(format!("{boom}; {p2030}"))
        );
        assert_eq!(last_error(&store, "m-q").await, Some(boom.clone()));
        assert_eq!(
            store
                .annotate_active_pr_monitors_pause(&p2029)
                .await
                .unwrap(),
            0,
            "an earlier deadline never rolls a later one back"
        );
        assert_eq!(
            store
                .annotate_active_pr_monitors_pause(&p2031)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            last_error(&store, "m-lgw").await,
            Some(format!("{boom}; {p2031}"))
        );

        // A write-back keeps the row's unexpired pause and drops the caller's.
        let caller = format!("new error; {p2029}");
        assert!(store
            .update_pr_monitor_poll(&mid("m-lgw"), poll(Some(&caller), T3, T2, &[]))
            .await
            .unwrap());
        assert_eq!(
            last_error(&store, "m-lgw").await,
            Some(format!("new error; {p2031}"))
        );
        assert!(store
            .update_pr_monitor_poll(
                &mid("m-lgw"),
                poll(None, "2026-01-05T00:00:00.000Z", T3, &[])
            )
            .await
            .unwrap());
        assert_eq!(last_error(&store, "m-lgw").await, Some(p2031.clone()));

        assert_eq!(
            store
                .clear_active_pr_monitors_pause(Some(&p2030))
                .await
                .unwrap(),
            0,
            "a newer pause survives a delayed clear"
        );
        assert_eq!(store.clear_active_pr_monitors_pause(None).await.unwrap(), 1);
        assert_eq!(last_error(&store, "m-lgw").await, None);
        assert_eq!(store.clear_active_pr_monitors_pause(None).await.unwrap(), 0);

        // Whole-second comparison: a deadline in the write's second still stands,
        // one before it is dropped.
        let due = pr_monitor_pause_error(Some("2026-01-06T00:00:00Z"));
        assert_eq!(
            store.annotate_active_pr_monitors_pause(&due).await.unwrap(),
            1
        );
        assert!(store
            .update_pr_monitor_poll(
                &mid("m-lgw"),
                poll(
                    Some("e1"),
                    "2026-01-06T00:00:00.900Z",
                    "2026-01-05T00:00:00.000Z",
                    &[]
                )
            )
            .await
            .unwrap());
        assert_eq!(
            last_error(&store, "m-lgw").await,
            Some(format!("e1; {due}"))
        );
        assert!(store
            .update_pr_monitor_poll(
                &mid("m-lgw"),
                poll(
                    Some("e2"),
                    "2026-01-06T00:00:01.000Z",
                    "2026-01-06T00:00:00.900Z",
                    &[]
                )
            )
            .await
            .unwrap());
        assert_eq!(last_error(&store, "m-lgw").await, Some("e2".to_owned()));
        assert_eq!(
            store
                .annotate_active_pr_monitors_pause(&p2030)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .clear_active_pr_monitors_pause(Some(&p2031))
                .await
                .unwrap(),
            1
        );
        assert_eq!(last_error(&store, "m-lgw").await, Some("e2".to_owned()));

        // Adoption: guarded on the owner and the read `updated_at`.
        assert!(!store
            .adopt_pr_monitor(
                &mid("m-q"),
                &aid("agent-b"),
                &aid("agent-b"),
                poll(None, T3, T2, &[])
            )
            .await
            .unwrap());
        assert!(!store
            .adopt_pr_monitor(
                &mid("m-q"),
                &aid("agent-a"),
                &aid("agent-b"),
                poll(None, T3, T1, &[])
            )
            .await
            .unwrap());
        assert!(!store
            .adopt_pr_monitor(
                &mid("m-missing"),
                &aid("agent-a"),
                &aid("agent-b"),
                poll(None, T3, T1, &[])
            )
            .await
            .unwrap());
        assert!(store
            .adopt_pr_monitor(
                &mid("m-q"),
                &aid("agent-a"),
                &aid("agent-b"),
                poll(Some("adopted"), T3, T2, &pending)
            )
            .await
            .unwrap());
        let q = store.get_pr_monitor(&mid("m-q")).await.unwrap();
        assert_eq!(
            (q.agent_id, q.pending_changes, q.last_error, q.updated_at),
            (
                aid("agent-b"),
                pending.clone(),
                Some("adopted".to_owned()),
                T3.to_owned()
            )
        );

        decoy_rows(&store, tenant, db).await;
    }
);

parity_test!(
    pr_monitor_inserts_qualification_and_rearm,
    |store, tenant, db| {
        seed(&store, tenant).await;

        let dup = PrMonitor {
            monitor_id: mid("m-dup"),
            ..monitor(spec("m-Z"), tenant)
        };
        assert!(
            !store.insert_pr_monitor(&dup).await.unwrap(),
            "identity index"
        );
        assert!(
            !store
                .insert_pr_monitor(&monitor(spec("m-old"), tenant))
                .await
                .unwrap(),
            "primary key"
        );
        let orphan = PrMonitor {
            agent_id: aid("agent-none"),
            ..monitor(&NEW, tenant)
        };
        assert!(internal(&store.insert_pr_monitor(&orphan).await));
        let (q_target, q_provenance) = qualified_target("m-q");
        let dup = PrMonitor {
            monitor_id: mid("m-dup"),
            ..monitor(spec("m-q"), tenant)
        };
        assert!(!store
            .insert_qualified_pr_monitor(&dup, &q_target, q_provenance)
            .await
            .unwrap());
        assert!(invalid(
            &store
                .insert_qualified_pr_monitor(&dup, &gh("Org/Repo2", 0), q_provenance)
                .await
        ));
        assert!(invalid(
            &store
                .insert_qualified_pr_monitor(
                    &monitor(&NEW, tenant),
                    &gl("Grp/Other", 11),
                    MonitorTargetProvenance::LegacyGithubWriter
                )
                .await
        ));
        assert!(internal(
            &store
                .insert_qualified_pr_monitor(&orphan, &gh("Grp/Other", 11), q_provenance)
                .await
        ));
        assert!(store.get_pr_monitor(&mid("m-dup")).await.is_err());

        // Qualification: applied once, idempotent, then a conflict for a stale capture.
        let captured = store.get_qualified_pr_monitor(&mid("m-a")).await.unwrap();
        let writer = MonitorTargetProvenance::LegacyGithubWriter;
        assert!(invalid(
            &store
                .qualify_legacy_pr_monitor_target(&captured, &gh("org/repo", 3), writer)
                .await
        ));
        assert_eq!(
            store
                .qualify_legacy_pr_monitor_target(&captured, &gh("org/repo", 2), writer)
                .await
                .unwrap(),
            MonitorQualificationOutcome::Applied
        );
        assert_eq!(
            store
                .qualify_legacy_pr_monitor_target(&captured, &gh("org/repo", 2), writer)
                .await
                .unwrap(),
            MonitorQualificationOutcome::AlreadyQualified
        );
        assert_eq!(
            store
                .qualify_legacy_pr_monitor_target(
                    &captured,
                    &gh("org/repo", 2),
                    MonitorTargetProvenance::CapturedRequest
                )
                .await
                .unwrap(),
            MonitorQualificationOutcome::Conflict
        );
        let a = store.get_qualified_pr_monitor(&mid("m-a")).await.unwrap();
        assert_eq!(
            a.monitor(),
            captured.monitor(),
            "only target columns change"
        );
        assert_eq!(
            a.target(),
            &PersistedMonitorTarget::Resolved {
                target: gh("org/repo", 2),
                provenance: writer,
            }
        );
        let u9 = store.get_qualified_pr_monitor(&mid("m-u9")).await.unwrap();
        assert_eq!(
            store
                .qualify_legacy_pr_monitor_target(
                    &u9,
                    &gl("Grp/Proj", 9),
                    MonitorTargetProvenance::CapturedRequest
                )
                .await
                .unwrap(),
            MonitorQualificationOutcome::Conflict,
            "m-c holds this qualified identity"
        );
        assert!(store
            .update_pr_monitor_state(&mid("m-b0"), PrMonitorState::Cancelled, T3)
            .await
            .unwrap());
        let b0 = store.get_qualified_pr_monitor(&mid("m-b0")).await.unwrap();
        assert_eq!(
            store
                .qualify_legacy_pr_monitor_target(
                    &b0,
                    &gh("Org/Repo", 3),
                    MonitorTargetProvenance::CapturedRequest
                )
                .await
                .unwrap(),
            MonitorQualificationOutcome::Applied,
            "qualification does not require an active row"
        );

        // Cancellation compares the whole captured row.
        let c = store.get_qualified_pr_monitor(&mid("m-c")).await.unwrap();
        assert!(store.cancel_qualified_pr_monitor(&c, T3).await.unwrap());
        assert!(!store.cancel_qualified_pr_monitor(&c, T3).await.unwrap());
        let c = store.get_pr_monitor(&mid("m-c")).await.unwrap();
        assert_eq!(
            (c.state, c.updated_at.as_str()),
            (PrMonitorState::Cancelled, T3)
        );
        let stale = store.get_qualified_pr_monitor(&mid("m-q")).await.unwrap();
        assert!(store
            .update_pr_monitor_poll(&mid("m-q"), poll(None, T3, T2, &[]))
            .await
            .unwrap());
        assert!(!store.cancel_qualified_pr_monitor(&stale, T3).await.unwrap());

        // Re-arm: a legacy row gains writer provenance with a fresh baseline.
        let z = monitor(spec("m-Z"), tenant);
        assert!(invalid(
            &store
                .rearm_legacy_pr_monitor(&z, &aid("agent-b"), &gl("Org/Repo", 1), None, T3, None)
                .await
        ));
        let q = store.get_pr_monitor(&mid("m-q")).await.unwrap();
        assert!(
            invalid(
                &store
                    .rearm_legacy_pr_monitor(
                        &q,
                        &aid("agent-a"),
                        &gh("Org/Repo2", 7),
                        None,
                        T3,
                        None
                    )
                    .await
            ),
            "captured-request rows are not legacy"
        );
        let ghost = PrMonitor {
            monitor_id: mid("m-missing"),
            ..z.clone()
        };
        assert!(not_found(
            &store
                .rearm_legacy_pr_monitor(
                    &ghost,
                    &aid("agent-b"),
                    &gh("Org/Repo", 1),
                    None,
                    T3,
                    None
                )
                .await
        ));
        let pause = pr_monitor_pause_error(Some("2030-01-01T00:00:00Z"));
        assert!(store
            .rearm_legacy_pr_monitor(
                &z,
                &aid("agent-b"),
                &gh("Org/Repo", 1),
                Some("{\"fresh\":1}"),
                T3,
                Some(&pause)
            )
            .await
            .unwrap());
        let rearmed = store.get_qualified_pr_monitor(&mid("m-Z")).await.unwrap();
        let row = rearmed.monitor();
        assert_eq!(
            (
                row.agent_id.clone(),
                row.last_snapshot.as_deref(),
                row.baseline_snapshot.as_deref(),
                row.pending_changes.len(),
                row.pending_since.clone(),
                row.last_polled_at.as_deref(),
                row.last_error.as_deref(),
                row.updated_at.as_str(),
            ),
            (
                aid("agent-b"),
                Some("{\"fresh\":1}"),
                Some("{\"fresh\":1}"),
                0,
                None,
                Some(T3),
                Some(pause.as_str()),
                T3,
            )
        );
        assert_eq!(
            rearmed.target(),
            &PersistedMonitorTarget::Resolved {
                target: gh("Org/Repo", 1),
                provenance: writer,
            }
        );
        assert!(
            !store
                .rearm_legacy_pr_monitor(&z, &aid("agent-b"), &gh("Org/Repo", 1), None, T3, None)
                .await
                .unwrap(),
            "stale expectation"
        );
        // A writer row re-arms again and keeps its unexpired pause.
        let current = store.get_pr_monitor(&mid("m-Z")).await.unwrap();
        assert!(store
            .rearm_legacy_pr_monitor(
                &current,
                &aid("agent-a"),
                &gh("Org/Repo", 1),
                None,
                T3,
                None
            )
            .await
            .unwrap());
        let row = store.get_pr_monitor(&mid("m-Z")).await.unwrap();
        assert_eq!(
            (
                row.agent_id,
                row.last_snapshot,
                row.last_polled_at,
                row.last_error
            ),
            (aid("agent-a"), None, None, Some(pause.clone()))
        );

        decoy_rows(&store, tenant, db).await;
    }
);

async fn commit(write: PreparedMonitorWrite) -> bool {
    tokio::task::spawn_blocking(move || write.commit())
        .await
        .unwrap()
        .unwrap()
}

parity_test!(
    pr_monitor_prepared_writes_commit_once,
    |store, tenant, db| {
        seed(&store, tenant).await;
        let new = monitor(&NEW, tenant);
        let target = gh("Grp/Other", 11);

        assert!(invalid(
            &store
                .prepare_monitor_insert(&new, &gh("Grp/Other", 12))
                .await
        ));
        let dropped = store.prepare_monitor_insert(&new, &target).await.unwrap();
        drop(dropped);
        assert!(not_found(&store.get_pr_monitor(&mid("m-new")).await));
        let write = store.prepare_monitor_insert(&new, &target).await.unwrap();
        assert!(commit(write).await);
        let write = store.prepare_monitor_insert(&new, &target).await.unwrap();
        assert!(!commit(write).await, "a unique conflict commits nothing");
        let captured = store.get_qualified_pr_monitor(&mid("m-new")).await.unwrap();
        assert_eq!(captured.monitor(), &new);
        assert_eq!(
            captured.target(),
            &PersistedMonitorTarget::Resolved {
                target: target.clone(),
                provenance: MonitorTargetProvenance::CapturedRequest,
            }
        );

        let moved = PrMonitor {
            repo_name: "Elsewhere".into(),
            ..new.clone()
        };
        assert!(invalid(
            &store.prepare_monitor_replace(&captured, &moved).await
        ));
        let replacement = PrMonitor {
            last_error: Some(format!("{tenant} replaced")),
            pending_changes: Vec::new(),
            updated_at: "2026-01-05T00:00:00.000Z".into(),
            ..new.clone()
        };
        let write = store
            .prepare_monitor_replace(&captured, &replacement)
            .await
            .unwrap();
        assert!(commit(write).await);
        assert_eq!(
            store.get_pr_monitor(&mid("m-new")).await.unwrap(),
            replacement
        );
        let write = store
            .prepare_monitor_replace(&captured, &replacement)
            .await
            .unwrap();
        assert!(!commit(write).await, "stale capture");
        let write = store.prepare_monitor_cancel(&captured, T3).await.unwrap();
        assert!(!commit(write).await, "stale capture");

        let current = store.get_qualified_pr_monitor(&mid("m-new")).await.unwrap();
        let dropped = store.prepare_monitor_cancel(&current, T3).await.unwrap();
        drop(dropped);
        assert_eq!(
            store.get_pr_monitor(&mid("m-new")).await.unwrap().state,
            PrMonitorState::Active
        );
        let write = store.prepare_monitor_cancel(&current, T3).await.unwrap();
        assert!(commit(write).await);
        let row = store.get_pr_monitor(&mid("m-new")).await.unwrap();
        assert_eq!(
            (row.state, row.updated_at.as_str()),
            (PrMonitorState::Cancelled, T3)
        );

        decoy_rows(&store, tenant, db).await;
    }
);

fn projection_hook(
    id: &str,
    agent: &str,
    created_at: &str,
    state: HookState,
    tenant: &str,
) -> Hook {
    let timed = id != "h-b";
    Hook {
        hook_id: HookId(id.into()),
        workspace_id: wid(if agent == "agent-c" { "ws-b" } else { "ws-a" }),
        agent_id: aid(agent),
        name: format!("{tenant} {id}"),
        code: format!("return '{tenant}'"),
        delay_ms: 1000,
        cron: None,
        run_at: None,
        state,
        created_at: created_at.into(),
        last_run_at: None,
        next_run_at: timed.then(|| T3.into()),
        run_count: 0,
        last_error: None,
        last_logs: Some(format!("{tenant} logs")),
        last_state: None,
        expires_at: timed.then(|| T3.into()),
        perpetual: false,
        dispatch_count: 0,
    }
}

/// `(id, workspace, name, message_count, last role, assistant preview,
/// hooks, monitors)`, sorted by id.
type ProjectionView = (
    String,
    String,
    String,
    u64,
    Option<String>,
    Option<Vec<String>>,
    Vec<Value>,
    Vec<Value>,
);

fn projection_view(rows: Vec<SubscriptionAgentProjection>) -> Vec<ProjectionView> {
    let mut out: Vec<ProjectionView> = rows
        .into_iter()
        .map(|p| {
            (
                p.session.id.0.clone(),
                p.session.workspace_id.0.clone(),
                p.session.name.clone(),
                p.messages.message_count,
                p.messages.last_message_role,
                p.messages.last_assistant_text_blocks,
                p.waiting_on_hooks,
                p.waiting_on_pr_monitors,
            )
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

fn hook_value(id: &str, tenant: &str) -> Value {
    if id == "h-b" {
        json!({"hookId": id, "name": format!("{tenant} {id}")})
    } else {
        json!({"hookId": id, "name": format!("{tenant} {id}"), "nextRunAt": T3, "expiresAt": T3})
    }
}

fn monitor_value(id: &str, repo: &str, number: i64, title: Option<&str>) -> Value {
    let mut value = json!({"monitorId": id, "repo": repo, "prNumber": number});
    if let Some(title) = title {
        value["title"] = json!(title);
    }
    value
}

// Decoy rule: the decoy owns every row the subject reads (sessions,
// messages, hooks, monitors via `decoy_rows`) plus `agent-missing`,
// which the subject probes as absent, and an extra active hook and
// monitors on `agent-a` that a leaked read would add.
parity_test!(
    subscription_agent_projections_batch_sessions_hooks_and_monitors,
    |store, tenant, db| {
        seed(&store, tenant).await;
        decoy_rows(&store, tenant, db).await;
        store
            .append_agent_message(
                &aid("agent-a"),
                "user",
                &json!([{"type": "text", "text": "hi"}]),
                T1,
            )
            .await
            .unwrap();
        store
            .append_agent_message(
                &aid("agent-a"),
                "assistant",
                &json!([{"type": "text", "text": format!("{tenant} reply")}]),
                T2,
            )
            .await
            .unwrap();
        for (id, agent, created_at, state) in [
            ("h-1", "agent-a", T1, HookState::Scheduled),
            ("h-0", "agent-a", T1, HookState::Running),
            ("h-gone", "agent-a", T0, HookState::Cancelled),
            ("h-b", "agent-b", T0, HookState::Scheduled),
            ("h-c", "agent-c", T2, HookState::Dispatched),
        ] {
            store
                .insert_hook(&projection_hook(id, agent, created_at, state, tenant))
                .await
                .unwrap();
        }
        let done = (
            "m-wdone",
            "agent-a",
            "ws-a",
            "Org",
            "Done",
            12,
            PrMonitorState::Completed,
            T1,
            T1,
        );
        assert!(store
            .insert_qualified_pr_monitor(
                &monitor(&done, tenant),
                &gh("Org/Done", 12),
                MonitorTargetProvenance::LegacyGithubWriter
            )
            .await
            .unwrap());
        store
            .insert_agent_session(&AgentSession {
                id: aid("agent-bad"),
                workspace_id: wid("ws-a"),
                name: format!("agent-bad of {tenant}"),
                created_at: T0.into(),
                updated_at: T0.into(),
                ..AgentSession::default()
            })
            .await
            .unwrap();
        match db {
            Some(db) => sqlx::query(
                "UPDATE agent_session SET status = 'bogus' WHERE tenant_id = $1 AND id = 'agent-bad'",
            )
            .bind(tenant)
            .execute(&db.admin)
            .await
            .map(drop)
            .unwrap(),
            None => sqlx::query("UPDATE agent_session SET status = 'bogus' WHERE id = 'agent-bad'")
                .execute(store.write_pool())
                .await
                .map(drop)
                .unwrap(),
        }
        if tenant == "decoy" {
            store
                .insert_agent_session(&AgentSession {
                    id: aid("agent-missing"),
                    workspace_id: wid("ws-a"),
                    name: "decoy only".into(),
                    created_at: T0.into(),
                    updated_at: T0.into(),
                    ..AgentSession::default()
                })
                .await
                .unwrap();
            for (id, agent) in [("h-ghost", "agent-a"), ("h-missing", "agent-missing")] {
                store
                    .insert_hook(&projection_hook(
                        id,
                        agent,
                        T0,
                        HookState::Scheduled,
                        tenant,
                    ))
                    .await
                    .unwrap();
                let spec = (
                    id,
                    agent,
                    "ws-a",
                    "Ghost",
                    id,
                    60,
                    PrMonitorState::Active,
                    T0,
                    T0,
                );
                assert!(store
                    .insert_qualified_pr_monitor(
                        &monitor(&spec, tenant),
                        &gh(&format!("Ghost/{id}"), 60),
                        MonitorTargetProvenance::LegacyGithubWriter
                    )
                    .await
                    .unwrap());
            }
        }

        assert!(store
            .get_subscription_agent_projections(&[])
            .await
            .unwrap()
            .is_empty());
        let rows = store
            .get_subscription_agent_projections(&[
                aid("agent-z"),
                aid("agent-missing"),
                aid("agent-a"),
                aid("agent-bad"),
                aid("agent-b"),
                aid("agent-c"),
            ])
            .await
            .unwrap();
        let title = format!("{tenant} one");
        // Legacy-inserted rows stay unresolved, so only active writer-proven
        // monitors (`m-lgw`, the decoy's ghosts; not `m-wdone`) are listed.
        let ghost = |id: &str| monitor_value(id, &format!("Ghost/{id}"), 60, Some(&title));
        let mut agent_a_monitors = Vec::new();
        if tenant == "decoy" {
            agent_a_monitors.push(ghost("h-ghost"));
        }
        agent_a_monitors.push(monitor_value("m-lgw", "Org/Repo3", 8, Some(&title)));
        let mut agent_a_hooks = Vec::new();
        if tenant == "decoy" {
            agent_a_hooks.push(hook_value("h-ghost", tenant));
        }
        agent_a_hooks.extend([hook_value("h-1", tenant), hook_value("h-0", tenant)]);
        let mut expected = vec![
            (
                "agent-a".to_owned(),
                "ws-a".to_owned(),
                format!("agent-a of {tenant}"),
                2,
                Some("assistant".to_owned()),
                Some(vec![format!("{tenant} reply")]),
                agent_a_hooks,
                agent_a_monitors,
            ),
            (
                "agent-b".to_owned(),
                "ws-a".to_owned(),
                format!("agent-b of {tenant}"),
                0,
                None,
                None,
                vec![hook_value("h-b", tenant)],
                Vec::new(),
            ),
            (
                "agent-c".to_owned(),
                "ws-b".to_owned(),
                format!("agent-c of {tenant}"),
                0,
                None,
                None,
                Vec::new(),
                Vec::new(),
            ),
        ];
        if tenant == "decoy" {
            expected.push((
                "agent-missing".to_owned(),
                "ws-a".to_owned(),
                "decoy only".to_owned(),
                0,
                None,
                None,
                vec![hook_value("h-missing", tenant)],
                vec![ghost("h-missing")],
            ));
        }
        expected.push((
            "agent-z".to_owned(),
            "ws-z".to_owned(),
            format!("agent-z of {tenant}"),
            0,
            None,
            None,
            Vec::new(),
            Vec::new(),
        ));
        assert_eq!(projection_view(rows), expected);
    }
);

parity_test!(
    migration_status_matches_the_embedded_migrations,
    |store, _tenant, db| {
        let status = store.migration_status().await.unwrap();
        assert!(!status.expected.is_empty());
        assert_eq!(status.applied, status.expected);
        assert!(status.is_current());
        if let Some(db) = db {
            // A part-way migration is not applied yet.
            let last = *status.expected.last().unwrap();
            for complete in [false, true] {
                sqlx::query("UPDATE intent_schema_migrations SET complete = $1 WHERE version = $2")
                    .bind(complete)
                    .bind(last)
                    .execute(&db.admin)
                    .await
                    .unwrap();
                let status = store.migration_status().await.unwrap();
                assert_eq!(status.applied.contains(&last), complete);
                assert_eq!(status.is_current(), complete);
            }
        }
    }
);

// Several chunks: real sessions sit in the first, second and third chunk
// of absent ids. Decoy rule: the decoy also owns one absent id per chunk.
parity_test!(
    subscription_agent_projections_span_several_chunks,
    |store, tenant| {
        let chunk = 500;
        let mut ids: Vec<AgentId> = (0..chunk * 2 + 10)
            .map(|i| aid(&format!("absent-{i:04}")))
            .collect();
        let placed = [
            (3, "agent-a"),
            (chunk + 7, "agent-b"),
            (chunk * 2 + 1, "agent-c"),
        ];
        for (at, id) in placed {
            ids[at] = aid(id);
        }
        for ws in ["ws-a", "ws-b"] {
            store
                .insert_workspace(&Workspace {
                    id: wid(ws),
                    title: format!("{tenant} {ws}"),
                    branch: "main".into(),
                    created_at: T0.into(),
                    updated_at: T0.into(),
                    ..Default::default()
                })
                .await
                .unwrap();
        }
        let mut sessions: Vec<String> = placed.iter().map(|(_, id)| (*id).to_owned()).collect();
        if tenant == "decoy" {
            sessions.extend(["absent-0004", "absent-0508", "absent-1002"].map(str::to_owned));
        }
        for id in &sessions {
            store
                .insert_agent_session(&AgentSession {
                    id: aid(id),
                    workspace_id: wid("ws-a"),
                    name: format!("{id} of {tenant}"),
                    created_at: T0.into(),
                    updated_at: T0.into(),
                    ..AgentSession::default()
                })
                .await
                .unwrap();
            store
                .insert_hook(&projection_hook(
                    &format!("h-{id}"),
                    id,
                    T1,
                    HookState::Scheduled,
                    tenant,
                ))
                .await
                .unwrap();
        }
        let mut got: Vec<(String, Vec<Value>)> = store
            .get_subscription_agent_projections(&ids)
            .await
            .unwrap()
            .into_iter()
            .map(|p| (p.session.id.0.clone(), p.waiting_on_hooks))
            .collect();
        got.sort_by(|a, b| a.0.cmp(&b.0));
        sessions.sort();
        let expected: Vec<(String, Vec<Value>)> = sessions
            .iter()
            .map(|id| (id.clone(), vec![hook_value(&format!("h-{id}"), tenant)]))
            .collect();
        assert_eq!(got, expected);
    }
);
