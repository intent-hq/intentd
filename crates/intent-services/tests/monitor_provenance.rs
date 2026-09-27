//! Reads actual old SQL columns, not modern qualified records labelled old.

#[path = "../src/monitor_provenance.rs"]
mod monitor_provenance;

use intent_core::{AgentId, PrMonitor, PrMonitorId, PrMonitorState, RepoRef, WorkspaceId};
use monitor_provenance::{classify_legacy_monitor, LegacyProviderSemantics, UnresolvedMonitor};
use serde_json::{json, Value};
use sqlx::{Connection, Row, SqliteConnection};

async fn legacy_database() -> SqliteConnection {
    let mut db = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(
        "PRAGMA foreign_keys = ON; CREATE TABLE workspace (id TEXT PRIMARY KEY); \
         CREATE TABLE agent_session (id TEXT PRIMARY KEY);",
    )
    .execute(&mut db)
    .await
    .unwrap();
    for sql in [
        include_str!("../../intent-store/migrations/0085_pr_monitor.sql"),
        include_str!("../../intent-store/migrations/0089_pr_monitor_baseline.sql"),
        include_str!("fixtures/pre_b_pr_monitors.sql"),
        include_str!("../../intent-store/migrations/0118_pr_monitor_workspace_identity.sql"),
        include_str!("../../intent-store/migrations/0119_pr_monitor_identity_nocase.sql"),
    ] {
        sqlx::raw_sql(sql).execute(&mut db).await.unwrap();
    }
    let columns = sqlx::query("PRAGMA table_info(pr_monitor)")
        .fetch_all(&mut db)
        .await
        .unwrap();
    let names: Vec<String> = columns.iter().map(|r| r.get("name")).collect();
    assert_eq!(names.len(), 16, "genuine pre-B table columns: {names:?}");
    for modern_column in ["provider", "instance_base_url", "connection_id", "target"] {
        assert!(!names.iter().any(|name| name == modern_column));
    }
    db
}

async fn read_old_row(db: &mut SqliteConnection, id: &str) -> PrMonitor {
    let row = sqlx::query("SELECT * FROM pr_monitor WHERE monitor_id = ?")
        .bind(id)
        .fetch_one(db)
        .await
        .unwrap();
    PrMonitor {
        monitor_id: PrMonitorId(row.get("monitor_id")),
        workspace_id: WorkspaceId(row.get("workspace_id")),
        agent_id: AgentId(row.get("agent_id")),
        repo_owner: row.get("repo_owner"),
        repo_name: row.get("repo_name"),
        pr_number: row.get("pr_number"),
        state: match row.get::<&str, _>("state") {
            "active" => PrMonitorState::Active,
            "completed" => PrMonitorState::Completed,
            "cancelled" => PrMonitorState::Cancelled,
            state => panic!("invalid legacy lifecycle: {state}"),
        },
        last_snapshot: row.get("last_snapshot"),
        baseline_snapshot: row.get("baseline_snapshot"),
        pending_changes: serde_json::from_str(row.get("pending_changes")).unwrap(),
        pending_since: row.get("pending_since"),
        last_change_at: row.get("last_change_at"),
        last_polled_at: row.get("last_polled_at"),
        last_error: row.get("last_error"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn github_target(repo: &RepoRef, number: u64) -> Value {
    json!({
        "repository": {
            "provider": "github",
            "instanceBaseUrl": "https://github.com",
            "projectPath": repo.identity_key(),
        },
        "kind": "pull-request",
        "number": number,
    })
}

#[tokio::test]
async fn genuine_pending_github_row_keeps_its_original_target_after_remote_migration() {
    let mut db = legacy_database().await;
    let row = read_old_row(&mut db, "prmon-legacy-pending").await;
    // Current Git configuration is explicitly different. It is not a
    // classifier argument and cannot become the old monitor's provenance.
    let current = json!({
        "repository": {"provider":"gitlab", "instanceBaseUrl":"https://gitlab.com", "projectPath":"intent-hq/example"},
        "kind":"merge-request", "number":42,
    });
    let qualified = classify_legacy_monitor(
        &row,
        LegacyProviderSemantics::BuiltinGithubDotCom,
        github_target,
    );
    let target = qualified.target.unwrap();
    assert_ne!(target, current);
    assert_eq!(target["repository"]["provider"], "github");
    assert_eq!(
        target["repository"]["instanceBaseUrl"],
        "https://github.com"
    );
    assert_eq!(target["repository"]["projectPath"], "intent-hq/example");
    assert_eq!(target["kind"], "pull-request");
    assert_eq!(target["number"], 42);
    assert_eq!(qualified.row.agent_id.0, "legacy-agent");
    assert_eq!(qualified.row.state, PrMonitorState::Active);
    assert_eq!(
        qualified.row.pending_changes,
        ["Conversation comments: 1 → 2"]
    );
    assert!(qualified.row.pending_since.is_some());
    assert_ne!(qualified.row.last_snapshot, qualified.row.baseline_snapshot);
    let reread = read_old_row(&mut db, "prmon-legacy-pending").await;
    assert_eq!(
        &reread, qualified.row,
        "classification preserves every old column"
    );
}

#[tokio::test]
async fn missing_provenance_keeps_the_original_row_for_inspection_and_cancellation() {
    let mut db = legacy_database().await;
    let row = read_old_row(&mut db, "prmon-legacy-unresolved").await;
    let unresolved = classify_legacy_monitor(
        &row,
        LegacyProviderSemantics::BuiltinGithubDotCom,
        |_, _| -> Value { panic!("must not contact or invent a target") },
    );
    assert_eq!(
        unresolved.target,
        Err(UnresolvedMonitor::ProjectProvenanceMissing)
    );
    assert_eq!(unresolved.row.agent_id.0, "legacy-agent");
    assert_eq!(unresolved.row.monitor_id.0, "prmon-legacy-unresolved");
    assert_eq!(unresolved.row.pending_changes, ["Unsent change"]);
    // Original identifiers still support cancellation; this is SQL-fixture
    // evidence only, not a claim of integrated alias/ownership enforcement.
    sqlx::query("UPDATE pr_monitor SET state = 'cancelled' WHERE monitor_id = ? AND agent_id = ?")
        .bind(&unresolved.row.monitor_id.0)
        .bind(&unresolved.row.agent_id.0)
        .execute(&mut db)
        .await
        .unwrap();
    assert_eq!(
        read_old_row(&mut db, "prmon-legacy-unresolved").await.state,
        PrMonitorState::Cancelled
    );
}

#[tokio::test]
async fn malformed_or_unproven_legacy_identity_is_never_filled_from_a_current_target() {
    let mut db = legacy_database().await;
    let mut row = read_old_row(&mut db, "prmon-legacy-pending").await;
    assert_eq!(
        classify_legacy_monitor(&row, LegacyProviderSemantics::Unproven, github_target).target,
        Err(UnresolvedMonitor::ProviderProvenanceMissing),
    );
    for invalid in [
        "",
        "..",
        "group/subgroup",
        "https://github.com/o",
        "owner@host",
        "o ",
    ] {
        row.repo_owner = invalid.into();
        assert_eq!(
            classify_legacy_monitor(
                &row,
                LegacyProviderSemantics::BuiltinGithubDotCom,
                github_target
            )
            .target,
            Err(UnresolvedMonitor::ProjectProvenanceMissing),
        );
    }
    row.repo_owner = "Intent-HQ".into();
    for invalid in [0, -1] {
        row.pr_number = invalid;
        assert_eq!(
            classify_legacy_monitor(
                &row,
                LegacyProviderSemantics::BuiltinGithubDotCom,
                github_target
            )
            .target,
            Err(UnresolvedMonitor::InvalidNumber),
        );
    }
}
