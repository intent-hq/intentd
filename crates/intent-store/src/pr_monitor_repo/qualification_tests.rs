use super::*;

use MonitorQualificationOutcome::{AlreadyQualified, Applied, Conflict};
use MonitorTargetProvenance::{CapturedRequest, LegacyGithubWriter};
use RepositoryProvider::{Github, Gitlab};
use RepositoryResourceKind::{Issue, MergeRequest, PullRequest};

async fn store() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("monitors.db")).await.unwrap();
    sqlx::raw_sql(
        "INSERT INTO workspace(id,title,branch,created_at,updated_at)
         VALUES ('w','Monitor storage','main','created','updated');
         INSERT INTO agent_session(id,workspace_id,name,status,created_at,updated_at)
         VALUES ('a','w','Owner','idle','created','updated'),
                ('b','w','Other owner','idle','created','updated');",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    (dir, store)
}

fn monitor(id: &str) -> PrMonitor {
    PrMonitor {
        monitor_id: PrMonitorId(id.into()),
        workspace_id: WorkspaceId("w".into()),
        agent_id: AgentId("a".into()),
        repo_owner: "o".into(),
        repo_name: "r".into(),
        pr_number: 42,
        state: PrMonitorState::Active,
        last_snapshot: Some("{\"v\":2}".into()),
        baseline_snapshot: Some("{\"v\":1}".into()),
        pending_changes: vec!["Pending original change".into()],
        pending_since: Some("pending".into()),
        last_change_at: Some("changed".into()),
        last_polled_at: Some("polled".into()),
        last_error: Some("Original error".into()),
        created_at: "created".into(),
        updated_at: "updated".into(),
    }
}

#[tokio::test]
async fn legacy_lookup_cannot_select_qualified_gitlab_or_issue() {
    for (provider, instance, kind) in [
        ("gitlab", "https://gitlab.com", "merge-request"),
        ("github", "https://github.com", "issue"),
        ("github", "https://enterprise.example", "pull-request"),
    ] {
        let (_dir, store) = store().await;
        assert!(store
            .insert_pr_monitor(&monitor("qualified"))
            .await
            .unwrap());
        sqlx::query(
            "UPDATE pr_monitor SET target_provider=?, target_instance_base_url=?,
             target_project_path='o/r', target_kind=?, target_provenance='captured-request',
             target_unresolved_reason=NULL WHERE monitor_id='qualified'",
        )
        .bind(provider)
        .bind(instance)
        .bind(kind)
        .execute(store.write_pool())
        .await
        .unwrap();
        assert!(
            store
                .find_active_pr_monitor(&AgentId("a".into()), "o", "r", 42)
                .await
                .unwrap()
                .is_none(),
            "unqualified lookup selected {provider}/{kind}"
        );
        assert!(store
            .find_active_pr_monitor_in_workspace(&WorkspaceId("w".into()), "o", "r", 42)
            .await
            .unwrap()
            .is_none());
    }
}

fn target(
    provider: RepositoryProvider,
    instance: &str,
    project: &str,
    kind: RepositoryResourceKind,
    number: u64,
) -> ReviewTarget {
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

fn gh() -> ReviewTarget {
    target(Github, "https://github.com", "o/r", PullRequest, 42)
}

fn gl() -> ReviewTarget {
    target(
        Gitlab,
        "https://git.example:8443/forge",
        "o/r",
        MergeRequest,
        42,
    )
}

async fn original_bytes(pool: &sqlx::SqlitePool, id: &str) -> Vec<serde_json::Value> {
    let row = sqlx::query(&format!(
        "SELECT {COLUMNS} FROM pr_monitor WHERE monitor_id=?"
    ))
    .bind(id)
    .fetch_one(pool)
    .await
    .unwrap();
    COLUMNS
        .split(',')
        .map(str::trim)
        .map(|name| {
            if name == "pr_number" {
                serde_json::json!(row.get::<i64, _>(name))
            } else {
                serde_json::json!(row.get::<Option<String>, _>(name))
            }
        })
        .collect()
}

/// Run the actual historical migration prefix, including the original 0085,
/// 0089, 0118 and 0119 files. The monitor table has only its old sixteen columns
/// when the pending fixture is inserted. No modern row is labelled "old".
async fn pre_target_database(path: &std::path::Path) -> sqlx::SqlitePool {
    let pool = crate::connect_write(path).await.unwrap();
    let prefix = sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|m| m.version < 137)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    prefix.run(&pool).await.unwrap();
    let columns = sqlx::query("PRAGMA table_info(pr_monitor)")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(columns.len(), 16);
    assert!(!columns
        .iter()
        .any(|r| r.get::<String, _>("name").starts_with("target_")));
    sqlx::raw_sql(
        "INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES
         ('workspace-before-migration','Old watch','main','created','updated');
         INSERT INTO agent_session(id,workspace_id,name,status,created_at,updated_at) VALUES
         ('legacy-agent','workspace-before-migration','Original owner','idle','created','updated');",
    ).execute(&pool).await.unwrap();
    sqlx::raw_sql(PRE_B_MONITORS).execute(&pool).await.unwrap();
    pool
}

#[tokio::test]
async fn genuine_legacy_pending_row_survives_upgrade_qualification_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.db");
    let pool = pre_target_database(&path).await;
    // The workspace moved to GitLab before the first target migration. This
    // config is deliberately absent from every Store qualification argument.
    let repo = dir.path().join("current-repo");
    std::fs::create_dir_all(repo.join(".git")).unwrap();
    std::fs::write(
        repo.join(".git/config"),
        "[remote \"origin\"]\nurl = https://gitlab.com/Intent-HQ/Example.git\n",
    )
    .unwrap();
    sqlx::query("UPDATE workspace SET repository_path=?, repository_owner='Intent-HQ', repository_name='Example' WHERE id='workspace-before-migration'")
        .bind(repo.to_str().unwrap()).execute(&pool).await.unwrap();
    let before = original_bytes(&pool, "prmon-legacy-pending").await;
    let incomplete = original_bytes(&pool, "prmon-legacy-unresolved").await;
    pool.close().await;
    let store = Store::open(&path).await.unwrap();
    assert_eq!(
        before,
        original_bytes(store.read_pool(), "prmon-legacy-pending").await
    );
    assert_eq!(
        incomplete,
        original_bytes(store.read_pool(), "prmon-legacy-unresolved").await
    );
    let id = PrMonitorId("prmon-legacy-pending".into());
    let old = store.get_qualified_pr_monitor(&id).await.unwrap();
    assert!(matches!(
        old.target(),
        PersistedMonitorTarget::Unresolved {
            reason: MonitorTargetUnresolvedReason::MissingProvenance
        }
    ));
    // Fixture-supplied, established original-writer evidence. Production
    // imported rows do not acquire this evidence from their schema or URL.
    let original = target(
        Github,
        "https://github.com",
        "intent-hq/example",
        PullRequest,
        42,
    );
    assert_eq!(
        store
            .qualify_legacy_pr_monitor_target(&old, &original, LegacyGithubWriter)
            .await
            .unwrap(),
        Applied
    );
    assert_eq!(
        store
            .qualify_legacy_pr_monitor_target(&old, &original, LegacyGithubWriter)
            .await
            .unwrap(),
        AlreadyQualified
    );
    assert_eq!(before, original_bytes(store.read_pool(), &id.0).await);
    let mut other = old.monitor().clone();
    other.monitor_id = PrMonitorId("new-gl-same-name-number".into());
    let current = target(
        Gitlab,
        "https://gitlab.com",
        "Intent-HQ/Example",
        MergeRequest,
        42,
    );
    assert!(store
        .insert_qualified_pr_monitor(&other, &current, CapturedRequest)
        .await
        .unwrap());
    assert_eq!(
        store
            .find_active_pr_monitor(&old.monitor().agent_id, "INTENT-HQ", "EXAMPLE", 42)
            .await
            .unwrap()
            .unwrap()
            .monitor_id,
        id
    );
    assert_eq!(
        store
            .load_active_qualified_pr_monitors()
            .await
            .unwrap()
            .len(),
        3
    );
    store.read_pool().close().await;
    store.write_pool().close().await;
    let reopened = Store::open(&path).await.unwrap();
    assert_eq!(before, original_bytes(reopened.read_pool(), &id.0).await);
    assert!(
        matches!(reopened.get_qualified_pr_monitor(&id).await.unwrap().target(), PersistedMonitorTarget::Resolved { target, provenance: LegacyGithubWriter } if target == &original)
    );
    assert!(matches!(
        reopened
            .get_qualified_pr_monitor(&PrMonitorId("prmon-legacy-unresolved".into()))
            .await
            .unwrap()
            .target(),
        PersistedMonitorTarget::Unresolved { .. }
    ));
}

#[tokio::test]
async fn qualified_identity_keeps_provider_instance_path_kind_and_ownership() {
    let (_dir, store) = store().await;
    let identities = [
        gh(),
        gl(),
        target(Gitlab, "https://git.example:8443/forge", "o/r", Issue, 42),
        target(
            Gitlab,
            "https://git.example:8443/other",
            "o/r",
            MergeRequest,
            42,
        ),
        target(
            Gitlab,
            "https://git.example:9443/forge",
            "o/r",
            MergeRequest,
            42,
        ),
        target(
            Gitlab,
            "https://git.example:8443/forge",
            "group/sub/r",
            MergeRequest,
            42,
        ),
        target(
            Gitlab,
            "https://git.example:8443/forge",
            "O/R",
            MergeRequest,
            42,
        ),
        target(Github, "https://github.com", "o/r", Issue, 42),
    ];
    for (i, target) in identities.iter().enumerate() {
        let m = monitor(&format!("m{i}"));
        assert!(store
            .insert_qualified_pr_monitor(&m, target, CapturedRequest)
            .await
            .unwrap());
        let mut duplicate = monitor(&format!("duplicate{i}"));
        duplicate.agent_id = AgentId("b".into());
        assert!(!store
            .insert_qualified_pr_monitor(&duplicate, target, CapturedRequest)
            .await
            .unwrap());
        assert_eq!(
            store
                .find_active_qualified_pr_monitor_in_workspace(&m.workspace_id, target)
                .await
                .unwrap()
                .unwrap()
                .monitor()
                .agent_id,
            m.agent_id
        );
        assert!(store
            .find_active_qualified_pr_monitor(&duplicate.agent_id, target)
            .await
            .unwrap()
            .is_none());
    }
    let upper = target(Github, "https://github.com", "O/R", PullRequest, 42);
    assert_eq!(
        store
            .find_active_qualified_pr_monitor(&AgentId("a".into()), &upper)
            .await
            .unwrap()
            .unwrap()
            .monitor()
            .monitor_id
            .0,
        "m0"
    );
    assert!(!store
        .insert_qualified_pr_monitor(&monitor("case-duplicate"), &upper, CapturedRequest)
        .await
        .unwrap());
    assert!(!store
        .insert_pr_monitor(&monitor("legacy-duplicate"))
        .await
        .unwrap());
    assert_eq!(
        store
            .load_active_qualified_pr_monitors()
            .await
            .unwrap()
            .len(),
        identities.len()
    );
}

#[tokio::test]
async fn unresolved_is_not_a_qualified_match_and_remains_cancellable() {
    let (_dir, store) = store().await;
    let mut row = monitor("unknown");
    row.repo_owner.clear();
    row.pr_number = -1;
    assert!(store.insert_pr_monitor(&row).await.unwrap());
    let old = store
        .get_qualified_pr_monitor(&row.monitor_id)
        .await
        .unwrap();
    assert_eq!(old.monitor(), &row);
    assert!(matches!(
        old.target(),
        PersistedMonitorTarget::Unresolved { .. }
    ));
    assert!(store
        .find_active_qualified_pr_monitor(&row.agent_id, &gl())
        .await
        .unwrap()
        .is_none());
    assert!(store
        .qualify_legacy_pr_monitor_target(&old, &gh(), LegacyGithubWriter)
        .await
        .is_err());
    assert!(store
        .update_pr_monitor_state(&row.monitor_id, PrMonitorState::Cancelled, "cancelled")
        .await
        .unwrap());
    assert_eq!(
        store
            .get_pr_monitor(&row.monitor_id)
            .await
            .unwrap()
            .pending_changes,
        row.pending_changes
    );
}

#[tokio::test]
async fn target_numbers_are_checked_before_storage() {
    let (_dir, store) = store().await;
    for number in [0, i64::MAX as u64 + 1, u64::MAX] {
        let mut bad = gl();
        bad.number = number;
        assert!(store
            .insert_qualified_pr_monitor(&monitor("bad"), &bad, CapturedRequest)
            .await
            .is_err());
        assert!(store
            .find_active_qualified_pr_monitor(&AgentId("a".into()), &bad)
            .await
            .is_err());
    }
    let mut maximum = gl();
    maximum.number = i64::MAX as u64;
    let mut row = monitor("max");
    row.pr_number = i64::MAX;
    assert!(store
        .insert_qualified_pr_monitor(&row, &maximum, CapturedRequest)
        .await
        .unwrap());
    assert_eq!(
        store
            .get_pr_monitor(&row.monitor_id)
            .await
            .unwrap()
            .pr_number,
        i64::MAX
    );
    let mut wrong_kind = gl();
    wrong_kind.kind = PullRequest;
    assert!(store
        .insert_qualified_pr_monitor(&monitor("wrong-kind"), &wrong_kind, CapturedRequest)
        .await
        .is_err());
    assert!(store
        .insert_qualified_pr_monitor(&monitor("wrong-evidence"), &gl(), LegacyGithubWriter)
        .await
        .is_err());
    let mut mismatched = monitor("mismatched");
    mismatched.repo_name = "elsewhere".into();
    assert!(store
        .insert_qualified_pr_monitor(&mismatched, &gh(), CapturedRequest)
        .await
        .is_err());
}

#[tokio::test]
async fn target_only_cas_detects_owner_state_and_raw_pending_changes_without_timestamp_help() {
    for (column, value) in [
        ("agent_id", "b"),
        ("state", "cancelled"),
        ("last_error", "new error"),
        ("last_snapshot", "new snapshot"),
        ("baseline_snapshot", "new baseline"),
        ("pending_changes", " [ \"Pending original change\" ] "),
        ("pending_since", "new pending time"),
        ("last_change_at", "new change time"),
        ("last_polled_at", "new poll time"),
        ("repo_owner", "other"),
        ("repo_name", "other"),
        ("created_at", "new creation"),
        ("updated_at", "new update"),
    ] {
        let (_dir, store) = store().await;
        let row = monitor("guarded");
        assert!(store.insert_pr_monitor(&row).await.unwrap());
        let captured = store
            .get_qualified_pr_monitor(&row.monitor_id)
            .await
            .unwrap();
        sqlx::query(&format!(
            "UPDATE pr_monitor SET {column}=? WHERE monitor_id='guarded'"
        ))
        .bind(value)
        .execute(store.write_pool())
        .await
        .unwrap();
        let before = original_bytes(store.read_pool(), "guarded").await;
        assert_eq!(
            store
                .qualify_legacy_pr_monitor_target(&captured, &gl(), CapturedRequest)
                .await
                .unwrap(),
            Conflict,
            "{column}"
        );
        assert_eq!(before, original_bytes(store.read_pool(), "guarded").await);
        assert!(matches!(
            store
                .get_qualified_pr_monitor(&row.monitor_id)
                .await
                .unwrap()
                .target(),
            PersistedMonitorTarget::Unresolved { .. }
        ));
    }
}

#[tokio::test]
async fn qualification_preserves_noncanonical_pending_json_bytes() {
    for raw in ["  [\"unsent\"]  ", "malformed legacy JSON"] {
        let (_dir, store) = store().await;
        let row = monitor("bytes");
        assert!(store.insert_pr_monitor(&row).await.unwrap());
        sqlx::query("UPDATE pr_monitor SET pending_changes=? WHERE monitor_id='bytes'")
            .bind(raw)
            .execute(store.write_pool())
            .await
            .unwrap();
        let before = original_bytes(store.read_pool(), "bytes").await;
        let captured = store
            .get_qualified_pr_monitor(&row.monitor_id)
            .await
            .unwrap();
        assert_eq!(
            store
                .qualify_legacy_pr_monitor_target(&captured, &gh(), LegacyGithubWriter)
                .await
                .unwrap(),
            Applied
        );
        assert_eq!(before, original_bytes(store.read_pool(), "bytes").await);
    }
}

#[tokio::test]
async fn competing_qualification_is_atomic_and_resolved_targets_cannot_be_replaced() {
    let (_dir, store) = store().await;
    let row = monitor("race");
    assert!(store.insert_pr_monitor(&row).await.unwrap());
    let expected = store
        .get_qualified_pr_monitor(&row.monitor_id)
        .await
        .unwrap();
    let before = original_bytes(store.read_pool(), "race").await;
    let github = gh();
    let gitlab = gl();
    let (a, b) = tokio::join!(
        store.qualify_legacy_pr_monitor_target(&expected, &github, LegacyGithubWriter),
        store.qualify_legacy_pr_monitor_target(&expected, &gitlab, CapturedRequest)
    );
    assert!(matches!(
        (a.unwrap(), b.unwrap()),
        (Applied, Conflict) | (Conflict, Applied)
    ));
    assert_eq!(before, original_bytes(store.read_pool(), "race").await);
    let actual = store
        .get_qualified_pr_monitor(&row.monitor_id)
        .await
        .unwrap();
    let PersistedMonitorTarget::Resolved { target, provenance } = actual.target() else {
        panic!("resolved")
    };
    assert_eq!(
        store
            .qualify_legacy_pr_monitor_target(&expected, target, *provenance)
            .await
            .unwrap(),
        AlreadyQualified
    );
    let replacement = if target == &github { &gitlab } else { &github };
    assert_eq!(
        store
            .qualify_legacy_pr_monitor_target(&actual, replacement, CapturedRequest)
            .await
            .unwrap(),
        Conflict
    );
}

#[tokio::test]
async fn qualification_collision_keeps_both_original_rows_and_owners() {
    let (_dir, store) = store().await;
    let a = monitor("existing");
    assert!(store
        .insert_qualified_pr_monitor(&a, &gl(), CapturedRequest)
        .await
        .unwrap());
    let mut b = monitor("legacy");
    b.agent_id = AgentId("b".into());
    b.repo_name = "unproven-old-name".into();
    assert!(store.insert_pr_monitor(&b).await.unwrap());
    let expected = store.get_qualified_pr_monitor(&b.monitor_id).await.unwrap();
    let before = original_bytes(store.read_pool(), "legacy").await;
    assert_eq!(
        store
            .qualify_legacy_pr_monitor_target(&expected, &gl(), CapturedRequest)
            .await
            .unwrap(),
        Conflict
    );
    assert_eq!(before, original_bytes(store.read_pool(), "legacy").await);
    assert_eq!(store.get_pr_monitor(&a.monitor_id).await.unwrap(), a);
    assert!(matches!(
        store
            .get_qualified_pr_monitor(&b.monitor_id)
            .await
            .unwrap()
            .target(),
        PersistedMonitorTarget::Unresolved { .. }
    ));
}

#[tokio::test]
async fn qualified_monitor_retains_existing_poll_adoption_and_terminal_guards() {
    let (_dir, store) = store().await;
    let row = monitor("lifecycle");
    let target = gl();
    assert!(store
        .insert_qualified_pr_monitor(&row, &target, CapturedRequest)
        .await
        .unwrap());
    let captured = store
        .get_qualified_pr_monitor(&row.monitor_id)
        .await
        .unwrap();
    let update = PrMonitorPollUpdate {
        last_snapshot: Some("new owner's snapshot"),
        baseline_snapshot: row.baseline_snapshot.as_deref(),
        pending_changes: &row.pending_changes,
        pending_since: row.pending_since.as_deref(),
        last_change_at: row.last_change_at.as_deref(),
        last_polled_at: row.last_polled_at.as_deref(),
        last_error: Some("retained error"),
        updated_at: "2026-09-27T16:00:01Z",
        expected_updated_at: &row.updated_at,
    };
    assert!(store
        .adopt_pr_monitor(&row.monitor_id, &row.agent_id, &AgentId("b".into()), update)
        .await
        .unwrap());
    assert!(!store
        .update_pr_monitor_poll(&row.monitor_id, update)
        .await
        .unwrap());
    assert!(!store
        .complete_pr_monitor(&row.monitor_id, "done", &row.updated_at)
        .await
        .unwrap());
    assert_eq!(
        store
            .qualify_legacy_pr_monitor_target(&captured, &target, CapturedRequest)
            .await
            .unwrap(),
        Conflict
    );
    let adopted = store
        .get_qualified_pr_monitor(&row.monitor_id)
        .await
        .unwrap();
    assert_eq!(adopted.target(), captured.target());
    assert_eq!(adopted.monitor().agent_id.0, "b");
    assert_eq!(adopted.monitor().pending_changes, row.pending_changes);
    assert!(store
        .update_pr_monitor_state(&row.monitor_id, PrMonitorState::Cancelled, "cancelled")
        .await
        .unwrap());
    assert!(!store
        .update_pr_monitor_poll(
            &row.monitor_id,
            PrMonitorPollUpdate {
                expected_updated_at: update.updated_at,
                ..update
            }
        )
        .await
        .unwrap());
    assert!(store
        .insert_qualified_pr_monitor(&monitor("fresh-owner"), &target, CapturedRequest)
        .await
        .unwrap());
    assert_eq!(
        store
            .get_qualified_pr_monitor(&row.monitor_id)
            .await
            .unwrap()
            .target(),
        captured.target()
    );
}

#[tokio::test]
async fn same_target_simultaneous_qualification_is_idempotent_without_resetting_pending() {
    let (_dir, store) = store().await;
    let row = monitor("same-target");
    assert!(store.insert_pr_monitor(&row).await.unwrap());
    let old = store
        .get_qualified_pr_monitor(&row.monitor_id)
        .await
        .unwrap();
    let before = original_bytes(store.read_pool(), "same-target").await;
    let target = gh();
    let (a, b) = tokio::join!(
        store.qualify_legacy_pr_monitor_target(&old, &target, LegacyGithubWriter),
        store.qualify_legacy_pr_monitor_target(&old, &target, LegacyGithubWriter)
    );
    assert!(matches!(
        (a.unwrap(), b.unwrap()),
        (Applied, AlreadyQualified) | (AlreadyQualified, Applied)
    ));
    assert_eq!(
        before,
        original_bytes(store.read_pool(), "same-target").await
    );
}

// Exact old monitor INSERTs from accepted0280 pre_b_pr_monitors.sql.
// Parent FK rows are seeded above with the same original identifiers.
const PRE_B_MONITORS: &str = r#"INSERT INTO pr_monitor (
    monitor_id, workspace_id, agent_id, repo_owner, repo_name, pr_number,
    state, last_snapshot, baseline_snapshot, pending_changes, pending_since,
    last_change_at, last_polled_at, last_error, created_at, updated_at
) VALUES (
    'prmon-legacy-pending', 'workspace-before-migration', 'legacy-agent',
    'Intent-HQ', 'Example', 42, 'active',
    '{"title":"Original GitHub review","url":"https://github.com/Intent-HQ/Example/pull/42","conversationCount":2,"reviewCommentCount":0,"requirements":{"state":"open","isDraft":false,"hasConflicts":false,"isBehind":false,"checks":{"total":0,"passed":0,"failed":0,"pending":0,"items":[],"failingRequired":[],"pendingRequired":[],"requiredKnown":false},"approvals":{"decision":"none","have":0,"changesRequested":0},"threads":{},"rulesKnown":false}}',
    '{"title":"Original GitHub review","url":"https://github.com/Intent-HQ/Example/pull/42","conversationCount":1,"reviewCommentCount":0,"requirements":{"state":"open","isDraft":false,"hasConflicts":false,"isBehind":false,"checks":{"total":0,"passed":0,"failed":0,"pending":0,"items":[],"failingRequired":[],"pendingRequired":[],"requiredKnown":false},"approvals":{"decision":"none","have":0,"changesRequested":0},"threads":{},"rulesKnown":false}}',
    '["Conversation comments: 1 → 2"]',
    '2026-09-20T12:00:01Z', '2026-09-20T12:00:02Z',
    '2026-09-20T12:00:02Z', 'temporary network failure',
    '2026-09-20T12:00:00Z', '2026-09-20T12:00:02Z'
);

-- An old incomplete row must stay inspectable/cancellable by its owner.
-- A current GitLab origin does not fill in its missing captured project.
INSERT INTO pr_monitor (
    monitor_id, workspace_id, agent_id, repo_owner, repo_name, pr_number,
    state, pending_changes, pending_since, created_at, updated_at
) VALUES (
    'prmon-legacy-unresolved', 'workspace-before-migration', 'legacy-agent',
    '', 'Example', 42, 'active', '["Unsent change"]',
    '2026-09-20T12:00:01Z', '2026-09-20T12:00:00Z', '2026-09-20T12:00:02Z'
);
"#;
