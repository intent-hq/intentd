//! Quotas: a write that admits one more row under a cap counts and writes
//! in one transaction (`quota.rs`). Both backends: the invite nonce rows
//! (issue under the per-invite and tenant caps, claim once, release, spend)
//! and the capped PR-monitor writes. Postgres: the quota row lock is what
//! orders two processes' admissions, and a nonce one handle issued is
//! claimed by another.

use std::time::Duration;

use intent_core::{
    AgentId, AgentSession, InviteScope, PrMonitor, PrMonitorId, PrMonitorState, RepositoryProvider,
    RepositoryResourceKind, RepositoryTarget, ReviewTarget, Workspace, WorkspaceId,
};
use intent_store::{
    InviteNonce, InviteNonceCaps, MonitorTargetProvenance, PreparedMonitorWrite, QuotaWrite, Store,
};
use intentd_test_support::test_timeout;
use sqlx::postgres::PgPool;

use crate::harness::{open_as_process, parity_test, trapped_db};

const T0: &str = "2026-01-01T00:00:00.000Z";
const NOW: i64 = 1_800_000_000_000;

fn nonce(invite: &str) -> InviteNonce {
    InviteNonce {
        invite_id: invite.into(),
        scope: InviteScope::Workspace,
        authorization_generation: 7,
        issued_at_ms: NOW,
        expires_at_ms: NOW + 600_000,
    }
}

const CAPS: InviteNonceCaps = InviteNonceCaps {
    per_invite: 2,
    total: 3,
};

parity_test!(invite_nonces_issue_under_caps_and_claim_once, |store| {
    let issue = |hash: &'static str, invite: &'static str, now: i64| {
        let store = store.clone();
        async move {
            store
                .issue_invite_nonce(hash, &nonce(invite), now, CAPS)
                .await
                .unwrap()
        }
    };
    assert_eq!(issue("h1", "inv-a", NOW).await, QuotaWrite::Written);
    assert_eq!(
        issue("h1", "inv-a", NOW).await,
        QuotaWrite::Lost,
        "same hash"
    );
    assert_eq!(issue("h2", "inv-a", NOW).await, QuotaWrite::Written);
    assert_eq!(
        issue("h3", "inv-a", NOW).await,
        QuotaWrite::Full { active: 2 },
        "the invite's cap"
    );
    assert_eq!(issue("h4", "inv-b", NOW).await, QuotaWrite::Written);
    assert_eq!(
        issue("h5", "inv-c", NOW).await,
        QuotaWrite::Full { active: 3 },
        "the tenant's cap"
    );
    assert_eq!(store.count_invite_nonces().await.unwrap(), 3);

    // A claim keeps the row (it still counts); a second claim misses.
    assert_eq!(
        store.claim_invite_nonce("h1", NOW).await.unwrap(),
        Some(nonce("inv-a"))
    );
    assert_eq!(store.claim_invite_nonce("h1", NOW).await.unwrap(), None);
    assert_eq!(store.claim_invite_nonce("nope", NOW).await.unwrap(), None);
    assert_eq!(
        issue("h5", "inv-c", NOW).await,
        QuotaWrite::Full { active: 3 },
        "a claimed nonce still counts against the tenant"
    );
    // ...but not against its invite (a prove holds it, as before rows).
    store.delete_invite_nonce("h4").await.unwrap();
    assert_eq!(
        issue("h7", "inv-a", NOW).await,
        QuotaWrite::Written,
        "inv-a has one unclaimed nonce left of two"
    );
    // Released, it can be claimed again; spent, it is gone.
    store.release_invite_nonce("h1").await.unwrap();
    assert!(store.claim_invite_nonce("h1", NOW).await.unwrap().is_some());
    store.delete_invite_nonce("h1").await.unwrap();
    assert_eq!(store.claim_invite_nonce("h1", NOW).await.unwrap(), None);
    store.release_invite_nonce("h1").await.unwrap();
    assert_eq!(store.count_invite_nonces().await.unwrap(), 2);

    // Expired rows (claimed or not) are purged by the next issue.
    assert!(store.claim_invite_nonce("h2", NOW).await.unwrap().is_some());
    let later = NOW + 600_000;
    assert_eq!(issue("h6", "inv-a", later).await, QuotaWrite::Written);
    assert_eq!(store.count_invite_nonces().await.unwrap(), 1);
    store.delete_invite_nonce("h6").await.unwrap();
    assert_eq!(store.count_invite_nonces().await.unwrap(), 0);
});

fn wid() -> WorkspaceId {
    WorkspaceId("ws-quota".into())
}

fn aid(id: &str) -> AgentId {
    AgentId(id.into())
}

fn gh(number: u64) -> ReviewTarget {
    ReviewTarget {
        repository: RepositoryTarget {
            provider: RepositoryProvider::Github,
            instance_base_url: "https://github.com".into(),
            project_path: "o/r".into(),
        },
        kind: RepositoryResourceKind::PullRequest,
        number,
    }
}

fn monitor(id: &str, agent: &str, number: i64) -> PrMonitor {
    PrMonitor {
        monitor_id: PrMonitorId(id.into()),
        workspace_id: wid(),
        agent_id: aid(agent),
        repo_owner: "o".into(),
        repo_name: "r".into(),
        pr_number: number,
        state: PrMonitorState::Active,
        last_snapshot: None,
        baseline_snapshot: None,
        pending_changes: Vec::new(),
        pending_since: None,
        last_change_at: None,
        last_polled_at: None,
        last_error: None,
        created_at: T0.into(),
        updated_at: T0.into(),
    }
}

async fn seed(store: &Store) {
    store
        .insert_workspace(&Workspace {
            id: wid(),
            title: "quota".into(),
            branch: "main".into(),
            created_at: T0.into(),
            updated_at: T0.into(),
            ..Default::default()
        })
        .await
        .unwrap();
    for agent in ["agent-a", "agent-b"] {
        store
            .insert_agent_session(&AgentSession {
                id: aid(agent),
                workspace_id: wid(),
                name: agent.into(),
                created_at: T0.into(),
                updated_at: T0.into(),
                ..AgentSession::default()
            })
            .await
            .unwrap();
    }
}

async fn commit(write: PreparedMonitorWrite) -> bool {
    tokio::task::spawn_blocking(move || write.commit())
        .await
        .unwrap()
        .unwrap()
}

const LEGACY: MonitorTargetProvenance = MonitorTargetProvenance::LegacyGithubWriter;

parity_test!(pr_monitor_admissions_count_in_the_write, |store| {
    seed(&store).await;
    let insert = |id: &'static str, agent: &'static str, number: u64, max: u64| {
        let store = store.clone();
        async move {
            store
                .insert_qualified_pr_monitor_within_quota(
                    &monitor(id, agent, number.cast_signed()),
                    &gh(number),
                    LEGACY,
                    max,
                )
                .await
                .unwrap()
        }
    };
    assert_eq!(insert("m-1", "agent-a", 1, 2).await, QuotaWrite::Written);
    assert_eq!(insert("m-2", "agent-a", 2, 2).await, QuotaWrite::Written);
    assert_eq!(
        insert("m-3", "agent-a", 3, 2).await,
        QuotaWrite::Full { active: 2 }
    );
    assert_eq!(
        insert("m-1b", "agent-b", 1, 5).await,
        QuotaWrite::Lost,
        "the workspace identity index refuses a second monitor of PR 1"
    );
    assert_eq!(insert("m-4", "agent-b", 4, 1).await, QuotaWrite::Written);

    // A re-arm by the owner adds nothing: never refused, even at max 1.
    let m2 = store
        .get_pr_monitor(&PrMonitorId("m-2".into()))
        .await
        .unwrap();
    assert_eq!(
        store
            .rearm_legacy_pr_monitor_within_quota(&m2, &aid("agent-a"), &gh(2), None, T0, None, 1)
            .await
            .unwrap(),
        QuotaWrite::Written
    );
    // An adoption counts against the adopter.
    let m2 = store
        .get_pr_monitor(&PrMonitorId("m-2".into()))
        .await
        .unwrap();
    assert_eq!(
        store
            .rearm_legacy_pr_monitor_within_quota(&m2, &aid("agent-b"), &gh(2), None, T0, None, 1)
            .await
            .unwrap(),
        QuotaWrite::Full { active: 1 }
    );
    assert_eq!(
        store
            .rearm_legacy_pr_monitor_within_quota(&m2, &aid("agent-b"), &gh(2), None, T0, None, 2)
            .await
            .unwrap(),
        QuotaWrite::Written
    );
    assert_eq!(
        store
            .rearm_legacy_pr_monitor_within_quota(&m2, &aid("agent-b"), &gh(2), None, T0, None, 9)
            .await
            .unwrap(),
        QuotaWrite::Lost,
        "a stale image loses its compare-and-swap"
    );

    // A prepared write: counted in its reserved transaction.
    let mut write = store
        .prepare_monitor_insert(&monitor("m-5", "agent-b", 5), &gl(5))
        .await
        .unwrap();
    assert_eq!(
        write.admit_under_quota(&aid("agent-b"), 2).await.unwrap(),
        Some(2)
    );
    drop(write);
    let mut write = store
        .prepare_monitor_insert(&monitor("m-5", "agent-b", 5), &gl(5))
        .await
        .unwrap();
    assert_eq!(
        write.admit_under_quota(&aid("agent-b"), 3).await.unwrap(),
        None
    );
    assert!(commit(write).await);
    assert_eq!(
        store
            .list_active_pr_monitors_by_agent(&aid("agent-b"))
            .await
            .unwrap()
            .len(),
        3
    );
});

fn gl(number: u64) -> ReviewTarget {
    ReviewTarget {
        repository: RepositoryTarget {
            provider: RepositoryProvider::Gitlab,
            instance_base_url: "https://gitlab.example".into(),
            project_path: "o/r".into(),
        },
        kind: RepositoryResourceKind::MergeRequest,
        number,
    }
}

/// Sessions of `admin`'s database waiting for a lock in a statement on
/// `quota_lock`.
async fn quota_lock_waiters(admin: &PgPool) -> usize {
    let n: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity \
         WHERE wait_event_type = 'Lock' AND datname = current_database() \
         AND query LIKE '%FROM quota_lock%'",
    )
    .fetch_one(admin)
    .await
    .expect("list quota lock waiters");
    n.try_into().unwrap()
}

/// Poll until a session waits on a quota row; Postgres emits no event when
/// a lock wait starts.
async fn until_waiting_on_quota(admin: &PgPool) {
    tokio::time::timeout(test_timeout(Duration::from_secs(15)), async {
        while quota_lock_waiters(admin).await == 0 {
            // timing-guard: poll interval; Postgres emits no event for lock waits.
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("an admission waits on the quota row");
}

/// Another process holds the tenant's nonce quota row while it inserts the
/// last nonce under the cap (not committed yet). An issue from this process
/// waits on that row, then counts the committed nonce and is refused: it
/// never counts below the cap while the other admission is in flight.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admission_waits_for_the_quota_row_and_counts_after_it() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "quota-row";
    let a = open_as_process(&db, tenant, "process-a").await;
    // Seed the quota row (and the tenant's writer row).
    assert_eq!(
        a.issue_invite_nonce("seed", &nonce("inv-seed"), NOW, CAPS)
            .await
            .unwrap(),
        QuotaWrite::Written
    );
    a.delete_invite_nonce("seed").await.unwrap();

    let mut other = db.admin.begin().await.unwrap();
    sqlx::query(
        "SELECT 1 FROM quota_lock WHERE tenant_id = $1 AND scope = 'invite-nonce' \
         AND lock_key = '' FOR UPDATE",
    )
    .bind(tenant)
    .fetch_one(&mut *other)
    .await
    .unwrap();
    for (hash, invite) in [("o1", "inv-x"), ("o2", "inv-y"), ("o3", "inv-z")] {
        sqlx::query(
            "INSERT INTO invite_nonce (tenant_id, nonce_hash, invite_id, scope, \
             authorization_generation, issued_at_ms, expires_at_ms) \
             VALUES ($1, $2, $3, 'workspace', 0, $4, $5)",
        )
        .bind(tenant)
        .bind(hash)
        .bind(invite)
        .bind(NOW)
        .bind(NOW + 600_000)
        .execute(&mut *other)
        .await
        .unwrap();
    }
    let issue = tokio::spawn({
        let a = a.clone();
        async move {
            a.issue_invite_nonce("mine", &nonce("inv-a"), NOW, CAPS)
                .await
        }
    });
    until_waiting_on_quota(&db.admin).await;
    assert!(!issue.is_finished(), "the issue waits on the quota row");
    other.commit().await.unwrap();
    assert_eq!(
        issue.await.unwrap().unwrap(),
        QuotaWrite::Full { active: 3 },
        "it counts the other process's committed nonces"
    );
    assert_eq!(a.count_invite_nonces().await.unwrap(), 3);
}

/// A nonce one process issued is claimed by another (a prove may land on
/// any replica), once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nonce_issued_by_one_process_is_claimed_by_another_once() {
    let Some(db) = trapped_db().await else {
        return;
    };
    let tenant = "quota-claim";
    let a = open_as_process(&db, tenant, "process-a").await;
    let b = open_as_process(&db, tenant, "process-b").await;
    assert_eq!(
        a.issue_invite_nonce("h", &nonce("inv"), NOW, CAPS)
            .await
            .unwrap(),
        QuotaWrite::Written
    );
    let (left, right) = tokio::join!(
        a.claim_invite_nonce("h", NOW),
        b.claim_invite_nonce("h", NOW)
    );
    let claims = [left.unwrap(), right.unwrap()];
    assert_eq!(
        claims.iter().filter(|c| c.is_some()).count(),
        1,
        "exactly one claim wins: {claims:?}"
    );
    assert_eq!(b.count_invite_nonces().await.unwrap(), 1, "it still counts");
}
