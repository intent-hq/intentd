//! Real HTTP regressions for authorized, bounded branch policy reuse.
#[path = "support/qwen.rs"]
mod qwen;

use std::sync::Arc;
use std::time::Duration;

use intent_sourcecontrol::branch_rules_cache::{with_freshness, MAX_AGE};
use intent_sourcecontrol::traffic::{with_traffic, Caller, Operation, Traffic};
use intent_sourcecontrol::{BranchRules, Error, GitHubSourceControl, RepoRef, SourceControl};
use qwen::MockQwen;
use serde_json::json;

// Authorization generation is process-wide, including under cargo test.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn shared(
    sc: &GitHubSourceControl,
    repo: &RepoRef,
    base: &str,
) -> Result<BranchRules, Error> {
    with_freshness(MAX_AGE, sc.branch_rules(repo, base)).await
}

async fn advance(by: Duration) {
    tokio::time::pause();
    tokio::time::advance(by).await;
    tokio::time::resume();
}

fn policy(mock: &MockQwen, count: u32) {
    mock.edit(|s| s.rules = json!([
        {"type":"pull_request","parameters":{"required_approving_review_count":count,"required_review_thread_resolution":true}},
        {"type":"pull_request","parameters":{"required_approving_review_count":1,"required_review_thread_resolution":false}},
        {"type":"required_status_checks","parameters":{"required_status_checks":[{"context":"CI Gate"}]}},
        {"type":"future_rule"}
    ]));
}

#[tokio::test]
async fn shared_rules_ttl_shorter_freshness_and_changed_policy() {
    let _serial = SERIAL.lock().await;
    let mock = MockQwen::start(10978).await;
    let repo = RepoRef::new("o", "r");
    policy(&mock, 2);
    let first = shared(&mock.sc, &repo, "main").await.unwrap();
    assert_eq!(first.required_approving_review_count, Some(2));
    assert_eq!(first.required_conversation_resolution, Some(true));
    assert_eq!(first.required_status_checks, ["CI Gate"]);
    policy(&mock, 3);
    advance(Duration::from_secs(10)).await;
    assert_eq!(shared(&mock.sc, &repo, "main").await.unwrap(), first);
    let changed = with_freshness(Duration::from_secs(5), mock.sc.branch_rules(&repo, "main"))
        .await
        .unwrap();
    assert_eq!(changed.required_approving_review_count, Some(3));
    assert_eq!(mock.calls("/rules/branches/"), 2);
    policy(&mock, 4);
    advance(MAX_AGE).await;
    assert_eq!(
        shared(&mock.sc, &repo, "main")
            .await
            .unwrap()
            .required_approving_review_count,
        Some(4)
    );
    assert_eq!(mock.calls("/rules/branches/"), 3);
    // Direct reads remain fresh even when called outside any PR scope.
    policy(&mock, 5);
    assert_eq!(
        mock.sc
            .branch_rules(&repo, "main")
            .await
            .unwrap()
            .required_approving_review_count,
        Some(5)
    );
    assert_eq!(mock.calls("/rules/branches/"), 4);
}

#[tokio::test]
async fn shared_rules_scope_isolates_host_token_repo_and_exact_base() {
    let _serial = SERIAL.lock().await;
    let a = MockQwen::start(10978).await;
    let b = MockQwen::start(10978).await;
    let repo = RepoRef::new("Owner", "Repo");
    let same = GitHubSourceControl::new("fixture-token", Some(&format!("{}/", a.base))).unwrap();
    let other_token = GitHubSourceControl::new("other-token", Some(&a.base)).unwrap();
    shared(&a.sc, &repo, "main").await.unwrap();
    shared(&same, &RepoRef::new("OWNER", "REPO"), "main")
        .await
        .unwrap();
    assert_eq!(a.calls("/rules/branches/"), 1);
    shared(&other_token, &repo, "main").await.unwrap();
    shared(&b.sc, &repo, "main").await.unwrap();
    shared(&a.sc, &RepoRef::new("other", "repo"), "main")
        .await
        .unwrap();
    shared(&a.sc, &repo, "Main").await.unwrap();
    shared(&a.sc, &repo, "release/v1").await.unwrap();
    assert_eq!(a.calls("/rules/branches/"), 5);
    assert_eq!(b.calls("/rules/branches/"), 1);
    intent_sourcecontrol::cache_scope::invalidate_authorization();
    assert!(shared(&a.sc, &repo, "main").await.is_err());
    let replaced = GitHubSourceControl::new("fixture-token", Some(&a.base)).unwrap();
    shared(&replaced, &repo, "main").await.unwrap();
    assert_eq!(a.calls("/rules/branches/"), 6);
}

#[tokio::test]
async fn shared_rules_concurrent_expiry_counts_joined_reuse() {
    let _serial = SERIAL.lock().await;
    let mock = MockQwen::start(10978).await;
    let repo = RepoRef::new("o", "r");
    shared(&mock.sc, &repo, "main").await.unwrap();
    advance(MAX_AGE).await;
    let gate = mock.gate("/rules/branches/");
    let traffic = Traffic::default();
    with_traffic(traffic.clone(), async {
        let leader = shared(&mock.sc, &repo, "main");
        let follower = async {
            gate.entered.notified().await;
            let read = shared(&mock.sc, &repo, "main");
            tokio::pin!(read);
            tokio::select! {biased; result = &mut read => panic!("unexpected early result {result:?}"), () = std::future::ready(()) => {}}
            gate.release.add_permits(100);
            read.await.unwrap()
        };
        let (a,b) = tokio::join!(leader, follower);
        assert_eq!(a.unwrap(),b);
    }).await;
    assert_eq!(mock.calls("/rules/branches/"), 2);
    let counts = traffic.snapshot();
    let rules = &counts.counts[&(Caller::OnDemand, Operation::Rules)];
    assert_eq!(rules.rest_requests, 1);
    assert_eq!(rules.in_flight_reuses, 1);
}

#[tokio::test]
async fn shared_rules_force_during_fetch_never_reuses_older_work() {
    let _serial = SERIAL.lock().await;
    let mock = MockQwen::start(10978).await;
    let repo = RepoRef::new("o", "r");
    policy(&mock, 2);
    let gate = mock.gate("/rules/branches/");
    let first = shared(&mock.sc, &repo, "main");
    let forced = async {
        gate.entered.notified().await;
        policy(&mock, 3);
        let read = with_freshness(Duration::ZERO, mock.sc.branch_rules(&repo, "main"));
        tokio::pin!(read);
        tokio::select! {biased; result = &mut read => panic!("unexpected early result {result:?}"), () = std::future::ready(()) => {}}
        gate.release.add_permits(100);
        read.await.unwrap()
    };
    let (a, b) = tokio::join!(first, forced);
    assert_eq!(a.unwrap().required_approving_review_count, Some(2));
    assert_eq!(b.required_approving_review_count, Some(3));
    assert_eq!(shared(&mock.sc, &repo, "main").await.unwrap(), b);
    assert_eq!(mock.calls("/rules/branches/"), 2);
}

#[tokio::test]
async fn shared_rules_auth_replacement_during_fetch_cannot_publish() {
    let _serial = SERIAL.lock().await;
    let mock = MockQwen::start(10978).await;
    let repo = RepoRef::new("o", "r");
    policy(&mock, 2);
    let gate = mock.gate("/rules/branches/");
    let old = shared(&mock.sc, &repo, "main");
    let replace = async {
        gate.entered.notified().await;
        intent_sourcecontrol::cache_scope::invalidate_authorization();
        policy(&mock, 3);
        let new = GitHubSourceControl::new("fixture-token", Some(&mock.base)).unwrap();
        gate.release.add_permits(100);
        shared(&new, &repo, "main").await.unwrap()
    };
    let (old, new) = tokio::join!(old, replace);
    assert!(matches!(old, Err(Error::Auth(_))));
    assert_eq!(new.required_approving_review_count, Some(3));
    let new_provider = GitHubSourceControl::new("fixture-token", Some(&mock.base)).unwrap();
    assert_eq!(shared(&new_provider, &repo, "main").await.unwrap(), new);
    assert_eq!(mock.calls("/rules/branches/"), 2);
}

#[tokio::test]
async fn shared_rules_cancelled_leader_allows_waiter_recovery() {
    let _serial = SERIAL.lock().await;
    let mock = MockQwen::start(10978).await;
    let gate = mock.gate("/rules/branches/");
    let repo = RepoRef::new("o", "r");
    let sc = mock.sc.clone();
    let leader = tokio::spawn(async move { shared(&sc, &RepoRef::new("o", "r"), "main").await });
    gate.entered.notified().await;
    let read = shared(&mock.sc, &repo, "main");
    tokio::pin!(read);
    tokio::select! {biased; result = &mut read => panic!("unexpected early result {result:?}"), () = std::future::ready(()) => {}}
    leader.abort();
    assert!(leader.await.unwrap_err().is_cancelled());
    gate.release.add_permits(100);
    read.await.unwrap();
    shared(&mock.sc, &repo, "main").await.unwrap();
    assert_eq!(mock.calls("/rules/branches/"), 2);
}

#[tokio::test]
async fn shared_rules_errors_coalesce_but_recover_without_caching_unknown_as_empty() {
    let _serial = SERIAL.lock().await;
    for (status, body) in [
        (401, json!({"message":"bad credentials"})),
        (500, json!({"message":"temporary failure"})),
        (404, json!({"message":"unavailable"})),
        (403, json!({"message":"API rate limit exceeded"})),
        (429, json!({"message":"API rate limit exceeded"})),
        (200, json!({})),
        (
            200,
            json!([{"type":"pull_request","parameters":{"required_approving_review_count":2}}]),
        ),
    ] {
        let mock = MockQwen::start(10978).await;
        mock.edit(|s| {
            s.rules_status = status;
            s.rules = body;
        });
        let repo = RepoRef::new("o", "r");
        let gate = mock.gate("/rules/branches/");
        let leader = shared(&mock.sc, &repo, "main");
        let follower = async {
            gate.entered.notified().await;
            let read = shared(&mock.sc, &repo, "main");
            tokio::pin!(read);
            tokio::select! {biased; result = &mut read => panic!("unexpected early result {result:?}"), () = std::future::ready(()) => {}}
            gate.release.add_permits(100);
            read.await
        };
        let (a, b) = tokio::join!(leader, follower);
        assert!(a.is_err(), "status={status}");
        assert!(b.is_err());
        if status == 403 || status == 429 {
            assert!(matches!(a, Err(Error::RateLimited(_))));
        }
        assert_eq!(
            mock.calls("/rules/branches/"),
            if status == 429 || status == 500 { 4 } else { 1 }
        );
        mock.edit(|s| {
            s.rules_status = 200;
            s.rules = json!([]);
        });
        assert_eq!(
            shared(&mock.sc, &repo, "main").await.unwrap(),
            BranchRules::default()
        );
        assert_eq!(
            mock.calls("/rules/branches/"),
            if status == 429 || status == 500 { 5 } else { 2 }
        );
    }
}

#[tokio::test]
async fn shared_rules_eviction_is_bounded_and_live_slots_are_not_detached() {
    let _serial = SERIAL.lock().await;
    let mock = MockQwen::start(10978).await;
    let repo = RepoRef::new("o", "r");
    for n in 0..129 {
        shared(&mock.sc, &repo, &format!("base-{n}")).await.unwrap();
    }
    shared(&mock.sc, &repo, "base-0").await.unwrap();
    assert_eq!(
        mock.calls("/rules/branches/"),
        130,
        "oldest idle slot was evicted"
    );
    let gate = mock.gate("/rules/branches/");
    let mut readers = Vec::new();
    for n in 0..128 {
        let sc = Arc::clone(&mock.sc);
        readers.push(tokio::spawn(async move {
            shared(&sc, &RepoRef::new("o", "r"), &format!("busy-{n}")).await
        }));
        gate.entered.notified().await;
    }
    assert!(shared(&mock.sc, &repo, "overflow").await.is_err());
    assert_eq!(
        mock.calls("/rules/branches/"),
        258,
        "saturation must not start extra work"
    );
    // A same-key follower still joins at capacity.
    let follower = shared(&mock.sc, &repo, "busy-0");
    tokio::pin!(follower);
    tokio::select! {biased; result = &mut follower => panic!("unexpected early result {result:?}"), () = std::future::ready(()) => {}}
    gate.release.add_permits(1000);
    follower.await.unwrap();
    for reader in readers {
        reader.await.unwrap().unwrap();
    }
    shared(&mock.sc, &repo, "overflow").await.unwrap();
    assert_eq!(mock.calls("/rules/branches/"), 259);
}

#[tokio::test]
async fn shared_rules_failed_forced_refresh_discards_previous_policy() {
    let _serial = SERIAL.lock().await;
    let mock = MockQwen::start(10978).await;
    let repo = RepoRef::new("o", "r");
    policy(&mock, 2);
    shared(&mock.sc, &repo, "main").await.unwrap();
    mock.edit(|s| {
        s.rules_status = 404;
        s.rules = json!({"message":"policy inaccessible"});
    });
    assert!(
        with_freshness(Duration::ZERO, mock.sc.branch_rules(&repo, "main"))
            .await
            .is_err()
    );
    assert!(
        shared(&mock.sc, &repo, "main").await.is_err(),
        "must not revive the prior successful policy"
    );
    mock.edit(|s| s.rules_status = 200);
    policy(&mock, 3);
    assert_eq!(
        shared(&mock.sc, &repo, "main")
            .await
            .unwrap()
            .required_approving_review_count,
        Some(3)
    );
    assert_eq!(mock.calls("/rules/branches/"), 4);
}

#[tokio::test]
async fn shared_rules_overlapping_forced_demands_share_one_newer_fill() {
    let _serial = SERIAL.lock().await;
    let mock = MockQwen::start(10978).await;
    let repo = RepoRef::new("o", "r");
    policy(&mock, 2);
    let gate = mock.gate("/rules/branches/");
    let traffic = Traffic::default();
    with_traffic(traffic.clone(), async {
        let old = shared(&mock.sc, &repo, "main");
        let forced = async {
            gate.entered.notified().await;
            policy(&mock, 3);
            let a = with_freshness(Duration::ZERO, mock.sc.branch_rules(&repo, "main"));
            let b = with_freshness(Duration::ZERO, mock.sc.branch_rules(&repo, "main"));
            tokio::pin!(a, b);
            // Both freshness demands precede the replacement fetch, while
            // neither may join the already-started old policy observation.
            tokio::select! {biased; r = &mut a => panic!("early result {r:?}"), () = std::future::ready(()) => {}}
            tokio::select! {biased; r = &mut b => panic!("early result {r:?}"), () = std::future::ready(()) => {}}
            gate.release.add_permits(100);
            let (a, b) = tokio::join!(a, b);
            assert_eq!(a.as_ref().unwrap().required_approving_review_count, Some(3));
            assert_eq!(a.unwrap(), b.unwrap());
        };
        let (old, ()) = tokio::join!(old, forced);
        assert_eq!(old.unwrap().required_approving_review_count, Some(2));
    }).await;
    assert_eq!(mock.calls("/rules/branches/"), 2);
    let counts = traffic.snapshot();
    let rules = &counts.counts[&(Caller::OnDemand, Operation::Rules)];
    assert_eq!(rules.rest_requests, 2);
    assert_eq!(rules.in_flight_reuses, 1);
}
