//! Immutable ancestry through the real HTTP adapter and shared service cache.
use super::qwen_regression::qwen::{MockQwen, ReadMode};
use super::*;
use serde_json::Value;

const BASE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const NEXT_BASE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn ancestry_fixture(mock: &MockQwen) {
    mock.edit(|s| {
        s.pr["baseRef"] = json!({"target":{"oid":BASE}});
        s.pr["baseRefOid"] = json!("cccccccccccccccccccccccccccccccccccccccc");
        s.pr["headRepository"] = json!({"id":"fork-one"});
        s.pr["mergeStateStatus"] = json!("CLEAN");
    });
}

async fn read(mock: &MockQwen, cache: &PrCache, policy: PrReadPolicy) -> PrCacheEntry {
    read_pr_via(
        mock.sc.as_ref(),
        &RepoRef::new("base-owner", "base-repo"),
        10978,
        cache,
        policy,
        &NONE,
    )
    .await
    .unwrap()
}

fn wire(entry: &PrCacheEntry) -> Value {
    serde_json::to_value(&entry.snapshot.requirements).unwrap()
}

#[tokio::test]
async fn ancestry_behind_is_informational_and_reads_are_bounded() {
    let mock = MockQwen::start(10978).await;
    ancestry_fixture(&mock);
    let cache = PrCache::default();
    let first = read(&mock, &cache, PrReadPolicy::Poll).await;
    let value = wire(&first);
    assert_eq!(value["ancestry"]["status"], "known");
    assert_eq!(value["ancestry"]["baseSha"], BASE);
    assert_eq!(
        value["ancestry"]["headSha"],
        first.pr.head_sha.as_deref().unwrap()
    );
    assert_eq!(value["ancestry"]["behindBy"], 1);
    assert_eq!(value["isBehind"], false);
    assert_eq!(value["branchUpdateRequired"], false);
    assert_eq!(mock.calls("/graphql"), 1);
    assert_eq!(mock.calls("/rules/branches/"), 1);
    assert_eq!(mock.calls("/compare/"), 1);
    let path = format!(
        "/repos/base-owner/base-repo/compare/{BASE}...{}?per_page=1&page=1",
        first.pr.head_sha.unwrap()
    );
    assert_eq!(
        mock.calls(&path),
        1,
        "base repository, immutable pair, one page"
    );
    for _ in 0..3 {
        assert_eq!(
            wire(
                &read(
                    &mock,
                    &cache,
                    PrReadPolicy::Serve {
                        max_age: Duration::from_secs(60)
                    }
                )
                .await
            ),
            value
        );
    }
    assert_eq!(mock.calls("/graphql"), 1, "Serve hit costs zero reads");
    for _ in 0..PR_MONITOR_MAX_CHEAP_POLLS {
        read(&mock, &cache, PrReadPolicy::Poll).await;
        assert_eq!(mock.calls("/compare/"), 1);
    }
    read(&mock, &cache, PrReadPolicy::Poll).await;
    assert_eq!(mock.calls("/compare/"), 2, "poll bound forces full refresh");
    backdate_pr_cache(&cache, PR_MONITOR_MAX_CHEAP_AGE);
    read(&mock, &cache, PrReadPolicy::Poll).await;
    assert_eq!(mock.calls("/compare/"), 3, "age bound forces full refresh");
    read(&mock, &cache, PrReadPolicy::REFRESH).await;
    assert_eq!(mock.calls("/compare/"), 4, "explicit refresh compares once");
}

#[tokio::test]
async fn ancestry_failures_cache_unknown_without_retries_or_false_zero() {
    let mock = MockQwen::start(10978).await;
    ancestry_fixture(&mock);
    for (status, body) in [
        (200, json!({})),
        (200, json!({"behind_by":null})),
        (200, json!({"behind_by":-1})),
        (200, json!({"behind_by":1.5})),
        (200, json!({"behind_by":"0"})),
        (404, json!({"message":"fork commit deleted"})),
        (403, json!({"message":"forbidden"})),
        (500, json!({"message":"unavailable"})),
    ] {
        let cache = PrCache::default();
        mock.edit(|s| {
            s.compare_status = status;
            s.compare = body;
        });
        let before = mock.calls("/compare/");
        let entry = read(&mock, &cache, PrReadPolicy::Poll).await;
        assert_eq!(wire(&entry)["ancestry"], json!({"status":"unknown"}));
        assert_eq!(wire(&entry)["branchUpdateRequired"], false);
        assert!(
            entry.snapshot.is_complete(),
            "ancestry failure alone must not defeat cheap polls"
        );
        for _ in 0..PR_MONITOR_MAX_CHEAP_POLLS {
            read(&mock, &cache, PrReadPolicy::Poll).await;
        }
        assert_eq!(
            mock.calls("/compare/"),
            before + 1,
            "single attempt; ordinary failure cached"
        );
        read(&mock, &cache, PrReadPolicy::Poll).await;
        assert_eq!(mock.calls("/compare/"), before + 2);
    }
}

#[tokio::test]
async fn ancestry_revision_changes_never_reuse_old_current_result() {
    let mock = MockQwen::start(10978).await;
    ancestry_fixture(&mock);
    let cache = PrCache::default();
    mock.edit(|s| s.compare = json!({"behind_by":0}));
    assert_eq!(
        wire(&read(&mock, &cache, PrReadPolicy::Poll).await)["ancestry"]["behindBy"],
        0
    );
    mock.edit(|s| {
        s.pr["baseRef"]["target"]["oid"] = json!(NEXT_BASE);
        s.compare = json!({"behind_by":2});
    });
    let moved = wire(&read(&mock, &cache, PrReadPolicy::Poll).await);
    assert_eq!(moved["ancestry"]["baseSha"], NEXT_BASE);
    assert_eq!(moved["ancestry"]["behindBy"], 2);
    mock.edit(|s| s.base_branch = "release".into());
    read(&mock, &cache, PrReadPolicy::Poll).await;
    mock.edit(|s| s.pr["headRepository"] = json!({"id":"fork-two"}));
    read(&mock, &cache, PrReadPolicy::Poll).await;
    mock.edit(|s| s.pr["headRefOid"] = json!(BASE));
    read(&mock, &cache, PrReadPolicy::Poll).await;
    assert_eq!(
        mock.calls("/compare/"),
        5,
        "base, target, fork and head each invalidate"
    );
    mock.edit(|s| s.pr["baseRef"] = Value::Null);
    assert_eq!(
        wire(&read(&mock, &cache, PrReadPolicy::Poll).await)["ancestry"],
        json!({"status":"unknown"})
    );
    assert_eq!(
        mock.calls("/compare/"),
        5,
        "missing live base cannot use baseRefOid or request a compare"
    );
    mock.edit(|s| s.mode = ReadMode::Standalone);
    assert_eq!(
        wire(&read(&mock, &cache, PrReadPolicy::REFRESH).await)["ancestry"],
        json!({"status":"unknown"})
    );
    assert_eq!(
        mock.calls("/compare/"),
        5,
        "REST-only path has no validated live base"
    );
}

#[tokio::test]
async fn ancestry_quota_failure_preserves_cached_baseline() {
    let mock = MockQwen::start(10978).await;
    ancestry_fixture(&mock);
    let cache = PrCache::default();
    let first = read(&mock, &cache, PrReadPolicy::Poll).await;
    mock.edit(|s| {
        s.pr["baseRef"]["target"]["oid"] = json!(NEXT_BASE);
        s.compare_status = 403;
        s.compare = json!({"message":"API rate limit exceeded"});
    });
    let error = read_pr_via(
        mock.sc.as_ref(),
        &RepoRef::new("base-owner", "base-repo"),
        10978,
        &cache,
        PrReadPolicy::Poll,
        &NONE,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, Error::RateLimited(_)));
    assert_eq!(mock.calls("/compare/"), 2);
    let retained = read(
        &mock,
        &cache,
        PrReadPolicy::Serve {
            max_age: Duration::from_secs(60),
        },
    )
    .await;
    assert_eq!(wire(&retained), wire(&first));
    mock.edit(|s| {
        s.compare_status = 200;
        s.compare = json!({"behind_by":3});
    });
    assert_eq!(
        wire(&read(&mock, &cache, PrReadPolicy::Poll).await)["ancestry"]["baseSha"],
        NEXT_BASE
    );
}

#[tokio::test]
async fn ancestry_paged_observation_identity_races_degrade_only_ancestry() {
    for (field, value) in [
        ("baseRef", json!({"target":{"oid":NEXT_BASE}})),
        ("baseRefName", json!("release")),
        ("headRepository", json!({"id":"fork-two"})),
    ] {
        let mock = MockQwen::start(11506).await;
        ancestry_fixture(&mock);
        mock.edit(|s| s.ancestry_page_change = Some((field, value)));
        let entry = read(&mock, &PrCache::default(), PrReadPolicy::Poll).await;
        assert_eq!(wire(&entry)["ancestry"], json!({"status":"unknown"}));
        assert!(entry.snapshot.requirements.checks.required_known);
        assert_eq!(mock.calls("/compare/"), 0);
        assert_eq!(mock.calls("/graphql"), 2);
    }
}

#[tokio::test]
async fn ancestry_older_poll_cannot_overwrite_newer_on_demand_comparison() {
    let mock = MockQwen::start(10978).await;
    ancestry_fixture(&mock);
    let cache = PrCache::default();
    let gate = mock.gate(&format!("/compare/{BASE}..."));
    let (sc, poll_cache) = (mock.sc.clone(), cache.clone());
    let poll = tokio::spawn(async move {
        read_pr_via(
            sc.as_ref(),
            &RepoRef::new("base-owner", "base-repo"),
            10978,
            &poll_cache,
            PrReadPolicy::Poll,
            &NONE,
        )
        .await
    });
    gate.entered.notified().await;
    mock.edit(|s| {
        s.pr["baseRef"]["target"]["oid"] = json!(NEXT_BASE);
        s.compare = json!({"behind_by":3});
    });
    let newer = read(&mock, &cache, PrReadPolicy::REFRESH).await;
    gate.release.add_permits(1);
    let older = poll.await.unwrap().unwrap();
    assert_eq!(wire(&older)["ancestry"]["baseSha"], BASE);
    let served = read(
        &mock,
        &cache,
        PrReadPolicy::Serve {
            max_age: Duration::from_secs(60),
        },
    )
    .await;
    assert_eq!(wire(&served), wire(&newer));
    assert_eq!(wire(&served)["ancestry"]["baseSha"], NEXT_BASE);
    assert_eq!(mock.calls("/compare/"), 2);
}

#[tokio::test]
async fn ancestry_scope_changes_and_restart_do_not_share_comparisons() {
    let mock = MockQwen::start(10978).await;
    ancestry_fixture(&mock);
    let cache = PrCache::default();
    let policy = PrReadPolicy::Serve {
        max_age: Duration::from_secs(60),
    };
    read(&mock, &cache, policy).await;
    let other_token =
        intent_sourcecontrol::GitHubSourceControl::new("other-token", Some(&mock.base)).unwrap();
    let repo = RepoRef::new("base-owner", "base-repo");
    read_pr_via(&other_token, &repo, 10978, &cache, policy, &NONE)
        .await
        .unwrap();
    read_pr_via(
        &other_token,
        &RepoRef::new("other-owner", "base-repo"),
        10978,
        &cache,
        policy,
        &NONE,
    )
    .await
    .unwrap();
    read_pr_via(&other_token, &repo, 10979, &cache, policy, &NONE)
        .await
        .unwrap();
    assert_eq!(mock.calls("/compare/"), 4);
    let other_host = MockQwen::start(10978).await;
    ancestry_fixture(&other_host);
    read(&other_host, &cache, policy).await;
    assert_eq!(other_host.calls("/compare/"), 1);
    read(&other_host, &PrCache::default(), policy).await;
    assert_eq!(other_host.calls("/compare/"), 2, "restart is cold");
    intent_sourcecontrol::cache_scope::invalidate_authorization();
    assert!(
        read_pr_via(other_host.sc.as_ref(), &repo, 10978, &cache, policy, &NONE)
            .await
            .is_err()
    );
    assert_eq!(
        other_host.calls("/compare/"),
        2,
        "revoked scope cannot serve or fetch"
    );
    let renewed =
        intent_sourcecontrol::GitHubSourceControl::new("fixture-token", Some(&other_host.base))
            .unwrap();
    read_pr_via(&renewed, &repo, 10978, &cache, policy, &NONE)
        .await
        .unwrap();
    assert_eq!(other_host.calls("/compare/"), 3);
}

#[tokio::test]
async fn ancestry_missing_refs_and_terminal_prs_never_compare() {
    for (field, value) in [
        ("baseRef", Value::Null),
        ("baseRef", json!({"target":{"oid":"main"}})),
        ("headRefOid", json!("")),
        ("state", json!("MERGED")),
        ("state", json!("CLOSED")),
    ] {
        let mock = MockQwen::start(10978).await;
        ancestry_fixture(&mock);
        mock.edit(|s| s.pr[field] = value);
        let entry = read(&mock, &PrCache::default(), PrReadPolicy::REFRESH).await;
        assert_eq!(wire(&entry)["ancestry"], json!({"status":"unknown"}));
        assert_eq!(mock.calls("/compare/"), 0);
    }
}

#[tokio::test]
async fn ancestry_counts_never_override_forge_behind_or_conflicts() {
    let mock = MockQwen::start(10978).await;
    ancestry_fixture(&mock);
    for (status, required, conflicts, legacy) in [
        ("BEHIND", Some(true), false, true),
        ("DIRTY", None, true, false),
        ("BLOCKED", None, false, false),
        ("UNKNOWN", None, false, false),
        ("CLEAN", Some(false), false, false),
    ] {
        for count in [json!(0), json!(1), Value::Null] {
            mock.edit(|s| {
                s.pr["mergeStateStatus"] = json!(status);
                s.compare = json!({"behind_by":count});
            });
            let value = wire(&read(&mock, &PrCache::default(), PrReadPolicy::REFRESH).await);
            assert_eq!(
                value.get("branchUpdateRequired"),
                required.map(Value::Bool).as_ref()
            );
            assert_eq!(value["isBehind"], legacy);
            assert_eq!(value["hasConflicts"], conflicts);
            if count.is_null() {
                assert_eq!(value["ancestry"], json!({"status":"unknown"}));
            } else {
                assert_eq!(value["ancestry"]["behindBy"], count);
            }
        }
    }
}

#[test]
fn ancestry_old_persisted_checklists_decode_to_unknown() {
    let snapshot = snapshot(|_| {});
    let mut old = serde_json::to_value(&snapshot).unwrap();
    let requirements = old["requirements"].as_object_mut().unwrap();
    requirements.remove("ancestry");
    requirements.remove("branchUpdateRequired");
    let loaded: PrMonitorSnapshot = serde_json::from_value(old).unwrap();
    let value = serde_json::to_value(loaded.requirements).unwrap();
    assert_eq!(value["ancestry"], json!({"status":"unknown"}));
    assert!(value.get("branchUpdateRequired").is_none());
}

#[tokio::test]
async fn ancestry_authorization_change_during_compare_cannot_populate_cache() {
    let mock = MockQwen::start(10978).await;
    ancestry_fixture(&mock);
    let cache = PrCache::default();
    let gate = mock.gate("/compare/");
    let (sc, poll_cache) = (mock.sc.clone(), cache.clone());
    let poll = tokio::spawn(async move {
        read_pr_via(
            sc.as_ref(),
            &RepoRef::new("base-owner", "base-repo"),
            10978,
            &poll_cache,
            PrReadPolicy::Poll,
            &NONE,
        )
        .await
    });
    gate.entered.notified().await;
    intent_sourcecontrol::cache_scope::invalidate_authorization();
    gate.release.add_permits(1);
    assert!(poll.await.unwrap().is_err());
    assert_eq!(pr_cache_len(&cache), 0);
}

#[tokio::test]
async fn ancestry_compare_quota_pauses_sweep_and_keeps_monitor_baseline() {
    let (_db, _root, svc, _forge, ws, owner) = setup().await;
    let mock = MockQwen::start(10978).await;
    ancestry_fixture(&mock);
    let svc = svc.with_source_control(mock.sc.clone());
    let (monitor, _) = svc
        .pr_monitor_register(&ws, &owner, "base-owner", "base-repo", 10978)
        .await
        .unwrap();
    let before = svc
        .store()
        .get_pr_monitor(&monitor.monitor_id)
        .await
        .unwrap();
    mock.edit(|s| {
        s.pr["baseRef"]["target"]["oid"] = json!(NEXT_BASE);
        s.compare_status = 403;
        s.compare = json!({"message":"API rate limit exceeded"});
    });
    svc.poll_pr_monitors().await;
    let failed = svc
        .store()
        .get_pr_monitor(&monitor.monitor_id)
        .await
        .unwrap();
    assert_eq!(failed.last_snapshot, before.last_snapshot);
    assert!(failed.last_error.is_some());
    assert!(svc.sweep_rate_limit.paused_remaining().is_some());
    assert_eq!(mock.calls("/compare/"), 2);
    let observed = mock.calls("GetPrObservation");
    svc.poll_pr_monitors().await;
    assert_eq!(
        mock.calls("GetPrObservation"),
        observed,
        "paused sweep issues no observation"
    );
    assert_eq!(
        mock.calls("/compare/"),
        2,
        "no ancestry work bypasses the pause"
    );
}
