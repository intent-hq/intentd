//! Unit tests for the generic per-provider model cache (PROTOCOL §5.30):
//! TTL, version-key invalidation, forceRefresh bypass, failure fallback,
//! single-flighting, negative caching, and persistence.

use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{json, Value};

use super::*;

fn rows(tag: &str) -> Vec<Value> {
    vec![json!({ "id": tag, "name": tag, "provider": "test" })]
}

fn ok_fetch(tag: &'static str) -> impl FnOnce() -> BoxFuture<'static, ModelFetchResult> {
    move || {
        Box::pin(async move {
            ModelFetchResult {
                models: Some(rows(tag)),
                warning: None,
            }
        })
    }
}

fn failing_fetch() -> impl FnOnce() -> BoxFuture<'static, ModelFetchResult> {
    || {
        Box::pin(async {
            ModelFetchResult {
                models: None,
                warning: Some("probe failed".to_string()),
            }
        })
    }
}

fn panicking_fetch() -> impl FnOnce() -> BoxFuture<'static, ModelFetchResult> {
    || panic!("fetch must not be called on a fresh cache hit")
}

/// A fetch that counts its runs: only the single-flight leader's closure
/// actually executes, so the counter proves how many probes ran.
fn counting_fetch(
    calls: &Arc<AtomicUsize>,
    tag: &'static str,
) -> impl FnOnce() -> BoxFuture<'static, ModelFetchResult> {
    let calls = calls.clone();
    move || {
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            ModelFetchResult {
                models: Some(rows(tag)),
                warning: None,
            }
        })
    }
}

const TTL_MS: u64 = super::MODELS_CACHE_TTL.as_millis() as u64;
const NEG_TTL_MS: u64 = super::MODELS_NEGATIVE_TTL.as_millis() as u64;

#[tokio::test]
async fn fresh_cache_hit_within_ttl_skips_fetch() {
    let cache = ModelCatalogCache::new(None);
    cache.store("p", "v1", rows("cached"), 1_000);
    let r = resolve_with_cache(
        &cache,
        "p",
        "v1",
        false,
        1_000 + TTL_MS - 1,
        panicking_fetch(),
    )
    .await;
    assert_eq!(r.models, Some(rows("cached")));
    assert!(!r.stale);
    assert!(r.warning.is_none());
}

#[tokio::test]
async fn entry_fetched_in_the_future_is_not_fresh() {
    // System clock moved backwards: an entry stamped ahead of `now` must not
    // be served as fresh (it could otherwise outlive the TTL indefinitely).
    let cache = ModelCatalogCache::new(None);
    cache.store("p", "v1", rows("future"), 10_000);
    let r = resolve_with_cache(&cache, "p", "v1", false, 5_000, ok_fetch("probed")).await;
    assert_eq!(r.models, Some(rows("probed")));
    assert!(!r.stale);
}

#[tokio::test]
async fn expired_cache_awaits_fresh_probe_and_stores() {
    let cache = ModelCatalogCache::new(None);
    cache.store("p", "v1", rows("old"), 1_000);
    let now = 1_000 + TTL_MS;
    let r = resolve_with_cache(&cache, "p", "v1", false, now, ok_fetch("new")).await;
    assert_eq!(r.models, Some(rows("new")));
    assert!(!r.stale);
    // The fresh result replaced the entry.
    assert_eq!(cache.fresh("p", "v1", now), Some(rows("new")));
}

#[tokio::test]
async fn version_key_bump_invalidates_within_ttl() {
    let cache = ModelCatalogCache::new(None);
    cache.store("p", "v1", rows("pinned-old"), 1_000);
    // Same instant, new version key → cache miss, fetch served + stored.
    let r = resolve_with_cache(&cache, "p", "v2", false, 1_001, ok_fetch("pinned-new")).await;
    assert_eq!(r.models, Some(rows("pinned-new")));
    // And a failed probe must not fall back to rows from another version key.
    let cache = ModelCatalogCache::new(None);
    cache.store("p", "v1", rows("pinned-old"), 1_000);
    let r = resolve_with_cache(&cache, "p", "v2", false, 1_001, failing_fetch()).await;
    assert!(r.models.is_none());
}

#[tokio::test]
async fn force_refresh_bypasses_fresh_cache() {
    let cache = ModelCatalogCache::new(None);
    cache.store("p", "v1", rows("cached"), 1_000);
    let r = resolve_with_cache(&cache, "p", "v1", true, 1_001, ok_fetch("forced")).await;
    assert_eq!(r.models, Some(rows("forced")));
    assert!(!r.stale);
    assert_eq!(cache.fresh("p", "v1", 1_001), Some(rows("forced")));
}

#[tokio::test]
async fn force_refresh_failure_serves_last_good_with_warning() {
    let cache = ModelCatalogCache::new(None);
    cache.store("p", "v1", rows("last-good"), 1_000);
    let r = resolve_with_cache(&cache, "p", "v1", true, 1_001, failing_fetch()).await;
    assert_eq!(r.models, Some(rows("last-good")));
    assert!(r.stale);
    let warning = r.warning.expect("stale data must be labeled");
    assert!(warning.contains("probe failed"), "{warning}");
}

#[tokio::test]
async fn failure_without_last_good_reports_nothing_to_serve() {
    let cache = ModelCatalogCache::new(None);
    let r = resolve_with_cache(&cache, "p", "v1", false, 1_000, failing_fetch()).await;
    assert!(r.models.is_none());
    assert!(!r.stale);
    assert_eq!(r.warning.as_deref(), Some("probe failed"));
}

#[tokio::test]
async fn empty_success_is_served_but_not_cached() {
    let cache = ModelCatalogCache::new(None);
    let empty = || -> BoxFuture<'static, ModelFetchResult> {
        Box::pin(async {
            ModelFetchResult {
                models: Some(Vec::new()),
                warning: Some("gated".to_string()),
            }
        })
    };
    let r = resolve_with_cache(&cache, "p", "v1", false, 1_000, empty).await;
    assert_eq!(r.models, Some(Vec::new()));
    assert_eq!(r.warning.as_deref(), Some("gated"));
    assert!(cache.last_good("p", "v1").is_none());
}

#[test]
fn persistence_roundtrips_across_instances() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(MODELS_CACHE_FILE);
    let cache = ModelCatalogCache::new(Some(path.clone()));
    cache.store("p", "v1", rows("persisted"), 1_000);
    let reloaded = ModelCatalogCache::new(Some(path.clone()));
    assert_eq!(reloaded.fresh("p", "v1", 1_001), Some(rows("persisted")));
    // A version-key bump invalidates the reloaded entry too.
    assert!(reloaded.fresh("p", "v2", 1_001).is_none());
    assert!(reloaded.last_good("p", "v2").is_none());
    // Corrupt files are ignored, not errors.
    std::fs::write(&path, b"not json").unwrap();
    let corrupt = ModelCatalogCache::new(Some(path));
    assert!(corrupt.last_good("p", "v1").is_none());
}

#[test]
fn registry_covers_all_eight_providers() {
    for provider in [
        "auggie",
        "cortex",
        "claude-code",
        "codex",
        "pi",
        "droid",
        "opencode",
        "grok",
    ] {
        assert!(source_for(provider).is_some(), "{provider} not registered");
    }
    assert!(source_for("no-such-provider").is_none());
}

#[test]
fn registry_version_keys_follow_adapter_pins() {
    let key = |provider: &str| (source_for(provider).unwrap().version_key)();
    assert_eq!(key("auggie"), "");
    assert_eq!(key("cortex"), "");
    // Keyed on the full npx package spec (name + version), not just the
    // version constant, so a package rename also invalidates cache entries.
    assert_eq!(
        key("claude-code"),
        intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE
    );
    assert_eq!(key("pi"), intent_providers::PI_ACP_NPX_PACKAGE);
    assert_eq!(key("droid"), "");
    assert_eq!(key("opencode"), "");
    assert_eq!(key("grok"), "");
    // codex mirrors the fetch dispatch: pinned to the npx fallback only when
    // no codex-acp binary resolves on this machine.
    let expected = if intent_providers::find_provider_binary("codex", "codex-acp", None).is_some() {
        String::new()
    } else {
        intent_providers::config::CODEX_ACP_NPX_PACKAGE.to_string()
    };
    assert_eq!(key("codex"), expected);
}

#[tokio::test]
async fn provider_fetch_failure_yields_stale_last_good_through_cache() {
    // A provider_models probe failure (models: None + warning) adapted via
    // `from_provider_fetch` must flow into the cache's stale fallback.
    let cache = ModelCatalogCache::new(None);
    cache.store("droid", "", rows("droid-last-good"), 1_000);
    let failed = || -> BoxFuture<'static, ModelFetchResult> {
        Box::pin(async {
            from_provider_fetch(crate::provider_models::ProviderModelsFetch {
                models: None,
                warning: Some("droid: droid binary not found".to_string()),
            })
        })
    };
    let r = resolve_with_cache(&cache, "droid", "", false, 1_000 + TTL_MS, failed).await;
    assert_eq!(r.models, Some(rows("droid-last-good")));
    assert!(r.stale);
    let warning = r.warning.expect("stale data must be labeled");
    assert!(warning.contains("droid binary not found"), "{warning}");
}

#[tokio::test]
async fn concurrent_cold_reads_single_flight_one_probe() {
    let cache = Arc::new(ModelCatalogCache::new(None));
    let calls = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let cache = cache.clone();
            let fetch = counting_fetch(&calls, "shared");
            tokio::spawn(
                async move { resolve_with_cache(&cache, "p", "v1", false, 1_000, fetch).await },
            )
        })
        .collect();
    for h in handles {
        let r = h.await.expect("join");
        assert_eq!(r.models, Some(rows("shared")));
        assert!(!r.stale);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one probe runs");
}

#[tokio::test]
async fn concurrent_forced_reads_single_flight_one_probe() {
    // forceRefresh bypasses the caches but still coalesces into one probe.
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("cached"), 1_000);
    let calls = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let cache = cache.clone();
            let fetch = counting_fetch(&calls, "forced");
            tokio::spawn(
                async move { resolve_with_cache(&cache, "p", "v1", true, 1_001, fetch).await },
            )
        })
        .collect();
    for h in handles {
        assert_eq!(h.await.expect("join").models, Some(rows("forced")));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one probe runs");
}

#[tokio::test]
async fn inflight_slot_is_released_after_probe() {
    // A later (post-completion) read must run its own probe, not the stale
    // shared cell — i.e. finish_inflight released the slot.
    let cache = ModelCatalogCache::new(None);
    let calls = Arc::new(AtomicUsize::new(0));
    let r = resolve_with_cache(
        &cache,
        "p",
        "v1",
        false,
        1_000,
        counting_fetch(&calls, "one"),
    )
    .await;
    assert_eq!(r.models, Some(rows("one")));
    let r = resolve_with_cache(
        &cache,
        "p",
        "v1",
        true,
        1_001,
        counting_fetch(&calls, "two"),
    )
    .await;
    assert_eq!(r.models, Some(rows("two")));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn negative_window_suppresses_reprobe_without_last_good() {
    let cache = ModelCatalogCache::new(None);
    let r = resolve_with_cache(&cache, "p", "v1", false, 1_000, failing_fetch()).await;
    assert!(r.models.is_none());
    // Within the negative window the fetch must not run again.
    let r = resolve_with_cache(
        &cache,
        "p",
        "v1",
        false,
        1_000 + NEG_TTL_MS - 1,
        panicking_fetch(),
    )
    .await;
    assert!(r.models.is_none());
    assert!(!r.stale);
    assert_eq!(r.warning.as_deref(), Some("probe failed"));
}

#[tokio::test]
async fn negative_window_serves_last_good_as_stale() {
    let cache = ModelCatalogCache::new(None);
    cache.store("p", "v1", rows("last-good"), 1_000);
    let now = 1_000 + TTL_MS;
    let r = resolve_with_cache(&cache, "p", "v1", false, now, failing_fetch()).await;
    assert_eq!(r.models, Some(rows("last-good")));
    assert!(r.stale);
    // Within the negative window: same stale fallback, no re-probe.
    let r = resolve_with_cache(
        &cache,
        "p",
        "v1",
        false,
        now + NEG_TTL_MS - 1,
        panicking_fetch(),
    )
    .await;
    assert_eq!(r.models, Some(rows("last-good")));
    assert!(r.stale);
    let warning = r.warning.expect("stale data must be labeled");
    assert!(warning.contains("probe failed"), "{warning}");
}

#[tokio::test]
async fn negative_entry_expires_and_reprobe_succeeds() {
    let cache = ModelCatalogCache::new(None);
    let r = resolve_with_cache(&cache, "p", "v1", false, 1_000, failing_fetch()).await;
    assert!(r.models.is_none());
    // Past the negative TTL the probe runs again; success clears the entry.
    let now = 1_000 + NEG_TTL_MS;
    let r = resolve_with_cache(&cache, "p", "v1", false, now, ok_fetch("recovered")).await;
    assert_eq!(r.models, Some(rows("recovered")));
    assert!(!r.stale);
    assert!(cache.negative_reason("p", "v1", now).is_none());
}

#[tokio::test]
async fn force_refresh_bypasses_negative_window() {
    let cache = ModelCatalogCache::new(None);
    let r = resolve_with_cache(&cache, "p", "v1", false, 1_000, failing_fetch()).await;
    assert!(r.models.is_none());
    // Within the window a forced read still probes — and its success clears
    // the negative entry for subsequent non-forced reads.
    let r = resolve_with_cache(&cache, "p", "v1", true, 1_001, ok_fetch("forced")).await;
    assert_eq!(r.models, Some(rows("forced")));
    assert_eq!(cache.fresh("p", "v1", 1_002), Some(rows("forced")));
}

#[tokio::test]
async fn only_the_leader_records_the_probe_outcome() {
    // Followers must not re-record the shared result: a late-scheduled
    // follower of a failed probe would otherwise re-arm the negative window
    // after a newer forced probe succeeded and cleared it. Recording lives
    // inside the get_or_init initializer, so a pre-resolved cell (follower's
    // view) must leave the caches untouched.
    let cache = ModelCatalogCache::new(None);
    let cell = cache.join_inflight("p", "v1");
    assert!(cell
        .set(ModelFetchResult {
            models: None,
            warning: Some("stale failure".to_string()),
        })
        .is_ok());
    // Simulate the newer probe's success landing first.
    cache.store("p", "v1", rows("recovered"), 1_000);
    // The "follower" resolves against the pre-resolved cell: it serves the
    // shared failure view but must not write a negative entry.
    let r = resolve_with_cache(&cache, "p", "v1", true, 1_001, panicking_fetch()).await;
    assert_eq!(r.models, Some(rows("recovered")));
    assert!(r.stale);
    assert!(
        cache.negative_reason("p", "v1", 1_001).is_none(),
        "follower must not record a negative entry"
    );
    // And the recovered rows were not clobbered.
    assert_eq!(cache.fresh("p", "v1", 1_001), Some(rows("recovered")));
}

#[tokio::test]
async fn negative_entry_is_version_key_scoped() {
    let cache = ModelCatalogCache::new(None);
    let r = resolve_with_cache(&cache, "p", "v1", false, 1_000, failing_fetch()).await;
    assert!(r.models.is_none());
    // A version-key bump must not be blocked by the old key's failure.
    let r = resolve_with_cache(&cache, "p", "v2", false, 1_001, ok_fetch("bumped")).await;
    assert_eq!(r.models, Some(rows("bumped")));
}
