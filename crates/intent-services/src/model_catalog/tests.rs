//! Unit tests for the generic per-provider model cache (PROTOCOL §5.30):
//! fresh-window serving, stale-while-revalidate background refresh past
//! [`MODELS_STALE_AFTER`] (intent-hq/intent#3874), version-key invalidation,
//! forceRefresh bypass, failure fallback, single-flighting, negative
//! caching, and persistence.

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

// `try_from` is not const-callable; the TTLs are far below `u64::MAX` millis.
#[allow(clippy::cast_possible_truncation)]
const NEG_TTL_MS: u64 = super::MODELS_NEGATIVE_TTL.as_millis() as u64;

/// The staleness threshold in millis (see `NEG_TTL_MS` for the cast note).
#[allow(clippy::cast_possible_truncation)]
const STALE_MS: u64 = super::MODELS_STALE_AFTER.as_millis() as u64;

/// Await a detached background-refresh outcome: poll `cond` until it holds
/// or the budget expires. Under a paused clock the sleeps auto-advance, so
/// this never slows a test down.
async fn wait_until(mut cond: impl FnMut() -> bool) {
    for _ in 0..1_000 {
        if cond() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    panic!("condition not reached within the polling budget");
}

#[test]
fn auggie_catalog_version_invalidates_pre_legacy_cache() {
    let source = source_for("auggie").expect("auggie source");
    assert_eq!((source.version_key)(), AUGGIE_CATALOG_VERSION);
    assert_ne!((source.version_key)(), "");
}

#[tokio::test]
async fn cache_hit_skips_fetch() {
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("cached"), 1_000);
    let r = resolve_with_cache(&cache, "p", "v1", false, 1_001, panicking_fetch()).await;
    assert_eq!(r.models, Some(rows("cached")));
    assert!(!r.stale);
    assert!(r.warning.is_none());
}

#[tokio::test]
async fn entry_below_stale_threshold_is_served_without_probe() {
    // Just under the staleness threshold the entry is still a plain hit — a
    // non-forced read must not spawn a probe while the entry is fresh.
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("aging"), 1_000);
    let r = resolve_with_cache(
        &cache,
        "p",
        "v1",
        false,
        1_000 + STALE_MS - 1,
        panicking_fetch(),
    )
    .await;
    assert_eq!(r.models, Some(rows("aging")));
    assert!(!r.stale);
    assert!(r.warning.is_none());
}

#[tokio::test]
async fn aged_entry_served_stale_while_background_refresh_stores_result() {
    // Stale-while-revalidate (intent-hq/intent#3874): an entry at or past
    // MODELS_STALE_AFTER is served immediately — labeled stale + warning —
    // and the refresh probe runs detached, so newly released provider models
    // still show up (intent-hq/intent#3682) without the read blocking.
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("old"), 1_000);
    let now = 1_000 + STALE_MS;
    let r = resolve_with_cache(&cache, "p", "v1", false, now, ok_fetch("refreshed")).await;
    assert_eq!(r.models, Some(rows("old")), "the stale list serves as-is");
    assert!(r.stale);
    let warning = r.warning.expect("stale data must be labeled");
    assert!(
        warning.contains("refreshing in the background"),
        "{warning}"
    );
    // The background refresh lands in the cache and releases its slot; the
    // next read is then a plain fresh hit of the refreshed rows — no probe.
    wait_until(|| {
        cache.last_good("p", "v1") == Some(rows("refreshed"))
            && !cache.test_inflight_active("p", "v1")
    })
    .await;
    let r = resolve_with_cache(&cache, "p", "v1", false, now + 1, panicking_fetch()).await;
    assert_eq!(r.models, Some(rows("refreshed")));
    assert!(!r.stale);
    assert!(r.warning.is_none());
}

#[tokio::test]
async fn aged_entry_failed_background_refresh_arms_negative_window() {
    // An aged entry whose background refresh fails keeps serving: the read
    // that spawned the refresh already returned the stale-labeled last-good
    // list, the failure lands in the negative cache, and within the window
    // subsequent non-forced reads keep serving the stale list without
    // re-spawning the probe (and without spawning a background refresh —
    // panicking_fetch proves the closure is never invoked).
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("last-good"), 1_000);
    let now = 1_000 + STALE_MS;
    let r = resolve_with_cache(&cache, "p", "v1", false, now, failing_fetch()).await;
    assert_eq!(r.models, Some(rows("last-good")));
    assert!(r.stale);
    assert!(r.warning.is_some());
    wait_until(|| cache.test_negative_reason("p", "v1", now).is_some()).await;
    assert!(!cache.test_inflight_active("p", "v1"), "slot released");
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
async fn concurrent_aged_reads_single_flight_one_background_probe() {
    // Concurrent stale reads spawn at most one background probe — none of
    // them awaits it, and every response serves immediately (the stale list,
    // or the refreshed rows once the probe has landed).
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("old"), 1_000);
    let calls = Arc::new(AtomicUsize::new(0));
    let now = 1_000 + STALE_MS;
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let cache = cache.clone();
            let fetch = counting_fetch(&calls, "refreshed");
            tokio::spawn(
                async move { resolve_with_cache(&cache, "p", "v1", false, now, fetch).await },
            )
        })
        .collect();
    for h in handles {
        let r = h.await.expect("join");
        let models = r.models.expect("every read serves");
        if r.stale {
            assert_eq!(models, rows("old"));
        } else {
            assert_eq!(
                models,
                rows("refreshed"),
                "post-refresh reads are fresh hits"
            );
        }
    }
    wait_until(|| {
        cache.last_good("p", "v1") == Some(rows("refreshed"))
            && !cache.test_inflight_active("p", "v1")
    })
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one probe runs");
}

#[tokio::test]
async fn stale_read_does_not_await_or_duplicate_running_probe() {
    // While a probe for the (provider, version key) is already in flight, a
    // stale-serving read neither starts a second probe (panicking_fetch
    // proves the closure is never invoked) nor awaits the running one — it
    // returns the stale list immediately.
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("last-good"), 1_000);
    let _held = cache.join_inflight("p", "v1"); // simulate a running probe
    let now = 1_000 + STALE_MS;
    let r = resolve_with_cache(&cache, "p", "v1", false, now, panicking_fetch()).await;
    assert_eq!(r.models, Some(rows("last-good")));
    assert!(r.stale);
    assert!(cache.test_inflight_active("p", "v1"), "slot still held");
}

#[tokio::test(start_paused = true)]
async fn background_refresh_timeout_releases_slot_and_negative_caches() {
    // A wedged background probe cannot pin the in-flight slot: at
    // MODELS_BACKGROUND_REFRESH_TIMEOUT the fetch future is dropped, the
    // timeout is recorded as a negative entry, and the slot is released —
    // while the stale entry keeps serving throughout.
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("last-good"), 1_000);
    let now = 1_000 + STALE_MS;
    let wedged = || Box::pin(std::future::pending()) as BoxFuture<'static, ModelFetchResult>;
    let r = resolve_with_cache(&cache, "p", "v1", false, now, wedged).await;
    assert_eq!(r.models, Some(rows("last-good")));
    assert!(r.stale);
    assert!(cache.test_inflight_active("p", "v1"), "probe in flight");
    // Advance the paused clock past the hard timeout (the sleep yields to
    // the background task, whose timeout then fires), then let the released
    // slot become observable.
    tokio::time::sleep(MODELS_BACKGROUND_REFRESH_TIMEOUT + std::time::Duration::from_secs(1)).await;
    wait_until(|| !cache.test_inflight_active("p", "v1")).await;
    let reason = cache
        .test_negative_reason("p", "v1", now)
        .expect("timeout is negatively cached");
    assert!(reason.contains("timed out"), "{reason}");
    assert_eq!(cache.last_good("p", "v1"), Some(rows("last-good")));
}

#[tokio::test]
async fn aged_entry_empty_success_background_refresh_stores_nothing() {
    // Pins the documented empty-success edge (see `resolve_with_cache`): a
    // background refresh that returns an empty success stores nothing and
    // clears no ground for negative caching — the aged last-good list keeps
    // serving, and the next non-forced read spawns another refresh (which,
    // succeeding non-empty here, finally replaces the entry). Acceptable
    // while all empty-success sources are cheap env checks; a costly source
    // must convert empty to failure instead.
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("last-good"), 1_000);
    let now = 1_000 + STALE_MS;
    let empty_fetch = || {
        Box::pin(async {
            ModelFetchResult {
                models: Some(vec![]),
                warning: None,
            }
        }) as BoxFuture<'static, ModelFetchResult>
    };
    let r = resolve_with_cache(&cache, "p", "v1", false, now, empty_fetch).await;
    assert_eq!(r.models, Some(rows("last-good")));
    assert!(r.stale);
    wait_until(|| !cache.test_inflight_active("p", "v1")).await;
    // Not stored: the last-good list is untouched and still aged, and no
    // negative window suppresses the next read's refresh.
    assert_eq!(cache.last_good("p", "v1"), Some(rows("last-good")));
    assert!(cache.test_negative_reason("p", "v1", now).is_none());
    let r = resolve_with_cache(&cache, "p", "v1", false, now + 1, ok_fetch("recovered")).await;
    assert_eq!(r.models, Some(rows("last-good")));
    assert!(r.stale);
    wait_until(|| cache.last_good("p", "v1") == Some(rows("recovered"))).await;
}

#[tokio::test]
async fn entry_fetched_in_the_future_is_still_served() {
    // System clock moved backwards: age uses saturating subtraction, so a
    // future-stamped entry reads as age zero (fresh) and is served like any
    // hit; the wall clock eventually catches up and the TTL applies again.
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("future"), 10_000);
    let r = resolve_with_cache(&cache, "p", "v1", false, 5_000, panicking_fetch()).await;
    assert_eq!(r.models, Some(rows("future")));
    assert!(!r.stale);
}

#[tokio::test]
async fn cache_miss_awaits_probe_and_stores() {
    let cache = Arc::new(ModelCatalogCache::new(None));
    let now = 1_000;
    let r = resolve_with_cache(&cache, "p", "v1", false, now, ok_fetch("new")).await;
    assert_eq!(r.models, Some(rows("new")));
    assert!(!r.stale);
    // The result was stored: a later read is a hit, no probe.
    let r = resolve_with_cache(&cache, "p", "v1", false, now + 1, panicking_fetch()).await;
    assert_eq!(r.models, Some(rows("new")));
}

#[tokio::test]
async fn version_key_bump_invalidates_cached_entry() {
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("pinned-old"), 1_000);
    // Same instant, new version key → cache miss, fetch served + stored.
    let r = resolve_with_cache(&cache, "p", "v2", false, 1_001, ok_fetch("pinned-new")).await;
    assert_eq!(r.models, Some(rows("pinned-new")));
    // And a failed probe must not fall back to rows from another version key.
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("pinned-old"), 1_000);
    let r = resolve_with_cache(&cache, "p", "v2", false, 1_001, failing_fetch()).await;
    assert!(r.models.is_none());
}

#[tokio::test]
async fn force_refresh_bypasses_cache() {
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("cached"), 1_000);
    let r = resolve_with_cache(&cache, "p", "v1", true, 1_001, ok_fetch("forced")).await;
    assert_eq!(r.models, Some(rows("forced")));
    assert!(!r.stale);
    assert_eq!(cache.last_good("p", "v1"), Some(rows("forced")));
}

#[tokio::test]
async fn force_refresh_failure_serves_last_good_with_warning() {
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("last-good"), 1_000);
    let r = resolve_with_cache(&cache, "p", "v1", true, 1_001, failing_fetch()).await;
    assert_eq!(r.models, Some(rows("last-good")));
    assert!(r.stale);
    let warning = r.warning.expect("stale data must be labeled");
    assert!(warning.contains("probe failed"), "{warning}");
}

#[tokio::test]
async fn failure_without_last_good_reports_nothing_to_serve() {
    let cache = Arc::new(ModelCatalogCache::new(None));
    let r = resolve_with_cache(&cache, "p", "v1", false, 1_000, failing_fetch()).await;
    assert!(r.models.is_none());
    assert!(!r.stale);
    assert_eq!(r.warning.as_deref(), Some("probe failed"));
}

#[tokio::test]
async fn empty_success_is_served_but_not_cached() {
    let cache = Arc::new(ModelCatalogCache::new(None));
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
    assert_eq!(reloaded.last_good("p", "v1"), Some(rows("persisted")));
    // `fetchedAtMs` survives the reload: staleness carries across a daemon
    // restart — an entry persisted past the threshold is not a fresh hit
    // (the first read after restart re-probes) while the last-good list
    // remains available as the failure fallback.
    assert_eq!(
        reloaded.fresh_hit("p", "v1", 1_000 + STALE_MS - 1),
        Some(rows("persisted"))
    );
    assert!(reloaded.fresh_hit("p", "v1", 1_000 + STALE_MS).is_none());
    // A version-key bump invalidates the reloaded entry too.
    assert!(reloaded.last_good("p", "v2").is_none());
    // Corrupt files are ignored, not errors.
    std::fs::write(&path, b"not json").unwrap();
    let corrupt = ModelCatalogCache::new(Some(path));
    assert!(corrupt.last_good("p", "v1").is_none());
}

#[test]
fn persistence_roundtrips_model_metadata_without_transform() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(MODELS_CACHE_FILE);
    let cache = ModelCatalogCache::new(Some(path.clone()));
    let models = vec![json!({
        "id": "legacy", "name": "Legacy", "provider": "auggie",
        "isLegacyModel": true
    })];
    cache.store("auggie", AUGGIE_CATALOG_VERSION, models.clone(), 1_000);

    let reloaded = ModelCatalogCache::new(Some(path));
    assert_eq!(
        reloaded.last_good("auggie", AUGGIE_CATALOG_VERSION),
        Some(models)
    );
}

#[test]
fn persistence_load_drops_stale_default_pseudo_row() {
    // A snapshot persisted by a daemon predating the parse-time pseudo-row
    // resolution stays valid under the same version key (the adapter pin), so
    // the load path must drop the pseudo-row when real rows exist next to it.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(MODELS_CACHE_FILE);
    let cache = ModelCatalogCache::new(Some(path.clone()));
    let stale = vec![
        json!({ "id": "default", "name": "Default (recommended)", "provider": "claude-code",
                "description": "Opus 5 with 1M context · Best for everyday, complex tasks" }),
        json!({ "id": "opus[1m]", "name": "Opus (1M context)", "provider": "claude-code",
                "description": "Opus 5 with 1M context · Best for everyday, complex tasks" }),
        json!({ "id": "sonnet", "name": "Sonnet", "provider": "claude-code" }),
    ];
    cache.store("claude-code", "pin@1", stale, 1_000);

    let reloaded = ModelCatalogCache::new(Some(path.clone()));
    let served = reloaded.last_good("claude-code", "pin@1").unwrap();
    let ids: Vec<&str> = served.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(ids, ["opus[1m]", "sonnet"]);

    // A sole pseudo-row is kept — the load never empties a catalog (D1).
    let cache = ModelCatalogCache::new(Some(path.clone()));
    let only_default = vec![
        json!({ "id": "default", "name": "Default (recommended)", "provider": "claude-code" }),
    ];
    cache.store("claude-code", "pin@1", only_default.clone(), 2_000);
    let reloaded = ModelCatalogCache::new(Some(path.clone()));
    assert_eq!(
        reloaded.last_good("claude-code", "pin@1"),
        Some(only_default)
    );

    // A list with no real rows (even several pseudo-rows) is left untouched —
    // retain never produces a stored-empty entry.
    let cache = ModelCatalogCache::new(Some(path.clone()));
    let all_pseudo = vec![
        json!({ "id": "default", "name": "Default", "provider": "claude-code" }),
        json!({ "id": "DEFAULT", "name": "Default (dup)", "provider": "claude-code" }),
    ];
    cache.store("claude-code", "pin@1", all_pseudo.clone(), 3_000);
    let reloaded = ModelCatalogCache::new(Some(path.clone()));
    assert_eq!(reloaded.last_good("claude-code", "pin@1"), Some(all_pseudo));

    // Sanitization is scoped to the pseudo-row-resolving providers: for any
    // other provider a `default` id is a legitimate model and survives load.
    let cache = ModelCatalogCache::new(Some(path.clone()));
    let opencode = vec![
        json!({ "id": "default", "name": "Default", "provider": "opencode" }),
        json!({ "id": "gpt-6", "name": "GPT-6", "provider": "opencode" }),
    ];
    cache.store("opencode", "pin@1", opencode.clone(), 4_000);
    let reloaded = ModelCatalogCache::new(Some(path));
    assert_eq!(reloaded.last_good("opencode", "pin@1"), Some(opencode));
}

/// cortex is hidden by default (gated on `INTENTD_ENABLE_CORTEX`, unset in
/// the test environment): its source serves an empty list with a gating
/// warning naming the env var — the default-deny shape.
#[tokio::test]
async fn cortex_source_serves_gated_empty_list_by_default() {
    let r = (source_for("cortex").unwrap().fetch)().await;
    assert_eq!(r.models, Some(Vec::new()));
    let warning = r.warning.expect("gated cortex carries a warning");
    assert!(
        warning.contains("INTENTD_ENABLE_CORTEX"),
        "gate warning names the env var: {warning}"
    );
}

/// droid is hidden by default (gated on `INTENTD_ENABLE_DROID`, unset in the
/// test environment): its source serves an empty list with a gating warning
/// without probing the droid binary.
#[tokio::test]
async fn droid_source_serves_gated_empty_list_by_default() {
    let r = (source_for("droid").unwrap().fetch)().await;
    assert_eq!(r.models, Some(Vec::new()));
    let warning = r.warning.expect("gated droid carries a warning");
    assert!(
        warning.contains("INTENTD_ENABLE_DROID"),
        "gate warning names the env var: {warning}"
    );
}

#[test]
fn registry_covers_all_nine_providers() {
    for provider in [
        "auggie",
        "cortex",
        "claude-code",
        "codex",
        "pi",
        "droid",
        "opencode",
        "grok",
        "unsloth",
    ] {
        assert!(source_for(provider).is_some(), "{provider} not registered");
    }
    assert!(source_for("no-such-provider").is_none());
}

#[test]
fn registry_version_keys_follow_adapter_pins() {
    let key = |provider: &str| (source_for(provider).unwrap().version_key)();
    assert_eq!(key("auggie"), AUGGIE_CATALOG_VERSION);
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
    assert_eq!(key("unsloth"), "");
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
    // `from_provider_fetch` must flow into the cache's stale fallback. Forced
    // read: a non-forced one would serve the cached entry without probing.
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("droid", "", rows("droid-last-good"), 1_000);
    let failed = || -> BoxFuture<'static, ModelFetchResult> {
        Box::pin(async {
            from_provider_fetch(crate::provider_models::ProviderModelsFetch {
                models: None,
                warning: Some("droid: droid binary not found".to_string()),
            })
        })
    };
    let r = resolve_with_cache(&cache, "droid", "", true, 1_001, failed).await;
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
    let cache = Arc::new(ModelCatalogCache::new(None));
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
    let cache = Arc::new(ModelCatalogCache::new(None));
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
async fn cache_hit_ignores_negative_window() {
    // A cached entry under the current version key is a hit — no probe would
    // run — so a fresh negative entry (e.g. a failed forced probe moments
    // ago) never degrades a non-forced read to the stale fallback.
    let cache = Arc::new(ModelCatalogCache::new(None));
    cache.store("p", "v1", rows("last-good"), 1_000);
    let r = resolve_with_cache(&cache, "p", "v1", true, 1_001, failing_fetch()).await;
    assert_eq!(r.models, Some(rows("last-good")));
    assert!(r.stale);
    // Within the negative window: the entry is served plainly, no re-probe.
    let r = resolve_with_cache(
        &cache,
        "p",
        "v1",
        false,
        1_001 + NEG_TTL_MS - 1,
        panicking_fetch(),
    )
    .await;
    assert_eq!(r.models, Some(rows("last-good")));
    assert!(!r.stale);
    assert!(r.warning.is_none());
}

#[tokio::test]
async fn negative_entry_expires_and_reprobe_succeeds() {
    let cache = Arc::new(ModelCatalogCache::new(None));
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
    let cache = Arc::new(ModelCatalogCache::new(None));
    let r = resolve_with_cache(&cache, "p", "v1", false, 1_000, failing_fetch()).await;
    assert!(r.models.is_none());
    // Within the window a forced read still probes — and its success clears
    // the negative entry for subsequent non-forced reads.
    let r = resolve_with_cache(&cache, "p", "v1", true, 1_001, ok_fetch("forced")).await;
    assert_eq!(r.models, Some(rows("forced")));
    assert_eq!(cache.last_good("p", "v1"), Some(rows("forced")));
}

#[tokio::test]
async fn only_the_leader_records_the_probe_outcome() {
    // Followers must not re-record the shared result: a late-scheduled
    // follower of a failed probe would otherwise re-arm the negative window
    // after a newer forced probe succeeded and cleared it. Recording lives
    // inside the get_or_init initializer, so a pre-resolved cell (follower's
    // view) must leave the caches untouched.
    let cache = Arc::new(ModelCatalogCache::new(None));
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
    assert_eq!(cache.last_good("p", "v1"), Some(rows("recovered")));
}

#[tokio::test]
async fn negative_entry_is_version_key_scoped() {
    let cache = Arc::new(ModelCatalogCache::new(None));
    let r = resolve_with_cache(&cache, "p", "v1", false, 1_000, failing_fetch()).await;
    assert!(r.models.is_none());
    // A version-key bump must not be blocked by the old key's failure.
    let r = resolve_with_cache(&cache, "p", "v2", false, 1_001, ok_fetch("bumped")).await;
    assert_eq!(r.models, Some(rows("bumped")));
}

// --- cached-catalog ownership evidence (monorepo#607) ---
// These use real registry provider ids (`source_for` gates the lookup):
// auggie uses a wire-shape version; grok remains version-pin-free.

#[test]
fn cached_catalog_claims_matches_bare_and_compound_row_ids() {
    let cache = ModelCatalogCache::new(None);
    cache.store(
        "auggie",
        AUGGIE_CATALOG_VERSION,
        vec![
            json!({ "id": "fable-5", "name": "Fable 5", "provider": "auggie" }),
            json!({ "id": "auggie:fable-6", "name": "Fable 6", "provider": "auggie" }),
            json!({ "id": "grok:foreign-model", "name": "Foreign", "provider": "grok" }),
        ],
        1_000,
    );
    // Exact bare id and the bare part of a self-prefixed compound row id
    // both claim.
    assert_eq!(cache.cached_catalog_claims("auggie", "fable-5"), Some(true));
    assert_eq!(cache.cached_catalog_claims("auggie", "fable-6"), Some(true));
    // A foreign-prefixed row id is not an ownership claim.
    assert_eq!(
        cache.cached_catalog_claims("auggie", "foreign-model"),
        Some(false)
    );
    // Present catalog without the id is affirmative disproof.
    assert_eq!(
        cache.cached_catalog_claims("auggie", "grok-4-fast"),
        Some(false)
    );
    // Prefix/substring must not match.
    assert_eq!(cache.cached_catalog_claims("auggie", "fable"), Some(false));
}

#[test]
fn cached_effort_levels_reads_matching_row() {
    let cache = ModelCatalogCache::new(None);
    cache.store(
        "auggie",
        AUGGIE_CATALOG_VERSION,
        vec![
            json!({ "id": "fable-5", "name": "Fable 5", "provider": "auggie",
                    "effortLevels": ["low", "high"] }),
            json!({ "id": "auggie:fable-6", "name": "Fable 6", "provider": "auggie" }),
        ],
        1_000,
    );
    assert_eq!(
        cache.cached_effort_levels("fable-5"),
        Some(vec!["low".to_string(), "high".to_string()])
    );
    // A compound id scopes the search to its provider and matches the bare part.
    assert_eq!(
        cache.cached_effort_levels("auggie:fable-5"),
        Some(vec!["low".to_string(), "high".to_string()])
    );
    // A row that declares no levels, an unknown id, and a foreign-scoped
    // compound id all carry no evidence.
    assert_eq!(cache.cached_effort_levels("fable-6"), None);
    assert_eq!(cache.cached_effort_levels("unknown-model"), None);
    assert_eq!(cache.cached_effort_levels("grok:fable-5"), None);
}

#[test]
fn cached_catalog_claims_none_without_usable_entry() {
    let cache = ModelCatalogCache::new(None);
    // No entry at all → no evidence.
    assert_eq!(cache.cached_catalog_claims("grok", "fable-5"), None);
    // Unregistered provider id → no evidence.
    cache.store("not-a-provider", "", rows("fable-5"), 1_000);
    assert_eq!(
        cache.cached_catalog_claims("not-a-provider", "fable-5"),
        None
    );
    // Version-key mismatch (stale pin) → the entry is not evidence.
    cache.store(
        "codex",
        "stale-pin",
        vec![json!({ "id": "fable-5", "name": "x", "provider": "codex" })],
        1_000,
    );
    assert_eq!(cache.cached_catalog_claims("codex", "fable-5"), None);
}

#[test]
fn cached_default_model_reads_is_default_row() {
    let cache = ModelCatalogCache::new(None);
    cache.store(
        "auggie",
        AUGGIE_CATALOG_VERSION,
        vec![
            json!({ "id": "fable-5", "name": "Fable 5", "provider": "auggie" }),
            json!({ "id": "sonnet5", "name": "Sonnet 5", "provider": "auggie",
                    "isDefault": true }),
        ],
        1_000,
    );
    assert_eq!(
        cache.cached_default_model("auggie"),
        Some("sonnet5".to_string())
    );
}

#[test]
fn cached_default_model_none_without_usable_entry() {
    let cache = ModelCatalogCache::new(None);
    // No entry at all → None.
    assert_eq!(cache.cached_default_model("auggie"), None);
    // Catalog without an isDefault row (including an explicit false) → None.
    cache.store(
        "auggie",
        AUGGIE_CATALOG_VERSION,
        vec![
            json!({ "id": "fable-5", "name": "Fable 5", "provider": "auggie" }),
            json!({ "id": "sonnet5", "name": "Sonnet 5", "provider": "auggie",
                    "isDefault": false }),
        ],
        1_000,
    );
    assert_eq!(cache.cached_default_model("auggie"), None);
    // Unregistered provider id → None.
    cache.store(
        "not-a-provider",
        "",
        vec![
            json!({ "id": "m", "name": "M", "provider": "not-a-provider",
                     "isDefault": true }),
        ],
        1_000,
    );
    assert_eq!(cache.cached_default_model("not-a-provider"), None);
    // Version-key mismatch (stale pin) → the entry is not usable.
    cache.store(
        "codex",
        "stale-pin",
        vec![json!({ "id": "gpt-5", "name": "GPT-5", "provider": "codex",
                     "isDefault": true })],
        1_000,
    );
    assert_eq!(cache.cached_default_model("codex"), None);
}

#[test]
fn providers_claiming_model_cached_walks_registry() {
    let cache = ModelCatalogCache::new(None);
    assert!(cache.providers_claiming_model_cached("fable-5").is_empty());
    cache.store(
        "auggie",
        AUGGIE_CATALOG_VERSION,
        vec![json!({ "id": "fable-5", "name": "Fable 5", "provider": "auggie" })],
        1_000,
    );
    cache.store(
        "grok",
        "",
        vec![json!({ "id": "grok-4-fast", "name": "Grok", "provider": "grok" })],
        1_000,
    );
    assert_eq!(
        cache.providers_claiming_model_cached("fable-5"),
        vec!["auggie".to_string()]
    );
    assert_eq!(
        cache.providers_claiming_model_cached("grok-4-fast"),
        vec!["grok".to_string()]
    );
}
