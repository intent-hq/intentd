//! Unit tests for the generic per-provider model cache (PROTOCOL §5.30):
//! TTL, version-key invalidation, forceRefresh bypass, failure fallback,
//! and persistence.

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

const TTL_MS: u64 = super::MODELS_CACHE_TTL.as_millis() as u64;

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
fn registry_lists_only_todays_sources() {
    assert!(source_for("auggie").is_some());
    assert!(source_for("cortex").is_some());
    assert!(source_for("no-such-provider").is_none());
}
