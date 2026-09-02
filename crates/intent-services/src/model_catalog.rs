//! Generic per-provider model-catalog cache + source registry (PROTOCOL §5.30).
//!
//! One cache implementation serves every provider: entries are keyed by
//! provider id and carry a version key (e.g. a pinned ACP adapter version) so
//! a pin bump invalidates the cached list automatically. Successful non-empty
//! fetches are persisted in the daemon data dir and served as fresh for
//! [`MODELS_STALE_AFTER`] (empty-but-successful results are served but not
//! cached). A non-forced read never blocks on a probe while there is any
//! last-good list to serve (intent-hq/intent#3874, stale-while-revalidate):
//! a fresh entry is a plain hit, and an **aged** entry (at or past the
//! staleness threshold) is served immediately — labeled `stale` with a
//! `warning` — while a refresh probe runs in the background so newly
//! released provider models appear without a manual refresh. A blocking
//! probe remains only where it is unavoidable: a true cache miss (no entry
//! for the provider under its current version key) and a `force_refresh`
//! read; each falls back to the last-good list — labeled `stale` with a
//! `warning` — only when the probe fails, so an aged entry whose provider
//! is unreachable never degrades below the last-good list.
//!
//! Probes are bounded four ways so a broken adapter cannot be re-spawned on
//! every `models.list` call: concurrent fetches for one (provider, version
//! key) are **single-flighted** (one probe runs; blocking callers share its
//! result, and a stale-serving read neither starts a second probe nor awaits
//! the running one), a failed probe is **negatively cached** for
//! [`MODELS_NEGATIVE_TTL`] — within that window a non-forced read that is
//! not a fresh hit skips the probe and serves what the cache has: the
//! stale-labeled last-good list for an aged entry (a fresh entry is a plain
//! hit before the negative window is even consulted), or nothing to serve on
//! a true miss — a background refresh is bounded by a hard timeout
//! ([`MODELS_BACKGROUND_REFRESH_TIMEOUT`]) that records a negative entry and
//! releases the in-flight slot, so a wedged probe cannot pin it forever, and
//! background refreshes across providers share a daemon-wide concurrency cap
//! ([`MODELS_BACKGROUND_REFRESH_CONCURRENCY`]), so simultaneous stale reads
//! cannot fan out one adapter/CLI spawn per provider at once.
//! `force_refresh` bypasses the negative entry but still single-flights.
//!
//! The registry lists every provider with a daemon-side model source: auggie
//! (rich CLI fetch), cortex (empty catalog — the provider CLI owns model
//! selection), the
//! ACP-probe sources (claude-code/codex/pi/droid), the native-CLI sources
//! (opencode, grok), and the HTTP-fetch source (unsloth) via
//! [`crate::provider_models`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use intent_core::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::OnceCell;

/// File name of the persisted cache inside the daemon data dir.
pub(crate) const MODELS_CACHE_FILE: &str = "models-cache.json";

/// How long a failed probe suppresses re-fetching (negative cache TTL): the
/// probe spawns an adapter/CLI, so a persistent failure must not be retried
/// on every `models.list` call (PROTOCOL §5.30).
pub(crate) const MODELS_NEGATIVE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

/// How long a cached success is served as fresh before a non-forced read
/// re-probes the provider (age-based staleness threshold): without it a
/// cached catalog is served forever and newly released provider models never
/// appear until a manual forced refresh (intent-hq/intent#3682). A failed
/// re-probe never degrades service — the aged entry keeps being served,
/// labeled `stale` + `warning` (PROTOCOL §5.30).
pub(crate) const MODELS_STALE_AFTER: std::time::Duration =
    std::time::Duration::from_secs(24 * 60 * 60);

/// Hard cap on a background stale-while-revalidate refresh probe
/// (intent-hq/intent#3874): on expiry the probe is abandoned (the fetch
/// future is dropped), the failure is negatively cached like any other probe
/// failure, and the in-flight slot is released — a wedged adapter can never
/// pin the slot and block future refreshes. Generous relative to observed
/// probe latency (~1s): the timeout only exists to unwedge, not to race.
pub(crate) const MODELS_BACKGROUND_REFRESH_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(30);

/// Daemon-wide cap on concurrently running background refresh probes — the
/// global concurrency cap the RPC cost contract requires on a rung-2
/// (stale-while-revalidate) refresher (AGENTS.md → Performance). Every
/// (provider, version key) slot reaches the spawn independently, so without
/// this cap simultaneous stale reads across providers would spawn one
/// adapter/CLI subprocess per provider at once. Excess refreshes queue on
/// the semaphore — they are detached and latency-insensitive — and the hard
/// timeout starts only once the fetch actually runs.
pub(crate) const MODELS_BACKGROUND_REFRESH_CONCURRENCY: usize = 2;

/// Outcome of one provider model probe: `models: None` means the probe failed
/// (CLI unavailable / nothing parseable) and the caller may fall back;
/// `warning` carries a human-readable reason either way.
#[derive(Clone)]
pub(crate) struct ModelFetchResult {
    /// The fetched wire `ModelInfo` rows, or `None` on probe failure.
    pub models: Option<Vec<Value>>,
    /// Human-readable degradation reason, when applicable.
    pub warning: Option<String>,
}

/// One registered provider model source: how to probe it and which version
/// key its cache entries are pinned to.
pub(crate) struct ModelSource {
    /// Provider id (`auggie`, `cortex`, …).
    pub provider_id: &'static str,
    /// Cache version key (adapter pin such as `CLAUDE_AGENT_ACP_VERSION`
    /// where applicable; empty when the source has no version pin).
    pub version_key: fn() -> String,
    /// Async probe returning the provider's current model list.
    pub fetch: fn() -> BoxFuture<'static, ModelFetchResult>,
}

/// No version pin: sources whose output does not depend on a pinned adapter.
fn no_version() -> String {
    String::new()
}

/// Auggie catalog wire-shape version. Bump when daemon-side filtering or
/// metadata projection changes so persisted rows from the old shape cannot be
/// served indefinitely by the last-good cache.
pub(crate) const AUGGIE_CATALOG_VERSION: &str = "preserve-legacy-v1";

fn auggie_catalog_version() -> String {
    AUGGIE_CATALOG_VERSION.to_string()
}

/// Auggie registry source. The models.list handler uses its Services-aware
/// fetch so tests and configured binaries keep working; this plain function is
/// retained for registry uniformity while the entry supplies the shared cache
/// version key and remains usable by registry-level consumers.
fn auggie_fetch() -> BoxFuture<'static, ModelFetchResult> {
    Box::pin(async {
        match crate::agent_ops::fetch_auggie_models_rich(None).await {
            Some(models) => ModelFetchResult {
                models: Some(models),
                warning: None,
            },
            None => ModelFetchResult {
                models: None,
                warning: Some("auggie CLI unavailable or returned no models".to_string()),
            },
        }
    })
}

/// Evaluate a provider's registry gate ([`intent_providers::gated_reason`] —
/// the single gate shared with `providers.catalog` and discovery's
/// `gatedOff`). A missing provider config is treated as gated (explicit
/// default-deny), not as an open gate.
fn source_gated_reason(provider_id: &str) -> Option<String> {
    match intent_providers::find_provider(provider_id) {
        None => Some("provider config missing".to_string()),
        Some(cfg) => intent_providers::gated_reason(cfg),
    }
}

/// cortex source: registry-gated catalog. Cortex is gated behind
/// `INTENTD_ENABLE_CORTEX` (hidden by default — not yet well-tested); a
/// closed gate means an empty list + warning (mirroring the reference FE's
/// default-deny). Cortex has no dynamic model discovery (and the static tier
/// catalog is retired), so an open gate also serves an empty list — the
/// provider CLI owns model selection.
fn cortex_fetch() -> BoxFuture<'static, ModelFetchResult> {
    Box::pin(async {
        ModelFetchResult {
            models: Some(Vec::new()),
            warning: source_gated_reason("cortex")
                .map(|reason| format!("Cortex not available ({reason})")),
        }
    })
}

/// Adapt a completed [`crate::provider_models`] fetch into the cache's fetch
/// result (same `Option<rows>` + warning semantics, so a probe failure flows
/// into the cache's last-good/stale fallback).
pub(crate) fn from_provider_fetch(
    fetched: crate::provider_models::ProviderModelsFetch,
) -> ModelFetchResult {
    ModelFetchResult {
        models: fetched.models,
        warning: fetched.warning,
    }
}

/// Probe a [`crate::provider_models`] source through its
/// [`crate::provider_models::fetch_provider_models`] dispatcher — the single
/// provider→fetch mapping, so the registry cannot drift from it.
fn provider_models_fetch(provider_id: &'static str) -> BoxFuture<'static, ModelFetchResult> {
    Box::pin(async move {
        from_provider_fetch(crate::provider_models::fetch_provider_models(provider_id).await)
    })
}

/// claude-code source: ACP probe via the pinned npx adapter.
fn claude_code_fetch() -> BoxFuture<'static, ModelFetchResult> {
    provider_models_fetch("claude-code")
}

/// claude-code cache entries are keyed to the full pinned npx package spec
/// (name + version) so both a version bump and a package rename invalidate
/// them automatically.
fn claude_code_version() -> String {
    intent_providers::CLAUDE_AGENT_ACP_NPX_PACKAGE.to_string()
}

/// codex source: ACP probe via a resolved `codex-acp` binary, else the pinned
/// npx fallback.
fn codex_fetch() -> BoxFuture<'static, ModelFetchResult> {
    provider_models_fetch("codex")
}

/// codex is pinned only when the probe falls back to the npx adapter; a
/// resolved `codex-acp` binary has no pin (mirrors the fetch dispatch in
/// [`crate::provider_models::fetch_codex_models`]). The binary is resolved
/// here and again inside the fetch — intentionally independent: the two
/// resolutions are milliseconds apart, so at worst an install/uninstall
/// mid-request stores one cache entry under the other branch's key, which
/// the next request's key mismatch simply treats as a miss.
fn codex_version() -> String {
    if intent_providers::find_provider_binary("codex", "codex-acp", None).is_some() {
        String::new()
    } else {
        intent_providers::config::CODEX_ACP_NPX_PACKAGE.to_string()
    }
}

/// pi source: ACP probe via the pinned npx adapter.
fn pi_fetch() -> BoxFuture<'static, ModelFetchResult> {
    provider_models_fetch("pi")
}

/// pi cache entries are keyed to the adapter pin (the probe always runs the
/// pinned npx package).
fn pi_version() -> String {
    intent_providers::PI_ACP_NPX_PACKAGE.to_string()
}

/// droid source: ACP probe via a resolved `droid` binary (no adapter pin).
/// Droid is gated behind `INTENTD_ENABLE_DROID` (hidden by default — not yet
/// well-tested): a closed gate serves an empty list + warning without ever
/// probing the binary (empty successes are never cached, so a gated probe
/// cannot masquerade as a last-good list).
fn droid_fetch() -> BoxFuture<'static, ModelFetchResult> {
    Box::pin(async {
        if let Some(reason) = source_gated_reason("droid") {
            return ModelFetchResult {
                models: Some(Vec::new()),
                warning: Some(format!("Droid not available ({reason})")),
            };
        }
        from_provider_fetch(crate::provider_models::fetch_provider_models("droid").await)
    })
}

/// opencode source: native `opencode models` CLI (no adapter pin).
fn opencode_fetch() -> BoxFuture<'static, ModelFetchResult> {
    provider_models_fetch("opencode")
}

/// grok source: native `grok models` CLI (no adapter pin).
fn grok_fetch() -> BoxFuture<'static, ModelFetchResult> {
    provider_models_fetch("grok")
}

/// unsloth source: Hugging Face `unsloth` org GGUF catalog (no adapter pin —
/// the fetch is a plain HTTP call, not a spawned/pinned adapter).
fn unsloth_fetch() -> BoxFuture<'static, ModelFetchResult> {
    provider_models_fetch("unsloth")
}

/// The provider→source registry: every provider with a daemon-side model
/// source.
static SOURCES: &[ModelSource] = &[
    ModelSource {
        provider_id: "antigravity",
        version_key: no_version,
        fetch: || provider_models_fetch("antigravity"),
    },
    ModelSource {
        provider_id: "auggie",
        version_key: auggie_catalog_version,
        fetch: auggie_fetch,
    },
    ModelSource {
        provider_id: "cortex",
        version_key: no_version,
        fetch: cortex_fetch,
    },
    ModelSource {
        provider_id: "claude-code",
        version_key: claude_code_version,
        fetch: claude_code_fetch,
    },
    ModelSource {
        provider_id: "codex",
        version_key: codex_version,
        fetch: codex_fetch,
    },
    ModelSource {
        provider_id: "pi",
        version_key: pi_version,
        fetch: pi_fetch,
    },
    ModelSource {
        provider_id: "droid",
        version_key: no_version,
        fetch: droid_fetch,
    },
    ModelSource {
        provider_id: "opencode",
        version_key: no_version,
        fetch: opencode_fetch,
    },
    ModelSource {
        provider_id: "grok",
        version_key: no_version,
        fetch: grok_fetch,
    },
    ModelSource {
        provider_id: "unsloth",
        version_key: no_version,
        fetch: unsloth_fetch,
    },
];

/// Look up the registered source for a provider id.
pub(crate) fn source_for(provider_id: &str) -> Option<&'static ModelSource> {
    SOURCES.iter().find(|s| s.provider_id == provider_id)
}

/// One persisted cache entry: the version key it was fetched under, the
/// fetch instant (unix millis), and the rows.
#[derive(Clone, Serialize, Deserialize)]
struct CacheEntry {
    #[serde(rename = "versionKey")]
    version_key: String,
    #[serde(rename = "fetchedAtMs")]
    fetched_at_ms: u64,
    models: Vec<Value>,
}

/// The persisted on-disk shape: a schema version plus per-provider entries.
#[derive(Serialize, Deserialize)]
struct PersistedCache {
    version: u32,
    entries: HashMap<String, CacheEntry>,
}

/// Schema version of [`PersistedCache`]; unknown versions are discarded.
const PERSIST_VERSION: u32 = 2;

/// One in-memory negative entry: a probe failure under `version_key` at
/// `failed_at_ms`, with the human-readable `reason` served to callers within
/// the [`MODELS_NEGATIVE_TTL`] window. Never persisted — a daemon restart
/// retries immediately.
struct NegativeEntry {
    version_key: String,
    failed_at_ms: u64,
    reason: String,
}

/// A shared in-flight probe slot: the first caller initializes the cell (runs
/// the fetch), concurrent callers await and clone the same result.
type InflightCell = Arc<OnceCell<ModelFetchResult>>;

/// The generic per-provider model cache: in-memory entries, optionally
/// mirrored to a JSON file in the daemon data dir so a restart keeps the
/// last-good lists. Shared across [`crate::Services`] clones via `Arc`.
pub(crate) struct ModelCatalogCache {
    entries: Mutex<HashMap<String, CacheEntry>>,
    persist_path: Option<PathBuf>,
    /// Per-provider negative entries (probe failures); in-memory only.
    /// Keyed by provider id — mirroring `entries` — because at most one
    /// version key is live per provider at a time (keys come from
    /// process-wide adapter pins); a key bump simply replaces the entry.
    negative: Mutex<HashMap<String, NegativeEntry>>,
    /// In-flight probes keyed by (provider id, version key).
    inflight: Mutex<HashMap<(String, String), InflightCell>>,
    /// Cache-wide permit pool bounding concurrent background refresh probes
    /// ([`MODELS_BACKGROUND_REFRESH_CONCURRENCY`]); the production daemon
    /// holds one cache, so this is the daemon-wide refresher cap the RPC
    /// cost contract requires. Never closed.
    refresh_permits: tokio::sync::Semaphore,
}

impl ModelCatalogCache {
    /// Build the cache, seeding from `persist_path` when it holds a readable
    /// current-version snapshot (corrupt/old files are ignored, not errors).
    pub(crate) fn new(persist_path: Option<PathBuf>) -> Self {
        let entries = persist_path
            .as_deref()
            .and_then(load_persisted)
            .unwrap_or_default();
        Self {
            entries: Mutex::new(entries),
            persist_path,
            negative: Mutex::new(HashMap::new()),
            inflight: Mutex::new(HashMap::new()),
            refresh_permits: tokio::sync::Semaphore::new(MODELS_BACKGROUND_REFRESH_CONCURRENCY),
        }
    }

    /// Current unix time in milliseconds (the cache clock).
    pub(crate) fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }

    /// The last successfully fetched rows for `provider_id` regardless of
    /// age, provided they were fetched under `version_key` — a version-pin
    /// bump invalidates automatically. This is the failure fallback source:
    /// entries never expire by age for fallback purposes (the fresh-serving
    /// read is [`Self::fresh_hit`]).
    fn last_good(&self, provider_id: &str, version_key: &str) -> Option<Vec<Value>> {
        let entries = self.entries.lock().expect("model catalog cache poisoned");
        let entry = entries.get(provider_id)?;
        (entry.version_key == version_key).then(|| entry.models.clone())
    }

    /// The cache-hit read: [`Self::last_good`] restricted to entries younger
    /// than [`MODELS_STALE_AFTER`]. Age is a saturating subtraction, so an
    /// entry stamped ahead of `now` (system clock moved backwards) reads as
    /// age zero — served as fresh until the wall clock catches up.
    fn fresh_hit(&self, provider_id: &str, version_key: &str, now_ms: u64) -> Option<Vec<Value>> {
        let entries = self.entries.lock().expect("model catalog cache poisoned");
        let entry = entries.get(provider_id)?;
        if entry.version_key != version_key {
            return None;
        }
        let age = now_ms.saturating_sub(entry.fetched_at_ms);
        (age < u64::try_from(MODELS_STALE_AFTER.as_millis()).unwrap_or(u64::MAX))
            .then(|| entry.models.clone())
    }

    /// Cached-catalog ownership evidence for one provider (monorepo#607):
    /// `Some(claims)` when `provider_id` holds an in-memory last-good entry
    /// under its **current** registry version key ([`source_for`]), `None`
    /// when there is no usable entry (unregistered provider, no entry, or a
    /// version-key mismatch — stale-pin entries are not evidence).
    /// Synchronous and read-only: no probe, no negative-cache interaction —
    /// the bare-model ownership guard must never block on or trigger a fetch.
    /// Trade-off of consulting entries regardless of age: a `Some(false)`
    /// disproof can outlive the provider's real catalog (e.g. a model added
    /// upstream after the last fetch keeps being rejected until the entry
    /// ages past [`MODELS_STALE_AFTER`] and a `models.list` read — or a
    /// forced refresh — replaces it); a stale entry is repaired by the next
    /// catalog refresh, so callers are never wedged for good.
    pub(crate) fn cached_catalog_claims(&self, provider_id: &str, bare_id: &str) -> Option<bool> {
        let source = source_for(provider_id)?;
        let version_key = (source.version_key)();
        let entries = self.entries.lock().expect("model catalog cache poisoned");
        let entry = entries.get(provider_id)?;
        (entry.version_key == version_key)
            .then(|| rows_claim_model(provider_id, &entry.models, bare_id))
    }

    /// The cached catalog's default-model id for `provider_id` (PROTOCOL
    /// §5.5 "Creation-time default-model resolution", catalog-default rung):
    /// the `id` of the row marked `isDefault: true` in the provider's
    /// last-good entry under its **current** registry version key
    /// ([`source_for`]). Synchronous, read-only, and probe-free like
    /// [`Self::cached_catalog_claims`]: the creation path must never block on
    /// or trigger a fetch — an unregistered provider, cold cache, stale-pin
    /// entry, or a catalog with no marked row all return `None` (the provider
    /// CLI default applies). The registry version-key function runs only when
    /// a cached entry with a marked row exists — for codex it resolves the
    /// `codex-acp` binary (an enhanced-PATH scan whose first call can block on
    /// the login-shell PATH capture), a cost the cold-cache fall-through must
    /// not pay; it also runs outside the cache lock so a slow first capture
    /// never stalls other cache readers.
    pub(crate) fn cached_default_model(&self, provider_id: &str) -> Option<String> {
        let source = source_for(provider_id)?;
        let (entry_version_key, default_id) = {
            let entries = self.entries.lock().expect("model catalog cache poisoned");
            let entry = entries.get(provider_id)?;
            let default_id = entry
                .models
                .iter()
                .find(|row| row.get("isDefault").and_then(Value::as_bool) == Some(true))
                .and_then(|row| row.get("id"))
                .and_then(Value::as_str)
                .map(str::to_string)?;
            (entry.version_key.clone(), default_id)
        };
        (entry_version_key == (source.version_key)()).then_some(default_id)
    }

    /// The cached catalog's default-model id for `provider_id`, falling back
    /// to the FIRST row of the provider's last-good entry when no row is
    /// marked `isDefault` (default-provider self-heal, monorepo#3044). Same
    /// synchronous, read-only, probe-free contract as
    /// [`Self::cached_default_model`]: an unregistered provider, cold cache,
    /// stale-pin entry, or an empty catalog all return `None` (the caller
    /// then persists the provider without a model).
    pub(crate) fn cached_default_or_first_model(&self, provider_id: &str) -> Option<String> {
        if let Some(m) = self.cached_default_model(provider_id) {
            return Some(m);
        }
        let source = source_for(provider_id)?;
        let (entry_version_key, first_id) = {
            let entries = self.entries.lock().expect("model catalog cache poisoned");
            let entry = entries.get(provider_id)?;
            let first_id = entry
                .models
                .first()
                .and_then(|row| row.get("id"))
                .and_then(Value::as_str)
                .map(str::to_string)?;
            (entry.version_key.clone(), first_id)
        };
        (entry_version_key == (source.version_key)()).then_some(first_id)
    }

    /// Cached `effortLevels` evidence for a model id (PROTOCOL §5.30/§5.11).
    /// `model_id` is a bare id (searched across every registered provider in
    /// registry order); a legacy compound `provider:model` value from an old
    /// session row still restricts the search to its named provider.
    /// Returns the first non-empty `effortLevels` list found
    /// on a matching cached row, or `None` when there is no evidence — no
    /// cached entry, no matching row, or a row that declares no levels.
    /// Synchronous, read-only, and probe-free like
    /// [`Self::cached_catalog_claims`]: the delegation effort guard must never
    /// block on a catalog fetch, and absence of evidence is never a rejection.
    pub(crate) fn cached_effort_levels(&self, model_id: &str) -> Option<Vec<String>> {
        let (scoped_provider, bare_id) = match model_id.split_once(':') {
            Some((prefix, bare)) if source_for(prefix).is_some() => (Some(prefix), bare),
            _ => (None, model_id),
        };
        let entries = self.entries.lock().expect("model catalog cache poisoned");
        for source in SOURCES {
            if scoped_provider.is_some_and(|p| p != source.provider_id) {
                continue;
            }
            let Some(entry) = entries.get(source.provider_id) else {
                continue;
            };
            if entry.version_key != (source.version_key)() {
                continue;
            }
            let levels = entry
                .models
                .iter()
                .find(|row| {
                    row.get("id")
                        .and_then(Value::as_str)
                        .is_some_and(|id| row_id_matches(source.provider_id, id, bare_id))
                })
                .and_then(|row| row.get("effortLevels"))
                .and_then(Value::as_array)
                .map(|levels| {
                    levels
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect::<Vec<String>>()
                })
                .filter(|levels| !levels.is_empty());
            if levels.is_some() {
                return levels;
            }
        }
        None
    }

    /// Seed a cache entry from tests in other modules (the real
    /// [`Self::store`] is module-private). No persistence side effects: the
    /// snapshot write is skipped when no `persist_path` is configured.
    #[cfg(test)]
    pub(crate) fn store_for_test(&self, provider_id: &str, version_key: &str, models: Vec<Value>) {
        self.store(provider_id, version_key, models, Self::now_ms());
    }

    /// Provider ids whose cached catalog claims `bare_id` (see
    /// [`Self::cached_catalog_claims`]), in registry order.
    pub(crate) fn providers_claiming_model_cached(&self, bare_id: &str) -> Vec<String> {
        SOURCES
            .iter()
            .filter(|s| self.cached_catalog_claims(s.provider_id, bare_id) == Some(true))
            .map(|s| s.provider_id.to_string())
            .collect()
    }

    /// Record a successful fetch and best-effort persist the snapshot. The
    /// write happens under the entries lock (the file is tiny — a handful of
    /// model rows) so concurrent stores for different providers cannot land
    /// their snapshots on disk out of order; temp-file + rename keeps a crash
    /// mid-write from corrupting the previous snapshot.
    fn store(&self, provider_id: &str, version_key: &str, models: Vec<Value>, now_ms: u64) {
        let mut entries = self.entries.lock().expect("model catalog cache poisoned");
        entries.insert(
            provider_id.to_string(),
            CacheEntry {
                version_key: version_key.to_string(),
                fetched_at_ms: now_ms,
                models,
            },
        );
        if let Some(path) = &self.persist_path {
            let persisted = PersistedCache {
                version: PERSIST_VERSION,
                entries: entries.clone(),
            };
            if let Ok(bytes) = serde_json::to_vec(&persisted) {
                let tmp = path.with_extension("json.tmp");
                if std::fs::write(&tmp, bytes).is_ok() {
                    let _ = std::fs::rename(&tmp, path);
                }
            }
        }
    }

    /// The negative entry's failure reason when it is still within
    /// [`MODELS_NEGATIVE_TTL`] **and** was recorded under `version_key`. An
    /// entry stamped ahead of `now` (system clock moved backwards) is not
    /// fresh, so a negative entry can never outlive its TTL through clock
    /// adjustments.
    fn negative_reason(&self, provider_id: &str, version_key: &str, now_ms: u64) -> Option<String> {
        let negative = self.negative.lock().expect("model negative cache poisoned");
        let entry = negative.get(provider_id)?;
        if entry.version_key != version_key || now_ms < entry.failed_at_ms {
            return None;
        }
        let age = now_ms - entry.failed_at_ms;
        (age < u64::try_from(MODELS_NEGATIVE_TTL.as_millis()).unwrap_or(u64::MAX))
            .then(|| entry.reason.clone())
    }

    /// Record a probe failure so non-forced reads within
    /// [`MODELS_NEGATIVE_TTL`] do not re-spawn the adapter.
    fn store_negative(&self, provider_id: &str, version_key: &str, reason: String, now_ms: u64) {
        self.negative
            .lock()
            .expect("model negative cache poisoned")
            .insert(
                provider_id.to_string(),
                NegativeEntry {
                    version_key: version_key.to_string(),
                    failed_at_ms: now_ms,
                    reason,
                },
            );
    }

    /// Drop the provider's negative entry (any successful probe clears it).
    fn clear_negative(&self, provider_id: &str) {
        self.negative
            .lock()
            .expect("model negative cache poisoned")
            .remove(provider_id);
    }

    /// Join (or create) the in-flight probe slot for `(provider_id,
    /// version_key)`. All concurrent callers get the same cell, so
    /// `get_or_init` runs exactly one fetch and everyone shares its result.
    /// Two coalescing consequences, both accepted: a `force_refresh` caller
    /// may join a probe that started *before* its request, and a caller
    /// landing in the record→[`Self::finish_inflight`] window joins an
    /// already-resolved cell and returns its result without a new probe.
    fn join_inflight(&self, provider_id: &str, version_key: &str) -> InflightCell {
        let mut inflight = self.inflight.lock().expect("model inflight map poisoned");
        inflight
            .entry((provider_id.to_string(), version_key.to_string()))
            .or_default()
            .clone()
    }

    /// Claim the in-flight probe slot for `(provider_id, version_key)` only
    /// when none exists — the stale-while-revalidate entry point
    /// (intent-hq/intent#3874): a stale-serving read must neither start a
    /// second probe nor await the running one, so `None` (a probe is already
    /// in flight) means "do nothing".
    fn try_claim_inflight(&self, provider_id: &str, version_key: &str) -> Option<InflightCell> {
        let mut inflight = self.inflight.lock().expect("model inflight map poisoned");
        match inflight.entry((provider_id.to_string(), version_key.to_string())) {
            std::collections::hash_map::Entry::Occupied(_) => None,
            std::collections::hash_map::Entry::Vacant(slot) => {
                Some(slot.insert(InflightCell::default()).clone())
            }
        }
    }

    /// Release the in-flight slot once its outcome has been recorded in the
    /// success/negative caches. Only removes `cell` itself (`ptr_eq`), so a
    /// late finisher cannot evict a newer probe's slot.
    fn finish_inflight(&self, provider_id: &str, version_key: &str, cell: &InflightCell) {
        let mut inflight = self.inflight.lock().expect("model inflight map poisoned");
        let key = (provider_id.to_string(), version_key.to_string());
        if inflight.get(&key).is_some_and(|cur| Arc::ptr_eq(cur, cell)) {
            inflight.remove(&key);
        }
    }

    /// Test-only cache seeding: production code must go through
    /// [`resolve_with_cache`] so the negative-window / single-flight
    /// invariants hold.
    #[cfg(test)]
    pub(crate) fn test_store(
        &self,
        provider_id: &str,
        version_key: &str,
        models: Vec<Value>,
        now_ms: u64,
    ) {
        self.store(provider_id, version_key, models, now_ms);
    }

    /// Test-only negative-window observation (see [`Self::test_store`]).
    #[cfg(test)]
    pub(crate) fn test_negative_reason(
        &self,
        provider_id: &str,
        version_key: &str,
        now_ms: u64,
    ) -> Option<String> {
        self.negative_reason(provider_id, version_key, now_ms)
    }

    /// Test-only in-flight observation: whether the `(provider_id,
    /// version_key)` probe slot is currently held — lets tests prove a
    /// background refresh released (or still holds) the slot.
    #[cfg(test)]
    pub(crate) fn test_inflight_active(&self, provider_id: &str, version_key: &str) -> bool {
        self.inflight
            .lock()
            .expect("model inflight map poisoned")
            .contains_key(&(provider_id.to_string(), version_key.to_string()))
    }
}

/// Read the persisted snapshot, discarding unreadable or version-mismatched
/// files. Loaded entries are sanitized: an adapter's `default` pseudo-row is
/// dropped when real rows exist next to it — a snapshot persisted by a daemon
/// predating the parse-time pseudo-row resolution stays valid under the same
/// version key (the adapter pin, not the daemon version), so without this the
/// stale row would be served indefinitely.
fn load_persisted(path: &Path) -> Option<HashMap<String, CacheEntry>> {
    let bytes = std::fs::read(path).ok()?;
    let persisted: PersistedCache = serde_json::from_slice(&bytes).ok()?;
    (persisted.version == PERSIST_VERSION)
        .then_some(persisted.entries)
        .map(|mut entries| {
            for (provider_id, entry) in &mut entries {
                if PSEUDO_ROW_RESOLVING_PROVIDERS.contains(&provider_id.as_str()) {
                    drop_stale_pseudo_row(&mut entry.models);
                }
            }
            entries
        })
}

/// Providers whose catalogs go through the shared ACP row parser and its
/// `default` pseudo-row resolution (`parse_acp_models`). Only their persisted
/// entries are sanitized on load — for any other provider a row with id
/// `default` would be a legitimate model the parse path serves as-is.
const PSEUDO_ROW_RESOLVING_PROVIDERS: [&str; 3] = ["claude-code", "pi", "droid"];

/// Drop the `default` pseudo-row(s) from a persisted row list when at least
/// one real (non-pseudo) row exists (mirrors the parse-time rule in
/// [`crate::provider_models::is_default_pseudo_row`]'s caller): a list with
/// no real rows is left untouched so a catalog never loads empty because of
/// this.
fn drop_stale_pseudo_row(models: &mut Vec<Value>) {
    if models
        .iter()
        .any(|row| !crate::provider_models::is_default_pseudo_row(row))
    {
        models.retain(|row| !crate::provider_models::is_default_pseudo_row(row));
    }
}

/// Outcome of [`resolve_with_cache`]: the rows to serve (or `None` when the
/// provider has nothing — probe failed and no last-good list), whether they
/// are stale (last-good served after a failed probe), and any warning.
pub(crate) struct ResolvedModels {
    /// Rows to serve, or `None` when the caller must fall back.
    pub models: Option<Vec<Value>>,
    /// `true` when `models` is the last-good list after a failed probe.
    pub stale: bool,
    /// Human-readable degradation reason, when applicable.
    pub warning: Option<String>,
}

/// The single cache policy every provider goes through (PROTOCOL §5.30):
/// non-forced reads serve a cached entry while it is fresh (younger than
/// [`MODELS_STALE_AFTER`]), and an **aged** entry is served immediately —
/// labeled `stale` + `warning` — while a refresh probe runs in the
/// background (stale-while-revalidate, intent-hq/intent#3874): the read
/// never blocks while any last-good list exists under the current version
/// key. A blocking probe runs only on a true miss (no entry under the
/// current version key) or on a forced read; a fresh negative entry (recent
/// probe failure) short-circuits a miss or aged read to the failure fallback
/// without probing — an aged entry within the negative window keeps being
/// served as the stale-labeled last-good list with no background refresh
/// (`force_refresh` skips both cache reads). Concurrent probes for one
/// (provider, version key) are single-flighted — at most one fetch runs;
/// blocking callers share its result, and a stale-serving read whose slot is
/// already taken simply skips spawning (it neither starts a second probe nor
/// awaits the running one). A background refresh is additionally bounded by
/// [`MODELS_BACKGROUND_REFRESH_TIMEOUT`]: on expiry the fetch future is
/// dropped, the timeout is recorded as a negative entry, and the slot is
/// released. Outcome recording is identical on both paths: a successful
/// non-empty probe is stored (empty successes are served but not cached, so
/// they never masquerade as a last-good list) and any success clears the
/// negative entry; a failed probe records a negative entry and — on the
/// blocking path — falls back to the last-good list labeled `stale` +
/// `warning`, or reports nothing to serve.
///
/// Empty-success edge: a probe that returns an **empty success** stores
/// nothing and clears no ground for negative caching, so while an aged entry
/// sits next to an empty-success source every non-forced read serves the
/// stale list and spawns another background refresh — bounded only by
/// single-flighting, with no negative-window suppression (a blocking miss
/// read serves the empty list inline, uncached, and re-probes likewise).
/// This is acceptable while every registered empty-success source is a cheap
/// env-var check (cortex/droid gating; unsloth converts empty to failure);
/// a future source with a costly probe that can return empty success would
/// need its own guard (e.g. converting empty to failure like unsloth does).
pub(crate) async fn resolve_with_cache<F>(
    cache: &Arc<ModelCatalogCache>,
    provider_id: &str,
    version_key: &str,
    force_refresh: bool,
    now_ms: u64,
    fetch: F,
) -> ResolvedModels
where
    F: FnOnce() -> BoxFuture<'static, ModelFetchResult> + Send + 'static,
{
    if !force_refresh {
        if let Some(models) = cache.fresh_hit(provider_id, version_key, now_ms) {
            return ResolvedModels {
                models: Some(models),
                stale: false,
                warning: None,
            };
        }
        if let Some(reason) = cache.negative_reason(provider_id, version_key, now_ms) {
            return failure_fallback(cache, provider_id, version_key, reason);
        }
        // Stale-while-revalidate: any last-good list under the current
        // version key is served immediately; the refresh runs detached.
        if let Some(models) = cache.last_good(provider_id, version_key) {
            spawn_background_refresh(cache, provider_id, version_key, now_ms, fetch);
            return ResolvedModels {
                models: Some(models),
                stale: true,
                warning: Some(format!(
                    "cached model list for '{provider_id}' is stale; refreshing in the background"
                )),
            };
        }
    }
    let cell = cache.join_inflight(provider_id, version_key);
    // Recording happens inside the initializer, so exactly one waiter — the
    // one whose fetch actually runs — records the outcome, and it does so
    // before the in-flight slot is released. Followers only consume the
    // shared result: a late-scheduled follower can never re-record a stale
    // outcome over a newer probe's caches.
    let fetched = cell
        .get_or_init(|| async {
            let fetched = fetch().await;
            record_probe_outcome(cache, provider_id, version_key, &fetched, now_ms);
            fetched
        })
        .await
        .clone();
    cache.finish_inflight(provider_id, version_key, &cell);
    if let Some(models) = fetched.models {
        ResolvedModels {
            models: Some(models),
            stale: false,
            warning: fetched.warning,
        }
    } else {
        let reason = fetched
            .warning
            .unwrap_or_else(|| format!("model discovery for '{provider_id}' failed"));
        failure_fallback(cache, provider_id, version_key, reason)
    }
}

/// Record one probe outcome in the success/negative caches — the shared
/// tail of the blocking and background paths, so stale-while-revalidate
/// cannot drift from the blocking semantics: a success clears the negative
/// entry and stores non-empty rows; a failure records a negative entry.
fn record_probe_outcome(
    cache: &ModelCatalogCache,
    provider_id: &str,
    version_key: &str,
    fetched: &ModelFetchResult,
    now_ms: u64,
) {
    if let Some(models) = &fetched.models {
        cache.clear_negative(provider_id);
        if !models.is_empty() {
            cache.store(provider_id, version_key, models.clone(), now_ms);
        }
    } else {
        let reason = fetched
            .warning
            .clone()
            .unwrap_or_else(|| format!("model discovery for '{provider_id}' failed"));
        cache.store_negative(provider_id, version_key, reason, now_ms);
    }
}

/// Kick off the detached stale-while-revalidate refresh probe
/// (intent-hq/intent#3874) — a no-op when the (provider, version key) slot
/// is already in flight, so concurrent stale reads spawn at most one probe.
/// The claimed cell is initialized through the same `get_or_init` protocol
/// as the blocking path, so a `force_refresh` caller arriving mid-refresh
/// joins this probe and awaits its result rather than racing a second fetch.
/// The fetch waits for a cache-wide permit
/// ([`MODELS_BACKGROUND_REFRESH_CONCURRENCY`]) before running — the
/// daemon-wide cap on concurrent refresh adapter/CLI spawns — and is then
/// capped at [`MODELS_BACKGROUND_REFRESH_TIMEOUT`]; on expiry the future is
/// dropped, the timeout is negatively cached (like any probe failure, so
/// stale reads within [`MODELS_NEGATIVE_TTL`] do not re-spawn a wedged
/// adapter), and the slot is released either way. Outcomes are stamped at
/// **completion** time (`now_ms` + elapsed on the tokio clock), so the
/// negative window always runs its full TTL from when the failure was
/// observed — a probe that spends 30s timing out must not burn 30s of the
/// 60s window before it even starts.
fn spawn_background_refresh<F>(
    cache: &Arc<ModelCatalogCache>,
    provider_id: &str,
    version_key: &str,
    now_ms: u64,
    fetch: F,
) where
    F: FnOnce() -> BoxFuture<'static, ModelFetchResult> + Send + 'static,
{
    let Some(cell) = cache.try_claim_inflight(provider_id, version_key) else {
        return;
    };
    let cache = Arc::clone(cache);
    let provider_id = provider_id.to_string();
    let version_key = version_key.to_string();
    tokio::spawn(async move {
        let started = tokio::time::Instant::now();
        let outcome_ms = |started: tokio::time::Instant| {
            now_ms.saturating_add(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX))
        };
        cell.get_or_init(|| async {
            let _permit = cache
                .refresh_permits
                .acquire()
                .await
                .expect("refresh semaphore is never closed");
            let fetched = tokio::time::timeout(MODELS_BACKGROUND_REFRESH_TIMEOUT, fetch())
                .await
                .unwrap_or_else(|_| ModelFetchResult {
                    models: None,
                    warning: Some(format!(
                        "model discovery for '{provider_id}' timed out after {}s",
                        MODELS_BACKGROUND_REFRESH_TIMEOUT.as_secs()
                    )),
                });
            record_probe_outcome(
                &cache,
                &provider_id,
                &version_key,
                &fetched,
                outcome_ms(started),
            );
            fetched
        })
        .await;
        cache.finish_inflight(&provider_id, &version_key, &cell);
    });
}

/// What a failed (or negatively cached) probe serves: the last-good list
/// labeled `stale` + `warning`, or nothing (the caller falls back to its
/// static catalog).
fn failure_fallback(
    cache: &ModelCatalogCache,
    provider_id: &str,
    version_key: &str,
    reason: String,
) -> ResolvedModels {
    match cache.last_good(provider_id, version_key) {
        Some(models) => ResolvedModels {
            models: Some(models),
            stale: true,
            warning: Some(format!("{reason}; serving last known model list")),
        },
        None => ResolvedModels {
            models: None,
            stale: false,
            warning: Some(reason),
        },
    }
}

/// Whether any cached wire row (`{ id, name, provider }`, §5.30) in
/// `provider_id`'s catalog names `bare_id`. Row ids are matched exactly,
/// tolerating a compound `provider:model` id by comparing its bare part —
/// but only when the prefix names the owning provider itself: a foreign-
/// prefixed row (`other:foo` in this provider's catalog) is not an ownership
/// claim (monorepo#607).
fn rows_claim_model(provider_id: &str, rows: &[Value], bare_id: &str) -> bool {
    rows.iter()
        .filter_map(|row| row.get("id").and_then(Value::as_str))
        .any(|id| row_id_matches(provider_id, id, bare_id))
}

/// Whether one cached row id (`row_id`, from `provider_id`'s catalog) names
/// `bare_id` under [`rows_claim_model`]'s matching rules.
fn row_id_matches(provider_id: &str, row_id: &str, bare_id: &str) -> bool {
    row_id == bare_id
        || row_id
            .split_once(':')
            .is_some_and(|(prefix, bare)| prefix == provider_id && bare == bare_id)
}

#[cfg(test)]
mod tests;
