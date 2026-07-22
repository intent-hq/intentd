//! Generic per-provider model-catalog cache + source registry (PROTOCOL §5.30).
//!
//! One cache implementation serves every provider: entries are keyed by
//! provider id and carry a version key (e.g. a pinned ACP adapter version) so
//! a pin bump invalidates the cached list automatically. Successful non-empty
//! fetches are persisted in the daemon data dir and stay fresh for
//! [`crate::agent_ops::MODELS_CACHE_TTL`] (empty-but-successful results are
//! served but not cached); expired reads await a fresh probe (no
//! stale-while-revalidate) and fall back to the last-good list — labeled
//! with a `warning` — only when the probe fails.
//!
//! Probes are bounded two ways so a broken adapter cannot be re-spawned on
//! every `models.list` call: concurrent fetches for one (provider, version
//! key) are **single-flighted** (one probe runs, everyone shares its result),
//! and a failed probe is **negatively cached** for [`MODELS_NEGATIVE_TTL`] —
//! within that window non-forced reads serve the last-good list (labeled
//! `stale` + `warning`) or report nothing to serve, without re-probing.
//! `force_refresh` bypasses the negative entry but still single-flights.
//!
//! The registry lists every provider with a daemon-side model source: auggie
//! (rich CLI fetch), cortex (feature-code-gated static catalog), the
//! ACP-probe sources (claude-code/codex/pi/droid), and opencode (native CLI)
//! via [`crate::provider_models`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use intent_core::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::OnceCell;

use crate::agent_ops::MODELS_CACHE_TTL;

/// File name of the persisted cache inside the daemon data dir.
pub(crate) const MODELS_CACHE_FILE: &str = "models-cache.json";

/// How long a failed probe suppresses re-fetching (negative cache TTL): the
/// probe spawns an adapter/CLI, so a persistent failure must not be retried
/// on every `models.list` call (PROTOCOL §5.30).
pub(crate) const MODELS_NEGATIVE_TTL: std::time::Duration = std::time::Duration::from_secs(60);

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

/// auggie source: the rich CLI fetch already backing `models.list`
/// (discovery via `resolve_auggie_bin` — registry sources are plain fns with
/// no `Services` handle, so the `auggie_bin` test seam is unavailable here).
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

/// cortex source: feature-code-gated static catalog. The gate is closed
/// whenever the provider config demands an env var that is unset or a feature
/// code (the daemon stores no feature-code enablement, so a configured code
/// always gates — today's cortex config always takes this path); closed means
/// an empty list + warning (mirroring the reference FE's default-deny) so the
/// tier catalog is never leaked past the gate. The static-rows branch below
/// only serves once the provider config stops requiring a code.
fn cortex_fetch() -> BoxFuture<'static, ModelFetchResult> {
    Box::pin(async {
        // A missing provider config is treated as gated (explicit default-deny),
        // not as an open gate.
        let gated = match intent_providers::find_provider("cortex") {
            None => Some("provider config missing".to_string()),
            Some(cfg) => {
                if let Some(var) = cfg
                    .requires_env_var
                    .filter(|v| std::env::var_os(v).is_none())
                {
                    Some(format!("requires env var {var}"))
                } else {
                    cfg.requires_feature_code
                        .map(|code| format!("requires feature code {code}"))
                }
            }
        };
        match gated {
            Some(reason) => ModelFetchResult {
                models: Some(Vec::new()),
                warning: Some(format!("Cortex not available ({reason})")),
            },
            None => ModelFetchResult {
                models: Some(crate::agent_ops::static_models_for("cortex")),
                warning: None,
            },
        }
    })
}

/// Adapt a completed [`crate::provider_models`] fetch into the cache's fetch
/// result (same `Option<rows>` + warning semantics, so a probe failure flows
/// into the cache's last-good/stale fallback).
fn from_provider_fetch(fetched: crate::provider_models::ProviderModelsFetch) -> ModelFetchResult {
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
    crate::provider_models::PI_ACP_NPX_PACKAGE.to_string()
}

/// droid source: ACP probe via a resolved `droid` binary (no adapter pin).
fn droid_fetch() -> BoxFuture<'static, ModelFetchResult> {
    provider_models_fetch("droid")
}

/// opencode source: native `opencode models` CLI (no adapter pin).
fn opencode_fetch() -> BoxFuture<'static, ModelFetchResult> {
    provider_models_fetch("opencode")
}

/// The provider→source registry: every provider with a daemon-side model
/// source.
static SOURCES: &[ModelSource] = &[
    ModelSource {
        provider_id: "auggie",
        version_key: no_version,
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
const PERSIST_VERSION: u32 = 1;

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
        }
    }

    /// Current unix time in milliseconds (the cache clock).
    pub(crate) fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// The cached rows when still within TTL **and** fetched under
    /// `version_key` — a version-pin bump invalidates automatically. An entry
    /// fetched in the future (system clock moved backwards) is not fresh, so
    /// a persisted entry can never outlive the TTL through clock adjustments.
    fn fresh(&self, provider_id: &str, version_key: &str, now_ms: u64) -> Option<Vec<Value>> {
        let entries = self.entries.lock().expect("model catalog cache poisoned");
        let entry = entries.get(provider_id)?;
        if entry.version_key != version_key || now_ms < entry.fetched_at_ms {
            return None;
        }
        let age = now_ms - entry.fetched_at_ms;
        (age < MODELS_CACHE_TTL.as_millis() as u64).then(|| entry.models.clone())
    }

    /// The last successfully fetched rows for `provider_id` regardless of
    /// age, provided they were fetched under `version_key`.
    fn last_good(&self, provider_id: &str, version_key: &str) -> Option<Vec<Value>> {
        let entries = self.entries.lock().expect("model catalog cache poisoned");
        let entry = entries.get(provider_id)?;
        (entry.version_key == version_key).then(|| entry.models.clone())
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
    /// [`MODELS_NEGATIVE_TTL`] **and** was recorded under `version_key`. Same
    /// clock-backwards guard as [`Self::fresh`]: an entry stamped ahead of
    /// `now` is not fresh.
    fn negative_reason(&self, provider_id: &str, version_key: &str, now_ms: u64) -> Option<String> {
        let negative = self.negative.lock().expect("model negative cache poisoned");
        let entry = negative.get(provider_id)?;
        if entry.version_key != version_key || now_ms < entry.failed_at_ms {
            return None;
        }
        let age = now_ms - entry.failed_at_ms;
        (age < MODELS_NEGATIVE_TTL.as_millis() as u64).then(|| entry.reason.clone())
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
    /// [`resolve_with_cache`] so the TTL / negative-window / single-flight
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
}

/// Read the persisted snapshot, discarding unreadable or version-mismatched
/// files.
fn load_persisted(path: &Path) -> Option<HashMap<String, CacheEntry>> {
    let bytes = std::fs::read(path).ok()?;
    let persisted: PersistedCache = serde_json::from_slice(&bytes).ok()?;
    (persisted.version == PERSIST_VERSION).then_some(persisted.entries)
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
/// non-forced reads within TTL serve the cache; a fresh negative entry
/// (recent probe failure) short-circuits to the failure fallback without
/// re-probing; expired or forced reads await the fresh probe (`force_refresh`
/// skips both cache reads). Concurrent probes for one (provider, version key)
/// are single-flighted — one fetch runs, everyone shares its result. A
/// successful non-empty probe is stored (empty successes are served but not
/// cached, so they never masquerade as a last-good list) and any success
/// clears the negative entry; a failed probe records a negative entry and
/// falls back to the last-good list labeled `stale` + `warning`, or reports
/// nothing to serve.
pub(crate) async fn resolve_with_cache<F>(
    cache: &ModelCatalogCache,
    provider_id: &str,
    version_key: &str,
    force_refresh: bool,
    now_ms: u64,
    fetch: F,
) -> ResolvedModels
where
    F: FnOnce() -> BoxFuture<'static, ModelFetchResult>,
{
    if !force_refresh {
        if let Some(models) = cache.fresh(provider_id, version_key, now_ms) {
            return ResolvedModels {
                models: Some(models),
                stale: false,
                warning: None,
            };
        }
        if let Some(reason) = cache.negative_reason(provider_id, version_key, now_ms) {
            return failure_fallback(cache, provider_id, version_key, reason);
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
            match &fetched.models {
                Some(models) => {
                    cache.clear_negative(provider_id);
                    if !models.is_empty() {
                        cache.store(provider_id, version_key, models.clone(), now_ms);
                    }
                }
                None => {
                    let reason = fetched
                        .warning
                        .clone()
                        .unwrap_or_else(|| format!("model discovery for '{provider_id}' failed"));
                    cache.store_negative(provider_id, version_key, reason, now_ms);
                }
            }
            fetched
        })
        .await
        .clone();
    cache.finish_inflight(provider_id, version_key, &cell);
    match fetched.models {
        Some(models) => ResolvedModels {
            models: Some(models),
            stale: false,
            warning: fetched.warning,
        },
        None => {
            let reason = fetched
                .warning
                .unwrap_or_else(|| format!("model discovery for '{provider_id}' failed"));
            failure_fallback(cache, provider_id, version_key, reason)
        }
    }
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

#[cfg(test)]
mod tests;
