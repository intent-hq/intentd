//! Generic per-provider model-catalog cache + source registry (PROTOCOL §5.30).
//!
//! One cache implementation serves every provider: entries are keyed by
//! provider id and carry a version key (e.g. a pinned ACP adapter version) so
//! a pin bump invalidates the cached list automatically. Successful non-empty
//! fetches are persisted in the daemon data dir and served **indefinitely** —
//! there is no age-based expiry (empty-but-successful results are served but
//! not cached). A probe runs only on a true cache miss (no entry for the
//! provider under its current version key) or on a `force_refresh` read;
//! either falls back to the last-good list — labeled with a `warning` — only
//! when the probe fails.
//!
//! Probes are bounded two ways so a broken adapter cannot be re-spawned on
//! every `models.list` call: concurrent fetches for one (provider, version
//! key) are **single-flighted** (one probe runs, everyone shares its result),
//! and a failed probe is **negatively cached** for [`MODELS_NEGATIVE_TTL`] —
//! within that window a non-forced miss reports nothing to serve without
//! re-probing (a matching last-good entry would have been a plain cache hit
//! before the negative window is even consulted). `force_refresh` bypasses
//! the negative entry but still single-flights.
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

/// cortex source: registry-gated catalog. The gate is open by default —
/// cortex's config demands no env var or feature code (un-gated,
/// monorepo#1902) — but the check stays wired through
/// [`intent_providers::gated_reason`] (the single gate shared with
/// `providers.catalog` and discovery's `gatedOff`) so a future gating field
/// re-closes it; closed means an empty list + warning (mirroring the
/// reference FE's default-deny). Cortex has no dynamic model discovery (and
/// the static tier catalog is retired), so an open gate also serves an empty
/// list — the provider CLI owns model selection.
fn cortex_fetch() -> BoxFuture<'static, ModelFetchResult> {
    Box::pin(async {
        // A missing provider config is treated as gated (explicit default-deny),
        // not as an open gate.
        let gated = match intent_providers::find_provider("cortex") {
            None => Some("provider config missing".to_string()),
            Some(cfg) => intent_providers::gated_reason(cfg),
        };
        ModelFetchResult {
            models: Some(Vec::new()),
            warning: gated.map(|reason| format!("Cortex not available ({reason})")),
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
    intent_providers::PI_ACP_NPX_PACKAGE.to_string()
}

/// droid source: ACP probe via a resolved `droid` binary (no adapter pin).
fn droid_fetch() -> BoxFuture<'static, ModelFetchResult> {
    provider_models_fetch("droid")
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
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }

    /// The last successfully fetched rows for `provider_id` regardless of
    /// age, provided they were fetched under `version_key` — a version-pin
    /// bump invalidates automatically. This is the cache-hit read: entries
    /// never expire by age, so it doubles as the failure fallback source.
    fn last_good(&self, provider_id: &str, version_key: &str) -> Option<Vec<Value>> {
        let entries = self.entries.lock().expect("model catalog cache poisoned");
        let entry = entries.get(provider_id)?;
        (entry.version_key == version_key).then(|| entry.models.clone())
    }

    /// Cached-catalog ownership evidence for one provider (monorepo#607):
    /// `Some(claims)` when `provider_id` holds an in-memory last-good entry
    /// under its **current** registry version key ([`source_for`]), `None`
    /// when there is no usable entry (unregistered provider, no entry, or a
    /// version-key mismatch — stale-pin entries are not evidence).
    /// Synchronous and read-only: no probe, no negative-cache interaction —
    /// the bare-model ownership guard must never block on or trigger a fetch.
    /// Trade-off of serving entries regardless of age: a `Some(false)`
    /// disproof can outlive the provider's real catalog (e.g. a model added
    /// upstream after the last fetch keeps being rejected until a forced
    /// `models.list` refresh replaces the entry); a compound `provider:model`
    /// id always bypasses the bare-model guard, so callers are never wedged.
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
    /// `model_id` may be compound (`provider:model`, restricting the search to
    /// that provider) or bare (searched across every registered provider in
    /// registry order). Returns the first non-empty `effortLevels` list found
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
            for entry in entries.values_mut() {
                drop_stale_pseudo_row(&mut entry.models);
            }
            entries
        })
}

/// Drop the `default` pseudo-row from a persisted row list when at least one
/// real row exists (mirrors the parse-time rule in
/// [`crate::provider_models::is_default_pseudo_row`]'s caller): a sole
/// pseudo-row is kept so a catalog never loads empty because of this.
fn drop_stale_pseudo_row(models: &mut Vec<Value>) {
    if models.len() > 1 {
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
/// non-forced reads serve a cached entry indefinitely — regardless of age —
/// so a probe runs only on a true miss (no entry under the current version
/// key) or a forced read; a fresh negative entry (recent probe failure)
/// short-circuits a miss to the failure fallback without re-probing
/// (`force_refresh` skips both cache reads). Concurrent probes for one
/// (provider, version key) are single-flighted — one fetch runs, everyone
/// shares its result. A successful non-empty probe is stored (empty successes
/// are served but not cached, so they never masquerade as a last-good list)
/// and any success clears the negative entry; a failed probe records a
/// negative entry and falls back to the last-good list labeled `stale` +
/// `warning`, or reports nothing to serve.
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
        if let Some(models) = cache.last_good(provider_id, version_key) {
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
