//! Generic per-provider model-catalog cache + source registry (PROTOCOL §5.30).
//!
//! One cache implementation serves every provider: entries are keyed by
//! provider id and carry a version key (e.g. a pinned ACP adapter version) so
//! a pin bump invalidates the cached list automatically. Successful fetches
//! are persisted in the daemon data dir and stay fresh for
//! [`crate::agent_ops::MODELS_CACHE_TTL`]; expired reads await a fresh probe
//! (no stale-while-revalidate) and fall back to the last-good list — labeled
//! with a `warning` — only when the probe fails.
//!
//! The registry lists the sources that exist today (auggie via the rich CLI
//! fetch, cortex via its feature-code-gated static catalog); ACP-probe sources
//! (claude-code/codex/pi/droid) and opencode plug in as additional
//! [`ModelSource`] entries.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use intent_core::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::agent_ops::MODELS_CACHE_TTL;

/// File name of the persisted cache inside the daemon data dir.
pub(crate) const MODELS_CACHE_FILE: &str = "models-cache.json";

/// Outcome of one provider model probe: `models: None` means the probe failed
/// (CLI unavailable / nothing parseable) and the caller may fall back;
/// `warning` carries a human-readable reason either way.
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

/// auggie source: the rich CLI fetch already backing `models.list`.
fn auggie_fetch() -> BoxFuture<'static, ModelFetchResult> {
    Box::pin(async {
        match crate::agent_ops::fetch_auggie_models_rich().await {
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

/// The provider→source registry. Only the sources that exist today are
/// listed; follow-up sources register here as they land.
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

/// The generic per-provider model cache: in-memory entries, optionally
/// mirrored to a JSON file in the daemon data dir so a restart keeps the
/// last-good lists. Shared across [`crate::Services`] clones via `Arc`.
pub(crate) struct ModelCatalogCache {
    entries: Mutex<HashMap<String, CacheEntry>>,
    persist_path: Option<PathBuf>,
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
    /// `version_key` — a version-pin bump invalidates automatically.
    fn fresh(&self, provider_id: &str, version_key: &str, now_ms: u64) -> Option<Vec<Value>> {
        let entries = self.entries.lock().expect("model catalog cache poisoned");
        let entry = entries.get(provider_id)?;
        if entry.version_key != version_key {
            return None;
        }
        let age = now_ms.saturating_sub(entry.fetched_at_ms);
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
/// non-forced reads within TTL serve the cache; expired or forced reads await
/// the fresh probe (`force_refresh` skips the cache read entirely); a
/// successful probe is stored; a failed probe falls back to the last-good
/// list labeled `stale` + `warning`, or reports nothing to serve.
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
    }
    let fetched = fetch().await;
    match fetched.models {
        Some(models) => {
            if !models.is_empty() {
                cache.store(provider_id, version_key, models.clone(), now_ms);
            }
            ResolvedModels {
                models: Some(models),
                stale: false,
                warning: fetched.warning,
            }
        }
        None => match cache.last_good(provider_id, version_key) {
            Some(models) => {
                let reason = fetched
                    .warning
                    .unwrap_or_else(|| format!("model discovery for '{provider_id}' failed"));
                ResolvedModels {
                    models: Some(models),
                    stale: true,
                    warning: Some(format!("{reason}; serving last known model list")),
                }
            }
            None => ResolvedModels {
                models: None,
                stale: false,
                warning: fetched.warning,
            },
        },
    }
}

#[cfg(test)]
mod tests;
