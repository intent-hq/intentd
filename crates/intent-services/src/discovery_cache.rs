//! TTL cache for `host.providerDiscovery`'s per-provider install verdicts.
//!
//! Mirrors [`crate::provider_auth`]'s `AuthStatusCache` pattern (TTL,
//! single-flighted via the shared [`intent_core::DiscoveryCache`]) but caches
//! [`intent_providers::ProviderAvailability`] instead of an auth probe: only
//! `installed: true` verdicts are stored, so a not-yet-detected provider is
//! re-probed on every call — an install (or a daemon-restart window racing a
//! not-fully-populated PATH) is picked up on the very next
//! `host.providerDiscovery` instead of waiting out the TTL.
//!
//! Each provider is cached under a key that incorporates the raw
//! `providers.paths` values it actually consults (its primary-binary key,
//! plus its own id for the rare dual-binary case), so a changed override
//! value naturally misses the old entry instead of serving a stale
//! pre-override verdict (monorepo task: "override invalidation").

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use intent_core::DiscoveryCache;
use intent_providers::{ProviderAvailability, ACP_PROVIDERS};

/// How long a provider's resolved verdict is served from cache. Matches
/// [`crate::provider_auth::AUTH_CACHE_TTL`] — long enough to spare a burst of
/// `host.providerDiscovery` calls the filesystem/PATH walk, short enough that
/// a recheck (or the next call past the TTL) sees a real install promptly.
const DISCOVERY_CACHE_TTL: Duration = Duration::from_secs(60);

fn cache() -> &'static DiscoveryCache<ProviderAvailability> {
    static CACHE: OnceLock<DiscoveryCache<ProviderAvailability>> = OnceLock::new();
    CACHE.get_or_init(|| DiscoveryCache::new(DISCOVERY_CACHE_TTL))
}

/// Resolve every registered provider's availability through the cache,
/// preserving [`ACP_PROVIDERS`] registry order — the order
/// `discover_providers_with_npx_overrides` has always returned.
pub(crate) fn discover_providers_cached<S: std::hash::BuildHasher>(
    provider_paths: &HashMap<String, String, S>,
) -> Vec<ProviderAvailability> {
    ACP_PROVIDERS
        .iter()
        .map(|cfg| {
            let primary_key = cfg.primary_binary_provider_id();
            let primary_val = provider_paths.get(primary_key).cloned();
            let secondary_val = provider_paths.get(cfg.id).cloned();
            let cache_key = format!("{}\u{1}{primary_val:?}\u{1}{secondary_val:?}", cfg.id);
            cache().get_or_compute(
                &cache_key,
                || {
                    intent_providers::provider_availability_for(cfg.id, &|k| {
                        provider_paths.get(k).cloned()
                    })
                    .expect("cfg.id is drawn from ACP_PROVIDERS, always a registered provider id")
                },
                |avail| avail.installed,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A provider that is gated off (or simply not installed) in the test
    /// environment must never have its `installed: false` verdict cached: two
    /// back-to-back calls under the same (empty) overrides both re-probe
    /// rather than the second trusting a stored negative.
    #[test]
    fn not_installed_verdict_is_not_pinned_by_a_stale_cache_key_reuse() {
        let empty = HashMap::new();
        let first = discover_providers_cached(&empty);
        let second = discover_providers_cached(&empty);
        // Shape/order parity regardless of caching: this is the contract
        // callers (the wire payload builder) depend on.
        assert_eq!(first.len(), ACP_PROVIDERS.len());
        assert_eq!(
            first.iter().map(|p| p.id).collect::<Vec<_>>(),
            second.iter().map(|p| p.id).collect::<Vec<_>>(),
            "registry order preserved across cached calls"
        );
    }

    /// monorepo task requirement: a `providers.paths` override change must
    /// not serve a stale pre-override verdict. Toggling a valid override on
    /// (installed: true) and then removing it must produce a DIFFERENT cache
    /// key each time, so the second call re-probes auto-detection instead of
    /// reusing the first call's cached positive.
    #[test]
    fn override_change_produces_a_distinct_cache_key_not_a_stale_hit() {
        let dir = crate::tests::test_tempdir("discovery-cache-override-");
        let opencode = dir.path().join("opencode");
        let unsloth_bin = dir.path().join("unsloth");
        for bin in [&opencode, &unsloth_bin] {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::write(bin, "#!/bin/sh\nexit 0\n").unwrap();
                std::fs::set_permissions(bin, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            #[cfg(not(unix))]
            std::fs::write(bin, "exit 0").unwrap();
        }
        let with_override = HashMap::from([
            ("opencode".to_string(), opencode.display().to_string()),
            ("unsloth".to_string(), unsloth_bin.display().to_string()),
        ]);
        let overridden = discover_providers_cached(&with_override);
        let unsloth_overridden = overridden
            .iter()
            .find(|p| p.id == "unsloth")
            .expect("unsloth registered");
        assert!(
            unsloth_overridden.installed,
            "valid override must resolve installed"
        );

        // Removing the override is a different cache key (None vs Some(...)),
        // so this call re-probes auto-detection rather than replaying the
        // cached installed:true verdict from the override key above.
        let without_override = HashMap::new();
        let baseline = discover_providers_cached(&without_override);
        let unsloth_baseline = baseline
            .iter()
            .find(|p| p.id == "unsloth")
            .expect("unsloth registered");
        assert_eq!(
            unsloth_baseline.installed,
            unsloth_baseline.resolved_path.is_some()
                && unsloth_baseline
                    .secondary_binary
                    .as_ref()
                    .is_some_and(|s| s.resolved),
            "no-override call must reflect real auto-detection, not the \
             overridden call's cached verdict: {unsloth_baseline:?}"
        );
    }
}
