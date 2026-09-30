//! Opaque, process-local authorization identity for shared forge reads.
//! Neither the token nor a reusable token digest is exposed or logged.
use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

static GENERATION: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CacheScope {
    identity: [u64; 2],
    generation: u64,
}

impl std::fmt::Debug for CacheScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CacheScope(<redacted>)")
    }
}

impl CacheScope {
    pub(crate) fn github(token: Option<&str>, base: &str) -> Self {
        static HASHERS: OnceLock<[RandomState; 2]> = OnceLock::new();
        let hashers = HASHERS.get_or_init(|| [RandomState::new(), RandomState::new()]);
        Self {
            identity: hashers
                .each_ref()
                .map(|h| h.hash_one(("github", base, token))),
            generation: GENERATION.load(Ordering::SeqCst),
        }
    }

    /// A provider built before a credential write may not publish or reuse data.
    #[must_use]
    pub fn is_current(&self) -> bool {
        self.generation == GENERATION.load(Ordering::SeqCst)
    }
}

pub(crate) fn generation() -> u64 {
    GENERATION.load(Ordering::SeqCst)
}

/// Called on daemon-owned credential/configuration writes, including writing
/// the same token after a permission change. External token changes get a new
/// opaque identity when the normal resolver next observes them.
pub fn invalidate_authorization() {
    GENERATION.fetch_add(1, Ordering::SeqCst);
}
