//! Short-lived reuse of complete effective REST branch rules. The cache is
//! process-local, shared even when the resolver rebuilds a provider, and never
//! shares authorization contexts. Direct rule reads default to forced freshness.
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::time::Instant;

use crate::cache_scope::CacheScope;
use crate::traffic::{record_reuse, Operation, Reuse};
use crate::{BranchRules, Error, RepoRef, Result};

/// Maximum age from the start of the effective rule read, never its completion.
pub const MAX_AGE: Duration = Duration::from_secs(60);
// Hard process-wide slot bound, including leaders and their waiting readers.
const MAX_ENTRIES: usize = 128;

#[derive(Clone, Copy)]
struct Freshness {
    max_age: Duration,
    after: Instant,
}

tokio::task_local! { static FRESHNESS: Freshness; }

/// Carry a shared PR read's freshness policy through both the folded and
/// standalone fallback paths. Zero requires a read started after this request,
/// including when older work is already in flight. Shorter caller TTLs win.
pub fn with_freshness<T>(
    max_age: Duration,
    future: impl Future<Output = T>,
) -> impl Future<Output = T> {
    let future = Box::pin(future);
    async move {
        FRESHNESS
            .scope(
                Freshness {
                    max_age: max_age.min(MAX_AGE),
                    after: Instant::now(),
                },
                future,
            )
            .await
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    scope: CacheScope,
    repo: RepoRef,
    branch: String,
}

struct Entry {
    started: Instant,
    result: Result<BranchRules>,
}

type Slot = Arc<tokio::sync::Mutex<Option<Entry>>>;

#[derive(Default)]
struct Cache {
    slots: Mutex<HashMap<Key, (Instant, Slot)>>,
}

fn current(scope: &CacheScope) -> Result<()> {
    if scope.is_current() {
        Ok(())
    } else {
        Err(Error::Auth(
            "branch rule authorization changed during read".into(),
        ))
    }
}

impl Cache {
    fn slot(&self, key: Key) -> Result<Slot> {
        let mut slots = self.slots.lock().unwrap();
        // Keep live slots, even when invalidated: evicting them would allow a
        // second fill and unbounded detached work. Holders reject old scopes.
        slots.retain(|key, (at, slot)| {
            Arc::strong_count(slot) > 1 || (key.scope.is_current() && at.elapsed() < MAX_AGE)
        });
        if let Some((at, slot)) = slots.get_mut(&key) {
            *at = Instant::now();
            return Ok(slot.clone());
        }
        if slots.len() >= MAX_ENTRIES {
            let oldest = slots
                .iter()
                .filter(|(_, (_, slot))| Arc::strong_count(slot) == 1)
                .min_by_key(|(_, (at, _))| *at)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                slots.remove(&oldest);
            } else {
                return Err(Error::Api("branch rule cache busy; policy unknown".into()));
            }
        }
        let slot = Slot::default();
        slots.insert(key, (Instant::now(), slot.clone()));
        Ok(slot)
    }

    async fn read(
        &self,
        key: &Key,
        fetch: impl Future<Output = Result<BranchRules>>,
    ) -> Result<BranchRules> {
        current(&key.scope)?;
        let freshness = FRESHNESS.try_with(|f| *f).unwrap_or(Freshness {
            max_age: Duration::ZERO,
            after: Instant::now(),
        });
        let slot = self.slot(key.clone())?;
        let (mut entry, joined) = match slot.try_lock() {
            Ok(guard) => (guard, false),
            Err(_) => (slot.lock().await, true),
        };
        current(&key.scope)?;
        if let Some(cached) = entry.as_ref() {
            let fresh = if freshness.max_age.is_zero() {
                cached.started >= freshness.after
            } else {
                cached.started.elapsed() < freshness.max_age
            };
            // Errors fan out only to readers already waiting for that attempt;
            // the next independent call retries. Never reuse a partial success.
            if fresh && (cached.result.is_ok() || joined) {
                record_reuse(
                    Operation::Rules,
                    if joined {
                        Reuse::InFlight
                    } else {
                        Reuse::CacheHit
                    },
                );
                return cached.result.clone();
            }
        }
        // Cancellation drops the lock with no result, so a waiter can retry.
        *entry = None;
        let started = Instant::now();
        let result = fetch.await;
        current(&key.scope)?;
        *entry = Some(Entry {
            started,
            result: result.clone(),
        });
        result
    }
}

pub(crate) async fn read(
    scope: CacheScope,
    repo: &RepoRef,
    branch: &str,
    fetch: impl Future<Output = Result<BranchRules>>,
) -> Result<BranchRules> {
    static CACHE: OnceLock<Cache> = OnceLock::new();
    CACHE
        .get_or_init(Cache::default)
        .read(
            &Key {
                scope,
                repo: repo.clone(),
                branch: branch.into(),
            },
            fetch,
        )
        .await
}
