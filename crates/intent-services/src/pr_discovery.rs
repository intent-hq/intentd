//! Repository-wide PR discovery and bounded background record reads.
//!
//! A list is authoritative only after every page succeeds. Single-flight slots
//! also remember failures for a window: a broken repository never falls back
//! to a request per branch. Lists never enter the richer PR-detail cache.
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_sourcecontrol::traffic::{record_reuse, Operation, Reuse};
use intent_sourcecontrol::{
    cache_scope::CacheScope, Error, PrQuery, PrState, PullRequest, RepoRef, Result, SourceControl,
};
use tokio::time::Instant;

use crate::{pr_ops, Services};

pub(crate) const WINDOW: Duration = Duration::from_secs(180);
const MAX_PAGES: usize = 10;
const REPOSITORIES: usize = 128;
const RECORDS: usize = 512;
/// Actual HTTP attempts, including retry and redirect hops, per window.
const ATTEMPTS: usize = 128;
const LIST_RESERVATION: usize = MAX_PAGES * 4;
const RECORD_RESERVATION: usize = 4;

tokio::task_local! { static FORCE_AFTER: Instant; }

pub(crate) async fn explicitly_refresh<T>(future: impl std::future::Future<Output = T>) -> T {
    FORCE_AFTER.scope(Instant::now(), Box::pin(future)).await
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct Key {
    scope: CacheScope,
    repo: RepoRef,
    number: Option<u64>,
}

#[derive(Default)]
struct Index {
    branches: HashMap<String, Vec<PullRequest>>,
}

impl Index {
    fn matching(
        &self,
        branch: &str,
        base: Option<&str>,
        exclude: Option<u64>,
    ) -> Option<PullRequest> {
        let best = |name: &str| {
            self.branches
                .get(name)
                .into_iter()
                .flatten()
                .filter(|pr| Some(pr.number) != exclude)
                .max_by_key(|pr| pr.number)
        };
        if !branch.is_empty() {
            if let Some(pr) = best(branch) {
                return Some(pr.clone());
            }
        }
        pr_ops::base_ref_match_candidates(base)
            .iter()
            .filter_map(|b| best(b))
            .max_by_key(|pr| pr.number)
            .cloned()
    }
}

#[derive(Clone)]
enum Value {
    Listing(Arc<Index>),
    Record(Arc<PullRequest>),
}

struct Entry {
    started: Instant,
    finished: Option<Instant>,
    revision: u64,
    result: Result<Value>,
}

#[derive(Default)]
struct Slot {
    revision: AtomicU64,
    entry: tokio::sync::Mutex<Option<Entry>>,
}

struct Budget {
    started: Instant,
    available: usize,
    pending: VecDeque<(Key, usize, Instant)>,
    grants: HashMap<Key, usize>,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            started: Instant::now(),
            available: ATTEMPTS,
            pending: VecDeque::new(),
            grants: HashMap::new(),
        }
    }
}

/// Unused reservations return only to their original window. The admission
/// closure retains the lease through Octocrab's buffer and every retry/hop.
struct Lease {
    budget: Arc<Mutex<Budget>>,
    window: Instant,
    remaining: AtomicUsize,
    scope: CacheScope,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut budget = self.budget.lock().unwrap();
        if budget.started == self.window {
            budget.available += self.remaining.load(Ordering::SeqCst);
        }
    }
}

impl Budget {
    fn reserve(shared: &Arc<Mutex<Self>>, key: &Key) -> Result<Arc<Lease>> {
        let now = Instant::now();
        let cost = if key.number.is_none() {
            LIST_RESERVATION
        } else {
            RECORD_RESERVATION
        };
        let mut state = shared.lock().unwrap();
        if now.duration_since(state.started) >= WINDOW {
            state.started = now;
            state.available = ATTEMPTS;
            state.grants.clear();
        }
        state
            .pending
            .retain(|(key, _, at)| key.scope.is_current() && now.duration_since(*at) < WINDOW * 10);
        // Reserve older denied work first, even when the sweep visits a busy
        // workspace first again. Unclaimed reservations expire next window.
        while let Some((_, cost, _)) = state.pending.front() {
            if *cost > state.available {
                break;
            }
            let (key, cost, _) = state.pending.pop_front().unwrap();
            state.available -= cost;
            state.grants.insert(key, cost);
        }
        let admitted = state.grants.remove(key).is_some();
        if !admitted {
            if state.pending.is_empty() && state.available >= cost {
                state.available -= cost;
            } else {
                if state.pending.len() < REPOSITORIES + RECORDS
                    && !state.pending.iter().any(|(k, _, _)| k == key)
                {
                    state.pending.push_back((key.clone(), cost, now));
                }
                return Err(unknown("background PR refresh deferred by request budget"));
            }
        }
        Ok(Arc::new(Lease {
            budget: shared.clone(),
            window: state.started,
            remaining: AtomicUsize::new(cost),
            scope: key.scope.clone(),
        }))
    }
}

type Slots = Arc<Mutex<HashMap<Key, (Instant, Arc<Slot>)>>>;

#[derive(Clone)]
pub(crate) struct Discovery {
    slots: Slots,
    budget: Arc<Mutex<Budget>>,
    concurrent: Arc<tokio::sync::Semaphore>,
}

impl Default for Discovery {
    fn default() -> Self {
        Self {
            slots: Arc::default(),
            budget: Arc::default(),
            concurrent: Arc::new(tokio::sync::Semaphore::new(4)),
        }
    }
}

fn unknown(message: &str) -> Error {
    Error::Api(message.into())
}

impl Discovery {
    fn newer_listing(&self, key: &Key, started: Instant) -> bool {
        if key.number.is_none() {
            return false;
        }
        let slots = self.slots.lock().unwrap();
        let Some((_, slot)) = slots.get(&Key {
            number: None,
            ..key.clone()
        }) else {
            return false;
        };
        let Ok(entry) = slot.entry.try_lock() else {
            return false;
        };
        entry
            .as_ref()
            .is_some_and(|entry| entry.started > started && entry.result.is_ok())
    }
    pub(crate) fn last_attempt(
        &self,
        sc: &dyn SourceControl,
        repo: &RepoRef,
        number: u64,
    ) -> Option<Instant> {
        let scope = sc.cache_scope()?;
        let slots = self.slots.lock().unwrap();
        let (_, slot) = slots.get(&Key {
            scope,
            repo: repo.clone(),
            number: Some(number),
        })?;
        let Ok(entry) = slot.entry.try_lock() else {
            return Some(Instant::now());
        };
        entry.as_ref().map(|entry| entry.started)
    }

    #[cfg(test)]
    pub(crate) fn expire(&self) {
        for (_, slot) in self.slots.lock().unwrap().values() {
            if let Ok(mut entry) = slot.entry.try_lock() {
                if let Some(entry) = entry.as_mut() {
                    entry.started -= WINDOW;
                }
            }
        }
        self.budget.lock().unwrap().started -= WINDOW;
    }

    fn slot(&self, key: &Key) -> Result<Arc<Slot>> {
        let now = Instant::now();
        let mut slots = self.slots.lock().unwrap();
        slots.retain(|key, (used, slot)| {
            Arc::strong_count(slot) > 1
                || (key.scope.is_current() && now.duration_since(*used) < WINDOW * 10)
        });
        if let Some((at, slot)) = slots.get_mut(key) {
            *at = now;
            return Ok(slot.clone());
        }
        let is_list = key.number.is_none();
        let cap = if is_list { REPOSITORIES } else { RECORDS };
        if slots
            .keys()
            .filter(|k| k.number.is_none() == is_list)
            .count()
            >= cap
        {
            let oldest = slots
                .iter()
                .filter(|(k, (_, s))| k.number.is_none() == is_list && Arc::strong_count(s) == 1)
                .min_by_key(|(_, (at, _))| *at)
                .map(|(k, _)| k.clone());
            let Some(oldest) = oldest else {
                return Err(unknown("PR refresh cache busy"));
            };
            slots.remove(&oldest);
        }
        let slot = Arc::new(Slot::default());
        slots.insert(key.clone(), (now, slot.clone()));
        Ok(slot)
    }

    pub(crate) fn invalidate(&self, sc: &dyn SourceControl, repo: &RepoRef) {
        let Some(scope) = sc.cache_scope() else {
            return;
        };
        for (key, (_, slot)) in self.slots.lock().unwrap().iter() {
            if key.scope == scope && key.repo == *repo {
                slot.revision.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    async fn read(&self, services: &Services, sc: &dyn SourceControl, key: Key) -> Result<Value> {
        if !key.scope.is_current() {
            return Err(unknown("PR refresh authorization changed"));
        }
        let slot = self.slot(&key)?;
        let operation = if key.number.is_none() {
            Operation::Discovery
        } else {
            Operation::PrDetail
        };
        let mut entry = if let Ok(guard) = slot.entry.try_lock() {
            guard
        } else {
            record_reuse(operation, Reuse::InFlight);
            slot.entry.lock().await
        };
        let revision = slot.revision.load(Ordering::SeqCst);
        let force_after = FORCE_AFTER.try_with(|at| *at).ok();
        // A monitor/hover may have observed a newer record since our last
        // background fill. Borrow that record before returning our own hit.
        if force_after.is_none() {
            if let Some(number) = key.number {
                if let Some((pr, at)) = crate::pr_monitor::cached_record_for_discovery(
                    &services.pr_cache,
                    &key.repo,
                    number,
                    &key.scope,
                    WINDOW,
                ) {
                    let at = Instant::from_std(at);
                    if entry.as_ref().is_none_or(|cached| cached.started <= at)
                        && (pr.state == PrState::Merged || !self.newer_listing(&key, at))
                    {
                        let value = Value::Record(Arc::new(pr));
                        *entry = Some(Entry {
                            started: at,
                            finished: Some(Instant::now()),
                            revision,
                            result: Ok(value.clone()),
                        });
                        record_reuse(Operation::PrDetail, Reuse::CacheHit);
                        return Ok(value);
                    }
                }
            }
        }
        if let Some(cached) = entry.as_ref() {
            let merged =
                matches!(&cached.result, Ok(Value::Record(pr)) if pr.state == PrState::Merged);
            if cached.revision == revision
                && key.scope.is_current()
                && (merged
                    || (cached.started.elapsed() < WINDOW
                        && !self.newer_listing(&key, cached.started)))
                && force_after
                    .is_none_or(|at| cached.finished.is_some_and(|finished| finished >= at))
            {
                record_reuse(operation, Reuse::CacheHit);
                return cached.result.clone();
            }
        }
        let started = Instant::now();
        // Cancellation keeps an unknown result for this window, not an empty
        // list or an immediate retry by the next checkout.
        *entry = Some(Entry {
            started,
            finished: None,
            revision,
            result: Err(unknown("PR refresh interrupted")),
        });
        let result = async {
            if services.sweeps_rate_limited() {
                return Err(unknown("PR refresh quota paused"));
            }
            let lease = Budget::reserve(&self.budget, &key)?;
            let _permit = self
                .concurrent
                .acquire()
                .await
                .map_err(|_| unknown("PR refresh stopped"))?;
            let owner = services.clone();
            let admission = Arc::new(move || {
                lease.scope.is_current()
                    && lease.window.elapsed() < WINDOW
                    && !owner.sweeps_rate_limited()
                    && lease
                        .remaining
                        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                        .is_ok()
            });
            intent_sourcecontrol::request_budget::with_admission(admission, async {
                if let Some(number) = key.number {
                    return sc
                        .get_pr(&key.repo, number)
                        .await
                        .map(|pr| Value::Record(Arc::new(pr)));
                }
                let mut index = Index::default();
                let mut cursor = None;
                let mut seen = HashSet::new();
                let mut numbers = HashSet::new();
                for _ in 0..MAX_PAGES {
                    let page = sc
                        .list_prs(
                            &key.repo,
                            PrQuery {
                                state: Some(PrState::Open),
                                limit: Some(100),
                                cursor: cursor.clone(),
                                ..PrQuery::default()
                            },
                        )
                        .await?;
                    if page.items.len() > 100 {
                        return Err(unknown("oversized open PR page"));
                    }
                    for pr in page.items {
                        // Duplicate PRs across pages mean the listing moved under
                        // pagination. Do not turn that torn snapshot into absence.
                        if pr.state != PrState::Open || !numbers.insert(pr.number) {
                            return Err(unknown("inconsistent open PR pages"));
                        }
                        index
                            .branches
                            .entry(pr.source_branch.clone())
                            .or_default()
                            .push(pr);
                    }
                    let Some(next) = page.next_cursor else {
                        return Ok(Value::Listing(Arc::new(index)));
                    };
                    if next.is_empty()
                        || !seen.insert(next.clone())
                        || Some(&next) == cursor.as_ref()
                    {
                        return Err(unknown("invalid open PR pagination cursor"));
                    }
                    cursor = Some(next);
                }
                Err(unknown("open PR pagination budget exhausted"))
            })
            .await
        }
        .await;
        if !key.scope.is_current() || revision != slot.revision.load(Ordering::SeqCst) {
            return Err(unknown("PR refresh invalidated during fetch"));
        }
        *entry = Some(Entry {
            started,
            finished: Some(Instant::now()),
            revision,
            result: result.clone(),
        });
        result
    }
}

impl Services {
    pub(crate) async fn linked_pr_record(
        &self,
        sc: &dyn SourceControl,
        repo: &RepoRef,
        number: u64,
    ) -> Result<PullRequest> {
        if let Some(scope) = sc.cache_scope() {
            if !scope.is_current() {
                return Err(unknown("PR refresh authorization changed"));
            }
            // Keep discovery alive even when every consumer currently has an
            // open link. Listing failure is unknown, but may not suppress a
            // separately successful linked-status confirmation.
            if let Err(e @ Error::RateLimited(_)) = self
                .pr_discovery
                .read(
                    self,
                    sc,
                    Key {
                        scope: scope.clone(),
                        repo: repo.clone(),
                        number: None,
                    },
                )
                .await
            {
                return Err(e);
            }
            if !scope.is_current() {
                return Err(unknown("PR refresh authorization changed"));
            }
        }
        self.shared_pr_record(sc, repo, number).await
    }

    pub(crate) async fn discover_shared_pr(
        &self,
        sc: &dyn SourceControl,
        repo: &RepoRef,
        branch: &str,
        base: Option<&str>,
        exclude: Option<u64>,
    ) -> Result<Option<PullRequest>> {
        let Some(scope) = sc.cache_scope() else {
            return pr_ops::discover_matching_open_pr(sc, repo, branch, base, exclude).await;
        };
        let Value::Listing(index) = self
            .pr_discovery
            .read(
                self,
                sc,
                Key {
                    scope,
                    repo: repo.clone(),
                    number: None,
                },
            )
            .await?
        else {
            unreachable!()
        };
        let Some(pr) = index.matching(branch, base, exclude) else {
            return Ok(None);
        };
        // A detail read, shared per number, supplies mergeability and confirms
        // this PR is still open before linking it. Failure retains existing links.
        let detail = self.shared_pr_record(sc, repo, pr.number).await?;
        Ok(
            (detail.state == PrState::Open && pr_ops::pr_matches_workspace(&detail, branch, base))
                .then_some(detail),
        )
    }

    pub(crate) async fn shared_pr_record(
        &self,
        sc: &dyn SourceControl,
        repo: &RepoRef,
        number: u64,
    ) -> Result<PullRequest> {
        let Some(scope) = sc.cache_scope() else {
            return sc.get_pr(repo, number).await;
        };
        let Value::Record(pr) = self
            .pr_discovery
            .read(
                self,
                sc,
                Key {
                    scope,
                    repo: repo.clone(),
                    number: Some(number),
                },
            )
            .await?
        else {
            unreachable!()
        };
        Ok((*pr).clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> CacheScope {
        intent_sourcecontrol::GitHubSourceControl::new("discovery-unit", None)
            .unwrap()
            .cache_scope()
            .unwrap()
    }
    fn key(scope: &CacheScope, repo: usize, number: Option<u64>) -> Key {
        Key {
            scope: scope.clone(),
            repo: RepoRef::new("o", format!("r{repo}")),
            number,
        }
    }

    #[test]
    fn shared_discovery_cache_is_bounded_and_in_flight_slots_are_not_evicted() {
        let discovery = Discovery::default();
        let scope = scope();
        let pinned: Vec<_> = (0..REPOSITORIES)
            .map(|i| discovery.slot(&key(&scope, i, None)).unwrap())
            .collect();
        assert!(discovery.slot(&key(&scope, REPOSITORIES, None)).is_err());
        drop(pinned);
        discovery.slot(&key(&scope, REPOSITORIES, None)).unwrap();
        for i in 0..RECORDS + 100 {
            discovery.slot(&key(&scope, 0, Some(i as u64))).unwrap();
        }
        let slots = discovery.slots.lock().unwrap();
        assert_eq!(
            slots.keys().filter(|k| k.number.is_none()).count(),
            REPOSITORIES
        );
        assert_eq!(slots.keys().filter(|k| k.number.is_some()).count(), RECORDS);
    }

    #[test]
    fn shared_discovery_budget_refunds_unused_and_prioritizes_deferred_work() {
        let budget = Arc::new(Mutex::new(Budget::default()));
        let scope = scope();
        let first = Budget::reserve(&budget, &key(&scope, 0, None)).unwrap();
        first.remaining.fetch_sub(3, Ordering::SeqCst);
        drop(first);
        assert_eq!(budget.lock().unwrap().available, ATTEMPTS - 3);
        // Exhaust the remaining budget, then register a backlog in visitation
        // order. Next window must reserve it before the first workspace again.
        budget.lock().unwrap().available = 0;
        for i in 1..=4 {
            assert!(Budget::reserve(&budget, &key(&scope, i, None)).is_err());
        }
        budget.lock().unwrap().started -= WINDOW;
        assert!(Budget::reserve(&budget, &key(&scope, 0, None)).is_err());
        for i in 1..=3 {
            let lease = Budget::reserve(&budget, &key(&scope, i, None)).unwrap();
            lease.remaining.store(0, Ordering::SeqCst);
        }
        budget.lock().unwrap().started -= WINDOW;
        let fourth = Budget::reserve(&budget, &key(&scope, 4, None)).unwrap();
        assert_eq!(fourth.remaining.load(Ordering::SeqCst), LIST_RESERVATION);
    }

    #[test]
    fn shared_discovery_old_leases_cannot_refill_a_new_window() {
        let budget = Arc::new(Mutex::new(Budget::default()));
        let scope = scope();
        let lease = Budget::reserve(&budget, &key(&scope, 0, None)).unwrap();
        budget.lock().unwrap().started -= WINDOW;
        let current = Budget::reserve(&budget, &key(&scope, 1, None)).unwrap();
        let available = budget.lock().unwrap().available;
        drop(lease);
        assert_eq!(budget.lock().unwrap().available, available);
        drop(current);
        assert_eq!(budget.lock().unwrap().available, ATTEMPTS);
    }
    #[tokio::test(start_paused = true)]
    async fn shared_discovery_repair_active_fifo_survives_sixty_windows() {
        let budget = Arc::new(Mutex::new(Budget::default()));
        let scope = scope();
        let keys: Vec<_> = (0..170)
            .map(|i| key(&scope, i, None))
            .chain((0..20).map(|i| key(&scope, 170, Some(i))))
            .collect();
        let mut admitted = HashSet::new();
        for _ in 0..60 {
            let mut attempts = 0;
            for (i, key) in keys.iter().enumerate() {
                if let Ok(lease) = Budget::reserve(&budget, key) {
                    // A complete ten-page list, or one targeted PR read.
                    let cost = if key.number.is_none() { 10 } else { 1 };
                    lease.remaining.fetch_sub(cost, Ordering::SeqCst);
                    attempts += cost;
                    admitted.insert(i);
                }
            }
            assert!(attempts <= ATTEMPTS);
            tokio::time::advance(WINDOW).await;
        }
        assert_eq!(
            admitted.len(),
            keys.len(),
            "continuously active late repositories and records must not starve"
        );
    }
}
