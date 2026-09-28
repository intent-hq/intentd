//! Process-local GitHub traffic, never account-wide quota consumption. Labels
//! are closed enums: no repository, URL, credential, query or body is retained.
//! Snapshots are cumulative; one aggregate log at most per minute is emitted
//! on activity. Task-local context must be captured BEFORE Octocrab's buffer.

use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum Caller {
    #[default]
    OnDemand,
    PrMonitor,
    WorkspaceRefresh,
    GitRootRefresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Operation {
    Discovery,
    PrDetail,
    Rules,
    QuotaProbe,
    Other,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub enum Attempt {
    #[default]
    Initial,
    Continuation,
    Fallback,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reuse {
    CacheHit,
    InFlight,
    DetailRefresh,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Resource {
    Core,
    Graphql,
    Search,
    Other,
    Unknown,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    pub rest_requests: u64,
    pub graphql_requests: u64,
    pub http_errors: u64,
    pub transport_errors: u64,
    pub graphql_errors: u64,
    pub page_requests: u64,
    pub continuation_requests: u64,
    pub fallback_requests: u64,
    pub cache_hits: u64,
    pub in_flight_reuses: u64,
    pub detail_refresh_reuses: u64,
    /// Sum only of costs returned by GitHub's rateLimit selection. Never
    /// substitute HTTP count or deltas in account-wide x-ratelimit-used.
    pub graphql_points: u64,
    pub graphql_cost_observations: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QuotaObservation {
    pub responses: u64,
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    pub used: Option<u64>,
    pub reset: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub counts: BTreeMap<(Caller, Operation), Counts>,
    pub quotas: BTreeMap<(Caller, Operation, Resource), QuotaObservation>,
}

#[derive(Default)]
struct State {
    snapshot: Snapshot,
    logged_at: Option<Instant>,
}

/// Isolated collectors support HTTP/service tests without resetting global state.
#[derive(Clone, Default)]
pub struct Traffic(Arc<Mutex<State>>);

impl Traffic {
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot
            .clone()
    }

    fn update(&self, f: impl FnOnce(&mut Snapshot)) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut state.snapshot);
        let now = Instant::now();
        let previous = *state.logged_at.get_or_insert(now);
        if now.duration_since(previous) >= Duration::from_secs(60) {
            tracing::info!(traffic = ?state.snapshot, "github traffic: cumulative process-local HTTP attempts and cache reuse");
            state.logged_at = Some(now);
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct Context {
    pub caller: Caller,
    pub continuation: bool,
    pub fallback: bool,
    pub traffic: Traffic,
}

tokio::task_local! { static CONTEXT: Context; }

pub(crate) fn context() -> Context {
    static GLOBAL: OnceLock<Traffic> = OnceLock::new();
    CONTEXT.try_with(Clone::clone).unwrap_or_else(|_| Context {
        traffic: GLOBAL.get_or_init(Traffic::default).clone(),
        ..Context::default()
    })
}

/// Scope a caller without leaking labels between concurrent futures. Spawned
/// tasks must explicitly re-enter a scope (Tokio task locals are not inherited).
pub fn with_caller<T>(caller: Caller, future: impl Future<Output = T>) -> impl Future<Output = T> {
    // Service sweep futures are large. Box before constructing the scope so
    // accounting does not duplicate their storage through nested async states.
    // Capture context when polled, preserving nested with_traffic/with_caller.
    let future = Box::pin(future);
    async move {
        CONTEXT
            .scope(
                Context {
                    caller,
                    ..context()
                },
                future,
            )
            .await
    }
}

/// Run against an isolated aggregate; useful for functional request assertions.
pub fn with_traffic<T>(
    traffic: Traffic,
    future: impl Future<Output = T>,
) -> impl Future<Output = T> {
    let future = Box::pin(future);
    async move {
        CONTEXT
            .scope(
                Context {
                    traffic,
                    ..context()
                },
                future,
            )
            .await
    }
}

/// Mark requests issued to continue a page chain or recover from a degraded
/// read. Nested scopes retain both facts (a fallback can itself paginate).
pub fn with_attempt<T>(
    attempt: Attempt,
    future: impl Future<Output = T>,
) -> impl Future<Output = T> {
    let future = Box::pin(future);
    async move {
        let mut ctx = context();
        ctx.continuation |= attempt == Attempt::Continuation;
        ctx.fallback |= attempt == Attempt::Fallback;
        CONTEXT.scope(ctx, future).await
    }
}

/// Called at the branch that actually reuses data, never at speculative lookup.
pub fn record_reuse(operation: Operation, reuse: Reuse) {
    let ctx = context();
    ctx.traffic.update(|s| {
        let c = s.counts.entry((ctx.caller, operation)).or_default();
        match reuse {
            Reuse::CacheHit => c.cache_hits += 1,
            Reuse::InFlight => c.in_flight_reuses += 1,
            Reuse::DetailRefresh => c.detail_refresh_reuses += 1,
        }
    });
}

impl Context {
    pub fn start(&self, operation: Operation, graphql: bool, page: bool) {
        self.traffic.update(|s| {
            let c = s.counts.entry((self.caller, operation)).or_default();
            if graphql {
                c.graphql_requests += 1;
            } else {
                c.rest_requests += 1;
            }
            c.page_requests += u64::from(page);
            c.continuation_requests += u64::from(self.continuation);
            c.fallback_requests += u64::from(self.fallback);
        });
    }

    pub fn finish(
        &self,
        operation: Operation,
        response: Option<(http::StatusCode, &http::HeaderMap)>,
    ) {
        self.traffic.update(|s| {
            let c = s.counts.entry((self.caller, operation)).or_default();
            let Some((status, headers)) = response else {
                c.transport_errors += 1;
                return;
            };
            c.http_errors += u64::from(status.is_client_error() || status.is_server_error());
            let resource = match headers
                .get("x-ratelimit-resource")
                .and_then(|h| h.to_str().ok())
            {
                Some("core") => Resource::Core,
                Some("graphql") => Resource::Graphql,
                Some("search") => Resource::Search,
                Some(_) => Resource::Other,
                None => Resource::Unknown,
            };
            let number = |name| {
                headers
                    .get(name)
                    .and_then(|h| h.to_str().ok()?.parse().ok())
            };
            let q = s
                .quotas
                .entry((self.caller, operation, resource))
                .or_default();
            q.responses += 1;
            q.limit = number("x-ratelimit-limit");
            q.remaining = number("x-ratelimit-remaining");
            q.used = number("x-ratelimit-used");
            q.reset = number("x-ratelimit-reset");
        });
    }
}

pub(crate) fn graphql_result(operation: Operation, cost: Option<u64>, failed: bool) {
    let ctx = context();
    ctx.traffic.update(|s| {
        let c = s.counts.entry((ctx.caller, operation)).or_default();
        c.graphql_errors += u64::from(failed);
        if let Some(cost) = cost {
            c.graphql_points += cost;
            c.graphql_cost_observations += 1;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accounting_scopes_do_not_embed_large_service_futures() {
        fn large_future() -> impl Future<Output = ()> {
            let data = [0_u8; 64 * 1024];
            async move {
                std::hint::black_box(data);
            }
        }
        assert!(std::mem::size_of_val(&large_future()) >= 64 * 1024);
        assert!(std::mem::size_of_val(&with_caller(Caller::PrMonitor, large_future())) < 1024);
        assert!(std::mem::size_of_val(&with_attempt(Attempt::Fallback, large_future())) < 1024);
        assert!(std::mem::size_of_val(&with_traffic(Traffic::default(), large_future())) < 1024);
    }

    #[tokio::test]
    async fn nested_scopes_restore_callers_and_keep_reuse_out_of_http_counts() {
        let traffic = Traffic::default();
        with_traffic(traffic.clone(), async {
            record_reuse(Operation::Discovery, Reuse::CacheHit);
            with_caller(Caller::GitRootRefresh, async {
                with_attempt(
                    Attempt::Fallback,
                    with_attempt(Attempt::Continuation, async {
                        let ctx = context();
                        ctx.start(Operation::PrDetail, true, true);
                        ctx.finish(Operation::PrDetail, None);
                    }),
                )
                .await;
                record_reuse(Operation::Discovery, Reuse::InFlight);
            })
            .await;
            record_reuse(Operation::Rules, Reuse::CacheHit);
        })
        .await;
        let s = traffic.snapshot();
        assert_eq!(
            s.counts[&(Caller::OnDemand, Operation::Discovery)],
            Counts {
                cache_hits: 1,
                ..Counts::default()
            }
        );
        assert_eq!(
            s.counts[&(Caller::GitRootRefresh, Operation::Discovery)],
            Counts {
                in_flight_reuses: 1,
                ..Counts::default()
            }
        );
        assert_eq!(
            s.counts[&(Caller::GitRootRefresh, Operation::PrDetail)],
            Counts {
                graphql_requests: 1,
                transport_errors: 1,
                page_requests: 1,
                continuation_requests: 1,
                fallback_requests: 1,
                ..Counts::default()
            }
        );
        assert_eq!(
            s.counts[&(Caller::OnDemand, Operation::Rules)].cache_hits,
            1
        );
    }

    #[test]
    fn malformed_headers_and_unbounded_resource_names_stay_bounded() {
        let ctx = Context::default();
        for value in 0..100 {
            let mut headers = http::HeaderMap::new();
            headers.insert(
                "x-ratelimit-resource",
                format!("unknown-{value}").parse().unwrap(),
            );
            headers.insert("x-ratelimit-remaining", "oops".parse().unwrap());
            headers.insert("x-ratelimit-used", "-1".parse().unwrap());
            ctx.finish(
                Operation::Other,
                Some((http::StatusCode::FORBIDDEN, &headers)),
            );
        }
        let s = ctx.traffic.snapshot();
        assert_eq!(s.quotas.len(), 1);
        let q = &s.quotas[&(Caller::OnDemand, Operation::Other, Resource::Other)];
        assert_eq!(q.responses, 100);
        assert_eq!(q.remaining, None);
        assert_eq!(q.used, None);
        assert_eq!(
            s.counts[&(Caller::OnDemand, Operation::Other)].http_errors,
            100
        );
    }

    #[test]
    fn summary_is_activity_driven_and_throttled() {
        let traffic = Traffic::default();
        traffic.update(|_| {});
        let initial = traffic.0.lock().unwrap().logged_at.unwrap();
        traffic.update(|_| {});
        assert_eq!(traffic.0.lock().unwrap().logged_at, Some(initial));
        traffic.0.lock().unwrap().logged_at = initial.checked_sub(Duration::from_secs(61));
        traffic.update(|_| {});
        let emitted = traffic.0.lock().unwrap().logged_at.unwrap();
        assert!(emitted >= initial);
        traffic.update(|_| {});
        assert_eq!(traffic.0.lock().unwrap().logged_at, Some(emitted));
    }
}
