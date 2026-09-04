//! Per-RPC dispatch profiling (expensive-RPC guardrail).
//!
//! [`RpcProfileLayer`] watches the `rpc_dispatch` span the JSON-RPC router
//! wraps around every dispatch (see
//! [`intent_transport::router::RPC_DISPATCH_SPAN_TARGET`]) and counts the
//! `sqlx::query` statement events sqlx emits — one per executed statement,
//! propagated into the span's scope by sqlx-sqlite's worker-thread span
//! forwarding. When the span closes, the layer emits:
//!
//! - one WARN when the statement count exceeds the statement threshold
//!   (default [`DEFAULT_STATEMENT_WARN_THRESHOLD`]; N+1 / hydrate-then-discard
//!   regressions), and
//! - one WARN when the wall-clock duration exceeds the duration threshold
//!   for the method's tier.
//!
//! Duration budgets are tiered: methods that fan out to a network-bound
//! upstream ([`is_network_tier_method`] — `github.*`, `linear.*`, `sentry.*`,
//! `pr.refresh`, `pr.state`, `workspace.create`) get a higher default budget
//! ([`DEFAULT_NETWORK_DURATION_WARN_MS`]) so normal upstream latency doesn't
//! drown out the local-regression signal; every other method keeps the
//! default budget ([`DEFAULT_DURATION_WARN_MS`]; catches fs walks / git scans
//! that never touch `SQLite`). The statement-count budget is tiered the same
//! way: legitimately compound multi-entity ops
//! ([`is_compound_statement_method`] — `workspace.create`,
//! `workspace.delete`, `workspace.import.commit`, `workspace.unarchive`,
//! `workspace.restore`) get a higher default budget
//! ([`DEFAULT_COMPOUND_STATEMENT_WARN_THRESHOLD`]) so their by-design
//! statement count doesn't drown out the N+1 signal, a compound op whose
//! legitimate ceiling exceeds even that tier carries its own budget
//! ([`PER_METHOD_STATEMENT_BUDGETS`] — `agent.sendMessage`,
//! `agent.sendToTask`), and every other method keeps the default budget
//! ([`DEFAULT_STATEMENT_WARN_THRESHOLD`]).
//!
//! All tier thresholds are overridable via [`STATEMENT_THRESHOLD_ENV`],
//! [`COMPOUND_STATEMENT_THRESHOLD_ENV`], [`DURATION_THRESHOLD_ENV`], and
//! [`NETWORK_DURATION_THRESHOLD_ENV`] (read once at layer construction);
//! the per-method budgets are compile-time constants. Logging only — no
//! wire-contract impact; overhead is one span per dispatch plus a counter
//! increment per statement.

use std::time::{Duration, Instant};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Subscriber};
use tracing_subscriber::filter::{LevelFilter, Targets};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

use intent_transport::router::{RPC_DISPATCH_SPAN_NAME, RPC_DISPATCH_SPAN_TARGET};

/// Default statement-count threshold: a dispatch executing more than this
/// many SQL statements draws a WARN.
pub const DEFAULT_STATEMENT_WARN_THRESHOLD: u64 = 25;
/// Default statement-count threshold for compound-op-tier methods (see
/// [`is_compound_statement_method`]): a dispatch executing more than this
/// many SQL statements draws a WARN. Higher than
/// [`DEFAULT_STATEMENT_WARN_THRESHOLD`] so a legitimately compound
/// multi-entity op doesn't trip the guardrail. Sized off observed dispatch
/// counts — `workspace.create` deterministically runs ~40 statements and
/// `workspace.delete` ~10 regardless of workspace contents (its per-agent
/// sweep was batched in intent-hq/monorepo#4130; 26–72 observed before that,
/// intent-hq/monorepo#3074) — while staying an order of magnitude below the
/// hundreds a real N+1 regression produces.
pub const DEFAULT_COMPOUND_STATEMENT_WARN_THRESHOLD: u64 = 100;
/// Default duration threshold in milliseconds for non-network-tier methods: a
/// dispatch running longer than this draws a WARN.
pub const DEFAULT_DURATION_WARN_MS: u64 = 1000;
/// Default duration threshold in milliseconds for network-tier methods (see
/// [`is_network_tier_method`]): a dispatch running longer than this draws a
/// WARN. Higher than [`DEFAULT_DURATION_WARN_MS`] so normal upstream latency
/// doesn't trip the guardrail.
pub const DEFAULT_NETWORK_DURATION_WARN_MS: u64 = 10_000;
/// Env override for the statement-count threshold (u64).
pub const STATEMENT_THRESHOLD_ENV: &str = "INTENTD_RPC_STATEMENT_WARN_THRESHOLD";
/// Env override for the compound-op-tier statement-count threshold (u64).
pub const COMPOUND_STATEMENT_THRESHOLD_ENV: &str = "INTENTD_RPC_COMPOUND_STATEMENT_WARN_THRESHOLD";
/// Env override for the non-network-tier duration threshold in milliseconds
/// (u64).
pub const DURATION_THRESHOLD_ENV: &str = "INTENTD_RPC_DURATION_WARN_MS";
/// Env override for the network-tier duration threshold in milliseconds
/// (u64).
pub const NETWORK_DURATION_THRESHOLD_ENV: &str = "INTENTD_RPC_NETWORK_DURATION_WARN_MS";

/// Method name prefixes that identify a network-bound RPC (see
/// [`is_network_tier_method`]).
const NETWORK_TIER_PREFIXES: &[&str] = &["github.", "linear.", "sentry."];
/// Exact method names (outside the prefix list) that identify a network-bound
/// RPC (see [`is_network_tier_method`]). `workspace.create` belongs here
/// because its dominant cost is git provisioning — clone/fetch from the
/// remote plus worktree checkout — so its normal duration tracks upstream
/// and disk latency, not local SQL work (intent-hq/monorepo#2994).
const NETWORK_TIER_METHODS: &[&str] = &["pr.refresh", "pr.state", "workspace.create"];

/// Whether `method` fans out to a network-bound upstream and should use the
/// network-tier duration budget ([`DEFAULT_NETWORK_DURATION_WARN_MS`] /
/// [`NETWORK_DURATION_THRESHOLD_ENV`]) instead of the default one.
fn is_network_tier_method(method: &str) -> bool {
    NETWORK_TIER_PREFIXES
        .iter()
        .any(|prefix| method.starts_with(prefix))
        || NETWORK_TIER_METHODS.contains(&method)
}

/// Exact method names of legitimately compound multi-entity ops (see
/// [`is_compound_statement_method`]). `workspace.delete` belongs here
/// because deletion fans out over the workspace's contents — per-session
/// teardown, completion-watch and subscription sweeps, then the store
/// cascade — so its statement count scales with workspace size
/// (intent-hq/monorepo#3074). `workspace.import.commit` likewise inserts one
/// row per transferred row inside the dispatch, so its count scales with the
/// imported workspace's contents. Import counts are unbounded (322 observed
/// on a large import), so a big import can still overrun the compound budget
/// — that residual WARN on a rare, deliberate op is accepted rather than
/// raising the shared threshold high enough to blunt the N+1 signal for the
/// bounded members. `workspace.unarchive` is a compound lifecycle op —
/// workspace row update, agent/watcher re-arming, watch re-registration for
/// the worktree, event emission — that runs ~40 statements
/// (intent-hq/monorepo#3496); `workspace.restore` is its wire alias
/// (`WorkspaceApi::restore_workspace` delegates to `unarchive_workspace`),
/// so it shares the exact same statement shape and tier. `workspace.archive`
/// stays on the default tier deliberately: its compound work (interrupt
/// sweep, hook/PR-monitor cancels, last-activity derive, event emit) runs on
/// a detached spawned tail (intent-hq/monorepo#1577) outside the dispatch
/// span, so only the workspace row read + update are counted — well under
/// the default budget.
const COMPOUND_STATEMENT_METHODS: &[&str] = &[
    "workspace.create",
    "workspace.delete",
    "workspace.import.commit",
    "workspace.unarchive",
    "workspace.restore",
];

/// Whether `method` is a legitimately compound multi-entity op — it
/// executes many statements by design (e.g. `workspace.create` persists the
/// workspace, spec note, initial agent, and bookkeeping rows in one
/// dispatch; `workspace.delete` tears all of that down) — and should use the
/// compound-op statement budget
/// ([`DEFAULT_COMPOUND_STATEMENT_WARN_THRESHOLD`] /
/// [`COMPOUND_STATEMENT_THRESHOLD_ENV`]) instead of the default one.
fn is_compound_statement_method(method: &str) -> bool {
    COMPOUND_STATEMENT_METHODS.contains(&method)
}

/// Per-method statement budgets for compound ops whose legitimate ceiling
/// exceeds even the compound-op tier. `agent.sendMessage` is a compound
/// queue operation (queue insert, queue reordering / single-pending-sender
/// enforcement, watch arming, event emission, attention bookkeeping) whose
/// statement count scales with message/queue content — 26–150 observed under
/// normal coordinator fan-out (intent-hq/monorepo#3492) — so it gets its own
/// budget sized above that ceiling with headroom, rather than raising the
/// shared compound threshold high enough to blunt the N+1 signal for the
/// bounded members. `agent.sendToTask` resolves the task's assignee and then
/// routes through the same delivery path (`agent_send_to_task_op` mirrors
/// `agent.sendMessage`'s `manager.send_message` / `interrupt_send_message`
/// routing, DELIV-1), so it carries the same content-scaled shape plus a
/// task lookup and gets the same budget.
const PER_METHOD_STATEMENT_BUDGETS: &[(&str, u64)] =
    &[("agent.sendMessage", 250), ("agent.sendToTask", 250)];

/// The per-method statement budget for `method`, if it has one (see
/// [`PER_METHOD_STATEMENT_BUDGETS`]). Takes precedence over the tier
/// budgets.
fn per_method_statement_budget(method: &str) -> Option<u64> {
    PER_METHOD_STATEMENT_BUDGETS
        .iter()
        .find(|(m, _)| *m == method)
        .map(|&(_, budget)| budget)
}

/// Target sqlx logs each executed statement under (`sqlx-core` `QueryLogger`).
const SQLX_QUERY_TARGET: &str = "sqlx::query";
/// Target the layer's own WARN events are emitted under.
const WARN_TARGET: &str = "intentd::rpc_profile";

/// Per-layer filter for [`RpcProfileLayer`]: enables the router's dispatch
/// span plus `sqlx::query` statement events (any level — sqlx raises slow
/// statements above DEBUG), independent of the output layers' `EnvFilter`.
pub fn profile_filter() -> Targets {
    Targets::new()
        .with_target(RPC_DISPATCH_SPAN_TARGET, LevelFilter::INFO)
        .with_target(SQLX_QUERY_TARGET, LevelFilter::TRACE)
}

/// Tracing layer that counts SQL statements and times each RPC dispatch,
/// warning when either exceeds its budget. See the module docs.
#[allow(clippy::struct_field_names)] // each field is a distinct named threshold; the suffix is the meaning
pub struct RpcProfileLayer {
    statement_threshold: u64,
    compound_statement_threshold: u64,
    duration_threshold: Duration,
    network_duration_threshold: Duration,
}

impl RpcProfileLayer {
    pub fn new(
        statement_threshold: u64,
        compound_statement_threshold: u64,
        duration_threshold: Duration,
        network_duration_threshold: Duration,
    ) -> Self {
        Self {
            statement_threshold,
            compound_statement_threshold,
            duration_threshold,
            network_duration_threshold,
        }
    }

    /// Build with defaults, honoring the [`STATEMENT_THRESHOLD_ENV`] /
    /// [`COMPOUND_STATEMENT_THRESHOLD_ENV`] / [`DURATION_THRESHOLD_ENV`] /
    /// [`NETWORK_DURATION_THRESHOLD_ENV`] overrides (unparseable values fall
    /// back to the defaults).
    pub fn from_env() -> Self {
        Self::from_env_with(|var| std::env::var(var).ok())
    }

    /// [`from_env`](Self::from_env) with an injectable variable lookup so
    /// tests never mutate process-global env (which races under parallel
    /// `cargo test`).
    fn from_env_with(get: impl Fn(&str) -> Option<String>) -> Self {
        let parse = |var: &str, default: u64| {
            get(var)
                .and_then(|s| s.trim().parse().ok())
                .unwrap_or(default)
        };
        Self::new(
            parse(STATEMENT_THRESHOLD_ENV, DEFAULT_STATEMENT_WARN_THRESHOLD),
            parse(
                COMPOUND_STATEMENT_THRESHOLD_ENV,
                DEFAULT_COMPOUND_STATEMENT_WARN_THRESHOLD,
            ),
            Duration::from_millis(parse(DURATION_THRESHOLD_ENV, DEFAULT_DURATION_WARN_MS)),
            Duration::from_millis(parse(
                NETWORK_DURATION_THRESHOLD_ENV,
                DEFAULT_NETWORK_DURATION_WARN_MS,
            )),
        )
    }

    /// The statement budget that applies to `method`: its own budget when
    /// [`per_method_statement_budget`] has one, the compound-op tier when
    /// [`is_compound_statement_method`] matches, the default tier otherwise.
    fn statement_threshold_for(&self, method: &str) -> u64 {
        if let Some(budget) = per_method_statement_budget(method) {
            budget
        } else if is_compound_statement_method(method) {
            self.compound_statement_threshold
        } else {
            self.statement_threshold
        }
    }

    /// The duration budget that applies to `method`: the network tier for
    /// [`is_network_tier_method`] matches, the default tier otherwise.
    fn duration_threshold_for(&self, method: &str) -> Duration {
        if is_network_tier_method(method) {
            self.network_duration_threshold
        } else {
            self.duration_threshold
        }
    }
}

/// Span-extension state for one in-flight dispatch.
struct DispatchProfile {
    method: String,
    statements: u64,
    started: Instant,
}

/// Extracts the `method` field recorded on the dispatch span.
struct MethodVisitor<'a>(&'a mut String);

impl Visit for MethodVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "method" {
            self.0.push_str(value);
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "method" && self.0.is_empty() {
            use std::fmt::Write;
            let _ = write!(self.0, "{value:?}");
        }
    }
}

impl<S> Layer<S> for RpcProfileLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let meta = attrs.metadata();
        if meta.target() != RPC_DISPATCH_SPAN_TARGET || meta.name() != RPC_DISPATCH_SPAN_NAME {
            return;
        }
        let mut method = String::new();
        attrs.record(&mut MethodVisitor(&mut method));
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(DispatchProfile {
                method,
                statements: 0,
                started: Instant::now(),
            });
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if event.metadata().target() != SQLX_QUERY_TARGET {
            return;
        }
        // Attribute the statement to the nearest enclosing dispatch span, if
        // any (sqlx-sqlite enters the caller's span on its worker thread).
        let Some(scope) = ctx.event_scope(event) else {
            return;
        };
        for span in scope {
            if let Some(profile) = span.extensions_mut().get_mut::<DispatchProfile>() {
                profile.statements += 1;
                return;
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else { return };
        let Some(profile) = span.extensions_mut().remove::<DispatchProfile>() else {
            return;
        };
        let elapsed = profile.started.elapsed();
        let elapsed_ms =
            u64::try_from(elapsed.as_millis().min(u128::from(u64::MAX))).unwrap_or(u64::MAX);
        let statement_threshold = self.statement_threshold_for(&profile.method);
        if profile.statements > statement_threshold {
            tracing::warn!(
                target: WARN_TARGET,
                method = %profile.method,
                statements = profile.statements,
                threshold = statement_threshold,
                elapsed_ms,
                "rpc dispatch exceeded SQL statement budget"
            );
        }
        let duration_threshold = self.duration_threshold_for(&profile.method);
        if elapsed > duration_threshold {
            tracing::warn!(
                target: WARN_TARGET,
                method = %profile.method,
                statements = profile.statements,
                threshold_ms = u64::try_from(duration_threshold.as_millis().min(u128::from(u64::MAX))).unwrap_or(u64::MAX),
                elapsed_ms,
                "rpc dispatch exceeded duration budget"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Write as _;
    use std::sync::{Arc, Mutex};
    use tracing_subscriber::layer::SubscriberExt;

    /// Test layer capturing every event as `(target, rendered fields)`.
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<(String, String)>>>);

    struct FieldsVisitor(String);

    impl Visit for FieldsVisitor {
        fn record_str(&mut self, field: &Field, value: &str) {
            let _ = write!(self.0, "{}={} ", field.name(), value);
        }
        fn record_u64(&mut self, field: &Field, value: u64) {
            let _ = write!(self.0, "{}={} ", field.name(), value);
        }
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            let _ = write!(self.0, "{}={:?} ", field.name(), value);
        }
    }

    impl<S: Subscriber> Layer<S> for Capture {
        fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
            let mut visitor = FieldsVisitor(String::new());
            event.record(&mut visitor);
            self.0
                .lock()
                .unwrap()
                .push((event.metadata().target().to_string(), visitor.0));
        }
    }

    fn warns(capture: &Capture) -> Vec<String> {
        capture
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|(target, _)| target == WARN_TARGET)
            .map(|(_, fields)| fields.clone())
            .collect()
    }

    /// Run `f` inside a dispatch-shaped span under a subscriber composed of
    /// the given profile layer plus a capture layer; returns captured warns.
    fn run_dispatch(layer: RpcProfileLayer, method: &str, f: impl FnOnce()) -> Vec<String> {
        let capture = Capture::default();
        let subscriber = tracing_subscriber::registry()
            .with(layer)
            .with(capture.clone());
        tracing::subscriber::with_default(subscriber, || {
            let span =
                tracing::info_span!(target: RPC_DISPATCH_SPAN_TARGET, "rpc_dispatch", method);
            span.in_scope(f);
            drop(span);
        });
        warns(&capture)
    }

    fn sqlx_event() {
        tracing::event!(target: "sqlx::query", tracing::Level::DEBUG, summary = "SELECT …");
    }

    #[test]
    fn over_threshold_emits_exactly_one_statement_warn() {
        let layer =
            RpcProfileLayer::new(2, 2, Duration::from_secs(3600), Duration::from_secs(3600));
        let warns = run_dispatch(layer, "workspace.list", || {
            sqlx_event();
            sqlx_event();
            sqlx_event();
        });
        assert_eq!(warns.len(), 1, "warns: {warns:?}");
        assert!(warns[0].contains("method=workspace.list"), "{warns:?}");
        assert!(warns[0].contains("statements=3"), "{warns:?}");
    }

    #[test]
    fn at_threshold_emits_no_warn() {
        let layer =
            RpcProfileLayer::new(2, 2, Duration::from_secs(3600), Duration::from_secs(3600));
        let warns = run_dispatch(layer, "workspace.list", || {
            sqlx_event();
            sqlx_event();
        });
        assert!(warns.is_empty(), "warns: {warns:?}");
    }

    #[test]
    fn slow_dispatch_emits_duration_warn() {
        let layer = RpcProfileLayer::new(
            u64::MAX,
            u64::MAX,
            Duration::from_millis(0),
            Duration::from_millis(0),
        );
        let warns = run_dispatch(layer, "git.diffs", || {
            std::thread::sleep(Duration::from_millis(2));
        });
        assert_eq!(warns.len(), 1, "warns: {warns:?}");
        assert!(warns[0].contains("method=git.diffs"), "{warns:?}");
        assert!(warns[0].contains("threshold_ms=0"), "{warns:?}");
    }

    #[test]
    fn statements_outside_dispatch_span_are_not_counted() {
        let capture = Capture::default();
        let subscriber = tracing_subscriber::registry()
            .with(RpcProfileLayer::new(
                0,
                0,
                Duration::from_secs(3600),
                Duration::from_secs(3600),
            ))
            .with(capture.clone());
        tracing::subscriber::with_default(subscriber, || {
            sqlx_event();
            sqlx_event();
            let span = tracing::info_span!(target: RPC_DISPATCH_SPAN_TARGET, "rpc_dispatch", method = "workspace.get");
            span.in_scope(|| {});
            drop(span);
        });
        assert!(warns(&capture).is_empty(), "warns: {:?}", warns(&capture));
    }

    #[test]
    fn network_tier_method_under_network_threshold_emits_no_warn() {
        // Below the network-tier threshold but above the default one: only
        // the network budget should apply to a network-tier method.
        let layer = RpcProfileLayer::new(
            u64::MAX,
            u64::MAX,
            Duration::from_millis(0),
            Duration::from_secs(3600),
        );
        let warns = run_dispatch(layer, "github.listIssues", || {
            std::thread::sleep(Duration::from_millis(2));
        });
        assert!(warns.is_empty(), "warns: {warns:?}");
    }

    #[test]
    fn network_tier_method_over_network_threshold_warns_with_network_threshold() {
        let layer = RpcProfileLayer::new(
            u64::MAX,
            u64::MAX,
            Duration::from_secs(3600),
            Duration::from_millis(0),
        );
        let warns = run_dispatch(layer, "pr.refresh", || {
            std::thread::sleep(Duration::from_millis(2));
        });
        assert_eq!(warns.len(), 1, "warns: {warns:?}");
        assert!(warns[0].contains("method=pr.refresh"), "{warns:?}");
        assert!(warns[0].contains("threshold_ms=0"), "{warns:?}");
    }

    #[test]
    fn non_network_method_over_default_threshold_still_warns() {
        // A high network-tier budget must not affect a non-network method:
        // it should still warn against the default threshold.
        let layer = RpcProfileLayer::new(
            u64::MAX,
            u64::MAX,
            Duration::from_millis(0),
            Duration::from_secs(3600),
        );
        let warns = run_dispatch(layer, "workspace.list", || {
            std::thread::sleep(Duration::from_millis(2));
        });
        assert_eq!(warns.len(), 1, "warns: {warns:?}");
        assert!(warns[0].contains("method=workspace.list"), "{warns:?}");
        assert!(warns[0].contains("threshold_ms=0"), "{warns:?}");
    }

    #[test]
    fn compound_tier_method_under_compound_threshold_emits_no_warn() {
        // Above the default statement budget but within the compound-op one:
        // only the compound budget should apply to a compound-tier method.
        let layer =
            RpcProfileLayer::new(2, 5, Duration::from_secs(3600), Duration::from_secs(3600));
        let warns = run_dispatch(layer, "workspace.create", || {
            sqlx_event();
            sqlx_event();
            sqlx_event();
        });
        assert!(warns.is_empty(), "warns: {warns:?}");
    }

    #[test]
    fn compound_tier_method_over_compound_threshold_warns_with_compound_threshold() {
        let layer = RpcProfileLayer::new(
            u64::MAX,
            2,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        );
        let warns = run_dispatch(layer, "workspace.create", || {
            sqlx_event();
            sqlx_event();
            sqlx_event();
        });
        assert_eq!(warns.len(), 1, "warns: {warns:?}");
        assert!(warns[0].contains("method=workspace.create"), "{warns:?}");
        assert!(warns[0].contains("statements=3"), "{warns:?}");
        assert!(warns[0].contains("threshold=2"), "{warns:?}");
    }

    #[test]
    fn non_compound_method_over_default_statement_threshold_still_warns() {
        // A high compound-op budget must not affect a non-compound method:
        // it should still warn against the default statement threshold.
        let layer = RpcProfileLayer::new(
            2,
            u64::MAX,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        );
        let warns = run_dispatch(layer, "workspace.list", || {
            sqlx_event();
            sqlx_event();
            sqlx_event();
        });
        assert_eq!(warns.len(), 1, "warns: {warns:?}");
        assert!(warns[0].contains("method=workspace.list"), "{warns:?}");
        assert!(warns[0].contains("threshold=2"), "{warns:?}");
    }

    #[test]
    fn from_env_overrides_defaults() {
        let layer = RpcProfileLayer::from_env_with(|var| match var {
            STATEMENT_THRESHOLD_ENV => Some("3".to_string()),
            COMPOUND_STATEMENT_THRESHOLD_ENV => Some("70".to_string()),
            DURATION_THRESHOLD_ENV => Some("50".to_string()),
            NETWORK_DURATION_THRESHOLD_ENV => Some("500".to_string()),
            _ => None,
        });
        assert_eq!(layer.statement_threshold, 3);
        assert_eq!(layer.compound_statement_threshold, 70);
        assert_eq!(layer.duration_threshold, Duration::from_millis(50));
        assert_eq!(layer.network_duration_threshold, Duration::from_millis(500));

        let defaults = RpcProfileLayer::from_env_with(|_| None);
        assert_eq!(
            defaults.statement_threshold,
            DEFAULT_STATEMENT_WARN_THRESHOLD
        );
        assert_eq!(
            defaults.compound_statement_threshold,
            DEFAULT_COMPOUND_STATEMENT_WARN_THRESHOLD
        );
        assert_eq!(
            defaults.duration_threshold,
            Duration::from_millis(DEFAULT_DURATION_WARN_MS)
        );
        assert_eq!(
            defaults.network_duration_threshold,
            Duration::from_millis(DEFAULT_NETWORK_DURATION_WARN_MS)
        );

        let unparseable = RpcProfileLayer::from_env_with(|_| Some("nonsense".to_string()));
        assert_eq!(
            unparseable.statement_threshold,
            DEFAULT_STATEMENT_WARN_THRESHOLD
        );
        assert_eq!(
            unparseable.compound_statement_threshold,
            DEFAULT_COMPOUND_STATEMENT_WARN_THRESHOLD
        );
        assert_eq!(
            unparseable.duration_threshold,
            Duration::from_millis(DEFAULT_DURATION_WARN_MS)
        );
        assert_eq!(
            unparseable.network_duration_threshold,
            Duration::from_millis(DEFAULT_NETWORK_DURATION_WARN_MS)
        );
    }

    #[test]
    fn is_network_tier_method_matches_prefixes_and_exact_methods() {
        assert!(is_network_tier_method("github.listIssues"));
        assert!(is_network_tier_method("linear.listIssues"));
        assert!(is_network_tier_method("sentry.listIssues"));
        assert!(is_network_tier_method("pr.refresh"));
        assert!(is_network_tier_method("pr.state"));
        assert!(is_network_tier_method("workspace.create"));
        assert!(!is_network_tier_method("workspace.list"));
        assert!(!is_network_tier_method("pr.list"));
        assert!(!is_network_tier_method("github"));
    }

    #[test]
    fn is_compound_statement_method_matches_exact_members_only() {
        assert!(is_compound_statement_method("workspace.create"));
        assert!(is_compound_statement_method("workspace.delete"));
        assert!(is_compound_statement_method("workspace.import.commit"));
        assert!(is_compound_statement_method("workspace.unarchive"));
        assert!(is_compound_statement_method("workspace.restore"));
        assert!(!is_compound_statement_method("workspace.list"));
        assert!(!is_compound_statement_method("workspace.get"));
        assert!(!is_compound_statement_method("workspace.archive"));
        assert!(!is_compound_statement_method("workspace.duplicate"));
        assert!(!is_compound_statement_method("workspace.import.begin"));
        assert!(!is_compound_statement_method("workspace"));
    }

    #[test]
    fn statement_threshold_for_selects_tier() {
        let layer = RpcProfileLayer::from_env_with(|_| None);
        assert_eq!(
            layer.statement_threshold_for("workspace.create"),
            DEFAULT_COMPOUND_STATEMENT_WARN_THRESHOLD
        );
        assert_eq!(
            layer.statement_threshold_for("workspace.delete"),
            DEFAULT_COMPOUND_STATEMENT_WARN_THRESHOLD
        );
        assert_eq!(
            layer.statement_threshold_for("workspace.import.commit"),
            DEFAULT_COMPOUND_STATEMENT_WARN_THRESHOLD
        );
        assert_eq!(
            layer.statement_threshold_for("workspace.unarchive"),
            DEFAULT_COMPOUND_STATEMENT_WARN_THRESHOLD
        );
        assert_eq!(
            layer.statement_threshold_for("workspace.restore"),
            DEFAULT_COMPOUND_STATEMENT_WARN_THRESHOLD
        );
        assert_eq!(
            layer.statement_threshold_for("agent.sendMessage"),
            per_method_statement_budget("agent.sendMessage").unwrap()
        );
        assert_eq!(
            layer.statement_threshold_for("agent.sendToTask"),
            per_method_statement_budget("agent.sendToTask").unwrap()
        );
        assert_eq!(
            layer.statement_threshold_for("workspace.list"),
            DEFAULT_STATEMENT_WARN_THRESHOLD
        );
    }

    #[test]
    fn per_method_budget_matches_exact_members_only() {
        assert_eq!(per_method_statement_budget("agent.sendMessage"), Some(250));
        assert_eq!(per_method_statement_budget("agent.sendToTask"), Some(250));
        assert_eq!(per_method_statement_budget("agent.list"), None);
        assert_eq!(per_method_statement_budget("workspace.create"), None);
    }

    #[test]
    fn unarchive_at_observed_ceiling_emits_no_warn_under_defaults() {
        // 40 statements observed on a real unarchive
        // (intent-hq/monorepo#3496) must fit the compound-op budget.
        let layer = RpcProfileLayer::from_env_with(|_| None);
        let warns = run_dispatch(layer, "workspace.unarchive", || {
            for _ in 0..40 {
                sqlx_event();
            }
        });
        assert!(warns.is_empty(), "warns: {warns:?}");
    }

    #[test]
    fn restore_at_observed_ceiling_emits_no_warn_under_defaults() {
        // `workspace.restore` is the wire alias of `workspace.unarchive`
        // (`WorkspaceApi::restore_workspace` delegates to
        // `unarchive_workspace`), so the same ~40-statement shape
        // (intent-hq/monorepo#3496) must fit its budget too.
        let layer = RpcProfileLayer::from_env_with(|_| None);
        let warns = run_dispatch(layer, "workspace.restore", || {
            for _ in 0..40 {
                sqlx_event();
            }
        });
        assert!(warns.is_empty(), "warns: {warns:?}");
    }

    #[test]
    fn send_to_task_at_observed_ceiling_emits_no_warn_under_defaults() {
        // `agent.sendToTask` routes through the same delivery path as
        // `agent.sendMessage` plus a task lookup, so the same content-scaled
        // ceiling (intent-hq/monorepo#3492) must fit its budget too.
        let layer = RpcProfileLayer::from_env_with(|_| None);
        let warns = run_dispatch(layer, "agent.sendToTask", || {
            for _ in 0..150 {
                sqlx_event();
            }
        });
        assert!(warns.is_empty(), "warns: {warns:?}");
    }

    #[test]
    fn unarchive_over_compound_budget_still_warns() {
        let layer = RpcProfileLayer::from_env_with(|_| None);
        let warns = run_dispatch(layer, "workspace.unarchive", || {
            for _ in 0..=DEFAULT_COMPOUND_STATEMENT_WARN_THRESHOLD {
                sqlx_event();
            }
        });
        assert_eq!(warns.len(), 1, "warns: {warns:?}");
        assert!(warns[0].contains("method=workspace.unarchive"), "{warns:?}");
        assert!(
            warns[0].contains(&format!(
                "threshold={DEFAULT_COMPOUND_STATEMENT_WARN_THRESHOLD}"
            )),
            "{warns:?}"
        );
    }

    #[test]
    fn send_message_at_observed_ceiling_emits_no_warn_under_defaults() {
        // 150 statements observed on a large coordinator send
        // (intent-hq/monorepo#3492) must fit the per-method budget.
        let layer = RpcProfileLayer::from_env_with(|_| None);
        let warns = run_dispatch(layer, "agent.sendMessage", || {
            for _ in 0..150 {
                sqlx_event();
            }
        });
        assert!(warns.is_empty(), "warns: {warns:?}");
    }

    #[test]
    fn send_message_over_per_method_budget_still_warns() {
        // Tier thresholds set to u64::MAX must not mask the per-method
        // budget: it takes precedence in both directions.
        let layer = RpcProfileLayer::new(
            u64::MAX,
            u64::MAX,
            Duration::from_secs(3600),
            Duration::from_secs(3600),
        );
        let budget = per_method_statement_budget("agent.sendMessage").unwrap();
        let warns = run_dispatch(layer, "agent.sendMessage", || {
            for _ in 0..=budget {
                sqlx_event();
            }
        });
        assert_eq!(warns.len(), 1, "warns: {warns:?}");
        assert!(warns[0].contains("method=agent.sendMessage"), "{warns:?}");
        assert!(
            warns[0].contains(&format!("threshold={budget}")),
            "{warns:?}"
        );
    }
}
