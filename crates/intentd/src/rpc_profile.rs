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
//!   (default [`DEFAULT_DURATION_WARN_MS`] ms; catches fs walks / git scans
//!   that never touch SQLite).
//!
//! Both thresholds are overridable via [`STATEMENT_THRESHOLD_ENV`] and
//! [`DURATION_THRESHOLD_ENV`] (read once at layer construction). Logging only
//! — no wire-contract impact; overhead is one span per dispatch plus a
//! counter increment per statement.

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
/// Default duration threshold in milliseconds: a dispatch running longer than
/// this draws a WARN.
pub const DEFAULT_DURATION_WARN_MS: u64 = 1000;
/// Env override for the statement-count threshold (u64).
pub const STATEMENT_THRESHOLD_ENV: &str = "INTENTD_RPC_STATEMENT_WARN_THRESHOLD";
/// Env override for the duration threshold in milliseconds (u64).
pub const DURATION_THRESHOLD_ENV: &str = "INTENTD_RPC_DURATION_WARN_MS";

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
pub struct RpcProfileLayer {
    statement_threshold: u64,
    duration_threshold: Duration,
}

impl RpcProfileLayer {
    pub fn new(statement_threshold: u64, duration_threshold: Duration) -> Self {
        Self {
            statement_threshold,
            duration_threshold,
        }
    }

    /// Build with defaults, honoring the [`STATEMENT_THRESHOLD_ENV`] /
    /// [`DURATION_THRESHOLD_ENV`] overrides (unparseable values fall back to
    /// the defaults).
    pub fn from_env() -> Self {
        Self::new(
            env_u64(STATEMENT_THRESHOLD_ENV, DEFAULT_STATEMENT_WARN_THRESHOLD),
            Duration::from_millis(env_u64(DURATION_THRESHOLD_ENV, DEFAULT_DURATION_WARN_MS)),
        )
    }
}

fn env_u64(var: &str, default: u64) -> u64 {
    std::env::var(var)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(default)
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
        let elapsed_ms = elapsed.as_millis().min(u128::from(u64::MAX)) as u64;
        if profile.statements > self.statement_threshold {
            tracing::warn!(
                target: WARN_TARGET,
                method = %profile.method,
                statements = profile.statements,
                threshold = self.statement_threshold,
                elapsed_ms,
                "rpc dispatch exceeded SQL statement budget"
            );
        }
        if elapsed > self.duration_threshold {
            tracing::warn!(
                target: WARN_TARGET,
                method = %profile.method,
                statements = profile.statements,
                threshold_ms = self.duration_threshold.as_millis().min(u128::from(u64::MAX)) as u64,
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
        let layer = RpcProfileLayer::new(2, Duration::from_secs(3600));
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
        let layer = RpcProfileLayer::new(2, Duration::from_secs(3600));
        let warns = run_dispatch(layer, "workspace.list", || {
            sqlx_event();
            sqlx_event();
        });
        assert!(warns.is_empty(), "warns: {warns:?}");
    }

    #[test]
    fn slow_dispatch_emits_duration_warn() {
        let layer = RpcProfileLayer::new(u64::MAX, Duration::from_millis(0));
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
            .with(RpcProfileLayer::new(0, Duration::from_secs(3600)))
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
    fn from_env_overrides_defaults() {
        std::env::set_var(STATEMENT_THRESHOLD_ENV, "3");
        std::env::set_var(DURATION_THRESHOLD_ENV, "50");
        let layer = RpcProfileLayer::from_env();
        std::env::remove_var(STATEMENT_THRESHOLD_ENV);
        std::env::remove_var(DURATION_THRESHOLD_ENV);
        assert_eq!(layer.statement_threshold, 3);
        assert_eq!(layer.duration_threshold, Duration::from_millis(50));

        let defaults = RpcProfileLayer::from_env();
        assert_eq!(
            defaults.statement_threshold,
            DEFAULT_STATEMENT_WARN_THRESHOLD
        );
        assert_eq!(
            defaults.duration_threshold,
            Duration::from_millis(DEFAULT_DURATION_WARN_MS)
        );
    }
}
