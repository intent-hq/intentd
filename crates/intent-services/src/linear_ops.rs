//! Wire-policy glue for the read-only `linear.*` methods (PROTOCOL §5.28).
//!
//! The engine + GraphQL transport live in `intent-linear`; this module owns
//! only the parity-critical wire glue so it stays unit-testable without a
//! network: resolving the active [`LinearEngine`] (injected handle else the
//! registry-built engine from default settings, i.e. `LINEAR_API_KEY` /
//! keychain), validating the `filter` enum, and mapping engine errors onto the
//! domain `Internal` error (→ `-32603`). It mirrors `pr_ops::resolve_source_control`.
//!
//! GUARDRAIL: the Linear API key is a secret. It never appears here — the
//! engine reads it internally and only derived identity crosses the wire.

use std::sync::Arc;

use intent_core::{Error, Result};
use intent_linear::{IssueFilter, LinearEngine, LinearRegistry, LinearSettings};

/// Map a Linear engine/registry error onto the domain `Internal` error
/// (→ `-32603`): a missing/invalid key (`NotConfigured`) and any other Linear
/// failure both surface as `Internal` with a descriptive message (§5.28, §9).
pub(crate) fn map_linear_err(e: intent_linear::Error) -> Error {
    Error::Internal(e.to_string())
}

/// Resolve the active [`LinearEngine`]: the injected handle (tests / explicit
/// wiring) else the registry-built engine from default settings (key from
/// `LINEAR_API_KEY` / keychain, §5.28). A missing key yields `Internal`
/// (graceful "not configured"), never a panic.
pub(crate) fn resolve_engine(
    injected: Option<Arc<dyn LinearEngine>>,
) -> Result<Arc<dyn LinearEngine>> {
    match injected {
        Some(engine) => Ok(engine),
        None => LinearRegistry::from_settings(&LinearSettings::default()).map_err(map_linear_err),
    }
}

/// Validate/default the `linear.listIssues` `filter` (default `assigned`); an
/// invalid value is rejected with `InvalidParams` (→ `-32602`, §5.28).
pub(crate) fn parse_filter(filter: Option<String>) -> Result<IssueFilter> {
    match filter.as_deref() {
        None | Some("assigned") => Ok(IssueFilter::Assigned),
        Some("created") => Ok(IssueFilter::Created),
        Some("subscribed") => Ok(IssueFilter::Subscribed),
        Some("team") => Ok(IssueFilter::Team),
        Some("all") => Ok(IssueFilter::All),
        Some(_) => Err(Error::InvalidParams(
            "filter must be one of: assigned, created, subscribed, team, all".to_string(),
        )),
    }
}

/// Narrow the wire `limit` (`i64`) to the engine's `Option<u32>`; non-positive
/// or out-of-range values fall through to the engine's own default/clamp.
pub(crate) fn wire_limit(limit: Option<i64>) -> Option<u32> {
    limit.and_then(|n| u32::try_from(n).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_filter_with_default() {
        assert_eq!(parse_filter(None).unwrap(), IssueFilter::Assigned);
        assert_eq!(
            parse_filter(Some("assigned".into())).unwrap(),
            IssueFilter::Assigned
        );
        assert_eq!(
            parse_filter(Some("created".into())).unwrap(),
            IssueFilter::Created
        );
        assert_eq!(
            parse_filter(Some("subscribed".into())).unwrap(),
            IssueFilter::Subscribed
        );
        assert_eq!(
            parse_filter(Some("team".into())).unwrap(),
            IssueFilter::Team
        );
        assert_eq!(parse_filter(Some("all".into())).unwrap(), IssueFilter::All);
    }

    #[test]
    fn rejects_invalid_filter() {
        assert!(matches!(
            parse_filter(Some("bogus".into())),
            Err(Error::InvalidParams(_))
        ));
    }

    #[test]
    fn narrows_wire_limit() {
        assert_eq!(wire_limit(None), None);
        assert_eq!(wire_limit(Some(50)), Some(50));
        assert_eq!(wire_limit(Some(-1)), None);
    }

    #[test]
    fn maps_not_configured_to_internal() {
        let mapped = map_linear_err(intent_linear::Error::NotConfigured("no key".into()));
        assert!(matches!(mapped, Error::Internal(_)));
    }
}
