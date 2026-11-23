//! Wire-policy glue for the read-only `sentry.*` methods (PROTOCOL §5.29).
//!
//! The engine + REST transport live in `intent-sentry`; this module owns only
//! the parity-critical wire glue so it stays unit-testable without a network:
//! building the [`SentryEngine`] from the saved account settings, validating
//! the `status` enum, and mapping engine errors onto the domain `Internal`
//! error (→ `-32603`).
//!
//! GUARDRAIL: the Sentry auth token is a secret. It is only handed to the
//! engine; only derived identity (the org slug) crosses the wire.

use std::sync::Arc;

use intent_core::{Error, Result};
use intent_sentry::{IssueStatusFilter, SentryEngine, SentryRegistry, SentrySettings};

/// Map a Sentry engine/registry error onto the domain `Internal` error
/// (→ `-32603`): a missing/invalid credential pair (`NotConfigured`) and any
/// other Sentry failure both surface as `Internal` with a descriptive
/// message (§5.29, §9).
pub(crate) fn map_sentry_err(e: &intent_sentry::Error) -> Error {
    Error::Internal(e.to_string())
}

/// Settings path of the Sentry organization slug (non-secret, `config.toml`).
pub(crate) const ORGANIZATION_SETTING_PATH: &str = "accounts.sentry.organization";
/// Settings path, and secrets-store account, of the Sentry API token.
pub(crate) const TOKEN_SETTING_PATH: &str = "accounts.sentry.token";

/// Build the [`SentryEngine`] from `settings` (the saved account, falling
/// back to `SENTRY_ORG` / `SENTRY_API_TOKEN` per half when `host_env`). A
/// missing pair yields `Internal` (graceful "not configured"), never a panic.
pub(crate) fn build_engine(
    settings: &SentrySettings,
    host_env: bool,
) -> Result<Arc<dyn SentryEngine>> {
    SentryRegistry::from_settings_with_host_env(settings, host_env)
        .map_err(|e: intent_sentry::Error| map_sentry_err(&e))
}

/// Validate/default the `sentry.listIssues` `status` (default `unresolved`);
/// an invalid value is rejected with `InvalidParams` (→ `-32602`, §5.29).
/// `None` returns `None` so the engine applies its own default policy
/// (matches the FE which omits the field).
pub(crate) fn parse_status(status: Option<&str>) -> Result<Option<IssueStatusFilter>> {
    match status {
        None => Ok(None),
        Some("unresolved") => Ok(Some(IssueStatusFilter::Unresolved)),
        Some("resolved") => Ok(Some(IssueStatusFilter::Resolved)),
        Some("ignored") => Ok(Some(IssueStatusFilter::Ignored)),
        Some("all") => Ok(Some(IssueStatusFilter::All)),
        Some(_) => Err(Error::InvalidParams(
            "status must be one of: unresolved, resolved, ignored, all".to_string(),
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
    fn parses_status_with_default() {
        assert_eq!(parse_status(None).unwrap(), None);
        assert_eq!(
            parse_status(Some("unresolved")).unwrap(),
            Some(IssueStatusFilter::Unresolved)
        );
        assert_eq!(
            parse_status(Some("resolved")).unwrap(),
            Some(IssueStatusFilter::Resolved)
        );
        assert_eq!(
            parse_status(Some("ignored")).unwrap(),
            Some(IssueStatusFilter::Ignored)
        );
        assert_eq!(
            parse_status(Some("all")).unwrap(),
            Some(IssueStatusFilter::All)
        );
    }

    #[test]
    fn rejects_invalid_status() {
        assert!(matches!(
            parse_status(Some("bogus")),
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
        let mapped = map_sentry_err(&intent_sentry::Error::NotConfigured("no creds".into()));
        assert!(matches!(mapped, Error::Internal(_)));
    }
}
