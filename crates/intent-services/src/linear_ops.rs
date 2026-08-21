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
use intent_linear::{
    CreateIssueRequest, IssueFilter, LinearEngine, LinearRegistry, LinearSettings,
    UpdateIssueRequest,
};

/// Map a Linear engine/registry error onto the domain `Internal` error
/// (→ `-32603`): a missing/invalid key (`NotConfigured`) and any other Linear
/// failure both surface as `Internal` with a descriptive message (§5.28, §9).
pub(crate) fn map_linear_err(e: &intent_linear::Error) -> Error {
    Error::Internal(e.to_string())
}

/// Resolve the active [`LinearEngine`]: the injected handle (tests / explicit
/// wiring) else the registry-built engine from default settings (key from
/// `LINEAR_API_KEY` / keychain, §5.28). A missing key yields `Internal`
/// (graceful "not configured"), never a panic. Async because the keychain
/// lookup runs on the blocking pool with a bounded timeout so a wedged OS
/// keychain never blocks the async runtime.
pub(crate) async fn resolve_engine(
    injected: Option<Arc<dyn LinearEngine>>,
) -> Result<Arc<dyn LinearEngine>> {
    match injected {
        Some(engine) => Ok(engine),
        None => LinearRegistry::from_settings(&LinearSettings::default())
            .await
            .map_err(|e: intent_linear::Error| map_linear_err(&e)),
    }
}

/// Validate/default the `linear.listIssues` `filter` (default `assigned`); an
/// invalid value is rejected with `InvalidParams` (→ `-32602`, §5.28).
pub(crate) fn parse_filter(filter: Option<&str>) -> Result<IssueFilter> {
    match filter {
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

/// Parse and validate a `linear.createIssue` request (§5.28). Rejects with
/// `InvalidParams` (→ `-32602`) when `title` or `teamId` is missing/empty or
/// when the JSON shape itself is invalid (e.g. `priority` not numeric). The
/// router enforces the same `-32602` contract before this is called.
pub(crate) fn parse_create_issue(request: serde_json::Value) -> Result<CreateIssueRequest> {
    let req: CreateIssueRequest = serde_json::from_value(request)
        .map_err(|e| Error::InvalidParams(format!("invalid createIssue request: {e}")))?;
    if req.title.trim().is_empty() {
        return Err(Error::InvalidParams(
            "Missing required parameter: title".to_string(),
        ));
    }
    if req.team_id.trim().is_empty() {
        return Err(Error::InvalidParams(
            "Missing required parameter: teamId".to_string(),
        ));
    }
    Ok(req)
}

/// Parse and validate a `linear.updateIssue` request (§5.28). Rejects with
/// `InvalidParams` (→ `-32602`) when `issueId` is missing/empty or the JSON
/// shape itself is invalid. The router enforces the same `-32602` contract
/// before this is called.
pub(crate) fn parse_update_issue(request: serde_json::Value) -> Result<UpdateIssueRequest> {
    let req: UpdateIssueRequest = serde_json::from_value(request)
        .map_err(|e| Error::InvalidParams(format!("invalid updateIssue request: {e}")))?;
    if req.issue_id.trim().is_empty() {
        return Err(Error::InvalidParams(
            "Missing required parameter: issueId".to_string(),
        ));
    }
    Ok(req)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_filter_with_default() {
        assert_eq!(parse_filter(None).unwrap(), IssueFilter::Assigned);
        assert_eq!(
            parse_filter(Some("assigned")).unwrap(),
            IssueFilter::Assigned
        );
        assert_eq!(parse_filter(Some("created")).unwrap(), IssueFilter::Created);
        assert_eq!(
            parse_filter(Some("subscribed")).unwrap(),
            IssueFilter::Subscribed
        );
        assert_eq!(parse_filter(Some("team")).unwrap(), IssueFilter::Team);
        assert_eq!(parse_filter(Some("all")).unwrap(), IssueFilter::All);
    }

    #[test]
    fn rejects_invalid_filter() {
        assert!(matches!(
            parse_filter(Some("bogus")),
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
        let mapped = map_linear_err(&intent_linear::Error::NotConfigured("no key".into()));
        assert!(matches!(mapped, Error::Internal(_)));
    }

    #[test]
    fn parse_create_issue_accepts_minimal_required() {
        let req = parse_create_issue(serde_json::json!({
            "title": "Do it",
            "teamId": "team-uuid",
        }))
        .unwrap();
        assert_eq!(req.title, "Do it");
        assert_eq!(req.team_id, "team-uuid");
    }

    #[test]
    fn parse_create_issue_rejects_missing_title() {
        for body in [
            serde_json::json!({ "teamId": "t1" }),
            serde_json::json!({ "title": "", "teamId": "t1" }),
            serde_json::json!({ "title": "   ", "teamId": "t1" }),
        ] {
            assert!(matches!(
                parse_create_issue(body),
                Err(Error::InvalidParams(_))
            ));
        }
    }

    #[test]
    fn parse_create_issue_rejects_missing_team_id() {
        for body in [
            serde_json::json!({ "title": "X" }),
            serde_json::json!({ "title": "X", "teamId": "" }),
        ] {
            assert!(matches!(
                parse_create_issue(body),
                Err(Error::InvalidParams(_))
            ));
        }
    }

    #[test]
    fn parse_update_issue_accepts_minimal_required() {
        let req = parse_update_issue(serde_json::json!({ "issueId": "uuid-1" })).unwrap();
        assert_eq!(req.issue_id, "uuid-1");
        assert!(req.title.is_none());
    }

    #[test]
    fn parse_update_issue_rejects_missing_issue_id() {
        for body in [
            serde_json::json!({}),
            serde_json::json!({ "issueId": "" }),
            serde_json::json!({ "title": "X" }),
        ] {
            assert!(matches!(
                parse_update_issue(body),
                Err(Error::InvalidParams(_))
            ));
        }
    }
}
