//! Data models for the Sentry engine.
//!
//! Field names serialize as `camelCase` to match the FE wire shapes
//! (`src/features/sentry-auth/types.ts`). Timestamps are kept as RFC-3339
//! strings (parity with the TS ground truth) rather than a datetime type.

use serde::{Deserialize, Serialize};

/// Issue status values reported by Sentry (`SentryIssueStatus` in the FE).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SentryIssueStatus {
    Resolved,
    Unresolved,
    Ignored,
}

/// Issue level/severity (`SentryIssueLevel` in the FE).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SentryIssueLevel {
    Error,
    Warning,
    Info,
    Fatal,
    Debug,
}

/// Status filter for `listIssues` — a superset of [`SentryIssueStatus`] that
/// adds `All` to omit the `is:<status>` clause from the search query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IssueStatusFilter {
    Resolved,
    #[default]
    Unresolved,
    Ignored,
    /// Match issues regardless of status. The default `is:<status>` clause
    /// is omitted entirely.
    All,
}

/// Request body for `listIssues` (mirrors the FE `FetchIssuesRequest`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchIssuesRequest {
    /// Project slug to filter by.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    /// Status filter (`unresolved` by default at the engine layer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<IssueStatusFilter>,
    /// Free-text search query appended to any `is:<status>` clause.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Page-size hint (clamped at the engine layer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    /// Raw Sentry page cursor from a previous page's `Link` header (threaded
    /// as the `cursor` query param); absent → first page. Engine-level only —
    /// not part of the FE request shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// Auth / connectivity probe result (`sentry.authStatus`).
///
/// Mirrors the FE `SentryAuthState` shape: `authenticated` plus the resolved
/// organization slug, and an `error` description when the probe failed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentryAuthState {
    pub authenticated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Project descriptor returned by `sentry.listProjects` (P1 read).
///
/// Mirrors the FE `SentryProject` (`sentry-auth/types.ts`) field-for-field
/// (`id`, `slug`, `name`, optional `platform`, optional `isMember`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentryProject {
    pub id: String,
    pub slug: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_member: Option<bool>,
}

/// The flattened issue shape returned by `sentry.listIssues` /
/// `sentry.searchIssues`.
///
/// Mirrors the FE `SentryIssueResult` (`sentry-auth/types.ts`) field-for-field.
/// Relations are flattened (project → `projectName`/`projectSlug`,
/// metadata → `type`/`value`/`filename`/`function`, `permalink` → `url`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentryIssueResult {
    pub id: String,
    pub short_id: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub culprit: Option<String>,
    pub status: SentryIssueStatus,
    pub level: SentryIssueLevel,
    /// Total event count (Sentry returns this as a string).
    pub count: String,
    pub user_count: u64,
    pub first_seen: String,
    pub last_seen: String,
    pub project_name: String,
    pub project_slug: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Error type from `metadata.type` (e.g. "`TypeError`").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    /// Error message detail from `metadata.value`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Filename from `metadata.filename`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// Function name from `metadata.function`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub function: Option<String>,
}

/// One engine-level page of issues backing `sentry.listIssues` /
/// `sentry.searchIssues`.
///
/// `next_token` carries the **raw** Sentry page cursor parsed from the
/// response `Link` header (`rel="next"` with `results="true"`) and is absent
/// on the last page. The services layer wraps it into the opaque base64 wire
/// `nextToken` (and emits explicit `null` on the last page) per the §5.5
/// uniform-pagination conventions — this struct never crosses the wire
/// directly.
#[derive(Debug, Clone, PartialEq)]
pub struct SentryIssuePage {
    pub issues: Vec<SentryIssueResult>,
    pub next_token: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issue_status_deserializes_lowercase() {
        let s: SentryIssueStatus = serde_json::from_str("\"resolved\"").unwrap();
        assert_eq!(s, SentryIssueStatus::Resolved);
    }

    #[test]
    fn issue_level_deserializes_lowercase() {
        let l: SentryIssueLevel = serde_json::from_str("\"fatal\"").unwrap();
        assert_eq!(l, SentryIssueLevel::Fatal);
    }

    #[test]
    fn status_filter_default_is_unresolved() {
        assert_eq!(IssueStatusFilter::default(), IssueStatusFilter::Unresolved);
        let f: IssueStatusFilter = serde_json::from_str("\"all\"").unwrap();
        assert_eq!(f, IssueStatusFilter::All);
    }

    #[test]
    fn issue_result_serializes_camel_case_and_omits_none() {
        let issue = SentryIssueResult {
            id: "1".into(),
            short_id: "PROJ-1".into(),
            title: "boom".into(),
            culprit: None,
            status: SentryIssueStatus::Unresolved,
            level: SentryIssueLevel::Error,
            count: "12".into(),
            user_count: 3,
            first_seen: "2026-01-01T00:00:00Z".into(),
            last_seen: "2026-01-02T00:00:00Z".into(),
            project_name: "Web".into(),
            project_slug: "web".into(),
            url: Some("https://sentry.io/organizations/acme/issues/1/".into()),
            r#type: Some("TypeError".into()),
            value: None,
            filename: Some("src/app.ts".into()),
            function: None,
        };
        let v = serde_json::to_value(&issue).unwrap();
        assert_eq!(v["shortId"], "PROJ-1");
        assert_eq!(v["userCount"], 3);
        assert_eq!(v["projectSlug"], "web");
        assert_eq!(v["firstSeen"], "2026-01-01T00:00:00Z");
        assert_eq!(v["type"], "TypeError");
        assert!(v.get("culprit").is_none());
        assert!(v.get("value").is_none());
        assert!(v.get("function").is_none());
    }

    #[test]
    fn project_serializes_camel_case_and_omits_none() {
        let p = SentryProject {
            id: "p1".into(),
            slug: "web".into(),
            name: "Web".into(),
            platform: None,
            is_member: Some(true),
        };
        let v = serde_json::to_value(&p).unwrap();
        assert_eq!(v["slug"], "web");
        assert_eq!(v["isMember"], true);
        assert!(v.get("platform").is_none());

        let p: SentryProject = serde_json::from_value(serde_json::json!({
            "id": "p2", "slug": "api", "name": "API", "platform": "python",
        }))
        .unwrap();
        assert_eq!(p.platform.as_deref(), Some("python"));
        assert!(p.is_member.is_none());
    }

    #[test]
    fn auth_state_serializes_camel_case_and_omits_none() {
        let a = SentryAuthState {
            authenticated: true,
            organization: Some("acme".into()),
            error: None,
        };
        let v = serde_json::to_value(&a).unwrap();
        assert_eq!(v["authenticated"], true);
        assert_eq!(v["organization"], "acme");
        assert!(v.get("error").is_none());

        let a = SentryAuthState {
            authenticated: false,
            organization: None,
            error: Some("invalid token".into()),
        };
        let v = serde_json::to_value(&a).unwrap();
        assert_eq!(v["authenticated"], false);
        assert_eq!(v["error"], "invalid token");
        assert!(v.get("organization").is_none());
    }
}
