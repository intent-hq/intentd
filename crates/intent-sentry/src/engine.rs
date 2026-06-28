//! `SentryEngine` — the P0 read surface backing `sentry.*`.
//!
//! Three methods mirror the FE calls that exist today: `authStatus`,
//! `listIssues`, `searchIssues`. The Sentry REST API returns a flat issue
//! envelope; [`map_issue`] flattens project + metadata onto
//! [`SentryIssueResult`] so the wire shape stays identical to the FE.

use async_trait::async_trait;
use serde_json::Value;

use crate::client::SentryClient;
use crate::error::{Error, Result};
use crate::model::{
    FetchIssuesRequest, IssueStatusFilter, SentryAuthState, SentryIssueLevel, SentryIssueResult,
    SentryIssueStatus,
};

/// Default page size when the caller does not specify one (matches the FE).
const DEFAULT_LIMIT: u32 = 100;
/// Sentry's `/issues/` endpoint caps `limit` at 100 per page.
const MAX_LIMIT: u32 = 100;

/// The P0 Sentry read API consumed by `sentry.*` wire methods.
#[async_trait]
pub trait SentryEngine: Send + Sync {
    /// Auth / connectivity probe via `GET /organizations/{org}/`.
    async fn auth_status(&self) -> Result<SentryAuthState>;

    /// List issues matching `request` (status, project, query, limit).
    async fn list_issues(&self, request: FetchIssuesRequest) -> Result<Vec<SentryIssueResult>>;

    /// Full-text search issues. Delegates to [`Self::list_issues`] with the
    /// supplied query; status defaults to `unresolved`.
    async fn search_issues(
        &self,
        query: &str,
        project: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Vec<SentryIssueResult>>;
}

/// REST-backed [`SentryEngine`] over [`SentryClient`].
#[derive(Debug)]
pub struct SentryEngineImpl {
    client: SentryClient,
}

impl SentryEngineImpl {
    /// Wrap an existing [`SentryClient`].
    pub fn new(client: SentryClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SentryEngine for SentryEngineImpl {
    async fn auth_status(&self) -> Result<SentryAuthState> {
        let path = format!("/organizations/{}/", self.client.organization());
        match self.client.get(&path).await {
            Ok(data) => Ok(map_auth_status_ok(self.client.organization(), &data)),
            Err(Error::Auth(msg)) | Err(Error::NotFound(msg)) => Ok(SentryAuthState {
                authenticated: false,
                organization: None,
                error: Some(msg),
            }),
            Err(e) => Err(e),
        }
    }

    async fn list_issues(&self, request: FetchIssuesRequest) -> Result<Vec<SentryIssueResult>> {
        let path = format!("/organizations/{}/issues/", self.client.organization());
        let limit = clamp_limit(request.limit).to_string();
        let query = build_query(request.status, request.query.as_deref());
        let mut params: Vec<(&str, &str)> = Vec::with_capacity(3);
        if let Some(q) = query.as_deref() {
            params.push(("query", q));
        }
        if let Some(project) = request.project.as_deref() {
            params.push(("project", project));
        }
        params.push(("limit", &limit));
        let data = self.client.get_with_query(&path, &params).await?;
        Ok(map_issue_nodes(&data))
    }

    async fn search_issues(
        &self,
        query: &str,
        project: Option<&str>,
        limit: Option<u32>,
    ) -> Result<Vec<SentryIssueResult>> {
        self.list_issues(FetchIssuesRequest {
            project: project.map(str::to_string),
            status: None,
            query: Some(query.to_string()),
            limit,
        })
        .await
    }
}

/// Clamp an optional limit into `[1, MAX_LIMIT]`, defaulting when absent.
pub(crate) fn clamp_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

/// Build the `query=` value for the issues endpoint, mirroring the FE's
/// composition rules in `sentry-auth.service.ts::fetchIssues`:
///
/// * `status=Some(other)` + free query → `"is:<status> <query>"`.
/// * `status=Some(other)` + no query   → `"is:<status>"`.
/// * `status=None`/`All`  + free query → `<query>` (no `is:` prefix).
/// * `status=None`        + no query   → `"is:unresolved"`.
/// * `status=All`         + no query   → `None` (omit the `query` param).
pub(crate) fn build_query(
    status: Option<IssueStatusFilter>,
    free_query: Option<&str>,
) -> Option<String> {
    let is_clause = match status {
        Some(IssueStatusFilter::Resolved) => Some("is:resolved"),
        Some(IssueStatusFilter::Unresolved) => Some("is:unresolved"),
        Some(IssueStatusFilter::Ignored) => Some("is:ignored"),
        Some(IssueStatusFilter::All) => None,
        None => {
            if free_query.is_none() {
                Some("is:unresolved")
            } else {
                None
            }
        }
    };
    let free = free_query.map(str::trim).filter(|s| !s.is_empty());
    match (is_clause, free) {
        (Some(c), Some(q)) => Some(format!("{c} {q}")),
        (Some(c), None) => Some(c.to_string()),
        (None, Some(q)) => Some(q.to_string()),
        (None, None) => None,
    }
}

/// Build the auth-status reply when the org probe succeeded.
pub(crate) fn map_auth_status_ok(org: &str, _data: &Value) -> SentryAuthState {
    SentryAuthState {
        authenticated: true,
        organization: Some(org.to_string()),
        error: None,
    }
}

/// Map the JSON array returned by `/issues/` into flattened results.
/// Sentry returns a top-level JSON array; non-array bodies (or `null`) map
/// to an empty list rather than an error.
pub(crate) fn map_issue_nodes(data: &Value) -> Vec<SentryIssueResult> {
    data.as_array()
        .map(|arr| arr.iter().filter_map(map_issue).collect())
        .unwrap_or_default()
}

/// Flatten a single raw Sentry issue node into [`SentryIssueResult`]. Returns
/// `None` when required fields (id/title/status/level/...) are missing or
/// malformed.
pub(crate) fn map_issue(node: &Value) -> Option<SentryIssueResult> {
    let id = str_field(node, "id")?;
    let short_id = str_field(node, "shortId").unwrap_or_default();
    let title = str_field(node, "title").unwrap_or_default();
    let status = parse_status(node.get("status").and_then(Value::as_str))?;
    let level = parse_level(node.get("level").and_then(Value::as_str))?;
    let count = node
        .get("count")
        .and_then(|v| {
            v.as_str()
                .map(str::to_string)
                .or_else(|| v.as_u64().map(|n| n.to_string()))
        })
        .unwrap_or_default();
    let user_count = node.get("userCount").and_then(Value::as_u64).unwrap_or(0);
    let first_seen = str_field(node, "firstSeen").unwrap_or_default();
    let last_seen = str_field(node, "lastSeen").unwrap_or_default();
    let project = node.get("project");
    let project_name = project
        .and_then(|p| str_field(p, "name"))
        .unwrap_or_default();
    let project_slug = project
        .and_then(|p| str_field(p, "slug"))
        .unwrap_or_default();
    let url = str_field(node, "permalink");
    let metadata = node.get("metadata");
    Some(SentryIssueResult {
        id,
        short_id,
        title,
        culprit: str_field(node, "culprit"),
        status,
        level,
        count,
        user_count,
        first_seen,
        last_seen,
        project_name,
        project_slug,
        url,
        r#type: metadata.and_then(|m| str_field(m, "type")),
        value: metadata.and_then(|m| str_field(m, "value")),
        filename: metadata.and_then(|m| str_field(m, "filename")),
        function: metadata.and_then(|m| str_field(m, "function")),
    })
}

fn parse_status(s: Option<&str>) -> Option<SentryIssueStatus> {
    match s? {
        "resolved" => Some(SentryIssueStatus::Resolved),
        "unresolved" => Some(SentryIssueStatus::Unresolved),
        "ignored" => Some(SentryIssueStatus::Ignored),
        _ => None,
    }
}

fn parse_level(s: Option<&str>) -> Option<SentryIssueLevel> {
    match s? {
        "error" => Some(SentryIssueLevel::Error),
        "warning" => Some(SentryIssueLevel::Warning),
        "info" => Some(SentryIssueLevel::Info),
        "fatal" => Some(SentryIssueLevel::Fatal),
        "debug" => Some(SentryIssueLevel::Debug),
        _ => None,
    }
}

/// Read a non-null string field as an owned `String`.
fn str_field(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn clamps_limit() {
        assert_eq!(clamp_limit(None), DEFAULT_LIMIT);
        assert_eq!(clamp_limit(Some(0)), 1);
        assert_eq!(clamp_limit(Some(50)), 50);
        assert_eq!(clamp_limit(Some(9999)), MAX_LIMIT);
    }

    #[test]
    fn status_filter_maps_to_query() {
        assert_eq!(
            build_query(Some(IssueStatusFilter::Resolved), None).as_deref(),
            Some("is:resolved")
        );
        assert_eq!(
            build_query(Some(IssueStatusFilter::Unresolved), None).as_deref(),
            Some("is:unresolved")
        );
        assert_eq!(
            build_query(Some(IssueStatusFilter::Ignored), None).as_deref(),
            Some("is:ignored")
        );
        assert_eq!(build_query(Some(IssueStatusFilter::All), None), None);
        assert_eq!(
            build_query(Some(IssueStatusFilter::Resolved), Some("foo")).as_deref(),
            Some("is:resolved foo")
        );
        assert_eq!(
            build_query(None, None).as_deref(),
            Some("is:unresolved"),
            "no status + no query defaults to is:unresolved",
        );
        assert_eq!(
            build_query(None, Some("foo")).as_deref(),
            Some("foo"),
            "no status + free query: no is: prefix",
        );
        assert_eq!(
            build_query(Some(IssueStatusFilter::All), Some("foo")).as_deref(),
            Some("foo"),
            "all + free query: no is: prefix",
        );
        // Blank free queries are treated as absent.
        assert_eq!(
            build_query(Some(IssueStatusFilter::Resolved), Some("   ")).as_deref(),
            Some("is:resolved")
        );
    }

    #[test]
    fn maps_auth_status_ok() {
        let s = map_auth_status_ok("acme", &json!({ "name": "Acme, Inc." }));
        assert!(s.authenticated);
        assert_eq!(s.organization.as_deref(), Some("acme"));
        assert!(s.error.is_none());
    }

    #[test]
    fn flattens_issue_node_with_metadata() {
        let node = json!({
            "id": "1",
            "shortId": "PROJ-1",
            "title": "TypeError: cannot read property",
            "culprit": "src/app.ts in render",
            "status": "unresolved",
            "level": "error",
            "count": "42",
            "userCount": 7,
            "firstSeen": "2026-01-01T00:00:00Z",
            "lastSeen": "2026-01-02T00:00:00Z",
            "project": { "id": "p1", "name": "Web", "slug": "web" },
            "permalink": "https://sentry.io/organizations/acme/issues/1/",
            "metadata": {
                "type": "TypeError",
                "value": "cannot read property",
                "filename": "src/app.ts",
                "function": "render"
            }
        });
        let r = map_issue(&node).expect("maps issue");
        assert_eq!(r.id, "1");
        assert_eq!(r.short_id, "PROJ-1");
        assert_eq!(r.status, SentryIssueStatus::Unresolved);
        assert_eq!(r.level, SentryIssueLevel::Error);
        assert_eq!(r.count, "42");
        assert_eq!(r.user_count, 7);
        assert_eq!(r.project_name, "Web");
        assert_eq!(r.project_slug, "web");
        assert_eq!(
            r.url.as_deref(),
            Some("https://sentry.io/organizations/acme/issues/1/")
        );
        assert_eq!(r.r#type.as_deref(), Some("TypeError"));
        assert_eq!(r.value.as_deref(), Some("cannot read property"));
        assert_eq!(r.filename.as_deref(), Some("src/app.ts"));
        assert_eq!(r.function.as_deref(), Some("render"));
    }

    #[test]
    fn flattens_issue_node_without_metadata_or_project() {
        let node = json!({
            "id": "2",
            "shortId": "PROJ-2",
            "title": "boom",
            "status": "resolved",
            "level": "warning",
            "count": 3,
            "firstSeen": "2026-01-01T00:00:00Z",
            "lastSeen": "2026-01-02T00:00:00Z",
        });
        let r = map_issue(&node).expect("maps issue");
        assert_eq!(r.count, "3", "numeric counts flatten to strings");
        assert_eq!(r.user_count, 0);
        assert_eq!(r.project_name, "");
        assert_eq!(r.project_slug, "");
        assert!(r.url.is_none());
        assert!(r.r#type.is_none());
        assert!(r.value.is_none());
        assert!(r.filename.is_none());
        assert!(r.function.is_none());
        assert!(r.culprit.is_none());
    }

    #[test]
    fn skips_invalid_issues() {
        // Missing id → None.
        assert!(map_issue(&json!({ "status": "resolved", "level": "error" })).is_none());
        // Unknown status → None.
        assert!(map_issue(&json!({ "id": "x", "status": "weird", "level": "error" })).is_none());
        // Unknown level → None.
        assert!(map_issue(&json!({ "id": "x", "status": "resolved", "level": "trace" })).is_none());
    }

    #[test]
    fn maps_empty_and_missing_nodes() {
        assert!(map_issue_nodes(&json!(null)).is_empty());
        assert!(map_issue_nodes(&json!({})).is_empty());
        assert!(map_issue_nodes(&json!([])).is_empty());
        let one = map_issue_nodes(&json!([
            { "id": "x", "shortId": "X-1", "title": "t",
              "status": "unresolved", "level": "error",
              "count": "1", "userCount": 0,
              "firstSeen": "2026-01-01T00:00:00Z",
              "lastSeen": "2026-01-01T00:00:00Z" }
        ]));
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].id, "x");
    }
}
