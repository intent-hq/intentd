//! `SentryEngine` — the P0 read surface backing `sentry.*`.
//!
//! Three methods mirror the FE calls that exist today: `authStatus`,
//! `listIssues`, `searchIssues`. The Sentry REST API returns a flat issue
//! envelope; [`map_issue`] flattens project + metadata onto
//! [`SentryIssueResult`] so the wire shape stays identical to the FE.

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::client::SentryClient;
use crate::error::{Error, Result};
use crate::model::{
    FetchIssuesRequest, IssueStatusFilter, SentryAuthState, SentryIssueLevel, SentryIssuePage,
    SentryIssueResult, SentryIssueStatus, SentryProject,
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
    /// `request.cursor` is the raw Sentry page cursor from a previous page
    /// (threaded as the `cursor` query param); absent → first page. The
    /// returned page carries the next cursor only when another page exists.
    async fn list_issues(&self, request: FetchIssuesRequest) -> Result<SentryIssuePage>;

    /// Full-text search issues. Delegates to [`Self::list_issues`] with the
    /// supplied query; status defaults to `unresolved`. `cursor` paginates
    /// like [`Self::list_issues`].
    async fn search_issues(
        &self,
        query: &str,
        project: Option<&str>,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<SentryIssuePage>;

    /// List projects for the configured organization (P1 read).
    /// `GET /organizations/{org}/projects/`.
    async fn list_projects(&self, limit: Option<u32>) -> Result<Vec<SentryProject>>;

    /// Fetch a single issue by id or shortId (P1 read).
    ///
    /// String shape selects the path: a shortId (e.g. `WEB-1`) is resolved via
    /// a `?query=` lookup on the org `/issues/` endpoint; everything else is
    /// fetched directly from `GET /organizations/{org}/issues/{id}/`.
    async fn get_issue(&self, id_or_short_id: &str) -> Result<SentryIssueResult>;

    /// Mutate an issue's status to `resolved` (P2 write).
    /// `PUT /organizations/{org}/issues/{id}/` body `{"status":"resolved"}`.
    async fn resolve_issue(&self, id: &str) -> Result<SentryIssueResult>;

    /// Mutate an issue's status to `ignored` (P2 write).
    /// `PUT /organizations/{org}/issues/{id}/` body `{"status":"ignored"}`.
    async fn ignore_issue(&self, id: &str) -> Result<SentryIssueResult>;

    /// Assign an issue (P2 write). `assigned_to=None` clears the assignment
    /// (`{"assignedTo": null}`). `PUT /organizations/{org}/issues/{id}/`.
    async fn assign_issue(&self, id: &str, assigned_to: Option<&str>) -> Result<SentryIssueResult>;
}

/// REST-backed [`SentryEngine`] over [`SentryClient`].
#[derive(Debug)]
pub(crate) struct SentryEngineImpl {
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

    async fn list_issues(&self, request: FetchIssuesRequest) -> Result<SentryIssuePage> {
        let path = format!("/organizations/{}/issues/", self.client.organization());
        let limit = clamp_limit(request.limit).to_string();
        let query = build_query(request.status, request.query.as_deref());
        let params = build_issue_params(
            query.as_deref(),
            request.project.as_deref(),
            &limit,
            request.cursor.as_deref(),
        );
        let (data, next_cursor) = self.client.get_with_query_paged(&path, &params).await?;
        Ok(SentryIssuePage {
            issues: map_issue_nodes(&data),
            next_token: next_cursor,
        })
    }

    async fn search_issues(
        &self,
        query: &str,
        project: Option<&str>,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<SentryIssuePage> {
        self.list_issues(FetchIssuesRequest {
            project: project.map(str::to_string),
            status: None,
            query: Some(query.to_string()),
            limit,
            cursor: cursor.map(str::to_string),
        })
        .await
    }

    async fn list_projects(&self, limit: Option<u32>) -> Result<Vec<SentryProject>> {
        let path = format!("/organizations/{}/projects/", self.client.organization());
        let lim = clamp_limit(limit).to_string();
        let params: Vec<(&str, &str)> = vec![("limit", &lim)];
        let data = self.client.get_with_query(&path, &params).await?;
        Ok(map_project_nodes(&data))
    }

    async fn get_issue(&self, id_or_short_id: &str) -> Result<SentryIssueResult> {
        if looks_like_short_id(id_or_short_id) {
            let path = format!("/organizations/{}/issues/", self.client.organization());
            let params: Vec<(&str, &str)> = vec![("query", id_or_short_id), ("limit", "1")];
            let data = self.client.get_with_query(&path, &params).await?;
            let first = data
                .as_array()
                .and_then(|arr| arr.first())
                .ok_or_else(|| Error::NotFound(format!("issue {id_or_short_id} not found")))?;
            map_issue(first).ok_or_else(|| Error::Api("malformed issue payload".to_string()))
        } else {
            let path = format!(
                "/organizations/{}/issues/{}/",
                self.client.organization(),
                id_or_short_id
            );
            let data = self.client.get(&path).await?;
            map_issue(&data).ok_or_else(|| Error::Api("malformed issue payload".to_string()))
        }
    }

    async fn resolve_issue(&self, id: &str) -> Result<SentryIssueResult> {
        self.mutate_issue(id, json!({ "status": "resolved" })).await
    }

    async fn ignore_issue(&self, id: &str) -> Result<SentryIssueResult> {
        self.mutate_issue(id, json!({ "status": "ignored" })).await
    }

    async fn assign_issue(&self, id: &str, assigned_to: Option<&str>) -> Result<SentryIssueResult> {
        self.mutate_issue(id, build_assign_body(assigned_to)).await
    }
}

impl SentryEngineImpl {
    /// Shared `PUT` path for the three P2 write arms. Mutates the issue and
    /// returns the flattened result.
    async fn mutate_issue(&self, id: &str, body: Value) -> Result<SentryIssueResult> {
        let path = format!(
            "/organizations/{}/issues/{}/",
            self.client.organization(),
            id
        );
        let data = self.client.put_json(&path, body).await?;
        map_issue(&data).ok_or_else(|| Error::Api("malformed issue payload".to_string()))
    }
}

/// Clamp an optional limit into `[1, MAX_LIMIT]`, defaulting when absent.
pub(crate) fn clamp_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
}

/// Build the query params for the `/issues/` endpoint: optional `query` /
/// `project`, the clamped `limit`, and the `cursor` only when paginating past
/// the first page.
pub(crate) fn build_issue_params<'a>(
    query: Option<&'a str>,
    project: Option<&'a str>,
    limit: &'a str,
    cursor: Option<&'a str>,
) -> Vec<(&'static str, &'a str)> {
    let mut params: Vec<(&'static str, &'a str)> = Vec::with_capacity(4);
    if let Some(q) = query {
        params.push(("query", q));
    }
    if let Some(p) = project {
        params.push(("project", p));
    }
    params.push(("limit", limit));
    if let Some(c) = cursor {
        params.push(("cursor", c));
    }
    params
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

/// Map the JSON array returned by `/projects/` into flattened
/// [`SentryProject`]s. Non-array bodies map to an empty list.
pub(crate) fn map_project_nodes(data: &Value) -> Vec<SentryProject> {
    data.as_array()
        .map(|arr| arr.iter().filter_map(map_project).collect())
        .unwrap_or_default()
}

/// Map a single raw Sentry project node. Returns `None` when required fields
/// (`id`/`slug`/`name`) are missing.
pub(crate) fn map_project(node: &Value) -> Option<SentryProject> {
    let id = str_field(node, "id")?;
    let slug = str_field(node, "slug")?;
    let name = str_field(node, "name")?;
    Some(SentryProject {
        id,
        slug,
        name,
        platform: str_field(node, "platform"),
        is_member: node.get("isMember").and_then(Value::as_bool),
    })
}

/// Detect a Sentry issue shortId like `WEB-1` / `PROJ-123`: an uppercase
/// alphanumeric prefix (starting with a letter), a `-`, and a digits-only
/// suffix. Numeric ids and UUIDs return `false`.
pub(crate) fn looks_like_short_id(s: &str) -> bool {
    let mut parts = s.splitn(2, '-');
    let prefix = parts.next().unwrap_or_default();
    let suffix = parts.next().unwrap_or_default();
    if prefix.is_empty() || suffix.is_empty() {
        return false;
    }
    if !prefix
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_uppercase())
    {
        return false;
    }
    if !prefix
        .chars()
        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
    {
        return false;
    }
    suffix.chars().all(|c| c.is_ascii_digit())
}

/// Build the `PUT /issues/{id}/` body for `assignIssue`. `None` clears the
/// assignment (`{"assignedTo": null}`); `Some(user)` assigns to that user.
pub(crate) fn build_assign_body(assigned_to: Option<&str>) -> Value {
    match assigned_to {
        Some(s) => json!({ "assignedTo": s }),
        None => json!({ "assignedTo": Value::Null }),
    }
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
    fn issue_params_thread_cursor() {
        assert_eq!(
            build_issue_params(Some("is:unresolved"), Some("web"), "5", Some("0:100:0")),
            vec![
                ("query", "is:unresolved"),
                ("project", "web"),
                ("limit", "5"),
                ("cursor", "0:100:0"),
            ]
        );

        // First page: no cursor → the param is omitted entirely.
        assert_eq!(
            build_issue_params(None, None, "100", None),
            vec![("limit", "100")]
        );
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

    #[test]
    fn maps_project_node_with_optional_fields() {
        let p = map_project(&json!({
            "id": "1", "slug": "web", "name": "Web",
            "platform": "javascript", "isMember": true,
        }))
        .expect("project");
        assert_eq!(p.id, "1");
        assert_eq!(p.slug, "web");
        assert_eq!(p.platform.as_deref(), Some("javascript"));
        assert_eq!(p.is_member, Some(true));
    }

    #[test]
    fn maps_project_node_without_optional_fields() {
        let p = map_project(&json!({ "id": "2", "slug": "api", "name": "API" })).expect("project");
        assert!(p.platform.is_none());
        assert!(p.is_member.is_none());
    }

    #[test]
    fn skips_invalid_projects() {
        assert!(map_project(&json!({ "slug": "x", "name": "x" })).is_none());
        assert!(map_project(&json!({ "id": "1", "name": "x" })).is_none());
        assert!(map_project(&json!({ "id": "1", "slug": "x" })).is_none());
    }

    #[test]
    fn maps_project_nodes_array_or_empty() {
        assert!(map_project_nodes(&json!(null)).is_empty());
        assert!(map_project_nodes(&json!({})).is_empty());
        let v = map_project_nodes(&json!([
            { "id": "1", "slug": "web", "name": "Web" },
            { "id": "2", "slug": "api", "name": "API" },
        ]));
        assert_eq!(v.len(), 2);
        assert_eq!(v[1].slug, "api");
    }

    #[test]
    fn detects_short_id_by_shape() {
        assert!(looks_like_short_id("WEB-1"));
        assert!(looks_like_short_id("PROJ-123"));
        assert!(looks_like_short_id("WEB42-7"));
        // numeric ids and UUIDs are NOT short ids.
        assert!(!looks_like_short_id("123"));
        assert!(!looks_like_short_id("abc-1"));
        assert!(!looks_like_short_id("WEB"));
        assert!(!looks_like_short_id("WEB-"));
        assert!(!looks_like_short_id("-1"));
        assert!(!looks_like_short_id("WEB-1a"));
        assert!(!looks_like_short_id(""));
        assert!(!looks_like_short_id("550e8400-e29b-41d4-a716-446655440000"));
    }

    #[test]
    fn assign_body_serializes_with_explicit_null() {
        // None → explicit null (unassign).
        let v = build_assign_body(None);
        assert_eq!(v["assignedTo"], Value::Null);
        assert!(v.as_object().unwrap().contains_key("assignedTo"));
        // Some → string.
        let v = build_assign_body(Some("user-1"));
        assert_eq!(v["assignedTo"], "user-1");
    }
}
