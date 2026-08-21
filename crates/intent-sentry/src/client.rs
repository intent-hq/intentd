//! Raw REST transport for Sentry.
//!
//! Sentry exposes a REST API rooted at `https://sentry.io/api/0`. Auth is a
//! `Bearer <token>` header; responses are JSON, errors come back as
//! standard HTTP statuses (401/403/404/429/...). We hand-build typed
//! [`crate::model`] DTOs and a small `get` helper rather than depending on a
//! generated client (the consumed surface is small).
//!
//! GUARDRAIL: the API token is a secret. It is stored only to build the
//! `Authorization` header and is never logged or exposed via `Debug`.

use serde_json::Value;

use crate::error::{Error, Result};

/// Default Sentry REST base URL (`SENTRY_API_BASE_URL` in the FE).
pub(crate) const SENTRY_API_BASE_URL: &str = "https://sentry.io/api/0";

/// Thin REST transport over `reqwest`.
pub(crate) struct SentryClient {
    http: reqwest::Client,
    /// Secret API token. Never logged, printed, or surfaced via `Debug`.
    token: String,
    /// Organization slug used to build org-scoped URLs.
    organization: String,
    base_url: String,
}

impl std::fmt::Debug for SentryClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SentryClient")
            .field("base_url", &self.base_url)
            .field("organization", &self.organization)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl SentryClient {
    /// Build a client from a token + org slug, optionally targeting a custom
    /// endpoint (defaults to [`SENTRY_API_BASE_URL`]).
    pub fn new(token: &str, organization: &str, base_url: Option<&str>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| Error::Config(format!("failed to build http client: {e}")))?;
        Ok(Self {
            http,
            token: token.to_string(),
            organization: organization.to_string(),
            base_url: base_url.unwrap_or(SENTRY_API_BASE_URL).to_string(),
        })
    }

    /// The org slug bound to this client.
    pub fn organization(&self) -> &str {
        &self.organization
    }

    /// Execute `GET {base}{path}` and return the parsed JSON body. `path`
    /// should start with `/`.
    pub async fn get(&self, path: &str) -> Result<Value> {
        self.get_with_query(path, &[]).await
    }

    /// Execute `GET {base}{path}?<params>` and return the parsed JSON body.
    /// `params` is a slice of `(name, value)` pairs which reqwest
    /// percent-encodes for us.
    pub(crate) async fn get_with_query(
        &self,
        path: &str,
        params: &[(&str, &str)],
    ) -> Result<Value> {
        Ok(self.get_with_query_paged(path, params).await?.0)
    }

    /// Execute `GET {base}{path}?<params>` and return the parsed JSON body
    /// plus the next-page cursor from the response `Link` header (`None` when
    /// no further page exists). Used by the cursor-paginated issue reads.
    pub(crate) async fn get_with_query_paged(
        &self,
        path: &str,
        params: &[(&str, &str)],
    ) -> Result<(Value, Option<String>)> {
        let url = format!("{}{}", self.base_url, path);
        let mut req = self
            .http
            .get(&url)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.token),
            )
            .header(reqwest::header::ACCEPT, "application/json");
        if !params.is_empty() {
            req = req.query(params);
        }
        let resp = req.send().await?;
        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(Error::Auth(format!("sentry returned {status}")));
        }
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(Error::NotFound(format!("sentry returned {status}")));
        }
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(Error::RateLimited(format!("sentry returned {status}")));
        }
        if !status.is_success() {
            return Err(Error::Api(format!("sentry returned {status}")));
        }
        let next_cursor = resp
            .headers()
            .get(reqwest::header::LINK)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_next_cursor);
        let body: Value = resp.json().await?;
        Ok((body, next_cursor))
    }

    /// Execute `PUT {base}{path}` with `body` as JSON and return the parsed
    /// JSON response. Used for the P2 write mutations (`resolveIssue`,
    /// `ignoreIssue`, `assignIssue`).
    pub(crate) async fn put_json(&self, path: &str, body: Value) -> Result<Value> {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .http
            .put(&url)
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", self.token),
            )
            .header(reqwest::header::ACCEPT, "application/json")
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(Error::Auth(format!("sentry returned {status}")));
        }
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(Error::NotFound(format!("sentry returned {status}")));
        }
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(Error::RateLimited(format!("sentry returned {status}")));
        }
        if !status.is_success() {
            return Err(Error::Api(format!("sentry returned {status}")));
        }
        let body: Value = resp.json().await?;
        Ok(body)
    }
}

/// Parse the next-page cursor out of a Sentry `Link` response header.
///
/// Sentry paginates with a header of the shape
/// `<url>; rel="previous"; results="false"; cursor="0:0:1", <url>;
/// rel="next"; results="true"; cursor="0:100:0"` — the cursor is returned
/// only from the `rel="next"` entry when `results="true"` (i.e. another page
/// actually exists; Sentry emits `results="false"` on the last page).
pub(crate) fn parse_next_cursor(link_header: &str) -> Option<String> {
    for entry in link_header.split(',') {
        let mut rel_next = false;
        let mut has_results = false;
        let mut cursor: Option<String> = None;
        for attr in entry.split(';').skip(1) {
            let Some((key, value)) = attr.split_once('=') else {
                continue;
            };
            let value = value.trim().trim_matches('"');
            match key.trim() {
                "rel" => rel_next = value == "next",
                "results" => has_results = value == "true",
                "cursor" => cursor = Some(value.to_string()),
                _ => {}
            }
        }
        if rel_next && has_results {
            return cursor;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_token() {
        let client = SentryClient::new("sntrys_supersecret", "acme", None).unwrap();
        let dbg = format!("{client:?}");
        assert!(!dbg.contains("supersecret"));
        assert!(dbg.contains("redacted"));
        assert!(dbg.contains("acme"));
        assert!(dbg.contains(SENTRY_API_BASE_URL));
    }

    #[test]
    fn organization_accessor_returns_slug() {
        let client = SentryClient::new("tok", "acme", None).unwrap();
        assert_eq!(client.organization(), "acme");
    }

    #[test]
    fn parses_next_cursor_when_results_true() {
        let header = "<https://sentry.io/api/0/organizations/acme/issues/?&cursor=0:0:1>; \
                      rel=\"previous\"; results=\"false\"; cursor=\"0:0:1\", \
                      <https://sentry.io/api/0/organizations/acme/issues/?&cursor=0:100:0>; \
                      rel=\"next\"; results=\"true\"; cursor=\"0:100:0\"";
        assert_eq!(parse_next_cursor(header).as_deref(), Some("0:100:0"));
    }

    #[test]
    fn no_cursor_when_results_false_or_header_malformed() {
        // Last page: rel="next" present but results="false" → no cursor.
        let header = "<https://sentry.io/api/0/organizations/acme/issues/?&cursor=0:0:1>; \
                      rel=\"previous\"; results=\"false\"; cursor=\"0:0:1\", \
                      <https://sentry.io/api/0/organizations/acme/issues/?&cursor=0:200:0>; \
                      rel=\"next\"; results=\"false\"; cursor=\"0:200:0\"";
        assert_eq!(parse_next_cursor(header), None);

        // Only a previous entry.
        let header =
            "<https://sentry.io/api/0/x/>; rel=\"previous\"; results=\"true\"; cursor=\"0:0:1\"";
        assert_eq!(parse_next_cursor(header), None);

        // Missing cursor attribute / degenerate inputs.
        let header = "<https://sentry.io/api/0/x/>; rel=\"next\"; results=\"true\"";
        assert_eq!(parse_next_cursor(header), None);
        assert_eq!(parse_next_cursor(""), None);
        assert_eq!(parse_next_cursor("garbage"), None);
    }
}
