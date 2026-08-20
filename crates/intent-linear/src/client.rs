//! Raw GraphQL transport for Linear.
//!
//! Linear is GraphQL-only: a single `POST https://api.linear.app/graphql`
//! endpoint. We hand-write query strings + typed serde DTOs (a full GraphQL
//! client crate is overkill for the small consumed surface) and validate the
//! `{ data, errors }` envelope ourselves, mirroring `github.rs`'s
//! `graphql_data` pattern.
//!
//! GUARDRAIL: the API key is a secret. It is stored only to build the
//! `Authorization` header and is never logged or exposed via `Debug`.

use serde_json::{json, Value};

use crate::error::{Error, Result};

/// Default Linear GraphQL endpoint.
pub(crate) const LINEAR_API_URL: &str = "https://api.linear.app/graphql";

/// Thin GraphQL transport over `reqwest`.
pub(crate) struct LinearClient {
    http: reqwest::Client,
    /// Secret API key. Never logged, printed, or surfaced via `Debug`.
    api_key: String,
    base_url: String,
}

impl std::fmt::Debug for LinearClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinearClient")
            .field("base_url", &self.base_url)
            .field("api_key", &"<redacted>")
            .finish()
    }
}

impl LinearClient {
    /// Build a client from a personal API key, optionally targeting a custom
    /// endpoint (defaults to [`LINEAR_API_URL`]).
    pub fn new(api_key: &str, base_url: Option<&str>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| Error::Config(format!("failed to build http client: {e}")))?;
        Ok(Self {
            http,
            api_key: api_key.to_string(),
            base_url: base_url.unwrap_or(LINEAR_API_URL).to_string(),
        })
    }

    /// Execute a GraphQL `query` with `variables` and return the validated
    /// `data` object.
    pub async fn graphql(&self, query: &str, variables: Value) -> Result<Value> {
        let body = json!({ "query": query, "variables": variables });
        let resp = self
            .http
            .post(&self.base_url)
            .header(
                reqwest::header::AUTHORIZATION,
                auth_header_value(&self.api_key),
            )
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .json(&body)
            .send()
            .await?;
        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            return Err(Error::Auth(format!("linear returned {status}")));
        }
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(Error::RateLimited(format!("linear returned {status}")));
        }
        let envelope: Value = resp.json().await?;
        graphql_data(envelope)
    }
}

/// Build the `Authorization` header value.
///
/// Linear **personal API keys** (`lin_api_…`) use the raw key with **no**
/// `Bearer` prefix; OAuth access tokens use the standard `Bearer <token>`
/// form. Pure + testable so we never need a live key.
pub(crate) fn auth_header_value(key: &str) -> String {
    if key.starts_with("lin_api_") {
        key.to_string()
    } else {
        format!("Bearer {key}")
    }
}

/// Validate a GraphQL envelope and return its `data` object (parity with
/// `github.rs::graphql_data` / the FE `validateGraphQLResponse`).
pub(crate) fn graphql_data(mut resp: Value) -> Result<Value> {
    if let Some(errors) = resp.get("errors").and_then(Value::as_array) {
        if !errors.is_empty() {
            let msg = errors[0]
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("unknown GraphQL error");
            return Err(Error::Api(format!("graphql error: {msg}")));
        }
    }
    match resp.get_mut("data") {
        Some(data) if !data.is_null() => Ok(data.take()),
        _ => Err(Error::Api("graphql response returned no data".to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn personal_key_has_no_bearer_prefix() {
        assert_eq!(auth_header_value("lin_api_secret"), "lin_api_secret");
    }

    #[test]
    fn oauth_token_uses_bearer_prefix() {
        assert_eq!(auth_header_value("oauth_abc"), "Bearer oauth_abc");
    }

    #[test]
    fn graphql_data_extracts_and_validates() {
        let data = graphql_data(json!({ "data": { "x": 1 } })).unwrap();
        assert_eq!(data, json!({ "x": 1 }));

        let err =
            graphql_data(json!({ "errors": [{ "message": "boom" }], "data": null })).unwrap_err();
        assert!(matches!(err, Error::Api(m) if m.contains("boom")));

        let no_data = graphql_data(json!({ "data": null })).unwrap_err();
        assert!(matches!(no_data, Error::Api(_)));
    }

    #[test]
    fn debug_redacts_api_key() {
        let client = LinearClient::new("lin_api_supersecret", None).unwrap();
        let dbg = format!("{client:?}");
        assert!(!dbg.contains("supersecret"));
        assert!(dbg.contains("redacted"));
    }
}
