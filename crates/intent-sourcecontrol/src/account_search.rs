//! Bounded public account suggestions for invitations, independent of repository auth.
//! A selected instance determines a fixed API path; redirects are never followed.
use crate::{Error, GitlabHost, Result, UserIdentity};
use serde::Deserialize;
use serde_json::Value;

const MAX_RESPONSE_BYTES: usize = 256 * 1024;

#[derive(Deserialize)]
struct Account {
    id: u64,
    #[serde(alias = "username")]
    login: String,
    name: Option<String>,
    avatar_url: Option<String>,
}

/// Search public GitHub usernames. The optional API root is daemon configuration,
/// never a client-supplied URL. No repository credential is loaded.
///
/// # Errors
/// Returns provider auth/rate/transport/decode errors without treating them as no hits.
pub async fn github(api_base: Option<&str>, query: &str, limit: u8) -> Result<Vec<UserIdentity>> {
    let base = api_base
        .unwrap_or("https://api.github.com")
        .trim_end_matches('/');
    let query = crate::github::build_user_search_query(query);
    search(
        &format!("{base}/search/users"),
        None,
        "q",
        &query,
        limit,
        true,
    )
    .await
}

/// Search one page of the selected GitLab instance's directory. Callers decide
/// whether a credential belongs to this instance; this client never follows redirects.
///
/// # Errors
/// Returns auth for restricted directories, rate for throttling, and API/decode
/// errors for unavailable or malformed directories.
pub async fn gitlab(
    host: &GitlabHost,
    token: Option<&str>,
    query: &str,
    limit: u8,
) -> Result<Vec<UserIdentity>> {
    search(
        &format!("{}/users", host.api_base()),
        token,
        "search",
        query,
        limit,
        false,
    )
    .await
}

async fn search(
    url: &str,
    token: Option<&str>,
    key: &str,
    query: &str,
    limit: u8,
    github: bool,
) -> Result<Vec<UserIdentity>> {
    let client = reqwest::Client::builder()
        .connect_timeout(crate::github::CONNECT_TIMEOUT)
        .timeout(crate::github::READ_WRITE_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("intentd/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|e| Error::Config(format!("account search client: {e}")))?;
    let limit = limit.clamp(1, 10);
    let mut request = client.get(url).query(&[
        (key, query),
        ("per_page", &limit.to_string()),
        ("page", "1"),
    ]);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let mut response = request
        .send()
        .await
        .map_err(|_| Error::Api("account search request failed".into()))?;
    let status = response.status();
    if response
        .content_length()
        .is_some_and(|n| n > MAX_RESPONSE_BYTES as u64)
    {
        return Err(Error::Decode("account search response too large".into()));
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| Error::Api("account search response failed".into()))?
    {
        if bytes.len() + chunk.len() > MAX_RESPONSE_BYTES {
            return Err(Error::Decode("account search response too large".into()));
        }
        bytes.extend_from_slice(&chunk);
    }
    if status.as_u16() == 429
        || (github
            && status.as_u16() == 403
            && serde_json::from_slice::<Value>(&bytes)
                .ok()
                .and_then(|v| v["message"].as_str().map(str::to_ascii_lowercase))
                .is_some_and(|s| s.contains("rate limit")))
    {
        return Err(Error::RateLimited("account search rate limited".into()));
    }
    if matches!(status.as_u16(), 401 | 403) {
        return Err(Error::Auth(
            "account directory requires authorized access".into(),
        ));
    }
    if !status.is_success() {
        return Err(Error::Api(format!(
            "account directory unavailable ({status})"
        )));
    }
    let mut value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| Error::Decode("invalid account search response".into()))?;
    if github {
        value = value
            .get_mut("items")
            .map(Value::take)
            .ok_or_else(|| Error::Decode("account search items missing".into()))?;
    }
    let accounts: Vec<Account> = serde_json::from_value(value)
        .map_err(|_| Error::Decode("invalid account search users".into()))?;
    Ok(accounts
        .into_iter()
        .take(usize::from(limit))
        .map(|u| UserIdentity {
            id: Some(u.id),
            login: u.login,
            name: u.name,
            avatar_url: u.avatar_url,
            html_url: None,
        })
        .collect())
}

#[cfg(test)]
mod tests;
