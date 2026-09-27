//! Logical GitLab instance identity, independent of a fixture's transport origin.
//!
//! Boundary predicates adapt Bert Colemont's Apache-2.0 contribution in
//! anubissbe/intentd (dcd9150b, 97d041cd); no settings/credential migration lives here.
use reqwest::Url;

use crate::{Error, Result};

/// Canonical logical HTTPS root, including a non-default port and installation prefix.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GitlabInstance(Url);

impl GitlabInstance {
    /// Parse a bare host or full HTTPS instance root. Project path case is untouched.
    ///
    /// # Errors
    /// Rejects ambiguous paths, userinfo, non-HTTPS, query and fragment components.
    pub fn parse(input: &str) -> Result<Self> {
        let mut url = parse_root(input, false)?;
        let path = url.path().trim_end_matches('/').to_owned();
        url.set_path(&path);
        Ok(Self(url))
    }

    /// The canonical origin and optional installation prefix.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str().trim_end_matches('/')
    }

    /// Whether a resource URL belongs to this origin and installation boundary.
    #[must_use]
    pub fn contains_url(&self, input: &str) -> bool {
        if ambiguous(input) {
            return false;
        }
        let Ok(url) = Url::parse(input) else {
            return false;
        };
        clean_url(&url) && url.origin() == self.0.origin() && path_within(self.0.path(), url.path())
    }
}

/// An admitted logical instance plus the independently approved request endpoint.
/// Production construction uses the logical root. The only override in this
/// increment is an explicit loopback fixture; deployment bindings remain owned
/// by the settings/auth integration and are not inferred from legacy host fields.
#[derive(Debug, Clone)]
pub struct GitlabDescriptor {
    instance: GitlabInstance,
    endpoint: Url,
}

impl GitlabDescriptor {
    /// Use the logical HTTPS instance for transport.
    #[must_use]
    pub fn new(instance: GitlabInstance) -> Self {
        let endpoint = instance.0.clone();
        Self { instance, endpoint }
    }

    /// Explicit local HTTP(S) fixture endpoint. Never changes logical identity.
    ///
    /// # Errors
    /// Rejects non-loopback or malformed endpoints.
    pub fn with_loopback_endpoint(instance: GitlabInstance, endpoint: &str) -> Result<Self> {
        let endpoint = parse_root(endpoint, true)?;
        if !loopback(&endpoint) {
            return Err(Error::Config(
                "GitLab fixture endpoint must be loopback".into(),
            ));
        }
        Ok(Self { instance, endpoint })
    }

    /// Logical instance, for request authority checks and result identity.
    #[must_use]
    pub fn instance(&self) -> &GitlabInstance {
        &self.instance
    }

    pub(crate) fn api_base(&self) -> Url {
        let mut api = self.endpoint.clone();
        api.set_path(&format!(
            "{}/api/v4/",
            self.endpoint.path().trim_end_matches('/')
        ));
        api
    }
}

fn loopback(url: &Url) -> bool {
    url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    })
}
fn clean_url(url: &Url) -> bool {
    url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.query().is_none()
        && url.fragment().is_none()
}
fn ambiguous(input: &str) -> bool {
    let folded = input.to_ascii_lowercase();
    input
        .chars()
        .any(|c| c.is_control() || c.is_whitespace() || c == '\\' || c == '*')
        || ["%2e", "%2f", "%5c", "%25", "%00"]
            .iter()
            .any(|s| folded.contains(s))
        || input.split('/').any(|s| s == "." || s == "..")
}
fn path_within(prefix: &str, path: &str) -> bool {
    let prefix = prefix.trim_end_matches('/');
    prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|rest| rest.starts_with('/'))
}
fn parse_root(input: &str, local_http: bool) -> Result<Url> {
    // Reject embedded whitespace/control characters before Url can discard them.
    if input.is_empty() || ambiguous(input) {
        return Err(Error::Config("GitLab instance root is ambiguous".into()));
    }
    let full = if input.contains("://") {
        input.into()
    } else {
        format!("https://{input}")
    };
    let url =
        Url::parse(&full).map_err(|_| Error::Config("invalid GitLab instance root".into()))?;
    if !clean_url(&url)
        || url.path().contains("//")
        || !(url.scheme() == "https" || local_http && url.scheme() == "http" && loopback(&url))
    {
        return Err(Error::Config(
            "GitLab requires an unambiguous HTTPS instance root".into(),
        ));
    }
    Ok(url)
}
