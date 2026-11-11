//! Whether a public image tag exists, asked of its OCI registry without
//! credentials, as Northflank will pull it.

use reqwest::StatusCode;
use serde::Deserialize;

use super::NorthflankClient;
use crate::cloud::{CloudError, CloudErrorKind};

const DOCKER_HUB: &str = "registry-1.docker.io";

/// Every manifest type a tag can name: a multi-platform index or one image.
const MANIFEST_TYPES: &str = "application/vnd.oci.image.index.v1+json, \
application/vnd.docker.distribution.manifest.list.v2+json, \
application/vnd.oci.image.manifest.v1+json, \
application/vnd.docker.distribution.manifest.v2+json";

/// An image reference split as a registry addresses it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImageRef {
    /// Host and optional port; Docker Hub is `registry-1.docker.io`.
    pub registry: String,
    /// For example `intent-hq/intent-node`, or `library/alpine`.
    pub repository: String,
    /// The tag, or a digest (`sha256:…`).
    pub reference: String,
}

impl ImageRef {
    /// Parse `[host[:port]/]repo[/more]:tag` or `…@digest`. Without a
    /// host the image is on Docker Hub, and a one-part name is in `library/`.
    ///
    /// # Errors
    ///
    /// [`CloudErrorKind::InvalidArgument`] without a tag or digest, or for
    /// a malformed name.
    pub fn parse(image: &str) -> Result<Self, CloudError> {
        let bad = |why: &str| {
            CloudError::new(
                CloudErrorKind::InvalidArgument,
                format!("bad image reference {image:?}: {why}"),
            )
        };
        let (name, reference) = if let Some((name, digest)) = image.split_once('@') {
            (name, digest)
        } else {
            let slash = image.rfind('/').map_or(0, |i| i + 1);
            match image[slash..].rfind(':') {
                Some(i) => (&image[..slash + i], &image[slash + i + 1..]),
                None => return Err(bad("no tag")),
            }
        };
        let tag_ok = !reference.is_empty()
            && reference.len() <= 128
            && reference
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-' | b':'));
        if !tag_ok {
            return Err(bad("bad tag"));
        }
        let (registry, path) = match name.split_once('/') {
            Some((first, rest))
                if first.contains('.') || first.contains(':') || first == "localhost" =>
            {
                (first.to_string(), rest.to_string())
            }
            _ => (DOCKER_HUB.to_string(), name.to_string()),
        };
        let registry = if registry == "docker.io" || registry == "index.docker.io" {
            DOCKER_HUB.to_string()
        } else {
            registry
        };
        let path_ok = !path.is_empty()
            && path.split('/').all(|part| {
                !part.is_empty()
                    && part.bytes().all(|b| {
                        b.is_ascii_lowercase()
                            || b.is_ascii_digit()
                            || matches!(b, b'.' | b'_' | b'-')
                    })
            });
        let host_ok = registry
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':'));
        if !path_ok || !host_ok {
            return Err(bad("bad name"));
        }
        let repository = if registry == DOCKER_HUB && !path.contains('/') {
            format!("library/{path}")
        } else {
            path
        };
        Ok(Self {
            registry,
            repository,
            reference: reference.to_string(),
        })
    }

    /// `https://`, except plain HTTP for a registry on the loopback.
    fn base(&self) -> String {
        let host = self.registry.split(':').next().unwrap_or("");
        let scheme = if host == "localhost" || host == "127.0.0.1" {
            "http"
        } else {
            "https"
        };
        format!("{scheme}://{}", self.registry)
    }
}

#[derive(Deserialize)]
struct TokenBody {
    #[serde(default)]
    token: Option<String>,
    #[serde(default)]
    access_token: Option<String>,
}

impl NorthflankClient {
    /// Whether `image` (`<repo>:<tag>`) can be pulled without credentials.
    /// A missing tag, and a private image, are `false`.
    ///
    /// # Errors
    ///
    /// [`CloudErrorKind::InvalidArgument`] for a malformed reference; an
    /// unreachable registry or an unexpected status is an error, not `false`.
    pub async fn image_exists(&self, image: &str) -> Result<bool, CloudError> {
        let image = ImageRef::parse(image)?;
        let url = format!(
            "{}/v2/{}/manifests/{}",
            self.registry_base(&image),
            image.repository,
            image.reference
        );
        let mut bearer: Option<String> = None;
        for _ in 0..2 {
            let mut request = self
                .http
                .head(&url)
                .header("Accept", MANIFEST_TYPES)
                .timeout(self.config.request_timeout);
            if let Some(token) = &bearer {
                request = request.bearer_auth(token);
            }
            let response = request.send().await.map_err(|e| registry_error(&e))?;
            match response.status() {
                s if s.is_success() => return Ok(true),
                StatusCode::NOT_FOUND | StatusCode::FORBIDDEN => return Ok(false),

                StatusCode::UNAUTHORIZED if bearer.is_some() => return Ok(false),
                StatusCode::UNAUTHORIZED => {
                    let challenge = response
                        .headers()
                        .get("www-authenticate")
                        .and_then(|v| v.to_str().ok())
                        .and_then(parse_challenge);
                    let Some(challenge) = challenge else {
                        return Ok(false);
                    };
                    bearer = Some(self.anonymous_token(&challenge).await?);
                }
                status => {
                    return Err(CloudError::new(
                        CloudErrorKind::Api,
                        format!(
                            "the image registry {} answered HTTP {status}",
                            image.registry
                        ),
                    )
                    .with_status(status.as_u16())
                    .retryable(status.is_server_error()))
                }
            }
        }
        Ok(false)
    }

    fn registry_base(&self, image: &ImageRef) -> String {
        match &self.config.registry_url {
            Some(base) => base.trim_end_matches('/').to_string(),
            None => image.base(),
        }
    }

    /// The challenge's realm, moved onto [`NorthflankConfig::registry_url`]
    /// when that is set (path and query kept).
    ///
    /// [`NorthflankConfig::registry_url`]: super::NorthflankConfig::registry_url
    fn realm_url(&self, realm: &str) -> String {
        let Some(base) = &self.config.registry_url else {
            return realm.to_string();
        };
        let rest = realm.split_once("://").map_or(realm, |(_, r)| r);
        let path = rest.find('/').map_or("", |i| &rest[i..]);
        format!("{}{path}", base.trim_end_matches('/'))
    }

    async fn anonymous_token(&self, challenge: &Challenge) -> Result<String, CloudError> {
        let mut query: Vec<(&str, &str)> = Vec::new();
        if let Some(service) = &challenge.service {
            query.push(("service", service));
        }
        if let Some(scope) = &challenge.scope {
            query.push(("scope", scope));
        }
        let response = self
            .http
            .get(self.realm_url(&challenge.realm))
            .query(&query)
            .timeout(self.config.request_timeout)
            .send()
            .await
            .map_err(|e| registry_error(&e))?;
        if !response.status().is_success() {
            return Err(CloudError::new(
                CloudErrorKind::Api,
                format!(
                    "the image registry refused an anonymous token (HTTP {})",
                    response.status()
                ),
            )
            .with_status(response.status().as_u16()));
        }
        let body: TokenBody = super::client::parse(response, "registry token").await?;
        body.token
            .or(body.access_token)
            .filter(|t| !t.is_empty())
            .ok_or_else(|| {
                CloudError::new(
                    CloudErrorKind::Other,
                    "the image registry sent no anonymous token",
                )
            })
    }
}

fn registry_error(error: &reqwest::Error) -> CloudError {
    let mut message = error.to_string();
    if let Some(url) = error.url() {
        message = message.replace(url.as_str(), "<url>");
    }
    CloudError::new(
        CloudErrorKind::Other,
        format!("the image registry is unreachable: {message}"),
    )
    .retryable(error.is_connect() || error.is_timeout())
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Challenge {
    pub realm: String,
    pub service: Option<String>,
    pub scope: Option<String>,
}

/// `Bearer realm="…",service="…",scope="…"`.
pub(super) fn parse_challenge(header: &str) -> Option<Challenge> {
    let rest = header
        .trim()
        .strip_prefix("Bearer ")
        .or_else(|| header.trim().strip_prefix("bearer "))?;
    let mut realm = None;
    let mut service = None;
    let mut scope = None;
    let mut chars = rest.chars().peekable();
    loop {
        while chars.peek().is_some_and(|c| *c == ',' || c.is_whitespace()) {
            chars.next();
        }
        let key: String = chars.by_ref().take_while(|c| *c != '=').collect();
        if key.is_empty() {
            break;
        }
        let value: String = if chars.peek() == Some(&'"') {
            chars.next();
            let mut value = String::new();
            while let Some(c) = chars.next() {
                match c {
                    '"' => break,
                    '\\' => value.extend(chars.next()),
                    c => value.push(c),
                }
            }
            value
        } else {
            chars.by_ref().take_while(|c| *c != ',').collect()
        };
        match key.trim() {
            "realm" => realm = Some(value),
            "service" => service = Some(value),
            "scope" => scope = Some(value),
            _ => {}
        }
    }
    let realm = realm.filter(|r| r.starts_with("https://") || r.starts_with("http://"))?;
    Some(Challenge {
        realm,
        service,
        scope,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn challenges_are_parsed() {
        assert_eq!(
            parse_challenge(
                r#"Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:intent-hq/intent-node:pull""#
            ),
            Some(Challenge {
                realm: "https://ghcr.io/token".into(),
                service: Some("ghcr.io".into()),
                scope: Some("repository:intent-hq/intent-node:pull".into()),
            })
        );
        assert_eq!(
            parse_challenge(r#"Bearer realm="https://auth.docker.io/token""#)
                .unwrap()
                .scope,
            None
        );
        assert_eq!(parse_challenge(r#"Basic realm="x""#), None);
        assert_eq!(
            parse_challenge(r#"Bearer realm="file:///etc/passwd""#),
            None
        );
    }
}
