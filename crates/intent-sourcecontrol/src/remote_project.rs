//! Pure remote-to-project resolution from explicitly supplied instance bindings.
//!
//! Git/config rewriting and admission belong to callers. This module neither
//! reads config nor resolves DNS/SSH aliases or credentials. Results are internal
//! descriptors, not wire DTOs or proof of access. Raw input is never retained in
//! a result, diagnostic or mapping's debug representation.

use reqwest::Url;

use crate::GitlabInstance;

/// Provider identity for the internal project descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteProvider {
    Github,
    Gitlab,
}

/// A configured logical instance, independent of its Git transport endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteInstance {
    provider: RemoteProvider,
    root: GitlabInstance,
}

impl RemoteInstance {
    /// Explicitly register the public GitHub instance and its standard transports.
    ///
    /// # Panics
    /// Panics only if the constant GitHub HTTPS root fails instance validation.
    #[must_use]
    pub fn github_com() -> Self {
        Self {
            provider: RemoteProvider::Github,
            root: GitlabInstance::parse("https://github.com").expect("constant HTTPS root"),
        }
    }

    /// Register the full configured GitLab root; aliases are supplied separately.
    #[must_use]
    pub fn gitlab(root: GitlabInstance) -> Self {
        Self {
            provider: RemoteProvider::Gitlab,
            root,
        }
    }
}

/// Provider-qualified identity only. No transport userinfo or connection secrets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalRemoteProject {
    pub provider: RemoteProvider,
    pub instance_base_url: String,
    pub project_path: String,
}

/// Non-sensitive reasons a remote cannot establish one canonical identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum UnresolvedRemote {
    #[error("remote instance or transport binding is unknown")]
    UnknownInstance,
    #[error("remote transport is unsupported")]
    UnsupportedTransport,
    #[error("remote transport bindings are ambiguous")]
    AmbiguousMapping,
    #[error("remote syntax or project path is invalid")]
    InvalidRemote,
}

/// An explicitly configured transport root mapped to a logical instance root.
///
/// For example, `git@work:` and `ssh://git@work:2222/repos/` are separate
/// mappings. An SCP relative root does not match an absolute path. Mapping a
/// transport prefix replaces just that prefix; it never truncates namespaces.
#[derive(Clone)]
pub struct RemoteTransportMapping {
    instance: RemoteInstance,
    endpoint: TransportEndpoint,
}

impl std::fmt::Debug for RemoteTransportMapping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteTransportMapping")
            .field("instance", &self.instance)
            .finish_non_exhaustive()
    }
}

impl RemoteTransportMapping {
    /// Bind an exact HTTP(S), SSH or SCP root to an already known instance.
    ///
    /// # Errors
    /// Rejects malformed roots and credential-bearing HTTP(S) configuration.
    pub fn new(instance: RemoteInstance, transport_root: &str) -> Result<Self, UnresolvedRemote> {
        let endpoint = parse_transport(transport_root, true)?;
        Ok(Self { instance, endpoint })
    }
}

/// Immutable, injected mapping table. Constructing it does not grant access.
#[derive(Debug, Clone)]
pub struct CanonicalRemoteResolver {
    mappings: Vec<RemoteTransportMapping>,
}

impl CanonicalRemoteResolver {
    /// Register exact HTTPS roots plus explicit transport aliases.
    ///
    /// Public GitHub aliases are a fixed provider catalog, never guesses about a
    /// configured host. GitLab HTTP/SSH/SCP aliases always require mappings.
    ///
    /// # Errors
    /// Rejects invalid roots and mappings to instances absent from `instances`.
    pub fn new(
        instances: Vec<RemoteInstance>,
        mut mappings: Vec<RemoteTransportMapping>,
    ) -> Result<Self, UnresolvedRemote> {
        if mappings.iter().any(|m| !instances.contains(&m.instance)) {
            return Err(UnresolvedRemote::UnknownInstance);
        }
        for instance in instances {
            mappings.push(RemoteTransportMapping::new(
                instance.clone(),
                instance.root.as_str(),
            )?);
            if instance.provider == RemoteProvider::Github {
                for endpoint in [
                    "https://www.github.com/",
                    "http://github.com/",
                    "http://www.github.com/",
                    "ssh://git@github.com/",
                    "git@github.com:",
                    "ssh://git@ssh.github.com:443/",
                ] {
                    mappings.push(RemoteTransportMapping::new(instance.clone(), endpoint)?);
                }
            }
        }
        Ok(Self { mappings })
    }

    /// Resolve the original *effective* Git URL before any lossy display cleanup.
    ///
    /// HTTP credentials are discarded only after isolating and validating the
    /// authority. SSH users participate in explicit matching. Percent escapes,
    /// query/fragment and path normalization ambiguities remain unresolved.
    ///
    /// # Errors
    /// Returns a static reason, never raw input or parsing-library diagnostics.
    pub fn resolve(&self, effective_url: &str) -> Result<CanonicalRemoteProject, UnresolvedRemote> {
        let endpoint = parse_transport(effective_url, false)?;
        let mut candidates = Vec::new();
        let mut invalid = false;
        for mapping in &self.mappings {
            let Some(relative) = endpoint.relative_to(&mapping.endpoint) else {
                continue;
            };
            let Ok(project_path) = project_path(relative, mapping.instance.provider) else {
                invalid = true;
                continue;
            };
            let candidate = CanonicalRemoteProject {
                provider: mapping.instance.provider,
                instance_base_url: mapping.instance.root.as_str().into(),
                project_path,
            };
            if !candidates.contains(&candidate) {
                candidates.push(candidate);
            }
        }
        match candidates.len() {
            0 if invalid => Err(UnresolvedRemote::InvalidRemote),
            0 => Err(UnresolvedRemote::UnknownInstance),
            1 if !invalid => candidates.pop().ok_or(UnresolvedRemote::UnknownInstance),
            _ => Err(UnresolvedRemote::AmbiguousMapping),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Transport {
    Http,
    Https,
    Ssh,
    Scp,
}

#[derive(Clone)]
struct TransportEndpoint {
    transport: Transport,
    host: String,
    port: u16,
    user: Option<String>,
    absolute: bool,
    path: String,
}

impl TransportEndpoint {
    fn relative_to(&self, root: &Self) -> Option<&str> {
        if self.transport != root.transport
            || self.host != root.host
            || self.port != root.port
            || self.user != root.user
            || self.absolute != root.absolute
        {
            return None;
        }
        if root.path.is_empty() {
            return Some(&self.path);
        }
        self.path.strip_prefix(&root.path)?.strip_prefix('/')
    }
}

fn parse_transport(input: &str, root: bool) -> Result<TransportEndpoint, UnresolvedRemote> {
    use UnresolvedRemote::{InvalidRemote, UnsupportedTransport};
    if input.is_empty()
        || !input.is_ascii()
        || input
            .chars()
            .any(|c| c.is_ascii_whitespace() || c.is_control())
        || input.contains(['\\', '%', '?', '#'])
    {
        return Err(InvalidRemote);
    }
    let (transport, authority, raw_path) = if let Some((scheme, rest)) = input.split_once("://") {
        let transport = match scheme.to_ascii_lowercase().as_str() {
            "http" => Transport::Http,
            "https" => Transport::Https,
            "ssh" => Transport::Ssh,
            _ => return Err(UnsupportedTransport),
        };
        let boundary = rest.find('/').unwrap_or(rest.len());
        (transport, &rest[..boundary], &rest[boundary..])
    } else {
        if input.starts_with(['/', '.', '~']) || input.contains("::") && !input.contains('[') {
            return Err(UnsupportedTransport);
        }
        let mut bracketed = false;
        let boundary = input
            .char_indices()
            .find_map(|(i, c)| {
                match c {
                    '[' => bracketed = true,
                    ']' => bracketed = false,
                    ':' if !bracketed => return Some(i),
                    _ => {}
                }
                None
            })
            .ok_or(UnsupportedTransport)?;
        let (authority, rest) = input.split_at(boundary);
        if authority.contains('/') || authority.len() == 1 && rest.starts_with(":/") {
            return Err(UnsupportedTransport);
        }
        (Transport::Scp, authority, &rest[1..])
    };
    if authority.is_empty() || authority.matches('@').count() > 1 || authority.ends_with(':') {
        return Err(InvalidRemote);
    }
    let scheme = match transport {
        Transport::Http => "http",
        Transport::Https => "https",
        Transport::Ssh | Transport::Scp => "ssh",
    };
    let url = Url::parse(&format!("{scheme}://{authority}/")).map_err(|_| InvalidRemote)?;
    let host = url
        .host_str()
        .filter(|h| !h.is_empty())
        .ok_or(InvalidRemote)?;
    if url.path() != "/" || url.query().is_some() || url.fragment().is_some() {
        return Err(InvalidRemote);
    }
    let ssh = matches!(transport, Transport::Ssh | Transport::Scp);
    if ssh && url.password().is_some() || root && !ssh && authority.contains('@') {
        return Err(InvalidRemote);
    }
    let user = if ssh && !url.username().is_empty() {
        if !safe_segment(url.username()) {
            return Err(InvalidRemote);
        }
        Some(url.username().into())
    } else {
        None
    };
    if ssh && authority.contains('@') && user.is_none() {
        return Err(InvalidRemote);
    }
    let port = url.port().unwrap_or(match transport {
        Transport::Http => 80,
        Transport::Https => 443,
        Transport::Ssh | Transport::Scp => 22,
    });
    if port == 0 || transport == Transport::Scp && url.port().is_some() {
        return Err(InvalidRemote);
    }
    let absolute = transport != Transport::Scp || raw_path.starts_with('/');
    let path = raw_path
        .strip_prefix('/')
        .unwrap_or(raw_path)
        .trim_end_matches('/');
    if raw_path.contains("//")
        || !path.is_empty() && !path.split('/').all(safe_segment)
        || !root && path.is_empty()
    {
        return Err(InvalidRemote);
    }
    Ok(TransportEndpoint {
        transport,
        host: host.to_ascii_lowercase(),
        port,
        user,
        absolute,
        path: path.into(),
    })
}

fn safe_segment(value: &str) -> bool {
    !matches!(value, "" | "." | ".." | "-")
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_-.".contains(&c))
}

fn project_path(path: &str, provider: RemoteProvider) -> Result<String, UnresolvedRemote> {
    let mut parts: Vec<_> = path.split('/').collect();
    let Some(name) = parts.last_mut() else {
        return Err(UnresolvedRemote::InvalidRemote);
    };
    let suffix = name.get(name.len().saturating_sub(4)..);
    if suffix.is_some_and(|s| match provider {
        RemoteProvider::Github => s.eq_ignore_ascii_case(".git"),
        RemoteProvider::Gitlab => s == ".git",
    }) {
        *name = &name[..name.len() - 4];
    }
    if parts.len() < 2 || !parts.iter().all(|part| safe_segment(part)) {
        return Err(UnresolvedRemote::InvalidRemote);
    }
    match provider {
        RemoteProvider::Github if parts.len() == 2 => {
            Ok(intent_core::RepoRef::new(parts[0], parts[1]).identity_key())
        }
        RemoteProvider::Github => Err(UnresolvedRemote::InvalidRemote),
        RemoteProvider::Gitlab => Ok(parts.join("/")),
    }
}
