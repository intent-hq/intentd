//! Mandatory MCP predicates shared by launch injection and agent-owned bridge calls.
//! Unknown enforced semantics are errors, never an unrestricted fallback.
use super::{storage::read_optional, ProviderProfileError};
use intent_acp::mcp_config::{NormalizedMcpServer, NormalizedMcpServers};
use serde_json::Value;
use std::path::PathBuf;

/// Authoritative policy inputs. The integration resolves platform/remote policy
/// sources before launching or authorizing a bridge request. Never accept these
/// paths from a project configuration or an agent's tool arguments.
#[derive(Clone, Debug)]
pub enum PolicySource {
    ClaudeSettings(PathBuf),
    ClaudeExclusive(PathBuf),
    CodexRequirements(PathBuf),
    /// An applicable policy whose semantics Intent cannot yet faithfully enforce.
    Unsupported(PathBuf),
    /// Remote/MDM policy was detected but cannot be read by Intent.
    Unavailable,
}

#[derive(Clone)]
enum Rule {
    Name(String),
    Command(Vec<String>),
    Url(UrlRule),
    CodexCommand(String, String),
    CodexUrl(String, String),
}

impl Rule {
    fn matches(&self, name: &str, server: &NormalizedMcpServer) -> bool {
        match (self, server) {
            (Self::Name(expected), _) => name == expected,
            (Self::Command(expected), NormalizedMcpServer::Stdio { command, args, .. }) => expected
                .iter()
                .map(String::as_str)
                .eq(std::iter::once(command.as_str()).chain(args.iter().map(String::as_str))),
            (
                Self::Url(pattern),
                NormalizedMcpServer::Http { url, .. } | NormalizedMcpServer::Sse { url, .. },
            ) => pattern.matches(url),
            (
                Self::CodexCommand(expected_name, expected),
                NormalizedMcpServer::Stdio { command, .. },
            ) => name == expected_name && command == expected,
            (Self::CodexUrl(expected_name, expected), NormalizedMcpServer::Http { url, .. }) => {
                name == expected_name && url == expected
            }
            _ => false,
        }
    }
}

#[derive(Clone, Default)]
struct Layer {
    allow: Option<Vec<Rule>>,
    deny: Vec<Rule>,
    claude: bool,
}

/// Compiled constraints contain no auth material and apply conjunctively. Clone
/// into the active agent context, but reload authoritative sources for changes.
#[derive(Clone, Default)]
pub struct McpPolicy {
    layers: Vec<Layer>,
    name_identity_required: bool,
}

impl McpPolicy {
    #[must_use]
    pub fn allows_server(&self, logical_name: &str, server: &NormalizedMcpServer) -> bool {
        if !self.layers.is_empty() {
            if let NormalizedMcpServer::Http { url, .. } | NormalizedMcpServer::Sse { url, .. } =
                server
            {
                if !reqwest::Url::parse(url).is_ok_and(|url| {
                    matches!(url.scheme(), "http" | "https")
                        && url.host_str().is_some()
                        && url.fragment().is_none()
                }) {
                    return false;
                }
            }
        }
        self.layers.iter().all(|layer| {
            if layer
                .deny
                .iter()
                .any(|rule| rule.matches(logical_name, server))
            {
                return false;
            }
            let Some(allow) = &layer.allow else {
                return true;
            };
            let transport_rule = layer.claude
                && allow.iter().any(|rule| {
                    matches!(
                        (rule, server),
                        (Rule::Command(_), NormalizedMcpServer::Stdio { .. })
                            | (
                                Rule::Url(_),
                                NormalizedMcpServer::Http { .. } | NormalizedMcpServer::Sse { .. }
                            )
                    )
                });
            allow.iter().any(|rule| {
                (!transport_rule || !matches!(rule, Rule::Name(_)))
                    && rule.matches(logical_name, server)
            })
        })
    }

    /// Re-check the actual selected server for every call. This implementation
    /// rejects policy files with tool restrictions during loading, so there is
    /// no unimplemented tool rule silently skipped here.
    #[must_use]
    pub fn allows_tool(
        &self,
        logical_name: &str,
        server: &NormalizedMcpServer,
        _tool: &str,
    ) -> bool {
        self.allows_server(logical_name, server)
    }

    #[must_use]
    pub fn constrain(&self, servers: &NormalizedMcpServers) -> NormalizedMcpServers {
        servers
            .iter()
            .filter(|(name, server)| self.allows_server(name, server))
            .map(|(name, server)| (name.clone(), server.clone()))
            .collect()
    }

    #[must_use]
    pub fn permits_internal_aliases(&self) -> bool {
        !self.name_identity_required
    }
}

fn unsupported() -> ProviderProfileError {
    ProviderProfileError::new("managed-policy-unsupported", "Intent cannot safely enforce this managed policy; ask your administrator for a supported policy before using external MCP.")
}

fn strings(value: &Value) -> Result<Vec<String>, ProviderProfileError> {
    value
        .as_array()
        .ok_or_else(unsupported)?
        .iter()
        .map(|v| {
            v.as_str()
                .filter(|s| !s.contains("${"))
                .map(str::to_owned)
                .ok_or_else(unsupported)
        })
        .collect()
}

fn claude_rules(value: &Value) -> Result<Vec<Rule>, ProviderProfileError> {
    value
        .as_array()
        .ok_or_else(unsupported)?
        .iter()
        .map(|value| {
            let object = value
                .as_object()
                .filter(|o| o.len() == 1)
                .ok_or_else(unsupported)?;
            if let Some(v) = object.get("serverName") {
                return v
                    .as_str()
                    .map(|v| Rule::Name(v.into()))
                    .ok_or_else(unsupported);
            }
            if let Some(v) = object.get("serverCommand") {
                let command = strings(v)?;
                if command.is_empty() {
                    return Err(unsupported());
                }
                return Ok(Rule::Command(command));
            }
            if let Some(v) = object.get("serverUrl") {
                return Ok(Rule::Url(UrlRule::parse(
                    v.as_str().ok_or_else(unsupported)?,
                )?));
            }
            Err(unsupported())
        })
        .collect()
}

/// Read only audited policy subsets. Errors are deliberately redacted. Native
/// policy remains in its original location; these checks additionally protect
/// daemon-side MCP access, which native CLI enforcement cannot cover.
///
/// # Errors
/// Refuses unreadable, malformed, exclusive, unknown or unsupported enforced
/// policy. Missing optional local policy files do not add a restriction.
pub fn load_mcp_policy(
    provider: &str,
    sources: &[PolicySource],
) -> Result<McpPolicy, ProviderProfileError> {
    let mut result = McpPolicy::default();
    for source in sources {
        let path = match source {
            PolicySource::Unavailable => return Err(unsupported()),
            PolicySource::ClaudeSettings(p)
            | PolicySource::ClaudeExclusive(p)
            | PolicySource::CodexRequirements(p)
            | PolicySource::Unsupported(p) => p,
        };
        let Some(bytes) = read_optional(path).map_err(|_| unsupported())? else {
            continue;
        };
        match source {
            PolicySource::ClaudeExclusive(_) if provider == "claude-code" => return Err(ProviderProfileError::new("managed-policy-conflict", "Exclusive managed MCP cannot be combined with Intent's strict MCP configuration; ask your administrator to resolve the conflict.")),
            PolicySource::ClaudeSettings(_) if provider == "claude-code" => {
                let json: Value = serde_json::from_slice(&bytes).map_err(|_|unsupported())?;
                let obj = json.as_object().ok_or_else(unsupported)?;
                // Native tool/host restrictions also need enforcement on bridge
                // calls; reject rather than guessing permission pattern semantics.
                for key in obj.keys() {
                    if !matches!(key.as_str(), "allowedMcpServers" | "deniedMcpServers" | "allowManagedMcpServersOnly") { return Err(unsupported()); }
                }
                if obj.get("allowManagedMcpServersOnly").is_some_and(|v|!v.is_boolean()) {return Err(unsupported());}
                result.layers.push(Layer {
                    allow: obj.get("allowedMcpServers").map(claude_rules).transpose()?,
                    deny: obj.get("deniedMcpServers").map(claude_rules).transpose()?.unwrap_or_default(),
                    claude: true,
                });
            }
            PolicySource::CodexRequirements(_) if provider == "codex" => {
                let doc = std::str::from_utf8(&bytes).map_err(|_|unsupported())?.parse::<toml_edit::DocumentMut>().map_err(|_|unsupported())?;
                if doc.iter().any(|(key,_)|key!="mcp_servers") { return Err(unsupported()); }
                if let Some(servers) = doc.get("mcp_servers") {
                    let mut rules = Vec::new();
                    for (name, entry) in servers.as_table_like().ok_or_else(unsupported)?.iter() {
                        let table = entry.as_table_like().ok_or_else(unsupported)?;
                        if table.len()!=1 {return Err(unsupported());}
                        let identity = entry.get("identity").and_then(toml_edit::Item::as_table_like).filter(|t|t.len()==1).ok_or_else(unsupported)?;
                        if let Some(command) = identity.get("command").and_then(toml_edit::Item::as_str) { rules.push(Rule::CodexCommand(name.into(),command.into())); }
                        else if let Some(url) = identity.get("url").and_then(toml_edit::Item::as_str) {rules.push(Rule::CodexUrl(name.into(),url.into()));}
                        else {return Err(unsupported());}
                    }
                    result.name_identity_required = true;
                    result.layers.push(Layer{allow:Some(rules),..Layer::default()});
                }
            }
            _ => return Err(unsupported()),
        }
    }
    Ok(result)
}

/// Apply the same mandatory predicate before creating external MCP connections.
///
/// # Errors
/// Returns the same enforced-policy errors as [`load_mcp_policy`].
pub fn constrain_mcp(
    provider: &str,
    sources: &[PolicySource],
    servers: &NormalizedMcpServers,
) -> Result<NormalizedMcpServers, ProviderProfileError> {
    Ok(load_mcp_policy(provider, sources)?.constrain(servers))
}

/// Component matching prevents host wildcards from consuming paths or queries.
/// Policy userinfo, queries, fragments and partial scheme patterns are not yet
/// audited and therefore rejected, including when used in deny rules.
#[derive(Clone)]
struct UrlRule {
    scheme: String,
    host: regex::Regex,
    port: Option<u16>,
    any_port: bool,
    path: Option<regex::Regex>,
}

impl UrlRule {
    fn parse(pattern: &str) -> Result<Self, ProviderProfileError> {
        if pattern.contains(['@', '?', '#', '\\'])
            || pattern.contains("${")
            || pattern.chars().any(char::is_whitespace)
        {
            return Err(unsupported());
        }
        let (scheme, rest) = pattern.split_once("://").ok_or_else(unsupported)?;
        if !matches!(scheme, "http" | "https" | "*") {
            return Err(unsupported());
        }
        let (authority, path) = rest.split_once('/').map_or((rest, None), |(host, path)| {
            (host, Some(format!("/{path}")))
        });
        let any_port = authority.ends_with(":*");
        let authority = authority.strip_suffix(":*").unwrap_or(authority);
        let parse_scheme = if scheme == "*" { "https" } else { scheme };
        let parsed = reqwest::Url::parse(&format!(
            "{parse_scheme}://{authority}{}",
            path.as_deref().unwrap_or("/")
        ))
        .map_err(|_| unsupported())?;
        let port = authority
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse::<u16>().ok());
        Ok(Self {
            scheme: scheme.into(),
            host: compile_glob(
                parsed
                    .host_str()
                    .ok_or_else(unsupported)?
                    .trim_end_matches('.'),
            )?,
            port,
            any_port,
            path: path
                .as_ref()
                .map(|_| compile_glob(parsed.path()))
                .transpose()?,
        })
    }

    fn matches(&self, value: &str) -> bool {
        let Ok(url) = reqwest::Url::parse(value) else {
            return false;
        };
        let Some(host) = url.host_str() else {
            return false;
        };
        (self.scheme == "*" || self.scheme == url.scheme())
            && self.host.is_match(host.trim_end_matches('.'))
            && (self.any_port
                || self.port.map_or_else(
                    || url.port().is_none(),
                    |port| url.port_or_known_default() == Some(port),
                ))
            && self.path.as_ref().is_none_or(|path| {
                path.is_match(&url.query().map_or_else(
                    || url.path().to_owned(),
                    |query| format!("{}?{query}", url.path()),
                ))
            })
    }
}

fn compile_glob(pattern: &str) -> Result<regex::Regex, ProviderProfileError> {
    if pattern.len() > 4096 {
        return Err(unsupported());
    }
    let regex = format!("\\A{}\\z", regex::escape(pattern).replace("\\*", ".*"));
    regex::RegexBuilder::new(&regex)
        .size_limit(256 * 1024)
        .build()
        .map_err(|_| unsupported())
}
