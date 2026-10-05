//! Selected-provider policy ingestion. Native precedence must be resolved by the
//! acquisition adapter before supplying a source; independent Intent restrictions
//! intersect, never union. Unavailable applicable remote/MDM sources are errors.
use std::collections::BTreeMap;
use std::io::Read as _;
use std::path::PathBuf;

use intent_acp::NormalizedMcpServer;
use serde_json::Value;

use super::{ProfileError, ProfileResult};

#[derive(Clone, PartialEq, Eq)]
pub enum PolicyScope {
    Host,
    Provider(String),
    Providers(Vec<String>),
}

impl PolicyScope {
    fn applies(&self, provider: &str) -> bool {
        match self {
            Self::Host => true,
            Self::Provider(id) => id == provider,
            Self::Providers(ids) => ids.iter().any(|id| id == provider),
        }
    }
}

#[derive(Clone, Copy)]
pub enum PolicyFormat {
    Intent,
    CodexRequirements,
    ClaudeManagedMcp,
}

pub struct PolicySource {
    pub scope: PolicyScope,
    pub label: String,
    pub format: PolicyFormat,
    pub input: PolicyInput,
}

/// Native defaults are not requirements. Acquisition adapters may supply an
/// already-resolved effective native requirement document, never raw defaults.
pub enum PolicyInput {
    Inline(String),
    File { path: PathBuf, optional: bool },
    Unavailable(&'static str),
}

impl PolicySource {
    #[must_use]
    pub fn inline(scope: PolicyScope, label: &str, format: PolicyFormat, text: &str) -> Self {
        Self {
            scope,
            label: label.into(),
            format,
            input: PolicyInput::Inline(text.into()),
        }
    }

    #[must_use]
    pub fn unavailable(scope: PolicyScope, label: &str, reason: &'static str) -> Self {
        Self {
            scope,
            label: label.into(),
            format: PolicyFormat::Intent,
            input: PolicyInput::Unavailable(reason),
        }
    }
}

/// A snapshot is safe only after acquisition has resolved all applicable native
/// layers. It never implies remote/MDM acquisition succeeded. No secret-bearing
/// matcher contents are exposed through Debug, diagnostics, or serialization.
#[derive(Clone, Default)]
pub struct HostPolicySnapshot {
    selected_provider: Option<String>,
    restrictions: Vec<Restriction>,
}

#[derive(Clone, Default)]
struct Restriction {
    source: String,
    allow: Option<Vec<ServerMatcher>>,
    deny: Vec<ServerMatcher>,
    skills: Option<bool>,
    approvals: Option<Vec<String>>,
    sandbox: Option<Vec<String>>,
    features: BTreeMap<String, bool>,
}

#[derive(Clone)]
struct ServerMatcher {
    name: Option<String>,
    identity: Identity,
}

#[derive(Clone)]
enum Identity {
    Any,
    Command {
        executable: String,
        args: Option<Vec<ValueMatcher>>,
    },
    Url(ValueMatcher),
    HttpUrl(ValueMatcher),
}

#[derive(Clone)]
enum ValueMatcher {
    Exact(String),
    Prefix(String),
    Regex(regex::Regex),
}

impl ValueMatcher {
    fn matches(&self, value: &str) -> bool {
        match self {
            Self::Exact(expected) => value == expected,
            Self::Prefix(prefix) => value.starts_with(prefix),
            Self::Regex(regex) => regex.is_match(value),
        }
    }
}

impl ServerMatcher {
    fn matches(&self, name: &str, server: &NormalizedMcpServer) -> bool {
        if self.name.as_ref().is_some_and(|expected| expected != name) {
            return false;
        }
        match (&self.identity, server) {
            (Identity::Any, _) => true,
            (
                Identity::Command { executable, args },
                NormalizedMcpServer::Stdio {
                    command,
                    args: actual,
                    ..
                },
            ) => {
                executable == command
                    && args.as_ref().is_none_or(|expected| {
                        expected.len() == actual.len()
                            && expected
                                .iter()
                                .zip(actual)
                                .all(|(matcher, value)| matcher.matches(value))
                    })
            }
            (
                Identity::Url(matcher),
                NormalizedMcpServer::Http { url, .. } | NormalizedMcpServer::Sse { url, .. },
            )
            | (Identity::HttpUrl(matcher), NormalizedMcpServer::Http { url, .. }) => {
                matcher.matches(url)
            }
            _ => false,
        }
    }
}

impl HostPolicySnapshot {
    /// Validate expanded original identity, before transport/cwd wrappers.
    /// # Errors
    /// Denied when any applicable allowlist or deny rule rejects this server.
    pub fn validate_server(&self, name: &str, server: &NormalizedMcpServer) -> ProfileResult<()> {
        for restriction in &self.restrictions {
            if restriction
                .deny
                .iter()
                .any(|matcher| matcher.matches(name, server))
                || restriction
                    .allow
                    .as_ref()
                    .is_some_and(|allow| !allow.iter().any(|matcher| matcher.matches(name, server)))
            {
                return Err(ProfileError::PolicyDenied {
                    source: restriction.source.clone(),
                    subject: "MCP server",
                });
            }
        }
        Ok(())
    }

    /// # Errors
    /// Denied if a host policy disallows an injected skill catalog.
    pub fn validate_skills(&self, has_skills: bool) -> ProfileResult<()> {
        if has_skills {
            for restriction in &self.restrictions {
                if restriction.skills == Some(false) {
                    return Err(ProfileError::PolicyDenied {
                        source: restriction.source.clone(),
                        subject: "skills",
                    });
                }
            }
        }
        Ok(())
    }

    /// Validate explicit launch choices. Missing choices cannot satisfy a
    /// constrained setting; the native default is not an enforcement mechanism.
    /// # Errors
    /// Denied if approval/sandbox or feature choices violate requirements.
    pub fn validate_launch(
        &self,
        approval: Option<&str>,
        sandbox: Option<&str>,
        features: &BTreeMap<String, bool>,
    ) -> ProfileResult<()> {
        for restriction in &self.restrictions {
            for (allowed, selected) in [
                (&restriction.approvals, approval),
                (&restriction.sandbox, sandbox),
            ] {
                if allowed.as_ref().is_some_and(|values| {
                    selected.is_none_or(|value| !values.iter().any(|v| v == value))
                }) {
                    return Err(ProfileError::PolicyDenied {
                        source: restriction.source.clone(),
                        subject: "launch settings",
                    });
                }
            }
            if restriction
                .features
                .iter()
                .any(|(name, required)| features.get(name) != Some(required))
            {
                return Err(ProfileError::PolicyDenied {
                    source: restriction.source.clone(),
                    subject: "feature requirements",
                });
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn provenance(&self) -> Vec<&str> {
        self.restrictions
            .iter()
            .map(|r| r.source.as_str())
            .collect()
    }
}

/// Read only applicable sources, before changing the child environment. Optional
/// absence is permitted; unreadable present documents and remote acquisition
/// gaps are not. The caller owns platform-specific effective-policy discovery.
/// # Errors
/// Returns sanitized I/O, parse, or unsupported requirement errors.
pub fn read_host_policy(
    selected_provider: &str,
    sources: &[PolicySource],
) -> ProfileResult<HostPolicySnapshot> {
    if sources
        .iter()
        .filter(|source| {
            source.scope.applies(selected_provider)
                && !matches!(source.format, PolicyFormat::Intent)
        })
        .count()
        > 1
    {
        return Err(unsupported(
            "native policy acquisition",
            "resolve native-layer precedence before ingestion",
        ));
    }
    let mut snapshot = HostPolicySnapshot {
        selected_provider: Some(selected_provider.into()),
        ..Default::default()
    };
    for source in sources
        .iter()
        .filter(|source| source.scope.applies(selected_provider))
    {
        let text = match &source.input {
            PolicyInput::Inline(text) => text.clone(),
            PolicyInput::Unavailable(reason) => return Err(unsupported(&source.label, reason)),
            PolicyInput::File { path, optional } => {
                match std::fs::symlink_metadata(path) {
                    Err(error) if *optional && error.kind() == std::io::ErrorKind::NotFound => {
                        continue
                    }
                    Err(_) => {
                        return Err(ProfileError::PolicyIo {
                            source: source.label.clone(),
                        })
                    }
                    Ok(_) => {}
                }
                let file = std::fs::File::open(path).map_err(|_| ProfileError::PolicyIo {
                    source: source.label.clone(),
                })?;
                let mut text = String::new();
                file.take(1_048_577)
                    .read_to_string(&mut text)
                    .map_err(|_| ProfileError::PolicyIo {
                        source: source.label.clone(),
                    })?;
                text
            }
        };
        if text.len() > 1_048_576 {
            return Err(unsupported(
                &source.label,
                "policy document exceeds size limit",
            ));
        }
        let parsed = match source.format {
            PolicyFormat::Intent => parse_intent(&text, &source.label)?,
            PolicyFormat::CodexRequirements if selected_provider == "codex" => {
                parse_codex_requirements(&text, &source.label)?
            }
            PolicyFormat::ClaudeManagedMcp if selected_provider == "claude-code" => {
                parse_claude(&text, &source.label)?
            }
            _ => {
                return Err(unsupported(
                    &source.label,
                    "native policy format does not apply to selected provider",
                ))
            }
        };
        snapshot.restrictions.extend(parsed.restrictions);
    }
    Ok(snapshot)
}

fn unsupported(source: &str, requirement: &'static str) -> ProfileError {
    ProfileError::UnsupportedPolicy {
        source: source.into(),
        requirement,
    }
}

fn object<'a>(value: &'a Value, source: &str) -> ProfileResult<&'a serde_json::Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| unsupported(source, "expected policy table"))
}

fn keys(value: &Value, allowed: &[&str], source: &str) -> ProfileResult<()> {
    if object(value, source)?
        .keys()
        .any(|key| !allowed.contains(&key.as_str()))
    {
        return Err(unsupported(source, "unknown policy field"));
    }
    Ok(())
}

fn strings(value: &Value, source: &str) -> ProfileResult<Vec<String>> {
    value
        .as_array()
        .ok_or_else(|| unsupported(source, "expected policy list"))?
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| unsupported(source, "expected policy string"))
        })
        .collect()
}

fn matcher(value: &Value, source: &str) -> ProfileResult<ValueMatcher> {
    if let Some(value) = value.as_str() {
        return Ok(ValueMatcher::Exact(value.into()));
    }
    match value.get("match").and_then(Value::as_str) {
        Some("exact" | "prefix") => {
            keys(value, &["match", "value"], source)?;
            let text = value["value"]
                .as_str()
                .ok_or_else(|| unsupported(source, "missing matcher value"))?
                .to_owned();
            Ok(if value["match"] == "exact" {
                ValueMatcher::Exact(text)
            } else {
                ValueMatcher::Prefix(text)
            })
        }
        Some("regex") => {
            keys(value, &["match", "expression"], source)?;
            let expression = value["expression"]
                .as_str()
                .ok_or_else(|| unsupported(source, "missing matcher expression"))?;
            let regex = regex::Regex::new(&format!(r"\A(?:{expression})\z"))
                .map_err(|_| unsupported(source, "invalid matcher expression"))?;
            Ok(ValueMatcher::Regex(regex))
        }
        _ => Err(unsupported(source, "unknown identity matcher")),
    }
}

fn identity(value: &Value, source: &str) -> ProfileResult<Identity> {
    keys(value, &["command", "url"], source)?;
    if value.get("command").is_some() == value.get("url").is_some() {
        return Err(unsupported(
            source,
            "identity requires exactly one transport",
        ));
    }
    if let Some(command) = value.get("command") {
        if let Some(command) = command.as_str() {
            return Ok(Identity::Command {
                executable: command.into(),
                args: None,
            });
        }
        keys(command, &["executable", "args"], source)?;
        let executable = command["executable"]
            .as_str()
            .ok_or_else(|| unsupported(source, "missing executable"))?
            .to_owned();
        let args = command["args"]
            .as_array()
            .ok_or_else(|| unsupported(source, "missing ordered arguments"))?
            .iter()
            .map(|v| matcher(v, source))
            .collect::<ProfileResult<_>>()?;
        return Ok(Identity::Command {
            executable,
            args: Some(args),
        });
    }
    Ok(Identity::Url(matcher(&value["url"], source)?))
}

/// Parse the supported Codex requirements subset, never config.toml defaults.
/// Unknown fields require a compatible native enforcement adapter before launch.
/// # Errors
/// Malformed input or unsupported requirements fail without quoting source text.
pub fn parse_codex_requirements(text: &str, source: &str) -> ProfileResult<HostPolicySnapshot> {
    let value: Value = super::auth::toml_json(text)
        .map_err(|_| unsupported(source, "malformed requirements TOML"))?;
    keys(
        &value,
        &[
            "mcp_servers",
            "allowed_approval_policies",
            "allowed_sandbox_modes",
            "features",
        ],
        source,
    )?;
    let mut restriction = Restriction {
        source: source.into(),
        ..Default::default()
    };
    if let Some(servers) = value.get("mcp_servers") {
        let mut allow = Vec::new();
        for (name, rule) in object(servers, source)? {
            keys(rule, &["identity"], source)?;
            allow.push(ServerMatcher {
                name: Some(name.clone()),
                identity: match identity(&rule["identity"], source)? {
                    Identity::Url(matcher) => Identity::HttpUrl(matcher),
                    identity => identity,
                },
            });
        }
        restriction.allow = Some(allow);
    }
    restriction.approvals = value
        .get("allowed_approval_policies")
        .map(|v| strings(v, source))
        .transpose()?;
    restriction.sandbox = value
        .get("allowed_sandbox_modes")
        .map(|v| strings(v, source))
        .transpose()?;
    if let Some(features) = value.get("features") {
        // Only a known launch invariant is supported here. Other native feature
        // requirements need their selected-runtime enforcement, not translation.
        keys(features, &["multi_agent"], source)?;
        for (key, value) in object(features, source)? {
            restriction.features.insert(
                key.clone(),
                value
                    .as_bool()
                    .ok_or_else(|| unsupported(source, "invalid feature requirement"))?,
            );
        }
    }
    Ok(HostPolicySnapshot {
        selected_provider: Some("codex".into()),
        restrictions: vec![restriction],
    })
}

fn parse_intent(text: &str, source: &str) -> ProfileResult<HostPolicySnapshot> {
    let value: Value = serde_json::from_str(text)
        .map_err(|_| unsupported(source, "malformed Intent policy JSON"))?;
    keys(&value, &["allowMcp", "denyMcp", "allowSkills"], source)?;
    let parse_rules = |value: &Value| -> ProfileResult<Vec<ServerMatcher>> {
        value
            .as_array()
            .ok_or_else(|| unsupported(source, "expected MCP rule list"))?
            .iter()
            .map(|rule| {
                keys(rule, &["name", "identity"], source)?;
                let name = rule
                    .get("name")
                    .map(|name| {
                        name.as_str()
                            .map(str::to_owned)
                            .ok_or_else(|| unsupported(source, "invalid MCP rule name"))
                    })
                    .transpose()?;
                let identity = rule
                    .get("identity")
                    .map(|v| identity(v, source))
                    .transpose()?
                    .unwrap_or(Identity::Any);
                if name.is_none() && matches!(identity, Identity::Any) {
                    return Err(unsupported(source, "empty MCP rule"));
                }
                Ok(ServerMatcher { name, identity })
            })
            .collect()
    };
    Ok(HostPolicySnapshot {
        selected_provider: None,
        restrictions: vec![Restriction {
            source: source.into(),
            allow: value.get("allowMcp").map(parse_rules).transpose()?,
            deny: value
                .get("denyMcp")
                .map(parse_rules)
                .transpose()?
                .unwrap_or_default(),
            skills: value
                .get("allowSkills")
                .map(|v| {
                    v.as_bool()
                        .ok_or_else(|| unsupported(source, "invalid skill requirement"))
                })
                .transpose()?,
            ..Default::default()
        }],
    })
}

fn parse_claude(text: &str, source: &str) -> ProfileResult<HostPolicySnapshot> {
    let value: Value = serde_json::from_str(text)
        .map_err(|_| unsupported(source, "malformed managed settings JSON"))?;
    keys(&value, &["allowedMcpServers", "deniedMcpServers"], source)?;
    let rules = |value: &Value| -> ProfileResult<Vec<ServerMatcher>> {
        value
            .as_array()
            .ok_or_else(|| unsupported(source, "expected MCP rule list"))?
            .iter()
            .map(|rule| {
                keys(rule, &["serverName", "serverCommand", "serverUrl"], source)?;
                if object(rule, source)?.len() != 1 {
                    return Err(unsupported(source, "managed MCP rule needs one matcher"));
                }
                if let Some(name) = rule.get("serverName") {
                    return Ok(ServerMatcher {
                        name: Some(
                            name.as_str()
                                .ok_or_else(|| unsupported(source, "invalid server name"))?
                                .into(),
                        ),
                        identity: Identity::Any,
                    });
                }
                if let Some(command) = rule.get("serverCommand") {
                    let words = strings(command, source)?;
                    let (exe, args) = words
                        .split_first()
                        .ok_or_else(|| unsupported(source, "empty server command"))?;
                    return Ok(ServerMatcher {
                        name: None,
                        identity: Identity::Command {
                            executable: exe.clone(),
                            args: Some(args.iter().cloned().map(ValueMatcher::Exact).collect()),
                        },
                    });
                }
                // Claude URLs may contain wildcard patterns. Exact URLs are the
                // supported subset; never mistake an unsupported wildcard for exact.
                let url = rule["serverUrl"]
                    .as_str()
                    .ok_or_else(|| unsupported(source, "invalid server URL"))?;
                if url.contains('*') {
                    return Err(unsupported(source, "wildcard managed server URL"));
                }
                Ok(ServerMatcher {
                    name: None,
                    identity: Identity::Url(ValueMatcher::Exact(url.into())),
                })
            })
            .collect()
    };
    Ok(HostPolicySnapshot {
        selected_provider: None,
        restrictions: vec![Restriction {
            source: source.into(),
            allow: value.get("allowedMcpServers").map(rules).transpose()?,
            deny: value
                .get("deniedMcpServers")
                .map(rules)
                .transpose()?
                .unwrap_or_default(),
            ..Default::default()
        }],
    })
}

/// Acquire the supported Linux system-policy source for the selected provider.
/// This is only the local layer: callers must supply effective remote/MDM/legacy
/// acquisition via `additional`, including `Unavailable` when not obtainable.
/// Ordinary `/etc/codex/config.toml` defaults are deliberately not requirements.
/// Native precedence must be resolved before combining overlapping native layers.
/// # Errors
/// Present unsupported managed policy or unreadable/malformed requirements fail.
pub fn read_linux_system_policy(
    provider: &str,
    etc_root: &std::path::Path,
    additional: &[PolicySource],
) -> ProfileResult<HostPolicySnapshot> {
    let mut snapshot = read_host_policy(provider, additional)?;
    let (relative, format, incompatible): (&str, PolicyFormat, &[&str]) = match provider {
        "codex" => (
            "codex/requirements.toml",
            PolicyFormat::CodexRequirements,
            &["codex/managed_config.toml"],
        ),
        "claude-code" => (
            "claude-code/managed-settings.json",
            PolicyFormat::ClaudeManagedMcp,
            &["claude-code/managed-mcp.json"],
        ),
        "grok" => (
            "",
            PolicyFormat::Intent,
            &["grok/requirements.toml", "grok/managed_config.toml"],
        ),
        "droid" => ("", PolicyFormat::Intent, &["factory/settings.json"]),
        "opencode" | "unsloth" => ("", PolicyFormat::Intent, &["opencode"]),
        _ => ("", PolicyFormat::Intent, &[]),
    };
    for path in incompatible {
        match std::fs::symlink_metadata(etc_root.join(path)) {
            Ok(_) => return Err(unsupported(path, "resolve this managed source with a compatible selected-provider policy adapter before isolation")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {},
            Err(_) => return Err(ProfileError::PolicyIo { source: (*path).into() }),
        }
    }
    if !relative.is_empty() {
        let local = PolicySource {
            scope: PolicyScope::Provider(provider.into()),
            label: relative.into(),
            format,
            input: PolicyInput::File {
                path: etc_root.join(relative),
                optional: true,
            },
        };
        let local_policy = read_host_policy(provider, &[local])?;
        if !local_policy.restrictions.is_empty()
            && additional.iter().any(|source| {
                source.scope.applies(provider) && !matches!(source.format, PolicyFormat::Intent)
            })
        {
            return Err(unsupported(
                "native policy acquisition",
                "resolve native-layer precedence before ingestion",
            ));
        }
        snapshot.restrictions.extend(local_policy.restrictions);
    }
    Ok(snapshot)
}

impl HostPolicySnapshot {
    /// Reject accidental reuse of a selected-provider snapshot for another provider.
    /// # Errors
    /// A snapshot acquired for another provider cannot authorize this launch.
    pub fn validate_provider(&self, provider: &str) -> ProfileResult<()> {
        if self
            .selected_provider
            .as_ref()
            .is_some_and(|selected| selected != provider)
        {
            return Err(unsupported(
                "policy snapshot",
                "reacquire policy for the selected provider",
            ));
        }
        Ok(())
    }

    /// Opaque change token; excludes input formatting, includes effective parsed
    /// restrictions and provider applicability. Source/order changes conservatively
    /// invalidate it. Compare alongside the catalog identity, never as authority.
    #[must_use]
    pub fn identity(&self) -> super::ConfigurationIdentity {
        super::ConfigurationIdentity::from_value(&self.identity_value())
    }

    pub(super) fn identity_value(&self) -> Value {
        use serde_json::json;
        let restrictions: Vec<_> = self.restrictions.iter().map(|rule| json!({
            "source":rule.source,
            "allow":rule.allow.as_ref().map(|rules| rules.iter().map(ServerMatcher::identity_value).collect::<Vec<_>>()),
            "deny":rule.deny.iter().map(ServerMatcher::identity_value).collect::<Vec<_>>(),
            "skills":rule.skills,"approvals":rule.approvals,"sandbox":rule.sandbox,"features":rule.features
        })).collect();
        json!({"provider":self.selected_provider,"restrictions":restrictions})
    }
}

impl ServerMatcher {
    fn identity_value(&self) -> Value {
        use serde_json::json;
        let identity = match &self.identity {
            Identity::Any => json!({"any":true}),
            Identity::Command { executable, args } => {
                json!({"command":executable,"args":args.as_ref().map(|args| args.iter().map(ValueMatcher::identity_value).collect::<Vec<_>>())})
            }
            Identity::Url(matcher) => json!({"url":matcher.identity_value()}),
            Identity::HttpUrl(matcher) => json!({"http":matcher.identity_value()}),
        };
        json!({"name":self.name,"identity":identity})
    }
}

impl ValueMatcher {
    fn identity_value(&self) -> Value {
        match self {
            Self::Exact(value) => serde_json::json!({"exact":value}),
            Self::Prefix(value) => serde_json::json!({"prefix":value}),
            Self::Regex(regex) => serde_json::json!({"regex":regex.as_str()}),
        }
    }
}
