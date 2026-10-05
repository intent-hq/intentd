//! Bounded, side-effect-free project MCP discovery.
//!
//! Only the workspace-root → launch-cwd chain is scanned. Within the provider
//! tier, nearer directories win; at each directory SOURCES defines increasing
//! priority. Common `.mcp.json` is a separate, higher project tier. Entries
//! replace whole entries (including disabled/rejected winners), never fields.
//! Explicit Intent settings override project preferences; authoritative Intent
//! disables apply last. The caller MUST then apply managed policy before any
//! connection or injection, including the reserved workspace bridge.
//!
//! This is a snapshot, not a provider-native suppression inventory or watcher.
//! No personal CLI locations, includes, credentials, shell, or network are read.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use intent_acp::mcp_config::{normalize_mcp_servers, NormalizedMcpServer, NormalizedMcpServers};
use serde_json::{Map, Value};

const BRIDGE: &str = "workspace-mcp";
const MAX_CONFIG_BYTES: u64 = 1024 * 1024;
const MAX_ANCESTORS: usize = 64;

#[derive(Clone, Copy)]
enum Format {
    Common,
    Toml,
    OpenCode,
    Auggie,
}

// Increasing priority at a given directory; .jsonc wins .json, Auggie local
// wins shared. Common .mcp.json is handled after every provider-specific file.
const SOURCES: &[(&str, Format)] = &[
    (".cursor/mcp.json", Format::Common),
    (".codex/config.toml", Format::Toml),
    (".factory/mcp.json", Format::Common),
    (".grok/config.toml", Format::Toml),
    ("opencode.json", Format::OpenCode),
    ("opencode.jsonc", Format::OpenCode),
    (".opencode/opencode.json", Format::OpenCode),
    (".opencode/opencode.jsonc", Format::OpenCode),
    (".augment/settings.json", Format::Auggie),
    (".augment/settings.local.json", Format::Auggie),
];

/// Redacted discovery/merge diagnostic. Messages never contain config values,
/// parser error excerpts, executable names, URLs, environment or header values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectMcpDiagnostic {
    pub code: &'static str,
    pub source: PathBuf,
    pub server_name: Option<String>,
    pub message: &'static str,
}

/// Project winners and their provenance. Discovery's `disabled_names` contains
/// winning project disables only; after merge it also includes authoritative
/// Intent denies. It is never a union of native names requiring suppression.
#[derive(Clone, Default)]
pub struct ProjectMcpDiscovery {
    pub servers: NormalizedMcpServers,
    pub disabled_names: BTreeSet<String>,
    pub sources: BTreeMap<String, PathBuf>,
    pub diagnostics: Vec<ProjectMcpDiagnostic>,
}

impl ProjectMcpDiscovery {
    fn diagnostic(
        &mut self,
        code: &'static str,
        source: &Path,
        name: Option<&str>,
        message: &'static str,
    ) {
        self.diagnostics.push(ProjectMcpDiagnostic {
            code,
            source: source.to_owned(),
            server_name: name.map(str::to_owned),
            message,
        });
    }
}

/// Discover audited project formats beneath a trusted workspace boundary.
/// Relative executables resolve against the launch cwd, just as the provider
/// subprocess does; argv and ordinary PATH commands remain literal. Config
/// paths and cwd must canonicalize inside the root. Call on a blocking worker.
#[must_use]
pub fn discover_project_mcp(workspace_root: &Path, launch_cwd: &Path) -> ProjectMcpDiscovery {
    let mut out = ProjectMcpDiscovery::default();
    let (Ok(root), Ok(cwd)) = (workspace_root.canonicalize(), launch_cwd.canonicalize()) else {
        out.diagnostic(
            "boundary",
            workspace_root,
            None,
            "Workspace root and launch directory must exist.",
        );
        return out;
    };
    if !root.is_dir() || !cwd.is_dir() || !cwd.starts_with(&root) {
        out.diagnostic(
            "boundary",
            workspace_root,
            None,
            "Launch directory must be inside the workspace.",
        );
        return out;
    }
    let mut dirs = Vec::new();
    for dir in cwd.ancestors() {
        dirs.push(dir);
        if dir == root {
            break;
        }
        if dirs.len() >= MAX_ANCESTORS {
            out.diagnostic(
                "limit",
                workspace_root,
                None,
                "Launch directory exceeds the discovery depth limit.",
            );
            return out;
        }
    }
    dirs.reverse();
    for dir in &dirs {
        for (relative, format) in SOURCES {
            read_source(&root, &cwd, &dir.join(relative), *format, &mut out);
        }
        // This is an unsupported extension convention, NOT a Pi parser.
        if dir.join(".pi/mcp.json").symlink_metadata().is_ok() {
            out.diagnostic(
                "unsupported-format",
                &dir.join(".pi/mcp.json"),
                None,
                "Pi has no audited built-in MCP file schema; configure this server in Intent.",
            );
        }
    }
    for dir in dirs {
        read_source(
            &root,
            &cwd,
            &dir.join(".mcp.json"),
            Format::Common,
            &mut out,
        );
    }
    out
}

fn read_source(
    root: &Path,
    cwd: &Path,
    source: &Path,
    format: Format,
    out: &mut ProjectMcpDiscovery,
) {
    let canonical = match source.canonicalize() {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(_) => {
            out.diagnostic(
                "read",
                source,
                None,
                "Cannot resolve project MCP configuration.",
            );
            return;
        }
    };
    if !canonical.starts_with(root) {
        out.diagnostic(
            "boundary",
            source,
            None,
            "Project configuration points outside the workspace.",
        );
        return;
    }
    // Check before opening: in particular, never wait on a FIFO/device.
    let Ok(metadata) = canonical.metadata() else {
        return;
    };
    if !metadata.is_file() || metadata.len() > MAX_CONFIG_BYTES {
        out.diagnostic(
            "limit",
            source,
            None,
            "Project configuration must be a regular file of at most 1 MiB.",
        );
        return;
    }
    let mut text = String::new();
    let read = std::fs::File::open(&canonical)
        .and_then(|file| file.take(MAX_CONFIG_BYTES + 1).read_to_string(&mut text));
    if read.is_err() || text.len() as u64 > MAX_CONFIG_BYTES {
        out.diagnostic(
            "read",
            source,
            None,
            "Cannot read bounded UTF-8 project MCP configuration.",
        );
        return;
    }
    let parsed = match format {
        Format::Toml => toml::from_str::<toml::Value>(&text)
            .ok()
            .and_then(|v| serde_json::to_value(v).ok()),
        Format::OpenCode | Format::Auggie => parse_jsonc(&text),
        Format::Common => serde_json::from_str(&text).ok(),
    };
    let Some(value) = parsed.filter(Value::is_object) else {
        out.diagnostic(
            "parse",
            source,
            None,
            "Invalid project configuration; expected a JSON/JSONC or TOML object.",
        );
        return;
    };
    let key = match format {
        Format::Toml => "mcp_servers",
        Format::OpenCode => "mcp",
        _ => "mcpServers",
    };
    let Some(servers) = value.get(key) else {
        return;
    };
    let Some(servers) = servers.as_object() else {
        out.diagnostic(
            "parse",
            source,
            None,
            "MCP server collection must be a name-to-object map.",
        );
        return;
    };
    let unsupported_source = matches!(format, Format::OpenCode)
        && ["tools", "permission", "permissions", "$ref"]
            .iter()
            .any(|key| value.get(*key).is_some());
    for (name, raw) in servers {
        if name.trim().is_empty() || name.trim() != name || name.chars().any(char::is_control) {
            out.diagnostic("invalid-name", source, None, "MCP server names must be nonempty without surrounding whitespace or control characters.");
            continue;
        }
        if name == BRIDGE {
            out.diagnostic(
                "reserved",
                source,
                Some(name),
                "The workspace-mcp identity is reserved for Intent.",
            );
            continue;
        }
        if out
            .sources
            .insert(name.clone(), source.to_owned())
            .is_some()
        {
            out.diagnostic(
                "collision",
                source,
                Some(name),
                "This source wins a project MCP name collision by documented precedence.",
            );
        }
        out.servers.remove(name);
        out.disabled_names.remove(name);
        // A rejected higher-priority definition must not resurrect a lower one.
        match parse_entry(raw, format, root, cwd) {
            Ok(None) => { out.disabled_names.insert(name.clone()); }
            Ok(Some(server)) if !unsupported_source => { out.servers.insert(name.clone(), server); }
            Ok(Some(_)) => out.diagnostic("unsupported", source, Some(name), "Source-level tool restrictions or includes cannot be represented; configure this server and its policy in Intent."),
            Err(message) => out.diagnostic("unsupported", source, Some(name), message),
        }
    }
}

/// Merge without I/O. The independent `disabled_names` input MUST include mapped
/// Intent IDs/configured names for global and workspace denies. Project disables
/// never override explicit Intent entries. Managed policy is a required later
/// pass: reserving the bridge name does not exempt it from policy or disables.
#[must_use]
pub fn merge_project_mcp(
    mut project: ProjectMcpDiscovery,
    explicit_intent: NormalizedMcpServers,
    disabled_names: &BTreeSet<String>,
    workspace_bridge: Option<NormalizedMcpServer>,
) -> ProjectMcpDiscovery {
    project.servers.remove(BRIDGE);
    project.disabled_names.remove(BRIDGE);
    project.sources.remove(BRIDGE);
    for (name, server) in explicit_intent {
        if name == BRIDGE {
            continue;
        }
        project.disabled_names.remove(&name);
        project.sources.remove(&name);
        project.servers.insert(name, server);
    }
    if let Some(bridge) = workspace_bridge {
        project.servers.insert(BRIDGE.into(), bridge);
    }
    for name in disabled_names {
        project.servers.remove(name);
        project.disabled_names.insert(name.clone());
    }
    project
}

/// Expand authoritative Intent disables to both IDs and configured names.
/// Accepts the existing `mcp.servers` map. Explicitly disabled or malformed
/// enabled flags fail closed, matching Intent's opt-in user-server semantics.
/// Repeats alias expansion to cover collisions where one ID is another name.
#[must_use]
pub fn intent_mcp_disabled_names(
    configs: &Value,
    global: &BTreeSet<String>,
    workspace: &BTreeSet<String>,
) -> BTreeSet<String> {
    let mut denied: BTreeSet<String> = global.union(workspace).cloned().collect();
    let Some(configs) = configs.as_object() else {
        return denied;
    };
    loop {
        let before = denied.len();
        for (id, config) in configs {
            let name = config
                .get("name")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(id);
            if config.get("enabled").and_then(Value::as_bool) != Some(true)
                || denied.contains(id)
                || denied.contains(name)
            {
                denied.insert(id.clone());
                denied.insert(name.to_owned());
            }
        }
        if before == denied.len() {
            break;
        }
    }
    denied
}

fn parse_entry(
    raw: &Value,
    format: Format,
    root: &Path,
    cwd: &Path,
) -> Result<Option<NormalizedMcpServer>, &'static str> {
    const INVALID: &str = "Malformed MCP entry; expected an explicit transport with string arguments, environment and headers.";
    const UNSUPPORTED: &str = "MCP entry has unsupported settings or substitution; configure it in Intent instead of discarding semantics.";
    let obj = raw.as_object().ok_or(INVALID)?;
    for flag in ["enabled", "disabled"] {
        if obj.get(flag).is_some_and(|v| !v.is_boolean()) {
            return Err(INVALID);
        }
    }
    if obj.get("enabled") == Some(&Value::Bool(false))
        || obj.get("disabled") == Some(&Value::Bool(true))
    {
        return Ok(None);
    }
    let allowed: &[&str] = match format {
        Format::Toml => &[
            "type",
            "command",
            "args",
            "env",
            "url",
            "headers",
            "http_headers",
            "enabled",
            "disabled",
        ],
        Format::OpenCode => &[
            "type",
            "command",
            "environment",
            "url",
            "headers",
            "enabled",
            "oauth",
        ],
        _ => &[
            "type",
            "command",
            "args",
            "env",
            "url",
            "headers",
            "enabled",
            "disabled",
            "description",
        ],
    };
    if obj.keys().any(|key| !allowed.contains(&key.as_str())) || has_substitution(raw) {
        return Err(UNSUPPORTED);
    }
    if obj.get("oauth").is_some_and(|v| *v != Value::Bool(false)) {
        return Err(UNSUPPORTED);
    }
    let mut canonical = obj.clone();
    if let Some(headers) = canonical.remove("http_headers") {
        let previous = canonical.insert("headers".into(), headers);
        if previous.is_some() {
            return Err(INVALID);
        }
    }
    if matches!(format, Format::OpenCode) {
        match obj.get("type").and_then(Value::as_str) {
            Some("local") => {
                let command = obj
                    .get("command")
                    .and_then(Value::as_array)
                    .ok_or(INVALID)?;
                let (first, args) = command.split_first().ok_or(INVALID)?;
                canonical.insert("command".into(), first.clone());
                canonical.insert("args".into(), Value::Array(args.to_vec()));
                canonical.insert("type".into(), Value::String("stdio".into()));
                if let Some(env) = canonical.remove("environment") {
                    canonical.insert("env".into(), env);
                }
            }
            // Without oauth:false OpenCode may automatically negotiate OAuth.
            // The normalized HTTP type has no equivalent lifecycle support.
            Some("remote") if obj.get("oauth") == Some(&Value::Bool(false)) => {
                canonical.insert("type".into(), Value::String("http".into()));
            }
            Some("remote") => return Err(UNSUPPORTED),
            _ => return Err(INVALID),
        }
    }
    let kind = canonical.get("type").and_then(Value::as_str);
    if canonical.get("type").is_some() && !matches!(kind, Some("stdio" | "http" | "sse")) {
        return Err(INVALID);
    }
    let command = canonical.get("command").and_then(Value::as_str);
    let url = canonical.get("url").and_then(Value::as_str);
    if let Some(command) = command {
        if command.trim().is_empty()
            || canonical.contains_key("url")
            || canonical.contains_key("headers")
            || matches!(kind, Some("http" | "sse"))
        {
            return Err(INVALID);
        }
        if canonical
            .get("args")
            .is_some_and(|v| !v.as_array().is_some_and(|a| a.iter().all(Value::is_string)))
        {
            return Err(INVALID);
        }
        if canonical.get("env").is_some_and(|v| !is_string_map(v)) {
            return Err(INVALID);
        }
        if command.contains('/') || command.contains('\\') {
            let path = Path::new(command);
            if !path.is_absolute() {
                let resolved = normalize_path(&cwd.join(path));
                if !resolved.starts_with(root) {
                    return Err("Relative MCP executable escapes the workspace; configure the executable explicitly in Intent.");
                }
                canonical.insert(
                    "command".into(),
                    Value::String(resolved.to_string_lossy().into_owned()),
                );
            }
        }
    } else if let Some(url) = url {
        if !(url.starts_with("https://") || url.starts_with("http://"))
            || canonical.contains_key("command")
            || canonical.contains_key("args")
            || canonical.contains_key("env")
            || canonical.contains_key("environment")
            || kind == Some("stdio")
        {
            return Err(INVALID);
        }
        if canonical.get("headers").is_some_and(|v| !is_string_map(v)) {
            return Err(INVALID);
        }
    } else {
        return Err(INVALID);
    }
    let input = Value::Object(Map::from_iter([(
        "server".into(),
        Value::Object(canonical),
    )]));
    normalize_mcp_servers(&input)
        .remove("server")
        .map(Some)
        .ok_or(INVALID)
}

fn is_string_map(value: &Value) -> bool {
    value
        .as_object()
        .is_some_and(|map| map.values().all(Value::is_string))
}

fn has_substitution(value: &Value) -> bool {
    match value {
        Value::String(s) => ["${", "{env:", "{file:"]
            .iter()
            .any(|marker| s.contains(marker)),
        Value::Array(items) => items.iter().any(has_substitution),
        Value::Object(map) => map.values().any(has_substitution),
        _ => false,
    }
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => (),
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    out
}

// JSONC adds comments and trailing commas, not JSON5 identifiers or strings.
// Replace comments with spaces so tokens cannot concatenate, honoring strings
// and escapes; then remove only commas immediately before a closing delimiter.
fn parse_jsonc(text: &str) -> Option<Value> {
    let mut bytes = text.as_bytes().to_vec();
    let mut i = 0;
    let mut quoted = false;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if quoted => {
                i += 2;
                continue;
            }
            b'"' => quoted = !quoted,
            b'/' if !quoted && bytes.get(i + 1) == Some(&b'/') => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    bytes[i] = b' ';
                    i += 1;
                }
                continue;
            }
            b'/' if !quoted && bytes.get(i + 1) == Some(&b'*') => {
                bytes[i] = b' ';
                bytes[i + 1] = b' ';
                i += 2;
                loop {
                    if i + 1 >= bytes.len() {
                        return None;
                    }
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' {
                        bytes[i] = b' ';
                        bytes[i + 1] = b' ';
                        i += 2;
                        break;
                    }
                    bytes[i] = b' ';
                    i += 1;
                }
                continue;
            }
            _ => (),
        }
        i += 1;
    }
    i = 0;
    quoted = false;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' if quoted => {
                i += 2;
                continue;
            }
            b'"' => quoted = !quoted,
            b',' if !quoted
                && bytes[i + 1..]
                    .iter()
                    .find(|c| !c.is_ascii_whitespace())
                    .is_some_and(|c| matches!(c, b'}' | b']')) =>
            {
                if bytes[..i]
                    .iter()
                    .rfind(|c| !c.is_ascii_whitespace())
                    .is_none_or(|c| matches!(c, b'{' | b'[' | b',' | b':'))
                {
                    return None;
                }
                bytes[i] = b' ';
            }
            _ => (),
        }
        i += 1;
    }
    serde_json::from_slice(&bytes).ok()
}

#[cfg(test)]
mod tests;
