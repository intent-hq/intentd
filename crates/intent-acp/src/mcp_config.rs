//! Universal MCP config conversion (§6.8) — port of `universal-mcp-config.ts`.
//!
//! We store MCP servers in an Auggie-compatible shape. Other ACP providers
//! (OpenCode, Claude Code, Codex) each expect different formats. This module
//! normalizes the internal representation into a canonical shape and converts to
//! provider-specific formats, and injects the safe baseline env into stdio
//! servers (the `applyBaselineEnvToStdioServers` analog).

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::Path;

use agent_client_protocol::schema::v1::{
    EnvVariable, HttpHeader, McpServer, McpServerHttp, McpServerSse, McpServerStdio,
};
use serde_json::{json, Map, Value};

use crate::mcp_env::{merge_mcp_env, EnvMap};

/// Canonical, provider-agnostic MCP server description.
#[derive(Debug, Clone, PartialEq)]
pub enum NormalizedMcpServer {
    /// A stdio (command-launched) server.
    Stdio {
        /// Executable to launch.
        command: String,
        /// Arguments passed to the command.
        args: Vec<String>,
        /// Environment for the child.
        env: EnvMap,
    },
    /// A streamable-HTTP remote server.
    Http {
        /// Endpoint URL.
        url: String,
        /// Optional request headers.
        headers: Option<BTreeMap<String, String>>,
    },
    /// An SSE remote server.
    Sse {
        /// Endpoint URL.
        url: String,
        /// Optional request headers.
        headers: Option<BTreeMap<String, String>>,
    },
}

/// A name → server map (canonical shape).
pub type NormalizedMcpServers = BTreeMap<String, NormalizedMcpServer>;

fn string_map(value: &Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(map) = value.as_object() {
        for (k, v) in map {
            let s = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            out.insert(k.clone(), s);
        }
    }
    out
}

fn string_vec(value: &Value) -> Vec<String> {
    value
        .as_array()
        .map(|a| {
            a.iter()
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Normalize a set of raw MCP server configs into the canonical shape (port of
/// `normalizeMcpServers`). Accepts typed (`type: http|sse`), command, and
/// legacy `{ url }` shapes; unrecognized entries are skipped.
pub fn normalize_mcp_servers(servers: &Value) -> NormalizedMcpServers {
    let mut out = NormalizedMcpServers::new();
    let Some(map) = servers.as_object() else {
        return out;
    };
    for (name, raw) in map {
        let kind = raw.get("type").and_then(Value::as_str);
        let headers = raw.get("headers").map(string_map);
        if kind == Some("http") {
            if let Some(url) = raw.get("url").and_then(Value::as_str) {
                out.insert(
                    name.clone(),
                    NormalizedMcpServer::Http {
                        url: url.to_string(),
                        headers,
                    },
                );
            }
            continue;
        }
        if kind == Some("sse") {
            if let Some(url) = raw.get("url").and_then(Value::as_str) {
                out.insert(
                    name.clone(),
                    NormalizedMcpServer::Sse {
                        url: url.to_string(),
                        headers,
                    },
                );
            }
            continue;
        }
        if let Some(command) = raw.get("command").and_then(Value::as_str) {
            out.insert(
                name.clone(),
                NormalizedMcpServer::Stdio {
                    command: command.to_string(),
                    args: raw.get("args").map(string_vec).unwrap_or_default(),
                    env: raw
                        .get("env")
                        .map(|e| string_map(e).into_iter().collect())
                        .unwrap_or_default(),
                },
            );
            continue;
        }
        if let Some(url) = raw.get("url").and_then(Value::as_str) {
            out.insert(
                name.clone(),
                NormalizedMcpServer::Http {
                    url: url.to_string(),
                    headers,
                },
            );
        }
    }
    out
}

fn headers_to_value(headers: &Option<BTreeMap<String, String>>) -> Option<Value> {
    headers.as_ref().map(|h| {
        let mut m = Map::new();
        for (k, v) in h {
            m.insert(k.clone(), Value::String(v.clone()));
        }
        Value::Object(m)
    })
}

fn env_to_value(env: &EnvMap) -> Value {
    let mut m = Map::new();
    for (k, v) in env {
        m.insert(k.clone(), Value::String(v.clone()));
    }
    Value::Object(m)
}

#[cfg(test)]
fn pairs_array(map: &BTreeMap<String, String>) -> Value {
    Value::Array(
        map.iter()
            .map(|(k, v)| json!({ "name": k, "value": v }))
            .collect(),
    )
}

/// Convert to the OpenCode config `mcp` block (port of `toOpenCodeMcpConfig`).
pub fn to_opencode_mcp_config(normalized: &NormalizedMcpServers) -> Value {
    let mut mcp = Map::new();
    for (name, server) in normalized {
        let entry = match server {
            NormalizedMcpServer::Stdio { command, args, env } => {
                let mut command_arr = vec![Value::String(command.clone())];
                command_arr.extend(args.iter().map(|a| Value::String(a.clone())));
                json!({
                    "type": "local",
                    "command": Value::Array(command_arr),
                    "enabled": true,
                    "environment": env_to_value(env),
                })
            }
            NormalizedMcpServer::Http { url, headers }
            | NormalizedMcpServer::Sse { url, headers } => {
                let mut obj = json!({ "type": "remote", "url": url, "enabled": true });
                if let Some(h) = headers_to_value(headers) {
                    obj["headers"] = h;
                }
                obj
            }
        };
        mcp.insert(name.clone(), entry);
    }
    Value::Object(mcp)
}

/// Convert to the Claude Code `.mcp.json` format (port of `toClaudeMcpJson`).
#[cfg(test)]
pub(crate) fn to_claude_mcp_json(normalized: &NormalizedMcpServers) -> Value {
    let mut servers = Map::new();
    for (name, server) in normalized {
        let entry = match server {
            NormalizedMcpServer::Stdio { command, args, env } => json!({
                "type": "stdio",
                "command": command,
                "args": args,
                "env": env_to_value(env),
            }),
            NormalizedMcpServer::Http { url, headers } => remote_entry("http", url, headers),
            NormalizedMcpServer::Sse { url, headers } => remote_entry("sse", url, headers),
        };
        servers.insert(name.clone(), entry);
    }
    json!({ "mcpServers": Value::Object(servers) })
}

fn remote_entry(kind: &str, url: &str, headers: &Option<BTreeMap<String, String>>) -> Value {
    let mut obj = json!({ "type": kind, "url": url });
    if let Some(h) = headers_to_value(headers) {
        obj["headers"] = h;
    }
    obj
}

/// Convert to the Auggie `--mcp-config` `{ mcpServers }` shape (the storage
/// format): stdio entries are `{ command, args, env }`, remotes carry `type`.
pub fn to_auggie_mcp_config(normalized: &NormalizedMcpServers) -> Value {
    let mut servers = Map::new();
    for (name, server) in normalized {
        let entry = match server {
            NormalizedMcpServer::Stdio { command, args, env } => json!({
                "command": command,
                "args": args,
                "env": env_to_value(env),
            }),
            NormalizedMcpServer::Http { url, headers } => remote_entry("http", url, headers),
            NormalizedMcpServer::Sse { url, headers } => remote_entry("sse", url, headers),
        };
        servers.insert(name.clone(), entry);
    }
    json!({ "mcpServers": Value::Object(servers) })
}

/// Convert to the ACP `session/new` `mcpServers` array (port of
/// `toAcpMcpServers`). `env`/`headers` become `{ name, value }` arrays; absent
/// values use empty arrays as the ACP Zod schema requires.
#[cfg(test)]
pub(crate) fn to_acp_mcp_servers(normalized: &NormalizedMcpServers) -> Vec<Value> {
    let mut servers = Vec::new();
    for (name, server) in normalized {
        match server {
            NormalizedMcpServer::Stdio { command, args, env } => servers.push(json!({
                "name": name,
                "command": command,
                "args": args,
                "env": pairs_array(env),
            })),
            NormalizedMcpServer::Http { url, headers } => {
                servers.push(acp_remote(name, "http", url, headers))
            }
            NormalizedMcpServer::Sse { url, headers } => {
                servers.push(acp_remote(name, "sse", url, headers))
            }
        }
    }
    servers
}

#[cfg(test)]
fn acp_remote(
    name: &str,
    kind: &str,
    url: &str,
    headers: &Option<BTreeMap<String, String>>,
) -> Value {
    let headers = headers.clone().unwrap_or_default();
    json!({ "name": name, "type": kind, "url": url, "headers": pairs_array(&headers) })
}

/// Convert to the typed ACP schema [`McpServer`] list carried in the
/// `session/new` / `session/load` request for providers that consume MCP
/// servers from the ACP session setup (claude-code, codex, droid, grok).
/// Same wire
/// shape as [`to_acp_mcp_servers`] — stdio entries serialize untagged (no
/// `type` field), remotes carry `type: http|sse` — but typed so the session
/// lifecycle helpers take `Vec<McpServer>` directly.
pub fn to_acp_session_mcp_servers(normalized: &NormalizedMcpServers) -> Vec<McpServer> {
    let mut servers = Vec::new();
    for (name, server) in normalized {
        match server {
            NormalizedMcpServer::Stdio { command, args, env } => {
                servers.push(McpServer::Stdio(
                    McpServerStdio::new(name.clone(), command.clone())
                        .args(args.clone())
                        .env(
                            env.iter()
                                .map(|(k, v)| EnvVariable::new(k.clone(), v.clone()))
                                .collect(),
                        ),
                ));
            }
            NormalizedMcpServer::Http { url, headers } => {
                servers.push(McpServer::Http(
                    McpServerHttp::new(name.clone(), url.clone()).headers(header_pairs(headers)),
                ));
            }
            NormalizedMcpServer::Sse { url, headers } => {
                servers.push(McpServer::Sse(
                    McpServerSse::new(name.clone(), url.clone()).headers(header_pairs(headers)),
                ));
            }
        }
    }
    servers
}

fn header_pairs(headers: &Option<BTreeMap<String, String>>) -> Vec<HttpHeader> {
    headers
        .as_ref()
        .map(|h| {
            h.iter()
                .map(|(k, v)| HttpHeader::new(k.clone(), v.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// A single Codex `-c key=value` config override.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg(test)]
pub(crate) struct CodexConfigOverride {
    /// The dotted config key (e.g. `mcp_servers.foo.command`).
    pub key: String,
    /// The TOML-encoded value literal.
    pub toml_value: String,
}

#[cfg(test)]
fn toml_string_literal(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

#[cfg(test)]
fn toml_string_array_literal(values: &[String]) -> String {
    let parts: Vec<String> = values.iter().map(|v| toml_string_literal(v)).collect();
    format!("[{}]", parts.join(", "))
}

#[cfg(test)]
fn toml_inline_table_literal(map: &EnvMap) -> String {
    if map.is_empty() {
        return "{}".to_string();
    }
    let parts: Vec<String> = map
        .iter()
        .map(|(k, v)| {
            let key = if k
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                k.clone()
            } else {
                toml_string_literal(k)
            };
            format!("{key} = {}", toml_string_literal(v))
        })
        .collect();
    format!("{{ {} }}", parts.join(", "))
}

/// Convert to Codex `-c key=value` overrides (port of `toCodexMcpOverrides`).
#[cfg(test)]
pub(crate) fn to_codex_mcp_overrides(
    normalized: &NormalizedMcpServers,
) -> Vec<CodexConfigOverride> {
    let mut overrides = Vec::new();
    let mut push = |key: String, toml_value: String| {
        overrides.push(CodexConfigOverride { key, toml_value });
    };
    for (name, server) in normalized {
        let base = format!("mcp_servers.{name}");
        match server {
            NormalizedMcpServer::Stdio { command, args, env } => {
                push(format!("{base}.command"), toml_string_literal(command));
                push(format!("{base}.args"), toml_string_array_literal(args));
                push(format!("{base}.env"), toml_inline_table_literal(env));
                push(format!("{base}.enabled"), "true".to_string());
            }
            NormalizedMcpServer::Http { url, headers }
            | NormalizedMcpServer::Sse { url, headers } => {
                push(format!("{base}.url"), toml_string_literal(url));
                if let Some(h) = headers {
                    if !h.is_empty() {
                        let h: EnvMap = h.clone().into_iter().collect();
                        push(
                            format!("{base}.http_headers"),
                            toml_inline_table_literal(&h),
                        );
                    }
                }
                push(format!("{base}.enabled"), "true".to_string());
            }
        }
    }
    overrides
}

/// Return a copy of `servers` where each stdio server's `env` is the parent
/// baseline merged with that server's existing env (existing env wins). Remote
/// servers are returned unchanged (port of `applyBaselineEnvToStdioServers`).
pub fn apply_baseline_env_to_stdio_servers(
    servers: &NormalizedMcpServers,
    baseline: &EnvMap,
) -> NormalizedMcpServers {
    let mut out = NormalizedMcpServers::new();
    for (name, server) in servers {
        let next = match server {
            NormalizedMcpServer::Stdio { command, args, env } => NormalizedMcpServer::Stdio {
                command: command.clone(),
                args: args.clone(),
                env: merge_mcp_env(&[baseline, env]),
            },
            other => other.clone(),
        };
        out.insert(name.clone(), next);
    }
    out
}

/// Command + optional `PATH` override for launching the workspace-mcp bridge
/// executable (monorepo#1049). Some provider launchers (Auggie) shell-split
/// the configured stdio command without preserving whitespace, so an absolute
/// executable path containing spaces (e.g. under `~/Library/Application
/// Support/...`) fails to spawn. When `exe` contains whitespace, return its
/// basename as the command plus a `PATH` value that prepends the executable's
/// parent directory to `inherited_path` so PATH lookup resolves the same
/// binary. Note the returned `PATH` must carry the full inherited value
/// itself: [`apply_baseline_env_to_stdio_servers`] merges the baseline env
/// with the server env and the server env wins, so a bare parent-dir `PATH`
/// would clobber the baseline `PATH`. Empty inherited-`PATH` segments are
/// dropped so the override never implicitly adds the current directory to
/// lookup. Whitespace-free paths (and edge cases a basename lookup cannot
/// fix, e.g. a spaced basename or a relative path whose parent would resolve
/// against the launcher child's cwd) are returned verbatim with no override.
pub fn normalize_spaced_bridge_command(
    exe: &Path,
    inherited_path: Option<&OsStr>,
) -> (String, Option<String>) {
    let command = exe.to_string_lossy().into_owned();
    if !command.chars().any(char::is_whitespace) {
        return (command, None);
    }
    if !exe.is_absolute() {
        return (command, None);
    }
    let (Some(parent), Some(file_name)) = (exe.parent(), exe.file_name()) else {
        return (command, None);
    };
    if parent.as_os_str().is_empty() {
        return (command, None);
    }
    let basename = file_name.to_string_lossy().into_owned();
    if basename.chars().any(char::is_whitespace) {
        return (command, None);
    }
    let entries = std::iter::once(parent.to_path_buf()).chain(
        inherited_path
            .map(|p| {
                std::env::split_paths(p)
                    .filter(|entry| !entry.as_os_str().is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
    );
    match std::env::join_paths(entries) {
        Ok(joined) => (basename, Some(joined.to_string_lossy().into_owned())),
        Err(_) => (command, None),
    }
}
