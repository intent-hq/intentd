//! MCP child-process environment helpers (§6.8) — port of `mcp-env.ts`.
//!
//! Stdio MCP servers (the built-in workspace server + user-configured servers)
//! are spawned with an explicit environment. These pure helpers build a safe
//! baseline from the parent process environment, merge Intent overrides on top,
//! and redact secret values when logging MCP config. Dependency-light: no
//! stores, services, or side effects.

use std::collections::BTreeMap;

#[cfg(test)]
use serde_json::Value;

/// Env type alias used throughout (deterministic ordering for parity/tests).
pub type EnvMap = BTreeMap<String, String>;

/// Keys Intent always sets explicitly when launching MCP children, so they must
/// not be inherited from the parent-process baseline.
pub(crate) const INTENT_CONTROLLED_ENV_KEYS: &[&str] = &["ELECTRON_RUN_AS_NODE"];

/// Well-known host secret env keys that must NOT be inherited by MCP children.
/// An explicit per-server `env` value can still re-introduce any of these — the
/// denylist only filters the parent-process baseline.
pub(crate) const SECRET_ENV_KEY_DENYLIST: &[&str] = &[
    "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY",
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "GOOGLE_API_KEY",
    "GEMINI_API_KEY",
    "HF_TOKEN",
    "NPM_TOKEN",
    "SLACK_TOKEN",
    "FIGMA_TOKEN",
];

/// Placeholder used to mask env/header values when logging MCP config.
#[cfg(test)]
pub(crate) const REDACTED_VALUE: &str = "[redacted]";

/// Conservative substring patterns (matched against the upper-cased key) that
/// flag likely-secret env vars beyond the explicit denylist.
const SECRET_SUBSTRING_PATTERNS: &[&str] = &[
    "API_KEY",
    "ACCESS_KEY",
    "PRIVATE_KEY",
    "PASSWORD",
    "PASSWD",
    "CREDENTIAL",
];

/// `_`-delimited words matched so they only hit credential-style names (the TS
/// `(^|_)SECRET(_|$)` / `(^|_)TOKEN(_|$)` patterns), not arbitrary identifiers.
const SECRET_DELIMITED_WORDS: &[&str] = &["SECRET", "TOKEN"];

/// Whether an env key name looks like a host secret that should not leak into
/// MCP children via the inherited baseline (port of `isLikelySecretEnvKey`).
pub(crate) fn is_likely_secret_env_key(key: &str) -> bool {
    if SECRET_ENV_KEY_DENYLIST.contains(&key) {
        return true;
    }
    let upper = key.to_uppercase();
    if SECRET_SUBSTRING_PATTERNS.iter().any(|p| upper.contains(p)) {
        return true;
    }
    SECRET_DELIMITED_WORDS
        .iter()
        .any(|word| upper.split('_').any(|seg| seg == *word))
}

/// Build a safe baseline environment from the parent process for launching MCP
/// child processes (port of `buildBaselineMcpEnv`). Drops Intent-controlled keys
/// and keys that look like host secrets.
pub(crate) fn build_baseline_mcp_env(parent_env: &EnvMap) -> EnvMap {
    let mut baseline = EnvMap::new();
    for (key, value) in parent_env {
        if INTENT_CONTROLLED_ENV_KEYS.contains(&key.as_str()) {
            continue;
        }
        if is_likely_secret_env_key(key) {
            continue;
        }
        baseline.insert(key.clone(), value.clone());
    }
    baseline
}

/// Build the baseline from the current process environment.
#[must_use]
pub fn build_baseline_mcp_env_from_process() -> EnvMap {
    let parent: EnvMap = std::env::vars().collect();
    build_baseline_mcp_env(&parent)
}

/// Merge env layers left-to-right; later layers win (port of `mergeMcpEnv`).
pub(crate) fn merge_mcp_env(layers: &[&EnvMap]) -> EnvMap {
    let mut merged = EnvMap::new();
    for layer in layers {
        for (key, value) in *layer {
            merged.insert(key.clone(), value.clone());
        }
    }
    merged
}

#[cfg(test)]
fn redact_object_values(value: &Value) -> Value {
    match value.as_object() {
        Some(map) => {
            let mut out = serde_json::Map::new();
            for key in map.keys() {
                out.insert(key.clone(), Value::String(REDACTED_VALUE.to_string()));
            }
            Value::Object(out)
        }
        None => Value::Object(serde_json::Map::new()),
    }
}

/// Produce a log-safe copy of an MCP config: `env` and `headers` values are
/// masked (keys preserved) so debug logs never contain secret values (port of
/// `redactMcpEnvForLogging`). Operates on a `{ "mcpServers": { ... } }` object.
#[cfg(test)]
pub(crate) fn redact_mcp_env_for_logging(config: &Value) -> Value {
    let mut servers_out = serde_json::Map::new();
    if let Some(servers) = config.get("mcpServers").and_then(Value::as_object) {
        for (name, server) in servers {
            match server.as_object() {
                Some(src) => {
                    let mut redacted = src.clone();
                    if src.contains_key("env") {
                        redacted.insert("env".to_string(), redact_object_values(&src["env"]));
                    }
                    if src.contains_key("headers") {
                        redacted
                            .insert("headers".to_string(), redact_object_values(&src["headers"]));
                    }
                    servers_out.insert(name.clone(), Value::Object(redacted));
                }
                None => {
                    servers_out.insert(name.clone(), server.clone());
                }
            }
        }
    }
    serde_json::json!({ "mcpServers": Value::Object(servers_out) })
}
