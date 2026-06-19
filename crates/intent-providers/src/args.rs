//! Launch argument / environment assembly and PATH enrichment (§6.2, §6.9).
//!
//! Pure data helpers — no process spawning or filesystem writes (that is M3.3).
//! Ports the arg assembly from `provider-registry.ts` (`getACPWithProvider`),
//! the per-provider flag appends from `acp-provider.ts`, `buildProviderEnv`
//! (`provider-config.ts`), and `getAuggieExecPATH` (`execute-auggie-command.ts`).

use std::collections::BTreeMap;
use std::path::Path;

use crate::config::ProviderConfig;

/// Sentinel model id meaning "let the provider pick" — never passed as a real
/// model flag value (matches the TS `'default'` guard).
const MODEL_SENTINEL_DEFAULT: &str = "default";

/// Inputs that drive optional flag appends during arg assembly.
///
/// `model` is the raw (already provider-stripped) model id. `rules_file` /
/// `mcp_config_file` are paths the spawn layer (M3.3) will have written; here
/// they are accepted as-is so assembly stays pure.
#[derive(Debug, Default, Clone)]
pub struct ArgInputs<'a> {
    /// Raw model id (no `provider:` prefix), or `None`.
    pub model: Option<&'a str>,
    /// Path to a rules file, appended when the provider supports rules.
    pub rules_file: Option<&'a str>,
    /// Path to an MCP config file, appended when the provider supports MCP.
    pub mcp_config_file: Option<&'a str>,
    /// Whether to append the provider's quiet flag (simple/background requests).
    pub quiet: bool,
}

/// Assemble the launch arguments for a provider.
///
/// Order mirrors the TS flow: `base_args`, then `--model <id>`, then quiet,
/// rules, and MCP-config flags — each gated on the provider's capability flags.
pub fn build_provider_args(config: &ProviderConfig, inputs: &ArgInputs) -> Vec<String> {
    let mut args: Vec<String> = config.base_args.iter().map(|s| s.to_string()).collect();

    if let (Some(model), Some(flag)) = (inputs.model, config.model_flag) {
        if !model.is_empty() && model != MODEL_SENTINEL_DEFAULT {
            args.push(flag.to_string());
            args.push(model.to_string());
        }
    }

    if inputs.quiet {
        if let Some(flag) = config.quiet_flag {
            if !args.iter().any(|a| a == flag) {
                args.push(flag.to_string());
            }
        }
    }

    if config.supports_rules_file {
        if let (Some(flag), Some(path)) = (config.rules_flag, inputs.rules_file) {
            args.push(flag.to_string());
            args.push(path.to_string());
        }
    }

    if config.supports_mcp_config {
        if let (Some(flag), Some(path)) = (config.mcp_config_flag, inputs.mcp_config_file) {
            args.push(flag.to_string());
            args.push(path.to_string());
        }
    }

    args
}

/// Upsert a Codex `-c key="value"` config override into an argument list.
///
/// Codex parses config overrides as TOML, so the value is TOML-quoted (with
/// embedded quotes escaped). Any existing `-c`/`--config` entry for the same
/// `key` is removed before the new value is appended. Port of
/// `upsertCodexConfigArgs` (`provider-registry.ts`).
pub fn upsert_codex_config_args(args: &[String], key: &str, value: &str) -> Vec<String> {
    let escaped = value.replace('"', "\\\"");
    let config_value = format!("{key}=\"{escaped}\"");
    let key_prefix = format!("{key}=");

    let mut next: Vec<String> = Vec::with_capacity(args.len() + 2);
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if (a == "-c" || a == "--config") && i + 1 < args.len() {
            let v = &args[i + 1];
            if v.trim_start().starts_with(&key_prefix) {
                // Skip the old flag + its value.
                i += 2;
                continue;
            }
        }
        next.push(a.clone());
        i += 1;
    }

    next.push("-c".to_string());
    next.push(config_value);
    next
}

/// Apply Codex model config args (`-c model=…`, `-c model_reasoning_effort=…`).
///
/// Mirrors the codex branch of `getACPWithProvider` (`provider-registry.ts`):
/// when `raw_model` is set and not the `default` sentinel, the model id is split
/// into base + reasoning effort; the base is written as `model`, and the effort
/// (from the model id, else the `env_effort` fallback) as `model_reasoning_effort`.
/// `env_effort` is supplied by the spawn layer (e.g. `CODEX_REASONING_EFFORT`).
pub fn apply_codex_config_args(
    args: Vec<String>,
    raw_model: Option<&str>,
    env_effort: Option<&str>,
) -> Vec<String> {
    let Some(model) = raw_model else {
        return args;
    };
    if model.is_empty() || model == MODEL_SENTINEL_DEFAULT {
        return args;
    }

    let (base_model, effort) = crate::models::parse_codex_reasoning_effort(model);
    let mut args = upsert_codex_config_args(&args, "model", &base_model);
    if let Some(effort) = effort {
        args = upsert_codex_config_args(&args, "model_reasoning_effort", &effort);
    } else if let Some(env_effort) = env_effort {
        if !env_effort.is_empty() {
            args = upsert_codex_config_args(&args, "model_reasoning_effort", env_effort);
        }
    }
    args
}

/// Build provider-specific environment variables. Port of `buildProviderEnv`.
///
/// - `cortex`: `ELECTRON_RUN_AS_NODE=1` (run the Electron binary as Node).
/// - `opencode`: `OPENCODE_CONFIG_CONTENT={"model":"<model>"}` when a model is
///   set, because the `opencode acp` subcommand has no `--model` flag.
pub fn build_provider_env(provider_id: &str, model: Option<&str>) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    match provider_id {
        "cortex" => {
            env.insert("ELECTRON_RUN_AS_NODE".to_string(), "1".to_string());
        }
        "opencode" => {
            if let Some(model) = model {
                env.insert(
                    "OPENCODE_CONFIG_CONTENT".to_string(),
                    format!("{{\"model\":\"{}\"}}", json_escape(model)),
                );
            }
        }
        _ => {}
    }
    env
}

/// Minimal JSON string escaping for the small `OPENCODE_CONFIG_CONTENT` value
/// (avoids pulling a serializer in to keep deps to `intent-core` only).
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(c),
        }
    }
    out
}

/// Platform PATH list separator.
const PATH_SEP: char = if cfg!(windows) { ';' } else { ':' };

/// Build an enhanced PATH for spawning a provider binary (§6.2).
///
/// Prepends the discovered provider binary's parent directory and `~/.augment/bin`
/// to the current PATH so a `#!/usr/bin/env node` shebang resolves the right
/// `node`. Entries are de-duplicated while preserving order. Port of the
/// `getAuggieExecPATH` behavior (generalized across providers).
pub fn enhanced_path(provider_binary: Option<&Path>) -> String {
    let mut parts: Vec<String> = Vec::new();
    let push = |p: String, parts: &mut Vec<String>| {
        if !p.is_empty() && !parts.contains(&p) {
            parts.push(p);
        }
    };

    if let Some(bin) = provider_binary {
        if bin.is_absolute() {
            if let Some(parent) = bin.parent() {
                push(parent.to_string_lossy().into_owned(), &mut parts);
            }
        }
    }

    if let Some(home) = home_dir() {
        let augment_bin = home.join(".augment").join("bin");
        push(augment_bin.to_string_lossy().into_owned(), &mut parts);
    }

    if let Some(current) = std::env::var_os("PATH") {
        for entry in current.to_string_lossy().split(PATH_SEP) {
            let trimmed = entry.trim();
            if !trimmed.is_empty() {
                push(trimmed.to_string(), &mut parts);
            }
        }
    }

    parts.join(&PATH_SEP.to_string())
}

/// Resolve the user's home directory from environment, cross-platform.
fn home_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(std::path::PathBuf::from)
}
