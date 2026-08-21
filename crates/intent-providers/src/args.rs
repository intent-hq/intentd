//! Launch argument / environment assembly and PATH enrichment (§6.2, §6.9).
//!
//! Pure data helpers — no process spawning or filesystem writes (that is M3.3).
//! Ports the arg assembly from `provider-registry.ts` (`getACPWithProvider`),
//! the per-provider flag appends from `acp-provider.ts`, `buildProviderEnv`
//! (`provider-config.ts`), and `getAuggieExecPATH` (`execute-auggie-command.ts`).

use std::collections::{BTreeMap, HashSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use crate::config::{ProviderConfig, ProviderRuntime};
use intent_core::path_utils;

/// Sentinel model id meaning "let the provider pick" — never passed as a real
/// model flag value (matches the TS `'default'` guard).
const MODEL_SENTINEL_DEFAULT: &str = "default";

/// npm package the unsloth provider block resolves through. Bundled inside
/// the opencode binary (verified via `strings` on `~/.opencode/bin/opencode`),
/// so no network install happens at spawn time.
const UNSLOTH_NPM_PACKAGE: &str = "@ai-sdk/openai-compatible";

/// Provider id used inside opencode's config for the injected block. Matches
/// the id Unsloth's own `unsloth start opencode --no-launch` writes into its
/// generated opencode.json, so both configs are interchangeable.
const UNSLOTH_OPENCODE_PROVIDER_ID: &str = "unsloth-studio";

/// Display name for the unsloth provider block inside opencode's config.
const UNSLOTH_PROVIDER_NAME: &str = "Unsloth (local)";

/// Per-model token limits for the unsloth provider block, discovered from the
/// running server by the managed-server lifecycle (mirrors the `limit` object
/// Unsloth writes into its generated opencode.json).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnslothModelLimit {
    /// Context window in tokens (e.g. 262144).
    pub context: u64,
    /// Max output tokens (e.g. 8192).
    pub output: u64,
}

/// Connection parameters for the Unsloth-managed OpenAI-compatible server,
/// injected as a custom `provider.unsloth-studio` block into
/// `OPENCODE_CONFIG_CONTENT` (the unsloth provider rides the opencode
/// binary). Values are produced by the managed-server lifecycle at spawn
/// time; until that lifecycle exists callers pass `None` and the child
/// spawns with permission-only config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnslothEndpoint {
    /// OpenAI-compatible base URL (e.g. `http://127.0.0.1:8752/v1`). Requests
    /// hit `POST {base_url}/chat/completions` with `stream: true` (SSE).
    pub base_url: String,
    /// API key, sent by opencode as `Authorization: Bearer <api_key>`.
    pub api_key: String,
    /// Model id served by the endpoint — the full repo id (e.g.
    /// `unsloth/gemma-4-26B-A4B-it-GGUF`), keying the provider's `models` map
    /// and the default when the session carries no model of its own.
    pub model_id: String,
    /// Human-readable model name for opencode's model picker; falls back to
    /// [`Self::model_id`] when `None`.
    pub model_display_name: Option<String>,
    /// Token limits for the served model, when discovered by the lifecycle.
    pub limit: Option<UnslothModelLimit>,
    /// Tokens reserved for compaction; emits
    /// `"compaction":{"auto":true,"reserved":<n>}` when set (matching
    /// Unsloth's generated opencode.json).
    pub compaction_reserved: Option<u64>,
}

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
    /// Provider-native tools to strip via the provider's `--remove-tool`
    /// equivalent. Emitted once per name, deduped, and gated on
    /// [`ProviderConfig::remove_tool_flag`] — unknown providers ignore this
    /// input rather than receive a flag they don't understand.
    pub tools_to_remove: &'a [&'a str],
}

/// Assemble the launch arguments for a provider.
///
/// Order mirrors the TS flow: `base_args`, then `--model <id>`, then quiet,
/// rules, and MCP-config flags — each gated on the provider's capability flags.
pub fn build_provider_args(config: &ProviderConfig, inputs: &ArgInputs) -> Vec<String> {
    let mut args: Vec<String> = config
        .base_args
        .iter()
        .map(std::string::ToString::to_string)
        .collect();

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

    if let Some(flag) = config.remove_tool_flag {
        // Dedupe by tool name so callers can safely concatenate lists.
        let mut seen: Vec<&str> = Vec::new();
        for tool in inputs.tools_to_remove {
            if tool.is_empty() || seen.contains(tool) {
                continue;
            }
            seen.push(tool);
            args.push(flag.to_string());
            args.push((*tool).to_string());
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
pub(crate) fn upsert_codex_config_args(args: &[String], key: &str, value: &str) -> Vec<String> {
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

/// Default V8 old-space cap (MB) injected for Node/Electron provider
/// subprocesses.
///
/// V8's default old-space cap (~1.7 GB) is too small for long-lived
/// coordinator sessions, which V8-OOM (SIGABRT, `FatalProcessOutOfMemory`)
/// mid-turn with no error surfaced (STAB-50). 8 GB gives ample headroom.
const DEFAULT_MAX_OLD_SPACE_MB: u32 = 8192;

/// Env seam overriding [`DEFAULT_MAX_OLD_SPACE_MB`].
const MAX_OLD_SPACE_ENV: &str = "INTENTD_ACP_NODE_MAX_OLD_SPACE_MB";

/// Build provider-specific environment variables. Port of `buildProviderEnv`.
///
/// - Any provider on a V8 runtime ([`ProviderRuntime::Node`] or
///   [`ProviderRuntime::Electron`]): `NODE_OPTIONS=--max-old-space-size=<MB>`
///   to raise the V8 heap cap (STAB-50); appends to an inherited
///   `NODE_OPTIONS` and skips entirely when the caller already set
///   `--max-old-space-size`. [`ProviderRuntime::Native`] binaries are left
///   untouched — unless the spawn goes through npx, which always runs a Node
///   child ([`build_provider_env_for_spawn`]).
/// - `cortex`: `ELECTRON_RUN_AS_NODE=1` (run the Electron binary as Node).
/// - `opencode` / `unsloth`: `OPENCODE_CONFIG_CONTENT` with `model` (when
///   set), `instructions` (when a rules file path is provided), and `mcp`
///   (when a pre-serialized MCP block is provided via `mcp_config_json`).
///   The unsloth endpoint (which adds the `provider.unsloth-studio` block)
///   rides [`build_provider_env_with_unsloth`]; this 4-arg form passes `None`.
///
/// `mcp_config_json` must be a serialized JSON object in the `OpenCode` `mcp`
/// config shape (see `to_opencode_mcp_config` in `intent-acp`); it is spliced
/// in verbatim. Providers that don't take env config ignore it.
pub fn build_provider_env(
    config: &ProviderConfig,
    model: Option<&str>,
    rules_file: Option<&str>,
    mcp_config_json: Option<&str>,
) -> BTreeMap<String, String> {
    build_provider_env_with_unsloth(config, model, rules_file, mcp_config_json, None)
}

/// [`build_provider_env`] plus the Unsloth endpoint. For the `unsloth`
/// provider (which rides the opencode binary), a `Some(endpoint)` injects the
/// custom OpenAI-compatible `provider.unsloth-studio` block plus
/// `model` / `small_model` defaults (`unsloth-studio/<model-id>`) and an
/// optional `compaction` block into `OPENCODE_CONFIG_CONTENT` — the same
/// shape Unsloth itself generates via `unsloth start opencode --no-launch`
/// (verified against opencode 1.18.3 and a real Unsloth install; see the
/// spike notes referenced on [`UnslothEndpoint`]). With `None`
/// (managed-server lifecycle not yet run) the unsloth child spawns with
/// permission-only config and no provider block. Every other provider
/// ignores `unsloth_endpoint`.
pub fn build_provider_env_with_unsloth(
    config: &ProviderConfig,
    model: Option<&str>,
    rules_file: Option<&str>,
    mcp_config_json: Option<&str>,
    unsloth_endpoint: Option<&UnslothEndpoint>,
) -> BTreeMap<String, String> {
    build_provider_env_for_spawn(
        config,
        model,
        rules_file,
        mcp_config_json,
        unsloth_endpoint,
        false,
    )
}

/// [`build_provider_env_with_unsloth`] plus the spawn-time npx signal.
///
/// `via_npx` reports that the spawn goes through `npx -y <package>` (the
/// provider binary did not resolve and a `fallback_npx_package` is set, or
/// the provider is npx-only). npx is Node's package runner, so the child is
/// a Node process regardless of the provider's declared [`ProviderRuntime`]
/// — the effective runtime is [`ProviderRuntime::Node`] and the STAB-50 heap
/// cap applies. Codex is the concrete case today: declared `Native` for the
/// Rust `codex-acp` binary, but its `@agentclientprotocol/codex-acp` npx
/// fallback is pure Node (intent-hq/monorepo#1661).
pub fn build_provider_env_for_spawn(
    config: &ProviderConfig,
    model: Option<&str>,
    rules_file: Option<&str>,
    mcp_config_json: Option<&str>,
    unsloth_endpoint: Option<&UnslothEndpoint>,
    via_npx: bool,
) -> BTreeMap<String, String> {
    let mut env = BTreeMap::new();
    // Effective runtime: an npx spawn always runs a Node child, whatever the
    // provider's static declaration says.
    let effective_runtime = if via_npx {
        ProviderRuntime::Node
    } else {
        config.runtime
    };
    if matches!(
        effective_runtime,
        ProviderRuntime::Node | ProviderRuntime::Electron
    ) {
        let parent = std::env::var("NODE_OPTIONS").ok();
        if let Some(node_options) =
            node_options_with_heap_cap(parent.as_deref(), max_old_space_mb())
        {
            env.insert("NODE_OPTIONS".to_string(), node_options);
        }
    }
    match config.id {
        "cortex" => {
            env.insert("ELECTRON_RUN_AS_NODE".to_string(), "1".to_string());
        }
        // unsloth spawns the opencode binary, so it shares the
        // OPENCODE_CONFIG_CONTENT assembly and additionally injects its
        // custom provider block (when the endpoint is known).
        "opencode" | "unsloth" => {
            // Always emit OPENCODE_CONFIG_CONTENT with permission.task = deny to
            // disallow the provider-native task tool (subagent spawning). Merge
            // with the model key when a model is set, the instructions array
            // when a rules file is provided, and the mcp block when workspace
            // MCP servers are supplied at spawn time. Filter out the sentinel
            // model id ("default") per build_provider_args. The permission key
            // is preserved across the MCP merge (see
            // opencode_permission_survives_mcp_merge test in intent-acp).
            let mut parts = Vec::new();
            // Always include permission.task = deny first.
            parts.push(r#""permission":{"task":"deny"}"#.to_string());
            // Filter out the sentinel model id ("default") and empty strings.
            let session_model = model.filter(|m| !m.is_empty() && *m != MODEL_SENTINEL_DEFAULT);
            if config.id == "unsloth" {
                if let Some(ep) = unsloth_endpoint {
                    // The session model (when set) wins over the endpoint's
                    // served model; the top-level `model` must use the
                    // `<provider-id>/<model-id>` form matching a key in the
                    // provider's `models` map (spike constraint), so the
                    // effective id is registered there too. `small_model`
                    // mirrors `model` and compaction is emitted when the
                    // lifecycle discovered a reserve — matching the config
                    // Unsloth itself generates via
                    // `unsloth start opencode --no-launch`.
                    let model_id = session_model.unwrap_or(&ep.model_id);
                    parts.push(unsloth_provider_part(ep, model_id));
                    let model_ref = format!(
                        "\"{UNSLOTH_OPENCODE_PROVIDER_ID}/{}\"",
                        json_escape(model_id)
                    );
                    parts.push(format!("\"model\":{model_ref}"));
                    parts.push(format!("\"small_model\":{model_ref}"));
                    if let Some(reserved) = ep.compaction_reserved {
                        parts.push(format!(
                            "\"compaction\":{{\"auto\":true,\"reserved\":{reserved}}}"
                        ));
                    }
                }
                // No endpoint: permission-only config — opencode spawns on
                // its own catalog until the managed-server lifecycle supplies
                // the endpoint at spawn time.
            } else if let Some(m) = session_model {
                parts.push(format!("\"model\":\"{}\"", json_escape(m)));
            }
            if let Some(path) = rules_file {
                parts.push(format!("\"instructions\":[\"{}\"]", json_escape(path)));
            }
            if let Some(mcp) = mcp_config_json.map(str::trim) {
                if !mcp.is_empty() {
                    parts.push(format!("\"mcp\":{mcp}"));
                }
            }
            env.insert(
                "OPENCODE_CONFIG_CONTENT".to_string(),
                format!("{{{}}}", parts.join(",")),
            );
        }
        _ => {}
    }
    env
}

/// Serialize the `provider.unsloth-studio` block for
/// `OPENCODE_CONFIG_CONTENT`: an OpenAI-compatible custom provider resolved
/// through the bundled [`UNSLOTH_NPM_PACKAGE`], carrying the endpoint's
/// baseURL + apiKey and a `models` map keyed by the full repo id (plus
/// `effective_model` when a session model overrides it, so the top-level
/// `model` always matches a key). Per-model `limit` (context/output tokens)
/// rides along when the lifecycle discovered it — the same shape Unsloth
/// writes into its generated opencode.json.
fn unsloth_provider_part(ep: &UnslothEndpoint, effective_model: &str) -> String {
    let display = ep.model_display_name.as_deref().unwrap_or(&ep.model_id);
    let limit = ep.limit.map_or(String::new(), |l| {
        format!(
            ",\"limit\":{{\"context\":{},\"output\":{}}}",
            l.context, l.output
        )
    });
    let mut models = format!(
        "\"{}\":{{\"name\":\"{}\"{limit}}}",
        json_escape(&ep.model_id),
        json_escape(display)
    );
    if effective_model != ep.model_id {
        let _ = write!(
            models,
            ",\"{}\":{{\"name\":\"{}\"}}",
            json_escape(effective_model),
            json_escape(effective_model)
        );
    }
    format!(
        "\"provider\":{{\"{UNSLOTH_OPENCODE_PROVIDER_ID}\":{{\"npm\":\"{UNSLOTH_NPM_PACKAGE}\",\"name\":\"{UNSLOTH_PROVIDER_NAME}\",\"options\":{{\"baseURL\":\"{}\",\"apiKey\":\"{}\"}},\"models\":{{{models}}}}}}}",
        json_escape(&ep.base_url),
        json_escape(&ep.api_key),
    )
}

/// Resolve the V8 old-space cap in MB: `INTENTD_ACP_NODE_MAX_OLD_SPACE_MB`
/// when set and parseable as `u32`, else [`DEFAULT_MAX_OLD_SPACE_MB`]
/// (with a WARN on unparseable or non-unicode overrides so a misconfigured
/// value is visible).
pub(crate) fn max_old_space_mb() -> u32 {
    match std::env::var(MAX_OLD_SPACE_ENV) {
        Ok(raw) => raw.trim().parse::<u32>().unwrap_or_else(|_| {
            tracing::warn!(
                value = %raw,
                default = DEFAULT_MAX_OLD_SPACE_MB,
                "invalid {MAX_OLD_SPACE_ENV}; falling back to default"
            );
            DEFAULT_MAX_OLD_SPACE_MB
        }),
        Err(std::env::VarError::NotUnicode(raw)) => {
            tracing::warn!(
                value = ?raw,
                default = DEFAULT_MAX_OLD_SPACE_MB,
                "non-unicode {MAX_OLD_SPACE_ENV}; falling back to default"
            );
            DEFAULT_MAX_OLD_SPACE_MB
        }
        Err(std::env::VarError::NotPresent) => DEFAULT_MAX_OLD_SPACE_MB,
    }
}

/// Compose the `NODE_OPTIONS` value carrying `--max-old-space-size=<mb>`.
///
/// - No (or blank) inherited `NODE_OPTIONS` → just the flag.
/// - Inherited value without the flag → append (never clobber user options).
/// - Inherited value that already sets `--max-old-space-size` → `None`
///   (respect the user's cap; the child inherits the parent env untouched).
pub(crate) fn node_options_with_heap_cap(parent: Option<&str>, mb: u32) -> Option<String> {
    let flag = format!("--max-old-space-size={mb}");
    match parent.map(str::trim) {
        None | Some("") => Some(flag),
        Some(existing) if existing.contains("--max-old-space-size") => None,
        Some(existing) => Some(format!("{existing} {flag}")),
    }
}

/// Minimal JSON string escaping for the small `OPENCODE_CONFIG_CONTENT` value.
/// Handles the subset needed for model ids and file paths: double-quote, backslash,
/// common whitespace escapes (`\n`, `\r`, `\t`), backspace (`\b`), form feed (`\f`),
/// and other control characters via `\uXXXX` escapes.
///
/// (avoids pulling a serializer in to keep deps minimal — `intent-core` plus
/// `tracing` for the heap-cap parse-failure WARN).
pub(crate) fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x08' => out.push_str("\\b"), // backspace
            '\x0C' => out.push_str("\\f"), // form feed
            c if c.is_control() => {
                // Escape other control characters as \uXXXX.
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
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
/// (auggie's install location, for back-compat) to the current PATH so a
/// `#!/usr/bin/env node` shebang resolves the right `node`. Entries are
/// de-duplicated while preserving order. Port of the `getAuggieExecPATH` behavior
/// (generalized across providers).
pub fn enhanced_path(provider_binary: Option<&Path>) -> String {
    enhanced_path_with(
        provider_binary,
        home_dir().as_deref(),
        std::env::var_os("PATH").as_deref(),
    )
}

/// The ordered, de-duplicated directory list behind [`enhanced_path`] — the
/// exact PATH the spawned provider child sees, as directories. Exposed so
/// pre-spawn probes (the pi CLI version gate) can resolve binaries against
/// the same directories, in the same order, as the child would.
pub(crate) fn enhanced_path_spawn_dirs(provider_binary: Option<&Path>) -> Vec<PathBuf> {
    enhanced_path_dirs_with(
        provider_binary,
        home_dir().as_deref(),
        std::env::var_os("PATH").as_deref(),
    )
}

/// [`enhanced_path`] with an explicit `home` and inherited `path` (test seam —
/// avoids mutating the process-global `HOME` / `PATH` in parallel tests).
pub(crate) fn enhanced_path_with(
    provider_binary: Option<&Path>,
    home: Option<&Path>,
    inherited_path: Option<&std::ffi::OsStr>,
) -> String {
    // Join with platform-specific separator
    enhanced_path_dirs_with(provider_binary, home, inherited_path)
        .iter()
        .map(|d| d.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(&PATH_SEP.to_string())
}

/// The directory list [`enhanced_path_with`] joins — the single source of the
/// spawn-time PATH precedence (provider-binary dir → `~/.augment/bin` →
/// enriched tool dirs → inherited PATH).
pub(crate) fn enhanced_path_dirs_with(
    provider_binary: Option<&Path>,
    home: Option<&Path>,
    inherited_path: Option<&std::ffi::OsStr>,
) -> Vec<PathBuf> {
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();

    // 1. Provider binary directory (highest priority for co-located dependencies like node)
    if let Some(bin) = provider_binary {
        if bin.is_absolute() {
            if let Some(parent) = bin.parent() {
                path_utils::push_dir(&mut dirs, &mut seen, parent.to_path_buf());
            }
        }
    }

    // 2. ~/.augment/bin (auggie's install location, kept for auggie back-compat)
    if let Some(home) = home {
        path_utils::push_dir(&mut dirs, &mut seen, home.join(".augment").join("bin"));
    }

    // 3. Enriched tool directories (node, nvm, homebrew, volta, asdf, etc.)
    let enriched_dirs = home.map_or_else(path_utils::enriched_tool_dirs, |home| {
        path_utils::enriched_tool_dirs_with_home(Some(home))
    });
    for dir in enriched_dirs {
        path_utils::push_dir(&mut dirs, &mut seen, dir);
    }

    // 4. Inherited PATH (lowest priority)
    if let Some(path) = inherited_path {
        for dir in std::env::split_paths(path) {
            path_utils::push_dir(&mut dirs, &mut seen, dir);
        }
    }

    dirs
}

/// Resolve the user's home directory from environment, cross-platform.
fn home_dir() -> Option<std::path::PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(std::path::PathBuf::from)
}
