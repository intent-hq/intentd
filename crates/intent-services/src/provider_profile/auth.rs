//! Allowlisted data projections. These functions never execute helpers, read the
//! daemon environment, follow includes, or print source text on parser failure.
use super::{ProfileError, ProfileResult};
use serde_json::{Map, Value};

pub(super) fn toml_json(text: &str) -> ProfileResult<Value> {
    let doc = text
        .parse::<toml_edit::DocumentMut>()
        .map_err(|_| ProfileError::InvalidAuth("malformed TOML"))?;
    item_json(doc.as_item())
}

fn item_json(item: &toml_edit::Item) -> ProfileResult<Value> {
    if let Some(table) = item.as_table_like() {
        return table
            .iter()
            .map(|(key, item)| Ok((key.to_owned(), item_json(item)?)))
            .collect::<ProfileResult<Map<_, _>>>()
            .map(Value::Object);
    }
    if let Some(tables) = item.as_array_of_tables() {
        return tables
            .iter()
            .map(|t| item_json(&toml_edit::Item::Table(t.clone())))
            .collect::<ProfileResult<Vec<_>>>()
            .map(Value::Array);
    }
    item.as_value().map_or(Ok(Value::Null), value_json)
}

fn value_json(value: &toml_edit::Value) -> ProfileResult<Value> {
    match value {
        toml_edit::Value::String(v) => Ok(Value::String(v.value().clone())),
        toml_edit::Value::Integer(v) => Ok((*v.value()).into()),
        toml_edit::Value::Float(v) => serde_json::Number::from_f64(*v.value())
            .map(Value::Number)
            .ok_or(ProfileError::InvalidAuth("non-finite TOML value")),
        toml_edit::Value::Boolean(v) => Ok((*v.value()).into()),
        toml_edit::Value::Array(v) => v
            .iter()
            .map(value_json)
            .collect::<ProfileResult<Vec<_>>>()
            .map(Value::Array),
        toml_edit::Value::InlineTable(v) => v
            .iter()
            .map(|(key, value)| Ok((key.to_owned(), value_json(value)?)))
            .collect::<ProfileResult<Map<_, _>>>()
            .map(Value::Object),
        toml_edit::Value::Datetime(_) => {
            Err(ProfileError::InvalidAuth("unsupported TOML datetime"))
        }
    }
}

/// Extract model/auth routing from Codex user config. Unrelated top-level
/// executable config is excluded, while unknown selected provider fields fail.
/// # Errors
/// Malformed input, unsupported auth helpers or invalid model field types fail.
pub fn project_codex_config(text: &str) -> ProfileResult<Value> {
    let source = toml_json(text)?;
    if source.get("profile").is_some() {
        return Err(ProfileError::InvalidAuth(
            "resolve the selected Codex profile before projecting routing",
        ));
    }
    let mut projected = Map::new();
    for key in [
        "model",
        "model_provider",
        "model_reasoning_effort",
        "model_reasoning_summary",
        "model_verbosity",
        "service_tier",
        "cli_auth_credentials_store",
        "forced_login_method",
        "forced_chatgpt_workspace_id",
    ] {
        if let Some(value) = source.get(key) {
            if !value.is_string() {
                return Err(ProfileError::InvalidAuth("invalid Codex routing field"));
            }
            projected.insert(key.into(), value.clone());
        }
    }
    if let Some(providers) = source.get("model_providers") {
        let providers = providers
            .as_object()
            .ok_or(ProfileError::InvalidAuth("invalid model providers"))?;
        let mut safe = Map::new();
        for (name, provider) in providers {
            let object = provider
                .as_object()
                .ok_or(ProfileError::InvalidAuth("invalid model provider"))?;
            for (field, value) in object {
                let valid = match field.as_str() {
                    "name" | "base_url" | "env_key" | "env_key_instructions" | "wire_api" => {
                        value.is_string()
                    }
                    "requires_openai_auth" | "supports_websockets" => value.is_boolean(),
                    "request_max_retries" | "stream_max_retries" | "stream_idle_timeout_ms" => {
                        value.is_u64()
                    }
                    "http_headers" | "env_http_headers" | "query_params" => value
                        .as_object()
                        .is_some_and(|map| map.values().all(Value::is_string)),
                    _ => false,
                };
                if !valid {
                    return Err(ProfileError::InvalidAuth("unsupported model provider field; configure a typed credential integration"));
                }
            }
            safe.insert(name.clone(), provider.clone());
        }
        projected.insert("model_providers".into(), Value::Object(safe));
    }
    Ok(Value::Object(projected))
}

/// Project Claude settings without retaining the native user settings tier.
/// apiKeyHelper requires a dedicated authorized integration and is not executed.
/// # Errors
/// A credential helper or unknown routing field cannot be preserved implicitly.
pub fn project_claude_settings(value: &Value) -> ProfileResult<Value> {
    let object = value
        .as_object()
        .ok_or(ProfileError::InvalidAuth("invalid Claude settings"))?;
    if object
        .get("apiKeyHelper")
        .is_some_and(|v| v.as_str() != Some(""))
    {
        return Err(ProfileError::InvalidAuth(
            "apiKeyHelper requires an explicitly authorized credential integration",
        ));
    }
    let mut projected = Map::new();
    for key in ["model", "modelOverrides", "availableModels"] {
        if let Some(value) = object.get(key) {
            projected.insert(key.into(), value.clone());
        }
    }
    if let Some(env) = object.get("env") {
        let env = env.as_object().ok_or(ProfileError::InvalidAuth(
            "invalid Claude routing environment",
        ))?;
        let mut safe = Map::new();
        for (key, value) in env {
            if credential_env_allowed("claude-code", key) {
                if !value.is_string() {
                    return Err(ProfileError::InvalidAuth(
                        "invalid credential environment value",
                    ));
                }
                safe.insert(key.clone(), value.clone());
            }
        }
        projected.insert("env".into(), Value::Object(safe));
    }
    Ok(Value::Object(projected))
}

pub(super) fn credential_env_allowed(provider: &str, name: &str) -> bool {
    if intent_core::cli_env::CLI_NETWORK_ENV.contains(&name) {
        return true;
    }
    match provider {
        "claude-code" => {
            intent_core::cli_env::CLAUDE_CLI_ENV.contains(&name) && name != "CLAUDE_CONFIG_DIR"
        }
        "codex" => ["OPENAI_API_KEY", "OPENAI_BASE_URL", "CODEX_CA_CERTIFICATE"].contains(&name),
        "pi" | "opencode" | "unsloth" => [
            "OPENAI_API_KEY",
            "ANTHROPIC_API_KEY",
            "GEMINI_API_KEY",
            "GOOGLE_API_KEY",
            "GROQ_API_KEY",
            "XAI_API_KEY",
            "OPENROUTER_API_KEY",
            "AWS_PROFILE",
            "AWS_REGION",
            "AWS_ACCESS_KEY_ID",
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "GOOGLE_APPLICATION_CREDENTIALS",
        ]
        .contains(&name),
        "auggie" => [
            "AUGMENT_SESSION_AUTH",
            "AUGMENT_API_TOKEN",
            "AUGMENT_API_URL",
        ]
        .contains(&name),
        "grok" => ["XAI_API_KEY", "GROK_API_KEY"].contains(&name),
        "droid" => name == "FACTORY_API_KEY",
        _ => false,
    }
}

/// Explicitly typed credential payloads. No arbitrary target paths or native
/// config directory copying. Callers read source material before isolation.
/// Values deliberately do not implement Debug/Serialize.
#[derive(Clone)]
pub enum CredentialFile {
    CodexAuth(Value),
    ClaudeCredentials(Value),
    PiAuth(Value),
    OpenCodeAuth(Value),
    GrokAuth(Value),
}

impl CredentialFile {
    pub(super) fn provider(&self) -> &'static str {
        match self {
            Self::CodexAuth(_) => "codex",
            Self::ClaudeCredentials(_) => "claude-code",
            Self::PiAuth(_) => "pi",
            Self::OpenCodeAuth(_) => "opencode",
            Self::GrokAuth(_) => "grok",
        }
    }
    pub(super) fn material(&self) -> ProfileResult<(&'static str, &Value)> {
        let (name, value) = match self {
            Self::CodexAuth(v) | Self::PiAuth(v) | Self::OpenCodeAuth(v) | Self::GrokAuth(v) => {
                ("auth.json", v)
            }
            Self::ClaudeCredentials(v) => (".credentials.json", v),
        };
        if !value.is_object() {
            return Err(ProfileError::InvalidAuth(
                "credential file must be an object",
            ));
        }
        // Pi supports command expansion in credential values. Importing a
        // command under an auth filename is still executable configuration.
        if matches!(self, Self::PiAuth(_)) && contains_command(value) {
            return Err(ProfileError::InvalidAuth(
                "Pi credential commands require an authorized integration",
            ));
        }
        Ok((name, value))
    }
}

pub(super) fn contains_command(value: &Value) -> bool {
    match value {
        Value::String(s) => s.starts_with('!'),
        Value::Array(a) => a.iter().any(contains_command),
        Value::Object(o) => o.values().any(contains_command),
        _ => false,
    }
}

/// Encode a literal for Pi's native config grammar, without enabling commands
/// or environment interpolation. This is for typed values, not imported syntax.
pub(super) fn pi_literal(value: &str) -> String {
    let escaped = value.replace('$', "$$");
    escaped
        .strip_prefix('!')
        .map_or_else(|| escaped.clone(), |rest| format!("$!{rest}"))
}

/// Explicit deletion of one known credential store. Omitted updates retain
/// native refresh state; `credentials` replaces it; this enum revokes it.
#[derive(Clone, Copy)]
pub enum CredentialKind {
    Codex,
    Claude,
    Pi,
    OpenCode,
    Grok,
}

impl CredentialKind {
    pub(super) fn target(self) -> (&'static str, &'static str) {
        match self {
            Self::Codex => ("codex", "auth.json"),
            Self::Claude => ("claude-code", ".credentials.json"),
            Self::Pi => ("pi", "auth.json"),
            Self::OpenCode => ("opencode", "auth.json"),
            Self::Grok => ("grok", "auth.json"),
        }
    }
}
