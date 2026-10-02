//! Audited environment names for installed provider CLIs. Values are secrets.
//!
//! Shared with login-shell capture so GUI/service launches recover the same
//! configuration without importing arbitrary shell variables. These lists cover
//! auth, configuration locations, endpoints, model aliases and networking, not
//! runtime selection, tool policy, shell hooks, or arbitrary Node options.
//! Sources: <https://code.claude.com/docs/en/env-vars> and Codex's configuration
//! reference (<https://developers.openai.com/codex/config-reference/>).
//! Custom Codex `env_key` / `env_http_headers` names cannot be inferred from an
//! allowlist: supply those through the daemon environment or explicit launch env.

/// Proxy and CA settings consumed by the native CLIs and/or their Node adapters.
/// Both proxy spellings are intentional; the receiving runtime decides priority.
pub const CLI_NETWORK_ENV: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "NODE_EXTRA_CA_CERTS",
];

/// Codex auth/config/endpoint inputs. `CODEX_PATH` and `CODEX_CONFIG` are
/// adapter controls owned by Intent, deliberately absent from shell capture.
/// Custom CA selection is defined in
/// <https://github.com/openai/codex/blob/main/codex-rs/http-client/src/custom_ca.rs>.
pub const CODEX_CLI_ENV: &[&str] = &[
    "CODEX_HOME",
    "CODEX_CA_CERTIFICATE",
    "OPENAI_API_KEY",
    "OPENAI_BASE_URL",
];

/// Claude Code auth/config/endpoint and model-routing inputs, including the
/// documented Bedrock, Vertex and Foundry credential chains. CLI versions may
/// support only a subset; forwarding a name does not guarantee compatibility.
/// `CLAUDE_CODE_EXECUTABLE` is exclusively selected by Intent.
pub const CLAUDE_CLI_ENV: &[&str] = &[
    "CLAUDE_CONFIG_DIR",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_CUSTOM_HEADERS",
    "ANTHROPIC_MODEL",
    "ANTHROPIC_SMALL_FAST_MODEL",
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "ANTHROPIC_DEFAULT_SONNET_MODEL",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL",
    "ANTHROPIC_CUSTOM_MODEL_OPTION",
    "ANTHROPIC_CUSTOM_MODEL_OPTION_NAME",
    "ANTHROPIC_CUSTOM_MODEL_OPTION_DESCRIPTION",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_SKIP_BEDROCK_AUTH",
    "ANTHROPIC_BEDROCK_BASE_URL",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_SKIP_VERTEX_AUTH",
    "ANTHROPIC_VERTEX_PROJECT_ID",
    "CLOUD_ML_REGION",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_CLOUD_PROJECT",
    "GCLOUD_PROJECT",
    "GOOGLE_CLOUD_QUOTA_PROJECT",
    "CLAUDE_CODE_USE_FOUNDRY",
    "CLAUDE_CODE_SKIP_FOUNDRY_AUTH",
    "ANTHROPIC_FOUNDRY_API_KEY",
    "ANTHROPIC_FOUNDRY_AUTH_TOKEN",
    "ANTHROPIC_FOUNDRY_BASE_URL",
    "ANTHROPIC_FOUNDRY_RESOURCE",
    "AZURE_CLIENT_ID",
    "AZURE_CLIENT_SECRET",
    "AZURE_TENANT_ID",
    "AZURE_CLIENT_CERTIFICATE_PATH",
    "AZURE_CLIENT_CERTIFICATE_PASSWORD",
    "AZURE_FEDERATED_TOKEN_FILE",
    "AZURE_AUTHORITY_HOST",
    "AWS_PROFILE",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_CONFIG_FILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_SDK_LOAD_CONFIG",
    "AWS_BEARER_TOKEN_BEDROCK",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
    "AWS_ROLE_ARN",
    "AWS_ROLE_SESSION_NAME",
    "CLAUDE_CODE_CLIENT_CERT",
    "CLAUDE_CODE_CLIENT_KEY",
    "CLAUDE_CODE_CLIENT_KEY_PASSPHRASE",
];

/// Whether a name is relevant to either installed CLI, for shell capture.
#[must_use]
pub fn is_installed_cli_env(name: &str) -> bool {
    CODEX_CLI_ENV.contains(&name)
        || CLAUDE_CLI_ENV.contains(&name)
        || CLI_NETWORK_ENV.contains(&name)
}
