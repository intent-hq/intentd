//! Audited environment names for installed provider CLIs. Values are secrets.
//!
//! Shared with login-shell capture so GUI/service launches recover the same
//! configuration without importing arbitrary shell variables. These lists cover
//! auth, configuration locations, endpoints, model aliases and networking, not
//! runtime selection, tool policy, shell hooks, or arbitrary Node options.
//! Sources: <https://code.claude.com/docs/en/env-vars> and Codex's configuration
//! reference (<https://developers.openai.com/codex/config-reference/>).
//! Custom Codex `env_key` / `env_http_headers` names are selected from user
//! configuration by [`CodexEnvNames`], never by accepting arbitrary shell env.

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

/// Version-manager configuration needed by the selected CLI's launcher.
pub const CLI_RUNTIME_ENV: &[&str] = &["VOLTA_HOME"];

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
    // Credential lifetime and nonessential networking (preserved from the old
    // CLAUDE_ capture); include the documented individual network opt-outs.
    "CLAUDE_CODE_API_KEY_HELPER_TTL_MS",
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC",
    "DISABLE_TELEMETRY",
    "DISABLE_ERROR_REPORTING",
    "DO_NOT_TRACK",
    "DISABLE_AUTOUPDATER",
    "CLAUDE_CODE_DISABLE_FEEDBACK_SURVEY",
    "CLAUDE_CODE_AWS_CHAIN_RESOLVE_TIMEOUT_MS",
    "CLAUDE_CODE_CERT_STORE",
    "CLAUDE_CODE_DISABLE_1M_CONTEXT",
    "CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS",
    "CLAUDE_CODE_OAUTH_REFRESH_TOKEN",
    "CLAUDE_CODE_OAUTH_SCOPES",
    "ANTHROPIC_VERTEX_BASE_URL",
    "API_TIMEOUT_MS",
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
        || CLI_RUNTIME_ENV.contains(&name)
}

/// Exact custom credential names read from Codex model-provider configuration.
/// No credential values are stored. Construction rejects runtime/policy names.
#[derive(Clone, Default)]
pub struct CodexEnvNames(std::collections::BTreeSet<String>);

impl CodexEnvNames {
    /// Parse provider `env_key` and `env_http_headers` references. Include every
    /// declared provider, since a profile can select one without changing the
    /// user's config file. Never treat literal header values as variable names.
    ///
    /// # Errors
    /// Returns a sanitized error on malformed TOML; the parser's error may
    /// include credential-bearing source lines and must not escape this API.
    pub fn from_config(config: &str) -> std::io::Result<Self> {
        let parsed = toml::from_str::<toml::Value>(config).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid Codex configuration",
            )
        })?;
        let mut names = Self::default();
        names.extend_providers(&parsed);
        if let Some(profiles) = parsed.get("profiles").and_then(toml::Value::as_table) {
            for profile in profiles.values() {
                names.extend_providers(profile);
            }
        }
        Ok(names)
    }

    /// Read only a bounded regular config file in the supplied Codex home.
    /// Missing files mean no custom names. This does not execute config helpers
    /// or inspect auth.json. Read the user's home before replacing it with an
    /// isolated probe home; carry the selected names into environment assembly.
    ///
    /// # Errors
    /// Returns sanitized IO/parse errors, including a config larger than 1 MiB.
    pub fn from_home(home: &std::path::Path) -> std::io::Result<Self> {
        use std::io::Read;
        const LIMIT: u64 = 1024 * 1024;
        let path = home.join("config.toml");
        let metadata = match std::fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default())
            }
            Err(error) => {
                return Err(std::io::Error::new(
                    error.kind(),
                    "cannot inspect Codex configuration",
                ))
            }
        };
        if !metadata.is_file() || metadata.len() > LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Codex configuration must be a regular file of at most 1 MiB",
            ));
        }
        let mut text = String::new();
        std::fs::File::open(path)
            .and_then(|file| file.take(LIMIT + 1).read_to_string(&mut text))
            .map_err(|error| {
                std::io::Error::new(error.kind(), "cannot read Codex configuration")
            })?;
        if text.len() as u64 > LIMIT {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Codex configuration exceeds 1 MiB",
            ));
        }
        Self::from_config(&text)
    }

    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.0.contains(name)
    }

    fn extend_providers(&mut self, config: &toml::Value) {
        let Some(providers) = config
            .get("model_providers")
            .and_then(toml::Value::as_table)
        else {
            return;
        };
        for provider in providers.values() {
            if let Some(name) = provider.get("env_key").and_then(toml::Value::as_str) {
                self.insert(name);
            }
            if let Some(headers) = provider
                .get("env_http_headers")
                .and_then(toml::Value::as_table)
            {
                for name in headers.values().filter_map(toml::Value::as_str) {
                    self.insert(name);
                }
            }
        }
    }

    fn insert(&mut self, name: &str) {
        // A config reference grants access to an auth value, not permission to
        // redirect the runtime, load arbitrary code, or change Intent policy.
        let reserved = [
            "CODEX_PATH",
            "CODEX_CONFIG",
            "CODEX_HOME",
            "INITIAL_AGENT_MODE",
            "PATH",
            "HOME",
            "USERPROFILE",
            "SHELL",
            "ENV",
            "BASH_ENV",
        ];
        let prefixes = [
            "INTENT_", "INTENTD_", "CLAUDE_", "NODE_", "LD_", "DYLD_", "npm_",
        ];
        if !reserved
            .iter()
            .any(|reserved| name.eq_ignore_ascii_case(reserved))
            && !prefixes.iter().any(|prefix| {
                name.get(..prefix.len())
                    .is_some_and(|start| start.eq_ignore_ascii_case(prefix))
            })
            && name
                .as_bytes()
                .first()
                .is_some_and(|b| b.is_ascii_alphabetic() || *b == b'_')
            && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            self.0.insert(name.to_owned());
        }
    }
}

#[cfg(test)]
mod tests;
