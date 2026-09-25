//! Private, bounded transport and authentication for diagnostic catalogs.
//! No raw subprocess message, account record, or credential is report text.

use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout, Command};

use super::catalog::CatalogFailure;
use super::process::ProbeProcess;
use super::CodexLaunch;

pub(super) const FILE_LIMIT: usize = 64 * 1024;
pub(super) const OUTPUT_LIMIT: usize = 1024 * 1024;
pub(super) const PHASE_TIMEOUT: Duration = Duration::from_secs(30);

/// Never Debug/Serialize: these bytes belong only in the isolated child.
#[derive(Default)]
pub(super) struct Authentication {
    file: Option<Vec<u8>>,
    env: Vec<(&'static str, OsString)>,
    pub secrets: BTreeSet<String>,
}

impl Authentication {
    pub async fn capture(launch: &CodexLaunch) -> Result<Self, CatalogFailure> {
        // Also protect this boundary if a future caller bypasses fresh_catalogs.
        super::process::ensure_supported()?;
        let command = intent_acp::spawn::build_command(&launch.spawn_options());
        let non_empty = |name| super::effective_env(&command, name).filter(|s| !s.is_empty());
        let source = non_empty("CODEX_HOME").map(PathBuf::from).or_else(|| {
            non_empty("HOME")
                .or_else(|| non_empty("USERPROFILE"))
                .map(|p| PathBuf::from(p).join(".codex"))
        });
        let env = ["OPENAI_API_KEY", "CODEX_API_KEY", "CODEX_ACCESS_TOKEN"]
            .into_iter()
            .filter_map(|key| non_empty(key).map(|value| (key, value)))
            .collect();
        Self::read(source.as_deref(), env).await
    }

    pub async fn read(
        source: Option<&Path>,
        env: Vec<(&'static str, OsString)>,
    ) -> Result<Self, CatalogFailure> {
        let mut auth = Self {
            env,
            ..Self::default()
        };
        for (_, value) in &auth.env {
            auth.secrets.insert(value.to_string_lossy().into_owned());
        }
        if let Some(source) = source {
            match read_file(&source.join("auth.json")).await {
                Ok(bytes) => {
                    let value: Value = serde_json::from_slice(&bytes)
                        .map_err(|_| CatalogFailure::AuthenticationUnavailable)?;
                    if !value.is_object() {
                        return Err(CatalogFailure::AuthenticationUnavailable);
                    }
                    collect_secrets(&value, &mut auth.secrets);
                    auth.file = Some(bytes);
                }
                Err(CatalogFailure::FileMissing) => {}
                Err(_) => return Err(CatalogFailure::AuthenticationUnavailable),
            }
        }
        Ok(auth)
    }

    pub async fn home(&self) -> Result<tempfile::TempDir, CatalogFailure> {
        let mut builder = tempfile::Builder::new();
        builder.prefix("intentd-codex-catalog-");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o700));
        }
        let home = builder
            .tempdir()
            .map_err(|_| CatalogFailure::IsolationFailed)?;
        if let Some(bytes) = &self.file {
            private_file(&home.path().join("auth.json"), bytes).await?;
        }
        // No user config, profiles, cached models, MCP definitions, or keyring.
        // A package boundary also keeps npm away from ancestor workspaces.
        private_file(&home.path().join("package.json"), b"{\"private\":true}").await?;
        private_file(
            &home.path().join("config.toml"),
            b"cli_auth_credentials_store = \"file\"\n",
        )
        .await?;
        Ok(home)
    }

    pub fn isolate(&self, command: &mut Command, launch: &CodexLaunch, home: &Path) {
        command
            .env_clear()
            .env("PATH", launch.effective_path())
            .env("HOME", home)
            .env("USERPROFILE", home)
            .env("CODEX_HOME", home)
            .env("XDG_CONFIG_HOME", home)
            .env("XDG_CACHE_HOME", home)
            .env("XDG_DATA_HOME", home)
            .env("APPDATA", home)
            .env("LOCALAPPDATA", home)
            .env("TMPDIR", home)
            .env("TMP", home)
            .env("TEMP", home)
            .env("NODE_DISABLE_COMPILE_CACHE", "1")
            .env("NO_COLOR", "1")
            .env("npm_config_cache", home.join("npm-cache"))
            .env("npm_config_userconfig", home.join("empty-npmrc"))
            .env("npm_config_globalconfig", home.join("empty-global-npmrc"))
            .current_dir(home);
        #[cfg(windows)]
        if let Some(root) = std::env::var_os("SystemRoot") {
            command.env("SystemRoot", root);
        }
        for (key, value) in &self.env {
            command.env(key, value);
        }
        // Match the snapshotted production selection. Config overrides are
        // deliberately not retained: they can reintroduce user MCP servers.
        if let Ok(Some(runtime)) = launch.runtime_override_path() {
            command.env("CODEX_PATH", runtime);
        }
    }

    pub fn model_id(&self, value: &str) -> Option<String> {
        let lower = value.to_ascii_lowercase();
        if value.is_empty()
            || value.len() > 160
            || !value
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"-._:/[]".contains(&c))
            || ["sk-", "eyj", "bearer", "access_token", "refresh_token"]
                .iter()
                .any(|s| lower.contains(s))
            || self
                .secrets
                .iter()
                .any(|s| !s.is_empty() && value.contains(s))
        {
            None
        } else {
            Some(value.to_owned())
        }
    }

    pub fn scrub(&self, text: &mut String) {
        for secret in &self.secrets {
            if !secret.is_empty() {
                *text = text.replace(secret, "[redacted]");
            }
        }
    }
}

pub(super) fn collect_secrets(value: &Value, secrets: &mut BTreeSet<String>) {
    match value {
        Value::String(s) if !s.is_empty() => {
            secrets.insert(s.clone());
        }
        Value::Array(values) => values.iter().for_each(|v| collect_secrets(v, secrets)),
        Value::Object(values) => values
            .iter()
            .filter(|(key, _)| {
                !matches!(
                    key.as_str(),
                    "type" | "auth_mode" | "planType" | "last_refresh"
                )
            })
            .for_each(|(_, value)| collect_secrets(value, secrets)),
        _ => {}
    }
}

pub(super) async fn read_file(path: &Path) -> Result<Vec<u8>, CatalogFailure> {
    let metadata = tokio::fs::metadata(path).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            CatalogFailure::FileMissing
        } else {
            CatalogFailure::IsolationFailed
        }
    })?;
    if !metadata.is_file() || metadata.len() > FILE_LIMIT as u64 {
        return Err(CatalogFailure::OutputLimit);
    }
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| CatalogFailure::IsolationFailed)?;
    let mut bytes = Vec::new();
    file.take((FILE_LIMIT + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| CatalogFailure::IsolationFailed)?;
    if bytes.len() > FILE_LIMIT {
        return Err(CatalogFailure::OutputLimit);
    }
    Ok(bytes)
}

pub(super) async fn private_file(path: &Path, bytes: &[u8]) -> Result<(), CatalogFailure> {
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options
        .open(path)
        .await
        .map_err(|_| CatalogFailure::IsolationFailed)?;
    write_private_contents(&mut file, bytes).await
}

async fn write_private_contents(
    file: &mut (impl AsyncWrite + Unpin),
    bytes: &[u8],
) -> Result<(), CatalogFailure> {
    file.write_all(bytes)
        .await
        .map_err(|_| CatalogFailure::IsolationFailed)?;
    // Tokio can accept the final write before its blocking filesystem work
    // completes. Observe completion/errors before handing these files to a child.
    file.flush()
        .await
        .map_err(|_| CatalogFailure::IsolationFailed)
}

#[cfg(test)]
#[path = "catalog_io_tests.rs"]
mod tests;

pub(super) struct Rpc {
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    consumed: usize,
    next_id: u32,
    pub notifications: Vec<Value>,
}

impl Rpc {
    pub fn new(guard: &mut ProbeProcess) -> Result<Self, CatalogFailure> {
        Ok(Self {
            input: guard.stdin.take().ok_or(CatalogFailure::SpawnFailed)?,
            output: BufReader::new(guard.stdout.take().ok_or(CatalogFailure::SpawnFailed)?),
            consumed: 0,
            next_id: 0,
            notifications: Vec::new(),
        })
    }

    pub async fn send(&mut self, value: Value) -> Result<(), CatalogFailure> {
        let mut bytes = serde_json::to_vec(&value).map_err(|_| CatalogFailure::InvalidResponse)?;
        bytes.push(b'\n');
        self.input
            .write_all(&bytes)
            .await
            .map_err(|_| CatalogFailure::ProcessExited)
    }

    async fn receive(&mut self) -> Result<Value, CatalogFailure> {
        let mut line = Vec::new();
        loop {
            let bytes = self
                .output
                .fill_buf()
                .await
                .map_err(|_| CatalogFailure::ProcessExited)?;
            if bytes.is_empty() {
                return Err(CatalogFailure::ProcessExited);
            }
            let size = bytes
                .iter()
                .position(|b| *b == b'\n')
                .map_or(bytes.len(), |i| i + 1);
            self.consumed += size;
            if self.consumed > OUTPUT_LIMIT {
                return Err(CatalogFailure::OutputLimit);
            }
            line.extend_from_slice(&bytes[..size]);
            self.output.consume(size);
            if line.last() == Some(&b'\n') {
                break;
            }
        }
        serde_json::from_slice(&line).map_err(|_| CatalogFailure::InvalidResponse)
    }

    pub async fn request(&mut self, method: &str, params: Value) -> Result<Value, CatalogFailure> {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc":"2.0", "id":id,"method":method,"params":params}))
            .await?;
        loop {
            let value = self.receive().await?;
            if value.get("method").is_some() {
                if value.get("id").is_some() {
                    // Never grant a server permission, read a file, or log in.
                    return Err(CatalogFailure::UnsupportedCapability);
                }
                self.notification(&value);
                continue;
            }
            if value.get("id").and_then(Value::as_u64) != Some(u64::from(id)) {
                return Err(CatalogFailure::InvalidResponse);
            }
            if let Some(error) = value.get("error") {
                return Err(match error.get("code").and_then(Value::as_i64) {
                    Some(-32601) => CatalogFailure::UnsupportedCapability,
                    Some(-32000) if method == "session/new" => {
                        CatalogFailure::AuthenticationUnavailable
                    }
                    Some(401 | 403) => CatalogFailure::AuthenticationUnavailable,
                    _ => CatalogFailure::RequestFailed,
                });
            }
            return value
                .get("result")
                .filter(|v| v.is_object())
                .cloned()
                .ok_or(CatalogFailure::InvalidResponse);
        }
    }

    fn notification(&mut self, value: &Value) {
        if matches!(
            value.get("method").and_then(Value::as_str),
            Some("session/update" | "sessionUpdate" | "session/updateModels")
        ) {
            if let Some(params) = value.get("params") {
                self.notifications.push(params.clone());
            }
        }
    }

    pub async fn late_notifications(&mut self) -> Result<(), CatalogFailure> {
        let collect = async {
            loop {
                match self.receive().await {
                    Ok(value) => self.notification(&value),
                    Err(CatalogFailure::ProcessExited) => return Ok(()),
                    Err(error) => return Err(error),
                }
            }
        };
        tokio::time::timeout(Duration::from_millis(100), collect)
            .await
            .unwrap_or(Ok(()))
    }
}

pub(super) async fn discard_stderr(mut stderr: tokio::process::ChildStderr) -> CatalogFailure {
    let mut buffer = [0; 8192];
    let mut count = 0;
    loop {
        match stderr.read(&mut buffer).await {
            Ok(0) => return std::future::pending().await,
            Ok(n) => {
                count += n;
                if count > OUTPUT_LIMIT {
                    return CatalogFailure::OutputLimit;
                }
            }
            Err(_) => return CatalogFailure::ProcessExited,
        }
    }
}
