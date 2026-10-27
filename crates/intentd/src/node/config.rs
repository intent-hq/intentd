//! The node operator's hand-written configuration: which node this is, where
//! it listens, the lease token head presents, its capacity and the provider
//! adapters it may launch. It is read from a private file and never from a
//! remote request.

use std::collections::BTreeMap;
use std::io::Read as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context as _};
use serde::Deserialize;

use intent_acp::PermissionPolicy;
use intent_transport::node_link::LeaseToken;

/// Largest accepted configuration file.
const MAX_CONFIG_BYTES: u64 = 64 * 1024;
/// Largest accepted `maxAgents`.
const MAX_AGENTS_LIMIT: u32 = 64;

/// How the node answers provider permission prompts. No client is attached
/// to a remote agent, so every prompt is resolved by policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) enum Permission {
    /// Allow low-risk prompts (reads), deny the rest.
    #[default]
    AutoByRisk,
    AllowAll,
    DenyAll,
}

impl Permission {
    pub(super) fn policy(self) -> PermissionPolicy {
        match self {
            Self::AutoByRisk => PermissionPolicy::AutoByRisk,
            Self::AllowAll => PermissionPolicy::AllowAll,
            Self::DenyAll => PermissionPolicy::DenyAll,
        }
    }
}

/// One provider adapter the node may launch.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ProviderEntry {
    /// Absolute path of the provider's ACP executable.
    pub binary: PathBuf,
    /// Replaces the provider's built-in launch arguments when present.
    #[serde(default)]
    pub args: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Raw {
    node_id: String,
    listen: SocketAddr,
    token: String,
    #[serde(default = "default_max_agents")]
    max_agents: u32,
    #[serde(default)]
    permission: Permission,
    providers: BTreeMap<String, ProviderEntry>,
}

fn default_max_agents() -> u32 {
    4
}

/// A validated node configuration.
#[derive(Debug, Clone)]
pub(super) struct Config {
    pub node_id: String,
    pub listen: SocketAddr,
    pub token: LeaseToken,
    pub max_agents: u32,
    pub permission: Permission,
    pub providers: BTreeMap<String, ProviderEntry>,
}

impl Config {
    /// Read and validate the configuration at `path`. The file must be a
    /// regular file (not a symlink) readable only by its owner, since it
    /// holds the lease token.
    pub(super) fn read(path: &Path) -> anyhow::Result<Self> {
        use std::os::unix::fs::{OpenOptionsExt as _, PermissionsExt as _};
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .with_context(|| format!("open node configuration {}", path.display()))?;
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file(),
            "node configuration must be a regular file"
        );
        ensure!(
            metadata.permissions().mode().trailing_zeros() >= 6,
            "node configuration holds the lease token and must not be readable by group or others (chmod 600)"
        );
        let mut bytes = Vec::new();
        file.take(MAX_CONFIG_BYTES + 1).read_to_end(&mut bytes)?;
        ensure!(
            bytes.len() as u64 <= MAX_CONFIG_BYTES,
            "node configuration exceeds 64 KiB"
        );
        Self::parse(&bytes)
    }

    fn parse(bytes: &[u8]) -> anyhow::Result<Self> {
        // serde's message can quote the offending value (the token), so
        // report only where parsing failed.
        let raw: Raw = serde_json::from_slice(bytes).map_err(|e| {
            anyhow::anyhow!(
                "invalid node configuration at line {} column {} ({:?})",
                e.line(),
                e.column(),
                e.classify()
            )
        })?;
        ensure!(
            valid_id(&raw.node_id),
            "nodeId must be 1-128 characters of letters, digits, '.', '_' or '-'"
        );
        let token = LeaseToken::new(raw.token)
            .map_err(|_| anyhow::anyhow!("token must be at least 32 header-safe characters"))?;
        ensure!(
            (1..=MAX_AGENTS_LIMIT).contains(&raw.max_agents),
            "maxAgents must be between 1 and {MAX_AGENTS_LIMIT}"
        );
        ensure!(
            !raw.providers.is_empty(),
            "providers must configure at least one provider"
        );
        for (id, entry) in &raw.providers {
            if intent_providers::find_provider(id).is_none() {
                bail!("provider {id:?} is not a known provider id");
            }
            executable(&entry.binary).with_context(|| format!("provider {id:?} binary"))?;
        }
        Ok(Self {
            node_id: raw.node_id,
            listen: raw.listen,
            token,
            max_agents: raw.max_agents,
            permission: raw.permission,
            providers: raw.providers,
        })
    }
}

/// An identifier that is safe in a path component and a log line.
pub(super) fn valid_id(id: &str) -> bool {
    (1..=128).contains(&id.len())
        && id != "."
        && id != ".."
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

fn executable(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    ensure!(path.is_absolute(), "must be an absolute path");
    let metadata = std::fs::metadata(path).context("cannot inspect it")?;
    ensure!(
        metadata.is_file() && metadata.permissions().mode() & 0o111 != 0,
        "is not an executable file"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt as _};

    fn token() -> String {
        "t".repeat(40)
    }

    fn contents() -> serde_json::Value {
        serde_json::json!({
            "nodeId": "node-1",
            "listen": "127.0.0.1:0",
            "token": token(),
            "maxAgents": 2,
            "providers": { "claude-code": { "binary": std::env::current_exe().unwrap() } }
        })
    }

    fn write(path: &Path, value: &serde_json::Value) {
        std::fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    #[test]
    fn node_config_reads_a_private_hand_written_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.json");
        write(&path, &contents());
        let config = Config::read(&path).unwrap();
        assert_eq!(config.node_id, "node-1");
        assert_eq!(config.max_agents, 2);
        assert_eq!(config.permission, Permission::AutoByRisk);
        assert!(config.token.matches(&token()));
        assert_eq!(config.providers.len(), 1);
        assert_eq!(config.providers["claude-code"].args, None);

        let mut value = contents();
        value.as_object_mut().unwrap().remove("maxAgents");
        value["permission"] = serde_json::json!("allowAll");
        value["providers"]["claude-code"]["args"] = serde_json::json!(["--acp"]);
        write(&path, &value);
        let config = Config::read(&path).unwrap();
        assert_eq!(config.max_agents, 4);
        assert_eq!(config.permission.policy(), PermissionPolicy::AllowAll);
        assert_eq!(
            config.providers["claude-code"].args.as_deref(),
            Some(&["--acp".to_owned()][..])
        );
    }

    #[test]
    fn node_config_rejects_invalid_values() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.json");
        let cases: Vec<(&str, serde_json::Value)> = vec![
            ("nodeId", serde_json::json!("../escape")),
            ("nodeId", serde_json::json!("")),
            ("token", serde_json::json!("short")),
            ("maxAgents", serde_json::json!(0)),
            ("maxAgents", serde_json::json!(65)),
            ("providers", serde_json::json!({})),
            (
                "providers",
                serde_json::json!({ "not-a-provider": { "binary": std::env::current_exe().unwrap() } }),
            ),
            (
                "providers",
                serde_json::json!({ "claude-code": { "binary": "relative/bin" } }),
            ),
            (
                "providers",
                serde_json::json!({ "claude-code": { "binary": dir.path() } }),
            ),
            ("listen", serde_json::json!("not an address")),
            ("unexpected", serde_json::json!(true)),
        ];
        for (field, bad) in cases {
            let mut value = contents();
            value[field] = bad.clone();
            write(&path, &value);
            assert!(Config::read(&path).is_err(), "{field} = {bad}");
        }
    }

    #[test]
    fn node_config_errors_never_quote_the_token() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.json");
        let secret = format!("canary-{}", "s".repeat(40));
        let mut value = contents();
        value["token"] = serde_json::json!(secret);
        // A type error right after the token makes serde describe the input.
        value["maxAgents"] = serde_json::json!("two");
        write(&path, &value);
        let error = format!("{:#}", Config::read(&path).unwrap_err());
        assert!(!error.contains("canary"), "{error}");
        // A token with a character that cannot travel in a header.
        value["maxAgents"] = serde_json::json!(2);
        value["token"] = serde_json::json!(format!("{secret}\n"));
        write(&path, &value);
        let error = format!("{:#}", Config::read(&path).unwrap_err());
        assert!(!error.contains("canary"), "{error}");
    }

    #[test]
    fn node_config_must_be_a_private_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.json");
        write(&path, &contents());
        let alias = dir.path().join("alias.json");
        symlink(&path, &alias).unwrap();
        assert!(Config::read(&alias).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let error = format!("{:#}", Config::read(&path).unwrap_err());
        assert!(error.contains("chmod 600"), "{error}");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(&path, vec![b' '; 64 * 1024 + 1]).unwrap();
        assert!(Config::read(&path).is_err());
    }
}
