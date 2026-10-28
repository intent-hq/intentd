//! The head's hand-written configuration of its one execution node: which
//! node, where it listens, its certificate pin and the lease token. It holds
//! the token, so it lives in a private file and errors never quote it.

use std::io::Read as _;
use std::path::Path;

use serde::Deserialize;

use intent_core::{Error, Result};
use intent_transport::node_link::{CertificatePin, LeaseToken, NodeEndpoint};

/// File name of the configuration in the daemon's data directory.
pub const CONFIG_FILE: &str = "remote-node.json";

/// Largest accepted configuration file.
const MAX_CONFIG_BYTES: u64 = 64 * 1024;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Raw {
    node_id: String,
    #[serde(default)]
    name: Option<String>,
    endpoint: String,
    certificate_sha256: String,
    token: String,
}

/// A validated node configuration. `Debug` never shows the token.
#[derive(Debug, Clone)]
pub struct HeadNodeConfig {
    pub node_id: String,
    /// Display name; the node id when the file names none.
    pub name: String,
    pub endpoint: NodeEndpoint,
    /// The pin as 64 lowercase hex digits.
    pub certificate_sha256: String,
}

fn invalid(detail: impl std::fmt::Display) -> Error {
    Error::InvalidParams(format!("remote-node-config: {detail}"))
}

impl HeadNodeConfig {
    /// Read and validate the configuration at `path`; `None` when there is
    /// no such file. The file must be a regular file, and on Unix readable
    /// only by its owner, since it holds the lease token.
    ///
    /// # Errors
    ///
    /// `remote-node-config` naming what is wrong, never the token.
    pub fn read_if_present(path: &Path) -> Result<Option<Self>> {
        let mut options = std::fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = match options.open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(invalid(format_args!("cannot open the file: {error}"))),
        };
        let metadata = file
            .metadata()
            .map_err(|e| invalid(format_args!("cannot read the file: {e}")))?;
        if !metadata.is_file() {
            return Err(invalid("must be a regular file"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(invalid(
                    "the file holds the lease token and must not be readable by group or others (chmod 600)",
                ));
            }
        }
        let mut bytes = Vec::new();
        file.take(MAX_CONFIG_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|e| invalid(format_args!("cannot read the file: {e}")))?;
        if bytes.len() as u64 > MAX_CONFIG_BYTES {
            return Err(invalid("the file exceeds 64 KiB"));
        }
        Self::parse(&bytes).map(Some)
    }

    /// Validate configuration JSON.
    ///
    /// # Errors
    ///
    /// `remote-node-config` naming what is wrong, never the token.
    pub fn parse(bytes: &[u8]) -> Result<Self> {
        // serde's message can quote the offending value (the token), so
        // report only where parsing failed.
        let raw: Raw = serde_json::from_slice(bytes).map_err(|e| {
            invalid(format_args!(
                "invalid JSON at line {} column {} ({:?})",
                e.line(),
                e.column(),
                e.classify()
            ))
        })?;
        let node_id = raw.node_id.trim().to_owned();
        if node_id.is_empty()
            || node_id.len() > 128
            || !node_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        {
            return Err(invalid(
                "nodeId must be 1-128 letters, digits, '-', '_' or '.'",
            ));
        }
        let url = raw.endpoint.trim().trim_end_matches('/').to_owned();
        let authority = url.strip_prefix("wss://").unwrap_or_default();
        if authority.is_empty() || authority.contains(['/', '?', '#', '@']) {
            return Err(invalid("endpoint must be wss://host:port"));
        }
        let pin = CertificatePin::parse(&raw.certificate_sha256)
            .map_err(|_| invalid("certificateSha256 must be a SHA-256 in hex"))?;
        let certificate_sha256: String = raw
            .certificate_sha256
            .chars()
            .filter(|c| *c != ':')
            .map(|c| c.to_ascii_lowercase())
            .collect();
        let token = LeaseToken::new(raw.token)
            .map_err(|_| invalid("token must be at least 32 printable ASCII bytes"))?;
        let name = raw
            .name
            .map(|name| name.trim().to_owned())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| node_id.clone());
        Ok(Self {
            node_id,
            name,
            endpoint: NodeEndpoint { url, pin, token },
            certificate_sha256,
        })
    }
}
