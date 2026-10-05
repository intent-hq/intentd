//! Provider session bundles are separate from repository objects. Selection is
//! exact, provider-owned and fail-closed; no recursive HOME traversal.

use std::path::Path;

use intent_core::{AgentMessage, Result};
use intent_providers::checkpoint::{checkpoint_policy, CheckpointPolicy};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{counter, invalid};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Session {
    pub provider: String,
    pub mode: Mode,
    pub through_seq: String,
    pub files: Vec<File>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    History,
    Load,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct File {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
}

/// Raw payloads stay outside the manifest, keyed by their validated paths.
pub struct Bundle {
    pub session: Session,
    pub payloads: Vec<(String, Vec<u8>)>,
}

fn sha256(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut hash = String::with_capacity(64);
    for byte in Sha256::digest(bytes) {
        let _ = write!(hash, "{byte:02x}");
    }
    hash
}

pub(super) fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn allowed(path: &str, policy: CheckpointPolicy) -> bool {
    intent_git::checkpoint::safe_relative_path(path)
        && policy.session_files.contains(&path)
        && !path.split('/').any(|p| {
            let p = p.to_ascii_lowercase();
            p.contains("auth")
                || p.contains("credential")
                || p.contains("secret")
                || p.contains("token")
                || p == "cache"
                || p == "caches"
        })
}

impl Session {
    /// Construct a session watermark for bounded transcript recovery.
    ///
    /// # Errors
    /// Rejects empty providers and invalid sequence counters.
    pub fn history(provider: &str, journal_seq: &str) -> Result<Self> {
        counter(journal_seq)?;
        if provider.is_empty() {
            return Err(invalid("empty provider"));
        }
        Ok(Self {
            provider: provider.into(),
            mode: Mode::History,
            through_seq: journal_seq.into(),
            files: Vec::new(),
        })
    }

    /// Validate the provider allowlist and exact capture watermark.
    ///
    /// # Errors
    /// Rejects unsafe paths, unsupported load modes and inconsistent watermarks.
    pub fn validate(&self, journal_seq: &str) -> Result<()> {
        self.validate_policy(journal_seq, checkpoint_policy(&self.provider))
    }

    fn validate_policy(&self, journal_seq: &str, policy: CheckpointPolicy) -> Result<()> {
        counter(&self.through_seq)?;
        if self.provider.is_empty() || self.through_seq != journal_seq {
            return Err(invalid("session watermark mismatch"));
        }
        match self.mode {
            Mode::History if self.files.is_empty() => Ok(()),
            Mode::Load
                if !policy.session_files.is_empty()
                    && self.files.len() == policy.session_files.len() =>
            {
                let mut seen = std::collections::HashSet::new();
                for file in &self.files {
                    if !allowed(&file.path, policy)
                        || !seen.insert(&file.path)
                        || !is_sha256(&file.sha256)
                        || file.bytes > MAX_SESSION_FILE_BYTES
                    {
                        return Err(invalid("invalid session file"));
                    }
                }
                Ok(())
            }
            _ => Err(invalid(
                "session load is not portable or history contains files",
            )),
        }
    }
}

const MAX_SESSION_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// Advertised session/load alone is insufficient. Without a provider-verified
/// allowlist and a quiesced matching journal cut, return history without reading
/// the provider directory at all.
///
/// # Errors
/// Rejects unsafe session files, inconsistent content and invalid watermarks.
pub fn capture(
    provider: &str,
    root: &Path,
    journal_seq: &str,
    quiesced_through_seq: Option<&str>,
    supports_load: bool,
) -> Result<Bundle> {
    capture_policy(
        provider,
        root,
        journal_seq,
        quiesced_through_seq,
        supports_load,
        checkpoint_policy(provider),
    )
}

fn capture_policy(
    provider: &str,
    root: &Path,
    journal_seq: &str,
    quiesced_through_seq: Option<&str>,
    supports_load: bool,
    policy: CheckpointPolicy,
) -> Result<Bundle> {
    let history = Session::history(provider, journal_seq)?;
    if !supports_load
        || quiesced_through_seq != Some(journal_seq)
        || policy.session_files.is_empty()
    {
        return Ok(Bundle {
            session: history,
            payloads: Vec::new(),
        });
    }
    let mut session = Session {
        mode: Mode::Load,
        ..history
    };
    let mut payloads: Vec<(String, Vec<u8>)> = Vec::new();
    for path in policy.session_files {
        if !allowed(path, policy) {
            return Err(invalid("unsafe provider allowlist"));
        }
        let bytes = read_file(root, path)?;
        session.files.push(File {
            path: (*path).into(),
            sha256: sha256(&bytes),
            bytes: bytes.len() as u64,
        });
        payloads.push(((*path).into(), bytes));
    }
    // Runtime holds the provider cut throughout. Re-read to reject detectable
    // writes before returning any bundle (no partial session is emitted).
    for (path, bytes) in &payloads {
        if read_file(root, path)? != *bytes {
            return Err(invalid("provider session changed during capture"));
        }
    }
    session.validate_policy(journal_seq, policy)?;
    Ok(Bundle { session, payloads })
}

fn read_file(root: &Path, relative: &str) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut path = root.to_path_buf();
    if root
        .symlink_metadata()
        .map_err(|e| invalid(e.to_string()))?
        .file_type()
        .is_symlink()
    {
        return Err(invalid("session root is symlink"));
    }
    for part in relative.split('/') {
        path.push(part);
        if path
            .symlink_metadata()
            .map_err(|e| invalid(e.to_string()))?
            .file_type()
            .is_symlink()
        {
            return Err(invalid("session symlink escape"));
        }
    }
    if !path
        .symlink_metadata()
        .map_err(|e| invalid(e.to_string()))?
        .is_file()
    {
        return Err(invalid("session file must be regular"));
    }
    let file = std::fs::File::open(&path).map_err(|e| invalid(e.to_string()))?;
    let metadata = file.metadata().map_err(|e| invalid(e.to_string()))?;
    if !metadata.is_file() || metadata.len() > MAX_SESSION_FILE_BYTES {
        return Err(invalid("invalid session file size/type"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_SESSION_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| invalid(e.to_string()))?;
    if bytes.len() as u64 > MAX_SESSION_FILE_BYTES {
        return Err(invalid("session file grew beyond limit"));
    }
    Ok(bytes)
}

/// Validate complete payloads before writing a new private session directory.
/// Provider cwd relocation is intentionally unavailable until an adapter proves
/// it safe; shipped policies therefore recover through bounded history.
///
/// # Errors
/// Rejects missing/corrupt/unsafe payloads, existing targets and filesystem failures.
pub fn restore(bundle: &Bundle, journal_seq: &str, destination: &Path) -> Result<()> {
    restore_policy(
        bundle,
        journal_seq,
        destination,
        checkpoint_policy(&bundle.session.provider),
    )
}

fn restore_policy(
    bundle: &Bundle,
    journal_seq: &str,
    destination: &Path,
    policy: CheckpointPolicy,
) -> Result<()> {
    bundle.session.validate_policy(journal_seq, policy)?;
    if bundle.payloads.len() != bundle.session.files.len() {
        return Err(invalid("session payload count mismatch"));
    }
    for (file, (path, bytes)) in bundle.session.files.iter().zip(&bundle.payloads) {
        if file.path != *path || file.bytes != bytes.len() as u64 || file.sha256 != sha256(bytes) {
            return Err(invalid("session payload hash/size mismatch"));
        }
    }
    std::fs::create_dir(destination).map_err(|e| invalid(e.to_string()))?;
    let result = (|| {
        for (path, bytes) in &bundle.payloads {
            let target = destination.join(path);
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent).map_err(|e| invalid(e.to_string()))?;
            }
            std::fs::write(target, bytes).map_err(|e| invalid(e.to_string()))?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_dir_all(destination);
    }
    result
}

/// Return bounded, quoted transcript context for a new provider session.
/// These are prompt bytes, never executable replay of acknowledged tool calls.
/// Callers supply only transcript messages at or below the session's capture watermark.
#[must_use]
pub fn replay_history(messages: &[AgentMessage], tool_content_chars: usize) -> String {
    crate::history_xml::format_history_as_xml(
        messages,
        crate::history_xml::MAX_HISTORY_CHARS,
        tool_content_chars,
    )
}

#[cfg(test)]
mod tests;
