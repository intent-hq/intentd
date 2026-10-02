//! Canonical installed runtimes behind the reviewed Codex/Claude ACP adapters.
//!
//! Resolution never consults adapter/runtime overrides and never falls back to
//! a bundled CLI. This module selects inputs only; launching and bounded version
//! probing belong to the daemon. Resolve once and carry that selection through
//! environment assembly, version probing and the subsequent adapter launch.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use intent_core::cli_env::{CLAUDE_CLI_ENV, CLI_NETWORK_ENV, CODEX_CLI_ENV};
use intent_core::path_utils::{enhanced_path_dirs, is_executable_file};

/// Provider runtime, distinct from its ACP adapter executable/package.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InstalledCli {
    Codex,
    Claude,
}

impl InstalledCli {
    #[must_use]
    pub fn for_provider(provider_id: &str) -> Option<Self> {
        match provider_id {
            "codex" => Some(Self::Codex),
            "claude-code" => Some(Self::Claude),
            _ => None,
        }
    }

    #[must_use]
    pub fn command(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }

    #[must_use]
    pub fn path_env(self) -> &'static str {
        match self {
            Self::Codex => "CODEX_PATH",
            Self::Claude => "CLAUDE_CODE_EXECUTABLE",
        }
    }

    #[must_use]
    pub fn accepts_env(self, key: &str) -> bool {
        let provider_keys = match self {
            Self::Codex => CODEX_CLI_ENV,
            Self::Claude => CLAUDE_CLI_ENV,
        };
        provider_keys.contains(&key) || CLI_NETWORK_ENV.contains(&key)
    }

    /// Inherited PATH, then Intent's known install/version-manager directories,
    /// then its cached login-shell PATH. Matches enhanced discovery precedence.
    /// Only real executable files named `codex` / `claude` qualify; aliases,
    /// shell functions and ACP adapters do not. Symlink spellings are preserved
    /// for launch (wrappers can depend on their own location).
    ///
    /// Blocking: first discovery may capture the login shell. Prewarm it or use
    /// `spawn_blocking`. The cached shell PATH/env changes on daemon restart;
    /// files in those directories are checked afresh on every call.
    ///
    /// # Errors
    /// Returns an actionable error when no canonical executable is installed.
    pub fn resolve(self) -> Result<InstalledCliRuntime, MissingInstalledCli> {
        self.resolve_in_dirs(&enhanced_path_dirs(), cfg!(windows))
    }

    fn resolve_in_dirs(
        self,
        dirs: &[PathBuf],
        is_windows: bool,
    ) -> Result<InstalledCliRuntime, MissingInstalledCli> {
        // Make relative inherited PATH entries stable before an adapter changes
        // cwd. Do not canonicalize: npm, asdf and other shims may need argv[0].
        let dirs: Vec<_> = dirs
            .iter()
            .filter_map(|dir| std::path::absolute(dir).ok())
            .collect();
        crate::discover::find_in_dirs_for(&dirs, self.command(), is_windows)
            .map(|path| InstalledCliRuntime { cli: self, path })
            .ok_or(MissingInstalledCli(self))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MissingInstalledCli(InstalledCli);

impl std::fmt::Display for MissingInstalledCli {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Install {} on the execution host and make it available on PATH, then retry. Intent requires the installed CLI; no bundled runtime is used.", self.0.command())
    }
}

impl std::error::Error for MissingInstalledCli {}

/// A validated, absolute CLI entry point, selected without runtime overrides.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstalledCliRuntime {
    cli: InstalledCli,
    path: PathBuf,
}

impl InstalledCliRuntime {
    #[must_use]
    pub fn cli(&self) -> InstalledCli {
        self.cli
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Select the relevant shell/inherited environment and layer trusted daemon
    /// overrides on top. Precedence: captured shell < inherited daemon (even an
    /// empty value) < explicit daemon env < resolved CLI and Intent Codex policy.
    /// Pass the usual provider/extra env as `overrides`, including isolated probe
    /// homes, PATH and Node policy. Never pass an untrusted shell map as overrides.
    ///
    /// Returned values may contain credentials: never log/serialize this map.
    /// This is an overlay, not `env_clear`: callers retain their existing process
    /// inheritance and removals. Unlisted custom credentials already inherited
    /// by the daemon still reach children. Shell-only custom names require an
    /// explicit daemon override. Non-Unicode daemon env should remain inherited
    /// (callers must prevent captured values replacing such entries).
    ///
    /// # Errors
    /// Returns `InvalidInput` for a non-Unicode executable path, since the
    /// adapter environment uses strings and must not launch a lossy spelling.
    pub fn environment(
        &self,
        captured: &BTreeMap<String, String>,
        inherited: &BTreeMap<String, String>,
        overrides: &BTreeMap<String, String>,
    ) -> std::io::Result<BTreeMap<String, String>> {
        let mut env = BTreeMap::new();
        for source in [captured, inherited] {
            env.extend(
                source
                    .iter()
                    .filter(|(key, _)| self.cli.accepts_env(key))
                    .map(|(key, value)| (key.clone(), value.clone())),
            );
        }
        env.extend(overrides.clone());
        // An old user/adapter override must never choose the runtime, even if
        // it survived another merge. The daemon-selected path is authoritative.
        env.remove("CODEX_PATH");
        env.remove("CLAUDE_CODE_EXECUTABLE");
        let path = self.path.to_str().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "installed CLI path is not Unicode",
            )
        })?;
        env.insert(self.cli.path_env().into(), path.into());
        if self.cli == InstalledCli::Codex {
            env.insert(
                "CODEX_CONFIG".into(),
                crate::CODEX_SUBAGENT_POLICY_CONFIG.into(),
            );
        }
        Ok(env)
    }

    /// Snapshot entry-point identity with a freshly observed CLI version.
    /// The daemon must bound `--version` time/output and run it with the same
    /// environment, PATH and working directory as the selected launch. Do not
    /// substitute the adapter version or memoize the version by entry metadata.
    /// Recheck after a model probe before publishing its result.
    ///
    /// Metadata catches normal in-place/atomic replacements and symlink changes;
    /// version catches an unchanged npm/asdf/script wrapper selecting a newly
    /// versioned payload. Neither proves arbitrary transitive payload identity:
    /// a wrapper may conceal replacements behind the same version/metadata.
    /// Catalog refresh must still reprobe on explicit refresh/TTL, and separately
    /// account for auth/config changes. This is not a cryptographic attestation.
    ///
    /// # Errors
    /// Returns an error for an empty observed version or an entry point that
    /// disappeared, lost executable permissions, or has unreadable metadata.
    pub fn identity(&self, observed_version: &str) -> std::io::Result<InstalledCliIdentity> {
        if observed_version.trim().is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "installed CLI version is empty",
            ));
        }
        if !is_executable_file(&self.path) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "installed CLI is no longer executable",
            ));
        }
        let target = std::fs::canonicalize(&self.path)?;
        let metadata = std::fs::metadata(&target)?;
        #[cfg(unix)]
        let unix_identity = {
            use std::os::unix::fs::MetadataExt;
            (
                metadata.dev(),
                metadata.ino(),
                metadata.ctime(),
                metadata.ctime_nsec(),
            )
        };
        Ok(InstalledCliIdentity {
            cli: self.cli,
            entry: self.path.clone(),
            target,
            length: metadata.len(),
            modified: metadata.modified().ok(),
            created: metadata.created().ok(),
            version: observed_version.trim().to_owned(),
            #[cfg(unix)]
            unix_identity,
        })
    }
}

/// Opaque equality/hash key for an in-memory catalog. Deliberately not Debug or
/// serializable: untrusted version output is not suitable for diagnostics.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct InstalledCliIdentity {
    cli: InstalledCli,
    entry: PathBuf,
    target: PathBuf,
    length: u64,
    modified: Option<SystemTime>,
    created: Option<SystemTime>,
    version: String,
    #[cfg(unix)]
    unix_identity: (u64, u64, i64, i64),
}

#[cfg(test)]
mod tests;
