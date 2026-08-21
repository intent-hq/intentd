//! File-backed secret persistence (`~/intent/secrets.json`).
//!
//! [`FileSecretStore`] is the single shared backend for all intentd secrets:
//! a flat JSON object mapping account name → secret string, stored by default
//! at `~/intent/secrets.json` (resolved from `HOME`, falling back to
//! `USERPROFILE` on Windows) and overridable via the
//! [`INTENTD_SECRETS_FILE`](SECRETS_FILE_ENV) environment variable.
//!
//! Semantics mirror the `SecretStore` trait in `intent-services` (which this
//! leaf crate must not depend on): `load` returns `None` for unset **or
//! empty-string** values, `store` replaces, and `delete` of an absent account
//! is an idempotent success.
//!
//! **Durability & corrupt-file semantics.** Writes are atomic: the full map is
//! serialized to a temp file in the same directory and renamed over the
//! target. On unix the file is created `0600` and missing parent directories
//! `0700`; on other platforms permissions are best-effort. A missing file is
//! an empty store. An unparseable file is tolerated leniently: reads log a
//! warning and behave as if the store were empty — never a panic — and the
//! corrupt content is left untouched on disk until the next successful
//! `store`, which rewrites the file as the map that was loadable at that
//! moment (empty, for a corrupt file) plus the mutation. In other words, a
//! `store` after corruption intentionally discards the unreadable content and
//! starts a fresh map; a `delete` of an account that is absent (as everything
//! is, under corruption) returns `Ok` without touching the file.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// Environment variable that overrides the default secrets-file path.
pub(crate) const SECRETS_FILE_ENV: &str = "INTENTD_SECRETS_FILE";

/// Resolve the secrets-file path: [`SECRETS_FILE_ENV`] when set and non-empty,
/// otherwise `~/intent/secrets.json` (`HOME`, falling back to `USERPROFILE`).
pub(crate) fn default_secrets_path() -> PathBuf {
    if let Some(p) = std::env::var_os(SECRETS_FILE_ENV) {
        if !p.is_empty() {
            return PathBuf::from(p);
        }
    }
    home_dir().join("intent").join("secrets.json")
}

fn home_dir() -> PathBuf {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map_or_else(|| PathBuf::from("."), PathBuf::from)
}

/// File-backed secret store (see the module docs for path resolution, atomic
/// write, and corrupt-file semantics).
#[derive(Debug, Clone)]
pub struct FileSecretStore {
    path: PathBuf,
}

impl Default for FileSecretStore {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSecretStore {
    /// Store backed by the default path ([`default_secrets_path`]).
    pub fn new() -> Self {
        Self::with_path(default_secrets_path())
    }

    /// Store backed by an explicit path (tests use this so they never touch
    /// the real `~/intent/secrets.json`).
    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// The backing file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return the stored secret for `account`: `Ok(Some(value))` if present,
    /// `Ok(None)` if confirmed absent (missing file or key not in map), or
    /// `Err` if the backing file is unreadable or corrupt (IO/parse failure).
    pub fn load(&self, account: &str) -> Result<Option<String>> {
        self.read_map_strict().map(|mut map| map.remove(account))
    }

    /// Persist `value` for `account`, replacing any existing secret.
    pub fn store(&self, account: &str, value: &str) -> Result<()> {
        let mut map = self.read_map();
        map.insert(account.to_string(), value.to_string());
        self.persist(&map)
    }

    /// Delete the secret for `account`; absence is an idempotent success (and
    /// does not rewrite the file).
    pub fn delete(&self, account: &str) -> Result<()> {
        let mut map = self.read_map();
        if map.remove(account).is_none() {
            return Ok(());
        }
        self.persist(&map)
    }

    /// Read and parse the backing file (strict, error-propagating variant).
    /// Missing file ⇒ `Ok(empty map)`; unreadable/corrupt file ⇒ `Err`.
    fn read_map_strict(&self) -> Result<BTreeMap<String, String>> {
        let bytes = match std::fs::read(&self.path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
            Err(e) => {
                return Err(Error::Internal(format!(
                    "failed to read secrets file {}: {}",
                    self.path.display(),
                    e
                )))
            }
        };
        serde_json::from_slice::<BTreeMap<String, String>>(&bytes)
            .map(|map| map.into_iter().filter(|(_, v)| !v.is_empty()).collect())
            .map_err(|e| {
                Error::Internal(format!(
                    "corrupt secrets file {}: {}",
                    self.path.display(),
                    e
                ))
            })
    }

    /// Read and parse the backing file, filtering out empty-string values.
    /// Missing file ⇒ empty map; unreadable/corrupt file ⇒ warn + empty map.
    /// (Used by `store`/`delete` which can tolerate corruption and overwrite.)
    fn read_map(&self) -> BTreeMap<String, String> {
        let bytes = match std::fs::read(&self.path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return BTreeMap::new(),
            Err(e) => {
                tracing::warn!(path = %self.path.display(), error = %e, "failed to read secrets file; treating as empty");
                return BTreeMap::new();
            }
        };
        match serde_json::from_slice::<BTreeMap<String, String>>(&bytes) {
            Ok(map) => map.into_iter().filter(|(_, v)| !v.is_empty()).collect(),
            Err(e) => {
                tracing::warn!(path = %self.path.display(), error = %e, "corrupt secrets file; treating as empty");
                BTreeMap::new()
            }
        }
    }

    /// Atomically rewrite the backing file with `map`: temp file in the same
    /// directory, then rename. Unix: file `0600`, created parent dirs `0700`.
    fn persist(&self, map: &BTreeMap<String, String>) -> Result<()> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        create_dir_private(parent)
            .map_err(|e| Error::Internal(format!("failed to create secrets dir: {e}")))?;

        let json = serde_json::to_string_pretty(map)
            .map_err(|e| Error::Internal(format!("failed to serialize secrets: {e}")))?;

        let tmp = parent.join(format!(
            ".secrets.json.tmp-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        write_private(&tmp, json.as_bytes()).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            Error::Internal(format!("failed to write secrets file: {e}"))
        })?;
        std::fs::rename(&tmp, &self.path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            Error::Internal(format!("failed to persist secrets file: {e}"))
        })
    }
}

/// `create_dir_all` that creates any missing directories with mode `0700` on
/// unix (plain `create_dir_all` elsewhere). Existing directories are left as-is.
#[cfg(unix)]
fn create_dir_private(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

#[cfg(not(unix))]
fn create_dir_private(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)
}

/// Write `contents` to a fresh file created with mode `0600` on unix (plain
/// write elsewhere), so the secrets never exist on disk with looser permissions.
#[cfg(unix)]
fn write_private(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(contents)?;
    f.sync_all()
}

#[cfg(not(unix))]
fn write_private(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    std::fs::write(path, contents)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unique temp dir under the system temp dir, removed on drop; keeps every
    /// test away from the real `~/intent/secrets.json`.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let dir =
                std::env::temp_dir().join(format!("intent-core-secrets-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn store(&self) -> FileSecretStore {
            FileSecretStore::with_path(self.0.join("secrets.json"))
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn round_trip_store_load() {
        let tmp = TempDir::new();
        let store = tmp.store();
        assert_eq!(store.load("github.token").unwrap(), None);
        store.store("github.token", "s3cret").unwrap();
        assert_eq!(
            store.load("github.token").unwrap(),
            Some("s3cret".to_string())
        );
        store.store("github.token", "rotated").unwrap();
        assert_eq!(
            store.load("github.token").unwrap(),
            Some("rotated".to_string())
        );
        store.store("linear.token", "other").unwrap();
        assert_eq!(
            store.load("github.token").unwrap(),
            Some("rotated".to_string())
        );
        assert_eq!(
            store.load("linear.token").unwrap(),
            Some("other".to_string())
        );
    }

    #[test]
    fn delete_is_idempotent() {
        let tmp = TempDir::new();
        let store = tmp.store();
        store.delete("missing").unwrap();
        assert!(
            !store.path().exists(),
            "delete of absent key must not create the file"
        );
        store.store("a", "1").unwrap();
        store.delete("a").unwrap();
        assert_eq!(store.load("a").unwrap(), None);
        store.delete("a").unwrap();
    }

    #[test]
    fn empty_values_are_filtered_on_load() {
        let tmp = TempDir::new();
        let store = tmp.store();
        std::fs::write(store.path(), r#"{"empty":"","set":"v"}"#).unwrap();
        assert_eq!(store.load("empty").unwrap(), None);
        assert_eq!(store.load("set").unwrap(), Some("v".to_string()));
    }

    #[test]
    fn corrupt_file_errors_on_load_but_rewritten_on_store() {
        let tmp = TempDir::new();
        let store = tmp.store();
        std::fs::write(store.path(), "not json {{{").unwrap();
        // Load now returns Err for corrupt file (new fail-closed semantics)
        assert!(store.load("a").is_err());
        // Delete is lenient and succeeds without touching the corrupt file
        store.delete("a").unwrap();
        assert_eq!(
            std::fs::read_to_string(store.path()).unwrap(),
            "not json {{{"
        );
        // Store overwrites the corrupt file with a fresh map
        store.store("a", "1").unwrap();
        assert_eq!(store.load("a").unwrap(), Some("1".to_string()));
        let map: BTreeMap<String, String> =
            serde_json::from_str(&std::fs::read_to_string(store.path()).unwrap()).unwrap();
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn env_override_takes_precedence() {
        let override_path = std::env::temp_dir().join("intent-core-secrets-env-override.json");
        std::env::set_var(SECRETS_FILE_ENV, &override_path);
        let resolved = default_secrets_path();
        std::env::remove_var(SECRETS_FILE_ENV);
        assert_eq!(resolved, override_path);

        let fallback = default_secrets_path();
        assert!(fallback.ends_with(Path::new("intent").join("secrets.json")));
    }

    #[cfg(unix)]
    #[test]
    fn unix_permissions_are_restrictive() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = TempDir::new();
        let store = FileSecretStore::with_path(tmp.0.join("nested").join("secrets.json"));
        store.store("a", "1").unwrap();
        let file_mode = std::fs::metadata(store.path())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(file_mode & 0o777, 0o600);
        let dir_mode = std::fs::metadata(tmp.0.join("nested"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(dir_mode & 0o777, 0o700);
    }
}
