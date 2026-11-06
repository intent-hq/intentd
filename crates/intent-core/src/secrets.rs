//! File-backed secret persistence (`~/intent/.secrets.json`).
//!
//! [`FileSecretStore`] is the single shared backend for all intentd secrets:
//! a flat JSON object mapping account name → secret string, stored by default
//! at `~/intent/.secrets.json` (resolved from `HOME`, falling back to
//! `USERPROFILE` on Windows) and overridable via the
//! [`INTENTD_SECRETS_FILE`](SECRETS_FILE_ENV) environment variable.
//!
//! **Legacy migration.** The default file used to be the non-dotted
//! `~/intent/secrets.json`. [`FileSecretStore::new`] migrates it once: when
//! the store targets the default path, `.secrets.json` does not exist, and
//! the legacy file does, the legacy file is renamed in place. When both
//! exist, `.secrets.json` wins and the legacy file is left untouched (a
//! warning is logged). Stores built via [`FileSecretStore::with_path`] or an
//! [`INTENTD_SECRETS_FILE`](SECRETS_FILE_ENV) override never migrate.
//!
//! Semantics mirror the `SecretStore` trait in `intent-services` (which this
//! leaf crate must not depend on): `load` returns `None` for unset **or
//! empty-string** values, `store` replaces, and `delete` of an absent account
//! is an idempotent success.
//!
//! **Durability & corrupt-file semantics.** Writes are atomic: the full map is
//! serialized to a temp file in the same directory and renamed over the
//! target. On unix the file is created `0600` and missing parent directories
//! `0700`; on Windows the file and any created parent directories get a
//! protected DACL granting access only to the current user, and the persisted
//! file is additionally marked `FILE_ATTRIBUTE_HIDDEN` (see [`write_private`]
//! and [`write_private_hidden`]). A missing file is
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

/// File name of the pre-dotfile default secrets file, migrated to
/// `.secrets.json` by [`FileSecretStore::new`] (see the module docs).
const LEGACY_SECRETS_FILE_NAME: &str = "secrets.json";

/// Resolve the secrets-file path: [`SECRETS_FILE_ENV`] when set and non-empty,
/// otherwise `~/intent/.secrets.json` (`HOME`, falling back to `USERPROFILE`).
pub(crate) fn default_secrets_path() -> PathBuf {
    if let Some(p) = std::env::var_os(SECRETS_FILE_ENV) {
        if !p.is_empty() {
            return PathBuf::from(p);
        }
    }
    home_dir().join("intent").join(".secrets.json")
}

/// Whether [`SECRETS_FILE_ENV`] is set and non-empty (i.e. the default path
/// is overridden and legacy migration must not run).
fn env_override_active() -> bool {
    std::env::var_os(SECRETS_FILE_ENV).is_some_and(|p| !p.is_empty())
}

/// One-time migration of the legacy default secrets file: when `path` (the
/// dotfile) does not exist but the sibling `secrets.json` does, rename the
/// legacy file onto `path`. When both exist, `path` wins and the legacy file
/// is left untouched (warning logged). Failures are non-fatal: a warning is
/// logged and the store proceeds against `path` as-is.
fn migrate_legacy_default(path: &Path) {
    let Some(parent) = path.parent() else {
        return;
    };
    let legacy = parent.join(LEGACY_SECRETS_FILE_NAME);
    if !legacy.exists() {
        return;
    }
    if path.exists() {
        tracing::warn!(
            legacy = %legacy.display(),
            path = %path.display(),
            "both legacy and dotfile secrets files exist; using the dotfile and leaving the legacy file untouched"
        );
        return;
    }
    match std::fs::rename(&legacy, path) {
        Ok(()) => {
            tracing::info!(from = %legacy.display(), to = %path.display(), "migrated legacy secrets file");
        }
        Err(e) => {
            tracing::warn!(
                from = %legacy.display(),
                to = %path.display(),
                error = %e,
                "failed to migrate legacy secrets file; proceeding without it"
            );
        }
    }
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
    /// Store backed by the default path ([`default_secrets_path`]), running
    /// the one-time legacy `secrets.json` → `.secrets.json` migration when
    /// the path is the true default (no env override; see the module docs).
    ///
    /// Under the test harness, a default that lands in the real home's Intent
    /// state is redirected to an empty per-process scratch file and never
    /// migrated (see [`crate::test_isolation::redirect_default_secrets`]); an
    /// explicit `INTENTD_SECRETS_FILE` into the real home aborts instead.
    #[must_use]
    pub fn new() -> Self {
        let path = default_secrets_path();
        if env_override_active() {
            // An explicit `INTENTD_SECRETS_FILE` into the real home is a
            // test's deliberate choice: fail loudly rather than redirect it.
            crate::test_isolation::assert_outside_forbidden_home(&path, "INTENTD_SECRETS_FILE");
        } else if let Some(scratch) = crate::test_isolation::redirect_default_secrets(&path) {
            return Self::with_path(scratch);
        }
        if !env_override_active() {
            migrate_legacy_default(&path);
        }
        Self::with_path(path)
    }

    /// Store backed by an explicit path (tests use this so they never touch
    /// the real `~/intent/.secrets.json`). Never migrates the legacy file.
    ///
    /// Under the test harness, aborts when `path` lands in the real home's
    /// Intent state (see [`crate::test_isolation`]).
    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        crate::test_isolation::assert_outside_forbidden_home(&path, "FileSecretStore::with_path");
        Self { path }
    }

    /// The backing file path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Return the stored secret for `account`: `Ok(Some(value))` if present,
    /// `Ok(None)` if confirmed absent (missing file or key not in map), or
    /// `Err` if the backing file is unreadable or corrupt (IO/parse failure).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if the backing file is unreadable or corrupt.
    pub fn load(&self, account: &str) -> Result<Option<String>> {
        self.read_map_strict().map(|mut map| map.remove(account))
    }

    /// Read selected accounts from one strict file snapshot, in input order.
    /// Repeated names return the same value; empty values follow `load` semantics.
    ///
    /// # Errors
    /// Returns the same IO or parse error as [`Self::load`].
    pub fn load_many(&self, accounts: &[&str]) -> Result<Vec<Option<String>>> {
        let map = self.read_map_strict()?;
        Ok(accounts
            .iter()
            .map(|account| map.get(*account).cloned())
            .collect())
    }

    /// Persist `value` for `account`, replacing any existing secret.
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if creating the secrets directory, serializing, or writing the file fails.
    pub fn store(&self, account: &str, value: &str) -> Result<()> {
        let mut map = self.read_map();
        map.insert(account.to_string(), value.to_string());
        self.persist(&map)
    }

    /// Delete the secret for `account`; absence is an idempotent success (and
    /// does not rewrite the file).
    ///
    /// # Errors
    ///
    /// Returns `Error::Internal` if rewriting the secrets file fails.
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
    /// Windows: owner-only DACLs, and the file is hidden (the attribute is
    /// set on the temp file so the rename lands it on the final path).
    fn persist(&self, map: &BTreeMap<String, String>) -> Result<()> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        create_dir_private(parent)
            .map_err(|e| Error::Internal(format!("failed to create secrets dir: {e}")))?;

        let json = serde_json::to_string_pretty(map)
            .map_err(|e| Error::Internal(format!("failed to serialize secrets: {e}")))?;

        let tmp = parent.join(format!(
            ".secrets.tmp-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        write_private_hidden(&tmp, json.as_bytes()).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            Error::Internal(format!("failed to write secrets file: {e}"))
        })?;
        std::fs::rename(&tmp, &self.path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            Error::Internal(format!("failed to persist secrets file: {e}"))
        })
    }
}

/// `create_dir_all` that creates any missing directories owner-only: mode
/// `0700` on unix; on Windows each newly created directory gets a protected
/// DACL whose single (inheritable) ACE grants access to the current user only
/// (plain `create_dir_all` on other platforms). Existing directories are left
/// as-is.
///
/// # Errors
///
/// Returns the underlying IO error if directory creation (or, on Windows,
/// applying the DACL) fails.
pub fn create_dir_private(dir: &Path) -> std::io::Result<()> {
    imp::create_dir_private(dir)
}

/// Write `contents` to a fresh file (`create_new`) with owner-only
/// permissions, so the contents never exist on disk with looser access: mode
/// `0600` on unix; on Windows the file is created empty, its DACL is replaced
/// with a protected one granting access to the current user only, and only
/// then are the bytes written (plain write on other platforms).
///
/// # Errors
///
/// Returns the underlying IO error if the file already exists, or if
/// creating, restricting, or writing it fails.
pub fn write_private(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    imp::write_private(path, contents, false)
}

/// [`write_private`], plus `FILE_ATTRIBUTE_HIDDEN` on Windows (a leading dot
/// does not hide files there); identical to [`write_private`] elsewhere. The
/// secrets file uses this; call sites whose output must stay visible (e.g. an
/// exported image) use [`write_private`] instead.
///
/// # Errors
///
/// Same as [`write_private`], plus failures setting the hidden attribute.
pub fn write_private_hidden(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    imp::write_private(path, contents, true)
}

#[cfg(unix)]
mod imp {
    use std::path::Path;

    pub(super) fn create_dir_private(dir: &Path) -> std::io::Result<()> {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(dir)
    }

    pub(super) fn write_private(
        path: &Path,
        contents: &[u8],
        _hidden: bool,
    ) -> std::io::Result<()> {
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
}

/// Windows: every file/dir created here gets a **protected DACL** (no
/// inherited ACEs, `PROTECTED_DACL_SECURITY_INFORMATION`) containing a single
/// access-allowed ACE for the current process token's user SID — no
/// `Everyone`/`Users`/`Authenticated Users`, and no explicit
/// SYSTEM/Administrators ACEs either (administrators can still take ownership;
/// that is inherent to Windows and not preventable via the DACL).
#[cfg(windows)]
mod imp {
    use std::io;
    use std::path::Path;

    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_SUCCESS, HANDLE};
    use windows_sys::Win32::Security::Authorization::{SetNamedSecurityInfoW, SE_FILE_OBJECT};
    use windows_sys::Win32::Security::{
        AddAccessAllowedAceEx, GetLengthSid, GetTokenInformation, InitializeAcl, TokenUser,
        ACCESS_ALLOWED_ACE, ACL, ACL_REVISION, CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION,
        OBJECT_INHERIT_ACE, PROTECTED_DACL_SECURITY_INFORMATION, PSID, TOKEN_QUERY, TOKEN_USER,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileAttributesW, SetFileAttributesW, FILE_ALL_ACCESS, FILE_ATTRIBUTE_HIDDEN,
        INVALID_FILE_ATTRIBUTES,
    };
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    pub(super) fn to_wide(path: &Path) -> Vec<u16> {
        use std::os::windows::ffi::OsStrExt;
        path.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    /// Raw process-token buffer whose leading `TOKEN_USER` holds the current
    /// user's SID. The SID pointer points into the buffer, so callers must
    /// keep the buffer alive while using [`user_sid`]'s result. Backed by
    /// `u64`s so it is aligned for `TOKEN_USER`.
    pub(super) fn current_user_token_buf() -> io::Result<Vec<u64>> {
        unsafe {
            let mut token: HANDLE = std::ptr::null_mut();
            if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &raw mut token) == 0 {
                return Err(io::Error::last_os_error());
            }
            let mut len = 0u32;
            GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &raw mut len);
            if len == 0 {
                let e = io::Error::last_os_error();
                CloseHandle(token);
                return Err(e);
            }
            let mut buf = vec![0u64; (len as usize).div_ceil(8)];
            let ok =
                GetTokenInformation(token, TokenUser, buf.as_mut_ptr().cast(), len, &raw mut len);
            CloseHandle(token);
            if ok == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(buf)
        }
    }

    /// The user SID inside a [`current_user_token_buf`] buffer.
    pub(super) fn user_sid(token_buf: &[u64]) -> PSID {
        unsafe { (*token_buf.as_ptr().cast::<TOKEN_USER>()).User.Sid }
    }

    /// Replace `path`'s DACL with a protected (non-inheriting) DACL whose only
    /// ACE grants the current user `FILE_ALL_ACCESS`. For directories the ACE
    /// is inheritable so children created without an explicit DACL of their
    /// own stay owner-only too.
    fn restrict_to_owner(path: &Path, is_dir: bool) -> io::Result<()> {
        let token_buf = current_user_token_buf()?;
        let sid = user_sid(&token_buf);
        let sid_len = unsafe { GetLengthSid(sid) } as usize;
        // ACL header + one ACCESS_ALLOWED_ACE (whose trailing u32 is the first
        // u32 of the SID) + the rest of the SID, rounded up to u32 alignment.
        let acl_len = (std::mem::size_of::<ACL>() + std::mem::size_of::<ACCESS_ALLOWED_ACE>()
            - std::mem::size_of::<u32>()
            + sid_len
            + 3)
            & !3;
        let acl_len_u32 = u32::try_from(acl_len).map_err(io::Error::other)?;
        let mut acl_buf = vec![0u32; acl_len.div_ceil(4)];
        let acl = acl_buf.as_mut_ptr().cast::<ACL>();
        let wide = to_wide(path);
        unsafe {
            if InitializeAcl(acl, acl_len_u32, ACL_REVISION) == 0 {
                return Err(io::Error::last_os_error());
            }
            let inherit_flags = if is_dir {
                OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE
            } else {
                0
            };
            if AddAccessAllowedAceEx(acl, ACL_REVISION, inherit_flags, FILE_ALL_ACCESS, sid) == 0 {
                return Err(io::Error::last_os_error());
            }
            let status = SetNamedSecurityInfoW(
                wide.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                acl,
                std::ptr::null_mut(),
            );
            if status != ERROR_SUCCESS {
                return Err(io::Error::from_raw_os_error(status.cast_signed()));
            }
        }
        Ok(())
    }

    fn set_hidden(path: &Path) -> io::Result<()> {
        let wide = to_wide(path);
        unsafe {
            let attrs = GetFileAttributesW(wide.as_ptr());
            if attrs == INVALID_FILE_ATTRIBUTES {
                return Err(io::Error::last_os_error());
            }
            if SetFileAttributesW(wide.as_ptr(), attrs | FILE_ATTRIBUTE_HIDDEN) == 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }

    /// Restrict a directory this call just created; on failure the (empty)
    /// directory is removed again so a retry cannot find it via the
    /// `AlreadyExists` branch and skip the DACL hardening.
    fn restrict_created_dir(dir: &Path) -> io::Result<()> {
        restrict_to_owner(dir, true).inspect_err(|_| {
            let _ = std::fs::remove_dir(dir);
        })
    }

    pub(super) fn create_dir_private(dir: &Path) -> io::Result<()> {
        // `create_dir_all` treats the empty path (e.g. the parent of a bare
        // relative file name) as success; mirror that, since `create_dir("")`
        // fails.
        if dir.as_os_str().is_empty() {
            return Ok(());
        }
        match std::fs::create_dir(dir) {
            Ok(()) => restrict_created_dir(dir),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists && dir.is_dir() => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let Some(parent) = dir.parent() else {
                    return Err(e);
                };
                create_dir_private(parent)?;
                match std::fs::create_dir(dir) {
                    Ok(()) => restrict_created_dir(dir),
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists && dir.is_dir() => Ok(()),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    pub(super) fn write_private(path: &Path, contents: &[u8], hidden: bool) -> io::Result<()> {
        use std::io::Write;
        // Create the file empty, clamp its DACL, and only then write the
        // bytes, so the contents never exist on disk under the broader
        // (inherited) DACL the file is born with.
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        restrict_to_owner(path, false)?;
        if hidden {
            set_hidden(path)?;
        }
        f.write_all(contents)?;
        f.sync_all()
    }
}

#[cfg(not(any(unix, windows)))]
mod imp {
    use std::path::Path;

    pub(super) fn create_dir_private(dir: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dir)
    }

    pub(super) fn write_private(
        path: &Path,
        contents: &[u8],
        _hidden: bool,
    ) -> std::io::Result<()> {
        std::fs::write(path, contents)
    }
}

/// Process-env lock shared by intent-core tests that mutate `HOME` or the
/// secrets/forbidden-home variables (plain `cargo test` runs them on threads).
#[cfg(test)]
pub(crate) fn tests_env_lock() -> std::sync::MutexGuard<'static, ()> {
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    ENV_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Unique temp dir under the system temp dir, removed on drop; keeps every
    /// test away from the real `~/intent/.secrets.json`.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let dir =
                std::env::temp_dir().join(format!("intent-core-secrets-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn store(&self) -> FileSecretStore {
            FileSecretStore::with_path(self.0.join(".secrets.json"))
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn selected_secrets_preserve_order_duplicates_and_missing_values() {
        let tmp = TempDir::new();
        let store = tmp.store();
        assert_eq!(store.load_many(&["a", "b"]).unwrap(), vec![None, None]);
        std::fs::write(
            store.path(),
            r#"{"a":"first","b":"second","empty":"","unselected":"private"}"#,
        )
        .unwrap();
        assert_eq!(
            store
                .load_many(&["b", "a", "b", "missing", "empty"])
                .unwrap(),
            vec![
                Some("second".into()),
                Some("first".into()),
                Some("second".into()),
                None,
                None
            ]
        );
        assert!(store.load_many(&[]).unwrap().is_empty());
    }

    #[test]
    fn selected_secrets_reject_corrupt_snapshot_without_rewriting_it() {
        let tmp = TempDir::new();
        let store = tmp.store();
        std::fs::write(store.path(), "not a map").unwrap();
        assert!(store.load_many(&["a", "b"]).is_err());
        assert_eq!(std::fs::read_to_string(store.path()).unwrap(), "not a map");
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
        let _env = EnvScope::new();
        let override_path = std::env::temp_dir().join("intent-core-secrets-env-override.json");
        std::env::set_var(SECRETS_FILE_ENV, &override_path);
        let resolved = default_secrets_path();
        std::env::remove_var(SECRETS_FILE_ENV);
        assert_eq!(resolved, override_path);

        let fallback = default_secrets_path();
        assert!(fallback.ends_with(Path::new("intent").join(".secrets.json")));
    }

    #[test]
    fn migrate_renames_legacy_default_file() {
        let tmp = TempDir::new();
        let legacy = tmp.0.join(LEGACY_SECRETS_FILE_NAME);
        let dotfile = tmp.0.join(".secrets.json");
        std::fs::write(&legacy, r#"{"a":"legacy"}"#).unwrap();
        migrate_legacy_default(&dotfile);
        assert!(!legacy.exists(), "legacy file must be renamed away");
        let store = FileSecretStore::with_path(&dotfile);
        assert_eq!(store.load("a").unwrap(), Some("legacy".to_string()));
    }

    #[test]
    fn migrate_is_noop_when_no_legacy_file() {
        let tmp = TempDir::new();
        let dotfile = tmp.0.join(".secrets.json");
        migrate_legacy_default(&dotfile);
        assert!(!dotfile.exists(), "migration must not create the dotfile");
    }

    #[test]
    fn migrate_prefers_dotfile_when_both_exist() {
        let tmp = TempDir::new();
        let legacy = tmp.0.join(LEGACY_SECRETS_FILE_NAME);
        let dotfile = tmp.0.join(".secrets.json");
        std::fs::write(&legacy, r#"{"a":"legacy"}"#).unwrap();
        std::fs::write(&dotfile, r#"{"a":"dotfile"}"#).unwrap();
        migrate_legacy_default(&dotfile);
        assert!(legacy.exists(), "legacy file must be left untouched");
        assert_eq!(
            std::fs::read_to_string(&legacy).unwrap(),
            r#"{"a":"legacy"}"#
        );
        let store = FileSecretStore::with_path(&dotfile);
        assert_eq!(store.load("a").unwrap(), Some("dotfile".to_string()));
    }

    #[test]
    fn with_path_never_migrates() {
        let tmp = TempDir::new();
        let legacy = tmp.0.join(LEGACY_SECRETS_FILE_NAME);
        std::fs::write(&legacy, r#"{"a":"legacy"}"#).unwrap();
        let store = FileSecretStore::with_path(tmp.0.join(".secrets.json"));
        assert_eq!(store.load("a").unwrap(), None);
        assert!(legacy.exists(), "explicit-path stores must not migrate");
    }

    /// Environment variable names the home-isolation tests drive. Spelled as
    /// literals so the tests stay independent of the guard's implementation.
    const FORBIDDEN_HOME: &str = "INTENTD_TEST_FORBIDDEN_HOME";

    /// Serializes the tests that mutate process env (`HOME`, the secrets
    /// override, the forbidden-home marker) and restores every touched
    /// variable on drop, so plain `cargo test` threads cannot observe them.
    struct EnvScope {
        saved: Vec<(&'static str, Option<std::ffi::OsString>)>,
        touched: usize,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvScope {
        fn new() -> Self {
            let lock = super::tests_env_lock();
            let saved = ["HOME", "USERPROFILE", SECRETS_FILE_ENV, FORBIDDEN_HOME]
                .into_iter()
                .map(|k| (k, std::env::var_os(k)))
                .collect();
            Self {
                saved,
                touched: 0,
                _lock: lock,
            }
        }

        /// Set `key` for the lifetime of this scope (restored on drop).
        fn set(&mut self, key: &str, value: impl AsRef<std::ffi::OsStr>) {
            self.touched += 1;
            std::env::set_var(key, value);
        }

        /// Remove `key` for the lifetime of this scope (restored on drop).
        fn remove(&mut self, key: &str) {
            self.touched += 1;
            std::env::remove_var(key);
        }
    }

    impl Drop for EnvScope {
        fn drop(&mut self) {
            for (key, value) in &self.saved {
                match value {
                    Some(v) => std::env::set_var(key, v),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    /// Recursive `(relative path, bytes, mtime)` listing of `root`, used to
    /// prove a sentinel home is byte-for-byte untouched.
    fn snapshot(root: &Path) -> Vec<(PathBuf, Vec<u8>, std::time::SystemTime)> {
        fn walk(root: &Path, dir: &Path, out: &mut Vec<(PathBuf, Vec<u8>, std::time::SystemTime)>) {
            let mut entries: Vec<_> = std::fs::read_dir(dir)
                .unwrap()
                .map(|e| e.unwrap().path())
                .collect();
            entries.sort();
            for path in entries {
                let meta = std::fs::metadata(&path).unwrap();
                let rel = path.strip_prefix(root).unwrap().to_path_buf();
                if meta.is_dir() {
                    out.push((rel, Vec::new(), meta.modified().unwrap()));
                    walk(root, &path, out);
                } else {
                    out.push((rel, std::fs::read(&path).unwrap(), meta.modified().unwrap()));
                }
            }
        }
        let mut out = Vec::new();
        walk(root, root, &mut out);
        out
    }

    /// A sentinel "real home" holding a legacy `intent/secrets.json` that
    /// `FileSecretStore::new` would otherwise migrate.
    fn sentinel_home() -> TempDir {
        let home = TempDir::new();
        std::fs::create_dir_all(home.0.join("intent")).unwrap();
        std::fs::write(
            home.0.join("intent").join(LEGACY_SECRETS_FILE_NAME),
            r#"{"real":"user-secret"}"#,
        )
        .unwrap();
        home
    }

    /// Child-process marker and sentinel for the abort tests below.
    const CHILD_ENV: &str = "INTENT_CORE_HOME_ISOLATION_CHILD";
    const SENTINEL_ENV: &str = "INTENT_CORE_HOME_ISOLATION_SENTINEL";

    /// Re-run this test binary on exactly `test` with the guard pointed at
    /// `home` (also `HOME`), and return its output. The violation aborts the
    /// child, so the parent observes it without dying itself.
    fn run_child(test: &str, mode: &str, home: &Path) -> std::process::Output {
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([test, "--exact", "--nocapture", "--test-threads=1"])
            .env(CHILD_ENV, mode)
            .env(SENTINEL_ENV, home)
            .env("HOME", home)
            .env("USERPROFILE", home)
            .env(FORBIDDEN_HOME, home)
            .env_remove(SECRETS_FILE_ENV)
            .output()
            .unwrap()
    }

    /// Child body (a no-op in normal runs): touch the forbidden home through
    /// the requested constructor. Reaching the end means the guard failed.
    #[test]
    fn home_isolation_child() {
        let Some(mode) = std::env::var_os(CHILD_ENV) else {
            return;
        };
        let home = PathBuf::from(std::env::var_os(SENTINEL_ENV).unwrap());
        let target = home.join("intent").join(".secrets.json");
        let store = if mode == "override" {
            std::env::set_var(SECRETS_FILE_ENV, &target);
            FileSecretStore::new()
        } else {
            assert_eq!(mode, "explicit");
            FileSecretStore::with_path(target)
        };
        store.store("test-only", "leak").unwrap();
        println!("home-isolation-child: reached the forbidden home");
    }

    fn assert_child_aborted(mode: &str) {
        let home = sentinel_home();
        let before = snapshot(&home.0);
        let out = run_child("secrets::tests::home_isolation_child", mode, &home.0);
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            !out.status.success() && stderr.contains("hermetic-tests:"),
            "the {mode} constructor must abort inside the forbidden home\n{stdout}\n{stderr}"
        );
        assert!(!stdout.contains("reached the forbidden home"));
        assert_eq!(
            snapshot(&home.0),
            before,
            "sentinel home must stay untouched"
        );
    }

    /// The default constructor resolves to an empty scratch store instead of
    /// the forbidden home: no read of the real secrets, no legacy migration,
    /// and writes land outside the home.
    #[test]
    fn home_isolation_default_store_redirects_away_from_forbidden_home() {
        let mut env = EnvScope::new();
        let home = sentinel_home();
        let before = snapshot(&home.0);
        env.set("HOME", &home.0);
        env.set("USERPROFILE", &home.0);
        env.remove(SECRETS_FILE_ENV);
        env.set(FORBIDDEN_HOME, &home.0);

        // A stale file at the old PID-only scratch location must not leak in.
        let stale = std::env::temp_dir().join(format!(
            "intentd-test-default-secrets-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&stale).unwrap();
        std::fs::write(stale.join(".secrets.json"), r#"{"test-only":"stale"}"#).unwrap();

        let store = FileSecretStore::new();
        let _ = std::fs::remove_dir_all(&stale);
        assert_eq!(
            store.load("test-only").unwrap(),
            None,
            "the scratch store must start empty, never reopen a stale PID-keyed file"
        );
        assert!(
            !store.path().starts_with(&home.0),
            "default store must be redirected, got {}",
            store.path().display()
        );
        assert_eq!(
            store.load("real").unwrap(),
            None,
            "the real secrets must not be readable through the default store"
        );
        store.store("test-only", "redirected").unwrap();
        assert_eq!(
            FileSecretStore::new().load("test-only").unwrap(),
            Some("redirected".to_string()),
            "every default store in the process shares the scratch file"
        );
        let _ = std::fs::remove_dir_all(store.path().parent().unwrap());

        assert_eq!(
            snapshot(&home.0),
            before,
            "sentinel home must stay untouched"
        );
    }

    #[test]
    fn home_isolation_explicit_path_refuses_forbidden_home() {
        assert_child_aborted("explicit");
    }

    /// An explicit `INTENTD_SECRETS_FILE` into the forbidden home is a test's
    /// deliberate choice, so it aborts instead of being redirected.
    #[test]
    fn home_isolation_env_override_into_forbidden_home_aborts() {
        assert_child_aborted("override");
    }

    #[test]
    fn home_isolation_allows_redirected_home_and_override() {
        let mut env = EnvScope::new();
        let forbidden = sentinel_home();
        let before = snapshot(&forbidden.0);
        env.set(FORBIDDEN_HOME, &forbidden.0);
        env.remove(SECRETS_FILE_ENV);

        let other = sentinel_home();
        env.set("HOME", &other.0);
        env.set("USERPROFILE", &other.0);
        let store = FileSecretStore::new();
        assert_eq!(store.path(), other.0.join("intent").join(".secrets.json"));
        assert_eq!(
            store.load("real").unwrap(),
            Some("user-secret".to_string()),
            "a tempdir HOME keeps the default resolution and legacy migration"
        );

        env.set("HOME", &forbidden.0);
        env.set("USERPROFILE", &forbidden.0);
        let override_path = other.0.join("override.json");
        env.set(SECRETS_FILE_ENV, &override_path);
        let store = FileSecretStore::new();
        assert_eq!(store.path(), override_path);
        store.store("a", "1").unwrap();

        assert_eq!(
            snapshot(&forbidden.0),
            before,
            "forbidden home must stay untouched"
        );
    }

    /// Every nextest test process has the guard active with no harness
    /// configuration: the forbidden home is the user-database home, not a
    /// possibly redirected `$HOME`.
    #[cfg(unix)]
    #[test]
    fn home_isolation_active_under_nextest_without_configuration() {
        if std::env::var_os("NEXTEST_RUN_ID").is_none() {
            return;
        }
        let mut env = EnvScope::new();
        env.remove(FORBIDDEN_HOME);
        let redirected = TempDir::new();
        env.set("HOME", &redirected.0);
        let real = crate::path_utils::user_db_home().expect("user-database home");
        let forbidden =
            crate::test_isolation::forbidden_intent_dir().expect("guard active under nextest");
        assert_eq!(
            forbidden,
            std::fs::canonicalize(&real)
                .unwrap_or(real.clone())
                .join("intent")
        );
        assert!(crate::test_isolation::is_in_forbidden_home(
            &real.join("intent").join(".secrets.json")
        ));
        assert!(!crate::test_isolation::is_in_forbidden_home(
            &redirected.0.join("intent").join(".secrets.json")
        ));
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

    /// Mirror of `unix_permissions_are_restrictive`: the persisted file and
    /// any created parent dir carry a DACL whose every ACE grants the current
    /// user (and no one else), and the file is hidden.
    #[cfg(windows)]
    #[test]
    fn windows_acls_are_owner_restricted() {
        use windows_sys::Win32::Foundation::{LocalFree, ERROR_SUCCESS};
        use windows_sys::Win32::Security::Authorization::{GetNamedSecurityInfoW, SE_FILE_OBJECT};
        use windows_sys::Win32::Security::{
            EqualSid, GetAce, ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, DACL_SECURITY_INFORMATION, PSID,
        };
        use windows_sys::Win32::Storage::FileSystem::{
            GetFileAttributesW, FILE_ATTRIBUTE_HIDDEN, INVALID_FILE_ATTRIBUTES,
        };
        use windows_sys::Win32::System::SystemServices::ACCESS_ALLOWED_ACE_TYPE;

        fn assert_owner_only_dacl(path: &Path) {
            let token_buf = imp::current_user_token_buf().unwrap();
            let me = imp::user_sid(&token_buf);
            let wide = imp::to_wide(path);
            let mut dacl: *mut ACL = std::ptr::null_mut();
            let mut sd: *mut std::ffi::c_void = std::ptr::null_mut();
            unsafe {
                let status = GetNamedSecurityInfoW(
                    wide.as_ptr(),
                    SE_FILE_OBJECT,
                    DACL_SECURITY_INFORMATION,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    &raw mut dacl,
                    std::ptr::null_mut(),
                    &raw mut sd,
                );
                assert_eq!(status, ERROR_SUCCESS, "GetNamedSecurityInfoW failed");
                assert!(!dacl.is_null(), "NULL DACL grants everyone full access");
                let ace_count = (*dacl).AceCount;
                assert!(ace_count >= 1, "empty DACL would deny the owner too");
                for i in 0..ace_count {
                    let mut ace: *mut std::ffi::c_void = std::ptr::null_mut();
                    assert_ne!(GetAce(dacl, u32::from(i), &raw mut ace), 0);
                    let header = ace.cast::<ACE_HEADER>();
                    assert_eq!(
                        u32::from((*header).AceType),
                        ACCESS_ALLOWED_ACE_TYPE,
                        "unexpected ACE type in {}",
                        path.display()
                    );
                    let allowed = ace.cast::<ACCESS_ALLOWED_ACE>();
                    let sid = std::ptr::addr_of!((*allowed).SidStart) as PSID;
                    assert_ne!(
                        EqualSid(sid, me),
                        0,
                        "{} has an ACE for a SID other than the current user",
                        path.display()
                    );
                }
                LocalFree(sd);
            }
        }

        let tmp = TempDir::new();
        let store = FileSecretStore::with_path(tmp.0.join("nested").join("secrets.json"));
        store.store("a", "1").unwrap();
        assert_owner_only_dacl(store.path());
        assert_owner_only_dacl(&tmp.0.join("nested"));
        let attrs = unsafe { GetFileAttributesW(imp::to_wide(store.path()).as_ptr()) };
        assert_ne!(attrs, INVALID_FILE_ATTRIBUTES);
        assert_ne!(
            attrs & FILE_ATTRIBUTE_HIDDEN,
            0,
            "secrets file must carry FILE_ATTRIBUTE_HIDDEN"
        );
        // Overwrite path: the atomic rename must land over the now-hidden,
        // owner-locked destination.
        store.store("a", "2").unwrap();
        assert_eq!(store.load("a").unwrap(), Some("2".to_string()));
    }

    /// A bare relative backing path (e.g. `secrets.json`) has parent `""`;
    /// creating that must succeed, mirroring `create_dir_all`.
    #[cfg(windows)]
    #[test]
    fn windows_empty_dir_path_is_ok() {
        imp::create_dir_private(Path::new("")).unwrap();
    }
}
