//! Tenant secrets for the orchestrator: every tenant's `SecretStore` is its
//! Postgres rows ([`TenantSecretStore`]), encrypted with one keyring the
//! process loads at start. The orchestrator never reads a `secrets.json`;
//! the desktop daemon keeps `intent_core::FileSecretStore` and none of this.
//!
//! The keyring comes from the environment, one of two ways:
//! - [`KEYS_FILE_ENV`] names a key file (`key_id=<base64 of 32 bytes>` per
//!   line, see [`SecretKeyring::parse`]) and [`ACTIVE_KEY_ENV`] the id of the
//!   key that seals new writes; the others only open older rows. On Unix
//!   the file must not be readable by group or others.
//! - [`KEY_ENV`] holds one base64 key and [`KEY_ID_ENV`] its id. `main`
//!   clears [`KEY_ENV`] before any thread starts, so no child inherits it.
//!
//! Neither, or a mix of both, stops the orchestrator before it opens a
//! tenant: without a key no tenant secret can be read, and every such read
//! must fail closed rather than look unset.
//!
//! **Who can read the key.** Clearing [`KEY_ENV`] keeps it out of children,
//! but the process's initial environment stays in `/proc/<pid>/environ`,
//! and its memory holds the keyring. On Linux, `main` first makes the
//! process non-dumpable ([`harden_process`]): `/proc/<pid>/environ`, `mem`
//! and ptrace then need root, so tenant workloads running as the same user
//! cannot read them. They can still read a key file owned by that user, so
//! with [`KEYS_FILE_ENV`] run tenant workloads under another user (or keep
//! the file where they cannot reach it).

use std::path::Path;
use std::sync::Arc;

use base64::Engine as _;
use intent_core::Result;
use intent_store::postgres::{SecretKeyring, TenantSecretStore};
use zeroize::Zeroizing;

/// A key file of `key_id=<base64 key>` lines.
pub const KEYS_FILE_ENV: &str = "INTENTD_SECRETS_KEYS_FILE";
/// The id, in [`KEYS_FILE_ENV`], of the key that seals new writes.
pub const ACTIVE_KEY_ENV: &str = "INTENTD_SECRETS_ACTIVE_KEY";
/// One base64 key, instead of a key file.
pub const KEY_ENV: &str = "INTENTD_SECRETS_KEY";
/// The id of the key in [`KEY_ENV`].
pub const KEY_ID_ENV: &str = "INTENTD_SECRETS_KEY_ID";

/// Make this process non-dumpable (Linux): its `/proc/<pid>/environ`,
/// `mem` and ptrace then need root, so a same-user process (a tenant's
/// agent) cannot read the keyring or the environment it came from. A child
/// becomes dumpable again when it execs. Elsewhere a no-op.
///
/// # Errors
/// When the kernel refuses the change.
pub fn harden_process() -> std::result::Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        nix::sys::prctl::set_dumpable(false)
            .map_err(|e| format!("prctl(PR_SET_DUMPABLE, 0): {e}"))?;
    }
    Ok(())
}

/// The process keyring, from the environment as `get` reads it (see the
/// module docs). Errors name variables and paths, never a key.
///
/// # Errors
/// When no key source is set, both are, one is incomplete, the key file
/// cannot be read or is readable by others, or a key is malformed.
pub fn keyring_from_env(
    get: impl Fn(&str) -> Option<String>,
) -> std::result::Result<SecretKeyring, String> {
    let set = |name: &str| get(name).filter(|v| !v.trim().is_empty());
    let file = set(KEYS_FILE_ENV);
    let active = set(ACTIVE_KEY_ENV);
    let key = set(KEY_ENV).map(Zeroizing::new);
    let key_id = set(KEY_ID_ENV);
    if (file.is_some() || active.is_some()) && (key.is_some() || key_id.is_some()) {
        return Err(format!(
            "set either {KEYS_FILE_ENV} and {ACTIVE_KEY_ENV}, or {KEY_ENV} and {KEY_ID_ENV}, not both"
        ));
    }
    match (file, active, key, key_id) {
        (Some(file), Some(active), None, None) => {
            keyring_from_file(Path::new(&file), active.trim())
        }
        (None, None, Some(key), Some(key_id)) => {
            let bytes = Zeroizing::new(
                base64::engine::general_purpose::STANDARD
                    .decode(key.trim())
                    .map_err(|_| format!("{KEY_ENV} is not valid base64"))?,
            );
            SecretKeyring::new(key_id.trim(), &bytes).map_err(|e| format!("{KEY_ENV}: {e}"))
        }
        (Some(_), None, ..) => Err(format!(
            "{KEYS_FILE_ENV} is set but {ACTIVE_KEY_ENV} is not"
        )),
        (None, Some(_), ..) => Err(format!(
            "{ACTIVE_KEY_ENV} is set but {KEYS_FILE_ENV} is not"
        )),
        (.., Some(_), None) => Err(format!("{KEY_ENV} is set but {KEY_ID_ENV} is not")),
        (.., None, Some(_)) => Err(format!("{KEY_ID_ENV} is set but {KEY_ENV} is not")),
        _ => Err(format!(
            "tenant secrets need a key: set {KEYS_FILE_ENV} and {ACTIVE_KEY_ENV}, \
             or {KEY_ENV} and {KEY_ID_ENV}"
        )),
    }
}

fn keyring_from_file(path: &Path, active: &str) -> std::result::Result<SecretKeyring, String> {
    let shown = path.display();
    let metadata = std::fs::metadata(path).map_err(|e| format!("{KEYS_FILE_ENV} {shown}: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = metadata.permissions().mode();
        if mode & 0o077 != 0 {
            return Err(format!(
                "{KEYS_FILE_ENV} {shown} is readable by group or others (mode {:o}); chmod 600 it",
                mode & 0o777
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = metadata;
    let text = Zeroizing::new(
        std::fs::read_to_string(path).map_err(|e| format!("{KEYS_FILE_ENV} {shown}: {e}"))?,
    );
    SecretKeyring::parse(&text, active).map_err(|e| format!("{KEYS_FILE_ENV} {shown}: {e}"))
}

/// A tenant's [`TenantSecretStore`] as the synchronous `SecretStore` the
/// services take. `intent-services` runs each call on the blocking pool.
#[derive(Debug, Clone)]
pub struct TenantSecrets(TenantSecretStore);

impl TenantSecrets {
    /// The secrets of `store`'s tenant, sealed with `keyring`.
    ///
    /// # Errors
    /// As [`TenantSecretStore::new`]: not a Postgres store, or not on the
    /// multi-thread runtime.
    pub fn new(store: &intent_store::Store, keyring: Arc<SecretKeyring>) -> Result<Self> {
        TenantSecretStore::new(store, keyring).map(Self)
    }
}

impl intent_services::SecretStore for TenantSecrets {
    fn load(&self, account: &str) -> Result<Option<String>> {
        self.0.load_blocking(account)
    }

    fn store(&self, account: &str, value: &str) -> Result<()> {
        self.0.store_blocking(account, value)
    }

    fn delete(&self, account: &str) -> Result<()> {
        self.0.delete_blocking(account)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    const KEY: [u8; 32] = [0x42; 32];

    fn b64(key: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(key)
    }

    fn env(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[cfg(unix)]
    fn key_file(dir: &Path, text: &str, mode: u32) -> String {
        use std::os::unix::fs::PermissionsExt as _;
        let path = dir.join("keys");
        std::fs::write(&path, text).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn hardening_makes_the_process_non_dumpable() {
        harden_process().unwrap();
        assert!(!nix::sys::prctl::get_dumpable().unwrap());
    }

    #[test]
    fn a_single_key_from_the_environment() {
        let key = b64(&KEY);
        let ring = keyring_from_env(env(&[(KEY_ENV, &key), (KEY_ID_ENV, "k1")])).unwrap();
        assert_eq!(ring.active_key_id(), "k1");
    }

    #[test]
    fn no_key_or_an_incomplete_or_mixed_source_is_refused_without_quoting_keys() {
        let key = b64(&KEY);
        let short = b64(&[1; 16]);
        let cases: Vec<(Vec<(&str, &str)>, &str)> = vec![
            (vec![], "tenant secrets need a key"),
            (vec![(KEY_ENV, "  ")], "tenant secrets need a key"),
            (vec![(KEY_ENV, &key)], "KEY_ID is not"),
            (vec![(KEY_ID_ENV, "k1")], "INTENTD_SECRETS_KEY is not"),
            (vec![(KEYS_FILE_ENV, "/x")], "ACTIVE_KEY is not"),
            (vec![(ACTIVE_KEY_ENV, "k1")], "KEYS_FILE is not"),
            (
                vec![
                    (KEYS_FILE_ENV, "/x"),
                    (ACTIVE_KEY_ENV, "k1"),
                    (KEY_ENV, &key),
                ],
                "not both",
            ),
            (
                vec![(KEY_ENV, "!!"), (KEY_ID_ENV, "k1")],
                "not valid base64",
            ),
            (vec![(KEY_ENV, &short), (KEY_ID_ENV, "k1")], "16 bytes"),
            (
                vec![(KEYS_FILE_ENV, "/no/such/keys"), (ACTIVE_KEY_ENV, "k1")],
                "/no/such/keys",
            ),
        ];
        for (vars, expected) in cases {
            let err = keyring_from_env(env(&vars)).unwrap_err();
            assert!(err.contains(expected), "{vars:?}: {err}");
            assert!(!err.contains(&key[..12]), "{err}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_private_key_file_loads_and_a_shared_one_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let text = format!("old={}\nnew={}\n", b64(&[1; 32]), b64(&KEY));
        let path = key_file(dir.path(), &text, 0o600);
        let ring =
            keyring_from_env(env(&[(KEYS_FILE_ENV, &path), (ACTIVE_KEY_ENV, "new")])).unwrap();
        assert_eq!(ring.active_key_id(), "new");

        let shared = key_file(dir.path(), &text, 0o640);
        let err = keyring_from_env(env(&[(KEYS_FILE_ENV, &shared), (ACTIVE_KEY_ENV, "new")]))
            .unwrap_err();
        assert!(err.contains("readable by group or others"), "{err}");

        let path = key_file(dir.path(), &text, 0o600);
        let err =
            keyring_from_env(env(&[(KEYS_FILE_ENV, &path), (ACTIVE_KEY_ENV, "gone")])).unwrap_err();
        assert!(
            err.contains("not in the key file") && err.contains(&path),
            "{err}"
        );
    }
}
