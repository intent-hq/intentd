//! Tenant secrets, encrypted, in Postgres (`tenant_secrets`).
//!
//! An orchestrator serves many tenants and hands a tenant to another
//! process on takeover, so a tenant's secrets cannot live in one process's
//! file. [`TenantSecretStore`] keeps them in the tenant's rows instead. The
//! desktop and the regular intentd keep using `intent_core::FileSecretStore`;
//! only the orchestrator's Postgres composition builds this store.
//!
//! **Encryption.** Each value is sealed with XChaCha20-Poly1305 under the
//! keyring's active key, with a fresh random 24-byte nonce (random nonces
//! are safe at this size, also across processes). The associated data
//! binds the format version, the tenant, the account and the key id, each
//! length-prefixed, so a row copied to another tenant or account, or
//! relabelled with another key id, fails to open. The database holds the
//! key id, the nonce and the ciphertext only.
//!
//! **Keys.** A [`SecretKeyring`] holds one active key, which seals every
//! write, and any number of older keys, which only open. To rotate, add the
//! new key as active and keep the old one: rows sealed under it still open,
//! and each is resealed under the active key on its next write. Where the
//! keys come from (an env value or a file) is the orchestrator's choice; an
//! external KMS can later supply the same keyring.
//!
//! **Fail closed.** A row whose key id the keyring lacks, or that does not
//! open under its key (wrong key, tampered row), is an error naming the
//! tenant, the account and the key id, never "no secret": the trait's
//! callers treat `Err` as "unknown" and fail closed, but `None` as absent.
//!
//! **Writes** take the tenant's fenced writer path
//! ([`PgStore::begin_write`]): a process that lost the tenant, or whose
//! self-fence fired, cannot store or delete a secret (`Error::NotOwner`).
//!
//! Semantics match the file store: `load` returns `None` for an unset or
//! empty value, `store` replaces, and deleting an absent account succeeds.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use base64::Engine as _;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use intent_core::{now_iso, Error, Result};
use zeroize::Zeroizing;

use super::{db, PgStore};

/// Size of a keyring key (XChaCha20-Poly1305 takes 256-bit keys).
pub const SECRET_KEY_LEN: usize = 32;
/// Size of the random nonce stored with each value.
const NONCE_LEN: usize = 24;
/// Longest accepted key id.
const KEY_ID_MAX: usize = 64;
/// Version tag in the associated data; a new format gets a new tag.
const AAD_VERSION: &[u8] = b"intent-tenant-secret/v1";

/// The keys that seal and open tenant secrets: one active key for writes,
/// and older keys kept for reads during a rotation. `Debug` never prints a
/// key.
#[derive(Clone)]
pub struct SecretKeyring {
    active: String,
    keys: BTreeMap<String, KeyBytes>,
}

impl fmt::Debug for SecretKeyring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretKeyring")
            .field("active", &self.active)
            .field("key_ids", &self.keys.keys().collect::<Vec<_>>())
            .finish()
    }
}

fn key_error(message: impl fmt::Display) -> Error {
    Error::Internal(format!("tenant secret keyring: {message}"))
}

/// Why `id` is not a valid key id, if it is not. Callers wrap the reason
/// once with [`key_error`].
///
/// Keyring construction errors never quote a key id: a misformatted key
/// file line (`<base64 key>` alone, split at its `=` padding) would put the
/// key itself where the id belongs.
fn check_key_id(id: &str) -> std::result::Result<(), String> {
    let valid = !id.is_empty()
        && id.len() <= KEY_ID_MAX
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(format!(
            "invalid key id: use 1-{KEY_ID_MAX} characters from A-Z a-z 0-9 . _ -"
        ))
    }
}

type KeyBytes = Arc<Zeroizing<[u8; SECRET_KEY_LEN]>>;

/// `key` as a keyring key, or why it is not one (never quoting it).
fn key_bytes(key: &[u8]) -> std::result::Result<KeyBytes, String> {
    let bytes: [u8; SECRET_KEY_LEN] = key.try_into().map_err(|_| {
        format!(
            "the key is {} bytes; it must be exactly {SECRET_KEY_LEN}",
            key.len()
        )
    })?;
    Ok(Arc::new(Zeroizing::new(bytes)))
}

impl SecretKeyring {
    /// A keyring whose active key is `key`, named `key_id`.
    ///
    /// # Errors
    /// `Error::Internal` when the id is not 1-64 characters of `A-Z a-z 0-9
    /// . _ -`, or the key is not exactly [`SECRET_KEY_LEN`] bytes.
    pub fn new(key_id: &str, key: &[u8]) -> Result<Self> {
        check_key_id(key_id).map_err(key_error)?;
        let mut keys = BTreeMap::new();
        keys.insert(key_id.to_owned(), key_bytes(key).map_err(key_error)?);
        Ok(Self {
            active: key_id.to_owned(),
            keys,
        })
    }

    /// Adds an older key that opens rows sealed under `key_id` but never
    /// seals.
    ///
    /// # Errors
    /// As [`Self::new`], and when the keyring already has `key_id`.
    pub fn with_previous_key(mut self, key_id: &str, key: &[u8]) -> Result<Self> {
        check_key_id(key_id).map_err(key_error)?;
        if self.keys.contains_key(key_id) {
            return Err(key_error("duplicate key id"));
        }
        self.keys
            .insert(key_id.to_owned(), key_bytes(key).map_err(key_error)?);
        Ok(self)
    }

    /// Parses a key file: one `key_id=<base64 of 32 bytes>` per line; blank
    /// lines and lines starting with `#` are skipped. `active` names the key
    /// that seals; it must be in the file.
    ///
    /// # Errors
    /// `Error::Internal` naming the line for a malformed line, a bad key id
    /// or key, or a duplicate id, and when `active` is not in the file.
    /// Errors name lines, never an id or a key (see [`check_key_id`]).
    pub fn parse(text: &str, active: &str) -> Result<Self> {
        let mut keys = BTreeMap::new();
        for (i, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let n = i + 1;
            let (id, encoded) = line
                .split_once('=')
                .ok_or_else(|| key_error(format!("line {n}: expected key_id=<base64 key>")))?;
            let (id, encoded) = (id.trim(), encoded.trim());
            check_key_id(id).map_err(|e| key_error(format!("line {n}: {e}")))?;
            let decoded = Zeroizing::new(
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .map_err(|_| key_error(format!("line {n}: the key is not valid base64")))?,
            );
            let key = key_bytes(&decoded).map_err(|e| key_error(format!("line {n}: {e}")))?;
            if keys.insert(id.to_owned(), key).is_some() {
                return Err(key_error(format!("line {n}: duplicate key id")));
            }
        }
        if !keys.contains_key(active) {
            return Err(key_error("the active key id is not in the key file"));
        }
        Ok(Self {
            active: active.to_owned(),
            keys,
        })
    }

    /// The id of the key that seals new writes.
    #[must_use]
    pub fn active_key_id(&self) -> &str {
        &self.active
    }

    fn cipher(&self, key_id: &str) -> Option<XChaCha20Poly1305> {
        self.keys
            .get(key_id)
            .map(|key| XChaCha20Poly1305::new(key.as_ref().as_ref().into()))
    }

    fn seal(&self, tenant: &str, account: &str, value: &str) -> Result<Sealed> {
        let cipher = self
            .cipher(&self.active)
            .ok_or_else(|| key_error("the active key is missing"))?;
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let aad = associated_data(tenant, account, &self.active);
        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: value.as_bytes(),
                    aad: &aad,
                },
            )
            .map_err(|_| key_error("encryption failed"))?;
        Ok(Sealed {
            key_id: self.active.clone(),
            nonce: nonce.to_vec(),
            ciphertext,
        })
    }

    fn open(&self, tenant: &str, account: &str, row: &Sealed) -> Result<Zeroizing<String>> {
        let fail = |why: &str| {
            Error::Internal(format!(
                "tenant secret {account:?} of tenant {tenant:?} (key {:?}) cannot be read: {why}",
                row.key_id
            ))
        };
        let cipher = self
            .cipher(&row.key_id)
            .ok_or_else(|| fail("the keyring has no key with this id"))?;
        if row.nonce.len() != NONCE_LEN {
            return Err(fail("the stored nonce is malformed"));
        }
        let aad = associated_data(tenant, account, &row.key_id);
        let plain = Zeroizing::new(
            cipher
                .decrypt(
                    XNonce::from_slice(&row.nonce),
                    Payload {
                        msg: &row.ciphertext,
                        aad: &aad,
                    },
                )
                .map_err(|_| {
                    fail("it does not decrypt under its key (wrong key or altered row)")
                })?,
        );
        let text = std::str::from_utf8(&plain).map_err(|_| fail("it is not UTF-8"))?;
        Ok(Zeroizing::new(text.to_owned()))
    }
}

/// `version || len(tenant) || tenant || len(account) || account || len(key_id) || key_id`,
/// lengths as 4-byte big-endian, so no two inputs share an encoding.
fn associated_data(tenant: &str, account: &str, key_id: &str) -> Vec<u8> {
    let mut aad =
        Vec::with_capacity(AAD_VERSION.len() + 12 + tenant.len() + account.len() + key_id.len());
    aad.extend_from_slice(AAD_VERSION);
    for part in [tenant, account, key_id] {
        let len = u32::try_from(part.len()).unwrap_or(u32::MAX);
        aad.extend_from_slice(&len.to_be_bytes());
        aad.extend_from_slice(part.as_bytes());
    }
    aad
}

/// One stored row: what the database holds for a secret.
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct Sealed {
    key_id: String,
    nonce: Vec<u8>,
    ciphertext: Vec<u8>,
}

impl PgStore {
    async fn get_tenant_secret(&self, account: &str) -> Result<Option<Sealed>> {
        let mut tx = self.begin().await?;
        let row = sqlx::query_as::<_, Sealed>(
            "SELECT key_id, nonce, ciphertext FROM tenant_secrets \
             WHERE tenant_id = $1 AND account = $2",
        )
        .bind(self.tenant())
        .bind(account)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(row)
    }

    async fn put_tenant_secret(&self, account: &str, sealed: &Sealed) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query(
            "INSERT INTO tenant_secrets \
             (tenant_id, account, key_id, nonce, ciphertext, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6) \
             ON CONFLICT (tenant_id, account) DO UPDATE SET \
             key_id = excluded.key_id, nonce = excluded.nonce, \
             ciphertext = excluded.ciphertext, updated_at = excluded.updated_at",
        )
        .bind(self.tenant())
        .bind(account)
        .bind(&sealed.key_id)
        .bind(&sealed.nonce)
        .bind(&sealed.ciphertext)
        .bind(now_iso())
        .execute(&mut *tx)
        .await
        .map_err(db)?;
        tx.commit().await.map_err(db)
    }

    async fn delete_tenant_secret(&self, account: &str) -> Result<()> {
        let mut tx = self.begin_write().await?;
        sqlx::query("DELETE FROM tenant_secrets WHERE tenant_id = $1 AND account = $2")
            .bind(self.tenant())
            .bind(account)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)
    }
}

/// One tenant's secrets, encrypted in its Postgres rows (see the module
/// docs). Cheap to clone. The `*_blocking` methods serve the synchronous
/// `SecretStore` trait of `intent-services`.
#[derive(Clone)]
pub struct TenantSecretStore {
    store: PgStore,
    keyring: Arc<SecretKeyring>,
    runtime: tokio::runtime::Handle,
}

impl fmt::Debug for TenantSecretStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TenantSecretStore")
            .field("tenant", &self.store.tenant())
            .field("keyring", &self.keyring)
            .finish_non_exhaustive()
    }
}

impl TenantSecretStore {
    /// The secret store of `store`'s tenant, sealed with `keyring`. Call it
    /// inside the multi-thread Tokio runtime the blocking methods will use;
    /// that runtime must outlive the store.
    ///
    /// # Errors
    /// `Error::Internal` when `store` is not a Postgres store, or no
    /// multi-thread Tokio runtime is running.
    pub fn new(store: &crate::Store, keyring: Arc<SecretKeyring>) -> Result<Self> {
        let crate::Backend::Postgres(pg) = &store.backend else {
            return Err(Error::Internal(
                "tenant secrets need a Postgres store; the SQLite daemon uses the file store"
                    .into(),
            ));
        };
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|e| Error::Internal(format!("tenant secrets need a Tokio runtime: {e}")))?;
        // A blocking call from a plain thread runs on this runtime; a
        // current-thread runtime drives no IO from another thread, so that
        // call would hang.
        if runtime.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
            return Err(Error::Internal(
                "tenant secrets need the multi-thread Tokio runtime".into(),
            ));
        }
        Ok(Self {
            store: pg.clone(),
            keyring,
            runtime,
        })
    }

    /// The tenant's secret for `account`; `None` when unset or empty.
    ///
    /// # Errors
    /// `Error::Internal` when the database fails, or the stored value does
    /// not open (unknown key id, wrong key, altered row).
    pub async fn load(&self, account: &str) -> Result<Option<String>> {
        let Some(row) = self.store.get_tenant_secret(account).await? else {
            return Ok(None);
        };
        let value = self.keyring.open(self.store.tenant(), account, &row)?;
        Ok((!value.is_empty()).then(|| value.as_str().to_owned()))
    }

    /// Seals `value` under the active key and stores it for `account`,
    /// replacing any earlier value.
    ///
    /// # Errors
    /// `Error::NotOwner` when this process lost the tenant; `Error::StoreBusy`
    /// past the writer wait budget; `Error::Internal` when the database fails.
    pub async fn store(&self, account: &str, value: &str) -> Result<()> {
        let sealed = self.keyring.seal(self.store.tenant(), account, value)?;
        self.store.put_tenant_secret(account, &sealed).await
    }

    /// Deletes the secret for `account`; absence is a success.
    ///
    /// # Errors
    /// As [`Self::store`].
    pub async fn delete(&self, account: &str) -> Result<()> {
        self.store.delete_tenant_secret(account).await
    }

    /// [`Self::load`] for synchronous callers.
    ///
    /// # Errors
    /// As [`Self::load`], and see [`Self::block`].
    pub fn load_blocking(&self, account: &str) -> Result<Option<String>> {
        self.block(self.load(account))
    }

    /// [`Self::store`] for synchronous callers.
    ///
    /// # Errors
    /// As [`Self::store`], and see [`Self::block`].
    pub fn store_blocking(&self, account: &str, value: &str) -> Result<()> {
        self.block(self.store(account, value))
    }

    /// [`Self::delete`] for synchronous callers.
    ///
    /// # Errors
    /// As [`Self::delete`], and see [`Self::block`].
    pub fn delete_blocking(&self, account: &str) -> Result<()> {
        self.block(self.delete(account))
    }

    /// Runs `op` to completion from synchronous code. On a multi-thread
    /// runtime worker it runs in place (`block_in_place`); outside any
    /// runtime, on the store's runtime. A current-thread runtime cannot
    /// drive the database while its only thread blocks, so there it fails
    /// instead of deadlocking.
    fn block<T>(&self, op: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        use tokio::runtime::{Handle, RuntimeFlavor};
        match Handle::try_current() {
            Ok(handle) if handle.runtime_flavor() == RuntimeFlavor::MultiThread => {
                tokio::task::block_in_place(|| handle.block_on(op))
            }
            Ok(_) => Err(Error::Internal(
                "tenant secrets cannot block on a current-thread Tokio runtime; \
                 run the orchestrator on the multi-thread runtime"
                    .into(),
            )),
            Err(_) => self.runtime.block_on(op),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY_A: [u8; 32] = [7; 32];
    const KEY_B: [u8; 32] = [9; 32];

    fn b64(key: &[u8]) -> String {
        base64::engine::general_purpose::STANDARD.encode(key)
    }

    #[test]
    fn a_value_round_trips_and_the_ciphertext_hides_it() {
        let ring = SecretKeyring::new("k1", &KEY_A).unwrap();
        let sealed = ring.seal("t1", "acct", "hunter2-secret").unwrap();
        assert_eq!(sealed.key_id, "k1");
        assert_eq!(sealed.nonce.len(), NONCE_LEN);
        assert!(!sealed
            .ciphertext
            .windows(b"hunter2".len())
            .any(|w| w == b"hunter2"));
        assert_eq!(
            ring.open("t1", "acct", &sealed).unwrap().as_str(),
            "hunter2-secret"
        );
    }

    #[test]
    fn each_seal_takes_a_fresh_nonce() {
        let ring = SecretKeyring::new("k1", &KEY_A).unwrap();
        let one = ring.seal("t1", "acct", "v").unwrap();
        let two = ring.seal("t1", "acct", "v").unwrap();
        assert_ne!(one.nonce, two.nonce);
        assert_ne!(one.ciphertext, two.ciphertext);
    }

    #[test]
    fn a_row_moved_to_another_tenant_account_or_key_id_does_not_open() {
        let ring = SecretKeyring::new("k1", &KEY_A)
            .unwrap()
            .with_previous_key("k0", &KEY_A)
            .unwrap();
        let sealed = ring.seal("t1", "acct", "v").unwrap();
        for (tenant, account) in [("t2", "acct"), ("t1", "other")] {
            let err = ring.open(tenant, account, &sealed).unwrap_err().to_string();
            assert!(err.contains("does not decrypt"), "{err}");
        }
        // Same key bytes under another id: the id is bound too.
        let relabelled = Sealed {
            key_id: "k0".into(),
            ..sealed.clone()
        };
        assert!(ring.open("t1", "acct", &relabelled).is_err());
        let mut tampered = sealed;
        tampered.ciphertext[0] ^= 1;
        assert!(ring.open("t1", "acct", &tampered).is_err());
    }

    #[test]
    fn a_wrong_or_missing_key_fails_closed_naming_the_row() {
        let sealed = SecretKeyring::new("k1", &KEY_A)
            .unwrap()
            .seal("t1", "acct", "v")
            .unwrap();
        let wrong = SecretKeyring::new("k1", &KEY_B).unwrap();
        let err = wrong.open("t1", "acct", &sealed).unwrap_err().to_string();
        assert!(
            err.contains("\"acct\"") && err.contains("\"t1\"") && err.contains("\"k1\""),
            "{err}"
        );
        let missing = SecretKeyring::new("k2", &KEY_A).unwrap();
        let err = missing.open("t1", "acct", &sealed).unwrap_err().to_string();
        assert!(err.contains("no key with this id"), "{err}");
    }

    #[test]
    fn rotation_seals_with_the_active_key_and_opens_older_rows() {
        let old = SecretKeyring::new("k1", &KEY_A).unwrap();
        let before = old.seal("t1", "acct", "v1").unwrap();
        let rotated = SecretKeyring::new("k2", &KEY_B)
            .unwrap()
            .with_previous_key("k1", &KEY_A)
            .unwrap();
        assert_eq!(rotated.open("t1", "acct", &before).unwrap().as_str(), "v1");
        let after = rotated.seal("t1", "acct", "v2").unwrap();
        assert_eq!(after.key_id, "k2");
        assert!(old.open("t1", "acct", &after).is_err());
    }

    #[test]
    fn the_key_file_parses_and_rejects_bad_input_without_quoting_keys() {
        let text = format!("# keys\n\nk1={}\n k2 = {} \n", b64(&KEY_A), b64(&KEY_B));
        let ring = SecretKeyring::parse(&text, "k2").unwrap();
        assert_eq!(ring.active_key_id(), "k2");
        assert_eq!(
            format!("{ring:?}"),
            "SecretKeyring { active: \"k2\", key_ids: [\"k1\", \"k2\"] }"
        );

        let short = b64(&[1; 16]);
        // Keys whose base64 has `+`, `/` and `=` padding: a line with the
        // key alone splits at the padding, putting the key where the id goes.
        let plus_slash: [u8; 32] = std::array::from_fn(|i| if i % 2 == 0 { 0xfb } else { 0xff });
        let cases = [
            (format!("k1={}", b64(&KEY_A)), "k9", "not in the key file"),
            ("k1".to_owned(), "k1", "line 1: expected key_id="),
            (
                format!("bad id={}", b64(&KEY_A)),
                "bad id",
                "invalid key id",
            ),
            (format!("k1={short}"), "k1", "line 1: the key is 16 bytes"),
            ("k1=!!!".to_owned(), "k1", "not valid base64"),
            (
                format!("k1={}\nk1={}", b64(&KEY_A), b64(&KEY_B)),
                "k1",
                "line 2: duplicate key id",
            ),
            (b64(&KEY_A), "k1", "line 1: the key is 0 bytes"),
            (b64(&plus_slash), "k1", "line 1: invalid key id"),
            (
                format!("{}=k1", b64(&KEY_B)),
                "k1",
                "line 1: the key is not valid base64",
            ),
        ];
        let secrets = [b64(&KEY_A), b64(&KEY_B), b64(&plus_slash), short.clone()];
        for (text, active, expected) in cases {
            let err = SecretKeyring::parse(&text, active).unwrap_err().to_string();
            assert!(err.contains(expected), "{text:?}: {err}");
            for secret in &secrets {
                // Any 12-character run of the encoded key would leak it.
                let unpadded = secret.trim_end_matches('=');
                for window in unpadded.as_bytes().windows(12) {
                    let window = std::str::from_utf8(window).unwrap();
                    assert!(!err.contains(window), "{err} quotes part of a key");
                }
            }
        }
        assert!(SecretKeyring::new("k1", &[0; 31]).is_err());
        assert!(SecretKeyring::new("", &KEY_A).is_err());
        assert!(SecretKeyring::new("k1", &KEY_A)
            .unwrap()
            .with_previous_key("k1", &KEY_B)
            .is_err());
    }

    #[test]
    fn associated_data_cannot_be_shifted_between_fields() {
        assert_ne!(
            associated_data("ab", "c", "k"),
            associated_data("a", "bc", "k")
        );
        assert_ne!(
            associated_data("a", "b", "ck"),
            associated_data("a", "bc", "k")
        );
    }
}
