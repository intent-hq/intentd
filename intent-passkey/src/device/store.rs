//! The encrypted device passkey store (spec §5.2): one JSON file. A random master
//! key encrypts each credential private key (AES-256-GCM, bound to the credential
//! ID, RP ID and user handle); a key from Argon2id(PIN, salt) wraps the master key.
//! The Argon2id parameters are stored in the file, so they can change later.

use std::fs::{self, DirBuilder, File, OpenOptions};
use std::io::{self, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, KeyInit as _, Nonce};
use rand::RngCore as _;
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

use crate::protocol::{Bytes, ErrorCode, PasskeyError};

const STORE_VERSION: u32 = 1;
const STORE_FILE: &str = "store.json";
const MASTER_AAD: &[u8] = b"intent-passkey/master-key/v1";
const CREDENTIAL_AAD: &[u8] = b"intent-passkey/credential/v1";
const KEY_LEN: usize = 32;
const NONCE_LEN: usize = 12;
const SALT_LEN: usize = 16;
/// Bounds for parameters read from the file, so a damaged file cannot make the
/// sidecar allocate without limit.
const MAX_MEMORY_KIB: u32 = 1 << 20;
const MAX_ITERATIONS: u32 = 16;
const MAX_PARALLELISM: u32 = 4;

/// Argon2id cost. The default (256 MiB, 3 passes, 1 lane) takes about 0.5 s on
/// the 2014 `MacBook Air` that runs Intent OS.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KdfParams {
    pub memory_kib: u32,
    pub iterations: u32,
    pub parallelism: u32,
}

impl Default for KdfParams {
    fn default() -> Self {
        Self {
            memory_kib: 256 * 1024,
            iterations: 3,
            parallelism: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Kdf {
    algorithm: String,
    #[serde(flatten)]
    params: KdfParams,
    salt: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Sealed {
    nonce: Bytes,
    ciphertext: Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StoredCredential {
    pub credential_id: Bytes,
    pub rp_id: String,
    pub user_id: Bytes,
    pub user_name: String,
    pub user_display_name: String,
    pub algorithm: i64,
    /// SPKI DER.
    pub public_key: Bytes,
    pub private_key: Sealed,
    pub created_at: u64,
    pub last_used_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct StoreFile {
    version: u32,
    kdf: Kdf,
    master_key: Sealed,
    pub credentials: Vec<StoredCredential>,
}

/// The wrapping of the master key under one PIN: the part of the file a PIN change
/// replaces.
#[derive(Debug, Clone)]
pub(crate) struct Wrap {
    kdf: Kdf,
    master_key: Sealed,
}

pub(crate) struct MasterKey(Zeroizing<[u8; KEY_LEN]>);

#[derive(Debug)]
pub(crate) enum UnlockError {
    /// The PIN does not unwrap the master key (or the wrap was altered).
    WrongPin,
    Damaged(&'static str),
}

pub(crate) fn damaged(detail: &str) -> PasskeyError {
    PasskeyError::new(
        ErrorCode::Unknown,
        format!("the device passkey store is damaged ({detail})"),
    )
}

fn io_error(error: &io::Error) -> PasskeyError {
    PasskeyError::new(
        ErrorCode::Unknown,
        format!("could not use the device passkey store: {error}"),
    )
}

fn random<const N: usize>() -> [u8; N] {
    let mut bytes = [0u8; N];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    bytes
}

fn derive(pin: &str, kdf: &Kdf) -> Result<Zeroizing<[u8; KEY_LEN]>, UnlockError> {
    let KdfParams {
        memory_kib,
        iterations,
        parallelism,
    } = kdf.params;
    if kdf.algorithm != "argon2id"
        || memory_kib > MAX_MEMORY_KIB
        || iterations == 0
        || iterations > MAX_ITERATIONS
        || parallelism == 0
        || parallelism > MAX_PARALLELISM
        || kdf.salt.0.len() < 8
    {
        return Err(UnlockError::Damaged("key derivation parameters"));
    }
    let params = argon2::Params::new(memory_kib, iterations, parallelism, Some(KEY_LEN))
        .map_err(|_| UnlockError::Damaged("key derivation parameters"))?;
    let argon = argon2::Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);
    let mut key = Zeroizing::new([0u8; KEY_LEN]);
    argon
        .hash_password_into(pin.as_bytes(), &kdf.salt.0, key.as_mut())
        .map_err(|_| UnlockError::Damaged("key derivation parameters"))?;
    Ok(key)
}

fn seal(key: &[u8; KEY_LEN], aad: &[u8], plaintext: &[u8]) -> Sealed {
    let cipher = Aes256Gcm::new(key.into());
    let nonce = random::<NONCE_LEN>();
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .expect("AES-GCM encrypts any input shorter than 64 GiB");
    Sealed {
        nonce: Bytes(nonce.to_vec()),
        ciphertext: Bytes(ciphertext),
    }
}

fn open(key: &[u8; KEY_LEN], aad: &[u8], sealed: &Sealed) -> Option<Zeroizing<Vec<u8>>> {
    if sealed.nonce.0.len() != NONCE_LEN {
        return None;
    }
    Aes256Gcm::new(key.into())
        .decrypt(
            Nonce::from_slice(&sealed.nonce.0),
            Payload {
                msg: &sealed.ciphertext.0,
                aad,
            },
        )
        .ok()
        .map(Zeroizing::new)
}

/// Length-prefixed fields, so no two (ID, RP ID, user) triples share one AAD.
fn credential_aad(credential_id: &[u8], rp_id: &str, user_id: &[u8]) -> Vec<u8> {
    let mut aad = CREDENTIAL_AAD.to_vec();
    for field in [credential_id, rp_id.as_bytes(), user_id] {
        aad.extend_from_slice(&u32::try_from(field.len()).unwrap_or(u32::MAX).to_be_bytes());
        aad.extend_from_slice(field);
    }
    aad
}

impl MasterKey {
    pub fn generate() -> Self {
        Self(Zeroizing::new(random::<KEY_LEN>()))
    }

    /// Wraps this key under `pin` with a fresh salt. Slow (Argon2id).
    pub fn wrap(&self, pin: &str, params: KdfParams) -> Result<Wrap, PasskeyError> {
        let kdf = Kdf {
            algorithm: "argon2id".to_owned(),
            params,
            salt: Bytes(random::<SALT_LEN>().to_vec()),
        };
        let kek = derive(pin, &kdf).map_err(|_| {
            PasskeyError::new(ErrorCode::Unknown, "invalid key derivation parameters")
        })?;
        let master_key = seal(&kek, MASTER_AAD, self.0.as_ref());
        Ok(Wrap { kdf, master_key })
    }

    pub fn seal_private(
        &self,
        credential_id: &[u8],
        rp_id: &str,
        user_id: &[u8],
        secret: &[u8],
    ) -> Sealed {
        seal(
            &self.0,
            &credential_aad(credential_id, rp_id, user_id),
            secret,
        )
    }

    pub fn open_private(
        &self,
        credential: &StoredCredential,
    ) -> Result<Zeroizing<Vec<u8>>, PasskeyError> {
        open(
            &self.0,
            &credential_aad(
                &credential.credential_id.0,
                &credential.rp_id,
                &credential.user_id.0,
            ),
            &credential.private_key,
        )
        .ok_or_else(|| damaged("a credential key does not decrypt"))
    }
}

impl StoreFile {
    pub fn new(wrap: Wrap) -> Self {
        Self {
            version: STORE_VERSION,
            kdf: wrap.kdf,
            master_key: wrap.master_key,
            credentials: Vec::new(),
        }
    }

    /// Unwraps the master key with `pin`. Slow (Argon2id).
    pub fn unlock(&self, pin: &str) -> Result<MasterKey, UnlockError> {
        let kek = derive(pin, &self.kdf)?;
        let plain = open(&kek, MASTER_AAD, &self.master_key).ok_or(UnlockError::WrongPin)?;
        let key: [u8; KEY_LEN] = plain
            .as_slice()
            .try_into()
            .map_err(|_| UnlockError::Damaged("master key length"))?;
        Ok(MasterKey(Zeroizing::new(key)))
    }

    /// Whether this file still has the wrap `wrap` was taken from.
    pub fn same_wrap(&self, wrap: &Wrap) -> bool {
        self.kdf == wrap.kdf && self.master_key == wrap.master_key
    }

    pub fn wrap(&self) -> Wrap {
        Wrap {
            kdf: self.kdf.clone(),
            master_key: self.master_key.clone(),
        }
    }

    pub fn set_wrap(&mut self, wrap: Wrap) {
        self.kdf = wrap.kdf;
        self.master_key = wrap.master_key;
    }
}

/// The store directory and its file.
#[derive(Debug, Clone)]
pub(crate) struct StoreDir {
    dir: PathBuf,
}

impl StoreDir {
    pub fn new(dir: PathBuf) -> Self {
        Self { dir }
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    /// Creates the directory (0700) when needed and tightens its mode.
    pub fn ensure(&self) -> Result<(), PasskeyError> {
        if let Some(parent) = self.dir.parent() {
            fs::create_dir_all(parent).map_err(|error| io_error(&error))?;
        }
        match DirBuilder::new().mode(0o700).create(&self.dir) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(io_error(&error)),
        }
        fs::set_permissions(&self.dir, fs::Permissions::from_mode(0o700))
            .map_err(|error| io_error(&error))
    }

    /// `None` when no passkey was ever created here.
    pub fn load(&self) -> Result<Option<StoreFile>, PasskeyError> {
        let bytes = match fs::read(self.path(STORE_FILE)) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error(&error)),
        };
        let file: StoreFile = serde_json::from_slice(&bytes).map_err(|_| damaged("not valid"))?;
        if file.version != STORE_VERSION {
            return Err(damaged("unknown version"));
        }
        Ok(Some(file))
    }

    pub fn save(&self, file: &StoreFile) -> Result<(), PasskeyError> {
        let bytes = serde_json::to_vec_pretty(file).map_err(|error| {
            PasskeyError::new(ErrorCode::Unknown, format!("could not encode: {error}"))
        })?;
        self.write_atomic(STORE_FILE, &bytes)
    }

    /// Removes the store file (a store whose creation was cancelled).
    pub fn remove(&self) -> Result<(), PasskeyError> {
        match fs::remove_file(self.path(STORE_FILE)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io_error(&error)),
        }
    }

    /// Writes `name` through a temporary file, `fsync` and `rename`.
    pub fn write_atomic(&self, name: &str, bytes: &[u8]) -> Result<(), PasskeyError> {
        self.ensure()?;
        let target = self.path(name);
        let temp = self.path(&format!(".{name}.{:016x}.tmp", rand::random::<u64>()));
        let result = write_then_rename(&temp, &target, bytes);
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result.map_err(|error| io_error(&error))?;
        if let Ok(dir) = File::open(&self.dir) {
            let _ = dir.sync_all();
        }
        Ok(())
    }
}

fn write_then_rename(temp: &Path, target: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(temp)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temp, target)
}
