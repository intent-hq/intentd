//! Device passkeys (spec §4.2, §5.2–5.4, §6): discoverable, device-bound
//! credentials kept in an encrypted store and confirmed with the Intent OS PIN
//! through PAM.

mod keys;
mod limiter;
pub mod pin;
mod store;

use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::RngCore as _;
use zeroize::Zeroizing;

use crate::authenticator::{device_unsupported, Authenticator, CeremonyIo, StopSignal};
use crate::protocol::{
    Account, BluetoothState, Bytes, CreateExtensionOutputs, CreateParams, CreateResult, CredProps,
    DeviceCredential, ErrorCode, Event, GetParams, GetResult, PasskeyError, PinKind, Status,
    Transport,
};
use limiter::{Gate, Limiter};
pub use pin::{LockScreenProcesses, PamPin, PinVerifier, ScreenLock};
pub use store::KdfParams;
use store::{MasterKey, StoreDir, StoreFile, StoredCredential, UnlockError};

/// The PAM service Intent OS ships; device passkeys are on only when it exists.
pub const PAM_SERVICE: &str = "intent-passkey";
const PAM_SERVICE_FILE: &str = "/etc/pam.d/intent-passkey";
const CREDENTIAL_ID_LEN: usize = 32;

pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

pub struct DeviceConfig {
    /// The store directory (`~/.local/share/intent/passkeys`).
    pub dir: PathBuf,
    pub kdf: KdfParams,
    /// Device passkeys are offered only while this file exists.
    pub pam_service_file: PathBuf,
    /// Changes when the machine restarts (`/proc/sys/kernel/random/boot_id`).
    pub boot_id: Option<String>,
    pub screen_poll: Duration,
    /// Wall clock in milliseconds since the Unix epoch.
    pub clock: Clock,
}

fn system_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

impl DeviceConfig {
    /// The Intent OS defaults; `None` without a home directory.
    #[must_use]
    pub fn system() -> Option<Self> {
        let data = std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .or_else(|| {
                std::env::var_os("HOME")
                    .map(PathBuf::from)
                    .filter(|path| path.is_absolute())
                    .map(|home| home.join(".local/share"))
            })?;
        Some(Self {
            dir: data.join("intent/passkeys"),
            kdf: KdfParams::default(),
            pam_service_file: PathBuf::from(PAM_SERVICE_FILE),
            boot_id: std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
                .ok()
                .map(|id| id.trim().to_owned()),
            screen_poll: Duration::from_secs(2),
            clock: Arc::new(system_now),
        })
    }
}

struct Inner<P, L> {
    store: StoreDir,
    kdf: KdfParams,
    pam_service_file: PathBuf,
    clock: Clock,
    pin: P,
    screen: L,
    limiter: Mutex<Limiter>,
    /// Serializes read-modify-write of the store file (held on a blocking thread).
    writes: Mutex<()>,
}

/// The device authenticator. Answers only `transport: "device"`; see [`WithDevice`].
pub struct Device<P, L> {
    inner: Arc<Inner<P, L>>,
}

fn not_allowed(message: &str) -> PasskeyError {
    PasskeyError::new(ErrorCode::NotAllowed, message)
}

fn task_failed(error: &tokio::task::JoinError) -> PasskeyError {
    PasskeyError::new(ErrorCode::Unknown, format!("internal error: {error}"))
}

impl<P: PinVerifier, L: ScreenLock> Device<P, L> {
    /// Must run inside a Tokio runtime: it starts the lock screen watch.
    pub fn new(config: DeviceConfig, pin: P, screen: L) -> Self {
        let store = StoreDir::new(config.dir);
        let limiter = Limiter::load(store.clone(), config.boot_id);
        let inner = Arc::new(Inner {
            store,
            kdf: config.kdf,
            pam_service_file: config.pam_service_file,
            clock: config.clock,
            pin,
            screen,
            limiter: Mutex::new(limiter),
            writes: Mutex::new(()),
        });
        tokio::spawn(watch_screen(Arc::downgrade(&inner), config.screen_poll));
        Self { inner }
    }
}

/// Clears the limiter once a lock screen has opened and closed after wrong PINs.
async fn watch_screen<P: PinVerifier, L: ScreenLock>(inner: Weak<Inner<P, L>>, every: Duration) {
    let mut tick = tokio::time::interval(every);
    loop {
        tick.tick().await;
        let Some(inner) = inner.upgrade() else { return };
        if !inner.limiter().has_failures() {
            continue;
        }
        let watched = Arc::clone(&inner);
        let Ok(locked) = tokio::task::spawn_blocking(move || watched.screen.locked()).await else {
            continue;
        };
        if inner.limiter().observe_screen(locked) {
            tracing::info!("the screen was unlocked; device passkey PIN tries reset");
        }
    }
}

impl<P: PinVerifier, L: ScreenLock> Inner<P, L> {
    fn limiter(&self) -> std::sync::MutexGuard<'_, Limiter> {
        self.limiter.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn now(&self) -> u64 {
        (self.clock)()
    }

    async fn load(self: &Arc<Self>) -> Result<Option<StoreFile>, PasskeyError> {
        let inner = Arc::clone(self);
        tokio::task::spawn_blocking(move || inner.store.load())
            .await
            .map_err(|error| task_failed(&error))?
    }

    /// Loads the store, applies `change` and saves it, under the write lock. With
    /// `stop`, a stopped ceremony leaves the store as it was: the check runs right
    /// before the save, and a stop that came during the save restores the
    /// previous file. Lock, save and restore all run on one blocking thread, so
    /// they finish even when the server drops the ceremony future.
    async fn update<T: Send + 'static>(
        self: &Arc<Self>,
        stop: Option<StopSignal>,
        change: impl FnOnce(Option<StoreFile>) -> Result<(StoreFile, T), PasskeyError> + Send + 'static,
    ) -> Result<T, PasskeyError> {
        let inner = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let _guard = inner.writes.lock().unwrap_or_else(PoisonError::into_inner);
            let before = inner.store.load()?;
            let (file, value) = change(before.clone())?;
            let Some(stop) = stop else {
                inner.store.save(&file)?;
                return Ok(value);
            };
            stop.check()?;
            inner.store.save(&file)?;
            if let Err(stopped) = stop.check() {
                match &before {
                    Some(previous) => inner.store.save(previous)?,
                    None => inner.store.remove()?,
                }
                return Err(stopped);
            }
            Ok(value)
        })
        .await
        .map_err(|error| task_failed(&error))?
    }

    /// Sends `pin-needed` until a PIN arrives while the attempt limit allows a
    /// try; a PIN sent during the wait is ignored and the prompt repeats.
    async fn ask_limited(
        &self,
        io: &mut CeremonyIo,
        wrong_pin: bool,
        old_pin: bool,
    ) -> Result<Zeroizing<String>, PasskeyError> {
        loop {
            let (attempts_left, retry_after_ms) = match self.limiter().gate(self.now()) {
                Gate::Locked => return Err(locked()),
                Gate::Open { attempts_left } => (attempts_left, None),
                Gate::Wait {
                    retry_after_ms,
                    attempts_left,
                } => (attempts_left, Some(retry_after_ms)),
            };
            let pin = io
                .ask_pin(Event::PinNeeded {
                    kind: PinKind::Device,
                    attempts_left: Some(attempts_left),
                    retry_after_ms,
                    wrong_pin,
                    old_pin,
                })
                .await?;
            if !pin.is_empty() && matches!(self.limiter().gate(self.now()), Gate::Open { .. }) {
                return Ok(pin);
            }
        }
    }

    /// Asks for the PIN until PAM accepts it, under the attempt limit.
    async fn confirm_pin(
        self: &Arc<Self>,
        io: &mut CeremonyIo,
    ) -> Result<Zeroizing<String>, PasskeyError> {
        let mut wrong_pin = false;
        loop {
            let pin = self.ask_limited(io, wrong_pin, false).await?;
            let inner = Arc::clone(self);
            let (pin, verified) = tokio::task::spawn_blocking(move || {
                let verified = inner.pin.verify(&pin);
                (pin, verified)
            })
            .await
            .map_err(|error| task_failed(&error))?;
            // The counter resets only once the store unlocks (see `unlock`), so
            // wrong old PINs add up even though PAM accepts the new one. A wrong
            // PIN counts even when the ceremony was cancelled meanwhile.
            if verified? {
                io.ensure_running()?;
                return Ok(pin);
            }
            self.limiter().failed(self.now());
            wrong_pin = true;
        }
    }

    /// Unwraps the master key with the PAM-checked `pin`. A store from before a PIN
    /// change asks once for the previous PIN and is re-wrapped under the new one;
    /// with `create`, a missing store is made.
    async fn unlock(
        self: &Arc<Self>,
        io: &mut CeremonyIo,
        pin: Zeroizing<String>,
        create: bool,
    ) -> Result<(StoreFile, MasterKey), PasskeyError> {
        let Some(file) = self.load().await? else {
            if !create {
                return Err(not_allowed("there are no passkeys on this device"));
            }
            let kdf = self.kdf;
            let (master, wrap) = blocking(move || {
                let master = MasterKey::generate();
                let wrap = master.wrap(&pin, kdf)?;
                Ok((master, wrap))
            })
            .await?;
            io.ensure_running()?;
            let file = self
                .update(Some(io.stop_signal()), move |existing| match existing {
                    None => {
                        let file = StoreFile::new(wrap);
                        Ok((file.clone(), file))
                    }
                    Some(_) => Err(not_allowed("the passkey store changed; try again")),
                })
                .await?;
            self.limiter().succeeded();
            return Ok((file, master));
        };
        let (file, pin, unlocked) = blocking(move || {
            let unlocked = file.unlock(&pin);
            Ok((file, pin, unlocked))
        })
        .await?;
        io.ensure_running()?;
        match unlocked {
            Ok(master) => {
                self.limiter().succeeded();
                Ok((file, master))
            }
            Err(UnlockError::Damaged(detail)) => Err(store::damaged(detail)),
            Err(UnlockError::WrongPin) => self.rewrap(io, file, pin).await,
        }
    }

    async fn rewrap(
        self: &Arc<Self>,
        io: &mut CeremonyIo,
        file: StoreFile,
        new_pin: Zeroizing<String>,
    ) -> Result<(StoreFile, MasterKey), PasskeyError> {
        // The old PIN is checked only against the Argon2id wrap (no PAM, no
        // faillock), so it goes through the same limit as any PIN.
        let old_pin = self.ask_limited(io, false, true).await?;
        let kdf = self.kdf;
        let old_wrap = file.wrap();
        let unlocked = blocking(move || match file.unlock(&old_pin) {
            Ok(master) => {
                let wrap = master.wrap(&new_pin, kdf)?;
                Ok(Ok((master, wrap)))
            }
            Err(error) => Ok(Err(error)),
        })
        .await?;
        let (master, wrap) = match unlocked {
            Ok(pair) => pair,
            Err(UnlockError::Damaged(detail)) => return Err(store::damaged(detail)),
            Err(UnlockError::WrongPin) => {
                self.limiter().failed(self.now());
                return Err(not_allowed("the previous PIN is not correct"));
            }
        };
        io.ensure_running()?;
        let file = self
            .update(Some(io.stop_signal()), move |existing| {
                let mut file = existing.ok_or_else(|| not_allowed("the passkey store is gone"))?;
                if !file.same_wrap(&old_wrap) {
                    return Err(not_allowed("the passkey store changed; try again"));
                }
                file.set_wrap(wrap);
                Ok((file.clone(), file))
            })
            .await?;
        self.limiter().succeeded();
        tracing::info!("device passkeys re-wrapped for the new PIN");
        Ok((file, master))
    }

    async fn create(
        self: &Arc<Self>,
        params: CreateParams,
        mut io: CeremonyIo,
    ) -> Result<CreateResult, PasskeyError> {
        let alg = keys::pick_algorithm(&params.pub_key_cred_params)
            .ok_or_else(|| PasskeyError::new(ErrorCode::NotSupported, "no supported algorithm"))?;
        let client_data_json = crate::webauthn::create_client_data(&params).await?;
        if let Some(file) = self.load().await? {
            let excluded = file.credentials.iter().any(|credential| {
                credential.rp_id == params.rp_id
                    && params
                        .exclude_credentials
                        .iter()
                        .any(|excluded| excluded.id == credential.credential_id)
            });
            if excluded {
                return Err(PasskeyError::new(
                    ErrorCode::InvalidState,
                    "this device already has a passkey for this account",
                ));
            }
        }
        let pin = self.confirm_pin(&mut io).await?;
        io.status(Status::Processing);
        let (file, master) = self.unlock(&mut io, pin, true).await?;
        let wrap = file.wrap();
        drop(file);

        let pair = keys::generate(alg)?;
        let mut credential_id = vec![0u8; CREDENTIAL_ID_LEN];
        rand::rngs::OsRng.fill_bytes(&mut credential_id);
        let credential = StoredCredential {
            private_key: master.seal_private(
                &credential_id,
                &params.rp_id,
                &params.user.id.0,
                &pair.secret,
            ),
            credential_id: Bytes(credential_id.clone()),
            rp_id: params.rp_id.clone(),
            user_id: params.user.id.clone(),
            user_name: params.user.name.clone(),
            user_display_name: params.user.display_name.clone(),
            algorithm: alg,
            public_key: Bytes(pair.spki.clone()),
            created_at: self.now(),
            last_used_at: None,
        };
        drop(master);
        io.ensure_running()?;
        self.update(Some(io.stop_signal()), move |existing| {
            let mut file = existing.ok_or_else(|| not_allowed("the passkey store is gone"))?;
            if !file.same_wrap(&wrap) {
                return Err(not_allowed("the passkey store changed; try again"));
            }
            // One discoverable credential per site and user, as on other authenticators.
            file.credentials.retain(|existing| {
                existing.rp_id != credential.rp_id || existing.user_id != credential.user_id
            });
            file.credentials.push(credential);
            Ok((file, ()))
        })
        .await?;

        let auth_data = keys::create_auth_data(&params.rp_id, &credential_id, &pair.cose);
        Ok(CreateResult {
            credential_id: Bytes(credential_id),
            client_data_json: Bytes(client_data_json.into_bytes()),
            attestation_object: Bytes(keys::none_attestation(&auth_data)),
            authenticator_data: Bytes(auth_data),
            public_key: Some(Bytes(pair.spki)),
            public_key_algorithm: alg,
            transports: vec!["internal".to_owned()],
            authenticator_attachment: "platform".to_owned(),
            extensions: CreateExtensionOutputs {
                cred_props: (params.extensions.cred_props == Some(true))
                    .then_some(CredProps { rk: Some(true) }),
            },
        })
    }

    async fn get(
        self: &Arc<Self>,
        params: GetParams,
        mut io: CeremonyIo,
    ) -> Result<GetResult, PasskeyError> {
        let client_data_json = crate::webauthn::get_client_data(&params).await?;
        let file = self.load().await?;
        let candidates: Vec<StoredCredential> = file
            .map(|file| file.credentials)
            .unwrap_or_default()
            .into_iter()
            .filter(|credential| {
                credential.rp_id == params.rp_id
                    && (params.allow_credentials.is_empty()
                        || params
                            .allow_credentials
                            .iter()
                            .any(|allowed| allowed.id == credential.credential_id))
            })
            .collect();
        let chosen = match candidates.len() {
            0 => {
                return Err(not_allowed(
                    "there is no passkey for this site on this device",
                ))
            }
            1 => &candidates[0],
            _ => {
                let accounts = candidates
                    .iter()
                    .enumerate()
                    .map(|(index, credential)| Account {
                        index,
                        user_name: credential.user_name.clone(),
                        user_display_name: credential.user_display_name.clone(),
                    })
                    .collect();
                &candidates[io.choose_account(accounts).await?]
            }
        };
        let credential_id = chosen.credential_id.clone();

        let pin = self.confirm_pin(&mut io).await?;
        io.status(Status::Processing);
        let (file, master) = self.unlock(&mut io, pin, false).await?;
        let credential = file
            .credentials
            .iter()
            .find(|credential| credential.credential_id == credential_id)
            .ok_or_else(|| not_allowed("the passkey was deleted"))?;
        let auth_data = keys::get_auth_data(&params.rp_id);
        let secret = master.open_private(credential)?;
        drop(master);
        let signature = keys::sign(
            credential.algorithm,
            &secret,
            &keys::signed_message(&auth_data, client_data_json.as_bytes()),
        )?;
        drop(secret);
        let user_handle = credential.user_id.clone();
        io.ensure_running()?;

        let now = self.now();
        let touched = credential_id.clone();
        let saved = self
            .update(Some(io.stop_signal()), move |existing| {
                let mut file = existing.ok_or_else(|| not_allowed("the passkey store is gone"))?;
                if let Some(credential) = file
                    .credentials
                    .iter_mut()
                    .find(|credential| credential.credential_id == touched)
                {
                    credential.last_used_at = Some(now);
                }
                Ok((file, ()))
            })
            .await;
        io.ensure_running()?;
        if let Err(error) = saved {
            tracing::warn!(%error, "could not record the passkey's last use");
        }

        Ok(GetResult {
            credential_id,
            client_data_json: Bytes(client_data_json.into_bytes()),
            authenticator_data: Bytes(auth_data),
            signature: Bytes(signature),
            user_handle: Some(user_handle),
            authenticator_attachment: "platform".to_owned(),
        })
    }

    async fn list(self: &Arc<Self>) -> Result<Vec<DeviceCredential>, PasskeyError> {
        Ok(self
            .load()
            .await?
            .map(|file| file.credentials)
            .unwrap_or_default()
            .into_iter()
            .map(|credential| DeviceCredential {
                credential_id: credential.credential_id,
                rp_id: credential.rp_id,
                user_name: credential.user_name,
                user_display_name: credential.user_display_name,
                created_at: credential.created_at,
                last_used_at: credential.last_used_at,
            })
            .collect())
    }

    async fn delete(self: &Arc<Self>, credential_id: Bytes) -> Result<(), PasskeyError> {
        if self.load().await?.is_none() {
            return Ok(());
        }
        self.update(None, move |existing| {
            let mut file = existing.ok_or_else(|| not_allowed("the passkey store is gone"))?;
            file.credentials
                .retain(|credential| credential.credential_id != credential_id);
            Ok((file, ()))
        })
        .await
    }
}

fn locked() -> PasskeyError {
    PasskeyError::new(
        ErrorCode::PinLocked,
        "too many wrong PINs; unlock the screen to use device passkeys again",
    )
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, PasskeyError> + Send + 'static,
) -> Result<T, PasskeyError> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|error| task_failed(&error))?
}

fn device_only() -> PasskeyError {
    PasskeyError::new(
        ErrorCode::NotSupported,
        "this authenticator has device passkeys only",
    )
}

impl<P: PinVerifier, L: ScreenLock> Authenticator for Device<P, L> {
    async fn bluetooth(&self) -> BluetoothState {
        BluetoothState::Missing
    }

    fn usb_supported(&self) -> bool {
        false
    }

    async fn create(
        &self,
        params: CreateParams,
        io: CeremonyIo,
    ) -> Result<CreateResult, PasskeyError> {
        if params.transport != Transport::Device {
            return Err(device_only());
        }
        self.inner.create(params, io).await
    }

    async fn get(&self, params: GetParams, io: CeremonyIo) -> Result<GetResult, PasskeyError> {
        if params.transport != Transport::Device {
            return Err(device_only());
        }
        self.inner.get(params, io).await
    }

    fn device_supported(&self) -> bool {
        self.inner.pam_service_file.exists()
    }

    async fn device_list(&self) -> Result<Vec<DeviceCredential>, PasskeyError> {
        self.inner.list().await
    }

    async fn device_delete(&self, credential_id: Bytes) -> Result<(), PasskeyError> {
        self.inner.delete(credential_id).await
    }
}

/// Routes `transport: "device"` and the `device.*` methods to `device`, the rest to
/// `other`.
pub struct WithDevice<A, D> {
    pub other: A,
    pub device: Option<D>,
}

impl<A: Authenticator, D: Authenticator> Authenticator for WithDevice<A, D> {
    async fn bluetooth(&self) -> BluetoothState {
        self.other.bluetooth().await
    }

    fn usb_supported(&self) -> bool {
        self.other.usb_supported()
    }

    async fn create(
        &self,
        params: CreateParams,
        io: CeremonyIo,
    ) -> Result<CreateResult, PasskeyError> {
        match (&self.device, params.transport) {
            (Some(device), Transport::Device) => device.create(params, io).await,
            (None, Transport::Device) => Err(device_unsupported()),
            _ => self.other.create(params, io).await,
        }
    }

    async fn get(&self, params: GetParams, io: CeremonyIo) -> Result<GetResult, PasskeyError> {
        match (&self.device, params.transport) {
            (Some(device), Transport::Device) => device.get(params, io).await,
            (None, Transport::Device) => Err(device_unsupported()),
            _ => self.other.get(params, io).await,
        }
    }

    fn device_supported(&self) -> bool {
        self.device.as_ref().is_some_and(D::device_supported)
    }

    async fn device_list(&self) -> Result<Vec<DeviceCredential>, PasskeyError> {
        match &self.device {
            Some(device) => device.device_list().await,
            None => Err(device_unsupported()),
        }
    }

    async fn device_delete(&self, credential_id: Bytes) -> Result<(), PasskeyError> {
        match &self.device {
            Some(device) => device.device_delete(credential_id).await,
            None => Err(device_unsupported()),
        }
    }
}
