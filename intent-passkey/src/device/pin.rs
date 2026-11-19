//! The Intent OS PIN check through PAM (service `intent-passkey`, so the try counts
//! in faillock with the lock screen), and lock screen detection for the limiter.

use std::ffi::{CStr, CString};
use std::os::unix::fs::MetadataExt as _;

use pam_client2::{ConversationHandler, ErrorCode as PamCode, Flag};
use zeroize::Zeroizing;

use crate::protocol::{ErrorCode, PasskeyError};

/// Checks a PIN. Called on a blocking thread; may take seconds (PAM fail delay).
pub trait PinVerifier: Send + Sync + 'static {
    /// `Ok(false)` for a wrong PIN.
    ///
    /// # Errors
    /// When the check itself cannot run (PAM misconfigured, account disabled).
    fn verify(&self, pin: &str) -> Result<bool, PasskeyError>;
}

/// Whether a lock screen is showing now.
pub trait ScreenLock: Send + Sync + 'static {
    fn locked(&self) -> bool;
}

pub struct PamPin {
    service: String,
    user: String,
}

impl PamPin {
    /// Checks PINs for the user that runs the sidecar.
    ///
    /// # Errors
    /// When the user name cannot be found.
    pub fn current_user(service: &str) -> Result<Self, PasskeyError> {
        let user = current_user_name().ok_or_else(|| {
            PasskeyError::new(ErrorCode::NotSupported, "could not find the user name")
        })?;
        Ok(Self {
            service: service.to_owned(),
            user,
        })
    }
}

/// The name of this process's user: `/etc/passwd` by uid, else `$USER`.
fn current_user_name() -> Option<String> {
    let uid = std::fs::metadata("/proc/self").ok()?.uid();
    let by_uid = std::fs::read_to_string("/etc/passwd")
        .ok()
        .and_then(|passwd| {
            passwd.lines().find_map(|line| {
                let mut fields = line.split(':');
                let name = fields.next()?;
                let entry_uid = fields.nth(1)?.parse::<u32>().ok()?;
                (entry_uid == uid).then(|| name.to_owned())
            })
        });
    by_uid.or_else(|| std::env::var("USER").ok().filter(|name| !name.is_empty()))
}

struct PinConversation {
    user: String,
    pin: Zeroizing<String>,
}

impl ConversationHandler for PinConversation {
    fn prompt_echo_on(&mut self, _msg: &CStr) -> Result<CString, PamCode> {
        CString::new(self.user.clone()).map_err(|_| PamCode::CONV_ERR)
    }

    fn prompt_echo_off(&mut self, _msg: &CStr) -> Result<CString, PamCode> {
        CString::new(self.pin.as_bytes()).map_err(|_| PamCode::CONV_ERR)
    }

    fn text_info(&mut self, _msg: &CStr) {}

    fn error_msg(&mut self, msg: &CStr) {
        tracing::debug!(message = %msg.to_string_lossy(), "PAM");
    }
}

impl PinVerifier for PamPin {
    fn verify(&self, pin: &str) -> Result<bool, PasskeyError> {
        let conversation = PinConversation {
            user: self.user.clone(),
            pin: Zeroizing::new(pin.to_owned()),
        };
        let mut context = pam_client2::Context::new(&self.service, Some(&self.user), conversation)
            .map_err(|error| {
                PasskeyError::new(
                    ErrorCode::NotSupported,
                    format!("PAM is not ready: {error}"),
                )
            })?;
        match context.authenticate(Flag::DISALLOW_NULL_AUTHTOK) {
            Ok(()) => {}
            Err(error)
                if matches!(
                    error.code(),
                    PamCode::AUTH_ERR | PamCode::MAXTRIES | PamCode::CRED_ERR
                ) =>
            {
                return Ok(false)
            }
            Err(error) => {
                return Err(PasskeyError::new(
                    ErrorCode::NotAllowed,
                    format!("the PIN could not be checked: {error}"),
                ))
            }
        }
        context.acct_mgmt(Flag::NONE).map_err(|error| {
            PasskeyError::new(
                ErrorCode::NotAllowed,
                format!("the account is not allowed to sign in: {error}"),
            )
        })?;
        Ok(true)
    }
}

/// Sees the Intent OS lock screens (hyprlock, or swaylock as the fallback) as
/// processes of this user.
pub struct LockScreenProcesses;

const LOCKERS: [&str; 2] = ["hyprlock", "swaylock"];

impl ScreenLock for LockScreenProcesses {
    fn locked(&self) -> bool {
        let Ok(uid) = std::fs::metadata("/proc/self").map(|meta| meta.uid()) else {
            return false;
        };
        let Ok(entries) = std::fs::read_dir("/proc") else {
            return false;
        };
        entries.flatten().any(|entry| {
            let path = entry.path();
            entry
                .file_name()
                .to_string_lossy()
                .bytes()
                .all(|b| b.is_ascii_digit())
                && std::fs::metadata(&path).is_ok_and(|meta| meta.uid() == uid)
                && std::fs::read_to_string(path.join("comm"))
                    .is_ok_and(|comm| LOCKERS.contains(&comm.trim_end()))
        })
    }
}
