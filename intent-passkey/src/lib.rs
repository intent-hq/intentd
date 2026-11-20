//! `intent-passkey`: the `WebAuthn` sidecar for the Intent browser panel. Electron main
//! starts `intent-passkey --stdio` and speaks JSON lines with it; see `protocol`.

pub mod authenticator;
#[cfg(target_os = "linux")]
pub mod device;
pub mod protocol;
pub mod rpid;
pub mod server;
#[cfg(target_os = "linux")]
pub mod webauthn;
