use std::process::ExitCode;

use tracing_subscriber::EnvFilter;

#[cfg(target_os = "linux")]
async fn run() -> std::io::Result<()> {
    use intent_passkey::device::{
        Device, DeviceConfig, LockScreenProcesses, PamPin, WithDevice, PAM_SERVICE,
    };
    let device = DeviceConfig::system().and_then(|config| {
        PamPin::current_user(PAM_SERVICE)
            .map(|pin| Device::new(config, pin, LockScreenProcesses))
            .map_err(|error| tracing::warn!(%error, "device passkeys are off"))
            .ok()
    });
    let backend = WithDevice {
        other: intent_passkey::webauthn::LibWebAuthn::default(),
        device,
    };
    intent_passkey::server::serve(backend, tokio::io::stdin(), tokio::io::stdout()).await
}

#[cfg(not(target_os = "linux"))]
async fn run() -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "intent-passkey supports Linux only",
    ))
}

#[tokio::main]
async fn main() -> ExitCode {
    if std::env::args().nth(1).as_deref() != Some("--stdio") {
        eprintln!("usage: intent-passkey --stdio");
        return ExitCode::from(2);
    }
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_env_filter(
            EnvFilter::try_from_env("INTENT_PASSKEY_LOG")
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "intent-passkey stopped");
            ExitCode::FAILURE
        }
    }
}
