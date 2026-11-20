//! The seam between the protocol server and an authenticator implementation, so a
//! later libwebauthn version (or the credentialsd portal) can replace the backend.

use std::future::Future;

use tokio::sync::{mpsc, watch};
use zeroize::Zeroizing;

use crate::protocol::{
    Account, BluetoothState, Bytes, CreateParams, CreateResult, DeviceCredential, ErrorCode, Event,
    GetParams, GetResult, PasskeyError, PinKind, Status,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    Cancelled,
    TimedOut,
}

impl StopReason {
    #[must_use]
    pub fn error(self) -> PasskeyError {
        match self {
            Self::Cancelled => PasskeyError::new(ErrorCode::Abort, "the ceremony was cancelled"),
            Self::TimedOut => PasskeyError::new(ErrorCode::Timeout, "the ceremony timed out"),
        }
    }
}

/// What a running ceremony may do: emit events, ask main for a PIN or an account
/// choice, and notice that main cancelled it or that it timed out.
pub struct CeremonyIo {
    events: mpsc::UnboundedSender<Event>,
    pins: mpsc::Receiver<Zeroizing<String>>,
    choices: mpsc::Receiver<usize>,
    stop: watch::Receiver<Option<StopReason>>,
}

/// See [`CeremonyIo::stop_signal`].
#[derive(Clone)]
pub struct StopSignal(watch::Receiver<Option<StopReason>>);

impl StopSignal {
    /// # Errors
    /// The stop reason's error once the ceremony must stop.
    pub fn check(&self) -> Result<(), PasskeyError> {
        match *self.0.borrow() {
            Some(reason) => Err(reason.error()),
            None => Ok(()),
        }
    }
}

/// The server's handles to a running ceremony's [`CeremonyIo`].
pub struct CeremonyControl {
    pub pins: mpsc::Sender<Zeroizing<String>>,
    pub choices: mpsc::Sender<usize>,
    pub stop: watch::Sender<Option<StopReason>>,
}

impl CeremonyIo {
    #[must_use]
    pub fn new(events: mpsc::UnboundedSender<Event>) -> (Self, CeremonyControl) {
        let (pin_tx, pins) = mpsc::channel(1);
        let (choice_tx, choices) = mpsc::channel(4);
        let (stop_tx, stop) = watch::channel(None);
        (
            Self {
                events,
                pins,
                choices,
                stop,
            },
            CeremonyControl {
                pins: pin_tx,
                choices: choice_tx,
                stop: stop_tx,
            },
        )
    }

    pub fn emit(&self, event: Event) {
        let _ = self.events.send(event);
    }

    pub fn status(&self, state: Status) {
        self.emit(Event::Status { state });
    }

    /// A clone of the event sender, for forwarding tasks that outlive a borrow of `self`.
    #[must_use]
    pub fn event_sender(&self) -> mpsc::UnboundedSender<Event> {
        self.events.clone()
    }

    /// The stop reason's error once main cancelled the ceremony or it timed out;
    /// for checks after blocking work and before committing anything.
    ///
    /// # Errors
    /// When the ceremony must stop.
    pub fn ensure_running(&self) -> Result<(), PasskeyError> {
        self.stop_signal().check()
    }

    /// A handle that checks for a stop from another task or a blocking thread.
    #[must_use]
    pub fn stop_signal(&self) -> StopSignal {
        StopSignal(self.stop.clone())
    }

    /// Resolves when the ceremony must stop.
    pub async fn stopped(&mut self) -> StopReason {
        loop {
            if let Some(reason) = *self.stop.borrow_and_update() {
                return reason;
            }
            if self.stop.changed().await.is_err() {
                return StopReason::Cancelled;
            }
        }
    }

    /// Emits `pin-needed` and waits for the PIN (or for the ceremony to stop).
    ///
    /// # Errors
    /// The stop reason's error when the ceremony is cancelled or times out first.
    pub async fn request_pin(
        &mut self,
        kind: PinKind,
        attempts_left: Option<u32>,
        wrong_pin: bool,
    ) -> Result<Zeroizing<String>, PasskeyError> {
        self.ask_pin(Event::PinNeeded {
            kind,
            attempts_left,
            retry_after_ms: None,
            wrong_pin,
            old_pin: false,
        })
        .await
    }

    /// Emits `prompt` (a `pin-needed` event) and waits for the PIN (or for the
    /// ceremony to stop).
    ///
    /// # Errors
    /// The stop reason's error when the ceremony is cancelled or times out first.
    pub async fn ask_pin(&mut self, prompt: Event) -> Result<Zeroizing<String>, PasskeyError> {
        while self.pins.try_recv().is_ok() {}
        self.emit(prompt);
        let mut stop = self.stop.clone();
        tokio::select! {
            pin = self.pins.recv() => pin.ok_or_else(|| StopReason::Cancelled.error()),
            reason = wait_stop(&mut stop) => Err(reason.error()),
        }
    }

    /// Emits `choose-account` and waits for a valid index.
    ///
    /// # Errors
    /// The stop reason's error when the ceremony is cancelled or times out first.
    pub async fn choose_account(&mut self, accounts: Vec<Account>) -> Result<usize, PasskeyError> {
        while self.choices.try_recv().is_ok() {}
        let count = accounts.len();
        self.emit(Event::ChooseAccount { accounts });
        let mut stop = self.stop.clone();
        loop {
            tokio::select! {
                choice = self.choices.recv() => match choice {
                    Some(index) if index < count => return Ok(index),
                    Some(_) => {}
                    None => return Err(StopReason::Cancelled.error()),
                },
                reason = wait_stop(&mut stop) => return Err(reason.error()),
            }
        }
    }
}

async fn wait_stop(stop: &mut watch::Receiver<Option<StopReason>>) -> StopReason {
    loop {
        if let Some(reason) = *stop.borrow_and_update() {
            return reason;
        }
        if stop.changed().await.is_err() {
            return StopReason::Cancelled;
        }
    }
}

/// A source of passkeys. Implementations must return promptly once
/// [`CeremonyIo::stopped`] resolves.
pub trait Authenticator: Send + Sync + 'static {
    fn bluetooth(&self) -> impl Future<Output = BluetoothState> + Send;

    fn usb_supported(&self) -> bool;

    fn create(
        &self,
        params: CreateParams,
        io: CeremonyIo,
    ) -> impl Future<Output = Result<CreateResult, PasskeyError>> + Send;

    fn get(
        &self,
        params: GetParams,
        io: CeremonyIo,
    ) -> impl Future<Output = Result<GetResult, PasskeyError>> + Send;

    /// Whether device passkeys are set up on this system (`hello.transports.device`).
    fn device_supported(&self) -> bool {
        false
    }

    fn device_list(
        &self,
    ) -> impl Future<Output = Result<Vec<DeviceCredential>, PasskeyError>> + Send {
        async { Err(device_unsupported()) }
    }

    fn device_delete(
        &self,
        credential_id: Bytes,
    ) -> impl Future<Output = Result<(), PasskeyError>> + Send {
        let _ = credential_id;
        async { Err(device_unsupported()) }
    }
}

#[must_use]
pub fn device_unsupported() -> PasskeyError {
    PasskeyError::new(
        ErrorCode::NotSupported,
        "device passkeys are not set up on this system",
    )
}
