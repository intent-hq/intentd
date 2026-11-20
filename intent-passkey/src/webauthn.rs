//! The libwebauthn backend: phone passkeys over hybrid (caBLE v2 QR) and USB HID
//! security keys with PIN. All libwebauthn types stay inside this module.

use std::time::Duration;

use libwebauthn::ops::webauthn::{
    Assertion, GetAssertionRequest, MakeCredentialRequest, OriginValidation, PublicSuffixList,
    RelatedOrigins, RequestOrigin, RequestSettings, WebAuthnIDLResponse as _,
};
use libwebauthn::proto::CtapError;
use libwebauthn::transport::hid::{list_devices, HidDevice};
use libwebauthn::transport::hybrid::channel::{HybridUxUpdate, PxpUpdate};
use libwebauthn::transport::hybrid::error::PxpError;
use libwebauthn::transport::hybrid::qr_code_device::{
    PxpQrCodeDevice, PxpTransports, QrCodeOperationHint,
};
use libwebauthn::transport::{Channel, ChannelSettings, Device as _};
use libwebauthn::webauthn::{PlatformError, WebAuthn, WebAuthnError};
use libwebauthn::UvUpdate;
use serde_json::{json, Value};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, warn};

use crate::authenticator::{device_unsupported, Authenticator, CeremonyIo};
use crate::protocol::{
    Account, BluetoothState, Bytes, CreateExtensionOutputs, CreateParams, CreateResult, CredProps,
    CredentialDescriptor, ErrorCode, GetParams, GetResult, PasskeyError, PinKind, Status,
    Transport,
};

const KNOWN_TRANSPORTS: [&str; 5] = ["usb", "nfc", "ble", "hybrid", "internal"];
const DEVICE_POLL: Duration = Duration::from_millis(500);
const SELECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// libwebauthn's RP ID validation backed by the compiled-in public suffix list, so
/// the sidecar does not depend on a distro `publicsuffix-list` package.
struct EmbeddedPsl;

impl PublicSuffixList for EmbeddedPsl {
    fn public_suffix(&self, host: &str) -> Option<String> {
        psl::suffix_str(host).map(str::to_owned)
    }
}

enum UsbSource {
    System,
    Fixed(Vec<HidDevice>),
}

pub struct LibWebAuthn {
    usb: UsbSource,
}

impl Default for LibWebAuthn {
    fn default() -> Self {
        Self {
            usb: UsbSource::System,
        }
    }
}

impl LibWebAuthn {
    /// Uses `devices` instead of the system's HID keys (tests use libwebauthn's virtual device).
    #[must_use]
    pub fn with_usb_devices(devices: Vec<HidDevice>) -> Self {
        Self {
            usb: UsbSource::Fixed(devices),
        }
    }

    async fn list_usb(&self) -> Vec<HidDevice> {
        match &self.usb {
            UsbSource::Fixed(devices) => devices.clone(),
            UsbSource::System => list_devices().await.unwrap_or_else(|error| {
                warn!(%error, "listing security keys failed");
                Vec::new()
            }),
        }
    }

    /// Waits for a security key. With several keys, all of them blink and the one
    /// the user touches is used (as browsers do); the others are cancelled.
    async fn select_usb_device(&self, io: &mut CeremonyIo) -> Result<HidDevice, PasskeyError> {
        io.status(Status::TouchKey);
        loop {
            let mut devices = self.list_usb().await;
            let chosen = match devices.len() {
                0 => None,
                1 => devices.pop(),
                _ => {
                    tokio::select! {
                        chosen = touched_device(devices) => chosen,
                        reason = io.stopped() => return Err(reason.error()),
                    }
                }
            };
            if let Some(device) = chosen {
                return Ok(device);
            }
            tokio::select! {
                () = tokio::time::sleep(DEVICE_POLL) => {}
                reason = io.stopped() => return Err(reason.error()),
            }
        }
    }
}

/// Blinks every key and returns the first one touched, or `None` when no key was.
async fn touched_device(devices: Vec<HidDevice>) -> Option<HidDevice> {
    let (handle_tx, mut handle_rx) = mpsc::unbounded_channel();
    let mut tasks = tokio::task::JoinSet::new();
    for (index, device) in devices.iter().cloned().enumerate() {
        let handle_tx = handle_tx.clone();
        tasks.spawn(async move {
            let mut device = device;
            let Ok(mut channel) = device.channel(ChannelSettings::default()).await else {
                return None;
            };
            let _ = handle_tx.send(channel.get_handle());
            match channel
                .blink_and_wait_for_user_presence(SELECTION_TIMEOUT)
                .await
            {
                Ok(true) => Some(index),
                Ok(false) | Err(_) => None,
            }
        });
    }
    drop(handle_tx);
    let mut chosen = None;
    while let Some(result) = tasks.join_next().await {
        if let Ok(Some(index)) = result {
            chosen = Some(index);
            break;
        }
    }
    while let Ok(handle) = handle_rx.try_recv() {
        handle.cancel_ongoing_operation().await;
    }
    while tasks.join_next().await.is_some() {}
    chosen.and_then(|index| devices.into_iter().nth(index))
}

fn error(code: ErrorCode, message: impl Into<String>) -> PasskeyError {
    PasskeyError::new(code, message)
}

fn map_ctap(ctap: CtapError) -> PasskeyError {
    match ctap {
        CtapError::CredentialExcluded => error(
            ErrorCode::InvalidState,
            "this security key already has a passkey for this account",
        ),
        CtapError::NoCredentials | CtapError::InvalidCredential => error(
            ErrorCode::NotAllowed,
            "no passkey for this site on the security key",
        ),
        CtapError::PINBlocked | CtapError::PINAuthBlocked | CtapError::UvBlocked => error(
            ErrorCode::PinLocked,
            "the security key is locked after too many wrong PINs",
        ),
        CtapError::PINNotSet | CtapError::PINRequired => error(
            ErrorCode::NotAllowed,
            "the security key needs a PIN; set one with the key's tools first",
        ),
        CtapError::UserActionTimeout | CtapError::ActionTimeout | CtapError::Timeout => {
            error(ErrorCode::Timeout, "the security key timed out")
        }
        CtapError::UnsupportedAlgorithm
        | CtapError::UnsupportedOption
        | CtapError::UnsupportedExtension => error(
            ErrorCode::NotSupported,
            format!("the authenticator does not support this request ({ctap})"),
        ),
        CtapError::KeepAliveCancel => error(ErrorCode::Abort, "the request was cancelled"),
        other => error(
            ErrorCode::NotAllowed,
            format!("the authenticator refused the request ({other})"),
        ),
    }
}

fn map_platform(platform: &PlatformError) -> PasskeyError {
    match platform {
        PlatformError::Cancelled => error(ErrorCode::Abort, "the request was cancelled"),
        PlatformError::NotSupported
        | PlatformError::PinNotSupported
        | PlatformError::NoUvAvailable => error(ErrorCode::NotSupported, platform.to_string()),
        PlatformError::PinTooShort | PlatformError::PinTooLong => {
            error(ErrorCode::NotAllowed, platform.to_string())
        }
        other => error(ErrorCode::Unknown, other.to_string()),
    }
}

fn map_webauthn<E: std::fmt::Display>(
    err: WebAuthnError<E>,
    transport: impl FnOnce(E) -> PasskeyError,
) -> PasskeyError {
    match err {
        WebAuthnError::Ctap(ctap) => map_ctap(ctap),
        WebAuthnError::Platform(platform) => map_platform(&platform),
        WebAuthnError::Transport(inner) => transport(inner),
        other => error(ErrorCode::Unknown, other.to_string()),
    }
}

fn map_pxp(err: &PxpError) -> PasskeyError {
    match err {
        PxpError::TransportUnavailable => error(
            ErrorCode::BluetoothUnavailable,
            "Bluetooth is not available for phone sign-in",
        ),
        PxpError::Timeout => error(ErrorCode::Timeout, "the phone did not answer in time"),
        other => error(
            ErrorCode::NotAllowed,
            format!("could not reach the phone ({other})"),
        ),
    }
}

fn map_hid(err: &impl std::fmt::Display) -> PasskeyError {
    error(
        ErrorCode::NotAllowed,
        format!("could not talk to the security key ({err})"),
    )
}

fn is_wrong_pin<E>(result: &Result<impl Sized, WebAuthnError<E>>) -> bool {
    matches!(
        result,
        Err(WebAuthnError::Ctap(
            CtapError::PINInvalid | CtapError::UVInvalid
        ))
    )
}

fn is_retryable<E>(result: &Result<impl Sized, WebAuthnError<E>>) -> bool {
    matches!(
        result,
        Err(WebAuthnError::Ctap(CtapError::UserActionTimeout))
    )
}

fn descriptors(list: &[CredentialDescriptor]) -> Vec<Value> {
    list.iter()
        .map(|descriptor| {
            let mut value = json!({ "type": "public-key", "id": descriptor.id.encode() });
            if let Some(transports) = &descriptor.transports {
                let known: Vec<&str> = transports
                    .iter()
                    .map(String::as_str)
                    .filter(|transport| KNOWN_TRANSPORTS.contains(transport))
                    .collect();
                value["transports"] = json!(known);
            }
            value
        })
        .collect()
}

fn timeout_ms(requested: Option<u64>) -> u64 {
    requested.filter(|ms| *ms > 0).unwrap_or(300_000)
}

fn creation_options(params: &CreateParams) -> Value {
    let selection = &params.authenticator_selection;
    let mut authenticator_selection = json!({});
    if let Some(resident_key) = &selection.resident_key {
        authenticator_selection["residentKey"] = json!(resident_key);
    }
    if let Some(require) = selection.require_resident_key {
        authenticator_selection["requireResidentKey"] = json!(require);
    }
    if let Some(uv) = &selection.user_verification {
        authenticator_selection["userVerification"] = json!(uv);
    }
    if let Some(attachment) = &selection.authenticator_attachment {
        authenticator_selection["authenticatorAttachment"] = json!(attachment);
    }
    let mut extensions = json!({});
    if params.extensions.cred_props == Some(true) {
        extensions["credProps"] = json!(true);
    }
    json!({
        "rp": { "id": params.rp_id, "name": params.rp.name },
        "user": {
            "id": params.user.id.encode(),
            "name": params.user.name,
            "displayName": params.user.display_name,
        },
        "challenge": params.challenge.encode(),
        "pubKeyCredParams": params.pub_key_cred_params,
        "timeout": timeout_ms(params.timeout_ms),
        "excludeCredentials": descriptors(&params.exclude_credentials),
        "authenticatorSelection": authenticator_selection,
        "attestation": params.attestation.as_deref().unwrap_or("none"),
        "extensions": extensions,
    })
}

fn request_options(params: &GetParams) -> Value {
    json!({
        "challenge": params.challenge.encode(),
        "timeout": timeout_ms(params.timeout_ms),
        "rpId": params.rp_id,
        "allowCredentials": descriptors(&params.allow_credentials),
        "userVerification": params.user_verification.as_deref().unwrap_or("preferred"),
    })
}

fn request_origin(origin: &str) -> Result<RequestOrigin, PasskeyError> {
    RequestOrigin::try_from(origin)
        .map_err(|err| error(ErrorCode::Security, format!("origin rejected: {err}")))
}

async fn prepare_create(params: &CreateParams) -> Result<MakeCredentialRequest, PasskeyError> {
    let origin = request_origin(&params.origin)?;
    let psl = EmbeddedPsl;
    let settings = RequestSettings {
        origin: OriginValidation::Validate {
            public_suffix_list: &psl,
            related_origins: RelatedOrigins::Disabled,
        },
    };
    MakeCredentialRequest::prepare(&origin, &creation_options(params).to_string(), &settings)
        .await
        .map_err(|err| {
            use libwebauthn::ops::webauthn::MakeCredentialPrepareError as E;
            match err {
                E::EncodingError(err) => error(
                    ErrorCode::NotSupported,
                    format!("unsupported options: {err}"),
                ),
                other => error(ErrorCode::Security, other.to_string()),
            }
        })
}

async fn prepare_get(params: &GetParams) -> Result<GetAssertionRequest, PasskeyError> {
    let origin = request_origin(&params.origin)?;
    let psl = EmbeddedPsl;
    let settings = RequestSettings {
        origin: OriginValidation::Validate {
            public_suffix_list: &psl,
            related_origins: RelatedOrigins::Disabled,
        },
    };
    GetAssertionRequest::prepare(&origin, &request_options(params).to_string(), &settings)
        .await
        .map_err(|err| {
            use libwebauthn::ops::webauthn::GetAssertionPrepareError as E;
            match err {
                E::EncodingError(err) => error(
                    ErrorCode::NotSupported,
                    format!("unsupported options: {err}"),
                ),
                E::NotSupported(message) => error(ErrorCode::NotSupported, message),
                other => error(ErrorCode::Security, other.to_string()),
            }
        })
}

/// The `clientDataJSON` bytes for a `create`, built the same way for every
/// transport (libwebauthn's builder, after its origin and RP ID checks).
pub(crate) async fn create_client_data(params: &CreateParams) -> Result<String, PasskeyError> {
    Ok(prepare_create(params).await?.client_data_json())
}

/// The `clientDataJSON` bytes for a `get`; see [`create_client_data`].
pub(crate) async fn get_client_data(params: &GetParams) -> Result<String, PasskeyError> {
    Ok(prepare_get(params).await?.client_data_json())
}

/// Turns a channel UX update into events; answers PIN requests through `io`.
/// Returns only when the ceremony must stop.
async fn drive_uv(
    rx: &mut broadcast::Receiver<UvUpdate>,
    io: &mut CeremonyIo,
    wrong_pin: bool,
) -> PasskeyError {
    let mut wrong_pin = wrong_pin;
    loop {
        let update = tokio::select! {
            update = rx.recv() => update,
            reason = io.stopped() => return reason.error(),
        };
        match update {
            Ok(UvUpdate::PresenceRequired | UvUpdate::UvRetry { .. }) => {
                io.status(Status::TouchKey);
            }
            Ok(UvUpdate::PinRequired(request)) => {
                match io
                    .request_pin(PinKind::Usb, request.attempts_left, wrong_pin)
                    .await
                {
                    Ok(pin) => {
                        wrong_pin = true;
                        if request.send_pin(&pin).is_err() {
                            warn!("the security key stopped waiting for the PIN");
                        }
                        io.status(Status::TouchKey);
                    }
                    Err(stop) => {
                        request.cancel();
                        return stop;
                    }
                }
            }
            Ok(UvUpdate::PinNotSet(request)) => {
                request.cancel();
                debug!("the security key has no PIN; not setting one from the sidecar");
            }
            Err(broadcast::error::RecvError::Lagged(_)) => {}
            Err(broadcast::error::RecvError::Closed) => {
                let reason = io.stopped().await;
                return reason.error();
            }
        }
    }
}

async fn drive_hybrid(
    rx: &mut broadcast::Receiver<HybridUxUpdate>,
    io: &mut CeremonyIo,
) -> PasskeyError {
    loop {
        let update = tokio::select! {
            update = rx.recv() => update,
            reason = io.stopped() => return reason.error(),
        };
        match update {
            Ok(HybridUxUpdate::PxpUpdate(PxpUpdate::ProximityCheck)) => {
                io.status(Status::WaitingForPhone);
            }
            Ok(HybridUxUpdate::PxpUpdate(PxpUpdate::Connecting | PxpUpdate::Authenticating)) => {
                io.status(Status::Connecting);
            }
            Ok(HybridUxUpdate::PxpUpdate(PxpUpdate::Connected)) => {
                io.status(Status::WaitingForApproval);
            }
            Ok(HybridUxUpdate::PxpUpdate(PxpUpdate::Error(err))) => {
                debug!(%err, "hybrid connection update");
            }
            Ok(HybridUxUpdate::UvUpdate(UvUpdate::PinRequired(request))) => request.cancel(),
            Ok(HybridUxUpdate::UvUpdate(UvUpdate::PinNotSet(request))) => request.cancel(),
            Ok(HybridUxUpdate::UvUpdate(_)) | Err(broadcast::error::RecvError::Lagged(_)) => {}
            Err(broadcast::error::RecvError::Closed) => {
                let reason = io.stopped().await;
                return reason.error();
            }
        }
    }
}

fn create_result(
    response: &libwebauthn::ops::webauthn::MakeCredentialResponse,
    request: &MakeCredentialRequest,
    params: &CreateParams,
    default_transport: &str,
) -> Result<CreateResult, PasskeyError> {
    let model = response.to_idl_model(request).map_err(|err| {
        error(
            ErrorCode::Unknown,
            format!("could not encode the response: {err}"),
        )
    })?;
    let mut transports = model.response.transports;
    if transports.is_empty() {
        transports.push(default_transport.to_owned());
    }
    let cred_props = (params.extensions.cred_props == Some(true)).then(|| CredProps {
        rk: model
            .client_extension_results
            .cred_props
            .and_then(|props| props.rk),
    });
    Ok(CreateResult {
        credential_id: Bytes(model.raw_id.0),
        client_data_json: Bytes(model.response.client_data_json.0),
        attestation_object: Bytes(model.response.attestation_object.0),
        authenticator_data: Bytes(model.response.authenticator_data.0),
        public_key: model.response.public_key.map(|key| Bytes(key.0)),
        public_key_algorithm: model.response.public_key_algorithm,
        transports,
        authenticator_attachment: "cross-platform".to_owned(),
        extensions: CreateExtensionOutputs { cred_props },
    })
}

fn get_result(
    assertion: &Assertion,
    request: &GetAssertionRequest,
) -> Result<GetResult, PasskeyError> {
    let model = assertion.to_idl_model(request).map_err(|err| {
        error(
            ErrorCode::Unknown,
            format!("could not encode the response: {err}"),
        )
    })?;
    Ok(GetResult {
        credential_id: Bytes(model.raw_id.0),
        client_data_json: Bytes(model.response.client_data_json.0),
        authenticator_data: Bytes(model.response.authenticator_data.0),
        signature: Bytes(model.response.signature.0),
        user_handle: model.response.user_handle.map(|handle| Bytes(handle.0)),
        authenticator_attachment: "cross-platform".to_owned(),
    })
}

async fn pick_assertion<'a>(
    assertions: &'a [Assertion],
    io: &mut CeremonyIo,
) -> Result<&'a Assertion, PasskeyError> {
    match assertions {
        [] => Err(error(ErrorCode::NotAllowed, "no passkey for this site")),
        [only] => Ok(only),
        many => {
            let accounts = many
                .iter()
                .enumerate()
                .map(|(index, assertion)| {
                    let user = assertion.user.as_ref();
                    Account {
                        index,
                        user_name: user.and_then(|user| user.name.clone()).unwrap_or_default(),
                        user_display_name: user
                            .and_then(|user| user.display_name.clone())
                            .unwrap_or_default(),
                    }
                })
                .collect();
            let index = io.choose_account(accounts).await?;
            many.get(index)
                .ok_or_else(|| error(ErrorCode::NotAllowed, "invalid account choice"))
        }
    }
}

/// Bluetooth adapter state from `BlueZ` over the system bus.
fn bluez_state() -> BluetoothState {
    use dbus::blocking::stdintf::org_freedesktop_dbus::ObjectManager as _;
    let Ok(connection) = dbus::blocking::Connection::new_system() else {
        return BluetoothState::Missing;
    };
    let proxy = connection.with_proxy("org.bluez", "/", Duration::from_secs(2));
    let Ok(objects) = proxy.get_managed_objects() else {
        return BluetoothState::Missing;
    };
    let mut state = BluetoothState::Missing;
    for interfaces in objects.values() {
        if let Some(adapter) = interfaces.get("org.bluez.Adapter1") {
            let powered = dbus::arg::prop_cast::<bool>(adapter, "Powered").copied();
            if powered == Some(true) {
                return BluetoothState::On;
            }
            state = BluetoothState::Off;
        }
    }
    state
}

async fn bluetooth_state() -> BluetoothState {
    tokio::task::spawn_blocking(bluez_state)
        .await
        .unwrap_or(BluetoothState::Missing)
}

async fn require_bluetooth() -> Result<(), PasskeyError> {
    match bluetooth_state().await {
        BluetoothState::On => Ok(()),
        BluetoothState::Off => Err(error(ErrorCode::BluetoothUnavailable, "Bluetooth is off")),
        BluetoothState::Missing => Err(error(
            ErrorCode::BluetoothUnavailable,
            "this computer has no Bluetooth adapter",
        )),
    }
}

impl LibWebAuthn {
    async fn create_usb(
        &self,
        params: &CreateParams,
        mut io: CeremonyIo,
    ) -> Result<CreateResult, PasskeyError> {
        let request = prepare_create(params).await?;
        let mut device = self.select_usb_device(&mut io).await?;
        let mut channel = device
            .channel(ChannelSettings::default())
            .await
            .map_err(|err| map_webauthn(err, |err| map_hid(&err)))?;
        let handle = channel.get_handle();
        let mut rx = channel.get_ux_update_receiver();
        let mut wrong_pin = false;
        loop {
            let outcome = tokio::select! {
                result = channel.webauthn_make_credential(&request) => Ok(result),
                stop = drive_uv(&mut rx, &mut io, wrong_pin) => Err(stop),
            };
            let result = match outcome {
                Ok(result) => result,
                Err(stop) => {
                    handle.cancel_ongoing_operation().await;
                    return Err(stop);
                }
            };
            if is_wrong_pin(&result) {
                wrong_pin = true;
                continue;
            }
            if is_retryable(&result) {
                continue;
            }
            io.status(Status::Processing);
            let response = result.map_err(|err| map_webauthn(err, |err| map_hid(&err)))?;
            return create_result(&response, &request, params, "usb");
        }
    }

    async fn get_usb(
        &self,
        params: &GetParams,
        mut io: CeremonyIo,
    ) -> Result<GetResult, PasskeyError> {
        let request = prepare_get(params).await?;
        let mut device = self.select_usb_device(&mut io).await?;
        let mut channel = device
            .channel(ChannelSettings::default())
            .await
            .map_err(|err| map_webauthn(err, |err| map_hid(&err)))?;
        let handle = channel.get_handle();
        let mut rx = channel.get_ux_update_receiver();
        let mut wrong_pin = false;
        let response = loop {
            let outcome = tokio::select! {
                result = channel.webauthn_get_assertion(&request) => Ok(result),
                stop = drive_uv(&mut rx, &mut io, wrong_pin) => Err(stop),
            };
            let result = match outcome {
                Ok(result) => result,
                Err(stop) => {
                    handle.cancel_ongoing_operation().await;
                    return Err(stop);
                }
            };
            if is_wrong_pin(&result) {
                wrong_pin = true;
                continue;
            }
            if is_retryable(&result) {
                continue;
            }
            break result.map_err(|err| map_webauthn(err, |err| map_hid(&err)))?;
        };
        let assertion = pick_assertion(&response.assertions, &mut io).await?;
        io.status(Status::Processing);
        get_result(assertion, &request)
    }

    async fn hybrid_channel(
        hint: QrCodeOperationHint,
        io: &CeremonyIo,
    ) -> Result<libwebauthn::transport::hybrid::channel::HybridChannel, PasskeyError> {
        require_bluetooth().await?;
        let mut device = PxpQrCodeDevice::new_transient(hint, PxpTransports::CloudAssistedOrLocal)
            .map_err(|err| map_pxp(&err))?;
        io.emit(crate::protocol::Event::Qr {
            data: device.qr_code.to_string(),
        });
        io.status(Status::WaitingForPhone);
        device
            .channel(ChannelSettings::default())
            .await
            .map_err(|err| map_webauthn(err, |err| map_pxp(&err)))
    }

    async fn create_hybrid(
        params: &CreateParams,
        mut io: CeremonyIo,
    ) -> Result<CreateResult, PasskeyError> {
        let request = prepare_create(params).await?;
        let mut channel = Self::hybrid_channel(QrCodeOperationHint::MakeCredential, &io).await?;
        let mut rx = channel.get_ux_update_receiver();
        let outcome = tokio::select! {
            result = channel.webauthn_make_credential(&request) => Ok(result),
            stop = drive_hybrid(&mut rx, &mut io) => Err(stop),
        };
        channel.close().await;
        let response = outcome?.map_err(|err| map_webauthn(err, |err| map_pxp(&err)))?;
        io.status(Status::Processing);
        create_result(&response, &request, params, "hybrid")
    }

    async fn get_hybrid(params: &GetParams, mut io: CeremonyIo) -> Result<GetResult, PasskeyError> {
        let request = prepare_get(params).await?;
        let mut channel =
            Self::hybrid_channel(QrCodeOperationHint::GetAssertionRequest, &io).await?;
        let mut rx = channel.get_ux_update_receiver();
        let outcome = tokio::select! {
            result = channel.webauthn_get_assertion(&request) => Ok(result),
            stop = drive_hybrid(&mut rx, &mut io) => Err(stop),
        };
        channel.close().await;
        let response = outcome?.map_err(|err| map_webauthn(err, |err| map_pxp(&err)))?;
        let assertion = pick_assertion(&response.assertions, &mut io).await?;
        io.status(Status::Processing);
        get_result(assertion, &request)
    }
}

impl Authenticator for LibWebAuthn {
    async fn bluetooth(&self) -> BluetoothState {
        bluetooth_state().await
    }

    fn usb_supported(&self) -> bool {
        true
    }

    async fn create(
        &self,
        params: CreateParams,
        io: CeremonyIo,
    ) -> Result<CreateResult, PasskeyError> {
        match params.transport {
            Transport::Usb => self.create_usb(&params, io).await,
            Transport::Hybrid => Self::create_hybrid(&params, io).await,
            Transport::Device => Err(device_unsupported()),
        }
    }

    async fn get(&self, params: GetParams, io: CeremonyIo) -> Result<GetResult, PasskeyError> {
        match params.transport {
            Transport::Usb => self.get_usb(&params, io).await,
            Transport::Hybrid => Self::get_hybrid(&params, io).await,
            Transport::Device => Err(device_unsupported()),
        }
    }
}
