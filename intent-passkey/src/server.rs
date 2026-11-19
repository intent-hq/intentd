//! The stdio JSON-lines server: request dispatch, the one-ceremony rule, events,
//! cancel / pin / choose routing and ceremony timeouts.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::mpsc;
use tracing::{info, warn};
use zeroize::Zeroize;

use crate::authenticator::{
    device_unsupported, Authenticator, CeremonyControl, CeremonyIo, StopReason,
};
use crate::protocol::{
    BluetoothState, CeremonyRef, CeremonyScope, ChooseParams, CreateParams, DeviceDeleteParams,
    ErrorCode, GetParams, HelloResult, PasskeyError, PinParams, Transport, Transports,
    PROTOCOL_VERSION,
};
use crate::rpid;

const MAX_LINE_BYTES: usize = 1 << 20;
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);
const MIN_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_TIMEOUT: Duration = Duration::from_secs(600);
/// How long a backend may take to return after it is told to stop.
const STOP_GRACE: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
struct Envelope<'a> {
    id: Option<String>,
    method: Option<String>,
    #[serde(borrow)]
    params: Option<&'a RawValue>,
}

struct Active {
    id: String,
    control: CeremonyControl,
}

type ActiveSlot = Arc<Mutex<Option<Active>>>;

struct Server<A> {
    auth: Arc<A>,
    out: mpsc::UnboundedSender<String>,
    active: ActiveSlot,
}

fn send_line(out: &mpsc::UnboundedSender<String>, value: &Value) {
    let _ = out.send(value.to_string());
}

fn respond(
    out: &mpsc::UnboundedSender<String>,
    id: Option<&str>,
    result: Result<Value, PasskeyError>,
) {
    let line = match result {
        Ok(result) => json!({ "id": id, "result": result }),
        Err(error) => json!({ "id": id, "error": error }),
    };
    send_line(out, &line);
}

fn parse_params<T: DeserializeOwned>(params: Option<&RawValue>) -> Result<T, PasskeyError> {
    let raw = params.map_or("{}", RawValue::get);
    serde_json::from_str(raw)
        .map_err(|error| PasskeyError::new(ErrorCode::Unknown, format!("invalid params: {error}")))
}

fn ceremony_timeout(requested_ms: Option<u64>) -> Duration {
    requested_ms
        .filter(|ms| *ms > 0)
        .map_or(DEFAULT_TIMEOUT, Duration::from_millis)
        .clamp(MIN_TIMEOUT, MAX_TIMEOUT)
}

fn lock(slot: &ActiveSlot) -> std::sync::MutexGuard<'_, Option<Active>> {
    slot.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Checks shared by `create` and `get` before a ceremony may start.
fn admit<A: Authenticator>(auth: &A, scope: &impl CeremonyScope) -> Result<(), PasskeyError> {
    rpid::check(scope.origin(), scope.top_origin(), scope.rp_id())?;
    if scope.cross_origin() {
        return Err(PasskeyError::new(
            ErrorCode::NotAllowed,
            "passkeys in cross-origin frames are not supported",
        ));
    }
    match scope.transport() {
        Transport::Device if !auth.device_supported() => Err(device_unsupported()),
        Transport::Usb if !auth.usb_supported() => Err(PasskeyError::new(
            ErrorCode::NotSupported,
            "security keys are not supported on this system",
        )),
        Transport::Usb | Transport::Hybrid | Transport::Device => Ok(()),
    }
}

impl<A: Authenticator> Server<A> {
    fn handle_line(&self, line: &str) {
        let envelope: Envelope<'_> = if let Ok(envelope) = serde_json::from_str(line) {
            envelope
        } else {
            warn!("ignoring a request line that is not a JSON object");
            respond(
                &self.out,
                None,
                Err(PasskeyError::new(
                    ErrorCode::Unknown,
                    "request is not valid JSON",
                )),
            );
            return;
        };
        let Some(id) = envelope.id else {
            warn!("ignoring a request without an id");
            return;
        };
        let method = envelope.method.unwrap_or_default();
        info!(%id, %method, "request");
        match method.as_str() {
            "hello" => self.hello(id),
            "create" => {
                let result = parse_params::<CreateParams>(envelope.params).and_then(|params| {
                    admit(self.auth.as_ref(), &params)?;
                    if params.rp.id.as_deref().is_some_and(|rp| rp != params.rp_id) {
                        return Err(PasskeyError::new(
                            ErrorCode::Security,
                            "rp.id differs from rpId",
                        ));
                    }
                    Ok(params)
                });
                self.start(id, result, |auth, params, io| async move {
                    auth.create(params, io).await
                });
            }
            "get" => {
                let result = parse_params::<GetParams>(envelope.params).and_then(|params| {
                    admit(self.auth.as_ref(), &params)?;
                    Ok(params)
                });
                self.start(id, result, |auth, params, io| async move {
                    auth.get(params, io).await
                });
            }
            "cancel" => {
                let result = parse_params::<CeremonyRef>(envelope.params).map(|params| {
                    if let Some(active) = lock(&self.active).as_ref() {
                        if active.id == params.ceremony {
                            active
                                .control
                                .stop
                                .send_replace(Some(StopReason::Cancelled));
                        }
                    }
                    json!({})
                });
                respond(&self.out, Some(&id), result);
            }
            "pin" => {
                let result = parse_params::<PinParams>(envelope.params).and_then(|params| {
                    let guard = lock(&self.active);
                    let active = Self::matching(guard.as_ref(), &params.ceremony)?;
                    let _ = active.control.pins.try_send(params.pin);
                    Ok(json!({}))
                });
                respond(&self.out, Some(&id), result);
            }
            "choose" => {
                let result = parse_params::<ChooseParams>(envelope.params).and_then(|params| {
                    let guard = lock(&self.active);
                    let active = Self::matching(guard.as_ref(), &params.ceremony)?;
                    let _ = active.control.choices.try_send(params.index);
                    Ok(json!({}))
                });
                respond(&self.out, Some(&id), result);
            }
            "device.list" => {
                let auth = Arc::clone(&self.auth);
                let out = self.out.clone();
                tokio::spawn(async move {
                    let result = auth
                        .device_list()
                        .await
                        .map(|credentials| json!({ "credentials": credentials }));
                    respond(&out, Some(&id), result);
                });
            }
            "device.delete" => match parse_params::<DeviceDeleteParams>(envelope.params) {
                Ok(params) => {
                    let auth = Arc::clone(&self.auth);
                    let out = self.out.clone();
                    tokio::spawn(async move {
                        let result = auth.device_delete(params.credential_id).await;
                        respond(&out, Some(&id), result.map(|()| json!({})));
                    });
                }
                Err(error) => respond(&self.out, Some(&id), Err(error)),
            },
            _ => respond(
                &self.out,
                Some(&id),
                Err(PasskeyError::new(
                    ErrorCode::NotSupported,
                    format!("unknown method {method}"),
                )),
            ),
        }
    }

    fn matching<'a>(
        active: Option<&'a Active>,
        ceremony: &str,
    ) -> Result<&'a Active, PasskeyError> {
        active
            .filter(|active| active.id == ceremony)
            .ok_or_else(|| PasskeyError::new(ErrorCode::NotAllowed, "no such ceremony is running"))
    }

    fn hello(&self, id: String) {
        let auth = Arc::clone(&self.auth);
        let out = self.out.clone();
        tokio::spawn(async move {
            let bluetooth = auth.bluetooth().await;
            let hello = HelloResult {
                protocol: PROTOCOL_VERSION,
                version: env!("CARGO_PKG_VERSION").to_owned(),
                transports: Transports {
                    hybrid: bluetooth != BluetoothState::Missing,
                    usb: auth.usb_supported(),
                    device: auth.device_supported(),
                },
                bluetooth,
            };
            respond(
                &out,
                Some(&id),
                serde_json::to_value(hello).map_err(|error| internal(&error)),
            );
        });
    }

    fn start<P, R, F, Fut>(&self, id: String, params: Result<P, PasskeyError>, run: F)
    where
        P: CeremonyScope + Send + 'static,
        R: serde::Serialize + Send,
        F: FnOnce(Arc<A>, P, CeremonyIo) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<R, PasskeyError>> + Send + 'static,
    {
        let params = match params {
            Ok(params) => params,
            Err(error) => return respond(&self.out, Some(&id), Err(error)),
        };
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let (io, control) = CeremonyIo::new(event_tx);
        let stop = control.stop.clone();
        {
            let mut slot = lock(&self.active);
            if slot.is_some() {
                drop(slot);
                return respond(
                    &self.out,
                    Some(&id),
                    Err(PasskeyError::new(
                        ErrorCode::NotAllowed,
                        "another passkey request is running",
                    )),
                );
            }
            *slot = Some(Active {
                id: id.clone(),
                control,
            });
        }

        let timeout = ceremony_timeout(params.timeout_ms());
        let out = self.out.clone();
        let events_out = self.out.clone();
        let active = Arc::clone(&self.active);
        let auth = Arc::clone(&self.auth);
        let ceremony = id.clone();
        let forwarder = tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                if let Ok(mut value) = serde_json::to_value(&event) {
                    value["ceremony"] = Value::String(ceremony.clone());
                    send_line(&events_out, &value);
                }
            }
        });
        tokio::spawn(async move {
            let timer_stop = stop.clone();
            let timer = tokio::spawn(async move {
                tokio::time::sleep(timeout).await;
                timer_stop.send_if_modified(|reason| {
                    reason.get_or_insert(StopReason::TimedOut);
                    true
                });
            });
            // The ceremony future owns the event sender; it ends here, so the
            // forwarder drains every event before the response is written.
            let result = tokio::select! {
                result = run(auth, params, io) => result,
                () = backstop(stop.subscribe()) => {
                    warn!("authenticator did not stop in time");
                    Err((*stop.borrow()).unwrap_or(StopReason::Cancelled).error())
                }
            };
            timer.abort();
            let _ = tokio::time::timeout(Duration::from_secs(1), forwarder).await;
            {
                let mut slot = lock(&active);
                if slot.as_ref().is_some_and(|current| current.id == id) {
                    *slot = None;
                }
            }
            if let Err(error) = &result {
                info!(%id, code = ?error.code, "ceremony failed");
            }
            respond(
                &out,
                Some(&id),
                result.and_then(|value| {
                    serde_json::to_value(value).map_err(|error| internal(&error))
                }),
            );
        });
    }
}

/// Resolves `STOP_GRACE` after the ceremony was told to stop.
async fn backstop(mut stop: tokio::sync::watch::Receiver<Option<StopReason>>) {
    loop {
        if stop.borrow_and_update().is_some() {
            break;
        }
        if stop.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
    tokio::time::sleep(STOP_GRACE).await;
}

fn internal(error: &serde_json::Error) -> PasskeyError {
    PasskeyError::new(ErrorCode::Unknown, error.to_string())
}

/// Serves requests from `input` until it closes, writing responses and events to `output`.
///
/// # Errors
/// Returns the I/O error that ended reading `input`.
pub async fn serve<A, R, W>(auth: A, input: R, mut output: W) -> std::io::Result<()>
where
    A: Authenticator,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let (out, mut lines) = mpsc::unbounded_channel::<String>();
    let writer = tokio::spawn(async move {
        while let Some(line) = lines.recv().await {
            if output.write_all(line.as_bytes()).await.is_err()
                || output.write_all(b"\n").await.is_err()
                || output.flush().await.is_err()
            {
                break;
            }
        }
    });
    let server = Server {
        auth: Arc::new(auth),
        out,
        active: Arc::new(Mutex::new(None)),
    };
    let mut reader = BufReader::new(input);
    let mut line = String::new();
    let result = loop {
        line.zeroize();
        line.clear();
        match (&mut reader)
            .take(MAX_LINE_BYTES as u64 + 1)
            .read_line(&mut line)
            .await
        {
            Ok(0) => break Ok(()),
            Ok(_) if line.len() > MAX_LINE_BYTES && !line.ends_with('\n') => {
                respond(
                    &server.out,
                    None,
                    Err(PasskeyError::new(
                        ErrorCode::Unknown,
                        "request line is too long",
                    )),
                );
                break Ok(());
            }
            Ok(_) => {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    server.handle_line(trimmed);
                }
            }
            Err(error) => break Err(error),
        }
    };
    line.zeroize();
    if let Some(active) = lock(&server.active).as_ref() {
        active
            .control
            .stop
            .send_replace(Some(StopReason::Cancelled));
    }
    drop(server);
    let _ = writer.await;
    result
}
