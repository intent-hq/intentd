//! Connecting to an exposed machine: race its direct hosts, then try its
//! tailcat route, and bind the executor link on the first that answers.
//!
//! The direct hosts are raced at the TCP level, all at once, each within
//! [`ConnectTuning::attempt`]; the link is then dialed (TLS pin, token,
//! bind) on each host that accepted, in the order they answered, until one
//! binds, so a stale address or another service on a listed host cannot
//! hide a valid one. If no direct host binds, the tailcat route is tried. The whole connect is bounded by
//! [`ConnectTuning::total`]. Every route's failure is kept, by its executor
//! link code, for `machine-unreachable`.

use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;

use serde_json::json;

use intent_core::machines::{MachineUpdateRequired, UpdateSide};
use intent_core::node_link::{LinkHello, ProtocolRange, NODE_LINK_PROTOCOLS};
use intent_core::{Error, Result};
use intent_transport::node_link::{self as link, codes, Backoff, DialedLink, NodeEndpoint};

use crate::machine::{MachineTarget, Route};

/// `error.data.code` of a machine none of whose routes bound.
pub(crate) const MACHINE_UNREACHABLE: &str = "machine-unreachable";
/// `error.data.code` of a machine that refused this backend's credential.
pub(crate) const MACHINE_REVOKED: &str = "machine-revoked";
/// `error.data.code` of a machine that now runs as an orchestrator.
pub(crate) const ORCHESTRATOR_MODE: &str = "orchestrator-mode";

/// Connect timing; tests shorten it.
#[derive(Debug, Clone)]
pub(crate) struct ConnectTuning {
    /// Longest one route may take to accept a TCP connection.
    pub attempt: Duration,
    /// Longest the whole connect may take.
    pub total: Duration,
    /// Right after a resume: the bound of the first tailcat attempt. Later
    /// attempts get [`Self::attempt`]. A healthy Northflank resume binds in
    /// about 1.2 s; this leaves headroom for a slow path.
    pub resume_attempt: Duration,
    /// Right after a resume: the delays between attempts.
    pub resume_retry: Backoff,
}

impl Default for ConnectTuning {
    fn default() -> Self {
        Self {
            attempt: Duration::from_secs(3),
            total: Duration::from_secs(10),
            resume_attempt: Duration::from_secs(2),
            resume_retry: Backoff::new(Duration::from_millis(250), Duration::from_secs(2)),
        }
    }
}

/// One route that did not bind, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Attempt {
    pub route: String,
    /// The executor link code, for example `node-link-unauthorized`.
    pub error: String,
}

/// Why a connect failed.
#[derive(Debug, Clone)]
pub(crate) enum ConnectFailure {
    /// No route bound; each tried route's error.
    Unreachable(Vec<Attempt>),
    /// The machine refused the credential (revoked or unknown).
    Revoked(Vec<Attempt>),
    /// The machine and this backend share no protocol version.
    UpdateRequired(Box<MachineUpdateRequired>),
    /// The machine now runs as an orchestrator and refuses every bind: the
    /// backend forgets it.
    Orchestrator,
    /// This backend could not prepare the hello (its own store failed),
    /// or the connector stopped.
    Local(String),
    /// A cloud machine's sandbox could not be made dialable (resumed) before
    /// the dial: the documented `cloud-*` error's message and data. `lost`
    /// when E2B no longer has the sandbox.
    Cloud {
        message: String,
        data: serde_json::Value,
        lost: bool,
    },
}

impl ConnectFailure {
    /// The JSON-RPC error for `remoteMachines.add` / `refresh`, naming the
    /// machine. Never carries the token.
    pub(crate) fn into_error(self, name: &str) -> Error {
        match self {
            Self::Unreachable(attempts) => Error::remote_machine(
                true,
                format!("Could not reach {name} on any of its routes."),
                json!({ "code": MACHINE_UNREACHABLE, "attempts": attempts_json(&attempts) }),
            ),
            Self::Revoked(attempts) => Error::remote_machine(
                true,
                format!(
                    "{name} no longer accepts this backend's credential; let this backend run agents on it again."
                ),
                json!({ "code": MACHINE_REVOKED, "attempts": attempts_json(&attempts) }),
            ),
            Self::UpdateRequired(update) => Error::MachineUpdateRequired(update),
            Self::Orchestrator => Error::remote_machine(
                false,
                format!(
                    "{name} is now an orchestrator and no longer runs agents for other backends, so it was removed. Pair it again to use it as a backend."
                ),
                json!({ "code": ORCHESTRATOR_MODE }),
            ),
            Self::Local(message) => Error::Internal(format!("remote machine {name}: {message}")),
            Self::Cloud { message, data, .. } => Error::remote_machine(true, message, data),
        }
    }
}

/// `machine-unreachable` for a link that bound on `route` but then failed
/// with the executor link `code` before the machine answered.
pub(crate) fn live_link_failure(name: &str, route: &str, code: &str) -> Error {
    let message = if code == codes::TIMEOUT {
        format!(
            "{name} is connected but did not report its capabilities in time; \
             a machine that just started can take a few minutes for its first report, \
             so try again shortly."
        )
    } else {
        format!(
            "The link to {name} failed ({code}) before it reported its capabilities; try again."
        )
    };
    Error::remote_machine(
        true,
        message,
        json!({
            "code": MACHINE_UNREACHABLE,
            "attempts": [{ "route": route, "error": code }],
        }),
    )
}

fn attempts_json(attempts: &[Attempt]) -> serde_json::Value {
    attempts
        .iter()
        .map(|attempt| json!({ "route": attempt.route, "error": attempt.error }))
        .collect()
}

/// The executor link code of a dial error.
fn code_of(error: &Error) -> String {
    link::error_code(error)
        .unwrap_or(codes::UNAVAILABLE)
        .to_owned()
}

/// The peer's protocol version in a `node-link-version-mismatch` error.
fn peer_protocol(error: &Error) -> Option<u32> {
    let text = error.to_string();
    let after = text.split("speaks ").nth(1)?;
    let digits: String = after.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok()
}

/// A successful connect: the dialed link and the endpoint it came over (the
/// git connection must use the same route).
pub(crate) struct Connected {
    pub dialed: DialedLink,
    pub endpoint: NodeEndpoint,
    /// The route's label, as in [`Attempt::route`].
    pub route: String,
}

/// Connect to `target`, binding with a hello from `hello` (called once per
/// dial, so each dial offers a newly advanced generation). `machine_version`
/// names the machine's last known intentd version for an update error.
pub(crate) async fn connect<F, Fut>(
    target: &MachineTarget,
    tuning: &ConnectTuning,
    machine_version: Option<&str>,
    mut hello: F,
) -> std::result::Result<Connected, ConnectFailure>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<LinkHello>>,
{
    let mut attempts = Vec::new();
    let raced = tokio::time::timeout(
        tuning.total,
        connect_once(
            target,
            tuning,
            machine_version,
            &mut hello,
            None,
            &mut attempts,
        ),
    )
    .await;
    match raced {
        Ok(Ok(connected)) => Ok(connected),
        Ok(Err(ConnectFailure::Unreachable(_))) | Err(_) => Err(unreachable(attempts)),
        Ok(Err(other)) => Err(other),
    }
}

/// [`connect`] right after a cloud sandbox resumed. Its tailcat path comes
/// back a moment after the sandbox: until then a pipe stalls (E2B) or is
/// refused. Each tailcat attempt is bounded and an unreachable one is
/// retried quickly, all within [`ConnectTuning::total`], so the first
/// stalled pipe no longer takes the whole budget.
pub(crate) async fn connect_after_resume<F, Fut>(
    target: &MachineTarget,
    tuning: &ConnectTuning,
    machine_version: Option<&str>,
    mut hello: F,
) -> std::result::Result<Connected, ConnectFailure>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<LinkHello>>,
{
    let deadline = Instant::now() + tuning.total;
    let mut retry = tuning.resume_retry.clone();
    let mut bound = tuning.resume_attempt;
    let mut attempts = Vec::new();
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(unreachable(attempts));
        }
        let mut round = Vec::new();
        let raced = tokio::time::timeout(
            left,
            connect_once(
                target,
                tuning,
                machine_version,
                &mut hello,
                Some(bound),
                &mut round,
            ),
        )
        .await;
        match raced {
            Ok(Ok(connected)) => return Ok(connected),
            Ok(Err(ConnectFailure::Unreachable(_))) | Err(_) => {}
            Ok(Err(other)) => return Err(other),
        }
        // A refused credential will not change on a retry.
        if round.iter().any(|a| a.error == codes::UNAUTHORIZED) {
            return Err(ConnectFailure::Revoked(round));
        }
        if !round.is_empty() {
            attempts = round;
        }
        tracing::debug!(machine = %target.id, ?attempts, "remote machine: not answering yet after the resume; retrying");
        bound = tuning.attempt;
        let left = deadline.saturating_duration_since(Instant::now());
        tokio::time::sleep(retry.next_delay().min(left)).await;
    }
}

/// `Unreachable` with `attempts`, or `Revoked` when a route refused the
/// credential.
fn unreachable(mut attempts: Vec<Attempt>) -> ConnectFailure {
    if attempts.is_empty() {
        attempts.push(Attempt {
            route: "none".into(),
            error: codes::UNAVAILABLE.into(),
        });
    }
    if attempts.iter().any(|a| a.error == codes::UNAUTHORIZED) {
        return ConnectFailure::Revoked(attempts);
    }
    ConnectFailure::Unreachable(attempts)
}

/// One pass over `target`'s routes: the direct hosts raced, then the
/// tailcat route, within `tailcat_bound` when given. `Unreachable` (with
/// no attempts) when none bound; their failures are in `attempts`.
async fn connect_once<F, Fut>(
    target: &MachineTarget,
    tuning: &ConnectTuning,
    machine_version: Option<&str>,
    hello: &mut F,
    tailcat_bound: Option<Duration>,
    attempts: &mut Vec<Attempt>,
) -> std::result::Result<Connected, ConnectFailure>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<LinkHello>>,
{
    let mut racing = race_direct(target.direct_routes(), tuning);
    loop {
        let route = match racing.join_next().await {
            None => break,
            Some(Ok((route, true))) => route,
            Some(Ok((route, false))) => {
                attempts.push(Attempt {
                    route: route.label,
                    error: codes::UNAVAILABLE.into(),
                });
                continue;
            }
            Some(Err(_)) => continue,
        };
        // A host that accepted TCP gets one attempt's budget for the
        // handshake, so one that stalls cannot hide the hosts after it.
        if let Some(connected) = dial(
            target,
            machine_version,
            hello,
            route,
            Some(tuning.attempt),
            attempts,
        )
        .await?
        {
            racing.abort_all();
            return Ok(connected);
        }
    }
    if let Some(route) = target.tailcat_route() {
        if let Some(connected) = dial(
            target,
            machine_version,
            hello,
            route,
            tailcat_bound,
            attempts,
        )
        .await?
        {
            return Ok(connected);
        }
    }
    Err(ConnectFailure::Unreachable(Vec::new()))
}

/// Dial and bind on `route`, within `bound` when given: `Ok(None)` records
/// its failure in `attempts` for the caller to try the next route; a
/// version mismatch or an orchestrator's refusal ends the connect, since
/// every route reaches the same daemon.
async fn dial<F, Fut>(
    target: &MachineTarget,
    machine_version: Option<&str>,
    hello: &mut F,
    route: Route,
    bound: Option<Duration>,
    attempts: &mut Vec<Attempt>,
) -> std::result::Result<Option<Connected>, ConnectFailure>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<LinkHello>>,
{
    let offer = hello()
        .await
        .map_err(|error| ConnectFailure::Local(error.to_string()))?;
    let dialed = match bound {
        Some(bound) => tokio::time::timeout(bound, link::dial_link(&route.endpoint, offer))
            .await
            .unwrap_or_else(|_| {
                Err(Error::Internal(format!(
                    "{}: the handshake took too long",
                    codes::TIMEOUT
                )))
            }),
        None => link::dial_link(&route.endpoint, offer).await,
    };
    match dialed {
        Ok(dialed) => Ok(Some(Connected {
            dialed,
            endpoint: route.endpoint,
            route: route.label,
        })),
        Err(error) => {
            let code = code_of(&error);
            if code == codes::ORCHESTRATOR_MODE {
                return Err(ConnectFailure::Orchestrator);
            }
            if code == codes::VERSION_MISMATCH {
                return Err(ConnectFailure::UpdateRequired(Box::new(update_required(
                    target,
                    machine_version,
                    peer_protocol(&error),
                ))));
            }
            attempts.push(Attempt {
                route: route.label,
                error: code,
            });
            Ok(None)
        }
    }
}

/// Race `routes` at the TCP level: each task yields its route and whether
/// it accepted a connection within [`ConnectTuning::attempt`], in the order
/// they finish.
pub(crate) fn race_direct(
    routes: Vec<Route>,
    tuning: &ConnectTuning,
) -> tokio::task::JoinSet<(Route, bool)> {
    let mut racing = tokio::task::JoinSet::new();
    for route in routes {
        let limit = tuning.attempt;
        // caller-binding: allow — a bare TCP reachability probe; no service call
        racing.spawn(async move {
            let reached =
                tokio::time::timeout(limit, tokio::net::TcpStream::connect(route.label.clone()))
                    .await;
            (route, matches!(reached, Ok(Ok(_))))
        });
    }
    racing
}

fn update_required(
    target: &MachineTarget,
    machine_version: Option<&str>,
    peer: Option<u32>,
) -> MachineUpdateRequired {
    let update = peer.map_or(UpdateSide::Machine, |peer| {
        UpdateSide::for_ranges(
            NODE_LINK_PROTOCOLS,
            ProtocolRange {
                min: peer,
                max: peer,
            },
        )
    });
    MachineUpdateRequired {
        machine_name: target.name.clone(),
        machine_version: machine_version.unwrap_or("an older version").to_owned(),
        backend_version: env!("CARGO_PKG_VERSION").to_owned(),
        update,
        hint: None,
        reason: None,
    }
}
