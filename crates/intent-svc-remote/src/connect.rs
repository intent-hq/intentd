//! Connecting to an exposed machine: race its direct hosts, then try its
//! tailcat route, and bind the executor link on the first that answers.
//!
//! The direct hosts are raced at the TCP level, all at once, each within
//! [`ConnectTuning::attempt`]; the link is then dialed (TLS pin, token,
//! bind) on the first host that accepted. If no direct host binds, the
//! tailcat route is tried. The whole connect is bounded by
//! [`ConnectTuning::total`]. Every route's failure is kept, by its executor
//! link code, for `machine-unreachable`.

use std::future::Future;
use std::time::Duration;

use serde_json::json;

use intent_core::machines::{MachineUpdateRequired, UpdateSide};
use intent_core::node_link::{LinkHello, ProtocolRange, NODE_LINK_PROTOCOLS};
use intent_core::{Error, Result};
use intent_transport::node_link::{self as link, codes, DialedLink, NodeEndpoint};

use crate::machine::{MachineTarget, Route};

/// `error.data.code` of a machine none of whose routes bound.
pub(crate) const MACHINE_UNREACHABLE: &str = "machine-unreachable";
/// `error.data.code` of a machine that refused this backend's credential.
pub(crate) const MACHINE_REVOKED: &str = "machine-revoked";

/// Connect timing; tests shorten it.
#[derive(Debug, Clone)]
pub(crate) struct ConnectTuning {
    /// Longest one route may take to accept a TCP connection.
    pub attempt: Duration,
    /// Longest the whole connect may take.
    pub total: Duration,
}

impl Default for ConnectTuning {
    fn default() -> Self {
        Self {
            attempt: Duration::from_secs(3),
            total: Duration::from_secs(10),
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
    /// This backend could not prepare the hello (its own store failed),
    /// or the connector stopped.
    Local(String),
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
                format!("{name} no longer accepts this backend's credential; make it available again."),
                json!({ "code": MACHINE_REVOKED, "attempts": attempts_json(&attempts) }),
            ),
            Self::UpdateRequired(update) => Error::MachineUpdateRequired(update),
            Self::Local(message) => Error::Internal(format!("remote machine {name}: {message}")),
        }
    }
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
    let raced = tokio::time::timeout(tuning.total, async {
        let mut candidates = Vec::new();
        if let Some(route) = race_direct(target.direct_routes(), tuning, &mut attempts).await {
            candidates.push(route);
        }
        candidates.extend(target.tailcat_route());
        for route in candidates {
            let offer = hello()
                .await
                .map_err(|error| ConnectFailure::Local(error.to_string()))?;
            match link::dial_link(&route.endpoint, offer).await {
                Ok(dialed) => {
                    return Ok(Connected {
                        dialed,
                        endpoint: route.endpoint,
                    })
                }
                Err(error) => {
                    let code = code_of(&error);
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
                }
            }
        }
        Err(ConnectFailure::Unreachable(Vec::new()))
    })
    .await;
    let failure = match raced {
        Ok(Ok(connected)) => return Ok(connected),
        Ok(Err(ConnectFailure::Unreachable(_))) | Err(_) => None,
        Ok(Err(other)) => Some(other),
    };
    if let Some(failure) = failure {
        return Err(failure);
    }
    if attempts.is_empty() {
        attempts.push(Attempt {
            route: "none".into(),
            error: codes::UNAVAILABLE.into(),
        });
    }
    if attempts.iter().any(|a| a.error == codes::UNAUTHORIZED) {
        return Err(ConnectFailure::Revoked(attempts));
    }
    Err(ConnectFailure::Unreachable(attempts))
}

/// Race `routes` at the TCP level; the first to accept wins. The others'
/// failures are recorded in `attempts`; the losers are cancelled.
async fn race_direct(
    routes: Vec<Route>,
    tuning: &ConnectTuning,
    attempts: &mut Vec<Attempt>,
) -> Option<Route> {
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
    while let Some(joined) = racing.join_next().await {
        let Ok((route, reached)) = joined else {
            continue;
        };
        if reached {
            racing.abort_all();
            return Some(route);
        }
        attempts.push(Attempt {
            route: route.label,
            error: codes::UNAVAILABLE.into(),
        });
    }
    None
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
    }
}
