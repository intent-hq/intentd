//! `remoteMachines.claim` (monorepo `docs/protocol/methods/nodes.md`, "Node
//! invites and claim"): claim an `intent://node` link's invite on the
//! machine's `/node/claim`, then probe and save the machine exactly as
//! `remoteMachines.add` does.
//!
//! Routes are tried as for the executor link (direct hosts raced, then
//! tailcat) only until a `claim` frame is written: after that the invite may
//! be spent, so no other route is tried and a failure carries
//! `inviteSpent: true`. The link's secret and the minted token never reach a
//! result, an error or a log.

use serde_json::{json, Value};

use intent_core::machines::{MachineUpdateRequired, UpdateSide};
use intent_core::node_invite::{codes, ClaimFrame, ClaimReply, NodeInviteLink};
use intent_core::node_link::NODE_LINK_PROTOCOLS;
use intent_core::{Error, Result};
use intent_services::Services;
use intent_transport::node_link::{self as link, codes as link_codes};

use crate::connect::{race_direct, Attempt, MACHINE_UNREACHABLE};
use crate::machine::{MachineTarget, Route};
use crate::methods::{add_machine, backend_name, AddParams};
use crate::RemoteFeature;

/// Stands in for the executor token on the claim dial, which sends none.
const NO_TOKEN: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn coded(internal: bool, code: &str, message: impl Into<String>, extra: Value) -> Error {
    let mut data = json!({ "code": code });
    if let (Some(data), Value::Object(extra)) = (data.as_object_mut(), extra) {
        data.extend(extra);
    }
    Error::remote_machine(internal, message, data)
}

fn invalid_link() -> Error {
    coded(
        false,
        codes::INVALID_LINK,
        "That is not a valid intent://node link. On the machine, run `intentd pair --node` for a new one.",
        json!({}),
    )
}

fn unreachable(attempts: &[Attempt], spent: bool) -> Error {
    let message = if spent {
        "The machine stopped answering during the claim, so the link may be used up. On the \
         machine, run `intentd pair --node` for a new one."
    } else {
        "Could not reach the machine on any route in the link."
    };
    coded(
        true,
        MACHINE_UNREACHABLE,
        message,
        json!({
            "attempts": attempts
                .iter()
                .map(|a| json!({ "route": a.route, "error": a.error }))
                .collect::<Vec<_>>(),
            "inviteSpent": spent,
        }),
    )
}

fn update_required(update: &MachineUpdateRequired, spent: bool) -> Error {
    let mut data = update.data();
    data["inviteSpent"] = json!(spent);
    Error::remote_machine(false, update.message(), data)
}

/// The refusal of a `claimRefused` reply, as the public error.
fn refused(code: &str, expires_at: Option<String>, machine_version: Option<String>) -> Error {
    match code {
        codes::INVALID => coded(
            false,
            code,
            "The machine does not know this link (wrong or mistyped). On the machine, run \
             `intentd pair --node` for a new one.",
            json!({}),
        ),
        codes::EXPIRED => coded(
            false,
            code,
            "This link has expired. On the machine, run `intentd pair --node` for a new one.",
            expires_at.map_or_else(|| json!({}), |at| json!({ "expiresAt": at })),
        ),
        codes::USED => coded(
            false,
            code,
            "This link was already used. On the machine, run `intentd pair --node` for a new one.",
            json!({}),
        ),
        codes::REVOKED => coded(
            false,
            code,
            "The machine's owner revoked this link.",
            json!({}),
        ),
        codes::SELF => coded(
            false,
            code,
            "This link is for this backend itself; open it on another backend.",
            json!({}),
        ),
        codes::VERSION => update_required(
            &MachineUpdateRequired {
                machine_name: "the machine".into(),
                machine_version: machine_version.unwrap_or_else(|| "an older version".into()),
                backend_version: env!("CARGO_PKG_VERSION").to_owned(),
                update: UpdateSide::Machine,
                hint: None,
                reason: None,
            },
            false,
        ),
        _ => unreachable(
            &[Attempt {
                route: "claim".into(),
                error: link_codes::PROTOCOL.into(),
            }],
            false,
        ),
    }
}

/// Mark a probe failure after a successful claim: the invite is spent.
fn spent(error: Error) -> Error {
    match error {
        Error::MachineUpdateRequired(update) => update_required(&update, true),
        Error::RemoteMachine(mut failure) => {
            failure.data["inviteSpent"] = json!(true);
            Error::RemoteMachine(failure)
        }
        other => other,
    }
}

/// Dial `route` and claim. `Ok(None)`: the frame was not written, try the
/// next route.
async fn claim_on(
    route: Route,
    frame: &ClaimFrame,
    attempts: &mut Vec<Attempt>,
) -> Result<Option<ClaimReply>> {
    match link::dial_claim(&route.endpoint, frame).await {
        Ok(reply) => Ok(Some(reply)),
        Err(failure) => {
            let code = link::error_code(&failure.error)
                .unwrap_or(link_codes::UNAVAILABLE)
                .to_owned();
            if code == link_codes::VERSION_MISMATCH && !failure.sent {
                return Err(refused(codes::VERSION, None, None));
            }
            attempts.push(Attempt {
                route: route.label,
                error: code,
            });
            if failure.sent {
                return Err(unreachable(attempts, true));
            }
            Ok(None)
        }
    }
}

pub(crate) async fn claim(
    feature: &RemoteFeature,
    svc: &Services,
    params: &Value,
) -> Result<Value> {
    let uri = params
        .get("uri")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::InvalidParams("uri is required".into()))?;
    let link = NodeInviteLink::parse(uri).ok_or_else(invalid_link)?;
    let target = MachineTarget::from_parts(
        "claim",
        "the machine",
        link.hosts.clone(),
        link.port,
        link.tc_address.clone(),
        &link.fingerprint,
        NO_TOKEN,
        feature.tailcat_bin(),
    )
    .map_err(|_| invalid_link())?;
    let frame = ClaimFrame {
        invite_id: link.invite_id.clone(),
        secret: link.secret.clone(),
        backend_id: svc.store().daemon_id().await?,
        backend_name: backend_name(svc).await,
        backend_fingerprint: svc.own_machine_routes().await.map(|r| r.fingerprint),
        protocol: NODE_LINK_PROTOCOLS,
    };
    let tuning = feature.tuning().connect.clone();
    let mut attempts = Vec::new();
    let reply = tokio::time::timeout(tuning.total, async {
        let mut racing = race_direct(target.direct_routes(), &tuning);
        while let Some(joined) = racing.join_next().await {
            let Ok((route, reached)) = joined else {
                continue;
            };
            if !reached {
                attempts.push(Attempt {
                    route: route.label,
                    error: link_codes::UNAVAILABLE.into(),
                });
                continue;
            }
            if let Some(reply) = claim_on(route, &frame, &mut attempts).await? {
                racing.abort_all();
                return Ok(Some(reply));
            }
        }
        if let Some(route) = target.tailcat_route() {
            if let Some(reply) = claim_on(route, &frame, &mut attempts).await? {
                return Ok(Some(reply));
            }
        }
        Ok::<_, Error>(None)
    })
    .await;
    let reply = match reply {
        Ok(Ok(Some(reply))) => reply,
        Ok(Err(error)) => return Err(error),
        Ok(Ok(None)) | Err(_) => {
            if attempts.is_empty() {
                attempts.push(Attempt {
                    route: "none".into(),
                    error: link_codes::UNAVAILABLE.into(),
                });
            }
            return Err(unreachable(&attempts, false));
        }
    };
    let (credential_id, token, machine) = match reply {
        ClaimReply::Claimed {
            credential_id,
            token,
            machine,
            ..
        } => (credential_id, token, machine),
        ClaimReply::ClaimRefused {
            code,
            expires_at,
            machine_version,
            ..
        } => return Err(refused(&code, expires_at, machine_version)),
    };
    let hosts = if link.hosts.is_empty() {
        machine.hosts.clone()
    } else {
        link.hosts.clone()
    };
    let params = AddParams {
        machine_id: machine.daemon_id.clone(),
        name: machine.name.clone(),
        hosts,
        port: link.port,
        tc_address: link.tc_address.clone().or(machine.tc_address.clone()),
        fingerprint: link.fingerprint.clone(),
        credential_id,
        token,
    };
    add_machine(feature, svc, params, None)
        .await
        .map(|(_, result)| result)
        .map_err(spent)
}
