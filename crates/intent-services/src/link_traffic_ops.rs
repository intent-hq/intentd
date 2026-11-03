//! `remoteLinkTraffic.*` (monorepo `docs/protocol/methods/nodes.md`, "Link
//! traffic capture"): the owner's capture of this daemon's executor-link
//! frames, both as a backend and as a machine. The capture itself is
//! [`LinkTraffic`]; the links tap it, and its batches are published as the
//! transient, owner-only `remoteLinkTraffic:records` event.
//!
//! The methods are on neither the collaborator nor the member allowlist, so
//! the transport refuses members and guests first; the owner is rechecked
//! here. Node mode never serves them.

use std::sync::Arc;

use intent_core::link_traffic::{LinkTraffic, SNAPSHOT_MAX_LIMIT};
use intent_core::{now_iso, Error, Result, WorkspaceId};
use serde_json::{json, Value};

use crate::{EventBus, Services};

impl Services {
    /// This daemon's link traffic capture, shared by every link it taps.
    #[must_use]
    pub fn link_traffic(&self) -> &Arc<LinkTraffic> {
        &self.link_traffic
    }

    /// `remoteLinkTraffic.<method>`: see
    /// [`intent_core::WorkspaceApi::remote_link_traffic`].
    pub(crate) async fn remote_link_traffic_op(
        &self,
        method: &'static str,
        params: Value,
    ) -> Result<Value> {
        let what = format!("remoteLinkTraffic.{method}");
        self.require_machine_owner(&what).await?;
        if self.node_mode {
            return Err(Error::NodeMode(format!(
                "{what}: this daemon runs in node mode"
            )));
        }
        match method {
            "start" => {
                let payloads = match params.get("payloads") {
                    None | Some(Value::Null) => true,
                    Some(Value::Bool(payloads)) => *payloads,
                    Some(_) => {
                        return Err(Error::InvalidParams("payloads must be a boolean".into()))
                    }
                };
                Ok(json!({ "capture": self.link_traffic.start(payloads) }))
            }
            "stop" => Ok(json!({ "capture": self.link_traffic.stop() })),
            "snapshot" => {
                let after_seq = optional_count(&params, "afterSeq", u64::MAX)?;
                let limit = optional_count(&params, "limit", SNAPSHOT_MAX_LIMIT as u64)?;
                if limit == Some(0) {
                    return Err(Error::InvalidParams(format!(
                        "limit must be between 1 and {SNAPSHOT_MAX_LIMIT}"
                    )));
                }
                Ok(self.link_traffic.snapshot(
                    after_seq,
                    limit.and_then(|limit| usize::try_from(limit).ok()),
                ))
            }
            other => Err(Error::InvalidParams(format!(
                "unknown method remoteLinkTraffic.{other}"
            ))),
        }
    }
}

/// An optional non-negative integer param of at most `max`.
fn optional_count(params: &Value, name: &str, max: u64) -> Result<Option<u64>> {
    match params.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value
            .as_u64()
            .filter(|count| *count <= max)
            .map(Some)
            .ok_or_else(|| {
                Error::InvalidParams(format!(
                    "{name} must be a non-negative integer of at most {max}"
                ))
            }),
    }
}

/// Publish each capture batch on `bus` as `remoteLinkTraffic:records`:
/// transient (never persisted), global and owner-only.
pub(crate) fn publish_link_traffic(traffic: &LinkTraffic, bus: EventBus) {
    traffic.set_sink(Arc::new(move |data| {
        let event = intent_store::NewEvent {
            workspace_id: WorkspaceId::from_string(String::new()),
            timestamp: now_iso(),
            event_type: intent_core::events::REMOTE_LINK_TRAFFIC_RECORDS.to_owned(),
            actor: crate::system_actor(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        };
        let _ = bus.publish_transient(&event);
    }));
}

#[cfg(test)]
mod tests;
