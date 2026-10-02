//! Desktop routing pins one authenticated connection incarnation, not a hello ID.
use super::{resolve_entry, PrimaryReverseRegistry};
use intent_core::desktop::{DesktopConnection, DesktopError, DesktopResult};
use intent_core::{PrincipalId, ReverseTarget};
use serde_json::Value;
use std::time::Duration;

pub(super) fn candidates(
    registry: &PrimaryReverseRegistry,
    principal: &PrincipalId,
) -> Vec<DesktopConnection> {
    let state = registry.inner.lock();
    let mut seen = std::collections::HashSet::new();
    state
        .entries
        .iter()
        .rev()
        .filter_map(|entry| {
            let device = entry.device.as_ref()?;
            if !entry.is_eligible()
                || &device.principal_id != principal
                || entry.identity.as_ref()?.capabilities["desktopControl"].as_u64() != Some(1)
                || !seen.insert(device.identity.client_id.clone())
            {
                return None;
            }
            Some(DesktopConnection {
                client_id: device.identity.client_id.clone(),
                principal_id: principal.clone(),
                connection_epoch: entry.desktop_epoch.clone(),
            })
        })
        .collect()
}

pub(super) fn resolve(
    registry: &PrimaryReverseRegistry,
    target: &ReverseTarget,
    principal: &PrincipalId,
) -> DesktopResult<DesktopConnection> {
    let state = registry.inner.lock();
    let entry = resolve_entry(&state.entries, target).map_err(|_| {
        DesktopError::new("desktop-offline", "Workspace primary desktop is offline")
    })?;
    if entry
        .identity
        .as_ref()
        .is_none_or(|id| id.capabilities["desktopControl"].as_u64() != Some(1))
    {
        return Err(DesktopError::new(
            "desktop-unsupported",
            "Workspace primary does not support desktop control",
        ));
    }
    let device = entry
        .device
        .as_ref()
        .filter(|d| &d.principal_id == principal)
        .ok_or_else(|| {
            DesktopError::new("forbidden", "Primary desktop belongs to another principal")
        })?;
    Ok(DesktopConnection {
        client_id: device.identity.client_id.clone(),
        principal_id: principal.clone(),
        connection_epoch: entry.desktop_epoch.clone(),
    })
}

pub(super) async fn dispatch(
    registry: &PrimaryReverseRegistry,
    connection: DesktopConnection,
    params: Value,
) -> DesktopResult<Value> {
    let channel = {
        let state = registry.inner.lock();
        state
            .entries
            .iter()
            .find(|entry| {
                entry.desktop_epoch == connection.connection_epoch
                    && entry.is_eligible()
                    && entry
                        .identity
                        .as_ref()
                        .is_some_and(|id| id.capabilities["desktopControl"].as_u64() == Some(1))
                    && entry.device.as_ref().is_some_and(|d| {
                        d.principal_id == connection.principal_id
                            && d.identity.client_id == connection.client_id
                    })
            })
            .map(|entry| entry.channel.clone())
            .ok_or_else(|| {
                DesktopError::new("desktop-offline", "Bound desktop connection has ended")
            })?
    };
    if params["connectionEpoch"].as_str() != Some(connection.connection_epoch.as_str())
        || params["principalId"].as_str() != Some(connection.principal_id.as_str())
    {
        return Err(DesktopError::new(
            "forbidden",
            "Desktop dispatch binding mismatch",
        ));
    }
    channel
        .request("desktop.control", params, Duration::from_secs(10))
        .await
        .map_err(|error| {
            error
                .data
                .and_then(|data| serde_json::from_value(data).ok())
                .unwrap_or_else(|| DesktopError::new("desktop-outcome-unknown", error.message))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reverse::{ReverseChannel, ReverseClientIdentity, ReverseTransport};
    use intent_core::{AgentReverseDispatch, ClientHostInfo, ClientId};
    use serde_json::json;
    use tokio::sync::mpsc;

    #[test]
    fn primary_is_selected_before_capability_and_principal_checks() {
        let registry = PrimaryReverseRegistry::new();
        let first = registry.register(
            ReverseChannel::new(mpsc::channel(4).0),
            ReverseTransport::Wss,
        );
        let second = registry.register(
            ReverseChannel::new(mpsc::channel(4).0),
            ReverseTransport::Wss,
        );
        let principal = PrincipalId::from("owner");
        let identity = |id: &str, desktop| ReverseClientIdentity {
            client_id: ClientId::from(id),
            name: None,
            capabilities: json!({"browserExec":true,"desktopControl":desktop}),
            host: ClientHostInfo::default(),
        };
        first.bind_device(identity("first", 0), principal.clone());
        first.bind(identity("first", 0));
        second.bind_device(identity("second", 1), principal.clone());
        second.bind(identity("second", 1));
        assert_eq!(
            registry
                .desktop_resolve(&ReverseTarget::Default, &principal)
                .unwrap_err()
                .code,
            "desktop-unsupported"
        );
        assert_eq!(
            registry
                .desktop_resolve(
                    &ReverseTarget::Pinned(ClientId::from("missing")),
                    &principal
                )
                .unwrap_err()
                .code,
            "desktop-offline"
        );
        assert_eq!(
            registry
                .desktop_resolve(
                    &ReverseTarget::Client(ClientId::from("second")),
                    &PrincipalId::from("foreign")
                )
                .unwrap_err()
                .code,
            "forbidden"
        );
    }

    #[tokio::test]
    async fn reconnect_and_rehello_never_reuse_incarnation() {
        let registry = PrimaryReverseRegistry::new();
        let (tx, mut rx) = mpsc::channel(4);
        let guard = registry.register(ReverseChannel::new(tx), ReverseTransport::Wss);
        let principal = PrincipalId::from("owner");
        let identity = ReverseClientIdentity {
            client_id: ClientId::from("client"),
            name: None,
            capabilities: json!({"browserExec":true,"desktopControl":1}),
            host: ClientHostInfo::default(),
        };
        guard.bind_device(identity.clone(), principal.clone());
        guard.bind(identity.clone());
        let old = registry
            .desktop_resolve(&ReverseTarget::Default, &principal)
            .unwrap();
        guard.bind_device(identity.clone(), principal.clone());
        guard.bind(identity);
        let new = registry
            .desktop_resolve(&ReverseTarget::Default, &principal)
            .unwrap();
        assert_ne!(old.connection_epoch, new.connection_epoch);
        assert_eq!(
            registry
                .desktop_dispatch(old, json!({}))
                .await
                .unwrap_err()
                .code,
            "desktop-offline"
        );
        assert!(rx.try_recv().is_err());
        drop(guard);
        assert_eq!(
            registry
                .desktop_dispatch(new, json!({}))
                .await
                .unwrap_err()
                .code,
            "desktop-offline"
        );
        assert!(rx.try_recv().is_err());
    }
}
