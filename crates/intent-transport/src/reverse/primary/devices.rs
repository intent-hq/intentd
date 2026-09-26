//! Authenticated device presence shares connection lifetimes with reverse RPC,
//! but guests appear here without ever becoming browser hosts.
use super::*;
use intent_core::{events::CLIENT_UPDATED, PrincipalId};

pub(super) struct DeviceBinding {
    identity: ReverseClientIdentity,
    pub(super) principal_id: PrincipalId,
    hello_seq: u64,
}

impl State {
    pub(super) fn queue_device(
        &self,
        tx: &mpsc::UnboundedSender<ClientTransition>,
        event_type: &'static str,
        row: &ReverseLiveClient,
    ) {
        let mut data = serde_json::to_value(row).expect("device row serializes");
        if event_type != CLIENT_UPDATED {
            data.as_object_mut().expect("object").retain(|key, _| {
                matches!(
                    key.as_str(),
                    "clientId" | "name" | "capabilities" | "principalId"
                )
            });
        }
        let _ = tx.send(ClientTransition::Device { event_type, data });
    }

    pub(super) fn refresh_devices(&mut self, tx: &mpsc::UnboundedSender<ClientTransition>) {
        let mut rows: Vec<(ReverseLiveClient, u64)> = Vec::new();
        let mut indexes = HashMap::new();
        for entry in &self.entries {
            let Some(device) = &entry.device else {
                continue;
            };
            let key = (&device.principal_id, &device.identity.client_id);
            let eligible = entry.channel.may_host_browser() && device.identity.browser_exec();
            let index = *indexes.entry(key).or_insert_with(|| {
                rows.push((
                    ReverseLiveClient {
                        client_id: device.identity.client_id.clone(),
                        principal_id: Some(device.principal_id.clone()),
                        host_role: None,
                        login: None,
                        display_name: None,
                        avatar_url: None,
                        identity: None,
                        name: None,
                        host: ClientHostInfo::default(),
                        capabilities: serde_json::json!({}),
                        connections: 0,
                        transports: Vec::new(),
                        connected_at: entry.connected_at.clone(),
                    },
                    0,
                ));
                rows.len() - 1
            });
            let (row, seq) = &mut rows[index];
            let browser_exec = eligible || row.capabilities["browserExec"] == true;
            row.connections += 1;
            row.transports.push(entry.transport.as_str().to_string());
            if device.hello_seq > *seq {
                *seq = device.hello_seq;
                row.name.clone_from(&device.identity.name);
                row.host.clone_from(&device.identity.host);
                row.capabilities = match &device.identity.capabilities {
                    Value::Object(_) => device.identity.capabilities.clone(),
                    _ => serde_json::json!({}),
                };
            }
            row.capabilities["browserExec"] = browser_exec.into();
        }
        let rows: Vec<_> = rows.into_iter().map(|(row, _)| row).collect();
        let key = |row: &ReverseLiveClient| (row.principal_id.clone(), row.client_id.clone());
        let old: HashMap<_, _> = self.devices.iter().map(|row| (key(row), row)).collect();
        let new: HashSet<_> = rows.iter().map(key).collect();
        for row in &self.devices {
            if !new.contains(&key(row)) {
                self.queue_device(tx, CLIENT_DISCONNECTED, row);
            }
        }
        for row in &rows {
            match old.get(&key(row)) {
                None => self.queue_device(tx, CLIENT_CONNECTED, row),
                Some(previous) if *previous != row => self.queue_device(tx, CLIENT_UPDATED, row),
                _ => {}
            }
        }
        self.devices = rows;
    }
}

impl PrimaryReverseGuard {
    pub(crate) fn refresh_devices(&self) {
        if let Some(inner) = &self.registry {
            inner.lock().refresh_devices(&inner.transitions);
        }
    }

    /// Bind only the transport's authenticated principal; hello person fields
    /// are never consumed. The caller has already reconciled browser authority.
    pub(crate) fn bind_device(&self, identity: ReverseClientIdentity, principal_id: PrincipalId) {
        let Some(inner) = &self.registry else { return };
        let mut state = inner.lock();
        let Some(entry) = state.entries.iter_mut().find(|e| e.id == self.id) else {
            return;
        };
        entry.device_managed = true;
        entry.device = Some(DeviceBinding {
            identity,
            principal_id,
            hello_seq: inner.next_hello_seq.fetch_add(1, Ordering::Relaxed) + 1,
        });
        state.refresh_devices(&inner.transitions);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn authenticated_devices_keep_principals_distinct_and_order_full_updates() {
        let registry = PrimaryReverseRegistry::new();
        let mut events = registry.take_transitions().unwrap();
        let channel = || ReverseChannel::new(mpsc::channel(4).0).with_administrator(false);
        let a = registry.register(channel(), ReverseTransport::Wss);
        let b = registry.register(channel(), ReverseTransport::Wss);
        let c = registry.register(channel(), ReverseTransport::Wss);
        let identity = |name: &str| ReverseClientIdentity {
            client_id: ClientId::from("same"),
            name: Some(name.into()),
            capabilities: json!({"browserExec":true}),
            host: ClientHostInfo::default(),
        };
        a.bind_device(identity("A"), PrincipalId::from("a"));
        b.bind_device(identity("B"), PrincipalId::from("b"));
        c.bind_device(identity("new A"), PrincipalId::from("a"));
        let rows = registry.authenticated_clients();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].connections, 2);
        assert_eq!(rows[0].name.as_deref(), Some("new A"));
        assert_eq!(rows[0].capabilities["browserExec"], false);
        assert!(registry.live_clients().is_empty());
        assert!(registry.primary().is_none());
        assert_eq!(events.try_recv().unwrap().event_type(), CLIENT_CONNECTED);
        assert_eq!(events.try_recv().unwrap().event_type(), CLIENT_CONNECTED);
        let updated = events.try_recv().unwrap();
        assert_eq!(updated.event_type(), CLIENT_UPDATED);
        assert_eq!(updated.data()["connections"], 2);
        drop(c);
        let updated = events.try_recv().unwrap();
        assert_eq!(updated.event_type(), CLIENT_UPDATED);
        assert_eq!(updated.data()["name"], "A");
        assert_eq!(updated.data()["connections"], 1);
        registry.client_profile_changed(&PrincipalId::from("b"));
        assert_eq!(events.try_recv().unwrap().data()["principalId"], "b");
        registry.client_principal_removed(&PrincipalId::from("a"));
        assert_eq!(events.try_recv().unwrap().event_type(), CLIENT_DISCONNECTED);
        drop(a);
        assert!(
            events.try_recv().is_err(),
            "revoked connection drop is idempotent"
        );
        assert_eq!(registry.authenticated_clients().len(), 1);
        drop(b);
        assert_eq!(events.try_recv().unwrap().event_type(), CLIENT_DISCONNECTED);
        assert!(registry.authenticated_clients().is_empty());
    }
}
