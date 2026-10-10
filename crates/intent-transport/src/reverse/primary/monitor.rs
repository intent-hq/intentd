//! Original ordinary hello generations own monitor attachment lifetimes.
//! Public client ids only select candidates inside an authenticated principal;
//! they never grant authority or permit rebinding to a replacement socket.
use std::sync::{Arc, Mutex, Weak};

use intent_core::{repository_request::ReviewMonitorHello, ClientId, PrincipalId};

use super::PrimaryReverseGuard;

#[derive(Default)]
pub(super) struct ConnectionHello {
    attachment: bool,
    ordinary_established: bool,
    attachment_source: Option<Weak<OrdinaryHello>>,
    ordinary: Option<Arc<OrdinaryHello>>,
    receipt: Option<Arc<dyn ReviewMonitorHello>>,
}

impl ConnectionHello {
    pub(super) fn retire(self) {
        if let Some(source) = self.ordinary {
            source.retire();
        }
        if let Some(receipt) = self.receipt {
            receipt.retire();
        }
    }
}

struct SourceState {
    live: bool,
    acknowledged: bool,
    children: Vec<Weak<dyn ReviewMonitorHello>>,
}

pub(crate) struct OrdinaryHello {
    client_id: ClientId,
    principal: PrincipalId,
    state: Mutex<SourceState>,
}

impl OrdinaryHello {
    pub(crate) fn acknowledge(&self) {
        let mut state = self.state.lock().unwrap();
        if state.live {
            state.acknowledged = true;
        }
    }

    fn ready(&self) -> bool {
        let state = self.state.lock().unwrap();
        state.live && state.acknowledged
    }

    fn retire(&self) {
        let children = {
            let mut state = self.state.lock().unwrap();
            state.live = false;
            std::mem::take(&mut state.children)
        };
        // Retirement can join source fences: never call it under registry or
        // source locks, including shutdown and physical connection drop.
        for child in children.into_iter().filter_map(|child| child.upgrade()) {
            child.retire();
        }
    }
}

pub(crate) struct AttachmentHello {
    source: Arc<OrdinaryHello>,
    receipt: Arc<dyn ReviewMonitorHello>,
}

impl AttachmentHello {
    pub(crate) fn acknowledge(&self) {
        if self.source.ready() {
            // The concrete receipt is permanently closed by retire(), so a
            // racing source retirement cannot be undone by a late ack.
            self.receipt.acknowledge();
        }
    }
}

impl PrimaryReverseGuard {
    /// Called on every hello before parsing/handling can mutate a profile.
    /// Preserve the established socket role while retiring the old generation.
    pub(crate) fn retire_monitor_hello(&self) {
        let Some(inner) = &self.registry else { return };
        let previous = {
            let mut state = inner.lock();
            let Some(entry) = state.entries.iter_mut().find(|entry| entry.id == self.id) else {
                return;
            };
            let previous = std::mem::take(&mut entry.monitor_hello);
            entry.monitor_hello.attachment = previous.attachment;
            entry.monitor_hello.ordinary_established = previous.ordinary_established;
            entry
                .monitor_hello
                .attachment_source
                .clone_from(&previous.attachment_source);
            previous
        };
        previous.retire();
    }

    pub(crate) fn is_monitor_attachment(&self) -> bool {
        self.registry.as_ref().is_some_and(|inner| {
            inner
                .lock()
                .entries
                .iter()
                .any(|entry| entry.id == self.id && entry.monitor_hello.attachment)
        })
    }

    /// Install an ordinary source after the actual hello handler succeeds.
    /// It remains unusable by attachments until response acknowledgement.
    pub(crate) fn prepare_ordinary_monitor_source(
        &self,
        client_id: ClientId,
        principal: PrincipalId,
    ) -> Option<Arc<OrdinaryHello>> {
        let inner = self.registry.as_ref()?;
        let mut state = inner.lock();
        let entry = state.entries.iter_mut().find(|entry| entry.id == self.id)?;
        if entry.monitor_hello.attachment || entry.monitor_hello.ordinary.is_some() {
            return None;
        }
        let source = Arc::new(OrdinaryHello {
            client_id,
            principal,
            state: Mutex::new(SourceState {
                live: true,
                acknowledged: false,
                children: Vec::new(),
            }),
        });
        entry.monitor_hello.ordinary = Some(source.clone());
        entry.monitor_hello.ordinary_established = true;
        Some(source)
    }

    pub(crate) fn prepare_monitor_attachment(
        &self,
        client_id: &ClientId,
        principal: &PrincipalId,
        receipt: Arc<dyn ReviewMonitorHello>,
    ) -> Option<AttachmentHello> {
        let inner = self.registry.as_ref()?;
        let mut state = inner.lock();
        let pos = state.entries.iter().position(|entry| entry.id == self.id)?;
        let entry = &state.entries[pos];
        // Existing ordinary sockets cannot be converted, even after failed
        // rehello. Device state covers non-browser-capable ordinary sources.
        if entry.monitor_hello.ordinary_established
            || entry.identity.is_some()
            || entry.device.is_some()
        {
            return None;
        }
        let mut sources = state
            .entries
            .iter()
            .filter(|entry| entry.id != self.id)
            .filter_map(|entry| entry.monitor_hello.ordinary.as_ref())
            .filter(|source| {
                &source.client_id == client_id && &source.principal == principal && source.ready()
            });
        let source = sources.next()?.clone();
        if sources.next().is_some() {
            return None;
        }
        if let Some(original) = &state.entries[pos].monitor_hello.attachment_source {
            if !original
                .upgrade()
                .is_some_and(|original| Arc::ptr_eq(&original, &source))
            {
                return None;
            }
        }
        {
            let mut source_state = source.state.lock().unwrap();
            if !source_state.live || !source_state.acknowledged {
                return None;
            }
            source_state
                .children
                .retain(|child| child.strong_count() != 0);
            source_state.children.push(Arc::downgrade(&receipt));
        }
        state.entries[pos].monitor_hello.attachment = true;
        state.entries[pos].monitor_hello.attachment_source = Some(Arc::downgrade(&source));
        state.entries[pos].monitor_hello.receipt = Some(receipt.clone());
        Some(AttachmentHello { source, receipt })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reverse::{
        PrimaryReverseRegistry, ReverseChannel, ReverseClientIdentity, ReverseTransport,
    };
    use intent_core::{AgentReverseDispatch, ClientHostInfo};
    use serde_json::json;
    use tokio::sync::mpsc;

    #[derive(Default)]
    struct Receipt(Mutex<(bool, bool)>);
    impl ReviewMonitorHello for Receipt {
        fn acknowledge(&self) {
            let mut state = self.0.lock().unwrap();
            if !state.1 {
                state.0 = true;
            }
        }
        fn retire(&self) {
            *self.0.lock().unwrap() = (false, true);
        }
    }
    fn socket(registry: &PrimaryReverseRegistry) -> PrimaryReverseGuard {
        registry.register(
            ReverseChannel::new(mpsc::channel(8).0),
            ReverseTransport::Wss,
        )
    }

    #[test]
    fn monitor_attachments_require_unique_acknowledged_source_and_retire_independently() {
        let registry = PrimaryReverseRegistry::new();
        let source = socket(&registry);
        let a = socket(&registry);
        let b = socket(&registry);
        let id = ClientId::from_string("desktop");
        let principal = PrincipalId::from_string("owner");
        let identity = ReverseClientIdentity {
            client_id: id.clone(),
            name: Some("Original desktop".into()),
            capabilities: json!({"browserExec":true}),
            host: ClientHostInfo::default(),
        };
        source.bind_device(identity.clone(), principal.clone());
        source.bind(identity);
        let before = registry.authenticated_clients();
        let live_before = registry.live_clients();
        let hello = source
            .prepare_ordinary_monitor_source(id.clone(), principal.clone())
            .unwrap();
        let first = Arc::new(Receipt::default());
        assert!(a
            .prepare_monitor_attachment(&id, &principal, first.clone())
            .is_none());
        hello.acknowledge();
        assert!(a
            .prepare_monitor_attachment(&id, &PrincipalId::from_string("foreign"), first.clone())
            .is_none());
        let admitted_a = a
            .prepare_monitor_attachment(&id, &principal, first.clone())
            .unwrap();
        let second = Arc::new(Receipt::default());
        let admitted_b = b
            .prepare_monitor_attachment(&id, &principal, second.clone())
            .unwrap();
        admitted_a.acknowledge();
        admitted_b.acknowledge();
        assert_eq!(*first.0.lock().unwrap(), (true, false));
        assert_eq!(registry.authenticated_clients(), before);
        assert_eq!(registry.live_clients(), live_before);
        assert!(a.bound_client_id().is_none());
        assert!(b.bound_client_id().is_none());
        drop(a);
        admitted_a.acknowledge();
        assert_eq!(*first.0.lock().unwrap(), (false, true));
        assert_eq!(*second.0.lock().unwrap(), (true, false));
        source.retire_monitor_hello();
        admitted_b.acknowledge();
        assert_eq!(*second.0.lock().unwrap(), (false, true));
        assert_eq!(registry.authenticated_clients(), before);
        assert!(
            b.is_monitor_attachment(),
            "failed/rehello cannot become ordinary"
        );
        assert!(b
            .prepare_ordinary_monitor_source(id.clone(), principal.clone())
            .is_none());
        assert!(b
            .prepare_monitor_attachment(&id, &principal, Arc::new(Receipt::default()))
            .is_none());

        let next = source
            .prepare_ordinary_monitor_source(id.clone(), principal.clone())
            .unwrap();
        next.acknowledge();
        let duplicate = socket(&registry);
        let duplicate_hello = duplicate
            .prepare_ordinary_monitor_source(id.clone(), principal.clone())
            .unwrap();
        duplicate_hello.acknowledge();
        assert!(b
            .prepare_monitor_attachment(&id, &principal, Arc::new(Receipt::default()))
            .is_none());
        drop(duplicate);
        assert!(
            b.prepare_monitor_attachment(&id, &principal, Arc::new(Receipt::default()))
                .is_none(),
            "old document cannot reparent to replacement source generation"
        );
        let c = socket(&registry);
        let third = Arc::new(Receipt::default());
        let admitted = c
            .prepare_monitor_attachment(&id, &principal, third.clone())
            .unwrap();
        drop(source);
        admitted.acknowledge();
        assert_eq!(*third.0.lock().unwrap(), (false, true));
        assert!(registry.authenticated_clients().is_empty());
        assert!(registry.live_clients().is_empty());
    }
}
