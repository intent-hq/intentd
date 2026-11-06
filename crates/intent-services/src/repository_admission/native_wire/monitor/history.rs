//! One original native chat subscription; compatibility never supplies access.
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use intent_core::repository_request::{RepositoryMonitorHistoryRecipient, RepositoryPrivateOutput};
use intent_core::{AgentId, AgentMessage, BoxFuture, Result};

use super::super::{unavailable, Connection, Request, RequestState, Services};
use super::{Projection, Receipt, RepositoryRetirement};

tokio::task_local! {
    static HISTORY: Arc<History>;
}

pub(super) struct History {
    weak: Weak<Self>,
    connection: Arc<Connection>,
    receipt: Arc<Receipt>,
    lifetime: RepositoryRetirement,
    projections: Mutex<HashMap<(AgentId, String), Arc<Projection>>>,
}

pub(crate) fn capture(
    connection: &Connection,
) -> Option<Arc<dyn RepositoryMonitorHistoryRecipient>> {
    connection.entered().ok()?;
    let receipt = connection.monitor.capture()?;
    if !receipt.capable() {
        return None;
    }
    let owner = Arc::new_cyclic(|weak| History {
        weak: weak.clone(),
        connection: connection.weak.upgrade().expect("owned connection"),
        receipt,
        lifetime: RepositoryRetirement::default(),
        projections: Mutex::new(HashMap::new()),
    });
    connection.parent.native_link(&owner.lifetime).ok()?;
    owner.receipt.lifetime.native_link(&owner.lifetime).ok()?;
    Some(owner)
}

pub(super) fn current() -> Option<Arc<History>> {
    HISTORY.try_with(Clone::clone).ok()
}

impl History {
    fn with_current(&self, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        self.lifetime
            .native_dispatch(|| {
                if self.connection.parent.native_closed() || self.receipt.lifetime.native_closed() {
                    return Err(super::super::AdmissionError::Retired);
                }
                Ok(action())
            })
            .map_err(|_| unavailable())?
    }

    pub(super) fn request(&self, services: &Services) -> Result<Arc<Request>> {
        if !std::ptr::eq(self.connection.services.as_ref(), services) {
            return Err(unavailable());
        }
        self.with_current(&mut || Ok(()))?;
        let request = self.connection.capture_native(None);
        request.check().map_err(|_| unavailable())?;
        if !request
            .monitor_receipt
            .as_ref()
            .is_some_and(|r| Arc::ptr_eq(r, &self.receipt))
        {
            return Err(unavailable());
        }
        *request.state.lock().map_err(|_| unavailable())? = RequestState::Pending;
        Ok(request)
    }

    pub(super) fn retain(
        &self,
        agent: &AgentId,
        messages: &HashMap<String, AgentMessage>,
        projection: &Arc<Projection>,
    ) -> Result<()> {
        self.with_current(&mut || {
            let mut current = self.projections.lock().map_err(|_| unavailable())?;
            for id in messages.keys() {
                current.insert((agent.clone(), id.clone()), projection.clone());
            }
            Ok(())
        })
    }
}

impl RepositoryMonitorHistoryRecipient for History {
    fn scope<'a>(&'a self, body: BoxFuture<'a, ()>) -> BoxFuture<'a, ()> {
        Box::pin(HISTORY.scope(
            self.weak.upgrade().expect("owned history subscription"),
            body,
        ))
    }

    fn bind_output(&self, frame: String) -> Arc<dyn RepositoryPrivateOutput> {
        // Snapshot the exact read owners for this immutable frame. Replacing a
        // generation never changes already queued output or revives its leaves.
        let projections = self
            .projections
            .lock()
            .map(|p| p.values().cloned().collect::<Vec<_>>())
            .ok();
        let fence = RepositoryRetirement::default();
        let mut parents = vec![
            self.lifetime.clone(),
            self.connection.parent.clone(),
            self.receipt.lifetime.clone(),
        ];
        let mut admissions = Vec::new();
        if let Some(projections) = &projections {
            for projection in projections {
                let (originals, accounts) = projection.history_inputs();
                parents.extend(originals);
                admissions.extend(accounts);
            }
        }
        let valid = projections.is_some()
            && parents
                .iter()
                .all(|parent| parent.native_link(&fence).is_ok());
        Arc::new(Output {
            fence,
            parents,
            admissions,
            valid,
            frame,
            _projections: projections.unwrap_or_default(),
        })
    }

    fn begin_generation(&self) {
        self.projections
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }

    fn retire(&self) {
        self.lifetime.end_scope();
    }
}

struct Output {
    _projections: Vec<Arc<Projection>>,
    fence: RepositoryRetirement,
    parents: Vec<RepositoryRetirement>,
    admissions:
        Vec<Arc<crate::source_control_auth_ops::repository_owner::RepositoryReadEligibility>>,
    valid: bool,
    frame: String,
}
impl RepositoryPrivateOutput for Output {
    fn frame(&self) -> &str {
        &self.frame
    }
    fn with_current(&self, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        use crate::source_control_auth_ops::repository_owner::RepositoryReadEligibility;
        self.fence
            .native_dispatch(|| {
                if !self.valid || self.parents.iter().any(RepositoryRetirement::native_closed) {
                    return Err(super::super::AdmissionError::Retired);
                }
                if self.admissions.is_empty() {
                    return Ok(action());
                }
                let accounts = self.admissions.iter().map(Arc::as_ref).collect::<Vec<_>>();
                let mut result = None;
                RepositoryReadEligibility::with_all_current_and_facts(&accounts, None, |_| {
                    result = Some(action());
                    Ok(())
                })
                .map_err(|_| super::super::AdmissionError::Retired)?;
                result.ok_or(super::super::AdmissionError::Retired)
            })
            .map_err(|_| unavailable())?
    }
}
