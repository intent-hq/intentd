//! The original ACP invocation's policy. ACP owns the only acquisition ledger;
//! this module binds its opaque records to real repository sources and consuming fences.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use intent_acp::mcp_server::private_results::{
    McpHostCall, McpPrivateAdmission, McpPrivateBoundary, McpPrivateHostScope, McpPrivatePolicy,
    McpReadEvidence, McpReadReservation, PreparedMcpTransfer,
};
use intent_acp::mcp_server::request_context::McpContextFuture;
use intent_core::{ExecutionScope, RepositoryConnectionScope};

use crate::observation_adapter::ConnectionObservations;
use crate::repository_admission::read_request::RepositoryReadRequest;
use crate::repository_admission::request_context::current_read_request;
use crate::repository_admission::{AdmissionError, AdmissionResult};
use crate::repository_read_source::{with_records, ReadRecord};
use crate::Services;

type Connections = HashMap<(ExecutionScope, RepositoryConnectionScope), ConnectionObservations>;

pub(crate) struct RepositoryReadPolicy {
    original: Arc<ReadContext>,
}

struct ReadContext {
    services: Arc<Services>,
    request: Arc<RepositoryReadRequest>,
    connections: Mutex<Connections>,
}

impl RepositoryReadPolicy {
    pub(crate) fn new(services: Arc<Services>, request: Arc<RepositoryReadRequest>) -> Self {
        Self {
            original: Arc::new(ReadContext {
                services,
                request,
                connections: Mutex::new(HashMap::new()),
            }),
        }
    }
}

/// Constructed only by the original ACP capture, before the host body's awaits.
#[derive(Clone)]
pub(crate) struct ReadHost {
    policy: Arc<ReadContext>,
    call: McpHostCall,
}

impl ReadHost {
    pub(crate) fn check_current(&self) -> AdmissionResult<()> {
        let current = current_read_request()?;
        if !Arc::ptr_eq(&current, &self.policy.request)
            || !current.retains(self.policy.services.as_ref())
        {
            return Err(AdmissionError::Denied);
        }
        current.check_current()
    }

    pub(crate) fn request(&self) -> &Arc<RepositoryReadRequest> {
        &self.policy.request
    }
    pub(crate) fn services(&self) -> &Arc<Services> {
        &self.policy.services
    }
    pub(crate) fn call(&self) -> &McpHostCall {
        &self.call
    }

    pub(crate) fn reserve(&self) -> AdmissionResult<McpReadReservation> {
        self.check_current()?;
        self.call.reserve().map_err(|_| AdmissionError::Retired)
    }

    pub(crate) fn connection(
        &self,
        execution: ExecutionScope,
        connection: RepositoryConnectionScope,
    ) -> AdmissionResult<ConnectionObservations> {
        self.check_current()?;
        let mut connections = self
            .policy
            .connections
            .lock()
            .map_err(|_| AdmissionError::Retired)?;
        Ok(connections
            .entry((execution.clone(), connection.clone()))
            .or_insert_with(|| ConnectionObservations::new(execution, connection))
            .clone())
    }
}

tokio::task_local! {
    static READ_HOST: ReadHost;
}

/// Call synchronously at the actual service entry, before `execution_call`. The
/// captured error stays an error; no future lookup can replace this host call.
pub(crate) fn capture(services: &Services) -> AdmissionResult<ReadHost> {
    let host = READ_HOST
        .try_with(Clone::clone)
        .map_err(|_| AdmissionError::Unavailable)?;
    if !std::ptr::eq(services, host.services().as_ref()) {
        return Err(AdmissionError::Denied);
    }
    host.check_current()?;
    Ok(host)
}

impl McpPrivateHostScope for ReadHost {
    fn scope<'a>(&'a self, body: McpContextFuture<'a>) -> McpContextFuture<'a> {
        Box::pin(READ_HOST.scope(self.clone(), body))
    }
}

// Retain this original policy allocation in each host. No re-selection or second
// request/connection factory occurs at body polling or at final admission.
impl McpPrivatePolicy for RepositoryReadPolicy {
    fn capture_host(&self, call: McpHostCall) -> Box<dyn McpPrivateHostScope> {
        Box::new(ReadHost {
            policy: self.original.clone(),
            call,
        })
    }

    fn admit<'a>(
        &'a self,
        boundary: &'a McpPrivateBoundary,
        originals: &'a [McpReadEvidence],
        packet: PreparedMcpTransfer<'a>,
    ) -> intent_js::BoxFuture<'a, McpPrivateAdmission> {
        Box::pin(async move {
            let Some(records) = originals
                .iter()
                .map(McpReadEvidence::downcast_ref::<ReadRecord>)
                .collect::<Option<Vec<_>>>()
            else {
                return McpPrivateAdmission::Refused;
            };
            with_records(
                &self.original.request,
                &self.original.services,
                &records,
                || packet.transfer(boundary),
            )
            .await
            .unwrap_or(McpPrivateAdmission::Refused)
        })
    }
}

impl Drop for ReadContext {
    fn drop(&mut self) {
        if let Ok(connections) = self.connections.get_mut() {
            for connection in connections.values() {
                connection.retire();
            }
        }
    }
}

#[cfg(test)]
#[path = "read_policy/tests.rs"]
mod tests;
