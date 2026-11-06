//! Private, connection-bound provenance for accepted human messages.
//!
//! Never deserialized from RPC parameters or transcript metadata. The transport
//! captures the hello identity before spawning the request; spawned background
//! tasks do not inherit this scope. Acceptance is marked only after a new queue
//! insertion or transcript append, then persisted before delivery can start.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use crate::{AgentId, Caller, ClientId};

pub struct BrowserMessageIngress {
    pub client_id: Option<ClientId>,
    pub received_order: String,
    accepted: Mutex<HashMap<AgentId, String>>,
}

impl BrowserMessageIngress {
    #[must_use]
    pub fn new(client_id: Option<ClientId>) -> Self {
        Self {
            client_id,
            received_order: uuid::Uuid::now_v7().to_string(),
            accepted: Mutex::new(HashMap::new()),
        }
    }
}

tokio::task_local! {
    static INGRESS: Option<BrowserMessageIngress>;
}

pub fn with_browser_message_ingress<F: Future>(
    ingress: Option<BrowserMessageIngress>,
    future: F,
) -> impl Future<Output = F::Output> {
    INGRESS.scope(ingress, future)
}

/// Called only on an accepted human submission, never on queue replay/drain.
pub fn accept_browser_message(agent_id: &AgentId, submission_id: &str) {
    if !matches!(crate::current_caller(), Some(Caller::Wire { .. })) {
        return;
    }
    let _ = INGRESS.try_with(|ingress| {
        if let Some(ingress) = ingress {
            if let Ok(mut accepted) = ingress.accepted.lock() {
                accepted
                    .entry(agent_id.clone())
                    .or_insert_with(|| submission_id.to_owned());
            }
        }
    });
}

/// The accepted submission and its trusted receiving connection. A missing
/// hello is an explicit unknown sender, which clears an older preference.
#[must_use]
pub fn accepted_browser_message(agent_id: &AgentId) -> Option<(String, String, Option<ClientId>)> {
    INGRESS
        .try_with(|ingress| {
            let ingress = ingress.as_ref()?;
            let id = ingress.accepted.lock().ok()?.get(agent_id)?.clone();
            Some((
                id,
                ingress.received_order.clone(),
                ingress.client_id.clone(),
            ))
        })
        .ok()
        .flatten()
}
