//! Original-connection history bootstrap. These correlations constrain separate
//! Services admission; a preparation neither admits history nor proves delivery.
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::json;

use super::{Generation, ReviewMonitoringContext};
use crate::callback_registration::{CallbackClient, CallbackSession};
use crate::error::{AcpError, AcpResult};
use crate::session::{LoadSessionResponse, McpServer, Meta};
use crate::transport::Connection;

pub use crate::transport::review_history::{
    PreparedReviewHistoryLoad, ReviewHistoryLoadAdmission, ReviewHistoryLoadBoundary,
    ReviewHistoryLoadReceipt, ReviewHistoryLoadTarget,
};

const PREPARE: &str = "_intent/session/prepare_review_history";
const TIMEOUT: Duration = Duration::from_secs(30);

/// Original private Store correlation, never an account or transfer receipt.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReviewHistoryMessage {
    /// Exact persisted original message identifier.
    pub message_id: String,
    /// Opaque digest including the private original Store binding.
    pub store_envelope_digest: String,
    /// Receiver-verifiable exact accepted ACP payload digest.
    pub acp_payload_digest: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct PreparedResponse {
    preparation_id: String,
    snapshot_digest: String,
    monitor_messages: Vec<ReviewHistoryMessage>,
}

/// Consuming original preparation, held only in memory. A caller must freshly
/// admit every returned message/source before consuming it to load native input.
/// No body or private binding is returned by the peer.
pub struct PreparedReviewHistory {
    connection: Arc<Connection>,
    generation: Arc<Generation>,
    context: ReviewMonitoringContext,
    session_id: String,
    response: PreparedResponse,
    deadline: Instant,
}

fn unavailable() -> AcpError {
    AcpError::Protocol("Review monitoring context unavailable".into())
}
fn bounded(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256
}
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl CallbackClient {
    /// Close local eligibility synchronously, before returning any future that
    /// waits for remote retirement. The caller retains the returned operation
    /// until its exact allocation is joined; dropping it is not completion.
    ///
    /// # Errors
    /// Refuses an unsupported client or invalid original session identifier.
    pub fn begin_review_context_retirement(
        &self,
        session: &str,
        context: &ReviewMonitoringContext,
    ) -> AcpResult<ReviewContextRetirement> {
        let generation = self.monitoring.as_ref().ok_or_else(unavailable)?;
        if !bounded(session) {
            return Err(unavailable());
        }
        let shared = generation.retire_context(session, context);
        Ok(ReviewContextRetirement {
            connection: self.connection.clone(),
            session: session.into(),
            context: context.clone(),
            shared,
        })
    }

    /// Prepare without Query spawn or callback ACK. The peer pins and classifies
    /// the full original snapshot; Services still must verify private provenance
    /// and independently admit the complete source set before calling load.
    ///
    /// # Errors
    /// Rejects absent negotiation, retired generation and malformed peer output.
    pub async fn prepare_review_history(
        &self,
        session_id: &str,
        context: ReviewMonitoringContext,
    ) -> AcpResult<PreparedReviewHistory> {
        let generation = self
            .monitoring
            .as_ref()
            .filter(|g| g.current())
            .ok_or_else(unavailable)?
            .clone();
        if !bounded(session_id) || !generation.context_current(session_id, &context) {
            return Err(unavailable());
        }
        let deadline = Instant::now() + TIMEOUT;
        let raw=self.connection.request_timeout(PREPARE,json!({
            "sessionId":session_id,"ownerKey":context.owner_key,"loadAttemptId":context.load_attempt_id,
        }),TIMEOUT).await?;
        let response: PreparedResponse = serde_json::from_value(raw).map_err(|_| unavailable())?;
        let mut messages = std::collections::HashSet::new();
        if !generation.context_current(session_id, &context)
            || Instant::now() >= deadline
            || !bounded(&response.preparation_id)
            || !digest(&response.snapshot_digest)
            || response.monitor_messages.len() > 4096
            || response.monitor_messages.iter().any(|message| {
                !bounded(&message.message_id)
                    || !digest(&message.store_envelope_digest)
                    || !digest(&message.acp_payload_digest)
                    || !messages.insert(message.message_id.clone())
            })
        {
            return Err(unavailable());
        }
        Ok(PreparedReviewHistory {
            connection: self.connection.clone(),
            generation,
            context,
            session_id: session_id.into(),
            response,
            deadline,
        })
    }
}

impl PreparedReviewHistory {
    /// Required original correlations for independent all-source admission. An
    /// empty slice is the peer's proven-ordinary claim, never missing metadata.
    #[must_use]
    pub fn messages(&self) -> &[ReviewHistoryMessage] {
        &self.response.monitor_messages
    }

    /// Retain the exact connection and complete load parameters before acquiring
    /// source authority. The returned target cannot mutate or send a request.
    ///
    /// # Errors
    /// Refuses conflicting metadata or an unencodable load request.
    pub fn load_target(
        &self,
        cwd: impl Into<std::path::PathBuf>,
        mcp_servers: Vec<McpServer>,
        meta: Option<Meta>,
    ) -> AcpResult<ReviewHistoryLoadTarget> {
        let request = self.load_request(cwd.into(), mcp_servers, meta)?;
        Ok(ReviewHistoryLoadTarget::new(
            &self.connection,
            serde_json::to_value(request)?,
        ))
    }

    fn load_request(
        &self,
        cwd: std::path::PathBuf,
        mcp_servers: Vec<McpServer>,
        meta: Option<Meta>,
    ) -> AcpResult<agent_client_protocol::schema::v1::LoadSessionRequest> {
        use agent_client_protocol::schema::v1::{LoadSessionRequest, SessionId};
        let mut meta = meta.unwrap_or_default();
        if meta.contains_key("reviewMonitoringHistory")
            || meta.contains_key("reviewMonitoringContext")
        {
            return Err(unavailable());
        }
        self.context.insert_into(&mut meta);
        meta.insert("reviewMonitoringHistory".into(), json!({"version":1,
            "preparationId":self.response.preparation_id,"snapshotDigest":self.response.snapshot_digest,
            "loadAttemptId":self.context.load_attempt_id,
        }));
        let mut request = LoadSessionRequest::new(SessionId::new(self.session_id.clone()), cwd)
            .mcp_servers(mcp_servers);
        request.meta = Some(meta);
        Ok(request)
    }

    /// Consume this exact prepared snapshot once, only after independent fresh
    /// admission. There is deliberately no ordinary/raw-load fallback here.
    ///
    /// # Errors
    /// Rejects foreign/retired client, expired preparation and ordinary load errors.
    pub async fn load(
        self,
        client: &CallbackClient,
        cwd: impl Into<std::path::PathBuf>,
        mcp_servers: Vec<McpServer>,
        meta: Option<Meta>,
        admission: Box<dyn ReviewHistoryLoadAdmission>,
    ) -> AcpResult<CallbackSession<LoadSessionResponse>> {
        if !Arc::ptr_eq(&self.connection, &client.connection)
            || !client
                .monitoring
                .as_ref()
                .is_some_and(|g| Arc::ptr_eq(g, &self.generation))
            || !self
                .generation
                .context_current(&self.session_id, &self.context)
            || Instant::now() >= self.deadline
        {
            return Err(unavailable());
        }
        let request = self.load_request(cwd.into(), mcp_servers, meta)?;
        client
            .load_prepared_review_history(request, &self.generation, self.deadline, admission)
            .await
    }
}

impl CallbackClient {
    /// Retire only the original allocation named by the retained startup context.
    /// Close local delivery eligibility before awaiting the peer's joined result.
    /// Failure/peer loss is not proof that the remote allocation has joined.
    ///
    /// # Errors
    /// Rejects unsupported clients, malformed responses, and failed original joins.
    pub async fn retire_review_context(
        &self,
        session: &str,
        context: &ReviewMonitoringContext,
    ) -> AcpResult<()> {
        self.begin_review_context_retirement(session, context)?
            .join()
            .await
    }
}

/// One waiter for the original remote retirement operation. Local closure is
/// already complete; the generation retains its shared terminal result. Dropping
/// this waiter starts or preserves the independent join, never settles it.
#[must_use = "local closure is not remote retirement; retain and join this operation"]
pub struct ReviewContextRetirement {
    connection: Arc<Connection>,
    session: String,
    context: ReviewMonitoringContext,
    shared: Arc<SharedRetirement>,
}

/// The original generation retains this record after local leaves disappear.
/// Negative, lost and malformed replies are terminal uncertainty, never a retry.
pub(super) struct SharedRetirement {
    started: std::sync::atomic::AtomicBool,
    completed: tokio::sync::watch::Sender<Option<bool>>,
}
impl Default for SharedRetirement {
    fn default() -> Self {
        Self {
            started: std::sync::atomic::AtomicBool::new(false),
            completed: tokio::sync::watch::channel(None).0,
        }
    }
}

struct RetirementCompletion(Arc<SharedRetirement>);
impl Drop for RetirementCompletion {
    fn drop(&mut self) {
        // A panic, runtime cancellation or lost worker is not a native join.
        self.0.completed.send_if_modified(|state| {
            if state.is_none() {
                *state = Some(false);
                true
            } else {
                false
            }
        });
    }
}

impl ReviewContextRetirement {
    /// Join exactly the captured native allocation, never a current session.
    ///
    /// # Errors
    /// Peer loss, timeout, malformed/negative response are unconfirmed retirement.
    pub async fn join(self) -> AcpResult<()> {
        let mut completed = self.shared.completed.subscribe();
        self.start();
        loop {
            let result = *completed.borrow_and_update();
            if let Some(joined) = result {
                return if joined { Ok(()) } else { Err(unavailable()) };
            }
            if completed.changed().await.is_err() {
                return Err(unavailable());
            }
        }
    }

    fn start(&self) {
        use std::sync::atomic::Ordering;
        if self.shared.started.swap(true, Ordering::SeqCst) {
            return;
        }
        let completion = RetirementCompletion(self.shared.clone());
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let connection = self.connection.clone();
        let session = self.session.clone();
        let context = self.context.clone();
        // This worker owns the original request after every response waiter is
        // cancelled. No registry, Store or credential lock crosses its await.
        runtime.spawn(async move {
            let result = async {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Retired {
                    retired: bool,
                }
                let response = connection
                    .request_timeout(
                        "_intent/session/retire_review_context",
                        json!({"sessionId":session,"loadAttemptId":context.load_attempt_id}),
                        TIMEOUT,
                    )
                    .await?;
                let response: Retired =
                    serde_json::from_value(response).map_err(|_| unavailable())?;
                if !response.retired {
                    return Err(unavailable());
                }
                Ok::<_, AcpError>(())
            }
            .await;
            completion.0.completed.send_replace(Some(result.is_ok()));
        });
    }
}

impl Drop for ReviewContextRetirement {
    fn drop(&mut self) {
        self.start();
    }
}
