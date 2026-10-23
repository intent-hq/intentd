//! Required queue admission for the exact prepared history load. This path
//! cannot select another method, rewrite the payload, or fall back to raw load.
use super::{
    catch_unwind, encode_message, mpsc, oneshot, prompt_response, AcpError, AcpResult, Arc,
    AssertUnwindSafe, AtomicBool, Connection, Ordering, PendingEntryGuard, PendingRequest, Value,
};
use crate::review_monitoring::{Generation, ReviewMonitoringContext};

/// Complete original destination and encoded load parameters. Equality only
/// constrains independent authority; public IDs do not grant admission.
pub struct ReviewHistoryLoadBoundary {
    writer: mpsc::Sender<String>,
    params: Value,
}

/// Immutable destination minted by an original history preparation. This is
/// comparison evidence for a separate admission owner, never authority itself.
pub struct ReviewHistoryLoadTarget {
    writer: mpsc::Sender<String>,
    params: Value,
}
impl ReviewHistoryLoadTarget {
    pub(crate) fn new(connection: &Connection, params: Value) -> Self {
        Self {
            writer: connection.writer_tx.clone(),
            params,
        }
    }

    /// Match the complete prepared envelope at its actual queue boundary.
    #[must_use]
    pub fn matches(&self, boundary: &ReviewHistoryLoadBoundary) -> bool {
        self.writer.same_channel(&boundary.writer) && self.params == boundary.params
    }
}
impl ReviewHistoryLoadBoundary {
    /// Compare with the retained original connection and full load envelope.
    #[must_use]
    pub fn matches(&self, connection: &Connection, params: &Value) -> bool {
        self.writer.same_channel(&connection.writer_tx) && &self.params == params
    }
}

/// Proof of this one queue insertion, never native execution or delivery ACK.
pub struct ReviewHistoryLoadReceipt(());

/// Required original all-source authority. Implementations must retain their
/// original guards while consuming the packet. No await or reconstructed
/// authority is permitted at this final queue boundary.
pub trait ReviewHistoryLoadAdmission: Send + Sync {
    /// Refusal must not consume the packet. Returning a receipt requires the
    /// exact original transfer; there is no ordinary variant.
    ///
    /// # Errors
    /// Returns the original admission refusal or a retired transfer error.
    fn admit(
        &self,
        boundary: &ReviewHistoryLoadBoundary,
        packet: PreparedReviewHistoryLoad<'_>,
    ) -> AcpResult<ReviewHistoryLoadReceipt>;
}

/// A borrowed, pre-encoded, one-use writer slot. No public constructor, Clone,
/// payload mutation, queue reservation, serialization or I/O in `transfer`.
pub struct PreparedReviewHistoryLoad<'a> {
    boundary: &'a ReviewHistoryLoadBoundary,
    generation: &'a Arc<Generation>,
    deadline: tokio::time::Instant,
    auth_required: &'a AtomicBool,
    slot: &'a mut Option<(mpsc::OwnedPermit<String>, String)>,
}
impl PreparedReviewHistoryLoad<'_> {
    /// Consume only the original slot while the caller holds all source guards.
    /// Retirement joins this synchronous transfer. Prior queue insertion cannot
    /// be recalled and is not proof that the peer loaded or acknowledged it.
    ///
    /// # Errors
    /// Refuses a foreign boundary, expired preparation or retired connection.
    pub fn transfer(
        self,
        boundary: &ReviewHistoryLoadBoundary,
    ) -> AcpResult<ReviewHistoryLoadReceipt> {
        if !std::ptr::eq(self.boundary, boundary) {
            return Err(unavailable());
        }
        let session = boundary
            .params
            .get("sessionId")
            .and_then(Value::as_str)
            .ok_or_else(unavailable)?;
        let meta = boundary.params.get("_meta").and_then(Value::as_object);
        let context = ReviewMonitoringContext::parse(meta).ok_or_else(unavailable)?;
        self.generation.transfer_prepared(session, &context, || {
            if tokio::time::Instant::now() >= self.deadline
                || boundary.writer.is_closed()
                || self.auth_required.load(Ordering::SeqCst)
            {
                return Err(unavailable());
            }
            let (permit, line) = self.slot.take().ok_or_else(unavailable)?;
            permit.send(line);
            Ok(ReviewHistoryLoadReceipt(()))
        })
    }
}
fn unavailable() -> AcpError {
    AcpError::Protocol("Review monitoring context unavailable".into())
}

impl Connection {
    pub(crate) async fn request_review_history_load(
        &self,
        params: Value,
        generation: &Arc<Generation>,
        deadline: std::time::Instant,
        admission: Box<dyn ReviewHistoryLoadAdmission>,
    ) -> AcpResult<Value> {
        let deadline = tokio::time::Instant::from_std(deadline);
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, mut rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(
            id,
            PendingRequest {
                sender: tx,
                session_id: params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            },
        );
        let _guard = PendingEntryGuard {
            pending: self.pending.clone(),
            id,
        };
        if self.auth_required.load(Ordering::SeqCst) || !generation.current() {
            return Err(unavailable());
        }
        let line = encode_message(Some(id), "session/load", &params)?;
        let permit = tokio::select! {
            biased;
            _ = &mut rx => return Err(unavailable()),
            () = tokio::time::sleep_until(deadline) => return Err(unavailable()),
            permit = self.writer_tx.clone().reserve_owned() => permit.map_err(|_| unavailable())?,
        };
        let boundary = ReviewHistoryLoadBoundary {
            writer: self.writer_tx.clone(),
            params,
        };
        let mut slot = Some((permit, line));
        let packet = PreparedReviewHistoryLoad {
            boundary: &boundary,
            generation,
            deadline,
            auth_required: &self.auth_required,
            slot: &mut slot,
        };
        let result = catch_unwind(AssertUnwindSafe(|| admission.admit(&boundary, packet)));
        // A transferred effect remains possible even if owner code subsequently
        // fails. Never retry or fall back; the caller owns exact remote retirement.
        match result {
            Ok(Ok(_)) if slot.is_none() => {}
            Ok(Err(error)) => return Err(error),
            _ => return Err(unavailable()),
        }
        match tokio::time::timeout_at(deadline, rx).await {
            Ok(response) => prompt_response(response),
            Err(_) => Err(AcpError::Timeout("session/load".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        mpsc, unavailable, AcpResult, Arc, AtomicBool, Connection, Ordering,
        PreparedReviewHistoryLoad, ReviewHistoryLoadAdmission, ReviewHistoryLoadBoundary,
        ReviewHistoryLoadReceipt, Value,
    };
    use crate::ConnectionHooks;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    struct Source {
        connection: Arc<Connection>,
        params: Value,
        current: Arc<AtomicBool>,
        calls: Arc<AtomicUsize>,
    }
    impl ReviewHistoryLoadAdmission for Source {
        fn admit(
            &self,
            boundary: &ReviewHistoryLoadBoundary,
            packet: PreparedReviewHistoryLoad<'_>,
        ) -> AcpResult<ReviewHistoryLoadReceipt> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.current.load(Ordering::SeqCst)
                || !boundary.matches(&self.connection, &self.params)
            {
                return Err(unavailable());
            }
            packet.transfer(boundary)
        }
    }
    // Hold the exact outbound queue independently of the peer's response reader.
    // This tests transport queue ordering, not native admission or execution.
    fn controlled_queue() -> (
        Arc<Connection>,
        mpsc::Receiver<String>,
        tokio::io::DuplexStream,
        mpsc::Sender<String>,
    ) {
        let (client, peer) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(client);
        let mut connection = Connection::new(write, read, None, ConnectionHooks::default());
        let (queue, receiver) = mpsc::channel(1);
        let ordinary_writer = connection.writer_tx.clone();
        connection.writer_tx = queue;
        (Arc::new(connection), receiver, peer, ordinary_writer)
    }

    #[tokio::test]
    async fn history_load_rechecks_original_source_after_waiting_for_writer_capacity() {
        for retirement in ["source", "generation", "authentication", "caller-drop"] {
            let (connection, mut queue, _peer, _ordinary_writer) = controlled_queue();
            let generation = connection.review_monitoring.lock().unwrap().begin();
            connection.writer_tx.send("held".into()).await.unwrap();
            let current = Arc::new(AtomicBool::new(true));
            let calls = Arc::new(AtomicUsize::new(0));
            let params = serde_json::json!({"sessionId":"original","cwd":"/fixture","mcpServers":[],"_meta":{"reviewMonitoringContext":{"version":1,"ownerKey":"owner","loadAttemptId":"attempt"},"reviewMonitoringHistory":{"version":1,"preparationId":"held","snapshotDigest":"a".repeat(64),"loadAttemptId":"attempt"}}});
            let admission = Source {
                connection: connection.clone(),
                params: params.clone(),
                current: current.clone(),
                calls: calls.clone(),
            };
            let call = connection.request_review_history_load(
                params,
                &generation,
                std::time::Instant::now()
                    + intentd_test_support::test_timeout(Duration::from_secs(5)),
                Box::new(admission),
            );
            let mut call = Box::pin(call);
            assert!(tokio::time::timeout(
                intentd_test_support::test_timeout(Duration::from_millis(20)),
                &mut call
            )
            .await
            .is_err());
            assert_eq!(
                calls.load(Ordering::SeqCst),
                0,
                "authority checked before writer reservation"
            );
            match retirement {
                "source" => current.store(false, Ordering::SeqCst),
                "generation" => connection.review_monitoring.lock().unwrap().retire(),
                "authentication" => connection.auth_required.store(true, Ordering::SeqCst),
                "caller-drop" => {
                    drop(call);
                    assert!(connection.pending.lock().unwrap().is_empty());
                    assert_eq!(queue.recv().await.unwrap(), "held");
                    assert!(queue.try_recv().is_err());
                    continue;
                }
                _ => unreachable!(),
            }
            assert_eq!(queue.recv().await.unwrap(), "held");
            assert!(call.await.is_err(), "{retirement}");
            assert!(
                queue.try_recv().is_err(),
                "retired load or fallback was queued"
            );
            assert!(connection.pending.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn history_load_requires_exact_connection_and_full_envelope_without_fallback() {
        for variant in ["live", "foreign", "altered"] {
            let (connection, mut queue, _peer, _ordinary_writer) = controlled_queue();
            let (foreign, _foreign_queue, _foreign_peer, _foreign_writer) = controlled_queue();
            let generation = connection.review_monitoring.lock().unwrap().begin();
            let params = serde_json::json!({"sessionId":"original","cwd":"/fixture","mcpServers":[],"_meta":{"reviewMonitoringContext":{"version":1,"ownerKey":"owner","loadAttemptId":"attempt"}}});
            let mut expected = params.clone();
            if variant == "altered" {
                expected["cwd"] = serde_json::json!("/different");
            }
            let admission = Source {
                connection: if variant == "foreign" {
                    foreign
                } else {
                    connection.clone()
                },
                params: expected,
                current: Arc::new(AtomicBool::new(true)),
                calls: Arc::new(AtomicUsize::new(0)),
            };
            let call = connection.request_review_history_load(
                params.clone(),
                &generation,
                std::time::Instant::now()
                    + intentd_test_support::test_timeout(Duration::from_secs(5)),
                Box::new(admission),
            );
            let mut call = Box::pin(call);
            if variant == "live" {
                let line = tokio::select! {
                    line = queue.recv() => line.unwrap(),
                    _ = &mut call => panic!("request completed before transfer"),
                };
                let request: Value = serde_json::from_str(&line).unwrap();
                assert_eq!(request["method"], "session/load");
                assert_eq!(request["params"], params);
                let id = request["id"].as_i64().unwrap();
                connection
                    .pending
                    .lock()
                    .unwrap()
                    .remove(&id)
                    .unwrap()
                    .sender
                    .send(Ok(serde_json::json!({})))
                    .unwrap();
                assert_eq!(call.await.unwrap(), serde_json::json!({}));
            } else {
                assert!(call.await.is_err());
            }
            assert!(queue.try_recv().is_err());
            assert!(connection.pending.lock().unwrap().is_empty());
        }
    }
}
