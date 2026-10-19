//! Local provenance correlation for pre-confirmation history bootstrap. The
//! exact verified envelopes are retained; none of these facts grants access.
use super::{AdmissionError, AdmissionResult, HashSet, RepositoryReviewBootstrap};
use intent_acp::review_monitoring::history::ReviewHistoryMessage;
use intent_store::MonitorMessageProvenance;

#[path = "history_admission.rs"]
mod admission;

pub(crate) struct VerifiedReviewHistory {
    original: RepositoryReviewBootstrap,
    envelopes: Vec<MonitorMessageProvenance>,
}
impl VerifiedReviewHistory {
    pub(crate) fn envelopes(&self) -> &[MonitorMessageProvenance] {
        &self.envelopes
    }

    /// A local lifetime fence only. Actual output still needs fresh all-source
    /// admission, original connection/attempt binding and joined remote lifetime.
    pub(crate) fn with_current<T>(
        &self,
        action: impl FnOnce() -> AdmissionResult<T>,
    ) -> AdmissionResult<T> {
        self.original.with_current(action)
    }
}

impl RepositoryReviewBootstrap {
    /// Correlate every declared original message from the prepared native
    /// closure. Use only the captured Store and loaded session, never a supplied
    /// database/current monitor/session lookup. Missing evidence refuses the
    /// entire set; no partial projection or unknown-source omission is allowed.
    pub(crate) async fn verify_history(
        &self,
        messages: &[ReviewHistoryMessage],
    ) -> AdmissionResult<VerifiedReviewHistory> {
        self.with_current(|| Ok(()))?;
        let session = self.session.as_deref().ok_or(AdmissionError::Denied)?;
        if messages.len() > 4096 {
            return Err(AdmissionError::Denied);
        }
        let mut ids = HashSet::new();
        let mut envelopes = Vec::with_capacity(messages.len());
        for correlation in messages {
            if !ids.insert(&correlation.message_id) {
                return Err(AdmissionError::Denied);
            }
            self.with_current(|| Ok(()))?;
            let proof = self
                .store
                .get_monitor_message_provenance(&self.agent, &correlation.message_id)
                .await
                .map_err(|_| AdmissionError::Unavailable)?
                .ok_or(AdmissionError::Denied)?;
            self.with_current(|| Ok(()))?;
            verify_envelope(self, session, correlation, &proof)?;
            envelopes.push(proof);
        }
        self.with_current(|| Ok(()))?;
        Ok(VerifiedReviewHistory {
            original: self.clone(),
            envelopes,
        })
    }
}

fn verify_envelope(
    original: &RepositoryReviewBootstrap,
    session: &str,
    correlation: &ReviewHistoryMessage,
    proof: &MonitorMessageProvenance,
) -> AdmissionResult<()> {
    let message = &proof.message;
    if proof.workspace_id != original.workspace
        || message.agent_id != original.agent
        || message.id != correlation.message_id
        || message.role != "user"
    {
        return Err(AdmissionError::Denied);
    }
    let stored_digest =
        proof
            .envelope_sha256
            .iter()
            .fold(String::with_capacity(64), |mut output, byte| {
                std::fmt::Write::write_fmt(&mut output, format_args!("{byte:02x}"))
                    .expect("writing to a String cannot fail");
                output
            });
    if stored_digest != correlation.store_envelope_digest {
        return Err(AdmissionError::Denied);
    }
    // v1 producer writes exactly one text block plus its matching public wake
    // metadata. Strip only that independently verified Store annotation when
    // rebuilding the accepted text-only ACP payload, never unknown rich fields.
    let blocks = message.content.as_array().ok_or(AdmissionError::Denied)?;
    if blocks.len() != 1 {
        return Err(AdmissionError::Denied);
    }
    let block = blocks[0].as_object().ok_or(AdmissionError::Denied)?;
    if block.len() != 3
        || block.get("type").and_then(serde_json::Value::as_str) != Some("text")
        || block.get("messageMetadata") != message.metadata.as_ref()
    {
        return Err(AdmissionError::Denied);
    }
    let text = block
        .get("text")
        .and_then(serde_json::Value::as_str)
        .ok_or(AdmissionError::Denied)?;
    let metadata: intent_core::review_monitor::ReviewMonitorWakeMetadata =
        serde_json::from_value(message.metadata.clone().ok_or(AdmissionError::Denied)?)
            .map_err(|_| AdmissionError::Denied)?;
    if metadata.extension.resource.as_ref() != Some(&proof.binding.resource)
        || metadata.pr_number != proof.binding.resource.number
        || metadata.repo != proof.binding.resource.repository.project_path
    {
        return Err(AdmissionError::Denied);
    }
    let params = intent_acp::review_monitoring::payload::classify_prompt(
        serde_json::json!({"sessionId":session,"prompt":[{"type":"text","text":text}]}),
        &message.id,
        &stored_digest,
    )
    .map_err(|_| AdmissionError::Denied)?;
    if params["_meta"]["reviewMonitoringMessage"]["acpPayloadDigest"].as_str()
        != Some(correlation.acp_payload_digest.as_str())
    {
        return Err(AdmissionError::Denied);
    }
    Ok(())
}
