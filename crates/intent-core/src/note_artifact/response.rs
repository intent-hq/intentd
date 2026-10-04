//! Prepared artifact receipt shapes. These types do not authorize or activate a route.
use super::request::Reservation;
use crate::{Error, Result};
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum JobPhase {
    Building,
    Sealed,
    Admitted,
    Aborted,
    Expired,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobState {
    pub job_id: String,
    pub job_ref: String,
    pub header_digest: String,
    pub state: JobPhase,
    pub next_sequence: u64,
    pub accepted_bytes: u64,
    pub current_digest: String,
    pub expires_at: String,
    pub status_until: String,
    pub reservation: Reservation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_artifact_ref: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LeaseReceipt {
    pub job_id: String,
    pub header_digest: String,
    pub final_digest: String,
    pub admission_id: String,
    pub artifact_ref: String,
    pub generation: String,
    pub expires_at: String,
    pub status_until: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind")]
pub enum Receipt {
    #[serde(rename = "artifactReleased")]
    Released,
    #[serde(rename = "artifactJobState")]
    Job(JobState),
    #[serde(rename = "artifactLease")]
    Lease(LeaseReceipt),
    #[serde(rename = "artifactUnknown", rename_all = "camelCase")]
    Unknown {
        job_id: String,
        header_digest: String,
    },
}

impl Receipt {
    /// Admit the exact result plus its real JSON-RPC envelope, including ID.
    /// A historical lease receipt is not a readability or allocation grant.
    ///
    /// # Errors
    /// Rejects serialization errors or a full escaped envelope over 4096 bytes.
    pub fn rpc_result(&self, id: &Value) -> Result<Value> {
        let result = serde_json::to_value(self).map_err(|e| Error::Internal(e.to_string()))?;
        let envelope = serde_json::json!({"jsonrpc":"2.0","id":id,"result":result});
        if serde_json::to_vec(&envelope)
            .map_err(|e| Error::Internal(e.to_string()))?
            .len()
            > 4096
        {
            return Err(Error::NotePage(crate::note_page::NotePageError::Budget));
        }
        Ok(result)
    }
}

/// Format a trusted retention deadline without losing millisecond precision.
/// Accepted source/job deadline text must instead be retained verbatim.
///
/// # Errors
/// Rejects out-of-range timestamps.
pub fn retention_timestamp(ms: i64) -> Result<String> {
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(ms) * 1_000_000)
        .ok()
        .and_then(|value| {
            value
                .format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .ok_or_else(|| Error::Internal("Invalid artifact retention deadline".into()))
}
