//! Bounded identity for receipt lookup; no source or current revision is needed.
use super::{bounded_token, NoteMutationError};
use crate::note_page::NoteScope;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NoteOperationStatusQuery {
    pub backend_id: String,
    pub workspace_id: String,
    pub note_id: String,
    pub note_instance_id: String,
    pub operation_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_digest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub header_digest: Option<String>,
}

impl NoteOperationStatusQuery {
    #[must_use]
    pub fn scope(&self) -> NoteScope {
        NoteScope {
            backend_id: self.backend_id.clone(),
            workspace_id: self.workspace_id.clone(),
            note_id: self.note_id.clone(),
            note_instance_id: self.note_instance_id.clone(),
        }
    }

    /// Validate the inline or staged lookup identity without reading live text.
    ///
    /// # Errors
    /// Rejects missing, oversized or noncanonical identities.
    pub fn validate(&self) -> Result<(), NoteMutationError> {
        for value in [
            &self.backend_id,
            &self.workspace_id,
            &self.note_id,
            &self.note_instance_id,
        ] {
            if !bounded_token(value) {
                return Err(NoteMutationError::Invalid);
            }
        }
        let operation =
            uuid::Uuid::parse_str(&self.operation_id).map_err(|_| NoteMutationError::Invalid)?;
        if operation.hyphenated().to_string() != self.operation_id
            || (self.header_digest.is_none() && self.payload_digest.is_none())
        {
            return Err(NoteMutationError::Invalid);
        }
        for digest in [self.header_digest.as_ref(), self.payload_digest.as_ref()]
            .into_iter()
            .flatten()
        {
            if digest.len() != 64
                || !digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(NoteMutationError::Invalid);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn note_operation_status_identity_has_no_revision_or_deadline_dependency() {
        let value = json!({"backendId":"backend","workspaceId":"workspace","noteId":"note",
            "noteInstanceId":"incarnation","operationId":"11111111-1111-4111-8111-111111111111",
            "payloadDigest":"a".repeat(64)});
        let query: NoteOperationStatusQuery = serde_json::from_value(value.clone()).unwrap();
        query.validate().unwrap();
        for (field, invalid) in [
            ("backendId", json!("")),
            ("noteId", json!("x".repeat(257))),
            ("workspaceId", json!("has\0nul")),
            ("payloadDigest", json!("A".repeat(64))),
            ("operationId", json!("11111111111141118111111111111111")),
        ] {
            let mut bad = value.clone();
            bad[field] = invalid;
            assert_eq!(
                serde_json::from_value::<NoteOperationStatusQuery>(bad)
                    .unwrap()
                    .validate(),
                Err(NoteMutationError::Invalid)
            );
        }
        let mut staged = query;
        staged.header_digest = staged.payload_digest.take();
        staged.validate().unwrap();
        staged.header_digest = None;
        assert_eq!(staged.validate(), Err(NoteMutationError::Invalid));
        let mut extra = value;
        extra["baseRevision"] = json!("not a status field");
        assert!(serde_json::from_value::<NoteOperationStatusQuery>(extra).is_err());
    }
}
