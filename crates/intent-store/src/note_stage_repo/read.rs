use super::{db, fail, require_workspace, source};
use crate::Store;
use intent_core::{
    note_mutation::{NoteMutationError, NoteOperationStatusQuery},
    Result,
};
use serde_json::Value;
use sqlx::Row;

impl Store {
    /// Read one indexed immutable base piece for an original operation owner.
    /// This internal storage seam does not implement the public staged view:
    /// services must authorize current workspace access before and after reading.
    /// # Errors
    /// Rejects unknown owners, expired retention, invalid offsets and wrong kinds.
    pub async fn read_note_stage_base_piece(
        &self,
        principal: &str,
        request: &NoteOperationStatusQuery,
        offset: u64,
    ) -> Result<(u64, u64, String)> {
        request.validate().map_err(fail)?;
        let digest = request
            .header_digest
            .as_deref()
            .ok_or_else(|| fail(NoteMutationError::Invalid))?;
        if principal.is_empty() || principal.len() > 256 || principal.contains('\0') {
            return Err(fail(NoteMutationError::Invalid));
        }
        let offset = i64::try_from(offset).map_err(|_| fail(NoteMutationError::Invalid))?;
        let mut tx = self.read_pool().begin().await.map_err(db)?;
        require_workspace(&mut tx, &request.workspace_id).await?;
        let row=sqlx::query("SELECT s.root_key,s.header_digest,s.phase,o.outcome,o.retain_until FROM note_operation o JOIN note_stage s USING(operation_key) WHERE o.principal=? AND o.backend_id=? AND o.workspace_id=? AND o.note_id=? AND o.instance_id=? AND o.operation_id=? AND o.method_kind='staged' AND EXISTS(SELECT 1 FROM workspace w WHERE w.id=o.workspace_id) AND NOT EXISTS(SELECT 1 FROM note_annotation_workspace_retirement r WHERE r.workspace_id=o.workspace_id)")
            .bind(principal).bind(&request.backend_id).bind(&request.workspace_id).bind(&request.note_id).bind(&request.note_instance_id).bind(&request.operation_id)
            .fetch_optional(&mut *tx).await.map_err(db)?.ok_or_else(||fail(NoteMutationError::Invalid))?;
        if row.get::<String, _>("header_digest") != digest {
            return Err(fail(NoteMutationError::Mismatch));
        }
        let phase: String = row.get("phase");
        let outcome: Value = serde_json::from_str(row.get("outcome")).map_err(db)?;
        let retain_until: i64 = row.get("retain_until");
        check_deadline(&phase, &outcome, retain_until, &intent_core::now_iso())?;
        let root: String = row.get("root_key");
        let (start, end, text) = source::source_piece(&mut tx, &root, offset).await?;
        // A readonly transaction is dropped before returning. No subsequent await
        // can move a successful piece beyond this original-deadline check.
        check_deadline(&phase, &outcome, retain_until, &intent_core::now_iso())?;
        Ok((
            u64::try_from(start).map_err(db)?,
            u64::try_from(end).map_err(db)?,
            text,
        ))
    }
}

// Preserve the uploaded millisecond deadline. The integer column is only a
// conservative retention/index predicate, not the precise live-read deadline.
fn check_deadline(phase: &str, outcome: &Value, retain_until: i64, now: &str) -> Result<()> {
    let text = match phase {
        "staging" | "sealed" => outcome["expiresAt"]
            .as_str()
            .ok_or_else(|| fail(NoteMutationError::Invalid))?
            .to_owned(),
        "committed" => intent_core::iso_from_unix_secs(retain_until),
        _ => return Err(fail(NoteMutationError::Expired)),
    };
    let deadline = intent_core::parse_iso(&text).ok_or_else(|| fail(NoteMutationError::Invalid))?;
    let now = intent_core::parse_iso(now).ok_or_else(|| fail(NoteMutationError::Invalid))?;
    if deadline <= now {
        return Err(fail(NoteMutationError::Expired));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::check_deadline;
    use intent_core::{note_mutation::NoteMutationError, Error};
    use serde_json::json;

    #[test]
    fn stage_base_read_preserves_exact_millisecond_deadline() {
        let state = json!({"expiresAt":"2030-01-01T00:00:00.123Z"});
        for phase in ["staging", "sealed"] {
            assert!(check_deadline(phase, &state, 0, "2030-01-01T00:00:00.122Z").is_ok());
            for now in ["2030-01-01T00:00:00.123Z", "2030-01-01T00:00:00.124Z"] {
                assert!(matches!(
                    check_deadline(phase, &state, 0, now),
                    Err(Error::NoteMutation(NoteMutationError::Expired))
                ));
            }
        }
        let until = intent_core::parse_iso("2030-01-02T00:00:00.000Z")
            .unwrap()
            .unix_timestamp();
        assert!(check_deadline("committed", &state, until, "2030-01-01T23:59:59.999Z").is_ok());
        assert!(matches!(
            check_deadline("committed", &state, until, "2030-01-02T00:00:00.000Z"),
            Err(Error::NoteMutation(NoteMutationError::Expired))
        ));
        assert!(matches!(
            check_deadline("cancelled", &state, until, "2029-01-01T00:00:00.000Z"),
            Err(Error::NoteMutation(NoteMutationError::Expired))
        ));
    }
}
