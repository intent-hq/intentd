use super::{db, invalid, Error, NoteMutationError, Result, Row, Store, Value};
use intent_core::note_receipt_detail::NoteGetReceiptContextRequest;

impl Store {
    /// Resolve a receipt context reference within its original principal and scope.
    /// Services must authorize current visibility before and after this read.
    /// # Errors
    /// Rejects unknown references, changed scope, retained revision mismatch or expiry.
    pub async fn read_note_receipt_context(
        &self,
        principal: &str,
        request: &NoteGetReceiptContextRequest,
        rpc_id: &Value,
    ) -> Result<Value> {
        self.read_note_receipt_context_retained(principal, request, rpc_id)
            .await
            .map(|(page, _)| page)
    }

    /// Carry the original retention deadline across the service authorization boundary.
    /// # Errors
    /// Rejects the same scoped invalid, expired or inaccessible references as the page reader.
    pub async fn read_note_receipt_context_retained(
        &self,
        principal: &str,
        request: &NoteGetReceiptContextRequest,
        rpc_id: &Value,
    ) -> Result<(Value, i64)> {
        request
            .query(uuid::Uuid::nil().to_string())
            .map_err(Error::NoteMutation)?;
        if principal.is_empty() || principal.len() > 256 || principal.contains('\0') {
            return Err(Error::NoteMutation(NoteMutationError::Invalid));
        }
        let (owner, _) = request
            .page
            .context_ref
            .split_once(':')
            .ok_or_else(invalid)?;
        if uuid::Uuid::parse_str(owner)
            .map_err(|_| invalid())?
            .to_string()
            != owner
        {
            return Err(invalid());
        }
        let row = sqlx::query("SELECT operation_id,outcome FROM note_operation WHERE operation_key=? AND principal=? AND backend_id=? AND workspace_id=? AND note_id=? AND instance_id=?")
            .bind(owner).bind(principal).bind(&request.backend_id).bind(&request.workspace_id)
            .bind(&request.note_id).bind(&request.note_instance_id)
            .fetch_optional(self.read_pool()).await.map_err(db)?.ok_or_else(invalid)?;
        let receipt: Value = serde_json::from_str(row.get::<&str, _>("outcome")).map_err(db)?;
        if receipt["afterRevision"] != request.source_revision {
            return Err(invalid());
        }
        let query = request
            .query(row.get("operation_id"))
            .map_err(Error::NoteMutation)?;
        self.read_note_receipt_detail_retained(principal, &query, rpc_id)
            .await
    }
}
