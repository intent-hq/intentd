//! Reauthorize receipt-owned data without hydrating a current/recreated note.
use crate::Services;
use intent_core::{note_receipt_detail::ReceiptDetailQuery, Caller, Error, Result, WorkspaceId};
use serde_json::Value;

impl Services {
    pub(crate) async fn read_note_receipt_context(
        &self,
        request: intent_core::note_receipt_detail::NoteGetReceiptContextRequest,
        rpc_id: Value,
    ) -> Result<Value> {
        request.validate().map_err(Error::NoteMutation)?;
        let workspace = WorkspaceId(request.workspace_id.clone());
        self.require_member(&workspace).await?;
        self.store.get_workspace(&workspace).await?;
        let principal = match intent_core::current_caller() {
            Some(Caller::Wire { principal_id, .. }) => format!("principal:{}", principal_id.0),
            Some(Caller::Agent { agent_id }) => format!("agent:{}", agent_id.0),
            Some(Caller::Daemon) => "daemon".into(),
            None => return Err(Error::Forbidden("Caller required".into())),
        };
        let result = self
            .store
            .read_note_receipt_context_retained(&principal, &request, &rpc_id)
            .await;
        #[cfg(test)]
        tests::pause_after_read(&result).await;
        self.require_member(&workspace).await?;
        self.store.get_workspace(&workspace).await?;
        checked_page(result)
    }

    pub(crate) async fn read_note_receipt(
        &self,
        query: ReceiptDetailQuery,
        rpc_id: Value,
    ) -> Result<Value> {
        query.validate().map_err(Error::NoteMutation)?;
        let workspace = WorkspaceId(query.scope.workspace_id.clone());
        self.require_member(&workspace).await?;
        self.store.get_workspace(&workspace).await?;
        let principal = match intent_core::current_caller() {
            Some(Caller::Wire { principal_id, .. }) => format!("principal:{}", principal_id.0),
            Some(Caller::Agent { agent_id }) => format!("agent:{}", agent_id.0),
            Some(Caller::Daemon) => "daemon".into(),
            None => return Err(Error::Forbidden("Caller required".into())),
        };
        let result = self
            .store
            .read_note_receipt_detail_retained(&principal, &query, &rpc_id)
            .await;
        #[cfg(test)]
        tests::pause_after_read(&result).await;
        self.require_member(&workspace).await?;
        self.store.get_workspace(&workspace).await?;
        checked_page(result)
    }
}

// No await may follow this check before returning the authorized page.
fn checked_page(result: Result<(Value, i64)>) -> Result<Value> {
    let (page, expires) = result?;
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    #[cfg(test)]
    let now = tests::observed_now(now);
    if expires <= now {
        return Err(Error::NotePage(
            intent_core::note_page::NotePageError::Expired,
        ));
    }
    Ok(page)
}

#[cfg(test)]
mod tests;
