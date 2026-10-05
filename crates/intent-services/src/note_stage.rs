//! Staged operation admission. Source writes share the canonical mutation path;
//! individual route support does not activate the complete paging capability.
use crate::Services;
use intent_core::{
    note_mutation::{NoteMutationError, NoteOperationStatusQuery},
    note_page::NoteScope,
    note_stage::{
        NoteStageAppend, NoteStageBegin, NoteStageCancel, NoteStageCommit, NoteStageSeal,
        NoteStageStream,
    },
    Caller, Error, Result, WorkspaceId,
};
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, Eq, Hash, PartialEq)]
struct Key(String, String, String, String, String, String);
#[derive(Default)]
struct Active {
    count: u8,
    streams: u8,
}

/// Only active request metadata is retained, never text/chunks or idle sessions.
#[derive(Default)]
pub(crate) struct Admission(Mutex<HashMap<Key, Active>>);
struct Guard {
    admission: Arc<Admission>,
    key: Key,
    stream: u8,
}
fn bit(stream: Option<NoteStageStream>) -> u8 {
    match stream {
        None => 0,
        Some(NoteStageStream::Text) => 1,
        Some(NoteStageStream::Dirty) => 2,
        Some(NoteStageStream::Selection) => 4,
        Some(NoteStageStream::Mutation) => 8,
        Some(NoteStageStream::Live) => 16,
    }
}
impl Admission {
    fn enter(
        self: &Arc<Self>,
        principal: &str,
        scope: &NoteScope,
        operation: &str,
        stream: Option<NoteStageStream>,
    ) -> Result<Guard> {
        let key = Key(
            principal.into(),
            scope.backend_id.clone(),
            scope.workspace_id.clone(),
            scope.note_id.clone(),
            scope.note_instance_id.clone(),
            operation.into(),
        );
        let stream = bit(stream);
        let mut active = self
            .0
            .lock()
            .map_err(|_| Error::Internal("Stage admission unavailable".into()))?;
        // A host resource admission limit, not a bound on operation/source size.
        if !active.contains_key(&key) && active.len() >= 256 {
            return Err(Error::NoteMutation(NoteMutationError::Budget));
        }
        let entry = active.entry(key.clone()).or_default();
        if entry.count >= 2 || stream & entry.streams != 0 {
            return Err(Error::NoteMutation(NoteMutationError::Budget));
        }
        entry.count += 1;
        entry.streams |= stream;
        Ok(Guard {
            admission: self.clone(),
            key,
            stream,
        })
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        if let Ok(mut active) = self.admission.0.lock() {
            if let Some(entry) = active.get_mut(&self.key) {
                entry.count -= 1;
                entry.streams &= !self.stream;
                if entry.count == 0 {
                    active.remove(&self.key);
                }
            }
        }
    }
}
fn principal() -> Result<String> {
    match intent_core::current_caller() {
        Some(Caller::Wire { principal_id, .. }) => Ok(format!("principal:{}", principal_id.0)),
        Some(Caller::Agent { agent_id }) => Ok(format!("agent:{}", agent_id.0)),
        Some(Caller::Daemon) => Ok("daemon".into()),
        None => Err(Error::Forbidden("Caller required".into())),
    }
}
fn state_at_return(mut value: Value) -> Result<Value> {
    if value["kind"] == "noteCommitReceipt" {
        return receipt_at_return(value);
    }
    if value["kind"] == "noteStageState"
        && matches!(value["phase"].as_str(), Some("staging" | "sealed"))
    {
        let deadline = value["expiresAt"]
            .as_str()
            .and_then(intent_core::parse_iso)
            .ok_or_else(|| Error::NoteMutation(NoteMutationError::Invalid))?;
        let now = time::OffsetDateTime::now_utc();
        #[cfg(test)]
        let now = boundary_tests::return_now(now);
        if deadline <= now {
            value["phase"] = Value::String("expired".into());
        }
    }
    Ok(value)
}

// No await may follow this check before returning the receipt. Its retention
// deadline is distinct from the original staging admission deadline.
fn receipt_at_return(receipt: Value) -> Result<Value> {
    let deadline = receipt["receiptExpiresAt"]
        .as_str()
        .and_then(intent_core::parse_iso)
        .ok_or(Error::NoteMutation(NoteMutationError::Invalid))?;
    let now = time::OffsetDateTime::now_utc();
    #[cfg(test)]
    let now = commit_tests::return_now(now);
    if deadline <= now {
        return Err(Error::NoteMutation(NoteMutationError::Expired));
    }
    Ok(receipt)
}
impl Services {
    pub(crate) async fn commit_note_stage(&self, request: NoteStageCommit) -> Result<Value> {
        request.validate().map_err(Error::NoteMutation)?;
        let workspace = WorkspaceId(request.workspace_id.clone());
        let _workspace = self.workspace_mutations.enter(&workspace)?;
        let principal = principal()?;
        let _admission = self.stage_request_admission.enter(
            &principal,
            &request.scope(),
            &request.operation_id,
            None,
        )?;
        self.stage_authorize(&workspace).await?;
        let agent = match intent_core::current_caller() {
            Some(Caller::Agent { agent_id }) => Some(agent_id),
            _ => None,
        };
        let author = crate::resolve_note_version_author(&self.store, agent.as_ref()).await;
        let reserved = self
            .store
            .reserve_note_stage_commit(&principal, &request)
            .await;
        #[cfg(test)]
        commit_tests::after_reservation(&reserved).await;
        self.stage_authorize(&workspace).await?;
        let reservation = match reserved? {
            intent_store::StageCommitAdmission::Replay(receipt) => {
                return receipt_at_return(receipt)
            }
            intent_store::StageCommitAdmission::Reserved(reservation) => reservation,
        };
        let writer = reservation
            .into_mutation(crate::note_ops::reject_numbered_read_presentation)
            .await?;
        self.stage_authorize(&workspace).await?;
        let result = self
            .finish_note_mutation(writer, workspace.clone(), agent, author)
            .await;
        #[cfg(test)]
        commit_tests::after_commit(&result).await;
        self.stage_authorize(&workspace).await?;
        receipt_at_return(result?)
    }
    async fn stage_authorize(&self, workspace: &WorkspaceId) -> Result<()> {
        #[cfg(test)]
        boundary_tests::before_authorize().await;
        self.require_member(workspace).await?;
        self.store.get_workspace(workspace).await?;
        Ok(())
    }
    pub(crate) async fn begin_note_stage(&self, request: NoteStageBegin) -> Result<Value> {
        request.validate().map_err(Error::NoteMutation)?;
        let workspace = WorkspaceId(request.workspace_id.clone());
        let _workspace = self.workspace_mutations.enter(&workspace)?;
        let principal = principal()?;
        let _admission = self.stage_request_admission.enter(
            &principal,
            &request.scope(),
            &request.operation_id,
            None,
        )?;
        self.stage_authorize(&workspace).await?;
        let result = self.store.begin_note_stage(&principal, &request).await;
        #[cfg(test)]
        boundary_tests::after_store(&result).await;
        self.stage_authorize(&workspace).await?;
        state_at_return(result?)
    }
    pub(crate) async fn append_note_stage(&self, request: NoteStageAppend) -> Result<Value> {
        NoteOperationStatusQuery {
            backend_id: request.backend_id.clone(),
            workspace_id: request.workspace_id.clone(),
            note_id: request.note_id.clone(),
            note_instance_id: request.note_instance_id.clone(),
            operation_id: request.operation_id.clone(),
            header_digest: Some(request.header_digest.clone()),
            payload_digest: None,
        }
        .validate()
        .map_err(Error::NoteMutation)?;
        if request.records.len() > 128 {
            return Err(Error::NoteMutation(NoteMutationError::Budget));
        }
        let workspace = WorkspaceId(request.workspace_id.clone());
        let _workspace = self.workspace_mutations.enter(&workspace)?;
        let principal = principal()?;
        let _admission = self.stage_request_admission.enter(
            &principal,
            &request.scope(),
            &request.operation_id,
            Some(request.stream),
        )?;
        self.stage_authorize(&workspace).await?;
        let result = self.store.append_note_stage(&principal, &request).await;
        #[cfg(test)]
        boundary_tests::after_store(&result).await;
        self.stage_authorize(&workspace).await?;
        result
    }
    pub(crate) async fn seal_note_stage(&self, request: NoteStageSeal) -> Result<Value> {
        request.validate().map_err(Error::NoteMutation)?;
        let workspace = WorkspaceId(request.workspace_id.clone());
        let _workspace = self.workspace_mutations.enter(&workspace)?;
        let principal = principal()?;
        let _admission = self.stage_request_admission.enter(
            &principal,
            &request.scope(),
            &request.operation_id,
            None,
        )?;
        self.stage_authorize(&workspace).await?;
        let result = self.store.seal_note_stage(&principal, &request).await;
        #[cfg(test)]
        boundary_tests::after_store(&result).await;
        self.stage_authorize(&workspace).await?;
        state_at_return(result?)
    }
    pub(crate) async fn read_stage_source(
        &self,
        request: intent_core::note_stage_read::NoteStageRead,
        rpc_id: Value,
    ) -> Result<Value> {
        request.validate().map_err(Error::NoteMutation)?;
        let workspace = WorkspaceId(request.workspace_id.clone());
        let _workspace = self.workspace_mutations.enter(&workspace)?;
        let principal = principal()?;
        let _admission = self.stage_request_admission.enter(
            &principal,
            &request.scope(),
            &request.operation_id,
            None,
        )?;
        self.stage_authorize(&workspace).await?;
        let result = self
            .store
            .read_note_stage_source(&principal, &request, &rpc_id)
            .await;
        #[cfg(test)]
        boundary_tests::after_store(&result).await;
        self.stage_authorize(&workspace).await?;
        let value = result?;
        let expiry = value["expiresAt"]
            .as_str()
            .and_then(intent_core::parse_iso)
            .ok_or_else(|| Error::NotePage(intent_core::note_page::NotePageError::CursorInvalid))?;
        let now = time::OffsetDateTime::now_utc();
        #[cfg(test)]
        let now = boundary_tests::return_now(now);
        if expiry <= now {
            return Err(Error::NotePage(
                intent_core::note_page::NotePageError::Expired,
            ));
        }
        Ok(value)
    }
    pub(crate) async fn cancel_note_stage(&self, request: NoteStageCancel) -> Result<Value> {
        request.validate().map_err(Error::NoteMutation)?;
        let workspace = WorkspaceId(request.workspace_id.clone());
        let _workspace = self.workspace_mutations.enter(&workspace)?;
        let principal = principal()?;
        let _admission = self.stage_request_admission.enter(
            &principal,
            &request.scope(),
            &request.operation_id,
            None,
        )?;
        self.stage_authorize(&workspace).await?;
        let result = self.store.cancel_note_stage(&principal, &request).await;
        #[cfg(test)]
        boundary_tests::after_store(&result).await;
        self.stage_authorize(&workspace).await?;
        state_at_return(result?)
    }
    pub(crate) async fn read_note_stage_status(
        &self,
        request: NoteOperationStatusQuery,
    ) -> Result<Value> {
        request.validate().map_err(Error::NoteMutation)?;
        let workspace = WorkspaceId(request.workspace_id.clone());
        let _workspace = self.workspace_mutations.enter(&workspace)?;
        let principal = principal()?;
        let _admission = self.stage_request_admission.enter(
            &principal,
            &request.scope(),
            &request.operation_id,
            None,
        )?;
        self.stage_authorize(&workspace).await?;
        let result = self.store.note_stage_status(&principal, &request).await;
        #[cfg(test)]
        boundary_tests::after_store(&result).await;
        self.stage_authorize(&workspace).await?;
        state_at_return(result?)
    }
}

#[cfg(test)]
mod tests {
    use super::Admission;
    use intent_core::{
        note_mutation::NoteMutationError, note_page::NoteScope, note_stage::NoteStageStream, Error,
    };
    use std::sync::Arc;
    #[test]
    fn staged_admission_enforces_two_total_and_one_per_stream_then_retires_keys() {
        let admission = Arc::new(Admission::default());
        let scope = NoteScope {
            backend_id: "b".into(),
            workspace_id: "w".into(),
            note_id: "n".into(),
            note_instance_id: "i".into(),
        };
        let first = admission
            .enter("p", &scope, "op", Some(NoteStageStream::Text))
            .unwrap();
        assert!(matches!(
            admission.enter("p", &scope, "op", Some(NoteStageStream::Text)),
            Err(Error::NoteMutation(NoteMutationError::Budget))
        ));
        let second = admission
            .enter("p", &scope, "op", Some(NoteStageStream::Dirty))
            .unwrap();
        assert!(matches!(
            admission.enter("p", &scope, "op", None),
            Err(Error::NoteMutation(NoteMutationError::Budget))
        ));
        let foreign = admission
            .enter("other", &scope, "op", Some(NoteStageStream::Text))
            .unwrap();
        drop(first);
        let third = admission
            .enter("p", &scope, "op", Some(NoteStageStream::Text))
            .unwrap();
        drop((second, third, foreign));
        assert!(admission.0.lock().unwrap().is_empty());
    }
}

#[cfg(test)]
mod boundary_tests;

#[cfg(test)]
mod commit_tests;
