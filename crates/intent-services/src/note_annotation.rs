//! Authorized annotation reads. No legacy Note/comment hydration is performed.
use crate::Services;
use intent_core::{
    note_annotation::{AnnotationMethod, AnnotationReadRequest},
    Caller, Error, Result, WorkspaceId,
};
use intent_store::note_annotation_repo::{AnnotationContextRequest, AnnotationPageRequest};
use serde_json::Value;

fn caller_identity() -> Result<String> {
    match intent_core::current_caller() {
        Some(Caller::Wire { principal_id, .. }) => Ok(format!("principal:{}", principal_id.0)),
        Some(Caller::Agent { agent_id }) => Ok(format!("agent:{}", agent_id.0)),
        Some(Caller::Daemon) => Ok("daemon".into()),
        None => Err(Error::Forbidden("Caller required".into())),
    }
}

impl Services {
    /// Internal service seam for the shared WorkspaceApi/router registration.
    /// Membership is read again after potentially expensive scalar preparation,
    /// so a revoked caller cannot receive a page admitted before revocation.
    pub(crate) async fn read_annotation_page(
        &self,
        method: AnnotationMethod,
        request: AnnotationReadRequest,
        rpc_id: Value,
    ) -> Result<Value> {
        request.validate(method)?;
        let workspace = WorkspaceId(request.workspace_id.clone());
        self.require_member(&workspace).await?;
        let principal = caller_identity()?;
        let scope = request.scope();
        let result = match method {
            AnnotationMethod::Context => {
                let page: AnnotationContextRequest = serde_json::from_value(request.page.clone())
                    .map_err(|_| {
                    Error::InvalidParams("Invalid annotation context request".into())
                })?;
                self.store
                    .read_note_annotation_context(
                        &principal,
                        &scope,
                        &request.source_revision,
                        request.epoch().ok_or_else(|| {
                            Error::InvalidParams("Annotation epoch required".into())
                        })?,
                        &page,
                        &rpc_id,
                    )
                    .await
            }
            AnnotationMethod::Attribution
            | AnnotationMethod::Comments
            | AnnotationMethod::Replies => {
                let page: AnnotationPageRequest = serde_json::from_value(request.page.clone())
                    .map_err(|_| Error::InvalidParams("Invalid annotation page request".into()))?;
                self.store
                    .read_note_annotation_page(
                        &principal,
                        &scope,
                        &request.source_revision,
                        request.epoch(),
                        request.thread_id.as_deref(),
                        &page,
                        &rpc_id,
                    )
                    .await
            }
        };
        #[cfg(test)]
        tests::pause_after_read(&workspace, tests::read_outcome(&result)).await;
        // Prefer current authorization failure even when storage found a stale
        // or expired token; do not expose persisted state to a revoked caller.
        self.require_member(&workspace).await?;
        let result = result?;
        if method == AnnotationMethod::Context
            && ((request.comment_revision.is_some() != result.get("commentRevision").is_some())
                || (request.attribution_generation.is_some()
                    != result.get("attributionGeneration").is_some()))
        {
            return Err(Error::InvalidParams(
                "Annotation context epoch kind mismatch".into(),
            ));
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests;
