//! Existing operation gates, evaluated under the original captured entry.
//!
//! This is one part of the concrete source. A successful gate read is not a
//! dispatch lease and cannot replace original bearer validation, durable
//! continuity, repository observations or the final retirement fence.

use intent_core::caller::{with_caller, with_wire_credential};
use intent_core::{NativeReviewStage, WorkspaceId};

use crate::repository_admission::{AdmissionError, AdmissionResult, OriginalRepositoryCaller};
use crate::Services;

pub(crate) async fn check_stage_gates(
    services: &Services,
    original: &OriginalRepositoryCaller,
    workspace: &WorkspaceId,
    stages: &[NativeReviewStage],
) -> AdmissionResult<()> {
    if stages.is_empty() {
        return Err(AdmissionError::InvalidPlan);
    }
    with_caller(
        original.caller().clone(),
        with_wire_credential(original.wire_credential().cloned(), async {
            services.require_member(workspace).await?;
            if stages.contains(&NativeReviewStage::CreatePr) {
                services
                    .require_host_execution("repository review creation")
                    .await?;
            }
            Ok(())
        }),
    )
    .await
    .map_err(|error| match error {
        intent_core::Error::NotFound(_) | intent_core::Error::Forbidden(_) => {
            AdmissionError::Denied
        }
        _ => AdmissionError::Unavailable,
    })
}

#[cfg(test)]
#[path = "source_tests/durable.rs"]
mod tests;
