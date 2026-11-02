//! Delegating optional consumers for original MCP and distinct prompt captures.
//! Normal endpoints and manager/default wiring remain unchanged.
use intent_acp::mcp_server::private_results::{
    McpGuidanceChoice, McpHostCall, McpOptionalContextScope, McpOptionalEvidence,
    McpPrivateAdmission, McpPrivateBoundary, McpPrivateHostScope, McpPrivatePolicy,
    McpReadEvidence, McpSealedReads, PreparedMcpTransfer, PreparedMcpVariants,
};
use intent_acp::mcp_server::repository_guidance::{
    GuidanceCandidate, GuidancePreparation, RepositoryGuidanceFence, RepositoryGuidanceSource,
};
use intent_acp::mcp_server::request_context::{
    McpContextFuture, McpRequestContext, McpRequestScope,
};
use intent_acp::transport::{
    AcpPromptAdmission, AcpPromptAdmissionOutcome, AcpPromptBoundary, PreparedAcpPromptTransfer,
    PromptVariant,
};
use intent_core::caller::current_caller;
use intent_core::{BoxFuture, Caller, WorkspaceId};
use std::sync::{Arc, Mutex};

use crate::harness::repository_guidance_v3::{render, GuidanceContext, GuidanceContextStamp};
use crate::repository_admission::read_request::{
    RepositoryOptionalMetadata, RepositoryOptionalScope, RepositoryReadRequest,
};
use crate::repository_admission::request_context::{
    RepositoryManagerPromptRequest, RepositoryPromptRequest,
};
use crate::repository_admission::AdmissionResult;
use crate::repository_context_live::{PreparedContextFacts, RepositoryContextOwner};
use crate::repository_read_source::{
    with_context_only, with_context_records, ContextOptional, MonitorRecord, OptionalContextOrigin,
    ReadRecord,
};

impl RepositoryContextOwner {
    pub(crate) fn mcp_context(self: &Arc<Self>) -> Arc<dyn McpRequestContext> {
        Arc::new(ContextCapture(self.clone()))
    }
    pub(crate) fn guidance_source(self: &Arc<Self>) -> Arc<dyn RepositoryGuidanceSource> {
        Arc::new(ContextSource(self.clone()))
    }
    /// Call synchronously at the real prompt/continuation entry, before queueing.
    pub(crate) fn capture_prompt(self: &Arc<Self>) -> AdmissionResult<RepositoryPromptContext> {
        Ok(RepositoryPromptContext {
            owner: self.clone(),
            original: PromptOrigin::Strict(self.callback.capture_prompt()?),
        })
    }

    /// Capture at the original confirmed handle, before prompt work is queued.
    pub(crate) fn capture_manager_prompt(
        self: &Arc<Self>,
    ) -> AdmissionResult<RepositoryPromptContext> {
        Ok(RepositoryPromptContext {
            owner: self.clone(),
            original: PromptOrigin::Manager(self.callback.capture_manager_prompt()?),
        })
    }
}
struct ContextCapture(Arc<RepositoryContextOwner>);
impl McpRequestContext for ContextCapture {
    fn capture(&self) -> Arc<dyn McpRequestScope> {
        let (required, read) = self.0.callback.capture_owned();
        let policy = read.ok().and_then(|request| {
            required.private_result_policy().map(|original| {
                Arc::new(ContextPolicy {
                    owner: self.0.clone(),
                    request,
                    original,
                }) as Arc<dyn McpPrivatePolicy>
            })
        });
        Arc::new(ContextScope { required, policy })
    }
}
struct ContextScope {
    required: Arc<dyn McpRequestScope>,
    policy: Option<Arc<dyn McpPrivatePolicy>>,
}
impl McpRequestScope for ContextScope {
    fn private_result_policy(&self) -> Option<Arc<dyn McpPrivatePolicy>> {
        self.policy.clone()
    }
    fn scope<'a>(&'a self, body: McpContextFuture<'a>) -> McpContextFuture<'a> {
        self.required.scope(body)
    }
}
struct ContextPolicy {
    owner: Arc<RepositoryContextOwner>,
    request: Arc<RepositoryReadRequest>,
    original: Arc<dyn McpPrivatePolicy>,
}
impl McpPrivatePolicy for ContextPolicy {
    fn capture_host(&self, call: McpHostCall) -> Box<dyn McpPrivateHostScope> {
        self.original.capture_host(call)
    }
    fn admit<'a>(
        &'a self,
        boundary: &'a McpPrivateBoundary,
        records: &'a [McpReadEvidence],
        packet: PreparedMcpTransfer<'a>,
    ) -> BoxFuture<'a, McpPrivateAdmission> {
        self.original.admit(boundary, records, packet)
    }
    fn capture_optional_context(&self) -> Option<Box<dyn McpOptionalContextScope>> {
        let scope = self.request.capture_optional().ok()?;
        let local = scope.metadata();
        Some(Box::new(OptionalScope {
            scope: Mutex::new(Some(scope)),
            cell: Arc::new(Preparation {
                owner: self.owner.clone(),
                local,
                state: Mutex::new(PreparationState {
                    entered: false,
                    closed: false,
                    facts: None,
                    ready: None,
                }),
            }),
        }))
    }
    fn admit_optional<'a>(
        &'a self,
        boundary: &'a McpPrivateBoundary,
        sealed: McpSealedReads<'a>,
        evidence: &'a McpOptionalEvidence,
        packet: PreparedMcpVariants<'a>,
    ) -> BoxFuture<'a, McpPrivateAdmission> {
        Box::pin(async move {
            let ready = evidence
                .downcast_ref::<Arc<Preparation>>()
                .filter(|cell| Arc::ptr_eq(&cell.owner, &self.owner))
                .and_then(|cell| cell.state.try_lock().ok()?.ready.clone());
            let Some(ready) =
                ready.filter(|ready| Arc::ptr_eq(&ready.facts.request, &self.request))
            else {
                return if sealed.is_empty() {
                    packet.without_guidance().transfer(boundary)
                } else {
                    self.original
                        .admit(boundary, sealed.records(), packet.without_guidance())
                        .await
                };
            };
            // Optional guidance cannot compose monitor receipts with its Git
            // source locks. Keep the complete private result under the original
            // required admission; omit only the optional guidance. Unknown or
            // retired receipts are still refused by that original policy.
            if sealed
                .records()
                .iter()
                .any(|record| record.downcast_ref::<MonitorRecord>().is_some())
            {
                return self
                    .original
                    .admit(boundary, sealed.records(), packet.without_guidance())
                    .await;
            }
            let consume = |include| {
                packet.transfer(
                    boundary,
                    if include {
                        McpGuidanceChoice::WithGuidance
                    } else {
                        McpGuidanceChoice::WithoutGuidance
                    },
                )
            };
            if sealed.is_empty() {
                with_context_only(OptionalContextOrigin::Mcp(sealed), &ready, consume)
                    .await
                    .unwrap_or(McpPrivateAdmission::Refused)
            } else {
                let Some(records) = sealed
                    .records()
                    .iter()
                    .map(McpReadEvidence::downcast_ref::<ReadRecord>)
                    .collect::<Option<Vec<_>>>()
                else {
                    return McpPrivateAdmission::Refused;
                };
                with_context_records(
                    &self.request,
                    &self.owner.services,
                    &records,
                    &ready,
                    consume,
                )
                .await
                .unwrap_or(McpPrivateAdmission::Refused)
            }
        })
    }
}

struct Preparation {
    owner: Arc<RepositoryContextOwner>,
    local: RepositoryOptionalMetadata,
    state: Mutex<PreparationState>,
}
struct PreparationState {
    entered: bool,
    closed: bool,
    facts: Option<Arc<PreparedContextFacts>>,
    ready: Option<Arc<ContextOptional>>,
}
impl Preparation {
    fn close(&self) {
        let dropped = if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            (state.facts.take(), state.ready.take())
        } else {
            (None, None)
        };
        self.local.cancel();
        drop(dropped);
    }
}
struct ClosePreparation {
    cell: Arc<Preparation>,
    completed: bool,
}
impl ClosePreparation {
    fn complete(&mut self) {
        self.completed = true;
    }
}
impl Drop for ClosePreparation {
    fn drop(&mut self) {
        if !self.completed {
            self.cell.close();
        }
    }
}
struct OptionalScope {
    scope: Mutex<Option<RepositoryOptionalScope>>,
    cell: Arc<Preparation>,
}
tokio::task_local! { static PREPARATION: Arc<Preparation>; }
impl McpOptionalContextScope for OptionalScope {
    fn scope<'a>(&'a self, body: McpContextFuture<'a>) -> McpContextFuture<'a> {
        // Take before constructing the future: cancellation before polling still
        // owns the optional scope and closes its one-shot evidence cell.
        let captured = self.scope.lock().ok().and_then(|mut scope| scope.take());
        let mut close = ClosePreparation {
            cell: self.cell.clone(),
            completed: false,
        };
        Box::pin(async move {
            let Some(scope) = captured else {
                return;
            };
            let cell = self.cell.clone();
            let Ok(run) = scope.run_optional(|_| {
                PREPARATION.scope(cell, async {
                    body.await;
                    Ok(())
                })
            }) else {
                return;
            };
            let Ok(prepared) = run.await else {
                return;
            };
            let Ok(mut state) = self.cell.state.lock() else {
                return;
            };
            if state.closed {
                return;
            }
            let Some(facts) = state.facts.take() else {
                return;
            };
            state.ready = Some(Arc::new(ContextOptional {
                prepared: Arc::new(prepared),
                facts,
            }));
            close.complete();
        })
    }
}

struct ContextSource(Arc<RepositoryContextOwner>);
impl RepositoryGuidanceSource for ContextSource {
    fn preparation(&self) -> GuidancePreparation {
        GuidancePreparation::Qualified
    }
    fn prepare<'a>(
        &'a self,
        workspace: &'a WorkspaceId,
        caller: &'a Caller,
        fence: &'a RepositoryGuidanceFence,
    ) -> BoxFuture<'a, Option<GuidanceCandidate>> {
        Box::pin(async move {
            let cell = PREPARATION.try_with(Clone::clone).ok()?;
            if !Arc::ptr_eq(&cell.owner, &self.0) || current_caller().as_ref() != Some(caller) {
                return None;
            }
            {
                let mut state = cell.state.lock().ok()?;
                if state.entered || state.closed {
                    return None;
                }
                state.entered = true;
            }
            let facts = self.0.prepare(cell.local.clone()).await.ok()?;
            if facts
                .context
                .roots
                .iter()
                .any(|root| &root.root.workspace_id != workspace)
            {
                return None;
            }
            let text = render_original(&facts).await?;
            let lease = fence
                .replace_context(facts.context.scope.clone(), facts.context.revision.clone())?;
            let candidate = lease
                .current_candidate(
                    facts.context.scope.clone(),
                    facts.context.revision.clone(),
                    text,
                )?
                .with_optional_evidence(McpOptionalEvidence::new(cell.clone()));
            let mut state = cell.state.lock().ok()?;
            if state.closed || state.facts.is_some() {
                return None;
            }
            state.facts = Some(facts);
            Some(candidate)
        })
    }
}
async fn render_original(facts: &PreparedContextFacts) -> Option<String> {
    let Some(Caller::Agent { agent_id }) = current_caller() else {
        return None;
    };
    let session = facts
        .owner
        .services
        .store
        .get_agent_session(&agent_id)
        .await
        .ok()?;
    // A plain-folder workspace (§5.1) has no git or forge, so its sessions
    // get no repository guidance or facts envelope — the same predicate that
    // swaps the system prompt's git guidance for the plain-folder note.
    let workspace = facts
        .owner
        .services
        .store
        .get_workspace(&session.workspace_id)
        .await
        .ok();
    let doctrine = crate::harness::resolve_entry(&session.harness_version).doctrine;
    if crate::instructions::plain_folder_gated(doctrine.instructions, workspace.as_ref()) {
        return None;
    }
    let expected = GuidanceContextStamp {
        scope: facts.context.scope.clone(),
        revision: facts.context.revision.clone(),
    };
    render(
        Some(&session),
        GuidanceContext::Current {
            context: &facts.context,
            expected: &expected,
        },
    )
    .map(|rendered| rendered.text)
}

/// Owns a distinct real non-MCP parent through preparation and prompt transfer.
pub(crate) struct RepositoryPromptContext {
    owner: Arc<RepositoryContextOwner>,
    original: PromptOrigin,
}

enum PromptOrigin {
    Strict(RepositoryPromptRequest),
    Manager(RepositoryManagerPromptRequest),
}
impl PromptOrigin {
    fn original(&self) -> &RepositoryPromptRequest {
        match self {
            Self::Strict(original) => original,
            Self::Manager(original) => original.original(),
        }
    }
    fn construction_check(&self) -> AdmissionResult<()> {
        match self {
            Self::Strict(_) => Ok(()),
            Self::Manager(original) => original.check_entry(),
        }
    }
    fn run<'a, T: Send + 'a>(
        &'a self,
        body: BoxFuture<'a, AdmissionResult<T>>,
    ) -> BoxFuture<'a, AdmissionResult<T>> {
        match self {
            Self::Strict(original) => match original.run(body) {
                Ok(future) => future,
                Err(error) => Box::pin(async move { Err(error) }),
            },
            Self::Manager(original) => original.run(body),
        }
    }
}

impl RepositoryPromptContext {
    pub(crate) async fn recover_monitor(
        self,
        row: &intent_store::QualifiedPrMonitor,
        connection: &intent_core::RepositoryConnectionScope,
    ) -> AdmissionResult<Arc<crate::repository_read_source::MonitorAuthority>> {
        let services = self.owner.services.clone();
        let destination = self.into_monitor_destination()?;
        destination
            .run(Box::pin(async {
                let retained = destination.original().read().retain_monitor()?;
                crate::repository_read_source::MonitorAuthority::recover(
                    &services, retained, row, connection,
                )
                .await
            }))
            .await
    }

    pub(crate) fn into_monitor_destination(
        self,
    ) -> AdmissionResult<RepositoryManagerPromptRequest> {
        match self.original {
            PromptOrigin::Manager(original) => {
                original.check_entry()?;
                Ok(original)
            }
            PromptOrigin::Strict(_) => Err(crate::repository_admission::AdmissionError::Denied),
        }
    }

    pub(crate) fn prepare(self) -> BoxFuture<'static, Option<intent_acp::session::PromptGuidance>> {
        let entry = self.original.construction_check();
        Box::pin(async move {
            entry.ok()?;
            let prepared = self
                .original
                .run(Box::pin(async {
                    let scope = self.original.original().read().capture_optional()?;
                    scope.run_optional(|local| self.owner.prepare(local))?.await
                }))
                .await
                .ok()?;
            let facts = prepared.value().clone();
            let text = self
                .original
                .run(Box::pin(async { Ok(render_original(&facts).await) }))
                .await
                .ok()??;
            let prepared = Arc::new(prepared.map(|_| ()));
            intent_acp::session::PromptGuidance::new(
                text,
                Box::new(PromptAdmission {
                    original: self.original,
                    optional: ContextOptional { prepared, facts },
                }),
            )
        })
    }
}
struct PromptAdmission {
    original: PromptOrigin,
    optional: ContextOptional,
}
impl AcpPromptAdmission for PromptAdmission {
    fn admit<'a>(
        &'a self,
        original: &'a AcpPromptBoundary,
        packet: PreparedAcpPromptTransfer<'a>,
    ) -> BoxFuture<'a, AcpPromptAdmissionOutcome> {
        let entry = self.original.construction_check();
        Box::pin(async move {
            if entry.is_err() {
                return AcpPromptAdmissionOutcome::OmitOptional;
            }
            let run = self.original.run(Box::pin(with_context_only(
                OptionalContextOrigin::Prompt(self.original.original()),
                &self.optional,
                |include| {
                    packet.transfer(
                        original,
                        if include {
                            PromptVariant::WithGuidance
                        } else {
                            PromptVariant::Base
                        },
                    )
                },
            )));
            run.await.unwrap_or(AcpPromptAdmissionOutcome::OmitOptional)
        })
    }
}

#[cfg(test)]
pub(crate) mod tests;
