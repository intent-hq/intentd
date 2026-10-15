//! A native Flush hands an admitted command to the existing monitor loop.
//! The request cannot mint daemon caller context or choose another ACP owner.
use super::super::{unavailable, Request, RequestState, Services, NATIVE_REQUEST};
use super::{admission, EventSource, Projection, Recipient};
use intent_core::repository_request::RepositoryMonitorEventSource;
use intent_core::{PrMonitorId, Result, WorkspaceId};
use intent_store::{MonitorTargetProvenance, PersistedMonitorTarget, QualifiedPrMonitor};
use std::sync::Arc;

pub(crate) struct Command {
    original: Arc<Services>,
    row: QualifiedPrMonitor,
    authority: Option<Arc<crate::repository_read_source::MonitorAuthority>>,
    connection: intent_core::RepositoryConnectionScope,
    source: Arc<dyn RepositoryMonitorEventSource>,
    check: bool,
    reply: tokio::sync::oneshot::Sender<Result<bool>>,
}
impl Command {
    /// Called only by the existing daemon-owned monitor loop. The immutable
    /// command keeps original admission; the loop supplies no replacement owner.
    pub(crate) async fn run(self) {
        let result = self
            .original
            .flush_qualified_pr_monitor(
                self.row,
                self.authority,
                self.connection,
                self.source,
                self.check,
            )
            .await
            .map_err(|_| unavailable());
        let _ = self.reply.send(result);
    }
}

pub(crate) async fn flush(
    services: &Services,
    workspace: &WorkspaceId,
    id: &PrMonitorId,
    check: bool,
) -> Result<Option<serde_json::Value>> {
    let Some(captured) = NATIVE_REQUEST.try_with(Clone::clone).ok() else {
        return Ok(None);
    };
    if !captured
        .monitor_receipt
        .as_ref()
        .is_some_and(|r| r.capable())
    {
        return Ok(None);
    }
    let row = services.store.get_qualified_pr_monitor(id).await?;
    if matches!(
        row.target(),
        PersistedMonitorTarget::Resolved {
            provenance: MonitorTargetProvenance::LegacyGithubWriter,
            ..
        }
    ) {
        return Ok(None);
    }
    if &row.monitor().workspace_id != workspace {
        return Err(unavailable());
    }
    let request = Request::current(services).map_err(|_| unavailable())?;
    let recipient = Recipient::acquire(&request, workspace).await?;
    services
        .require_workspace_manager(workspace, "prMonitor.flush")
        .await?;
    let admitted = admission::acquire(&request, &recipient, &row).await?;
    if row.monitor().state != intent_core::PrMonitorState::Active {
        return Ok(Some(serde_json::json!({"ok":true,"flushed":false})));
    }
    let authority = services
        .review_monitor_authorities
        .lock()
        .unwrap()
        .get(id)
        .cloned();
    let connection = admitted.binding.connection.clone();
    let mut admissions = vec![admitted.eligibility];
    if let Some(authority) = &authority {
        if authority.binding.resource != admitted.binding.resource
            || authority.owner() != &row.monitor().agent_id
        {
            return Err(unavailable());
        }
    }
    let original = if let Some(authority) = authority
        .as_ref()
        .filter(|authority| authority.binding.connection == connection)
    {
        // A still-current producer retains exact generation admission.
        let exact = admission::acquire_event(&request, &recipient, &authority.binding).await?;
        admissions.push(exact.eligibility);
        authority.original_services().map_err(|_| unavailable())?
    } else {
        // The saved original account was freshly verified above. The loop must
        // independently capture the actual agent runtime and replace the old
        // authority by identity; account ABA never revives that old authority.
        request.connection.services.clone()
    };
    let source = EventSource::capture(recipient.clone())?;
    let projection = Projection::new(recipient, admissions);
    let sender = services
        .review_monitor_controls
        .lock()
        .unwrap()
        .clone()
        .ok_or_else(unavailable)?;
    let permit = sender.try_reserve().map_err(|_| unavailable())?;
    let (reply, receive) = tokio::sync::oneshot::channel();
    let mut command = Some(Command {
        original,
        row,
        authority,
        connection,
        source,
        check,
        reply,
    });
    let mut permit = Some(permit);
    projection.with_current(&mut || {
        permit
            .take()
            .ok_or_else(unavailable)?
            .send(command.take().ok_or_else(unavailable)?);
        Ok(())
    })?;
    let flushed = receive.await.map_err(|_| unavailable())??;
    projection.with_current(&mut || Ok(()))?;
    *request.state.lock().map_err(|_| unavailable())? = RequestState::Monitor(projection);
    Ok(Some(serde_json::json!({"ok":true,"flushed":flushed})))
}
