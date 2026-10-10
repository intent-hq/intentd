//! Existing lifecycle events on the original private monitor bus route.
use std::sync::Arc;

use intent_core::events::{
    PR_MONITOR_CANCELLED, PR_MONITOR_CHANGED, PR_MONITOR_COMPLETED, PR_MONITOR_EMITTED,
    PR_MONITOR_REGISTERED,
};
use intent_core::{Event, PrMonitor};
use serde_json::{Value, json};

use crate::Services;
use crate::repository_read_source::MonitorAuthority;

impl Services {
    /// Called only after the original admitted transition commits. Event output
    /// remains ephemeral and guarded; it is never evidence of wake delivery.
    pub(crate) fn emit_qualified_monitor_event(
        &self,
        authority: &Arc<MonitorAuthority>,
        event_type: &str,
        monitor: &PrMonitor,
        extra: Option<Value>,
    ) {
        let Some(bus) = &self.event_bus else { return };
        if !matches!(
            event_type,
            PR_MONITOR_REGISTERED
                | PR_MONITOR_CHANGED
                | PR_MONITOR_EMITTED
                | PR_MONITOR_COMPLETED
                | PR_MONITOR_CANCELLED
        ) {
            return;
        }
        let Ok(source) = authority.native_event_source(monitor) else {
            return;
        };
        let Ok(wire) = super::routing::wire(monitor, &authority.binding.resource) else {
            return;
        };
        let mut data = json!({
            "workspaceId":monitor.workspace_id,"agentId":monitor.agent_id,"monitorId":monitor.monitor_id,
            "repo":authority.binding.resource.repository.project_path,"prNumber":authority.binding.resource.number,
            "state":monitor.state,"resource":authority.binding.resource,
            "monitorAvailability":wire["monitorAvailability"],
        });
        if let (Some(object), Some(Value::Object(extra))) = (data.as_object_mut(), extra) {
            object.extend(extra);
        }
        let event = Event {
            id: uuid::Uuid::now_v7().to_string(),
            workspace_id: monitor.workspace_id.clone(),
            timestamp: intent_core::now_iso(),
            event_type: event_type.into(),
            actor: crate::system_actor(),
            session_id: Some(monitor.agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        };
        let _ = bus.publish_monitor(event, Some(authority.binding.clone()), source);
    }
}
