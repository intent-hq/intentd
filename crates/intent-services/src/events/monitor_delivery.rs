//! Private monitor envelopes use the existing bus and bounded subscription
//! queue. Ordinary subscribers never receive their payload or metadata.
use std::sync::Arc;

use intent_core::repository_request::{
    RepositoryMonitorEventRecipient, RepositoryMonitorEventSource, RepositoryPrivateOutput,
};
use intent_core::{review_monitor::ReviewMonitorBinding, Event, Result};

pub(crate) enum BroadcastEvent {
    Public(Event),
    Monitor(MonitorEvent),
    // Routing identity only; no message, binding, or private content.
    HistoryChanged(String),
}

// Public-event writer tests inspect fields on ordinary broadcasts. A private
// envelope is never a public Event, including in a debug or test projection.
impl std::ops::Deref for BroadcastEvent {
    type Target = Event;
    fn deref(&self) -> &Event {
        match self {
            Self::Public(event) => event,
            Self::Monitor(_) | Self::HistoryChanged(_) => {
                panic!("private monitor event has no public projection")
            }
        }
    }
}

pub(crate) struct MonitorEvent {
    pub(super) event: Event,
    pub(super) binding: Option<ReviewMonitorBinding>,
    pub(super) source: Arc<dyn RepositoryMonitorEventSource>,
}

pub(super) struct Recipient {
    pub owner: Arc<dyn RepositoryMonitorEventRecipient>,
    pub subscription_id: String,
}
impl Recipient {
    pub async fn project(&self, event: &MonitorEvent) -> Result<PrivateEventDelivery> {
        let output = self
            .owner
            .project(
                &event.event,
                event.binding.as_ref(),
                event.source.clone(),
                &self.subscription_id,
            )
            .await?;
        output.with_current(&mut || Ok(()))?;
        Ok(PrivateEventDelivery(output))
    }
}

/// Only an already authorized immutable native frame may leave the private bus
/// route. Generic `recv()` consumers discard it; native writers retain its guard.
pub struct PrivateEventDelivery(Arc<dyn RepositoryPrivateOutput>);
impl PrivateEventDelivery {
    #[must_use]
    pub fn into_output(self) -> Arc<dyn RepositoryPrivateOutput> {
        self.0
    }
}
impl std::fmt::Debug for PrivateEventDelivery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PrivateEventDelivery([withheld])")
    }
}
