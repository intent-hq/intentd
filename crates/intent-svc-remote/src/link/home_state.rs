//! A machine's home timers, link side (monorepo
//! `docs/protocol/methods/nodes.md`, "Detached workspaces" and "Continuous
//! backup"): the link-loss grace after which the machine's homes are
//! detached ([`intent_node_connections::NodeInbound::homes_link_lost`]), and
//! one backup-warning wait per home. The domain side (`crate::home_state`)
//! detaches, attaches and announces.

use std::collections::HashMap;
use std::sync::{Mutex, Weak};
use std::time::Duration;

use intent_core::{BoxFuture, HomeId};
use tokio::task::JoinHandle;

use crate::connector::Shared;

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One machine's detach grace and the backup warnings of its homes.
#[derive(Default)]
pub(crate) struct HomeLink {
    grace: Mutex<Option<JoinHandle<()>>>,
    warnings: Mutex<HashMap<HomeId, JoinHandle<()>>>,
}

impl Drop for HomeLink {
    fn drop(&mut self) {
        if let Some(grace) = lock(&self.grace).take() {
            grace.abort();
        }
        for (_, warning) in lock(&self.warnings).drain() {
            warning.abort();
        }
    }
}

impl HomeLink {
    /// The machine's link is down (or not up yet at start): its homes are
    /// detached once `grace` passes without a bind.
    pub(crate) fn link_lost(&self, shared: Weak<Shared>, grace: Duration) {
        let holds = shared
            .upgrade()
            .is_some_and(|shared| shared.holds_homes.load(std::sync::atomic::Ordering::SeqCst));
        if !holds {
            return;
        }
        let lost_at = intent_core::now_iso();
        let mut slot = lock(&self.grace);
        if let Some(previous) = slot.take() {
            previous.abort();
        }
        // caller-binding: allow — detaches one machine's homes after the link-loss grace
        *slot = Some(tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            let Some(shared) = shared.upgrade() else {
                return;
            };
            if shared.current_link().is_some() {
                return;
            }
            if let Some(inbound) = shared.inbound() {
                inbound.homes_link_lost(shared.as_ref(), &lost_at).await;
            }
        }));
    }

    /// A link bound: no home of the machine is detached for the loss.
    pub(crate) fn bound(&self) {
        if let Some(grace) = lock(&self.grace).take() {
            grace.abort();
        }
    }

    /// A backup of `home_id` arrived: its streak is over.
    pub(crate) fn backed_up(&self, home_id: &HomeId) {
        if let Some(warning) = lock(&self.warnings).remove(home_id) {
            warning.abort();
        }
    }

    /// Hold `wait`, `home_id`'s backup-warning wait, unless one is pending.
    pub(crate) fn watch_warning(&self, home_id: &HomeId, wait: BoxFuture<'static, ()>) {
        let mut warnings = lock(&self.warnings);
        warnings.retain(|_, waiting| !waiting.is_finished());
        if warnings
            .get(home_id)
            .is_some_and(|waiting| !waiting.is_finished())
        {
            return;
        }
        // caller-binding: allow — announces one home's backup warning when its failing streak reaches 15 minutes
        let wait = tokio::spawn(wait);
        warnings.insert(home_id.clone(), wait);
    }
}
