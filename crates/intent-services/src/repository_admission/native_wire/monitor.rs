//! A negotiated consumer receipt belongs to one physical socket and hello.
//! It supplies compatibility only; private output still needs fresh admission.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use intent_core::repository_request::ReviewMonitorHello;
use intent_core::review_monitor::ReviewMonitorRecipientSupport;

use super::{Connection, RepositoryRetirement};

#[derive(Default)]
pub(super) struct ConnectionState {
    generation: Mutex<Option<Arc<Receipt>>>,
    closed: AtomicBool,
}

pub(super) struct Receipt {
    support: ReviewMonitorRecipientSupport,
    acknowledged: AtomicBool,
    lifetime: RepositoryRetirement,
}

impl ConnectionState {
    pub(super) fn capture(&self) -> Option<Arc<Receipt>> {
        let receipt = self.generation.lock().ok()?.clone()?;
        receipt.support().map(|_| receipt)
    }
    pub(super) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let old = self.generation.lock().unwrap().take();
        if let Some(old) = old {
            old.lifetime.end_scope();
        }
    }

    fn replace(&self, support: ReviewMonitorRecipientSupport) -> Option<Arc<Receipt>> {
        let receipt = Arc::new(Receipt {
            support,
            acknowledged: AtomicBool::new(false),
            lifetime: RepositoryRetirement::default(),
        });
        let old = {
            let mut current = self.generation.lock().ok()?;
            if self.closed.load(Ordering::Acquire) {
                return None;
            }
            current.replace(receipt.clone())
        };
        // Never join admitted users while holding the cohort map.
        if let Some(old) = old {
            old.lifetime.end_scope();
        }
        Some(receipt)
    }
}

impl Receipt {
    pub(super) fn support(&self) -> Option<ReviewMonitorRecipientSupport> {
        self.lifetime
            .native_dispatch(|| {
                if self.acknowledged.load(Ordering::Acquire) {
                    Ok(self.support)
                } else {
                    Err(super::AdmissionError::Denied)
                }
            })
            .ok()
    }
}

struct Hello {
    connection: Weak<Connection>,
    receipt: Arc<Receipt>,
}

pub(super) fn hello(
    connection: &Connection,
    support: ReviewMonitorRecipientSupport,
) -> Option<Arc<dyn ReviewMonitorHello>> {
    // Even a failed/rejected new hello retires the old cohort. A caller with no
    // original native entry cannot create a usable replacement.
    let receipt = connection.monitor.replace(support)?;
    connection.entered().ok()?;
    Some(Arc::new(Hello {
        connection: connection.weak.clone(),
        receipt,
    }))
}

impl ReviewMonitorHello for Hello {
    fn retire(&self) {
        self.receipt.lifetime.end_scope();
    }

    fn acknowledge(&self) {
        let Some(connection) = self.connection.upgrade() else {
            return;
        };
        if connection.entered().is_err() {
            return;
        }
        let _ = connection.parent.native_dispatch(|| {
            self.receipt.lifetime.native_dispatch(|| {
                self.receipt.acknowledged.store(true, Ordering::Release);
                Ok(())
            })
        });
    }
}
