//! Serialize human instruction admission with committed host revocation.
use std::future::Future;

use intent_core::{lift_from_principal_id, AgentId, BoxFuture, PrincipalId, Result};

use crate::Services;

#[cfg(test)]
#[derive(Default)]
pub(crate) struct MutationBarrier(
    std::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
);

#[cfg(test)]
impl MutationBarrier {
    pub(crate) fn arm(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered, reached) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        *self.0.lock().unwrap() = Some((entered, wait));
        (reached, release)
    }
    pub(crate) async fn pause(&self) {
        let barrier = self.0.lock().unwrap().take();
        if let Some((entered, wait)) = barrier {
            let _ = entered.send(());
            let _ = wait.await;
        }
    }
}

/// A held read admission. Clones share one guard, so a detached task can
/// keep its caller's admission alive after the caller is dropped.
#[derive(Clone)]
pub(crate) struct InstructionAuthority {
    identity: usize,
    _guard: std::sync::Arc<tokio::sync::OwnedRwLockReadGuard<()>>,
}

tokio::task_local! {
    static INSTRUCTION_AUTHORITY: InstructionAuthority;
}

/// The admission held by the current task, for [`with_instruction_authority`].
pub(crate) fn current_instruction_authority() -> Option<InstructionAuthority> {
    INSTRUCTION_AUTHORITY.try_with(Clone::clone).ok()
}

/// Run `future` under `authority`, so nested admissions reuse it instead of
/// queueing a second read behind a pending removal writer.
pub(crate) async fn with_instruction_authority<F: Future>(
    authority: Option<InstructionAuthority>,
    future: F,
) -> F::Output {
    match authority {
        Some(authority) => INSTRUCTION_AUTHORITY.scope(authority, future).await,
        None => future.await,
    }
}

impl Services {
    /// Nested send/drain paths share one read admission. A worker's actual
    /// execution runs outside this scope, so removal never waits for a turn.
    pub(crate) fn instruction_admission<'a, F, T>(&'a self, future: F) -> BoxFuture<'a, T>
    where
        F: Future<Output = T> + Send + 'a,
        T: Send + 'a,
    {
        let identity = std::sync::Arc::as_ptr(&self.human_instruction_authority) as usize;
        if INSTRUCTION_AUTHORITY
            .try_with(|held| held.identity == identity)
            .unwrap_or(false)
        {
            return Box::pin(future);
        }
        Box::pin(async move {
            let guard = self.human_instruction_authority.clone().read_owned().await;
            let authority = InstructionAuthority {
                identity,
                _guard: std::sync::Arc::new(guard),
            };
            INSTRUCTION_AUTHORITY.scope(authority, future).await
        })
    }

    /// Called only after the durable sweep, with queue persistence excluded.
    pub(crate) fn drop_principal_queues(
        &self,
        principal: &PrincipalId,
        removed: bool,
    ) -> Vec<AgentId> {
        if !removed {
            return Vec::new();
        }
        let mut changed = Vec::new();
        let mut queues = self.agent_queues.lock().expect("agent queues poisoned");
        for (agent, queue) in queues.iter_mut() {
            let before = queue.len();
            queue.retain(|m| {
                lift_from_principal_id(m.message_metadata.as_ref()).as_ref() != Some(principal)
            });
            if before != queue.len() {
                changed.push(agent.clone());
            }
        }
        changed
    }

    /// Restart/retry backstop: a removed person's stale queued instructions
    /// cannot be admitted even if an old snapshot was persisted late.
    pub(crate) async fn discard_revoked_instructions(&self, agent: &AgentId) -> Result<()> {
        let principals: std::collections::HashSet<_> = self
            .agent_queues
            .lock()
            .expect("agent queues poisoned")
            .get(agent)
            .into_iter()
            .flatten()
            .filter_map(|m| lift_from_principal_id(m.message_metadata.as_ref()))
            .collect();
        let mut changed = false;
        for principal in principals {
            if self
                .store
                .principal_instructions_revoked(&principal)
                .await?
            {
                let mut queues = self.agent_queues.lock().expect("agent queues poisoned");
                if let Some(queue) = queues.get_mut(agent) {
                    let before = queue.len();
                    queue.retain(|m| {
                        lift_from_principal_id(m.message_metadata.as_ref()).as_ref()
                            != Some(&principal)
                    });
                    changed |= before != queue.len();
                }
            }
        }
        if changed {
            #[cfg(test)]
            self.hold_periodic_commit("revoked-queue").await;
            self.publish_queue_updated(agent).await;
        }
        Ok(())
    }
}
