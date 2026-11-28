//! Test seams for feature-crate tests: one-line `*_for_test` forwarders to
//! crate-private methods those tests drive directly.
//!
//! Compiled only for this crate's tests and under `test-hooks`, which only a
//! dev-dependency may enable (scripts/check-test-hooks-feature.sh keeps it out
//! of the production build). Each forwarder adds no logic or state; the two
//! that return a private guard type return it opaquely, since their callers
//! only hold and drop it.

use std::future::Future;
use std::sync::Arc;

use intent_core::{
    AgentId, BoxFuture, Event, MessageOrigin, Principal, Result, Workspace, WorkspaceId,
};
use intent_sourcecontrol::UserIdentity;
use serde_json::Value;

use crate::agent_manager::AgentManager;
use crate::agent_ops::QueuedMessage;
use crate::delivery_tasks::DeliveryTasks;
use crate::settings::AsyncSecretStore;
use crate::workspace_mutations::WorkspaceMutations;
use crate::workspace_status::ScriptsRunningPark;
use crate::{CompletionClassifyPark, Services};

#[doc(hidden)]
impl Services {
    /// Test seam: `handle_completion_event`.
    pub async fn handle_completion_event_for_test(&self, event: &Event) {
        self.handle_completion_event(event).await;
    }

    /// Test seam: `agent_retire_op`.
    pub async fn agent_retire_op_for_test(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
        reason: Option<String>,
    ) -> Result<Value> {
        self.agent_retire_op(agent_id, workspace_id, reason).await
    }

    /// Test seam: `agent_restore_op`.
    pub async fn agent_restore_op_for_test(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> Result<Value> {
        self.agent_restore_op(agent_id, workspace_id).await
    }

    /// Test seam: `auto_retire_workspace_op`.
    pub async fn auto_retire_workspace_op_for_test(
        &self,
        id: WorkspaceId,
        archived_before: time::OffsetDateTime,
    ) -> Result<Option<Workspace>> {
        self.auto_retire_workspace_op(id, archived_before).await
    }

    /// Test seam: `dequeue_message_draining`; the draining guard is opaque.
    #[must_use]
    pub fn dequeue_message_draining_for_test(
        &self,
        agent_id: &AgentId,
    ) -> Option<(QueuedMessage, impl Sized + Send)> {
        self.dequeue_message_draining(agent_id)
    }

    /// Test seam: `enqueue_message` with `prepend: None` (the prepend type is
    /// crate-private and every feature-crate caller passes `None`).
    #[expect(
        clippy::too_many_arguments,
        reason = "mirrors enqueue_message's argument list"
    )]
    #[expect(
        clippy::must_use_candidate,
        reason = "callers enqueue for the side effect, as with the forwarded fn"
    )]
    pub fn enqueue_message_for_test(
        &self,
        agent_id: &AgentId,
        content: String,
        image_blocks: Option<Value>,
        file_blocks: Option<Value>,
        message_metadata: Option<Value>,
        interrupt: bool,
        origin: MessageOrigin,
    ) -> (QueuedMessage, usize) {
        self.enqueue_message(
            agent_id,
            content,
            image_blocks,
            file_blocks,
            message_metadata,
            None,
            interrupt,
            origin,
        )
    }

    /// Test seam: `enqueue_message_with_id` with `prepend: None` (the prepend
    /// type is crate-private and every feature-crate caller passes `None`).
    #[expect(
        clippy::too_many_arguments,
        reason = "mirrors enqueue_message_with_id's argument list"
    )]
    #[expect(
        clippy::must_use_candidate,
        reason = "callers enqueue for the side effect, as with the forwarded fn"
    )]
    pub fn enqueue_message_with_id_for_test(
        &self,
        agent_id: &AgentId,
        message_id: Option<String>,
        content: String,
        image_blocks: Option<Value>,
        file_blocks: Option<Value>,
        message_metadata: Option<Value>,
        interrupt: bool,
        origin: MessageOrigin,
    ) -> (QueuedMessage, usize) {
        self.enqueue_message_with_id(
            agent_id,
            message_id,
            content,
            image_blocks,
            file_blocks,
            message_metadata,
            None,
            interrupt,
            origin,
        )
    }

    /// Test seam: `has_ready_to_send`.
    #[must_use]
    pub fn has_ready_to_send_for_test(&self, agent_id: &AgentId) -> bool {
        self.has_ready_to_send(agent_id)
    }

    /// Test seam: `with_invite_mint_park`.
    #[must_use]
    pub fn with_invite_mint_park_for_test(
        self,
        park: Arc<intentd_test_support::PausePoint>,
    ) -> Self {
        self.with_invite_mint_park(park)
    }

    /// Test seam: `with_identity_write_park`.
    #[must_use]
    pub fn with_identity_write_park_for_test(
        self,
        park: Arc<intentd_test_support::PausePoint>,
    ) -> Self {
        self.with_identity_write_park(park)
    }

    /// Test seam: `apply_primary_identity` (the locking variant).
    pub async fn apply_primary_identity_for_test(
        &self,
        principal: Principal,
        user: &UserIdentity,
    ) -> Result<Principal> {
        self.apply_primary_identity(principal, user).await
    }

    /// Test seam: `persist_queue_snapshot`.
    pub async fn persist_queue_snapshot_for_test(&self, agent_id: &AgentId) {
        self.persist_queue_snapshot(agent_id).await;
    }

    /// Test seam: `agent_get_queue_op` without a workspace scope.
    pub async fn agent_get_queue_op_for_test(&self, agent_id: &AgentId) -> Result<Value> {
        self.agent_get_queue_op(agent_id.clone(), None).await
    }

    /// Test seam: the drain's claimed pop of the queue head
    /// (`claim_queue_pop` over `dequeue_message_draining`). Returns the
    /// popped entry's id and the claimed ids another process took first;
    /// the draining guard is dropped before returning.
    pub async fn claim_next_queued_message_for_test(
        &self,
        agent_id: &AgentId,
    ) -> Option<(String, Vec<String>)> {
        self.claim_queue_pop(agent_id, || self.dequeue_message_draining(agent_id))
            .await
            .map(|((next, _draining), lost)| (next.id, lost))
    }

    /// Test seam: [`Self::claim_next_queued_message_for_test`] that keeps
    /// the draining guard (opaque): the claim ends when it drops, and never
    /// when it is leaked (a crash before the delivery).
    pub async fn claim_next_queued_message_held_for_test(
        &self,
        agent_id: &AgentId,
    ) -> Option<(String, Vec<String>, impl Sized)> {
        self.claim_queue_pop(agent_id, || self.dequeue_message_draining(agent_id))
            .await
            .map(|((next, draining), lost)| (next.id, lost, draining))
    }

    /// Test seam: `agent_remove_queued_message_op`.
    pub async fn agent_remove_queued_message_op_for_test(
        &self,
        agent_id: AgentId,
        message_id: String,
    ) -> Result<Value> {
        self.agent_remove_queued_message_op(agent_id, message_id)
            .await
    }

    /// Test seam: `agent_remove_queued_message_owned_op`.
    pub async fn agent_remove_queued_message_owned_op_for_test(
        &self,
        agent_id: AgentId,
        message_id: String,
        caller_agent_id: AgentId,
    ) -> Result<Value> {
        self.agent_remove_queued_message_owned_op(agent_id, message_id, caller_agent_id)
            .await
    }

    /// Test seam: `with_queue_claim_retry_park`.
    #[must_use]
    pub fn with_queue_claim_retry_park_for_test(
        self,
        base: std::time::Duration,
        park: Arc<CompletionClassifyPark>,
    ) -> Self {
        self.with_queue_claim_retry_park(base, park)
    }

    /// Test seam: resolves once a queue claim retry ran its drain.
    pub async fn queue_claim_retried_for_test(&self) {
        self.queue_claim_retried().await;
    }

    /// Test seam: the flush drain's claimed batch pop (`claim_queue_pop`
    /// over `dequeue_ready_batch_draining`, at least `min_ready` entries),
    /// running `before_pop` after the claim's snapshot and right before the
    /// pop: the window in which another worker can enqueue. Returns the
    /// popped ids and the claimed ids another process took first.
    pub async fn claim_queue_batch_for_test(
        &self,
        agent_id: &AgentId,
        min_ready: usize,
        before_pop: impl FnOnce(),
    ) -> Option<(Vec<String>, Vec<String>)> {
        self.claim_queue_pop(agent_id, || {
            before_pop();
            self.dequeue_ready_batch_draining(agent_id, false, min_ready)
        })
        .await
        .map(|((batch, _draining), lost)| (batch.into_iter().map(|m| m.id).collect(), lost))
    }

    /// Test seam: `queue_snapshot`.
    #[must_use]
    pub fn queue_snapshot_for_test(&self, agent_id: &AgentId) -> Vec<Value> {
        // queue-egress: allow — test seam: hands the snapshot to a feature-crate test; no entry leaves the daemon
        self.queue_snapshot(agent_id)
    }

    /// Test seam: `workspace_has_active_hooks_or_script_monitors`.
    pub async fn workspace_has_active_hooks_or_script_monitors_for_test(
        &self,
        workspace_id: &WorkspaceId,
    ) -> bool {
        self.workspace_has_active_hooks_or_script_monitors(workspace_id)
            .await
    }

    /// Test seam: `workspace_is_waiting`.
    pub async fn workspace_is_waiting_for_test(&self, workspace_id: &WorkspaceId) -> bool {
        self.workspace_is_waiting(workspace_id).await
    }

    /// Test seam: `workspace_scripts_running`.
    #[must_use]
    pub fn workspace_scripts_running_for_test(&self, workspace_id: &WorkspaceId) -> bool {
        self.workspace_scripts_running(workspace_id)
    }

    /// Test seam: `arm_scripts_running_park`.
    #[must_use]
    pub fn arm_scripts_running_park_for_test(&self) -> Arc<ScriptsRunningPark> {
        self.arm_scripts_running_park()
    }

    /// Test seam: `with_completion_claim_park`.
    #[must_use]
    pub fn with_completion_claim_park_for_test(self, park: Arc<CompletionClassifyPark>) -> Self {
        self.with_completion_claim_park(park)
    }

    /// Test seam: `with_completion_send_park`.
    #[must_use]
    pub fn with_completion_send_park_for_test(self, park: Arc<CompletionClassifyPark>) -> Self {
        self.with_completion_send_park(park)
    }

    /// Test seam: `with_guarded_enqueue_park`.
    #[must_use]
    pub fn with_guarded_enqueue_park_for_test(self, park: Arc<CompletionClassifyPark>) -> Self {
        self.with_guarded_enqueue_park(park)
    }

    /// Test seam: park a poisoned-queue migration right before it takes the
    /// queue persist gate.
    #[must_use]
    pub fn with_migration_park_for_test(self, park: Arc<CompletionClassifyPark>) -> Self {
        let _ = self.delivery_guards.migration_park.set(park);
        self
    }

    /// Test seam: `migrate_queue_and_gc_poisoned_session`.
    pub async fn migrate_queue_and_gc_poisoned_session_for_test(
        &self,
        poisoned_id: &AgentId,
        target_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Result<usize> {
        self.migrate_queue_and_gc_poisoned_session(poisoned_id, target_id, workspace_id)
            .await
    }

    /// Test seam: `persist_shutdown_queues`.
    pub async fn persist_shutdown_queues_for_test(&self) {
        self.persist_shutdown_queues().await;
    }

    /// Test seam: `agent_send_queued_message_now_op`.
    pub async fn agent_send_queued_message_now_op_for_test(
        &self,
        agent_id: AgentId,
        message_id: String,
    ) -> Result<Value> {
        self.agent_send_queued_message_now_op(agent_id, message_id)
            .await
    }

    /// Test seam: `with_watch_registration_park`.
    #[must_use]
    pub fn with_watch_registration_park_for_test(self, park: Arc<CompletionClassifyPark>) -> Self {
        self.with_watch_registration_park(park)
    }

    /// Test seam: `register_completion_watch_strict_durable` (an ask-only
    /// watch, parent and child in `workspace_id`).
    pub async fn register_completion_watch_strict_for_test(
        &self,
        workspace_id: &WorkspaceId,
        parent_agent_id: AgentId,
        child_agent_id: AgentId,
    ) -> Result<String> {
        self.register_completion_watch_strict_durable(
            workspace_id,
            workspace_id,
            parent_agent_id,
            "parent".into(),
            child_agent_id,
        )
        .await
    }

    /// Test seam: `register_completion_watch_durable`.
    pub async fn register_completion_watch_durable_for_test(
        &self,
        workspace_id: &WorkspaceId,
        parent_agent_id: AgentId,
        child_agent_id: AgentId,
        group_id: Option<String>,
    ) -> Result<String> {
        self.register_completion_watch_durable(
            workspace_id,
            workspace_id,
            parent_agent_id,
            "parent".into(),
            child_agent_id,
            group_id,
        )
        .await
    }

    /// Test seam: the ids of `find_watches_for_child`.
    #[must_use]
    pub fn watch_ids_for_child_for_test(&self, child_agent_id: &AgentId) -> Vec<String> {
        self.find_watches_for_child(child_agent_id)
            .into_iter()
            .map(|watch| watch.id)
            .collect()
    }

    /// Test seam: `execution_call`.
    pub fn execution_call_for_test<'a, T: Send + 'a>(
        &'a self,
        future: impl Future<Output = Result<T>> + Send + 'a,
    ) -> BoxFuture<'a, Result<T>> {
        self.execution_call(future)
    }
}

#[doc(hidden)]
impl AgentManager {
    /// Test seam: `is_busy`.
    #[must_use]
    pub fn is_busy_for_test(&self, agent_id: &AgentId) -> bool {
        self.is_busy(agent_id)
    }

    /// Test seam: `try_begin_turn`; the turn admission is opaque and the
    /// slot stays held until the test releases it another way.
    pub async fn try_begin_turn_for_test(
        self: &Arc<Self>,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Option<impl Sized + Send> {
        self.try_begin_turn(agent_id, workspace_id)
            .await
            .map(crate::agent_manager::TurnSlotClaim::hand_off)
    }
}

#[doc(hidden)]
impl DeliveryTasks {
    /// Test seam: `drain_finite`.
    pub async fn drain_finite_for_test(&self) {
        self.drain_finite().await;
    }
}

#[doc(hidden)]
impl AsyncSecretStore {
    /// Test seam: `delete`.
    pub async fn delete_for_test(&self, account: &str) -> Result<()> {
        self.delete(account).await
    }
}

#[doc(hidden)]
impl WorkspaceMutations {
    /// Test seam: `delete`; the held deletion is opaque.
    pub async fn delete_for_test(&self, id: &WorkspaceId) -> impl Sized + Send {
        self.delete(id).await
    }
}
