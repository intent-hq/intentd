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

use intent_core::{AgentId, BoxFuture, MessageOrigin, Result, Workspace, WorkspaceId};
use serde_json::Value;

use crate::agent_manager::AgentManager;
use crate::agent_ops::QueuedMessage;
use crate::delivery_tasks::DeliveryTasks;
use crate::workspace_mutations::WorkspaceMutations;
use crate::workspace_status::ScriptsRunningPark;
use crate::{CompletionClassifyPark, Services};

#[doc(hidden)]
impl Services {
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

    /// Test seam: `persist_queue_snapshot`.
    pub async fn persist_queue_snapshot_for_test(&self, agent_id: &AgentId) {
        self.persist_queue_snapshot(agent_id).await;
    }

    /// Test seam: `queue_snapshot`.
    #[must_use]
    pub fn queue_snapshot_for_test(&self, agent_id: &AgentId) -> Vec<Value> {
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

    /// Test seam: `try_begin_turn`; the turn admission is opaque.
    pub async fn try_begin_turn_for_test(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Option<impl Sized + Send> {
        self.try_begin_turn(agent_id, workspace_id).await
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
impl WorkspaceMutations {
    /// Test seam: `delete`; the held deletion is opaque.
    pub async fn delete_for_test(&self, id: &WorkspaceId) -> impl Sized + Send {
        self.delete(id).await
    }
}
