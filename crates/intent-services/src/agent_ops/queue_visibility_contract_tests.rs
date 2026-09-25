//! The services half of the per-user queue visibility contract
//! (multiplayer, intentd#2068): every [`Harness::Services`] cell of
//! [`QUEUE_VISIBILITY_CONTRACT`] — `agent.getQueue`, `agent.diagnostics`,
//! `agent.editQueuedMessage`, `agent.removeQueuedMessage`,
//! `agent.sendQueuedMessageNow` — driven through the real [`Services`] under
//! [`with_caller`], one seeded entry per cell, failing by [`Cell::name`].
//! The event surfaces belong to the transport harness.

use std::collections::HashSet;
use std::future::Future;

use intent_core::queue_visibility_contract::{
    AttributionTier, CallerClass, Cell, Expected, Harness, QueueSurface, QUEUE_VISIBILITY_CONTRACT,
};
use intent_core::{
    queue_attribution_with, with_caller, AgentId, Caller, Error, MessageOrigin, PrincipalId,
    QueueAttribution, WorkspaceId, FROM_PRINCIPAL_ID_KEY,
};
use serde_json::{json, Value};

use super::tests::{create_agent, owner_and_guest_callers, setup, TempDb};
use super::QueuedMessage;
use crate::Services;

/// One shared workspace with an owner (administrator), a guest collaborator
/// and one agent whose queue every cell seeds into. The workspace resolves
/// NO fallback author (the same fixture as
/// `unstamped_human_entries_fail_closed_when_the_fallback_lookup_fails`), so
/// an unstamped human-origin entry is an [`QueueAttribution::UnknownHuman`].
struct Fixture {
    _tmp: TempDb,
    svc: Services,
    ws: WorkspaceId,
    agent: AgentId,
    as_admin: Caller,
    as_guest: Caller,
}

impl Fixture {
    async fn new() -> Self {
        let (tmp, svc, ws) = setup().await;
        let agent = create_agent(&svc, &ws, "Shared").await;
        let (as_admin, as_guest) = owner_and_guest_callers(&svc, &ws).await;
        sqlx::query(
            "UPDATE workspace SET legacy_author_principal_id = NULL, owner_principal_id = NULL WHERE id = ?",
        )
        .bind(&ws.0)
        .execute(svc.store().write_pool())
        .await
        .expect("clear the workspace author fallback");
        assert_eq!(
            crate::principal_ops::MessageAuthorResolver::new(&svc, &ws)
                .fallback_principal_id()
                .await,
            None,
            "the fallback read resolves nothing"
        );
        Self {
            _tmp: tmp,
            svc,
            ws,
            agent,
            as_admin,
            as_guest,
        }
    }

    fn owner(&self) -> PrincipalId {
        self.as_admin
            .principal_id()
            .expect("owner principal")
            .clone()
    }

    fn guest(&self) -> PrincipalId {
        self.as_guest
            .principal_id()
            .expect("guest principal")
            .clone()
    }

    /// The real caller for a table class: the table's wire callers are the
    /// fixture's owner and guest, the rest bind exactly as the table says.
    fn caller_for(&self, class: CallerClass) -> Option<Caller> {
        match class {
            CallerClass::Administrator => Some(self.as_admin.clone()),
            CallerClass::AuthorGuest | CallerClass::ForeignGuest => Some(self.as_guest.clone()),
            CallerClass::Agent | CallerClass::Daemon | CallerClass::Unbound => class.caller(),
        }
    }

    /// Seed one entry resolving to the cell's tier. A stamped entry carries
    /// the guest's stamp — the caller's own for [`CallerClass::AuthorGuest`],
    /// foreign to the administrator (the table's `AuthorOnly` edit cell) and
    /// to the agent / daemon / unbound callers — except for
    /// [`CallerClass::ForeignGuest`], where it carries the owner's.
    fn seed(&self, cell: Cell) -> QueuedMessage {
        let stamp_author = if cell.caller == CallerClass::ForeignGuest {
            self.owner()
        } else {
            self.guest()
        };
        let (metadata, origin, expected) = match cell.tier {
            AttributionTier::PrincipalStamped => (
                Some(json!({ FROM_PRINCIPAL_ID_KEY: stamp_author.0 })),
                MessageOrigin::User,
                QueueAttribution::Principal(stamp_author),
            ),
            AttributionTier::UnknownHuman => {
                (None, MessageOrigin::User, QueueAttribution::UnknownHuman)
            }
            AttributionTier::Unattributed => (
                Some(json!({ "fromAgentId": "agent-peer", "fromAgentName": "Peer" })),
                MessageOrigin::Automatic,
                QueueAttribution::Unattributed,
            ),
        };
        let (entry, _) = self.svc.enqueue_message(
            &self.agent,
            format!("entry for {}", cell.name()),
            None,
            None,
            metadata,
            None,
            false,
            origin,
        );
        // The seed resolves to the tier the cell names (real principal ids
        // stand in for the table's constants), and the fixture's fallback is
        // gone, so `None` is what every gate and projection resolves.
        assert_eq!(
            queue_attribution_with(entry.message_metadata.as_ref(), None),
            expected,
            "{}: seeded entry attribution",
            cell.name()
        );
        assert_eq!(
            std::mem::discriminant(&expected),
            std::mem::discriminant(&cell.attribution()),
            "{}: seeded tier",
            cell.name()
        );
        entry
    }

    /// Run `fut` as the cell's caller (no scope for [`CallerClass::Unbound`]).
    async fn run_as<R>(&self, cell: Cell, fut: impl Future<Output = R>) -> R {
        match self.caller_for(cell.caller) {
            Some(caller) => with_caller(caller, fut).await,
            None => fut.await,
        }
    }

    /// The live queue in wire shape, for byte-identical "no side effects"
    /// comparisons around a refused mutation.
    fn snapshot(&self) -> Vec<Value> {
        self.svc.queue_snapshot(&self.agent)
    }

    /// Drive one cell against its seeded `entry` and classify the outcome in
    /// [`Expected`] terms; a shape the table has no word for is `Err(text)`.
    /// Side-effect checks (a performed mutation landed; a refused one left the
    /// snapshot byte-identical) are asserted here, named by cell.
    async fn drive(
        &self,
        cell: Cell,
        entry: &QueuedMessage,
    ) -> std::result::Result<Expected, String> {
        let id = entry.id.clone();
        let contains = |queue: &[Value]| queue.iter().any(|e| e["id"] == id);
        let not_found = format!("queued message not found: {id}");
        let author_only = format!("queued message {id} can only be edited by its author");
        let classify_error = |err: Error| match err {
            Error::InvalidParams(m) if m == not_found => Ok(Expected::NotFound),
            Error::InvalidParams(m) if m == author_only => Ok(Expected::AuthorOnly),
            other => Err(format!("unexpected error {other:?}")),
        };
        match cell.surface {
            QueueSurface::GetQueue => {
                let result = self
                    .run_as(
                        cell,
                        self.svc
                            .agent_get_queue_op(self.agent.clone(), Some(self.ws.clone())),
                    )
                    .await
                    .map_err(|e| format!("getQueue failed: {e:?}"))?;
                let queue = result["queue"]
                    .as_array()
                    .ok_or("getQueue: no queue array")?;
                Ok(if contains(queue) {
                    Expected::Visible
                } else {
                    Expected::Hidden
                })
            }
            QueueSurface::Diagnostics => {
                let result = self
                    .run_as(
                        cell,
                        self.svc.agent_diagnostics_op(
                            self.ws.clone(),
                            Some(self.agent.clone()),
                            None,
                            None,
                        ),
                    )
                    .await
                    .map_err(|e| format!("diagnostics failed: {e:?}"))?;
                let queues = result["diagnostics"]["queues"]
                    .as_array()
                    .ok_or("diagnostics: no queues array")?;
                let visible = queues
                    .iter()
                    .filter(|q| q["agentId"] == self.agent.0)
                    .filter_map(|q| q["entries"].as_array())
                    .any(|entries| contains(entries));
                Ok(if visible {
                    Expected::Visible
                } else {
                    Expected::Hidden
                })
            }
            QueueSurface::EditQueuedMessage => {
                let before = self.snapshot();
                let content = format!("edited for {}", cell.name());
                let outcome = self
                    .run_as(
                        cell,
                        self.svc.agent_edit_queued_message_op(
                            self.agent.clone(),
                            id.clone(),
                            content.clone(),
                            None,
                        ),
                    )
                    .await;
                match outcome {
                    Ok(_) => {
                        let live = self
                            .svc
                            .find_queued_message(&self.agent, &id)
                            .ok_or("edit: the entry left the queue")?;
                        // A collaborator's edit may prepend its sender preamble.
                        if !live.content.ends_with(&content) {
                            return Err(format!("edit: content not applied: {:?}", live.content));
                        }
                        Ok(Expected::Allowed)
                    }
                    Err(err) => {
                        let expected = classify_error(err)?;
                        if self.snapshot() != before {
                            return Err("edit: refused, but the snapshot changed".into());
                        }
                        Ok(expected)
                    }
                }
            }
            QueueSurface::RemoveQueuedMessage => {
                let before = self.snapshot();
                let outcome = self
                    .run_as(
                        cell,
                        self.svc
                            .agent_remove_queued_message_op(self.agent.clone(), id.clone()),
                    )
                    .await;
                match outcome {
                    Ok(_) => {
                        if self.svc.find_queued_message(&self.agent, &id).is_some() {
                            return Err("remove: the entry is still queued".into());
                        }
                        Ok(Expected::Allowed)
                    }
                    Err(err) => {
                        let expected = classify_error(err)?;
                        if self.snapshot() != before {
                            return Err("remove: refused, but the snapshot changed".into());
                        }
                        Ok(expected)
                    }
                }
            }
            QueueSurface::SendQueuedMessageNow => {
                let before = self.snapshot();
                let outcome = self
                    .run_as(
                        cell,
                        self.svc
                            .agent_send_queued_message_now_op(self.agent.clone(), id.clone()),
                    )
                    .await;
                match outcome {
                    Ok(result) => {
                        if result["success"] != true || result["messageId"] != id {
                            return Err(format!("sendNow: unexpected result {result}"));
                        }
                        if self.svc.find_queued_message(&self.agent, &id).is_some() {
                            return Err("sendNow: the entry is still queued".into());
                        }
                        Ok(Expected::Allowed)
                    }
                    Err(err) => {
                        let expected = classify_error(err)?;
                        if self.snapshot() != before {
                            return Err("sendNow: refused, but the snapshot changed".into());
                        }
                        Ok(expected)
                    }
                }
            }
            QueueSurface::QueueUpdatedEvent | QueueSurface::QueueProcessingEvent => {
                Err("transport-harness surface".into())
            }
        }
    }

    /// Leave nothing behind for the next cell: the seeded entry (whatever
    /// the cell did to it) is removed unbound.
    async fn cleanup(&self, entry: &QueuedMessage) {
        self.svc
            .agent_remove_queued_message_op(self.agent.clone(), entry.id.clone())
            .await
            .expect("cleanup remove");
        assert!(self.snapshot().is_empty(), "queue drained between cells");
    }
}

/// Every [`Harness::Services`] cell passes through the real services under
/// its caller; a mismatch names the cell(s). The `match` in
/// [`Fixture::drive`] is exhaustive over [`QueueSurface`], and the set of
/// surfaces exercised is asserted against [`QueueSurface::all`] at the end.
#[tokio::test]
async fn services_surfaces_match_the_contract_table() {
    let fx = Fixture::new().await;
    let cells: Vec<Cell> = QUEUE_VISIBILITY_CONTRACT
        .iter()
        .copied()
        .filter(|c| c.surface.owner() == Harness::Services)
        .collect();
    assert!(!cells.is_empty());

    let mut exercised: HashSet<QueueSurface> = HashSet::new();
    let mut failures: Vec<String> = Vec::new();
    for &cell in &cells {
        let entry = fx.seed(cell);
        match fx.drive(cell, &entry).await {
            Ok(observed) if observed == cell.expected => {}
            Ok(observed) => failures.push(format!(
                "{}: expected {:?}, observed {observed:?}",
                cell.name(),
                cell.expected
            )),
            Err(text) => failures.push(format!("{}: {text}", cell.name())),
        }
        exercised.insert(cell.surface);
        fx.cleanup(&entry).await;
    }
    assert!(
        failures.is_empty(),
        "{} contract cell(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );

    let services_surfaces: HashSet<QueueSurface> = QueueSurface::all()
        .iter()
        .copied()
        .filter(|s| s.owner() == Harness::Services)
        .collect();
    assert_eq!(
        exercised, services_surfaces,
        "every services surface was driven"
    );
    assert_eq!(
        cells.len(),
        CallerClass::ALL.len() * AttributionTier::ALL.len() * services_surfaces.len()
    );
}
