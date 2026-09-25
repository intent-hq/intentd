//! The per-user queue visibility / mutation contract as ONE data table
//! (multiplayer, intentd#2068): every (caller class × attribution tier ×
//! surface) cell of the policy with its expected outcome, so the services
//! and transport contract harnesses and the queue-entry egress lint read a
//! single source of truth and fail by cell name.
//!
//! The outcomes encode the policy as merged in #2068 — the doc comments on
//! [`crate::queue_attribution_visible_to`], `QueueEntryGate::check`
//! (intent-services), `project_queue_event_for_current_caller`
//! (intent-transport) and [`crate::project_queue_for_caller`] — plus the
//! `agent.diagnostics` decision: its queue entries are projected per caller
//! exactly like `agent.getQueue`, so the [`QueueSurface::Diagnostics`] rows
//! mirror the [`QueueSurface::GetQueue`] rows.
//!
//! This module carries no behavior: the predicate stays in [`crate::caller`],
//! and a unit test here cross-checks every pure-policy cell against it.

use crate::ids::{AgentId, PrincipalId};
use crate::{Caller, QueueAttribution};

/// The principal a wire caller in the table is bound to.
pub const CALLER_PRINCIPAL: &str = "principal-caller";
/// The other workspace member a foreign stamp names.
pub const OTHER_PRINCIPAL: &str = "principal-other";
/// The agent session an [`CallerClass::Agent`] caller runs as.
pub const AGENT_CALLER: &str = "agent-caller";

/// Who is asking. `AuthorGuest` and `ForeignGuest` are the same
/// non-administrator wire principal; they differ only in whether a
/// [`AttributionTier::PrincipalStamped`] entry is the caller's own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CallerClass {
    /// Wire caller with `is_administrator: true` (the workspace owner).
    Administrator,
    /// Wire guest who authored the entry.
    AuthorGuest,
    /// Wire guest looking at someone else's entry.
    ForeignGuest,
    /// [`Caller::Agent`] calling back through `workspace_api`.
    Agent,
    /// [`Caller::Daemon`] (hook runs, background work).
    Daemon,
    /// No bound caller (`current_caller()` is `None`).
    Unbound,
}

impl CallerClass {
    /// Every caller class, in table order.
    pub const ALL: &'static [CallerClass] = &[
        CallerClass::Administrator,
        CallerClass::AuthorGuest,
        CallerClass::ForeignGuest,
        CallerClass::Agent,
        CallerClass::Daemon,
        CallerClass::Unbound,
    ];

    /// The variant name as it appears in [`Cell::name`].
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            CallerClass::Administrator => "Administrator",
            CallerClass::AuthorGuest => "AuthorGuest",
            CallerClass::ForeignGuest => "ForeignGuest",
            CallerClass::Agent => "Agent",
            CallerClass::Daemon => "Daemon",
            CallerClass::Unbound => "Unbound",
        }
    }

    /// The [`Caller`] to bind for this class; `None` for [`Self::Unbound`].
    /// Wire callers are bound to [`CALLER_PRINCIPAL`].
    #[must_use]
    pub fn caller(self) -> Option<Caller> {
        match self {
            CallerClass::Administrator => Some(Caller::Wire {
                principal_id: PrincipalId(CALLER_PRINCIPAL.into()),
                host_role: crate::HostRole::Owner,
            }),
            CallerClass::AuthorGuest | CallerClass::ForeignGuest => Some(Caller::Wire {
                principal_id: PrincipalId(CALLER_PRINCIPAL.into()),
                host_role: crate::HostRole::Guest,
            }),
            CallerClass::Agent => Some(Caller::Agent {
                agent_id: AgentId(AGENT_CALLER.into()),
            }),
            CallerClass::Daemon => Some(Caller::Daemon),
            CallerClass::Unbound => None,
        }
    }
}

/// Who the entry is attributed to ([`QueueAttribution`]'s three tiers).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AttributionTier {
    /// Resolves to a principal: the caller's own for
    /// [`CallerClass::AuthorGuest`], another member's
    /// ([`OTHER_PRINCIPAL`]) for every other caller class — so the
    /// administrator's row is the interesting one where it sees an entry it
    /// did not author.
    PrincipalStamped,
    /// Human-origin entry nobody could attribute (fails closed).
    UnknownHuman,
    /// Agent-sent / automatic entry (public).
    Unattributed,
}

impl AttributionTier {
    /// Every tier, in table order.
    pub const ALL: &'static [AttributionTier] = &[
        AttributionTier::PrincipalStamped,
        AttributionTier::UnknownHuman,
        AttributionTier::Unattributed,
    ];

    /// The variant name as it appears in [`Cell::name`].
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            AttributionTier::PrincipalStamped => "PrincipalStamped",
            AttributionTier::UnknownHuman => "UnknownHuman",
            AttributionTier::Unattributed => "Unattributed",
        }
    }

    /// The [`QueueAttribution`] of an entry in this tier as seen by `caller`
    /// (see [`Self::PrincipalStamped`] for whose stamp it carries).
    #[must_use]
    pub fn attribution(self, caller: CallerClass) -> QueueAttribution {
        match self {
            AttributionTier::PrincipalStamped => {
                QueueAttribution::Principal(PrincipalId(if caller == CallerClass::AuthorGuest {
                    CALLER_PRINCIPAL.into()
                } else {
                    OTHER_PRINCIPAL.into()
                }))
            }
            AttributionTier::UnknownHuman => QueueAttribution::UnknownHuman,
            AttributionTier::Unattributed => QueueAttribution::Unattributed,
        }
    }
}

/// Which contract harness drives a surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Harness {
    /// intent-services: RPC results and the per-id mutation gate.
    Services,
    /// intent-transport: subscriber event projection.
    Transport,
}

/// Every place a queue entry crosses the daemon boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueueSurface {
    /// `agent.getQueue` result (`project_queue_for_caller`).
    GetQueue,
    /// `agent:queue:updated` event `data.queue`.
    QueueUpdatedEvent,
    /// `agent:queue:processing` event `data.content`.
    QueueProcessingEvent,
    /// `agent.editQueuedMessage` (gate with `author_only`).
    EditQueuedMessage,
    /// `agent.removeQueuedMessage` (gate).
    RemoveQueuedMessage,
    /// `agent.sendQueuedMessageNow` (gate).
    SendQueuedMessageNow,
    /// `agent.diagnostics` `queues[].entries` (mirrors [`Self::GetQueue`]).
    Diagnostics,
}

impl QueueSurface {
    /// Every surface, in table order.
    #[must_use]
    pub const fn all() -> &'static [QueueSurface] {
        &[
            QueueSurface::GetQueue,
            QueueSurface::QueueUpdatedEvent,
            QueueSurface::QueueProcessingEvent,
            QueueSurface::EditQueuedMessage,
            QueueSurface::RemoveQueuedMessage,
            QueueSurface::SendQueuedMessageNow,
            QueueSurface::Diagnostics,
        ]
    }

    /// The harness that drives this surface.
    #[must_use]
    pub const fn owner(&self) -> Harness {
        match self {
            QueueSurface::GetQueue
            | QueueSurface::EditQueuedMessage
            | QueueSurface::RemoveQueuedMessage
            | QueueSurface::SendQueuedMessageNow
            | QueueSurface::Diagnostics => Harness::Services,
            QueueSurface::QueueUpdatedEvent | QueueSurface::QueueProcessingEvent => {
                Harness::Transport
            }
        }
    }

    /// `true` for a read surface whose outcome is fully determined by
    /// [`crate::queue_attribution_visible_to`] (visible / hidden / redacted);
    /// `false` for the per-id mutations, whose gate adds the author-only rule.
    #[must_use]
    pub const fn is_pure_policy(&self) -> bool {
        matches!(
            self,
            QueueSurface::GetQueue
                | QueueSurface::QueueUpdatedEvent
                | QueueSurface::QueueProcessingEvent
                | QueueSurface::Diagnostics
        )
    }

    /// The variant name as it appears in [`Cell::name`].
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            QueueSurface::GetQueue => "GetQueue",
            QueueSurface::QueueUpdatedEvent => "QueueUpdatedEvent",
            QueueSurface::QueueProcessingEvent => "QueueProcessingEvent",
            QueueSurface::EditQueuedMessage => "EditQueuedMessage",
            QueueSurface::RemoveQueuedMessage => "RemoveQueuedMessage",
            QueueSurface::SendQueuedMessageNow => "SendQueuedMessageNow",
            QueueSurface::Diagnostics => "Diagnostics",
        }
    }
}

/// What the caller gets for the entry on that surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Expected {
    /// Read surface: the entry is served whole.
    Visible,
    /// Read surface: the entry is dropped from the snapshot.
    Hidden,
    /// `agent:queue:processing`: the frame keeps `agentId` / `messageId` /
    /// `turnId` but loses `data.content`.
    ContentRedacted,
    /// Mutation: performed.
    Allowed,
    /// Mutation: `-32602 "queued message not found"`, no side effects (the
    /// entry is hidden from the caller, so it reads as absent).
    NotFound,
    /// Mutation: `-32602 "can only be edited by its author"` — a visible
    /// entry the caller did not author (`agent.editQueuedMessage` only).
    AuthorOnly,
}

/// One cell of the contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Cell {
    pub caller: CallerClass,
    pub tier: AttributionTier,
    pub surface: QueueSurface,
    pub expected: Expected,
}

impl Cell {
    /// `(<caller>, <tier>, <surface>)` for failure messages.
    #[must_use]
    pub fn name(&self) -> String {
        format!(
            "({}, {}, {})",
            self.caller.label(),
            self.tier.label(),
            self.surface.label()
        )
    }

    /// The [`Caller`] to bind when driving this cell (`None` for
    /// [`CallerClass::Unbound`]).
    #[must_use]
    pub fn caller(&self) -> Option<Caller> {
        self.caller.caller()
    }

    /// The entry attribution this cell exercises.
    #[must_use]
    pub fn attribution(&self) -> QueueAttribution {
        self.tier.attribution(self.caller)
    }
}

/// The cell for `(caller, tier, surface)`, or `None` when the table has no
/// such row (the completeness test guarantees it always does).
#[must_use]
pub fn contract_cell(
    caller: CallerClass,
    tier: AttributionTier,
    surface: QueueSurface,
) -> Option<&'static Cell> {
    QUEUE_VISIBILITY_CONTRACT
        .iter()
        .find(|c| c.caller == caller && c.tier == tier && c.surface == surface)
}

const fn cell(
    caller: CallerClass,
    tier: AttributionTier,
    surface: QueueSurface,
    expected: Expected,
) -> Cell {
    Cell {
        caller,
        tier,
        surface,
        expected,
    }
}

use self::{AttributionTier as T, CallerClass as C, Expected as E, QueueSurface as S};

/// Every (caller class × attribution tier × surface) cell exactly once;
/// [`tests::contract_is_complete_and_unique`] asserts it. Grouped by caller
/// class; within a class by tier; within a tier in [`QueueSurface::all`]
/// order.
#[rustfmt::skip]
pub const QUEUE_VISIBILITY_CONTRACT: &[Cell] = &[
    // Administrator: sees the whole queue; the author-only edit rule refuses
    // another principal's entry and an unknown-human one.
    cell(C::Administrator, T::PrincipalStamped, S::GetQueue, E::Visible),
    cell(C::Administrator, T::PrincipalStamped, S::QueueUpdatedEvent, E::Visible),
    cell(C::Administrator, T::PrincipalStamped, S::QueueProcessingEvent, E::Visible),
    cell(C::Administrator, T::PrincipalStamped, S::EditQueuedMessage, E::AuthorOnly),
    cell(C::Administrator, T::PrincipalStamped, S::RemoveQueuedMessage, E::Allowed),
    cell(C::Administrator, T::PrincipalStamped, S::SendQueuedMessageNow, E::Allowed),
    cell(C::Administrator, T::PrincipalStamped, S::Diagnostics, E::Visible),
    cell(C::Administrator, T::UnknownHuman, S::GetQueue, E::Visible),
    cell(C::Administrator, T::UnknownHuman, S::QueueUpdatedEvent, E::Visible),
    cell(C::Administrator, T::UnknownHuman, S::QueueProcessingEvent, E::Visible),
    cell(C::Administrator, T::UnknownHuman, S::EditQueuedMessage, E::AuthorOnly),
    cell(C::Administrator, T::UnknownHuman, S::RemoveQueuedMessage, E::Allowed),
    cell(C::Administrator, T::UnknownHuman, S::SendQueuedMessageNow, E::Allowed),
    cell(C::Administrator, T::UnknownHuman, S::Diagnostics, E::Visible),
    cell(C::Administrator, T::Unattributed, S::GetQueue, E::Visible),
    cell(C::Administrator, T::Unattributed, S::QueueUpdatedEvent, E::Visible),
    cell(C::Administrator, T::Unattributed, S::QueueProcessingEvent, E::Visible),
    cell(C::Administrator, T::Unattributed, S::EditQueuedMessage, E::Allowed),
    cell(C::Administrator, T::Unattributed, S::RemoveQueuedMessage, E::Allowed),
    cell(C::Administrator, T::Unattributed, S::SendQueuedMessageNow, E::Allowed),
    cell(C::Administrator, T::Unattributed, S::Diagnostics, E::Visible),
    // AuthorGuest: own stamped entries and unattributed ones; an
    // unknown-human entry is withheld like a foreign one.
    cell(C::AuthorGuest, T::PrincipalStamped, S::GetQueue, E::Visible),
    cell(C::AuthorGuest, T::PrincipalStamped, S::QueueUpdatedEvent, E::Visible),
    cell(C::AuthorGuest, T::PrincipalStamped, S::QueueProcessingEvent, E::Visible),
    cell(C::AuthorGuest, T::PrincipalStamped, S::EditQueuedMessage, E::Allowed),
    cell(C::AuthorGuest, T::PrincipalStamped, S::RemoveQueuedMessage, E::Allowed),
    cell(C::AuthorGuest, T::PrincipalStamped, S::SendQueuedMessageNow, E::Allowed),
    cell(C::AuthorGuest, T::PrincipalStamped, S::Diagnostics, E::Visible),
    cell(C::AuthorGuest, T::UnknownHuman, S::GetQueue, E::Hidden),
    cell(C::AuthorGuest, T::UnknownHuman, S::QueueUpdatedEvent, E::Hidden),
    cell(C::AuthorGuest, T::UnknownHuman, S::QueueProcessingEvent, E::ContentRedacted),
    cell(C::AuthorGuest, T::UnknownHuman, S::EditQueuedMessage, E::NotFound),
    cell(C::AuthorGuest, T::UnknownHuman, S::RemoveQueuedMessage, E::NotFound),
    cell(C::AuthorGuest, T::UnknownHuman, S::SendQueuedMessageNow, E::NotFound),
    cell(C::AuthorGuest, T::UnknownHuman, S::Diagnostics, E::Hidden),
    cell(C::AuthorGuest, T::Unattributed, S::GetQueue, E::Visible),
    cell(C::AuthorGuest, T::Unattributed, S::QueueUpdatedEvent, E::Visible),
    cell(C::AuthorGuest, T::Unattributed, S::QueueProcessingEvent, E::Visible),
    cell(C::AuthorGuest, T::Unattributed, S::EditQueuedMessage, E::Allowed),
    cell(C::AuthorGuest, T::Unattributed, S::RemoveQueuedMessage, E::Allowed),
    cell(C::AuthorGuest, T::Unattributed, S::SendQueuedMessageNow, E::Allowed),
    cell(C::AuthorGuest, T::Unattributed, S::Diagnostics, E::Visible),
    // ForeignGuest: only unattributed entries; everything human-authored by
    // someone else (or by nobody anyone can name) reads as absent.
    cell(C::ForeignGuest, T::PrincipalStamped, S::GetQueue, E::Hidden),
    cell(C::ForeignGuest, T::PrincipalStamped, S::QueueUpdatedEvent, E::Hidden),
    cell(C::ForeignGuest, T::PrincipalStamped, S::QueueProcessingEvent, E::ContentRedacted),
    cell(C::ForeignGuest, T::PrincipalStamped, S::EditQueuedMessage, E::NotFound),
    cell(C::ForeignGuest, T::PrincipalStamped, S::RemoveQueuedMessage, E::NotFound),
    cell(C::ForeignGuest, T::PrincipalStamped, S::SendQueuedMessageNow, E::NotFound),
    cell(C::ForeignGuest, T::PrincipalStamped, S::Diagnostics, E::Hidden),
    cell(C::ForeignGuest, T::UnknownHuman, S::GetQueue, E::Hidden),
    cell(C::ForeignGuest, T::UnknownHuman, S::QueueUpdatedEvent, E::Hidden),
    cell(C::ForeignGuest, T::UnknownHuman, S::QueueProcessingEvent, E::ContentRedacted),
    cell(C::ForeignGuest, T::UnknownHuman, S::EditQueuedMessage, E::NotFound),
    cell(C::ForeignGuest, T::UnknownHuman, S::RemoveQueuedMessage, E::NotFound),
    cell(C::ForeignGuest, T::UnknownHuman, S::SendQueuedMessageNow, E::NotFound),
    cell(C::ForeignGuest, T::UnknownHuman, S::Diagnostics, E::Hidden),
    cell(C::ForeignGuest, T::Unattributed, S::GetQueue, E::Visible),
    cell(C::ForeignGuest, T::Unattributed, S::QueueUpdatedEvent, E::Visible),
    cell(C::ForeignGuest, T::Unattributed, S::QueueProcessingEvent, E::Visible),
    cell(C::ForeignGuest, T::Unattributed, S::EditQueuedMessage, E::Allowed),
    cell(C::ForeignGuest, T::Unattributed, S::RemoveQueuedMessage, E::Allowed),
    cell(C::ForeignGuest, T::Unattributed, S::SendQueuedMessageNow, E::Allowed),
    cell(C::ForeignGuest, T::Unattributed, S::Diagnostics, E::Visible),
    // Agent: acts on its own authority; sees and may mutate everything.
    cell(C::Agent, T::PrincipalStamped, S::GetQueue, E::Visible),
    cell(C::Agent, T::PrincipalStamped, S::QueueUpdatedEvent, E::Visible),
    cell(C::Agent, T::PrincipalStamped, S::QueueProcessingEvent, E::Visible),
    cell(C::Agent, T::PrincipalStamped, S::EditQueuedMessage, E::Allowed),
    cell(C::Agent, T::PrincipalStamped, S::RemoveQueuedMessage, E::Allowed),
    cell(C::Agent, T::PrincipalStamped, S::SendQueuedMessageNow, E::Allowed),
    cell(C::Agent, T::PrincipalStamped, S::Diagnostics, E::Visible),
    cell(C::Agent, T::UnknownHuman, S::GetQueue, E::Visible),
    cell(C::Agent, T::UnknownHuman, S::QueueUpdatedEvent, E::Visible),
    cell(C::Agent, T::UnknownHuman, S::QueueProcessingEvent, E::Visible),
    cell(C::Agent, T::UnknownHuman, S::EditQueuedMessage, E::Allowed),
    cell(C::Agent, T::UnknownHuman, S::RemoveQueuedMessage, E::Allowed),
    cell(C::Agent, T::UnknownHuman, S::SendQueuedMessageNow, E::Allowed),
    cell(C::Agent, T::UnknownHuman, S::Diagnostics, E::Visible),
    cell(C::Agent, T::Unattributed, S::GetQueue, E::Visible),
    cell(C::Agent, T::Unattributed, S::QueueUpdatedEvent, E::Visible),
    cell(C::Agent, T::Unattributed, S::QueueProcessingEvent, E::Visible),
    cell(C::Agent, T::Unattributed, S::EditQueuedMessage, E::Allowed),
    cell(C::Agent, T::Unattributed, S::RemoveQueuedMessage, E::Allowed),
    cell(C::Agent, T::Unattributed, S::SendQueuedMessageNow, E::Allowed),
    cell(C::Agent, T::Unattributed, S::Diagnostics, E::Visible),
    // Daemon: internal work; unrestricted.
    cell(C::Daemon, T::PrincipalStamped, S::GetQueue, E::Visible),
    cell(C::Daemon, T::PrincipalStamped, S::QueueUpdatedEvent, E::Visible),
    cell(C::Daemon, T::PrincipalStamped, S::QueueProcessingEvent, E::Visible),
    cell(C::Daemon, T::PrincipalStamped, S::EditQueuedMessage, E::Allowed),
    cell(C::Daemon, T::PrincipalStamped, S::RemoveQueuedMessage, E::Allowed),
    cell(C::Daemon, T::PrincipalStamped, S::SendQueuedMessageNow, E::Allowed),
    cell(C::Daemon, T::PrincipalStamped, S::Diagnostics, E::Visible),
    cell(C::Daemon, T::UnknownHuman, S::GetQueue, E::Visible),
    cell(C::Daemon, T::UnknownHuman, S::QueueUpdatedEvent, E::Visible),
    cell(C::Daemon, T::UnknownHuman, S::QueueProcessingEvent, E::Visible),
    cell(C::Daemon, T::UnknownHuman, S::EditQueuedMessage, E::Allowed),
    cell(C::Daemon, T::UnknownHuman, S::RemoveQueuedMessage, E::Allowed),
    cell(C::Daemon, T::UnknownHuman, S::SendQueuedMessageNow, E::Allowed),
    cell(C::Daemon, T::UnknownHuman, S::Diagnostics, E::Visible),
    cell(C::Daemon, T::Unattributed, S::GetQueue, E::Visible),
    cell(C::Daemon, T::Unattributed, S::QueueUpdatedEvent, E::Visible),
    cell(C::Daemon, T::Unattributed, S::QueueProcessingEvent, E::Visible),
    cell(C::Daemon, T::Unattributed, S::EditQueuedMessage, E::Allowed),
    cell(C::Daemon, T::Unattributed, S::RemoveQueuedMessage, E::Allowed),
    cell(C::Daemon, T::Unattributed, S::SendQueuedMessageNow, E::Allowed),
    cell(C::Daemon, T::Unattributed, S::Diagnostics, E::Visible),
    // Unbound: no caller to filter for; the projections and the gate
    // (`queue_entry_gate` → `None`) pass everything through.
    cell(C::Unbound, T::PrincipalStamped, S::GetQueue, E::Visible),
    cell(C::Unbound, T::PrincipalStamped, S::QueueUpdatedEvent, E::Visible),
    cell(C::Unbound, T::PrincipalStamped, S::QueueProcessingEvent, E::Visible),
    cell(C::Unbound, T::PrincipalStamped, S::EditQueuedMessage, E::Allowed),
    cell(C::Unbound, T::PrincipalStamped, S::RemoveQueuedMessage, E::Allowed),
    cell(C::Unbound, T::PrincipalStamped, S::SendQueuedMessageNow, E::Allowed),
    cell(C::Unbound, T::PrincipalStamped, S::Diagnostics, E::Visible),
    cell(C::Unbound, T::UnknownHuman, S::GetQueue, E::Visible),
    cell(C::Unbound, T::UnknownHuman, S::QueueUpdatedEvent, E::Visible),
    cell(C::Unbound, T::UnknownHuman, S::QueueProcessingEvent, E::Visible),
    cell(C::Unbound, T::UnknownHuman, S::EditQueuedMessage, E::Allowed),
    cell(C::Unbound, T::UnknownHuman, S::RemoveQueuedMessage, E::Allowed),
    cell(C::Unbound, T::UnknownHuman, S::SendQueuedMessageNow, E::Allowed),
    cell(C::Unbound, T::UnknownHuman, S::Diagnostics, E::Visible),
    cell(C::Unbound, T::Unattributed, S::GetQueue, E::Visible),
    cell(C::Unbound, T::Unattributed, S::QueueUpdatedEvent, E::Visible),
    cell(C::Unbound, T::Unattributed, S::QueueProcessingEvent, E::Visible),
    cell(C::Unbound, T::Unattributed, S::EditQueuedMessage, E::Allowed),
    cell(C::Unbound, T::Unattributed, S::RemoveQueuedMessage, E::Allowed),
    cell(C::Unbound, T::Unattributed, S::SendQueuedMessageNow, E::Allowed),
    cell(C::Unbound, T::Unattributed, S::Diagnostics, E::Visible),
];

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{project_queue_for_caller, queue_attribution_visible_to};
    use std::collections::HashSet;

    #[test]
    fn contract_is_complete_and_unique() {
        let mut seen = HashSet::new();
        for c in QUEUE_VISIBILITY_CONTRACT {
            assert!(
                seen.insert((c.caller, c.tier, c.surface)),
                "duplicate cell {}",
                c.name()
            );
        }
        for &caller in CallerClass::ALL {
            for &tier in AttributionTier::ALL {
                for &surface in QueueSurface::all() {
                    assert!(
                        seen.contains(&(caller, tier, surface)),
                        "missing cell ({}, {}, {})",
                        caller.label(),
                        tier.label(),
                        surface.label()
                    );
                    assert!(contract_cell(caller, tier, surface).is_some());
                }
            }
        }
        assert_eq!(
            QUEUE_VISIBILITY_CONTRACT.len(),
            CallerClass::ALL.len() * AttributionTier::ALL.len() * QueueSurface::all().len()
        );
    }

    /// `true` when the caller may see the entry per the one shared predicate;
    /// an unbound caller is never filtered (`project_queue_for_caller(None)`).
    fn predicate_visible(c: Cell) -> bool {
        if let Some(caller) = c.caller() {
            return queue_attribution_visible_to(&caller, &c.attribution());
        }
        let queue = vec![serde_json::json!({ "id": "e" })];
        project_queue_for_caller(None, queue.clone()) == queue
    }

    #[test]
    fn pure_policy_cells_match_the_visibility_predicate() {
        for c in QUEUE_VISIBILITY_CONTRACT
            .iter()
            .filter(|c| c.surface.is_pure_policy())
        {
            let expected = match (predicate_visible(*c), c.surface) {
                (true, _) => Expected::Visible,
                (false, QueueSurface::QueueProcessingEvent) => Expected::ContentRedacted,
                (false, _) => Expected::Hidden,
            };
            assert_eq!(c.expected, expected, "{}", c.name());
        }
    }

    #[test]
    fn mutation_cells_hide_exactly_what_the_predicate_hides() {
        for c in QUEUE_VISIBILITY_CONTRACT
            .iter()
            .filter(|c| !c.surface.is_pure_policy())
        {
            assert!(
                matches!(
                    c.expected,
                    Expected::Allowed | Expected::NotFound | Expected::AuthorOnly
                ),
                "{}: {:?} is not a mutation outcome",
                c.name(),
                c.expected
            );
            assert_eq!(
                c.expected == Expected::NotFound,
                !predicate_visible(*c),
                "{}: NotFound iff the predicate hides the entry",
                c.name()
            );
            if c.expected == Expected::AuthorOnly {
                assert_eq!(c.surface, QueueSurface::EditQueuedMessage, "{}", c.name());
                assert_eq!(c.caller, CallerClass::Administrator, "{}", c.name());
                assert_ne!(c.tier, AttributionTier::Unattributed, "{}", c.name());
            }
        }
    }

    #[test]
    fn diagnostics_rows_mirror_get_queue() {
        for &caller in CallerClass::ALL {
            for &tier in AttributionTier::ALL {
                let get = contract_cell(caller, tier, QueueSurface::GetQueue).unwrap();
                let diag = contract_cell(caller, tier, QueueSurface::Diagnostics).unwrap();
                assert_eq!(get.expected, diag.expected, "{}", diag.name());
            }
        }
    }

    #[test]
    fn every_surface_has_an_owner_and_both_harnesses_own_something() {
        let owners: HashSet<Harness> = QueueSurface::all()
            .iter()
            .map(QueueSurface::owner)
            .collect();
        assert_eq!(
            owners,
            HashSet::from([Harness::Services, Harness::Transport])
        );
        assert_eq!(
            QueueSurface::QueueProcessingEvent.owner(),
            Harness::Transport
        );
        assert_eq!(QueueSurface::EditQueuedMessage.owner(), Harness::Services);
    }

    #[test]
    fn cell_name_is_the_parenthesised_triple() {
        let c = contract_cell(
            CallerClass::ForeignGuest,
            AttributionTier::PrincipalStamped,
            QueueSurface::QueueProcessingEvent,
        )
        .unwrap();
        assert_eq!(
            c.name(),
            "(ForeignGuest, PrincipalStamped, QueueProcessingEvent)"
        );
        assert_eq!(c.expected, Expected::ContentRedacted);
        let c = contract_cell(
            CallerClass::Administrator,
            AttributionTier::UnknownHuman,
            QueueSurface::EditQueuedMessage,
        )
        .unwrap();
        assert_eq!(c.name(), "(Administrator, UnknownHuman, EditQueuedMessage)");
        assert_eq!(c.expected, Expected::AuthorOnly);
    }

    #[test]
    fn fixtures_distinguish_author_from_foreign_stamp() {
        let own = AttributionTier::PrincipalStamped.attribution(CallerClass::AuthorGuest);
        let foreign = AttributionTier::PrincipalStamped.attribution(CallerClass::ForeignGuest);
        assert_eq!(
            own,
            QueueAttribution::Principal(PrincipalId(CALLER_PRINCIPAL.into()))
        );
        assert_eq!(
            foreign,
            QueueAttribution::Principal(PrincipalId(OTHER_PRINCIPAL.into()))
        );
        assert_eq!(
            CallerClass::AuthorGuest.caller(),
            CallerClass::ForeignGuest.caller()
        );
        assert!(CallerClass::Unbound.caller().is_none());
        assert!(CallerClass::Administrator
            .caller()
            .is_some_and(|c| c.is_administrator()));
    }
}
