//! Read-only agent-session projections and the sealed bound that keeps them
//! out of the full-row write paths.
//!
//! The summary reads ([`Store::get_agent_session_summary`] and the
//! `list_*_summaries` family) select the summary column list, which omits
//! the heavy `system_prompt`, `image_blocks` and `initial_message` columns and
//! never hydrates the message log. A summary row is therefore an INCOMPLETE
//! [`AgentSession`]: persisting it through [`Store::update_agent_session`]
//! would write `NULL` over the stored `system_prompt`.
//!
//! [`AgentSessionSummary`] makes that mistake a compile error instead of a
//! review finding. It dereferences to [`AgentSession`] for field reads, but it
//! offers no `DerefMut`, and the store's full-row writers accept only types that
//! implement the sealed [`FullAgentSessionRow`] bound — which
//! [`AgentSessionSummary`] does not. Because those writers are generic over
//! the bound, deref coercion never turns `&AgentSessionSummary` into
//! `&AgentSession` at the call site.
//!
//! The one way back to an owned [`AgentSession`] is the explicitly named
//! [`AgentSessionSummary::into_read_only_session`], reserved for read-only
//! wire projections. A caller that must update a session it found through a
//! summary re-reads the complete row with
//! [`Store::get_agent_session_without_messages`] (or uses a narrow column
//! writer such as [`Store::set_agent_session_status`]).
//!
//! `tests/compile_fail.rs` pins these rejections with trybuild
//! compile-fail cases.
//!
//! [`Store::get_agent_session_summary`]: crate::Store::get_agent_session_summary
//! [`Store::get_agent_session_without_messages`]: crate::Store::get_agent_session_without_messages
//! [`Store::update_agent_session`]: crate::Store::update_agent_session
//! [`Store::set_agent_session_status`]: crate::Store::set_agent_session_status

use std::ops::Deref;

use intent_core::AgentSession;
use serde::Serialize;

/// Metadata-only view of an agent session: every column of the store's
/// summary column list, with `system_prompt`, `image_blocks` and
/// `initial_message` left `None` and `messages` left empty. Read fields through `Deref`; it cannot be written
/// back (see the [module docs](self)).
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct AgentSessionSummary(AgentSession);

impl AgentSessionSummary {
    /// Wrap a session as a summary, clearing the columns a summary read does
    /// not select (`system_prompt`, `image_blocks`, `initial_message`) and the
    /// message log, so every value of this type has the summary shape no
    /// matter where it came from.
    #[must_use]
    pub fn from_session(mut session: AgentSession) -> Self {
        session.system_prompt = None;
        session.image_blocks = None;
        session.initial_message = None;
        session.messages = Vec::new();
        Self(session)
    }

    /// Unwrap into an owned [`AgentSession`] for READ-ONLY use: wire
    /// projections (`AgentLite` and its runtime-flag overlay take the session
    /// by value). The result still lacks `system_prompt` and the other heavy
    /// columns: never pass it to a store writer. A caller that needs to persist a
    /// change re-reads the full row with
    /// [`Store::get_agent_session_without_messages`](crate::Store::get_agent_session_without_messages).
    #[must_use]
    pub fn into_read_only_session(self) -> AgentSession {
        self.0
    }
}

impl Deref for AgentSessionSummary {
    type Target = AgentSession;

    fn deref(&self) -> &AgentSession {
        &self.0
    }
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for intent_core::AgentSession {}
}

/// A complete agent-session row: the only shape the store's full-row writers
/// ([`Store::insert_agent_session`](crate::Store::insert_agent_session),
/// [`Store::update_agent_session`](crate::Store::update_agent_session) and
/// their variants) accept. Sealed and implemented only for [`AgentSession`],
/// so an [`AgentSessionSummary`] — which lacks `system_prompt` and the other
/// heavy columns — is rejected at compile time.
pub trait FullAgentSessionRow: sealed::Sealed + Sync {
    /// The row to persist.
    fn full_row(&self) -> &AgentSession;
}

impl FullAgentSessionRow for AgentSession {
    fn full_row(&self) -> &AgentSession {
        self
    }
}
