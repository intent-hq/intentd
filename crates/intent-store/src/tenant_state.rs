//! The tenant state (orchestrator mode): whether a tenant takes writes at
//! all, whoever holds its lease. It is the `state` column of the tenant's
//! `tenant_writer` row, read by every writer under the writer lock it
//! already takes (`postgres::lease` has the Postgres side):
//!
//! - `open`: writes run as usual.
//! - `closing`: the tenant is being stopped, retired or migrated. A write in
//!   flight when the state moved commits (the move waits for its row lock);
//!   every later write is refused with `Error::TenantClosed`.
//! - `closed`: the tenant's work has stopped too. Writes are refused.
//!
//! Moves: to `closing` from `open` (or `closing`), to `closed` from
//! `closing` (or `closed`), and back to `open` from any state. A move from
//! another state changes nothing and reports the state it found, so a
//! tenant reopened while a stop was closing it stays open.
//!
//! `SQLite` has one daemon per database: a `SQLite` tenant is always open
//! and a move is a no-op.

use intent_core::{Error, Result};
use std::fmt;

/// Whether a tenant takes writes; see the [module docs](self).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TenantState {
    /// Writes run.
    #[default]
    Open,
    /// Writes are refused; the tenant's work is stopping.
    Closing,
    /// Writes are refused; the tenant's work has stopped.
    Closed,
}

impl TenantState {
    /// The column value.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Closing => "closing",
            Self::Closed => "closed",
        }
    }

    /// The states a move to `self` may start from.
    #[must_use]
    pub fn moves_from(self) -> &'static [TenantState] {
        match self {
            Self::Open => &[Self::Open, Self::Closing, Self::Closed],
            Self::Closing => &[Self::Open, Self::Closing],
            Self::Closed => &[Self::Closing, Self::Closed],
        }
    }
}

impl fmt::Display for TenantState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<String> for TenantState {
    type Error = Error;

    fn try_from(value: String) -> Result<Self> {
        match value.as_str() {
            "open" => Ok(Self::Open),
            "closing" => Ok(Self::Closing),
            "closed" => Ok(Self::Closed),
            other => Err(Error::Internal(format!("unknown tenant state {other:?}"))),
        }
    }
}

/// What a tenant state move found and left.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantStateMove {
    /// The state before the move.
    pub from: TenantState,
    /// The state after it: the asked-for state when the move was allowed,
    /// otherwise `from`, unchanged.
    pub state: TenantState,
    /// The tenant's row counter (`tenant_writer.last_row_seq`) when the
    /// move committed. Every row a write committed before the move has a
    /// `row_seq` at most this; once the tenant is closing no write commits
    /// another, so it is the tenant's last row while it stays closed.
    pub last_row_seq: i64,
}

impl TenantStateMove {
    /// Whether the tenant is now in `to`.
    #[must_use]
    pub fn reached(&self, to: TenantState) -> bool {
        self.state == to
    }
}

impl crate::Store {
    /// Move this store's tenant to `to` (see the [module docs](self) for
    /// the allowed moves), under the tenant writer lock, so the move waits
    /// for a write in flight and every later write sees it.
    ///
    /// When this process claimed the tenant (`Store::claim_tenant`), the
    /// move happens only while the tenant lease still names that claim,
    /// even after the claim's fence fired for a clean stop; a process that
    /// lost the tenant cannot close or reopen it under its new owner. A
    /// store that never claimed moves it whoever holds the lease.
    ///
    /// `SQLite`: always open; returns `Open` and changes nothing.
    ///
    /// # Errors
    /// `Error::NotOwner` when this process's claim was taken over;
    /// `Error::StoreBusy` when the writer lock stays held past the writer
    /// wait budget; `Error::Internal` when a statement fails.
    #[cfg_attr(
        not(feature = "postgres"),
        expect(clippy::unused_async, unused_variables, reason = "SQLite: always open")
    )]
    pub async fn move_tenant_state(&self, to: TenantState) -> Result<TenantStateMove> {
        match &self.backend {
            #[cfg(feature = "postgres")]
            crate::Backend::Postgres(store) => store.move_state(to).await,
            crate::Backend::Sqlite(_) => Ok(sqlite_move()),
        }
    }

    /// The tenant's state, read without the writer lock. `SQLite`: `Open`.
    ///
    /// # Errors
    /// `Error::StoreBusy` when no pool connection comes free;
    /// `Error::Internal` when the read fails.
    #[cfg_attr(
        not(feature = "postgres"),
        expect(clippy::unused_async, reason = "SQLite: always open")
    )]
    pub async fn tenant_state(&self) -> Result<TenantState> {
        match &self.backend {
            #[cfg(feature = "postgres")]
            crate::Backend::Postgres(store) => store.read_state().await,
            crate::Backend::Sqlite(_) => Ok(TenantState::Open),
        }
    }

    /// [`Self::move_tenant_state`] for Postgres tenant `tenant` without
    /// opening (or provisioning) a store: for a tool that stops, retires or
    /// migrates a tenant whichever process holds its lease. In a process
    /// that claimed the tenant it acts as that process's stores do (the
    /// claim must still be current).
    ///
    /// # Errors
    /// As [`Self::move_tenant_state`].
    #[cfg(feature = "postgres")]
    pub async fn postgres_move_tenant_state(
        pool: crate::postgres::PgPool,
        tenant: crate::postgres::TenantId,
        to: TenantState,
    ) -> Result<TenantStateMove> {
        crate::postgres::state_mover(pool, tenant)
            .move_state(to)
            .await
    }
}

fn sqlite_move() -> TenantStateMove {
    TenantStateMove {
        from: TenantState::Open,
        state: TenantState::Open,
        last_row_seq: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::TenantState;

    #[test]
    fn moves_close_through_closing_and_reopen_from_anywhere() {
        use TenantState::{Closed, Closing, Open};
        assert_eq!(Closing.moves_from(), &[Open, Closing]);
        assert_eq!(
            Closed.moves_from(),
            &[Closing, Closed],
            "never open to closed"
        );
        assert_eq!(Open.moves_from(), &[Open, Closing, Closed]);
        for state in [Open, Closing, Closed] {
            assert_eq!(
                TenantState::try_from(state.as_str().to_owned()).unwrap(),
                state
            );
        }
        assert!(TenantState::try_from("shut".to_owned()).is_err());
    }

    #[tokio::test]
    async fn a_sqlite_tenant_is_always_open_and_a_move_changes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::Store::open(&dir.path().join("t.db")).await.unwrap();
        let moved = store.move_tenant_state(TenantState::Closed).await.unwrap();
        assert_eq!(moved.state, TenantState::Open);
        assert_eq!(store.tenant_state().await.unwrap(), TenantState::Open);
        store
            .set_setting("userRules", "\"still open\"")
            .await
            .unwrap();
    }
}
