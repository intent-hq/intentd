//! A machine's dial gate, link side: what the connector asks before each
//! dial of a machine whose availability the domain manages (a cloud
//! sandbox that may be paused). The domain implements it
//! (`crate::cloud::CloudGate`).

use crate::connect::ConnectFailure;
use crate::connector::Shared;

/// What [`DialGate::before_dial`] saw.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Dialable {
    /// The machine's pause count under its transition lock.
    pub pauses: u64,
    /// The sandbox was resumed (or its intentd started again) just now.
    pub resumed: bool,
}

/// Asked by the connector around each dial of its machine.
pub(crate) trait DialGate: Send + Sync {
    /// How many pauses of the machine began, so a dial that binds after
    /// one knows it reached a frozen machine.
    fn pauses(&self) -> u64;

    /// Whether the machine is paused: no dial until something asks.
    fn parked(&self) -> bool;

    /// Before a dial: make the machine dialable. `asked` when something
    /// asked for this dial.
    fn before_dial<'a>(
        &'a self,
        shared: &'a Shared,
        asked: bool,
    ) -> intent_core::BoxFuture<'a, Result<Dialable, ConnectFailure>>;

    /// The machine answered the dial with `machine-update-required`: it can
    /// never link again.
    fn outdated(&self, shared: &Shared);
}
