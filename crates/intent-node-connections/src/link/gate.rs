//! A machine's dial gate, link side: what the connector asks before each
//! dial of a machine whose availability the domain manages (a cloud
//! sandbox that may be paused). The domain implements it
//! (`intent_svc_remote::cloud::CloudGate`).

use crate::connect::ConnectFailure;
use crate::connector::{RemoteNode, Shared};
use crate::machine::MachineTarget;

/// What [`DialGate::before_dial`] saw.
#[derive(Debug, Clone, Copy)]
pub struct Dialable {
    /// The machine's pause count under its transition lock.
    pub pauses: u64,
    /// The sandbox was resumed (or its intentd started again) just now.
    pub resumed: bool,
}

/// Asked by the connector around each dial of its machine.
pub trait DialGate: Send + Sync {
    /// How many pauses of the machine began, so a dial that binds after
    /// one knows it reached a frozen machine.
    fn pauses(&self) -> u64;

    /// Whether the machine is paused: no dial until something asks.
    fn parked(&self) -> bool;

    /// Before a dial: make the machine dialable. `asked` when something
    /// asked for this dial.
    fn before_dial<'a>(
        &'a self,
        link: &'a dyn GateLink,
        asked: bool,
    ) -> intent_core::BoxFuture<'a, Result<Dialable, ConnectFailure>>;

    /// The machine answered the dial with `machine-update-required`: it can
    /// never link again.
    fn outdated(&self, link: &dyn GateLink);
}

/// What a dial gate may do to its machine's connector.
pub trait GateLink: Send + Sync {
    /// Where the connector dials.
    fn target(&self) -> MachineTarget;

    /// Dial `target` from now on, keeping the current gate when it has none.
    fn set_target(&self, target: MachineTarget);

    /// Fail every turn waiting for a link with `turn_error`, until the next
    /// bind.
    fn refuse_turns(&self, turn_error: &str);

    /// Drop the queued stops: the machine's agents are gone.
    fn drop_stops(&self);

    /// End every live agent with `error`; how many there were.
    fn end_all_agents(&self, error: &str) -> usize;
}

impl GateLink for Shared {
    fn target(&self) -> MachineTarget {
        Shared::target(self)
    }

    fn set_target(&self, target: MachineTarget) {
        Shared::set_target(self, target);
    }

    fn refuse_turns(&self, turn_error: &str) {
        Shared::refuse_turns(self, turn_error);
    }

    fn drop_stops(&self) {
        Shared::drop_stops(self);
    }

    fn end_all_agents(&self, error: &str) -> usize {
        self.agents.end_all(error)
    }
}

impl GateLink for RemoteNode {
    fn target(&self) -> MachineTarget {
        self.shared_ref().target()
    }

    fn set_target(&self, target: MachineTarget) {
        self.shared_ref().set_target(target);
    }

    fn refuse_turns(&self, turn_error: &str) {
        self.shared_ref().refuse_turns(turn_error);
    }

    fn drop_stops(&self) {
        self.shared_ref().drop_stops();
    }

    fn end_all_agents(&self, error: &str) -> usize {
        self.shared_ref().agents.end_all(error)
    }
}
