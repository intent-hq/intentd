//! Journal ordering on head: which incoming records to ingest.
//!
//! Head ingests each sequence number exactly once and in order. Records at
//! or below the ingested sequence are replays of what head already has and
//! are skipped. While the node replays (between bind and `ReplayComplete`)
//! it sends every record it still holds after head's ack, so a gap there
//! means the missing records are gone from the node: head notes the loss
//! and continues from the record it got. A gap in the live stream means the
//! link dropped records; head stops ingesting and rebinds, so the node
//! replays from head's ack.

use intent_core::node_link::JournalOrder;
use intent_core::nodes::NodeCounter;

/// What to do with one record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Step {
    /// The next record: ingest it.
    Ingest,
    /// Ingest it; the records in `lost` (inclusive) will never arrive.
    IngestAfterLoss { lost: (NodeCounter, NodeCounter) },
    /// Already ingested: skip it.
    Skip,
    /// Records are missing from the live stream: ingest nothing more on
    /// this link and rebind.
    Resync,
}

#[derive(Debug)]
pub(crate) struct Ingest {
    ingested: NodeCounter,
    replaying: bool,
}

impl Ingest {
    /// Start from `ingested`, the highest contiguous sequence head has.
    pub(crate) fn new(ingested: NodeCounter) -> Self {
        Self {
            ingested,
            replaying: false,
        }
    }

    /// The highest contiguous sequence ingested.
    pub(crate) fn ingested(&self) -> NodeCounter {
        self.ingested
    }

    /// A new link was bound: the node replays first.
    pub(crate) fn start_link(&mut self) {
        self.replaying = true;
    }

    /// The node finished replaying.
    pub(crate) fn replay_complete(&mut self) {
        self.replaying = false;
    }

    /// Classify `seq`, advancing the ingested sequence when it is ingested.
    pub(crate) fn accept(&mut self, seq: NodeCounter) -> Step {
        match JournalOrder::classify(self.ingested, seq) {
            JournalOrder::Next => {
                self.ingested = seq;
                Step::Ingest
            }
            JournalOrder::Duplicate => Step::Skip,
            JournalOrder::Gap if self.replaying => {
                let lost = (NodeCounter(self.ingested.0 + 1), NodeCounter(seq.0 - 1));
                self.ingested = seq;
                Step::IngestAfterLoss { lost }
            }
            JournalOrder::Gap => Step::Resync,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(n: u64) -> NodeCounter {
        NodeCounter(n)
    }

    #[test]
    fn records_are_ingested_once_in_order() {
        let mut ingest = Ingest::new(seq(0));
        ingest.start_link();
        assert_eq!(ingest.accept(seq(1)), Step::Ingest);
        assert_eq!(ingest.accept(seq(2)), Step::Ingest);
        assert_eq!(ingest.accept(seq(1)), Step::Skip);
        assert_eq!(ingest.accept(seq(2)), Step::Skip);
        assert_eq!(ingest.accept(seq(3)), Step::Ingest);
        assert_eq!(ingest.ingested(), seq(3));
    }

    #[test]
    fn a_live_gap_resyncs_without_advancing() {
        let mut ingest = Ingest::new(seq(4));
        ingest.start_link();
        ingest.replay_complete();
        assert_eq!(ingest.accept(seq(5)), Step::Ingest);
        assert_eq!(ingest.accept(seq(7)), Step::Resync);
        assert_eq!(ingest.ingested(), seq(5));
        // After the rebind the node replays the missing record.
        ingest.start_link();
        assert_eq!(ingest.accept(seq(6)), Step::Ingest);
        assert_eq!(ingest.accept(seq(7)), Step::Ingest);
    }

    #[test]
    fn a_gap_while_replaying_means_the_records_are_gone() {
        let mut ingest = Ingest::new(seq(2));
        ingest.start_link();
        assert_eq!(ingest.accept(seq(2)), Step::Skip);
        assert_eq!(
            ingest.accept(seq(6)),
            Step::IngestAfterLoss {
                lost: (seq(3), seq(5))
            }
        );
        assert_eq!(ingest.ingested(), seq(6));
        assert_eq!(ingest.accept(seq(7)), Step::Ingest);
    }
}
