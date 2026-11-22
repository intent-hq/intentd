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
//!
//! [`Acks`] batches the acks of one link: each is persisted before it is
//! sent, so the node never deletes a record head could still lose.

use std::time::Duration;

use intent_core::node_link::{JournalOrder, LinkFrame};
use intent_core::nodes::NodeCounter;
use intent_node_link::NodeLink;
use tokio::time::Instant;

use crate::lease::{HeadLease, LinkWrite};

/// What to do with one record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
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
pub struct Ingest {
    ingested: NodeCounter,
    replaying: bool,
}

impl Ingest {
    /// Start from `ingested`, the highest contiguous sequence head has.
    #[must_use]
    pub fn new(ingested: NodeCounter) -> Self {
        Self {
            ingested,
            replaying: false,
        }
    }

    /// The highest contiguous sequence ingested.
    #[must_use]
    pub fn ingested(&self) -> NodeCounter {
        self.ingested
    }

    /// A new link was bound: the node replays first.
    pub fn start_link(&mut self) {
        self.replaying = true;
    }

    /// The node finished replaying.
    pub fn replay_complete(&mut self) {
        self.replaying = false;
    }

    /// Classify `seq`, advancing the ingested sequence when it is ingested.
    pub fn accept(&mut self, seq: NodeCounter) -> Step {
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

/// Ack batching for one link.
#[derive(Default)]
pub struct Acks {
    pending: usize,
    due: Option<Instant>,
    persisted: Option<NodeCounter>,
}

impl Acks {
    /// When the pending batch must be flushed, if a record is pending.
    #[must_use]
    pub fn due(&self) -> Option<Instant> {
        self.due
    }

    /// Count a received record; `true` when the batch of `batch` is full.
    /// The first record of a batch makes it due after `interval`.
    pub fn record(&mut self, interval: Duration, batch: usize) -> bool {
        self.pending += 1;
        if self.due.is_none() {
            self.due = Some(Instant::now() + interval);
        }
        self.pending >= batch
    }

    /// Persist `seq`, then ack it. A failed persist sends no ack, so the
    /// node keeps the records; a fenced one also closes the link, which
    /// another process owns now.
    pub async fn flush(&mut self, lease: &HeadLease, link: &NodeLink, seq: NodeCounter) {
        self.pending = 0;
        self.due = None;
        if self.persisted != Some(seq) {
            match lease
                .persist_ack(seq, Some(link.binding().generation))
                .await
            {
                Ok(_) => self.persisted = Some(seq),
                Err(error) => {
                    tracing::warn!(%error, seq = seq.0, "remote node: could not persist the journal ack; not acking");
                    if matches!(error, LinkWrite::Fenced(_)) {
                        link.close();
                    }
                    return;
                }
            }
        }
        if !link.is_closed() {
            let _ = link.send(&LinkFrame::Ack { seq }).await;
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
