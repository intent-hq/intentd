//! The node's durable journal of [`JournalRecord`]s for its current lease.
//!
//! One record per file, installed with fsync + rename + directory fsync
//! before [`Journal::append`] returns, so a record is never streamed to head
//! before it is durable. Head acknowledges the highest contiguous sequence it
//! has ingested; acknowledged records are deleted. After a reconnect the node
//! resends every record after head's acknowledged sequence.
//!
//! Single writer: the node holds an exclusive lock on its storage root, and
//! callers serialize access (the methods block on IO, so call them off async
//! executor threads).

use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use intent_core::node_link::{JournalBody, JournalRecord};
use intent_core::nodes::NodeCounter;
use intent_core::{AgentId, Error, Result, WorkspaceId};

const STATE: &str = "state.json";
const RECORD_SUFFIX: &str = ".record";
const PENDING_SUFFIX: &str = ".pending";

/// What the journal persists besides its records: which lease it belongs to
/// and head's acknowledged sequence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct State {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lease_id: Option<String>,
    acked: NodeCounter,
}

/// One record to append; the journal assigns its sequence.
#[derive(Debug, Clone)]
pub struct JournalEntry {
    pub agent_id: AgentId,
    pub workspace_id: WorkspaceId,
    pub turn_id: Option<String>,
    pub recorded_at: String,
    pub body: JournalBody,
}

/// The durable journal for one node.
#[derive(Debug)]
pub struct Journal {
    dir: PathBuf,
    state: State,
    last: NodeCounter,
}

fn io_error(context: &str, error: &std::io::Error) -> Error {
    Error::Internal(format!("node-journal: {context}: {error}"))
}

fn corrupt(detail: &str) -> Error {
    Error::Internal(format!("node-journal-corrupt: {detail}"))
}

/// Write `bytes` to `dir/name` durably: a private temporary file, fsync,
/// rename over the target, fsync the directory.
fn install(dir: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let pending = dir.join(format!("{name}{PENDING_SUFFIX}"));
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options.open(&pending).map_err(|e| io_error("create", &e))?;
    file.write_all(bytes).map_err(|e| io_error("write", &e))?;
    file.sync_all().map_err(|e| io_error("sync", &e))?;
    fs::rename(&pending, dir.join(name)).map_err(|e| io_error("rename", &e))?;
    sync_dir(dir)
}

fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io_error("sync directory", &e))
}

fn record_name(seq: u64) -> String {
    format!("{seq:020}{RECORD_SUFFIX}")
}

impl Journal {
    /// Open (or create) the journal in `dir`. Leftover temporary files from
    /// an interrupted write are discarded; installed records must run
    /// contiguously from the acknowledged sequence.
    ///
    /// # Errors
    ///
    /// `node-journal-corrupt` for an unreadable state file, a malformed
    /// record name or a gap; `node-journal` on IO failure.
    pub fn open(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir).map_err(|e| io_error("create directory", &e))?;
        let state_path = dir.join(STATE);
        let state = match fs::read(&state_path) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|_| corrupt("state"))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let state = State {
                    lease_id: None,
                    acked: NodeCounter(0),
                };
                install(dir, STATE, &encode(&state)?)?;
                state
            }
            Err(e) => return Err(io_error("read state", &e)),
        };
        let mut seqs = Vec::new();
        for entry in fs::read_dir(dir).map_err(|e| io_error("list", &e))? {
            let entry = entry.map_err(|e| io_error("list", &e))?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(PENDING_SUFFIX) {
                fs::remove_file(entry.path()).map_err(|e| io_error("discard pending", &e))?;
            } else if let Some(seq) = name.strip_suffix(RECORD_SUFFIX) {
                let seq: u64 = seq.parse().map_err(|_| corrupt("record name"))?;
                if name != record_name(seq) || seq == 0 {
                    return Err(corrupt("record name"));
                }
                seqs.push(seq);
            }
        }
        seqs.sort_unstable();
        let mut journal = Self {
            dir: dir.to_owned(),
            state,
            last: NodeCounter(0),
        };
        // Records at or below the acknowledged sequence are left over from a
        // crash between saving the ack and deleting them.
        let mut last = journal.state.acked.0;
        for seq in seqs {
            if seq <= journal.state.acked.0 {
                journal.remove(seq)?;
                continue;
            }
            if Some(seq) != last.checked_add(1) {
                return Err(corrupt("gap"));
            }
            last = seq;
        }
        journal.last = NodeCounter(last);
        Ok(journal)
    }

    /// The highest appended sequence.
    #[must_use]
    pub fn last_seq(&self) -> NodeCounter {
        self.last
    }

    /// Head's highest acknowledged sequence.
    #[must_use]
    pub fn acked_seq(&self) -> NodeCounter {
        self.state.acked
    }

    /// The lease the journal's records belong to; `None` before the first
    /// bind.
    #[must_use]
    pub fn lease_id(&self) -> Option<&str> {
        self.state.lease_id.as_deref()
    }

    /// Adopt `lease_id`. A different lease starts a new sequence: its head
    /// has acknowledged nothing of this lease, so the old records (and their
    /// numbering) are dropped. Returns whether the lease changed.
    ///
    /// # Errors
    ///
    /// `node-journal` on IO failure.
    pub fn use_lease(&mut self, lease_id: &str) -> Result<bool> {
        if self.state.lease_id.as_deref() == Some(lease_id) {
            return Ok(false);
        }
        // Delete newest first, so a crash part-way leaves the old lease with
        // a contiguous prefix of its records, never a gap.
        for seq in (self.state.acked.0.saturating_add(1)..=self.last.0).rev() {
            self.remove(seq)?;
            self.last = NodeCounter(seq - 1);
        }
        sync_dir(&self.dir)?;
        let state = State {
            lease_id: Some(lease_id.to_owned()),
            acked: NodeCounter(0),
        };
        install(&self.dir, STATE, &encode(&state)?)?;
        self.state = state;
        self.last = NodeCounter(0);
        Ok(true)
    }

    /// Durably append one record and return it with its sequence.
    ///
    /// # Errors
    ///
    /// `node-journal` on IO failure or an exhausted sequence.
    pub fn append(&mut self, entry: JournalEntry) -> Result<JournalRecord> {
        let seq = self
            .last
            .0
            .checked_add(1)
            .ok_or_else(|| Error::Internal("node-journal: sequence exhausted".into()))?;
        let record = JournalRecord {
            seq: NodeCounter(seq),
            agent_id: entry.agent_id,
            workspace_id: entry.workspace_id,
            turn_id: entry.turn_id,
            recorded_at: entry.recorded_at,
            body: entry.body,
        };
        install(&self.dir, &record_name(seq), &encode(&record)?)?;
        self.last = record.seq;
        Ok(record)
    }

    /// Record head's acknowledgement of every record up to `seq` and delete
    /// them. An acknowledgement at or below the current one changes nothing.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidParams`] `node-journal-ack-ahead` when `seq` is past
    /// the last appended record; `node-journal` on IO failure.
    pub fn acknowledge(&mut self, seq: NodeCounter) -> Result<()> {
        if seq > self.last {
            return Err(Error::InvalidParams(format!(
                "node-journal-ack-ahead: ack {} past last record {}",
                seq.0, self.last.0
            )));
        }
        if seq <= self.state.acked {
            return Ok(());
        }
        let previous = self.state.acked.0;
        let state = State {
            lease_id: self.state.lease_id.clone(),
            acked: seq,
        };
        install(&self.dir, STATE, &encode(&state)?)?;
        self.state = state;
        for old in previous.saturating_add(1)..=seq.0 {
            self.remove(old)?;
        }
        sync_dir(&self.dir)
    }

    /// Every record after `seq` (and after the acknowledged sequence), in
    /// order.
    ///
    /// # Errors
    ///
    /// `node-journal-corrupt` for an unreadable record; `node-journal` on IO
    /// failure.
    pub fn records_after(&self, seq: NodeCounter) -> Result<Vec<JournalRecord>> {
        let from = seq.max(self.state.acked).0.saturating_add(1);
        (from..=self.last.0)
            .map(|seq| {
                let bytes = fs::read(self.dir.join(record_name(seq)))
                    .map_err(|e| io_error("read record", &e))?;
                let record: JournalRecord =
                    serde_json::from_slice(&bytes).map_err(|_| corrupt("record"))?;
                if record.seq.0 != seq {
                    return Err(corrupt("record sequence"));
                }
                Ok(record)
            })
            .collect()
    }

    fn remove(&self, seq: u64) -> Result<()> {
        match fs::remove_file(self.dir.join(record_name(seq))) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(io_error("delete record", &e)),
        }
    }
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| Error::Internal(format!("node-journal: encode: {e}")))
}

#[cfg(test)]
mod tests;
