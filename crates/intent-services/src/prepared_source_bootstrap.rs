//! Unregistered bootstrap ownership only. No source grant or lifecycle methods.
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

use crate::Services;

/// Trusted acceptor class, never selected by a renderer request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Read,
    Cleanup,
}

impl Mode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Cleanup => "cleanup",
        }
    }
    const fn range(self) -> std::ops::Range<usize> {
        match self {
            Self::Read => 0..240,
            Self::Cleanup => 240..256,
        }
    }
}

/// Fixed context metadata, separate from frames/TLS/Store allocations.
struct Cell {
    bytes: Box<[u8; 4096]>,
    guest: bool,
}

/// Process-local context root. Construction/replacement is owned by Services.
pub struct Contexts {
    incarnation: String,
    slots: Mutex<Vec<Option<Cell>>>,
    changed: [Notify; 2],
}

/// Observation of retained contexts, not physical allocation or remote delivery.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Counts {
    pub read: usize,
    pub cleanup: usize,
    pub ready: usize,
    pub uncertain: usize,
}

impl Services {
    /// Obtain the isolated root shared with this Services incarnation. Does not listen.
    #[must_use]
    pub fn prepared_source_contexts(&self) -> Arc<Contexts> {
        self.prepared_source_contexts
            .get_or_init(|| {
                Arc::new(Contexts {
                    incarnation: self.daemon_boot_id.clone(),
                    slots: Mutex::new((0..256).map(|_| None).collect()),
                    changed: [Notify::new(), Notify::new()],
                })
            })
            .clone()
    }
}

impl Contexts {
    #[must_use]
    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }

    /// Nonblocking admission before accept/spawn; a pending accept owns a cell too.
    ///
    /// # Panics
    /// Panics if the shared registry lock is poisoned.
    #[must_use]
    pub fn try_admit(self: &Arc<Self>, mode: Mode) -> Option<Context> {
        let mut slots = self.slots.lock().expect("source context registry poisoned");
        let index = mode.range().find(|&i| slots[i].is_none())?;
        let mut bytes = Box::new([0; 4096]);
        bytes[0] = 1;
        bytes[1] = if mode == Mode::Read { 1 } else { 2 };
        let root = self.incarnation.as_bytes();
        bytes[12..16].copy_from_slice(&u32::try_from(root.len()).ok()?.to_be_bytes());
        bytes[16..16 + root.len()].copy_from_slice(root);
        slots[index] = Some(Cell {
            bytes,
            guest: false,
        });
        Some(Context {
            root: self.clone(),
            index,
            mode,
            retired: false,
        })
    }

    /// Used only by the two admitted accept loops; no per-connection waiter spawn.
    pub async fn changed(&self, mode: Mode) {
        self.changed[usize::from(mode == Mode::Cleanup)]
            .notified()
            .await;
    }

    /// Inspect retained context counts.
    ///
    /// # Panics
    /// Panics if the shared registry lock is poisoned.
    #[must_use]
    pub fn counts(&self) -> Counts {
        let slots = self.slots.lock().expect("source context registry poisoned");
        let mut n = Counts::default();
        for (i, cell) in slots.iter().enumerate() {
            if let Some(cell) = cell {
                if i < 240 {
                    n.read += 1;
                } else {
                    n.cleanup += 1;
                }
                n.ready += usize::from(cell.bytes[2] == 5);
                n.uncertain += usize::from(cell.bytes[2] == 7);
            }
        }
        n
    }
}

/// Non-clonable owner. Drop retains uncertainty; only observed retirement releases.
pub struct Context {
    root: Arc<Contexts>,
    index: usize,
    mode: Mode,
    retired: bool,
}

impl Context {
    #[must_use]
    pub const fn mode(&self) -> Mode {
        self.mode
    }
    #[must_use]
    pub fn incarnation(&self) -> &str {
        self.root.incarnation()
    }

    /// Bind the exact authenticated principal, with room for any allowed workspace.
    /// Returns false for source-composition capacity, not invalid authentication.
    ///
    /// # Panics
    /// Panics if the registry lock is poisoned or its private owner/length invariant is broken.
    pub fn bind(
        &mut self,
        principal: &str,
        epoch: [u8; 16],
        guest_limits: Option<(usize, usize)>,
    ) -> bool {
        let root = self.root.incarnation.as_bytes();
        if 112_usize
            .checked_add(principal.len())
            .and_then(|v| v.checked_add(root.len()))
            .and_then(|v| v.checked_add(256))
            .is_none_or(|v| v > 4096)
        {
            return false;
        }
        let mut slots = self
            .root
            .slots
            .lock()
            .expect("source context registry poisoned");
        if let Some((total_limit, per_limit)) = guest_limits.filter(|_| self.mode == Mode::Read) {
            let mut total = 0;
            let mut same = 0;
            for cell in slots.iter().flatten().filter(|c| c.guest) {
                total += 1;
                let n = u32::from_be_bytes(cell.bytes[8..12].try_into().expect("length")) as usize;
                same += usize::from(&cell.bytes[12..12 + n] == principal.as_bytes());
            }
            if (total_limit != 0 && total >= total_limit) || (per_limit != 0 && same >= per_limit) {
                return false;
            }
        }
        slots[self.index].as_mut().expect("owned context").guest =
            guest_limits.is_some() && self.mode == Mode::Read;
        let bytes = &mut slots[self.index].as_mut().expect("owned context").bytes;
        if bytes[4..8] != [0; 4] {
            return false;
        }
        bytes.fill(0);
        bytes[0] = 1;
        bytes[1] = if self.mode == Mode::Read { 1 } else { 2 };
        bytes[2] = 3;
        bytes[4..8].copy_from_slice(&9_u32.to_be_bytes());
        let mut p = 8;
        for text in [principal.as_bytes(), root, b""] {
            let n = u32::try_from(text.len()).expect("bounded metadata length");
            bytes[p..p + 4].copy_from_slice(&n.to_be_bytes());
            p += 4;
            bytes[p..p + text.len()].copy_from_slice(text);
            p += text.len();
        }
        bytes[p + 32..p + 48].copy_from_slice(&epoch);
        bytes[p + 88..p + 92].copy_from_slice(&1_u32.to_be_bytes());
        true
    }

    /// Record a trusted transport phase without restoring eligibility after revocation.
    ///
    /// # Panics
    /// Panics for an invalid phase tag, poisoned registry, or missing owned cell.
    pub fn phase(&mut self, phase: u8) {
        assert!(phase <= 7);
        let mut slots = self
            .root
            .slots
            .lock()
            .expect("source context registry poisoned");
        let cell = slots[self.index].as_mut().expect("owned context");
        // Revocation is irreversible, including late hello/auth completion.
        if cell.bytes[2] < 6 || phase >= 6 {
            cell.bytes[2] = phase;
        }
    }

    /// The transport calls this only after all owned work and local handles retire.
    /// This is not a public cleanup receipt or proof of peer/kernel drain.
    ///
    /// # Panics
    /// Panics if the shared registry lock is poisoned.
    pub fn retire(mut self) {
        self.root
            .slots
            .lock()
            .expect("source context registry poisoned")[self.index] = None;
        self.retired = true;
        self.root.changed[usize::from(self.mode == Mode::Cleanup)].notify_one();
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        if !self.retired {
            if let Ok(mut slots) = self.root.slots.lock() {
                if let Some(cell) = slots[self.index].as_mut() {
                    cell.bytes[2] = 7;
                }
            }
        }
    }
}
