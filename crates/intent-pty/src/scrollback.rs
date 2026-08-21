//! Bounded server-side scrollback (§12.1), porting the ring-buffer idea from
//! `script-output-buffer.ts`.
//!
//! Unlike the TS line-oriented buffer, this stores the PTY's raw byte stream so
//! control/escape sequences are preserved verbatim (matching the byte buffer in
//! `MainProcessTerminalManager.ts`). It is a fixed-capacity ring: once the byte
//! budget is exceeded the oldest bytes are dropped, so memory is bounded while a
//! newly attached subscriber can still back-fill recent history before tailing
//! live output.

use std::collections::VecDeque;

/// Default scrollback budget per PTY (512 KiB), matching the terminal byte
/// buffer cap in `MainProcessTerminalManager.ts`.
pub(crate) const DEFAULT_SCROLLBACK_BYTES: usize = 512 * 1024;

/// A bounded byte ring buffer holding the most recent PTY output.
#[derive(Debug)]
pub struct Scrollback {
    buf: VecDeque<u8>,
    max_bytes: usize,
}

impl Scrollback {
    /// Create a ring buffer that retains at most `max_bytes` of recent output.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            buf: VecDeque::new(),
            max_bytes,
        }
    }

    /// Append `data`, dropping the oldest bytes so the total never exceeds the
    /// configured budget. If `data` alone is larger than the budget, only its
    /// trailing `max_bytes` are retained.
    pub fn push(&mut self, data: &[u8]) {
        if self.max_bytes == 0 {
            return;
        }
        // Only the trailing `max_bytes` of `data` can possibly survive.
        let tail = if data.len() > self.max_bytes {
            &data[data.len() - self.max_bytes..]
        } else {
            data
        };
        self.buf.extend(tail.iter().copied());
        let overflow = self.buf.len().saturating_sub(self.max_bytes);
        if overflow > 0 {
            self.buf.drain(..overflow);
        }
    }

    /// Snapshot the retained history as a contiguous, oldest-first byte vector.
    pub fn snapshot(&self) -> Vec<u8> {
        self.buf.iter().copied().collect()
    }

    /// Number of bytes currently retained.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether the buffer holds no bytes.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Configured retention budget in bytes.
    #[cfg(test)]
    pub(crate) fn capacity_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Drop all retained history.
    pub fn clear(&mut self) {
        self.buf.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retains_only_recent_bytes_when_over_budget() {
        let mut sb = Scrollback::new(4);
        sb.push(b"AB");
        sb.push(b"CDEF");
        // Total pushed is 6 bytes into a 4-byte ring: oldest two dropped.
        assert_eq!(sb.len(), 4);
        assert_eq!(sb.capacity_bytes(), 4);
        assert_eq!(sb.snapshot(), b"CDEF");
    }

    #[test]
    fn preserves_chronological_order_within_budget() {
        let mut sb = Scrollback::new(64);
        sb.push(b"AAA");
        assert_eq!(sb.snapshot(), b"AAA");
        sb.push(b"BBB");
        // Newer bytes append after older ones: history stays oldest-first.
        assert_eq!(sb.snapshot(), b"AAABBB");
        assert!(!sb.is_empty());
    }

    #[test]
    fn single_push_larger_than_budget_keeps_tail() {
        let mut sb = Scrollback::new(3);
        sb.push(b"ABCDEFG");
        assert_eq!(sb.snapshot(), b"EFG");
    }

    #[test]
    fn zero_budget_retains_nothing() {
        let mut sb = Scrollback::new(0);
        sb.push(b"anything");
        assert!(sb.is_empty());
        assert_eq!(sb.snapshot(), b"");
    }

    #[test]
    fn clear_empties_the_ring() {
        let mut sb = Scrollback::new(16);
        sb.push(b"data");
        assert_eq!(sb.len(), 4);
        sb.clear();
        assert!(sb.is_empty());
    }
}
