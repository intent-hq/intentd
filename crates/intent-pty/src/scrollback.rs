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
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
use std::cell::Cell;

/// Default scrollback budget per PTY (512 KiB), matching the terminal byte
/// buffer cap in `MainProcessTerminalManager.ts`.
pub(crate) const DEFAULT_SCROLLBACK_BYTES: usize = 512 * 1024;

/// A contiguous snapshot of an oldest-indexed line window in the retained
/// scrollback. `bytes` contains `start_line..end_line`, preserving the raw
/// newline separators between those lines. When the window starts inside an
/// OSC escape sequence, it also includes that sequence's opener and hidden
/// prefix so callers can strip it without leaking control payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LineSnapshot {
    /// Raw bytes for the selected lines, in chronological order.
    pub bytes: Vec<u8>,
    /// Number of raw lines in the retained scrollback at snapshot time.
    pub total_lines: usize,
    /// Oldest-indexed inclusive line boundary copied into `bytes`.
    pub start_line: usize,
    /// Oldest-indexed exclusive line boundary copied into `bytes`.
    pub end_line: usize,
    /// Whether retained scrollback contains any non-ASCII-whitespace byte.
    pub retained_has_non_whitespace: bool,
}

impl LineSnapshot {
    /// Number of logical raw lines copied into this snapshot.
    #[must_use]
    pub fn line_count(&self) -> usize {
        self.end_line.saturating_sub(self.start_line)
    }
}

/// A bounded byte ring buffer holding the most recent PTY output.
#[derive(Debug)]
pub struct Scrollback {
    buf: VecDeque<u8>,
    max_bytes: usize,
    /// Absolute byte positions of retained newline bytes. Absolute positions
    /// avoid rewriting the entire index whenever the byte ring drops a prefix.
    newlines: VecDeque<u64>,
    stream_offset: u64,
    non_whitespace_bytes: usize,
    line_snapshot_calls: AtomicUsize,
    line_snapshot_scanned_bytes: AtomicUsize,
    #[cfg(test)]
    last_snapshot_copied_bytes: Cell<usize>,
}

impl Scrollback {
    /// Create a ring buffer that retains at most `max_bytes` of recent output.
    #[must_use]
    pub fn new(max_bytes: usize) -> Self {
        Self {
            buf: VecDeque::new(),
            max_bytes,
            newlines: VecDeque::new(),
            stream_offset: 0,
            non_whitespace_bytes: 0,
            line_snapshot_calls: AtomicUsize::new(0),
            line_snapshot_scanned_bytes: AtomicUsize::new(0),
            #[cfg(test)]
            last_snapshot_copied_bytes: Cell::new(0),
        }
    }

    /// Append `data`, dropping the oldest bytes so the total never exceeds the
    /// configured budget. If `data` alone is larger than the budget, only its
    /// trailing `max_bytes` are retained.
    ///
    /// # Panics
    ///
    /// Panics if one PTY produces more than `u64::MAX` bytes during its lifetime.
    pub fn push(&mut self, data: &[u8]) {
        let data_start = self.stream_offset;
        self.stream_offset = self
            .stream_offset
            .checked_add(u64::try_from(data.len()).expect("slice length fits in u64"))
            .expect("PTY byte offset overflow");
        if self.max_bytes == 0 {
            return;
        }
        // Only the trailing `max_bytes` of `data` can possibly survive.
        let tail = if data.len() > self.max_bytes {
            &data[data.len() - self.max_bytes..]
        } else {
            data
        };
        let tail_start =
            data_start + u64::try_from(data.len() - tail.len()).expect("slice length fits in u64");
        self.newlines.extend(
            tail.iter()
                .enumerate()
                .filter(|(_, byte)| **byte == b'\n')
                .map(|(index, _)| {
                    tail_start + u64::try_from(index).expect("slice index fits in u64")
                }),
        );
        self.non_whitespace_bytes += tail
            .iter()
            .filter(|byte| !byte.is_ascii_whitespace())
            .count();
        self.buf.extend(tail.iter().copied());
        let overflow = self.buf.len().saturating_sub(self.max_bytes);
        if overflow > 0 {
            self.non_whitespace_bytes -= self
                .buf
                .range(..overflow)
                .filter(|byte| !byte.is_ascii_whitespace())
                .count();
            self.buf.drain(..overflow);
        }
        let retained_start =
            self.stream_offset - u64::try_from(self.buf.len()).expect("buffer length fits in u64");
        while self
            .newlines
            .front()
            .is_some_and(|offset| *offset < retained_start)
        {
            self.newlines.pop_front();
        }
    }

    /// Snapshot the retained history as a contiguous, oldest-first byte vector.
    #[must_use]
    pub fn snapshot(&self) -> Vec<u8> {
        self.copy_range(0, self.buf.len())
    }

    /// Snapshot at most the trailing `max_bytes`, without first copying the
    /// retained prefix that the caller does not need.
    #[must_use]
    pub fn snapshot_tail(&self, max_bytes: usize) -> Vec<u8> {
        self.copy_range(self.buf.len().saturating_sub(max_bytes), self.buf.len())
    }

    /// Snapshot an oldest-indexed line window ending at `end_line` (or the
    /// current newest line when omitted), bounded to at most `max_lines`.
    /// Newline positions are maintained on append, so locating the requested
    /// range does not scan or clone the retained prefix.
    ///
    /// # Panics
    ///
    /// Panics if the retained buffer violates its internal absolute-offset
    /// invariants or its length cannot fit in `u64`.
    #[must_use]
    pub fn snapshot_lines(&self, max_lines: usize, end_line: Option<usize>) -> LineSnapshot {
        self.line_snapshot_calls.fetch_add(1, Ordering::Relaxed);
        let total_lines = if self.buf.is_empty() {
            0
        } else {
            self.newlines.len() + 1
        };
        let end_line = end_line.unwrap_or(total_lines).min(total_lines);
        let start_line = end_line.saturating_sub(max_lines);
        let retained_start =
            self.stream_offset - u64::try_from(self.buf.len()).expect("buffer length fits in u64");
        let byte_start = self.line_start(start_line, retained_start);
        let byte_end = self.line_start(end_line, retained_start);
        let context_start = self.open_osc_start(byte_start).unwrap_or(byte_start);
        LineSnapshot {
            bytes: self.copy_range(context_start, byte_end),
            total_lines,
            start_line,
            end_line,
            retained_has_non_whitespace: self.non_whitespace_bytes > 0,
        }
    }

    fn line_start(&self, line: usize, retained_start: u64) -> usize {
        if line == 0 {
            return 0;
        }
        if line > self.newlines.len() {
            return self.buf.len();
        }
        usize::try_from(self.newlines[line - 1] + 1 - retained_start)
            .expect("retained offset fits in usize")
    }

    /// Return the unmatched OSC opener before a window boundary, if any. OSC
    /// payload may contain newlines, so slicing at a raw line boundary must
    /// retain this prefix for the caller's ANSI stripper. Only bytes since the
    /// last BEL or ST terminator can participate.
    fn open_osc_start(&self, byte_start: usize) -> Option<usize> {
        let mut index = byte_start;
        let mut scanned = 0;
        while index > 0 {
            index -= 1;
            scanned += 1;
            if self.buf[index] == 0x07
                || (index > 0 && self.buf[index - 1] == 0x1b && self.buf[index] == b'\\')
            {
                self.record_snapshot_scan(scanned);
                return None;
            }
            if index + 1 < byte_start && self.buf[index] == 0x1b && self.buf[index + 1] == b']' {
                self.record_snapshot_scan(scanned);
                return Some(index);
            }
        }
        self.record_snapshot_scan(scanned);
        None
    }

    fn record_snapshot_scan(&self, scanned: usize) {
        self.line_snapshot_scanned_bytes
            .fetch_add(scanned, Ordering::Relaxed);
    }

    #[must_use]
    pub(crate) fn line_snapshot_metrics(&self) -> (usize, usize) {
        (
            self.line_snapshot_calls.load(Ordering::Relaxed),
            self.line_snapshot_scanned_bytes.load(Ordering::Relaxed),
        )
    }

    fn copy_range(&self, start: usize, end: usize) -> Vec<u8> {
        #[cfg(test)]
        self.last_snapshot_copied_bytes.set(end - start);
        self.buf.range(start..end).copied().collect()
    }

    /// Number of bytes currently retained.
    #[must_use]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether the buffer holds no bytes.
    #[must_use]
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
        self.newlines.clear();
        self.non_whitespace_bytes = 0;
    }

    #[cfg(test)]
    fn last_snapshot_copied_bytes(&self) -> usize {
        self.last_snapshot_copied_bytes.get()
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

    #[test]
    fn tail_caps_copy_only_the_requested_bytes() {
        let mut sb = Scrollback::new(8);
        sb.push(b"ABCDEFGH");

        assert_eq!(sb.snapshot_tail(0), b"");
        assert_eq!(sb.last_snapshot_copied_bytes(), 0);
        assert_eq!(sb.snapshot_tail(8), b"ABCDEFGH");
        assert_eq!(sb.last_snapshot_copied_bytes(), 8);
        assert_eq!(sb.snapshot_tail(80), b"ABCDEFGH");
        assert_eq!(sb.last_snapshot_copied_bytes(), 8);
        assert_eq!(sb.snapshot_tail(3), b"FGH");
        assert_eq!(sb.last_snapshot_copied_bytes(), 3);
    }

    #[test]
    fn tail_is_binary_exact_across_utf8_and_ansi_boundaries() {
        let mut sb = Scrollback::new(64);
        sb.push("é\u{1b}[31mred\u{1b}[0m".as_bytes());
        let full = sb.snapshot();

        for cap in 0..=full.len() {
            assert_eq!(sb.snapshot_tail(cap), full[full.len() - cap..]);
        }
    }

    #[test]
    fn line_window_survives_ring_wrap_and_reports_total() {
        let mut sb = Scrollback::new(13);
        sb.push(b"drop\none\n");
        sb.push(b"two\nthree");

        assert_eq!(sb.snapshot(), b"one\ntwo\nthree");
        let lines = sb.snapshot_lines(2, None);
        assert_eq!(lines.total_lines, 3);
        assert_eq!((lines.start_line, lines.end_line), (1, 3));
        assert_eq!(lines.bytes, b"two\nthree");
    }

    #[test]
    fn bounded_line_window_never_clones_the_retained_prefix() {
        let mut sb = Scrollback::new(4096);
        for index in 0..200 {
            sb.push(format!("line-{index:03}\n").as_bytes());
        }

        let lines = sb.snapshot_lines(3, Some(150));
        assert_eq!(lines.total_lines, 201);
        assert_eq!(lines.line_count(), 3);
        assert_eq!(lines.bytes, b"line-147\nline-148\nline-149\n");
        assert_eq!(sb.last_snapshot_copied_bytes(), lines.bytes.len());
        assert!(sb.last_snapshot_copied_bytes() < sb.len());
    }

    #[test]
    fn line_window_includes_unterminated_osc_prefix_for_safe_decoding() {
        let mut sb = Scrollback::new(128);
        sb.push(b"visible-before\n\x1b]0;hidden\nhidden-tail\x07visible-after\nlast");

        let lines = sb.snapshot_lines(2, None);
        assert_eq!((lines.start_line, lines.end_line), (2, 4));
        assert_eq!(
            lines.bytes,
            b"\x1b]0;hidden\nhidden-tail\x07visible-after\nlast"
        );
    }

    #[test]
    fn line_window_stops_at_st_terminated_osc() {
        let mut sb = Scrollback::new(128);
        sb.push(b"\x1b]0;t\x1b\\line1\nline2\nline3");

        let lines = sb.snapshot_lines(1, None);
        assert_eq!((lines.start_line, lines.end_line), (2, 3));
        assert_eq!(lines.bytes, b"line3");
    }
}
