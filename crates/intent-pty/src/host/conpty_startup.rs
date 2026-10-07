//! Answer portable-pty's `ConPTY` cursor-inheritance handshake without a UI.
//! Only the first query belongs to startup; later queries belong to the terminal
//! application and remain in its output for an attached renderer to answer.

use std::io;
#[cfg(test)]
use std::io::Write;

const QUERY: &[u8] = b"\x1b[6n";

#[derive(Default)]
pub(super) struct CursorHandshake {
    matched: usize,
    answered: bool,
}

impl CursorHandshake {
    #[cfg(test)]
    pub(super) fn filter(&mut self, input: &[u8], writer: &mut impl Write) -> io::Result<Vec<u8>> {
        self.filter_with_reply(input, |reply| {
            writer.write_all(reply)?;
            writer.flush()
        })
    }

    pub(super) fn filter_with_reply(
        &mut self,
        input: &[u8],
        mut reply: impl FnMut(&[u8]) -> io::Result<()>,
    ) -> io::Result<Vec<u8>> {
        let mut output = Vec::with_capacity(input.len());
        for &byte in input {
            if self.answered {
                output.push(byte);
                continue;
            }
            if byte == QUERY[self.matched] {
                self.matched += 1;
                if self.matched == QUERY.len() {
                    // There is no parent terminal cursor to inherit: each PTY
                    // starts at its own origin. Consume the query so a later UI
                    // attach cannot send a second reply into application stdin.
                    reply(b"\x1b[1;1R")?;
                    self.matched = 0;
                    self.answered = true;
                }
            } else {
                output.extend_from_slice(&QUERY[..self.matched]);
                self.matched = 0;
                if byte == QUERY[0] {
                    self.matched = 1;
                } else {
                    output.push(byte);
                }
            }
        }
        Ok(output)
    }

    pub(super) fn finish(&mut self) -> Vec<u8> {
        let tail = QUERY[..self.matched].to_vec();
        self.matched = 0;
        tail
    }
}

#[cfg(test)]
mod tests {
    use super::CursorHandshake;
    use std::io::{self, Write};

    #[test]
    fn answers_startup_query_at_every_chunk_boundary_without_losing_output() {
        let stream = b"before\x1b[6nafter";
        for split in 0..=stream.len() {
            let mut handshake = CursorHandshake::default();
            let mut replies = Vec::new();
            let mut output = handshake.filter(&stream[..split], &mut replies).unwrap();
            output.extend(handshake.filter(&stream[split..], &mut replies).unwrap());
            output.extend(handshake.finish());
            assert_eq!(output, b"beforeafter", "split {split}");
            assert_eq!(replies, b"\x1b[1;1R", "split {split}");
        }
    }

    #[test]
    fn handles_bytewise_query_and_leaves_later_application_queries_alone() {
        let mut handshake = CursorHandshake::default();
        let mut replies = Vec::new();
        let mut output = Vec::new();
        for byte in b"\x1b[6nhello\x1b[6n" {
            output.extend(handshake.filter(&[*byte], &mut replies).unwrap());
        }
        output.extend(handshake.finish());
        assert_eq!(output, b"hello\x1b[6n");
        assert_eq!(replies, b"\x1b[1;1R");
    }

    #[test]
    fn preserves_false_and_incomplete_query_prefixes() {
        for stream in [
            b"\x1b".as_slice(),
            b"\x1b[",
            b"\x1b[6",
            b"\x1b[60n",
            b"\x1b\x1b[7n",
        ] {
            let mut handshake = CursorHandshake::default();
            let mut replies = Vec::new();
            let mut output = Vec::new();
            for byte in stream {
                output.extend(handshake.filter(&[*byte], &mut replies).unwrap());
            }
            output.extend(handshake.finish());
            assert_eq!(output, stream);
            assert!(replies.is_empty());
        }
    }

    #[test]
    fn surfaces_failed_cursor_reply() {
        struct BrokenWriter;
        impl Write for BrokenWriter {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let error = CursorHandshake::default()
            .filter(b"\x1b[6n", &mut BrokenWriter)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }
}
