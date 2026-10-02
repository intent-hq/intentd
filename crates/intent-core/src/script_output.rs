//! Bounded streaming normalization for script monitor observation windows.
//! The PTY reader keeps only framing state. Watches clone that state at their
//! admission cursor, discard any pre-existing partial line, and capture new lines.

/// A normalized logical line. Oversized lines count but cannot regex-match.
#[derive(Debug, PartialEq, Eq)]
pub struct Line {
    pub text: Option<String>,
}

#[derive(Clone, Copy, Default)]
enum Control {
    #[default]
    Text,
    Escape,
    EscapeIntermediate,
    Csi,
    String {
        osc: bool,
        escape: bool,
    },
}

/// Constant decoder/control state plus at most 4096 retained UTF-8 bytes.
#[derive(Clone, Default)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Independent stream framing flags keep decoder state bounded without buffering control strings"
)]
pub struct LineDecoder {
    control: Control,
    utf8: [u8; 4],
    utf8_len: usize,
    cr: bool,
    partial: bool,
    line_started: bool,
    discard: bool,
    oversized: bool,
    text: String,
    capture: bool,
}

impl LineDecoder {
    /// Start at a known stream boundary, optionally retaining normalized text.
    #[must_use]
    pub fn new(capture: bool) -> Self {
        Self {
            capture,
            ..Self::default()
        }
    }

    /// Begin a fresh window without joining a line that predates registration.
    #[must_use]
    pub fn window(&self) -> Self {
        let mut next = self.clone();
        next.capture = true;
        next.discard = self.line_started;
        next.text.clear();
        next.oversized = false;
        next
    }

    /// Missing bytes cannot be reconstructed from scrollback. Skip to a delimiter.
    pub fn gap(&mut self) {
        *self = Self {
            capture: self.capture,
            discard: true,
            ..Self::default()
        };
    }

    /// Consume one byte; UTF-8 and ANSI parsing remain valid across any chunking.
    pub fn push(&mut self, byte: u8) -> Option<Line> {
        self.line_started = true;
        if self.utf8_len == 0 && byte < 0x80 {
            return self.character(char::from(byte));
        }
        self.utf8[self.utf8_len] = byte;
        self.utf8_len += 1;
        match std::str::from_utf8(&self.utf8[..self.utf8_len]) {
            Ok(text) => {
                let c = text.chars().next()?;
                self.utf8_len = 0;
                self.character(c)
            }
            Err(error) if error.error_len().is_none() && self.utf8_len < 4 => None,
            Err(error) => {
                let consumed = error.error_len().unwrap_or(self.utf8_len);
                let remaining = self.utf8_len - consumed;
                let mut tail = [0; 4];
                tail[..remaining].copy_from_slice(&self.utf8[consumed..self.utf8_len]);
                self.utf8_len = 0;
                let mut line = self.character('\u{fffd}');
                for b in &tail[..remaining] {
                    line = self.push(*b).or(line);
                }
                line
            }
        }
    }

    fn character(&mut self, c: char) -> Option<Line> {
        match self.control {
            Control::Escape => {
                self.control = match c {
                    '[' => Control::Csi,
                    ']' => Control::String {
                        osc: true,
                        escape: false,
                    },
                    'P' | 'X' | '^' | '_' => Control::String {
                        osc: false,
                        escape: false,
                    },
                    '\u{1b}' => Control::Escape,
                    '\u{20}'..='\u{2f}' => Control::EscapeIntermediate,
                    _ => Control::Text,
                };
                return None;
            }
            Control::EscapeIntermediate => {
                if ('\u{30}'..='\u{7e}').contains(&c) {
                    self.control = Control::Text;
                }
                return None;
            }
            Control::Csi => {
                if ('\u{40}'..='\u{7e}').contains(&c) {
                    self.control = Control::Text;
                }
                return None;
            }
            Control::String { osc, escape } => {
                self.control = if (osc && c == '\u{7}') || (escape && c == '\\') {
                    Control::Text
                } else {
                    Control::String {
                        osc,
                        escape: c == '\u{1b}',
                    }
                };
                return None;
            }
            Control::Text => {}
        }
        if c == '\u{1b}' {
            self.control = Control::Escape;
            return None;
        }
        if c == '\n' && self.cr {
            self.cr = false;
            self.line_started = false;
            self.discard = false;
            return None;
        }
        if c == '\n' || c == '\r' {
            self.cr = c == '\r';
            return self.finish();
        }
        if c.is_control() && c != '\t' {
            return None;
        }
        self.cr = false;
        self.partial = true;
        if self.capture && !self.discard && !self.oversized {
            if self.text.len() + c.len_utf8() > 4096 {
                self.text.clear();
                self.oversized = true;
            } else {
                self.text.push(c);
            }
        }
        None
    }

    fn finish(&mut self) -> Option<Line> {
        self.line_started = false;
        self.partial = false;
        let discard = std::mem::take(&mut self.discard);
        let oversized = std::mem::take(&mut self.oversized);
        let text = std::mem::take(&mut self.text);
        (!discard).then_some(Line {
            text: (!oversized).then_some(text),
        })
    }

    /// Actual process-output EOF only; never flush for a timeout or a stream gap.
    pub fn eof(&mut self) -> Option<Line> {
        if self.utf8_len != 0 {
            self.utf8_len = 0;
            self.character('\u{fffd}');
        }
        self.control = Control::Text;
        self.partial.then(|| self.finish()).flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lines(bytes: &[u8]) -> Vec<Option<String>> {
        let mut decoder = LineDecoder::new(true);
        let mut lines: Vec<_> = bytes
            .iter()
            .filter_map(|b| decoder.push(*b))
            .map(|l| l.text)
            .collect();
        lines.extend(decoder.eof().map(|l| l.text));
        lines
    }

    #[test]
    fn utf8_controls_crlf_empty_and_partial() {
        assert_eq!(
            lines(b"\xef\xbb\xbf\xc3\xa9\x1b[31mred\x1b[0m\r\n\n\rprogress\t\x08\xff"),
            vec![
                Some("\u{feff}éred".into()),
                Some(String::new()),
                Some(String::new()),
                Some("progress\t\u{fffd}".into())
            ]
        );
        assert_eq!(
            lines(b"a\x1b]ignored\n\r\x07b\x1bPignored\n\x1b\\c\n"),
            vec![Some("abc".into())]
        );
        assert_eq!(lines(b"\xe2\x82x\n"), vec![Some("\u{fffd}x".into())]);
    }

    #[test]
    fn rearm_and_loss_never_join_fragments() {
        let mut cursor = LineDecoder::new(false);
        for b in b"old partial" {
            cursor.push(*b);
        }
        let mut watch = cursor.window();
        let mut got = Vec::new();
        for b in b" suffix\r\nnew\n" {
            got.extend(watch.push(*b));
        }
        assert_eq!(
            got,
            vec![Line {
                text: Some("new".into())
            }]
        );
        for b in b"first" {
            watch.push(*b);
        }
        watch.gap();
        for b in b"suffix\n" {
            assert!(watch.push(*b).is_none());
        }
        assert_eq!(
            watch.push(b'\n'),
            Some(Line {
                text: Some(String::new())
            })
        );
    }

    #[test]
    fn oversized_lines_and_controls_are_bounded() {
        let mut decoder = LineDecoder::new(true);
        for _ in 0..100_000 {
            decoder.push(b'x');
        }
        assert!(decoder.text.len() <= 4096);
        assert_eq!(decoder.eof(), Some(Line { text: None }));
        for b in b"\x1b]" {
            decoder.push(*b);
        }
        for _ in 0..100_000 {
            decoder.push(b'x');
        }
        assert!(decoder.text.is_empty());
        assert_eq!(decoder.eof(), None);
    }
    #[test]
    fn monitor_window_boundaries_cover_utf8_controls_crlf_and_bom() {
        for prefix in [
            b"old".as_slice(),
            b"\xe2\x82",
            b"\x1b[3",
            b"\x1b]old",
            b"\x1bPold",
        ] {
            let mut source = LineDecoder::new(false);
            for b in prefix {
                source.push(*b);
            }
            let mut watch = source.window();
            let mut got = Vec::new();
            for b in b"m\x1b\\\x07suffix\nnew\n" {
                got.extend(watch.push(*b));
            }
            assert_eq!(
                got,
                vec![Line {
                    text: Some("new".into())
                }],
                "prefix {prefix:?}"
            );
        }
        let mut source = LineDecoder::new(false);
        for b in b"old\r" {
            source.push(*b);
        }
        let mut watch = source.window();
        assert!(watch.push(b'\n').is_none());
        assert_eq!(
            watch.push(b'\n'),
            Some(Line {
                text: Some(String::new())
            })
        );
        assert_eq!(lines(b"a\x1b(Bb\xc2\x9bc\n"), vec![Some("abc".into())]);
        assert_eq!(lines(&[b'x'; 4096]), vec![Some("x".repeat(4096))]);
        assert_eq!(lines(&[b'x'; 4097]), vec![None]);
    }
}
