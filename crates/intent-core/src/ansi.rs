//! Plain-text views of terminal output for agents and chat messages.
//!
//! Saved scripts run with `FORCE_COLOR=1`, so their output carries ANSI
//! escape sequences and carriage-return redraws. The app's script view replays
//! those bytes in a terminal; agents and markdown messages need plain text.
//!
//! This is a line-oriented approximation, not a terminal emulator: cursor-up
//! redraws (multi-line progress) leave every frame, and a shorter final frame
//! does not keep the tail of a longer earlier one.

/// Remove ANSI escape sequences and non-printing control characters.
///
/// Drops CSI sequences (colors, cursor moves, private modes), string sequences
/// (OSC titles and hyperlinks, DCS, SOS, PM, APC) up to their BEL or ST
/// terminator, other escapes, C0 controls other than tab, newline and carriage
/// return, DEL and C1 controls. A string sequence cut short by a stray ESC or
/// the end of input ends at its first newline, so later lines survive; any
/// sequence cut short keeps the character that interrupted it.
#[must_use]
pub fn strip_ansi(input: &str) -> String {
    strip(input, false)
}

/// [`strip_ansi`] plus terminal line semantics: backspace erases the previous
/// character, CRLF becomes LF, and a bare carriage return followed by more
/// text replaces the current line, so a progress bar keeps its final frame.
#[must_use]
pub fn clean_terminal_text(input: &str) -> String {
    let mut text = strip(input, true);
    if text.contains('\u{8}') {
        text = apply_backspaces(&text);
    }
    if !text.contains('\r') {
        return text;
    }
    let mut out = String::with_capacity(text.len());
    for (index, line) in text.split('\n').enumerate() {
        if index > 0 {
            out.push('\n');
        }
        let line = line.trim_end_matches('\r');
        out.push_str(line.rsplit('\r').next().unwrap_or_default());
    }
    out
}

const ESC: u8 = 0x1b;
const BEL: u8 = 0x07;
const BACKSPACE: u8 = 0x08;

/// One pass over the bytes. Every dropped span starts and ends at an ASCII
/// byte or a two-byte C1 character, so kept runs are valid `str` slices.
fn strip(input: &str, keep_backspace: bool) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut kept_from = 0;
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        let skip_to = match byte {
            ESC => escape_end(bytes, i),
            b'\t' | b'\n' | b'\r' => {
                i += 1;
                continue;
            }
            BACKSPACE if keep_backspace => {
                i += 1;
                continue;
            }
            0x00..=0x1f | 0x7f => i + 1,
            0xc2 if matches!(bytes.get(i + 1), Some(0x80..=0x9f)) => i + 2,
            _ => {
                i += 1;
                continue;
            }
        };
        out.push_str(&input[kept_from..i]);
        i = skip_to;
        kept_from = i;
    }
    out.push_str(&input[kept_from..]);
    out
}

/// Index just past the escape sequence that starts at `start` (an ESC byte).
fn escape_end(bytes: &[u8], start: usize) -> usize {
    match bytes.get(start + 1) {
        None => bytes.len(),
        Some(b'[') => csi_end(bytes, start + 2),
        Some(b']' | b'P' | b'X' | b'^' | b'_') => string_sequence_end(bytes, start + 2),
        Some(0x20..=0x2f) => {
            let mut j = start + 2;
            while matches!(bytes.get(j), Some(0x20..=0x2f)) {
                j += 1;
            }
            match bytes.get(j) {
                Some(0x30..=0x7e) => j + 1,
                Some(_) => j,
                None => bytes.len(),
            }
        }
        Some(0x30..=0x7e) => start + 2,
        Some(_) => start + 1,
    }
}

fn csi_end(bytes: &[u8], from: usize) -> usize {
    let mut j = from;
    while matches!(bytes.get(j), Some(0x20..=0x3f)) {
        j += 1;
    }
    match bytes.get(j) {
        Some(0x40..=0x7e) => j + 1,
        Some(_) => j,
        None => bytes.len(),
    }
}

/// A BEL or ST terminator ends the sequence, even across lines. A stray ESC
/// or the end of input means it was never terminated: it then ends at its
/// first newline, or right before the stray ESC. The scan never passes an
/// ESC, so the whole strip stays linear.
fn string_sequence_end(bytes: &[u8], from: usize) -> usize {
    let mut first_newline = None;
    let mut j = from;
    while j < bytes.len() {
        match bytes[j] {
            BEL => return j + 1,
            ESC if bytes.get(j + 1) == Some(&b'\\') => return j + 2,
            ESC => break,
            b'\n' if first_newline.is_none() => first_newline = Some(j),
            _ => {}
        }
        j += 1;
    }
    first_newline.unwrap_or(j)
}

fn apply_backspaces(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if c == '\u{8}' {
            if !out.ends_with('\n') {
                out.pop();
            }
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_text_is_unchanged() {
        assert_eq!(strip_ansi("plain\ttext é✓"), "plain\ttext é✓");
        assert_eq!(clean_terminal_text("a\nb\n"), "a\nb\n");
    }

    #[test]
    fn removes_csi_sequences_and_keeps_unicode() {
        assert_eq!(
            strip_ansi("\u{1b}[1m\u{1b}[92m   Compiling\u{1b}[0m gimli \u{1b}[?25l\u{1b}[2K😀"),
            "   Compiling gimli 😀"
        );
        assert_eq!(strip_ansi("\u{1b}[38;5;208mo\u{1b}[48;2;1;2;3mt"), "ot");
    }

    #[test]
    fn removes_terminated_string_sequences_even_across_lines() {
        assert_eq!(
            strip_ansi("\u{1b}]8;;https://x.dev\u{7}link\u{1b}]8;;\u{1b}\\ end"),
            "link end"
        );
        assert_eq!(
            strip_ansi("before\u{1b}]0;hidden\nhidden-tail\u{1b}\\after"),
            "beforeafter"
        );
    }

    #[test]
    fn unterminated_string_sequence_ends_at_the_line_end() {
        assert_eq!(
            strip_ansi("before\u{1b}]0;title\nline2\nline3"),
            "before\nline2\nline3"
        );
        assert_eq!(
            strip_ansi("abc\u{1b}Pgarbage\nmore output\n"),
            "abc\nmore output\n"
        );
        assert_eq!(strip_ansi("tail\u{1b}]8;;https://x"), "tail");
    }

    #[test]
    fn stray_escape_inside_a_string_sequence_does_not_swallow_output() {
        assert_eq!(
            strip_ansi("\u{1b}]0;a\nreal output\n\u{1b}]0;b\u{7}rest"),
            "\nreal output\nrest"
        );
        assert_eq!(strip_ansi("\u{1b}]0;a\u{1b}[31mred"), "red");
    }

    #[test]
    fn long_terminated_string_sequence_is_removed_whole() {
        let image = format!("\u{1b}Pq{}\u{1b}\\done", "#".repeat(10_000));
        assert_eq!(strip_ansi(&image), "done");
    }

    #[test]
    fn interrupted_sequences_keep_the_interrupting_character() {
        assert_eq!(strip_ansi("a\u{1b}[\nb"), "a\nb");
        assert_eq!(strip_ansi("\u{1b}[\u{1b}[31mred"), "red");
        assert_eq!(strip_ansi("ok\u{1b}[32"), "ok");
        assert_eq!(strip_ansi("a\u{1b}\nb"), "a\nb");
        assert_eq!(strip_ansi("a\u{1b}(\nb"), "a\nb");
        assert_eq!(strip_ansi("a\u{1b}é"), "aé");
    }

    #[test]
    fn removes_short_and_multi_byte_escapes() {
        assert_eq!(
            strip_ansi("\u{1b}[32mok\u{1b}(B\u{1b}[m done\u{1b}="),
            "ok done"
        );
        assert_eq!(strip_ansi("\u{1b}$(Cx\u{1b}7y"), "xy");
        assert_eq!(strip_ansi("\u{1b}\u{1b}[31mx\u{7}y"), "xy");
        assert_eq!(strip_ansi("ok\u{1b}"), "ok");
    }

    #[test]
    fn removes_other_control_characters() {
        assert_eq!(strip_ansi("x\u{e}\u{f}y\u{9b}z\u{7f}!\u{0}"), "xyz!");
        assert_eq!(strip_ansi("50%\u{8}\u{8}\u{8}60%"), "50%60%");
    }

    #[test]
    fn clean_terminal_text_applies_backspaces() {
        assert_eq!(clean_terminal_text("50%\u{8}\u{8}\u{8}60%"), "60%");
        assert_eq!(clean_terminal_text("a\n\u{8}b"), "a\nb");
    }

    #[test]
    fn clean_terminal_text_normalizes_crlf_and_redraws() {
        assert_eq!(clean_terminal_text("a\r\nb\r\n"), "a\nb\n");
        assert_eq!(
            clean_terminal_text(
                "Packages: +4\r\nProgress: 10\rProgress: 212\r\u{1b}[32mdone\u{1b}[39m\r\nnext"
            ),
            "Packages: +4\ndone\nnext"
        );
        assert_eq!(clean_terminal_text("kept\r"), "kept");
        assert_eq!(clean_terminal_text("x\r\r\n\ry"), "x\ny");
    }
}
