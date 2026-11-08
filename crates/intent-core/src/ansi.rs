//! Plain-text views of terminal output for agents and chat messages.
//!
//! Saved scripts run with `FORCE_COLOR=1`, so their output carries ANSI
//! escape sequences and carriage-return redraws. The app's script view replays
//! those bytes in a terminal; agents and markdown messages need plain text.

const ESC: char = '\u{1b}';
const BEL: char = '\u{7}';
/// Longest unterminated string sequence (OSC, DCS, …) scanned for a terminator.
const MAX_STRING_SEQUENCE_CHARS: usize = 8192;

/// Remove ANSI escape sequences and BEL characters.
///
/// Drops CSI sequences (colors, cursor moves, private modes), string sequences
/// (OSC titles and hyperlinks, DCS, SOS, PM, APC) up to their BEL or ST
/// terminator, and other two- and three-character escapes. An unterminated
/// string sequence ends at the next newline so later lines survive; a CSI cut
/// short by a newline or another escape keeps that character.
#[must_use]
pub fn strip_ansi(input: &str) -> String {
    if !input.contains(ESC) && !input.contains(BEL) {
        return input.to_owned();
    }
    let chars: Vec<char> = input.chars().collect();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == BEL {
            i += 1;
            continue;
        }
        if c != ESC {
            out.push(c);
            i += 1;
            continue;
        }
        i = match chars.get(i + 1) {
            None => chars.len(),
            Some(&ESC) => i + 1,
            Some('[') => csi_end(&chars, i + 2),
            Some(']' | 'P' | 'X' | '^' | '_') => string_sequence_end(&chars, i + 2),
            Some(next) if ('\u{20}'..='\u{2f}').contains(next) => (i + 3).min(chars.len()),
            Some(_) => i + 2,
        };
    }
    out
}

/// [`strip_ansi`] plus terminal line semantics: CRLF becomes LF, and a bare
/// carriage return followed by more text replaces the current line, so a
/// progress bar or spinner keeps only its final frame.
#[must_use]
pub fn clean_terminal_text(input: &str) -> String {
    let stripped = strip_ansi(input);
    if !stripped.contains('\r') {
        return stripped;
    }
    let mut out = String::with_capacity(stripped.len());
    for (index, line) in stripped.split('\n').enumerate() {
        if index > 0 {
            out.push('\n');
        }
        let line = line.trim_end_matches('\r');
        out.push_str(line.rsplit('\r').next().unwrap_or_default());
    }
    out
}

fn csi_end(chars: &[char], from: usize) -> usize {
    let mut j = from;
    while j < chars.len() && ('\u{20}'..='\u{3f}').contains(&chars[j]) {
        j += 1;
    }
    match chars.get(j) {
        Some(c) if ('\u{40}'..='\u{7e}').contains(c) => j + 1,
        Some(_) => j,
        None => chars.len(),
    }
}

fn string_sequence_end(chars: &[char], from: usize) -> usize {
    let limit = chars
        .len()
        .min(from.saturating_add(MAX_STRING_SEQUENCE_CHARS));
    let mut first_newline = None;
    for (j, &c) in chars.iter().enumerate().take(limit).skip(from) {
        match c {
            BEL => return j + 1,
            ESC if chars.get(j + 1) == Some(&'\\') => return j + 2,
            '\n' if first_newline.is_none() => first_newline = Some(j),
            _ => {}
        }
    }
    first_newline.unwrap_or(limit)
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
    fn unterminated_string_sequence_scan_is_bounded() {
        let long = format!("a\u{1b}]{}b", "x".repeat(MAX_STRING_SEQUENCE_CHARS + 10));
        assert_eq!(strip_ansi(&long), "axxxxxxxxxxb");
    }

    #[test]
    fn csi_cut_short_keeps_the_interrupting_character() {
        assert_eq!(strip_ansi("a\u{1b}[\nb"), "a\nb");
        assert_eq!(strip_ansi("\u{1b}[\u{1b}[31mred"), "red");
        assert_eq!(strip_ansi("ok\u{1b}[32"), "ok");
    }

    #[test]
    fn removes_short_escapes_bel_and_trailing_esc() {
        assert_eq!(
            strip_ansi("\u{1b}[32mok\u{1b}(B\u{1b}[m done\u{1b}="),
            "ok done"
        );
        assert_eq!(strip_ansi("\u{1b}\u{1b}[31mx\u{7}y"), "xy");
        assert_eq!(strip_ansi("ok\u{1b}"), "ok");
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
