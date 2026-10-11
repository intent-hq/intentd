//! Match the renderer's default `skipIfHTML` decision before Markdown parsing.
//! This identifies an entry path, not native-node or editable-source authority.

fn javascript_trim_character(ch: char) -> bool {
    matches!(
        ch,
        '\u{0009}'..='\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200a}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202f}'
            | '\u{205f}'
            | '\u{3000}'
            | '\u{feff}'
    )
}

pub(crate) fn uses_html_entry(source: &str) -> bool {
    // Only leading trim affects startsWith; unlike Rust's trim, JavaScript
    // includes BOM and excludes NEXT LINE (U+0085).
    let trimmed = source.trim_start_matches(javascript_trim_character);
    trimmed.starts_with('<')
        && !trimmed.starts_with("<!--anchor:")
        && !source.contains("```ws-block")
}

#[cfg(test)]
mod tests {
    use super::uses_html_entry;

    #[test]
    fn note_entry_path_matches_renderer_whitespace_and_exceptions() {
        for source in [
            "<p>html</p>",
            " \r\n<div>html</div>\n\nabc",
            "\u{feff}<p>html</p>",
            "<div>literal ```WS-BLOCK stays HTML</div>",
            "<!--Anchor:id-->abc",
        ] {
            assert!(uses_html_entry(source), "{source:?}");
        }
        for source in [
            "abc",
            "abc\n\ndef",
            "\u{0085}<p>markdown entry</p>",
            "<!--anchor:id-->abc",
            "<div>html</div>\n```ws-block\n{}\n```",
            "<div>literal ```ws-block without a fence</div>",
        ] {
            assert!(!uses_html_entry(source), "{source:?}");
        }
        for whitespace in [
            '\u{0009}', '\u{000a}', '\u{000b}', '\u{000c}', '\u{000d}', '\u{0020}', '\u{00a0}',
            '\u{1680}', '\u{2000}', '\u{2001}', '\u{2002}', '\u{2003}', '\u{2004}', '\u{2005}',
            '\u{2006}', '\u{2007}', '\u{2008}', '\u{2009}', '\u{200a}', '\u{2028}', '\u{2029}',
            '\u{202f}', '\u{205f}', '\u{3000}', '\u{feff}',
        ] {
            assert!(uses_html_entry(&format!("{whitespace}<p>html</p>")));
        }
        for non_whitespace in ['\u{0085}', '\u{180e}', '\u{200b}'] {
            assert!(!uses_html_entry(&format!("{non_whitespace}<p>html</p>")));
        }
    }
}
