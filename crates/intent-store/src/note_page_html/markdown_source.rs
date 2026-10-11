//! Write-time HTML escaping receipts for Markdown's literal-tag entry path.
use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};
use std::ops::Range;

pub(super) struct Prepared {
    pub text: String,
    pieces: Vec<(Range<usize>, Range<usize>)>,
    pub escaped: Vec<Range<usize>>,
}

pub(super) fn options() -> Options {
    // The native marked entry has no typography transform. In particular,
    // literal attribute quotes and punctuation must retain their source values.
    let mut options = Options::all();
    options.remove(Options::ENABLE_SMART_PUNCTUATION);
    options
}

fn js_space(ch: char) -> bool {
    matches!(
        ch,
        '\t' | '\n' | '\u{000b}' | '\u{000c}' | '\r' | ' ' | '\u{00a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

fn inline_code_ranges(source: &str) -> Vec<Range<usize>> {
    let mut ranges = Vec::new();
    let mut at = 0;
    while let Some(start) = source[at..].find('`').map(|i| at + i) {
        at = start + 1;
        if source.as_bytes().get(at) == Some(&b'`') {
            continue;
        }
        let Some(end) = source[at..].find('`').map(|i| at + i + 1) else {
            break;
        };
        ranges.push(start..end);
        at = end;
    }
    ranges
}

// The production tag regex takes the first `>` even inside an attribute. This
// scanner preserves that exact syntax without adding a runtime regex compiler
// to the note writer's stack. Every unsuccessful candidate advances the scan.
fn tags(source: &str) -> impl Iterator<Item = (Range<usize>, usize, usize)> + '_ {
    let mut at = 0;
    std::iter::from_fn(move || loop {
        let start = source[at..].find('<').map(|i| at + i)?;
        at = start + 1;
        let slash = usize::from(source.as_bytes().get(at) == Some(&b'/'));
        let mut name = at + slash;
        for ch in source[name..].chars() {
            if !js_space(ch) {
                break;
            }
            name += ch.len_utf8();
        }
        if !source
            .as_bytes()
            .get(name)
            .is_some_and(u8::is_ascii_alphabetic)
        {
            continue;
        }
        let end = source[name..].find('>').map(|i| name + i + 1)?;
        at = end;
        return Some((start..end, name, slash));
    })
}

fn allowed_tag(tag: &str) -> bool {
    if ["<sub>", "</sub>", "<sup>", "</sup>"]
        .iter()
        .any(|allowed| tag.eq_ignore_ascii_case(allowed))
    {
        return true;
    }
    if tag
        .get(..3)
        .is_some_and(|start| start.eq_ignore_ascii_case("<br"))
    {
        let rest = tag[3..tag.len() - 1].trim_start_matches(js_space);
        return rest.is_empty() || rest == "/";
    }
    false
}

impl Prepared {
    pub fn new(source: &str) -> Self {
        // Code and math reach their tokenizers unchanged. The extra inline-code
        // shield matters inside lexical HTML blocks, whose contents the initial
        // Markdown block scan does not tokenize into inline events.
        let mut protected = Vec::new();
        let mut fence = None;
        for (event, raw) in Parser::new_ext(source, options()).into_offset_iter() {
            match event {
                Event::Start(Tag::CodeBlock(_)) => fence = Some(raw.start),
                Event::End(TagEnd::CodeBlock) => {
                    if let Some(start) = fence.take() {
                        protected.push(start..raw.end);
                    }
                }
                Event::Code(_) | Event::InlineMath(_) | Event::DisplayMath(_) => {
                    protected.push(raw);
                }
                _ => {}
            }
        }
        protected.extend(inline_code_ranges(source));
        protected.sort_by_key(|r| (r.start, r.end));
        let mut merged: Vec<Range<usize>> = Vec::new();
        for range in protected {
            if let Some(last) = merged.last_mut().filter(|last| last.end >= range.start) {
                last.end = last.end.max(range.end);
            } else {
                merged.push(range);
            }
        }
        let mut result = Self {
            text: String::new(),
            pieces: Vec::new(),
            escaped: Vec::new(),
        };
        let mut copied = 0;
        for (tag, name, slash) in tags(source) {
            let next = merged.partition_point(|r| r.end <= tag.start);
            if merged.get(next).is_some_and(|r| r.start < tag.end)
                || allowed_tag(&source[tag.clone()])
            {
                continue;
            }
            result.push(&source[copied..tag.start], copied..tag.start);
            result.push("&lt;", tag.start..tag.start + 1);
            // Match the production tag reconstruction, including removal of
            // whitespace before the tag name and preservation of its attributes.
            result.push(
                &source[tag.start + 1..tag.start + 1 + slash],
                tag.start + 1..tag.start + 1 + slash,
            );
            result.push(&source[name..tag.end - 1], name..tag.end - 1);
            result.push("&gt;", tag.end - 1..tag.end);
            copied = tag.end;
            result.escaped.push(tag);
        }
        result.push(&source[copied..], copied..source.len());
        result
    }

    fn push(&mut self, value: &str, raw: Range<usize>) {
        if value.is_empty() {
            return;
        }
        let start = self.text.len();
        self.text.push_str(value);
        self.pieces.push((start..self.text.len(), raw));
    }

    pub fn original(&self, range: &Range<usize>) -> Range<usize> {
        let first = self
            .pieces
            .partition_point(|(view, _)| view.end <= range.start);
        let mut raw: Option<Range<usize>> = None;
        for (view, source) in &self.pieces[first..] {
            if view.start >= range.end {
                break;
            }
            let piece = if view.len() == source.len() {
                source.start + range.start.max(view.start) - view.start
                    ..source.start + range.end.min(view.end) - view.start
            } else {
                source.clone()
            };
            if let Some(raw) = &mut raw {
                raw.end = piece.end;
            } else {
                raw = Some(piece);
            }
        }
        raw.unwrap_or_else(|| {
            let at = self.pieces.get(first).map_or_else(
                || self.pieces.last().map_or(0, |(_, r)| r.end),
                |(_, r)| r.start,
            );
            at..at
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn escaped_tags_keep_original_scalar_receipts_and_code_source() {
        let source = "## Before\r\n\r\n<div>**é😀** `a <b>` &amp; \\* </div>";
        let prepared = Prepared::new(source);
        assert_eq!(
            prepared.text,
            "## Before\r\n\r\n&lt;div&gt;**é😀** `a <b>` &amp; \\* &lt;/div&gt;"
        );
        for (event, range) in Parser::new_ext(&prepared.text, Options::all()).into_offset_iter() {
            if let Event::Code(text) = event {
                assert_eq!(text.as_ref(), "a <b>");
                assert_eq!(&source[prepared.original(&range)], "`a <b>`");
            }
        }
        let at = prepared.text.find("é😀").unwrap();
        assert_eq!(&source[prepared.original(&(at..at + "é😀".len()))], "é😀");
    }

    #[test]
    fn tag_scanner_matches_production_tag_expression() {
        let expression =
            regex::Regex::new(r"<(/?)\s*([a-zA-Z][a-zA-Z0-9_-]*)(\s*)([^>]*?)(/?)>").unwrap();
        for source in [
            "<div>**bold**</div>",
            "< div a=\"x>y\">",
            "</ \tDIV x/>",
            "<3> <_x> <<x> <x-y_1/>",
            "<br /> <BR/> </br> <sub> <sup x='1'>",
            "<div a='é😀'>中</div>",
            "<div\r\nx=\"a\">\n</div>",
        ] {
            let expected = expression
                .replace_all(source, "&lt;$1$2$3$4$5&gt;")
                .into_owned();
            let mut actual = String::new();
            let mut copied = 0;
            for (range, name, slash) in tags(source) {
                actual.push_str(&source[copied..range.start]);
                actual.push_str("&lt;");
                actual.push_str(&source[range.start + 1..range.start + 1 + slash]);
                actual.push_str(&source[name..range.end - 1]);
                actual.push_str("&gt;");
                copied = range.end;
            }
            actual.push_str(&source[copied..]);
            assert_eq!(actual, expected, "{source}");
        }
    }
}
