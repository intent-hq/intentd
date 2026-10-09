//! Write-time Markdown code ownership and source normalization receipts.
use super::{canonical, TextMapping};
use crate::note_page_index::Entries;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use pulldown_cmark::{CodeBlockKind, Event, Parser, Tag, TagEnd};
use serde_json::{json, Value};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write as _};
use std::ops::Range;

struct Output<'a> {
    html: String,
    current: &'a Cell<usize>,
    ranges: Vec<Option<Range<usize>>>,
}
impl fmt::Write for Output<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let start = self.html.len();
        self.html.push_str(value);
        let range = &mut self.ranges[self.current.get()];
        if let Some(range) = range {
            range.end = self.html.len();
        } else {
            *range = Some(start..self.html.len());
        }
        Ok(())
    }
}

struct Code {
    raw: Range<usize>,
    body: Range<usize>,
    generated: Range<usize>,
    // HTML scalar/escape receipts. Long identity runs use bounded chunks.
    atoms: Vec<(Range<usize>, Range<usize>)>,
    ordinary: bool,
}

#[derive(Clone, Copy)]
enum Container {
    Quote,
    Indent(usize),
}

#[derive(Clone, Default)]
struct Prefix {
    at: usize,
    column: usize,
    remaining: usize,
}
impl Prefix {
    fn spaces(&mut self, bytes: &[u8], mut wanted: usize) -> bool {
        let pending = self.remaining.min(wanted);
        self.remaining -= pending;
        wanted -= pending;
        while wanted > 0 {
            let width = match bytes.get(self.at) {
                Some(b' ') => 1,
                Some(b'\t') => 4 - self.column % 4,
                _ => break,
            };
            self.at += 1;
            self.column += width;
            let consumed = wanted.min(width);
            wanted -= consumed;
            self.remaining = width - consumed;
        }
        wanted == 0
    }
}

fn skip_prefix(bytes: &[u8], containers: &[Container]) -> usize {
    let mut cursor = Prefix::default();
    for container in containers {
        let saved = cursor.clone();
        let matched = match container {
            Container::Quote => {
                cursor.spaces(bytes, 3);
                if bytes.get(cursor.at) == Some(&b'>') {
                    cursor.at += 1;
                    cursor.column += 1;
                    cursor.spaces(bytes, 1);
                    true
                } else {
                    false
                }
            }
            Container::Indent(width) => {
                cursor.spaces(bytes, *width)
                    || matches!(bytes.get(cursor.at), None | Some(b'\n' | b'\r'))
            }
        };
        if !matched {
            cursor = saved;
            break;
        }
    }
    cursor.at
}

fn marker_indent(source: &str, raw: &Range<usize>) -> usize {
    let bytes = source[raw.clone()].as_bytes();
    let mut cursor = Prefix::default();
    cursor.spaces(bytes, 3);
    if bytes.get(cursor.at).is_some_and(u8::is_ascii_digit) {
        while bytes.get(cursor.at).is_some_and(u8::is_ascii_digit) {
            cursor.at += 1;
            cursor.column += 1;
        }
    }
    cursor.at += 1;
    cursor.column += 1;
    let marker = cursor.clone();
    if cursor.spaces(bytes, 5) {
        cursor = marker;
        cursor.spaces(bytes, 1);
    }
    cursor.column - cursor.remaining
}

fn code_atoms<'a>(
    source: &'a str,
    body: Range<usize>,
    containers: &'a [Container],
    table: bool,
) -> impl Iterator<Item = (Range<usize>, char)> + 'a {
    let mut at = body.start;
    std::iter::from_fn(move || {
        if at >= body.end {
            return None;
        }
        let start = at;
        let ch = source[at..].chars().next().expect("code scalar");
        if table && ch == '\\' && source.as_bytes().get(at + 1) == Some(&b'|') {
            at += 2;
            return Some((start..at, '|'));
        }
        at += ch.len_utf8();
        let raw = start..at;
        if matches!(ch, '\r' | '\n') {
            at += skip_prefix(&source.as_bytes()[at..body.end], containers);
            Some((raw, ' '))
        } else {
            Some((raw, ch))
        }
    })
}

fn code_receipts(
    source: &str,
    raw: Range<usize>,
    generated: Range<usize>,
    containers: &[Container],
    table: bool,
) -> Code {
    let width = source[raw.clone()]
        .bytes()
        .take_while(|b| *b == b'`')
        .count();
    let body = raw.start + width..raw.end - width;
    // Two streaming passes avoid a per-scalar allocation for giant bodies.
    let mut first = None;
    let mut last = None;
    let mut nonspace = false;
    for (range, ch) in code_atoms(source, body.clone(), containers, table) {
        first.get_or_insert((range.clone(), ch));
        last = Some((range, ch));
        nonspace |= ch != ' ';
    }
    let trim = first.as_ref().is_some_and(|(_, ch)| *ch == ' ')
        && last.as_ref().is_some_and(|(_, ch)| *ch == ' ')
        && nonspace;
    let kept = if trim {
        first.expect("first").0.end..last.expect("last").0.start
    } else {
        body.clone()
    };
    let mut position = generated.start + "<code>".len();
    let mut atoms: Vec<(Range<usize>, Range<usize>)> = Vec::new();
    for (raw, ch) in code_atoms(source, body.clone(), containers, table)
        .filter(|(range, _)| range.start >= kept.start && range.end <= kept.end)
    {
        let bytes = match ch {
            '&' => 5,
            '<' | '>' => 4,
            _ => ch.len_utf8(),
        };
        let next = position + bytes;
        let identity = bytes == raw.len() && source[raw.clone()].starts_with(ch);
        if identity {
            if let Some((prior_html, prior_raw)) = atoms.last_mut() {
                if prior_html.end == position
                    && prior_raw.end == raw.start
                    && prior_html.len() == prior_raw.len()
                    && prior_html.len() + bytes <= crate::note_page_index::PIECE_BYTES
                {
                    prior_html.end = next;
                    prior_raw.end = raw.end;
                    position = next;
                    continue;
                }
            }
        }
        atoms.push((position..next, raw));
        position = next;
    }
    Code {
        raw,
        body,
        generated,
        atoms,
        ordinary: false,
    }
}

fn text_receipts(
    prepared: &super::markdown_source::Prepared,
    input: Range<usize>,
    text: &str,
    generated: Range<usize>,
) -> Code {
    let raw = prepared.original(&input);
    let mut atoms: Vec<(Range<usize>, Range<usize>)> = Vec::new();
    let mut at = generated.start;
    let mut decoded = super::TextAtoms {
        source: &prepared.text,
        position: input.start,
        end: input.end,
    };
    let mut consumed = 0;
    while let Some(mut atom) = decoded.next() {
        if atom.text == "\\" && !text[consumed..].starts_with('\\') {
            if let Some(next) = decoded.next() {
                atom.raw.end = next.raw.end;
                atom.text = next.text;
            }
        }
        debug_assert!(
            text[consumed..].starts_with(&atom.text),
            "Markdown text receipt: {:?} != {:?}",
            atom.text,
            &text[consumed..]
        );
        consumed += atom.text.len();
        let raw = prepared.original(&atom.raw);
        let bytes: usize = atom
            .text
            .chars()
            .map(|ch| match ch {
                '&' => 5,
                '<' | '>' => 4,
                _ => ch.len_utf8(),
            })
            .sum();
        if let Some((html, prior)) = atoms.last_mut().filter(|(html, prior)| {
            html.end == at
                && prior.end == raw.start
                && html.len() == prior.len()
                && bytes == raw.len()
                && html.len() + bytes <= crate::note_page_index::PIECE_BYTES
        }) {
            html.end += bytes;
            prior.end = raw.end;
        } else {
            atoms.push((at..at + bytes, raw));
        }
        at += bytes;
    }
    debug_assert_eq!(consumed, text.len());
    debug_assert_eq!(at, generated.end);
    Code {
        body: raw.clone(),
        raw,
        generated,
        atoms,
        ordinary: true,
    }
}

fn raw_projection(code: &Code, range: &Range<usize>) -> Option<Range<usize>> {
    let first = code
        .atoms
        .partition_point(|(html, _)| html.end <= range.start);
    let mut result: Option<Range<usize>> = None;
    for (html, raw) in &code.atoms[first..] {
        if html.start >= range.end {
            break;
        }
        let selected = if html.len() == raw.len() {
            raw.start + range.start.max(html.start) - html.start
                ..raw.start + range.end.min(html.end) - html.start
        } else {
            raw.clone()
        };
        if let Some(result) = &mut result {
            result.end = selected.end;
        } else {
            result = Some(selected);
        }
    }
    result
}

// Only parser-confirmed container delimiters belong to the document. Child
// envelopes remain opaque, so text, links, code and atoms never become omitted
// merely because another mapping producer does not support them.
fn document_delimiters(source: &str, events: &[(Event<'_>, Range<usize>)]) -> Vec<Range<usize>> {
    struct Frame {
        full: Range<usize>,
        children: Vec<Range<usize>>,
        container: bool,
        item: bool,
    }
    fn gaps(mut frame: Frame, output: &mut Vec<Range<usize>>) {
        if !frame.container {
            return;
        }
        frame.children.sort_by_key(|range| (range.start, range.end));
        let mut at = frame.full.start;
        for child in frame
            .children
            .into_iter()
            .chain(std::iter::once(frame.full.end..frame.full.end))
        {
            if at < child.start {
                output.push(at..child.start);
            }
            at = at.max(child.end);
        }
    }
    let mut stack = vec![Frame {
        full: 0..source.len(),
        children: Vec::new(),
        container: true,
        item: false,
    }];
    let mut result = Vec::new();
    for (event, range) in events {
        if matches!(event, Event::End(_)) {
            gaps(stack.pop().expect("balanced parser events"), &mut result);
            continue;
        }
        let marker = matches!(event, Event::TaskListMarker(_));
        let parent = if marker {
            stack
                .iter()
                .rposition(|frame| frame.item)
                .expect("checkbox belongs to an item")
        } else {
            stack.len() - 1
        };
        stack[parent].children.push(range.clone());
        if marker {
            // The parser emits an explicit checkbox token with no text payload.
            result.push(range.clone());
        }
        if let Event::Start(tag) = event {
            stack.push(Frame {
                full: range.clone(),
                children: Vec::new(),
                container: matches!(tag, Tag::List(_) | Tag::Item | Tag::BlockQuote(_)),
                item: matches!(tag, Tag::Item),
            });
        }
    }
    gaps(stack.pop().expect("document frame"), &mut result);
    result
}

// Tight lists and table cells omit paragraph tags. Retain only consecutive
// direct inline children of each parser container as evidence for the native
// paragraph the schema inserts. Block children and task markers end a group.
fn implicit_paragraph_groups(
    events: &[(Event<'_>, Range<usize>)],
) -> BTreeMap<usize, Vec<Vec<Range<usize>>>> {
    struct Frame {
        event: usize,
        container: bool,
        pending: Vec<Range<usize>>,
    }
    fn flush(frame: &mut Frame, groups: &mut BTreeMap<usize, Vec<Vec<Range<usize>>>>) {
        if !frame.pending.is_empty() {
            groups
                .entry(frame.event)
                .or_default()
                .push(std::mem::take(&mut frame.pending));
        }
    }
    let mut stack: Vec<Frame> = Vec::new();
    let mut groups = BTreeMap::new();
    for (index, (event, range)) in events.iter().enumerate() {
        if matches!(event, Event::End(_)) {
            let mut frame = stack.pop().expect("balanced parser events");
            flush(&mut frame, &mut groups);
            continue;
        }
        let inline = matches!(
            event,
            Event::Start(
                Tag::Emphasis
                    | Tag::Strong
                    | Tag::Strikethrough
                    | Tag::Link { .. }
                    | Tag::Image { .. }
                    | Tag::Superscript
                    | Tag::Subscript
            ) | Event::Text(_)
                | Event::Code(_)
                | Event::SoftBreak
                | Event::HardBreak
                | Event::InlineHtml(_)
        );
        if let Some(parent) = stack.last_mut().filter(|frame| frame.container) {
            if inline && !range.is_empty() {
                parent.pending.push(range.clone());
            } else {
                flush(parent, &mut groups);
            }
        }
        if let Event::Start(tag) = event {
            stack.push(Frame {
                event: index,
                container: matches!(tag, Tag::Item | Tag::TableCell),
                pending: Vec::new(),
            });
        }
    }
    groups
}

// Match the full editor's existing task-list grouping without changing parser
// source coordinates or the text/code mapping path. Ordinary lists are untouched.
fn task_list_html(
    events: &[(Event<'_>, Range<usize>)],
) -> (BTreeMap<usize, String>, BTreeMap<usize, Range<usize>>) {
    let mut parents: Vec<usize> = Vec::new();
    let mut items = BTreeMap::<usize, Vec<usize>>::new();
    let mut checked = BTreeMap::new();
    let mut ends = BTreeMap::new();
    let mut markers = Vec::new();
    for (index, (event, _)) in events.iter().enumerate() {
        match event {
            Event::Start(tag) => {
                if matches!(tag, Tag::Item) {
                    let list = *parents
                        .iter()
                        .rev()
                        .find(|&&parent| matches!(events[parent].0, Event::Start(Tag::List(_))))
                        .expect("list item parent");
                    items.entry(list).or_default().push(index);
                }
                parents.push(index);
            }
            Event::End(_) => {
                ends.insert(parents.pop().expect("balanced parser events"), index);
            }
            Event::TaskListMarker(value) => {
                let item = *parents
                    .iter()
                    .rev()
                    .find(|&&parent| matches!(events[parent].0, Event::Start(Tag::Item)))
                    .expect("checkbox item");
                checked.insert(item, *value);
                markers.push(index);
            }
            _ => {}
        }
    }
    let mut html = BTreeMap::new();
    let mut list_ranges = BTreeMap::new();
    let opening = |task| {
        if task {
            "<ul data-type=\"taskList\">\n"
        } else {
            "<ul>\n"
        }
    };
    for (list, children) in items {
        if !children.iter().any(|item| checked.contains_key(item)) {
            continue;
        }
        let mut groups: Vec<Vec<usize>> = Vec::new();
        for item in children {
            if groups
                .last()
                .is_none_or(|group| checked.contains_key(&group[0]) != checked.contains_key(&item))
            {
                groups.push(Vec::new());
            }
            groups.last_mut().unwrap().push(item);
        }
        html.insert(
            list,
            opening(checked.contains_key(&groups[0][0])).to_owned(),
        );
        html.insert(ends[&list], "</ul>\n".into());
        for (group_index, group) in groups.iter().enumerate() {
            let first = group[0];
            let last = *group.last().unwrap();
            let event = if group_index == 0 { list } else { first };
            list_ranges.insert(event, events[first].1.start..events[last].1.end);
            for &item in group {
                let mut start = if item == first && group_index > 0 {
                    format!("</ul>\n{}", opening(checked.contains_key(&item)))
                } else {
                    String::new()
                };
                if let Some(value) = checked.get(&item) {
                    write!(
                        start,
                        "<li data-type=\"taskItem\" data-checked=\"{value}\" data-status=\"{}\">",
                        if *value { "done" } else { "todo" }
                    )
                    .expect("String output cannot fail");
                } else {
                    start.push_str("<li>");
                }
                html.insert(item, start);
                html.insert(ends[&item], "</li>\n".into());
            }
        }
    }
    for marker in markers {
        html.insert(marker, String::new());
    }
    (html, list_ranges)
}

pub(crate) fn append_codes(
    source: &str,
    units: &[usize],
    entries: &mut Entries,
    descriptors: &mut Vec<(usize, usize, String, Value)>,
) {
    // The existing entry path interprets raw HTML separately from Markdown.
    if super::uses_html_entry(source) {
        return;
    }
    let prepared = super::markdown_source::Prepared::new(source);
    let input_events: Vec<_> = Parser::new_ext(&prepared.text, super::markdown_source::options())
        .into_offset_iter()
        .collect();
    let events: Vec<_> = input_events
        .iter()
        .map(|(event, range)| (event.clone(), prepared.original(range)))
        .collect();
    let mut document_ranges = document_delimiters(source, &events);
    let primitive_starts: BTreeSet<_> = events.iter().enumerate().filter_map(|(index, (event, _))| {
        matches!(event, Event::Start(Tag::CodeBlock(CodeBlockKind::Fenced(info))) if format!("language-{info}").split_ascii_whitespace().any(|class| matches!(class, "language-diff" | "language-mermaid"))).then_some(index)
    }).collect();
    let (task_html, task_list_ranges) = task_list_html(&events);
    let current = Cell::new(0);
    let mut output = Output {
        html: String::new(),
        current: &current,
        ranges: vec![None; events.len()],
    };
    let mut primitive_body = false;
    pulldown_cmark::html::write_html_fmt(
        &mut output,
        events.iter().enumerate().map(|(index, (event, _))| {
            current.set(index);
            if primitive_body {
                if matches!(event, Event::End(TagEnd::CodeBlock)) { primitive_body = false; }
                return Event::Html("".into());
            }
            if primitive_starts.contains(&index) {
                let Event::Start(Tag::CodeBlock(CodeBlockKind::Fenced(language))) = event else { unreachable!() };
                let mut body = String::new();
                for (part, _) in &events[index + 1..] {
                    match part {
                        Event::End(TagEnd::CodeBlock) => break,
                        Event::Text(text) => body.push_str(text),
                        _ => {}
                    }
                }
                let body = body.strip_suffix('\n').unwrap_or(&body);
                primitive_body = true;
                if matches!(language.as_ref(), "diff" | "mermaid") {
                    let encoded = STANDARD.encode(body.as_bytes());
                    return Event::Html(format!("<div data-type=\"{language}-block\" data-{language}-code=\"{encoded}\"></div>\n").into());
                }
                // The native Markdown renderer uses a pre/code fallback for
                // titled fences. Its schema recognizes the language class and
                // preserves raw code rather than the exact-language base64.
                let escape = |value: &str| value.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;").replace('\'', "&#039;");
                return Event::Html(format!("<pre><code class=\"language-{}\">{}</code></pre>\n", escape(language), escape(body)).into());
            }
            if let Some(html) = task_html.get(&index) {
                return Event::Html(html.clone().into());
            }
            // The Markdown entry path treats source HTML as literal text. Only
            // the explicit generated primitive above creates native HTML atoms.
            match event {
                Event::Html(text) | Event::InlineHtml(text) => Event::Text(text.clone()),
                Event::SoftBreak => Event::HardBreak,
                _ => event.clone(),
            }
        }),
    )
    .expect("String output cannot fail");
    let mut codes = Vec::new();
    let mut stack = Vec::new();
    let mut containers = Vec::new();
    let mut tables = 0;
    let mut code_block = false;
    for (event_index, ((event, raw), generated)) in events.iter().zip(&output.ranges).enumerate() {
        match event {
            Event::Start(tag) => {
                if matches!(tag, Tag::CodeBlock(_)) {
                    code_block = true;
                }
                let container = match tag {
                    Tag::BlockQuote(_) => Some(Container::Quote),
                    Tag::Item | Tag::DefinitionListDefinition => {
                        Some(Container::Indent(marker_indent(source, raw)))
                    }
                    // Options::all includes the legacy footnote grammar, which
                    // does not strip the GFM-only four-space continuation.
                    _ => None,
                };
                stack.push((container.is_some(), matches!(tag, Tag::Table(_))));
                if let Some(container) = container {
                    containers.push(container);
                }
                if matches!(tag, Tag::Table(_)) {
                    tables += 1;
                }
            }
            Event::End(end) => {
                if matches!(end, TagEnd::CodeBlock) {
                    code_block = false;
                }
                if let Some((container, table)) = stack.pop() {
                    if container {
                        containers.pop();
                    }
                    if table {
                        tables -= 1;
                    }
                }
            }
            Event::Code(_) => {
                if let Some(generated) = generated
                    .as_ref()
                    .filter(|range| output.html[(*range).clone()].starts_with("<code>"))
                {
                    let mut code = code_receipts(
                        source,
                        raw.clone(),
                        generated.clone(),
                        &containers,
                        tables > 0,
                    );
                    if !descriptors.iter().any(|(start, end, _, value)| {
                        *start == units[raw.start]
                            && *end == units[raw.end]
                            && value["role"] == "code"
                    }) {
                        code.ordinary = true;
                    }
                    codes.push(code);
                }
            }
            Event::Text(text) if !code_block => {
                if let Some(generated) = generated {
                    codes.push(text_receipts(
                        &prepared,
                        input_events[event_index].1.clone(),
                        text,
                        generated.clone(),
                    ));
                }
            }
            _ => {}
        }
    }
    let tree = canonical(&output.html);
    let mut active = Vec::new();
    let mut pending = vec![0];
    while let Some(id) = pending.pop() {
        active.push(id);
        pending.extend(tree.nodes[id].children.iter().rev().copied());
    }
    let mut maps: Vec<Vec<(Option<usize>, TextMapping)>> =
        (0..codes.len()).map(|_| Vec::new()).collect();
    // Receipt seeks avoid scanning every preceding code span for each text map.
    for leaf in active
        .iter()
        .copied()
        .filter(|id| tree.nodes[*id].kind == "text")
    {
        for map in &tree.nodes[leaf].maps {
            let first = codes.partition_point(|code| code.generated.end <= map.raw.start);
            for (offset_code, code) in codes[first..].iter().enumerate() {
                if code.generated.start >= map.raw.end {
                    break;
                }
                if map.mapping == "identity" {
                    let first_atom = code
                        .atoms
                        .partition_point(|(html, _)| html.end <= map.raw.start);
                    for (html, original) in &code.atoms[first_atom..] {
                        if html.start >= map.raw.end {
                            break;
                        }
                        let segment = html.start.max(map.raw.start)..html.end.min(map.raw.end);
                        let raw = if html.len() == original.len() {
                            original.start + segment.start - html.start
                                ..original.start + segment.end - html.start
                        } else {
                            original.clone()
                        };
                        let text =
                            &map.text[segment.start - map.raw.start..segment.end - map.raw.start];
                        let offset = map.text[..segment.start - map.raw.start]
                            .encode_utf16()
                            .count();
                        maps[first + offset_code].push((
                            Some(leaf),
                            TextMapping {
                                mapping: if source[raw.clone()] == *text {
                                    "identity"
                                } else {
                                    "normalized"
                                },
                                raw,
                                rendered: map.rendered.start + offset
                                    ..map.rendered.start + offset + text.encode_utf16().count(),
                                text: text.to_owned(),
                            },
                        ));
                    }
                } else if let Some(raw) = raw_projection(code, &map.raw) {
                    maps[first + offset_code].push((
                        Some(leaf),
                        TextMapping {
                            mapping: if map.text.is_empty() {
                                "omitted"
                            } else if source[raw.clone()] == map.text {
                                "identity"
                            } else {
                                "normalized"
                            },
                            raw,
                            rendered: map.rendered.clone(),
                            text: map.text.clone(),
                        },
                    ));
                }
            }
        }
    }
    // Native ancestry supplies ownership even when Markdown emits no paragraph
    // tag (tight lists and table cells). Active nodes are visited parent first.
    let mut text_owner = vec![None; tree.nodes.len()];
    for &id in &active {
        text_owner[id] = if matches!(tree.nodes[id].kind, "paragraph" | "heading") {
            Some(id)
        } else {
            tree.nodes[id].parent.and_then(|parent| text_owner[parent])
        };
    }
    let generated_ranges: Vec<_> = output
        .ranges
        .iter()
        .enumerate()
        .filter_map(|(event, range)| range.as_ref().filter(|r| !r.is_empty()).map(|r| (r, event)))
        .collect();
    let original_event_at = |opening: &Range<usize>| {
        let index = generated_ranges.partition_point(|(range, _)| range.end <= opening.start);
        generated_ranges
            .get(index)
            .filter(|(range, _)| range.start <= opening.start)
            .map(|(_, event)| *event)
    };
    let original_at =
        |opening: &Range<usize>| original_event_at(opening).map(|event| events[event].1.clone());
    let mut owner_ranges = BTreeMap::<usize, Range<usize>>::new();
    let mut explicit = BTreeSet::new();
    for &id in &active {
        if text_owner[id] == Some(id) {
            if let Some(raw) = tree.nodes[id]
                .source
                .as_ref()
                .and_then(|source| source.opening.as_ref())
                .and_then(original_at)
            {
                owner_ranges.insert(id, raw);
                explicit.insert(id);
            }
        }
    }
    let mut extend_owner = |owner: usize, raw: &Range<usize>| {
        if !explicit.contains(&owner) {
            owner_ranges
                .entry(owner)
                .and_modify(|range| {
                    range.start = range.start.min(raw.start);
                    range.end = range.end.max(raw.end);
                })
                .or_insert_with(|| raw.clone());
        }
    };
    for (leaf, map) in maps.iter().flatten() {
        if let Some(owner) = leaf.and_then(|id| text_owner[id]) {
            extend_owner(owner, &map.raw);
        }
    }
    let mut atom_ranges = Vec::new();
    for &id in &active {
        if matches!(tree.nodes[id].kind, "hardBreak" | "image") {
            if let Some(raw) = tree.nodes[id]
                .source
                .as_ref()
                .and_then(|source| source.opening.as_ref())
                .and_then(original_at)
            {
                if let Some(owner) = text_owner[id] {
                    extend_owner(owner, &raw);
                    atom_ranges.push((owner, raw));
                }
            }
        }
    }
    let inline_groups = implicit_paragraph_groups(&events);
    let mut repaired_paragraphs = BTreeMap::<usize, Vec<Range<usize>>>::new();
    for (&owner, range) in &mut owner_ranges {
        if explicit.contains(&owner) || tree.nodes[owner].kind != "paragraph" {
            continue;
        }
        let Some(parent) = tree.nodes[owner].parent else {
            continue;
        };
        if !matches!(
            tree.nodes[parent].kind,
            "listItem" | "taskItem" | "tableCell" | "tableHeader"
        ) {
            continue;
        }
        let Some(parent_event) = tree.nodes[parent]
            .source
            .as_ref()
            .and_then(|source| source.opening.as_ref())
            .and_then(original_event_at)
        else {
            continue;
        };
        let Some(groups) = inline_groups.get(&parent_event) else {
            continue;
        };
        let candidates: Vec<_> = groups
            .iter()
            .filter(|pieces| {
                let start = pieces.first().expect("nonempty group").start;
                let end = pieces.last().expect("nonempty group").end;
                start <= range.start && range.end <= end
            })
            .collect();
        if let [pieces] = candidates.as_slice() {
            *range = pieces.first().unwrap().start..pieces.last().unwrap().end;
            repaired_paragraphs.insert(owner, (*pieces).clone());
        }
    }
    // Existing block owners retain their exact source envelopes and omissions.
    // Subtract those envelopes from parser-confirmed delimiter candidates to
    // avoid assigning the same delimiter to two canonical owners.
    let mut blocks: Vec<_> = owner_ranges.values().cloned().collect();
    blocks.sort_by_key(|range| (range.start, range.end));
    document_ranges.sort_by_key(|range| (range.start, range.end));
    let mut merged: Vec<Range<usize>> = Vec::new();
    for range in document_ranges {
        if let Some(last) = merged.last_mut() {
            if range.start <= last.end {
                last.end = last.end.max(range.end);
                continue;
            }
        }
        if !range.is_empty() {
            merged.push(range);
        }
    }
    let mut document_ranges = Vec::new();
    let mut first_block = 0;
    for range in merged {
        while first_block < blocks.len() && blocks[first_block].end <= range.start {
            first_block += 1;
        }
        let mut at = range.start;
        for block in blocks[first_block..]
            .iter()
            .take_while(|block| block.start < range.end)
        {
            if at < block.start {
                document_ranges.push(at..block.start);
            }
            at = at.max(block.end);
        }
        if at < range.end {
            document_ranges.push(at..range.end);
        }
    }
    let mut retained = BTreeSet::new();
    if !document_ranges.is_empty() || source.is_empty() {
        retained.insert(0);
    }
    for &owner in owner_ranges.keys() {
        let mut pending = vec![owner];
        while let Some(id) = pending.pop() {
            retained.insert(id);
            pending.extend(tree.nodes[id].children.iter().copied());
        }
        let mut parent = tree.nodes[owner].parent;
        while let Some(id) = parent {
            if !retained.insert(id) {
                break;
            }
            parent = tree.nodes[id].parent;
        }
    }
    for id in active
        .iter()
        .copied()
        .filter(|id| matches!(tree.nodes[*id].kind, "diffBlock" | "mermaidBlock" | "image"))
    {
        let mut next = Some(id);
        while let Some(id) = next {
            if !retained.insert(id) {
                break;
            }
            next = tree.nodes[id].parent;
        }
    }
    for group in &maps {
        for (leaf, map) in group {
            if map.text.is_empty() {
                continue;
            }
            let mut next = *leaf;
            while let Some(id) = next {
                if !retained.insert(id) {
                    break;
                }
                next = tree.nodes[id].parent;
            }
        }
    }
    let mut pieces_by_leaf = BTreeMap::<usize, Vec<Range<usize>>>::new();
    for (leaf, map) in maps.iter().flatten() {
        if let Some(leaf) = leaf {
            pieces_by_leaf
                .entry(*leaf)
                .or_default()
                .push(map.raw.clone());
        }
    }
    let code_descriptors: BTreeMap<_, _> = descriptors
        .iter()
        .enumerate()
        .filter(|(_, (_, _, _, value))| value["role"] == "code")
        .map(|(index, (start, end, _, _))| ((*start, *end), index))
        .collect();
    let primitive_descriptors: BTreeMap<_, _> = descriptors
        .iter()
        .enumerate()
        .filter(|(_, (_, _, _, value))| value["construct"] == "codeBlock")
        .map(|(index, (start, end, _, _))| ((*start, *end), index))
        .collect();
    let direct_descriptors: BTreeMap<_, _> = entries
        .rows
        .iter()
        .enumerate()
        .filter(|(_, (_, position, value))| *position == 0 && value["construct"] == "codeBlock")
        .map(|(index, (collection, _, _))| (collection.clone(), index))
        .collect();
    let mut child_indices = vec![0; tree.nodes.len()];
    for node in &tree.nodes {
        for (index, child) in node.children.iter().enumerate() {
            child_indices[*child] = index;
        }
    }
    let ids: BTreeMap<_, _> = retained.iter().map(|id| (*id, entries.id())).collect();
    let range_value =
        |range: &Range<usize>| json!({"start":units[range.start],"end":units[range.end]});
    let mut native_attributes = BTreeMap::new();
    for &id in &retained {
        let node = &tree.nodes[id];
        let mut pieces = pieces_by_leaf.remove(&id).unwrap_or_default();
        let literal = pieces
            .iter()
            .cloned()
            .reduce(|a, b| a.start.min(b.start)..a.end.max(b.end));
        let generated_open = node.source.as_ref().and_then(|node| node.opening.as_ref());
        let owner_range = generated_open.and_then(|opening| {
            let index = generated_ranges.partition_point(|(range, _)| range.end <= opening.start);
            generated_ranges
                .get(index)
                .filter(|(range, _)| range.start <= opening.start)
                .map(|(_, event)| {
                    if matches!(node.kind, "taskList" | "bulletList" | "orderedList") {
                        task_list_ranges
                            .get(event)
                            .cloned()
                            .unwrap_or_else(|| events[*event].1.clone())
                    } else {
                        events[*event].1.clone()
                    }
                })
        });
        let repaired_paragraph = repaired_paragraphs.get(&id);
        let range = if let Some(receipts) = repaired_paragraph {
            pieces.clone_from(receipts);
            owner_ranges[&id].clone()
        } else {
            literal.or(owner_range).unwrap_or(0..0)
        };
        let attributes_start = entries.rows.len();
        let attrs = entries.context_attributes(&if node.attributes.is_null() {
            json!({})
        } else {
            node.attributes.clone()
        });
        native_attributes.insert(id, attrs.clone());
        let primitive = match node.kind {
            "diffBlock" => Some("diff"),
            "mermaidBlock" => Some("mermaid"),
            _ => None,
        };
        if let Some(primitive) = primitive {
            let code_ref = entries.rows[attributes_start..]
                .iter()
                .find_map(|(_, _, value)| {
                    (value["key"] == "code")
                        .then(|| value["valueRef"].as_str())
                        .flatten()
                })
                .expect("primitive code field")
                .to_owned();
            entries
                .artifact_sources
                .push((format!("d:{}", ids[&id]), code_ref, primitive));
        }
        let child_index = child_indices[id];
        let mut native = json!({"kind":"nativeNode","id":ids[&id],"profile":"canonicalNote","profileVersion":1,
            "nodeType":node.kind,"nodeClass":if node.kind=="text" {"text"} else if primitive.is_some() || matches!(node.kind,"hardBreak" | "image") {"atom"} else {"container"},
            "parentRef":node.parent.map(|parent|format!("d:{}",ids[&parent])),"childIndex":child_index,
            "sourceRange":range_value(&range),"provenance":if range.is_empty(){"implicit"}else{"explicit"},"attributesRef":attrs});
        if !node.marks.is_empty() {
            native["marksRef"] = json!(entries.context_attributes(&json!(node.marks)));
        }
        if repaired_paragraph.is_some()
            || pieces.windows(2).any(|pair| pair[0].end != pair[1].start)
        {
            let reference = format!("d:{}:pieces", ids[&id]);
            for (position, piece) in pieces.iter().enumerate() {
                let piece_id = entries.id();
                entries.rows.push((reference.clone(),position,json!({"kind":"sourcePiece","id":piece_id,"nodeRef":format!("d:{}",ids[&id]),"sourceRange":range_value(piece),"role":"body"})));
            }
            native["provenance"] = json!("repaired");
            native["sourcePiecesRef"] = json!(reference);
        }
        if primitive.is_some() {
            native["nativeRef"] = json!(format!("d:{}", ids[&id]));
            if let Some(index) = primitive_descriptors.get(&(units[range.start], units[range.end]))
            {
                let (_, _, owner, descriptor) = &mut descriptors[*index];
                descriptor["nativeRef"] = native["nativeRef"].clone();
                let direct = direct_descriptors
                    .get(&format!("d:{owner}"))
                    .expect("persisted lexical owner");
                entries.rows[*direct].2["nativeRef"] = native["nativeRef"].clone();
            }
            descriptors.push((
                units[range.start],
                units[range.end],
                ids[&id].clone(),
                native.clone(),
            ));
        }
        if matches!(node.kind, "hardBreak" | "image") {
            descriptors.push((
                units[range.start],
                units[range.end],
                ids[&id].clone(),
                native.clone(),
            ));
        }
        entries.rows.push((format!("d:{}", ids[&id]), 0, native));
    }
    if !document_ranges.is_empty() || source.is_empty() {
        let document_id = entries.id();
        let mut owner = json!({"kind":"boundary","id":document_id,"construct":"markdownDocument",
            "profile":"canonicalNote","profileVersion":1,"entryPath":"markdown",
            "sourceRange":{"start":0,"end":units[source.len()]},
            "nativeRef":format!("d:{}",ids[&0]),"attributesRef":native_attributes[&0]});
        entries
            .rows
            .push((format!("d:{document_id}"), 0, owner.clone()));
        owner["sourceMapRef"] = json!(format!("h:{document_id}"));
        if source.is_empty() {
            descriptors.push((0, 0, document_id.clone(), owner.clone()));
        }
        for (position, range) in document_ranges.into_iter().enumerate() {
            let id = entries.id();
            entries.rows.push((
                format!("h:{document_id}"),
                position,
                json!({"kind":"sourceMap","id":id,
                "profile":"canonicalNote","profileVersion":1,"ownerRef":format!("d:{document_id}"),
                "textNodeId":null,"textNodeRef":null,"sourceRange":range_value(&range),
                "renderedRange":{"start":0,"end":0},"mapping":"omitted","textRef":null}),
            ));
            // Admit this full-source owner only where its own indexed gaps meet
            // the requested window; never pull preceding blocks into a seek.
            descriptors.push((
                units[range.start],
                units[range.end],
                document_id.clone(),
                owner.clone(),
            ));
        }
    }
    let mut block_maps = BTreeMap::<usize, Vec<(Option<usize>, TextMapping)>>::new();
    let mut block_excluded = BTreeMap::<usize, Vec<Range<usize>>>::new();
    for (owner, raw) in atom_ranges {
        block_excluded.entry(owner).or_default().push(raw);
    }
    for (code, mut group) in codes.into_iter().zip(maps) {
        if code.ordinary {
            for (leaf, map) in group {
                if let Some(owner) = leaf.and_then(|id| text_owner[id]) {
                    block_maps.entry(owner).or_default().push((leaf, map));
                }
            }
            continue;
        }
        let Some(index) = code_descriptors.get(&(units[code.raw.start], units[code.raw.end]))
        else {
            continue;
        };
        for owner in group
            .iter()
            .filter_map(|(leaf, _)| leaf.and_then(|id| text_owner[id]))
            .collect::<BTreeSet<_>>()
        {
            block_excluded
                .entry(owner)
                .or_default()
                .push(code.raw.clone());
        }
        let (_, _, owner, descriptor) = &mut descriptors[*index];
        let native = group
            .iter()
            .find(|(_, map)| !map.text.is_empty())
            .and_then(|(leaf, _)| *leaf);
        descriptor["codeSource"] = json!({"profile":"canonicalNote","profileVersion":1,
            "openingRange":range_value(&(code.raw.start..code.body.start)),"bodyRange":range_value(&code.body),
            "closingRange":range_value(&(code.body.end..code.raw.end))});
        descriptor["nativeRef"] = json!(native.map(|id| format!("d:{}", ids[&id])));
        entries
            .rows
            .push((format!("d:{owner}"), 0, descriptor.clone()));
        descriptor["sourceMapRef"] = json!(format!("h:{owner}"));
        group.sort_by_key(|(_, map)| (map.raw.start, map.raw.end));
        let mut cursor = code.raw.start;
        let mut gaps = Vec::new();
        for (_, map) in &group {
            if cursor < map.raw.start {
                gaps.push((
                    None,
                    TextMapping {
                        raw: cursor..map.raw.start,
                        rendered: 0..0,
                        text: String::new(),
                        mapping: "omitted",
                    },
                ));
            }
            cursor = cursor.max(map.raw.end);
        }
        if cursor < code.raw.end {
            gaps.push((
                None,
                TextMapping {
                    raw: cursor..code.raw.end,
                    rendered: 0..0,
                    text: String::new(),
                    mapping: "omitted",
                },
            ));
        }
        group.extend(gaps);
        group.sort_by_key(|(_, map)| (map.raw.start, map.raw.end));
        for (position, (leaf, map)) in group.into_iter().enumerate() {
            let id = entries.id();
            let leaf = leaf.filter(|id| ids.contains_key(id));
            let text_ref =
                (!map.text.is_empty()).then(|| entries.fragment("renderedText", &map.text));
            entries.rows.push((format!("h:{owner}"),position,json!({"kind":"sourceMap","id":id,"profile":"canonicalNote","profileVersion":1,
                "ownerRef":format!("d:{owner}"),"textNodeId":leaf.map(|id|ids[&id].clone()),"textNodeRef":leaf.map(|id|format!("d:{}",ids[&id])),
                "sourceRange":range_value(&map.raw),"renderedRange":{"start":map.rendered.start,"end":map.rendered.end},"mapping":map.mapping,"textRef":text_ref})));
        }
    }
    for (node, raw) in owner_ranges {
        let mut group = block_maps.remove(&node).unwrap_or_default();
        let mut covered = block_excluded.remove(&node).unwrap_or_default();
        covered.extend(group.iter().map(|(_, map)| map.raw.clone()));
        covered.sort_by_key(|range| (range.start, range.end));
        let mut at = raw.start;
        for range in covered.into_iter().chain(std::iter::once(raw.end..raw.end)) {
            if at < range.start {
                group.push((
                    None,
                    TextMapping {
                        raw: at..range.start,
                        rendered: 0..0,
                        text: String::new(),
                        mapping: "omitted",
                    },
                ));
            }
            at = at.max(range.end);
        }
        let owner = entries.id();
        let attributes = &native_attributes[&node];
        let mut descriptor = json!({"kind":"boundary","id":owner,"construct":"markdownBlock","profile":"canonicalNote","profileVersion":1,"entryPath":"markdown","sourceRange":range_value(&raw),"nativeRef":format!("d:{}",ids[&node]),"attributesRef":attributes});
        entries
            .rows
            .push((format!("d:{owner}"), 0, descriptor.clone()));
        descriptor["sourceMapRef"] = json!(format!("h:{owner}"));
        descriptors.push((units[raw.start], units[raw.end], owner.clone(), descriptor));
        group.sort_by_key(|(_, map)| (map.raw.start, map.raw.end));
        let mut compact: Vec<(Option<usize>, TextMapping)> = Vec::with_capacity(group.len());
        for (leaf, map) in group {
            if let Some((prior_leaf, prior)) = compact.last_mut() {
                // Only collapse scalar-preserving identity runs within this owner
                // and native leaf. Native provenance/source pieces were recorded
                // independently above and are deliberately left intact.
                if leaf.is_some()
                    && *prior_leaf == leaf
                    && prior.mapping == "identity"
                    && map.mapping == "identity"
                    && prior.raw.end == map.raw.start
                    && prior.rendered.end == map.rendered.start
                    && prior.text.len() + map.text.len() <= crate::note_page_index::PIECE_BYTES
                {
                    prior.raw.end = map.raw.end;
                    prior.rendered.end = map.rendered.end;
                    prior.text.push_str(&map.text);
                    continue;
                }
            }
            compact.push((leaf, map));
        }
        for (position, (leaf, map)) in compact.into_iter().enumerate() {
            let id = entries.id();
            let text_ref =
                (!map.text.is_empty()).then(|| entries.fragment("renderedText", &map.text));
            entries.rows.push((format!("h:{owner}"),position,json!({"kind":"sourceMap","id":id,"profile":"canonicalNote","profileVersion":1,"ownerRef":format!("d:{owner}"),"textNodeId":leaf.map(|id|ids[&id].clone()),"textNodeRef":leaf.map(|id|format!("d:{}",ids[&id])),"sourceRange":range_value(&map.raw),"renderedRange":{"start":map.rendered.start,"end":map.rendered.end},"mapping":map.mapping,"textRef":text_ref})));
        }
    }
}

#[cfg(test)]
mod delimiter_tests {
    use super::*;

    #[test]
    fn indexed_markdown_source_maps_preserve_parser_children() {
        for source in [
            "- [ ] [First task](intent://local/task/first)\n\n- [ ] [Second task](intent://local/task/second)\n",
            "- outer\n\n  - [ ] nested café 🙂\n\n  - next\n",
            "> first café 🙂\n>\n> second\n\n> third\n",
            "> **bold** &amp; [link](https://example.test) `code` ![alt](image.png)\n\n- [x] literal &amp; `code`\n",
        ] {
            let events: Vec<_> = Parser::new_ext(source, super::super::markdown_source::options())
                .into_offset_iter().collect();
            let omitted = document_delimiters(source, &events);
            let protected: Vec<_> = events.iter().filter(|(event, _)| matches!(event,
                Event::Text(_) | Event::Code(_) | Event::Html(_) | Event::InlineHtml(_) |
                Event::Start(Tag::Link { .. } | Tag::Image { .. } | Tag::CodeBlock(_))
            )).collect();
            for range in &omitted {
                assert!(source.is_char_boundary(range.start) && source.is_char_boundary(range.end));
                for (event, child) in &protected {
                    assert!(range.end <= child.start || range.start >= child.end,
                        "delimiter {range:?} overlaps {event:?} child {child:?} in {source:?}");
                }
            }
            let utf16 = |range: &Range<usize>| json!({"start":source[..range.start].encode_utf16().count(),"end":source[..range.end].encode_utf16().count()});
            eprintln!("MARKDOWN_DELIMITER_RECEIPT {}", json!({
                "source":source,
                "events":events.iter().map(|(event,range)|json!({"event":format!("{event:?}"),"sourceRange":utf16(range)})).collect::<Vec<_>>(),
                "delimiterCandidates":omitted.iter().map(|range|json!({"sourceRange":utf16(range),"source":&source[range.clone()]})).collect::<Vec<_>>()
            }));
        }
    }
}
