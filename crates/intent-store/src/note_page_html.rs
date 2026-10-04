//! Write-time HTML tree construction with original source provenance.
//!
//! Token source coordinates and repaired DOM ownership are separate: foster
//! parenting must never turn a DOM ancestor into a fabricated literal range.
mod index;
mod markdown;
use html5ever::interface::{ElementFlags, NodeOrText, QuirksMode, Tracer, TreeSink};
use html5ever::tendril::StrTendril;
use html5ever::tokenizer::{states::RawKind, Tag, TagKind, Token, TokenSink, TokenSinkResult};
use html5ever::tree_builder::{TreeBuilder, TreeBuilderOpts};
use html5ever::{Attribute, ExpandedName, QualName};
use html5gum::{DefaultEmitter, Emitter, ForwardingEmitter, State, Tokenizer};
pub(crate) use index::append;
pub(crate) use markdown::append_codes;
use serde_json::{json, Value};
use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::ops::Range;
use std::rc::{Rc, Weak};

type Handle = Rc<Node>;

#[derive(Default, Clone, Debug)]
struct ActiveToken {
    range: Range<usize>,
    name: String,
    closing: bool,
}

#[derive(Debug)]
struct Node {
    name: QualName,
    text: RefCell<String>,
    attrs: RefCell<Vec<Attribute>>,
    children: RefCell<Vec<Handle>>,
    parent: RefCell<Weak<Node>>,
    opening: Option<Range<usize>>,
    closing: RefCell<Option<Range<usize>>>,
    text_pieces: RefCell<Vec<Range<usize>>>,
    template: RefCell<Option<Handle>>,
    integration: bool,
}

impl Node {
    fn new(
        name: QualName,
        attrs: Vec<Attribute>,
        opening: Option<Range<usize>>,
        integration: bool,
    ) -> Handle {
        Rc::new(Self {
            name,
            attrs: RefCell::new(attrs),
            opening,
            integration,
            text: RefCell::default(),
            children: RefCell::default(),
            parent: RefCell::default(),
            closing: RefCell::default(),
            text_pieces: RefCell::default(),
            template: RefCell::default(),
        })
    }
    fn special(name: &str) -> Handle {
        Self::new(
            QualName::new(None, "".into(), name.into()),
            Vec::new(),
            None,
            false,
        )
    }
}

struct SourceTree {
    document: Handle,
    active: Rc<RefCell<ActiveToken>>,
    text_range: Rc<RefCell<Range<usize>>>,
}

impl SourceTree {
    fn detach(node: &Handle) {
        if let Some(parent) = node.parent.borrow().upgrade() {
            parent
                .children
                .borrow_mut()
                .retain(|child| !Rc::ptr_eq(child, node));
        }
        *node.parent.borrow_mut() = Weak::new();
    }

    fn insert(&self, parent: &Handle, at: usize, child: NodeOrText<Handle>) {
        match child {
            NodeOrText::AppendNode(node) => {
                Self::detach(&node);
                *node.parent.borrow_mut() = Rc::downgrade(parent);
                let at = at.min(parent.children.borrow().len());
                parent.children.borrow_mut().insert(at, node);
            }
            NodeOrText::AppendText(text) => {
                let previous = at
                    .checked_sub(1)
                    .and_then(|i| parent.children.borrow().get(i).cloned());
                let node = if let Some(node) = previous.filter(|n| n.name.local.as_ref() == "#text")
                {
                    node
                } else {
                    let node = Node::special("#text");
                    *node.parent.borrow_mut() = Rc::downgrade(parent);
                    parent.children.borrow_mut().insert(at, node.clone());
                    node
                };
                node.text.borrow_mut().push_str(&text);
                // Table text can be buffered until the following tag. Its
                // provenance belongs to the character token, not that tag.
                let range = self.text_range.borrow().clone();
                if !range.is_empty() && node.text_pieces.borrow().last() != Some(&range) {
                    node.text_pieces.borrow_mut().push(range);
                }
            }
        }
    }
}

impl TreeSink for SourceTree {
    type Handle = Handle;
    type Output = Handle;
    type ElemName<'a> = ExpandedName<'a>;

    fn finish(self) -> Handle {
        self.document
    }
    fn parse_error(&self, _: Cow<'static, str>) {}
    fn get_document(&self) -> Handle {
        self.document.clone()
    }
    fn elem_name<'a>(&'a self, target: &'a Handle) -> ExpandedName<'a> {
        target.name.expanded()
    }
    fn create_element(&self, name: QualName, attrs: Vec<Attribute>, flags: ElementFlags) -> Handle {
        let token = self.active.borrow();
        let opening =
            (!token.closing && token.name == name.local.as_ref()).then(|| token.range.clone());
        let node = Node::new(
            name,
            attrs,
            opening,
            flags.mathml_annotation_xml_integration_point,
        );
        if flags.template {
            *node.template.borrow_mut() = Some(Node::special("#fragment"));
        }
        node
    }
    fn create_comment(&self, _: StrTendril) -> Handle {
        Node::special("#comment")
    }
    fn create_pi(&self, _: StrTendril, _: StrTendril) -> Handle {
        Node::special("#pi")
    }
    fn append(&self, parent: &Handle, child: NodeOrText<Handle>) {
        let at = parent.children.borrow().len();
        self.insert(parent, at, child);
    }
    fn append_based_on_parent_node(
        &self,
        element: &Handle,
        previous: &Handle,
        child: NodeOrText<Handle>,
    ) {
        if element.parent.borrow().upgrade().is_some() {
            self.append_before_sibling(element, child);
        } else {
            self.append(previous, child);
        }
    }
    fn append_doctype_to_document(&self, _: StrTendril, _: StrTendril, _: StrTendril) {}
    fn pop(&self, node: &Handle) {
        let token = self.active.borrow();
        if token.closing && token.name == node.name.local.as_ref() {
            *node.closing.borrow_mut() = Some(token.range.clone());
        }
    }
    fn get_template_contents(&self, node: &Handle) -> Handle {
        node.template
            .borrow()
            .as_ref()
            .expect("template contents")
            .clone()
    }
    fn same_node(&self, a: &Handle, b: &Handle) -> bool {
        Rc::ptr_eq(a, b)
    }
    fn set_quirks_mode(&self, _: QuirksMode) {}
    fn append_before_sibling(&self, sibling: &Handle, child: NodeOrText<Handle>) {
        let parent = sibling.parent.borrow().upgrade().expect("attached sibling");
        let at = parent
            .children
            .borrow()
            .iter()
            .position(|n| Rc::ptr_eq(n, sibling))
            .expect("sibling index");
        self.insert(&parent, at, child);
    }
    fn add_attrs_if_missing(&self, node: &Handle, attrs: Vec<Attribute>) {
        let mut existing = node.attrs.borrow_mut();
        for attr in attrs {
            if !existing.iter().any(|a| a.name == attr.name) {
                existing.push(attr);
            }
        }
    }
    fn remove_from_parent(&self, node: &Handle) {
        Self::detach(node);
    }
    fn reparent_children(&self, node: &Handle, parent: &Handle) {
        for child in std::mem::take(&mut *node.children.borrow_mut()) {
            *child.parent.borrow_mut() = Rc::downgrade(parent);
            parent.children.borrow_mut().push(child);
        }
    }
    fn is_mathml_annotation_xml_integration_point(&self, node: &Handle) -> bool {
        node.integration
    }
}

struct SpannedEmitter {
    inner: DefaultEmitter<usize>,
    foreign: Rc<Cell<bool>>,
}

#[derive(Default)]
struct RetainedHandles(RefCell<Vec<Handle>>);
impl Tracer for RetainedHandles {
    type Handle = Handle;
    fn trace_handle(&self, node: &Handle) {
        self.0.borrow_mut().push(node.clone());
    }
}
impl ForwardingEmitter for SpannedEmitter {
    type Token = html5gum::Token<usize>;
    fn inner(&mut self) -> &mut impl Emitter<Token = Self::Token> {
        &mut self.inner
    }
    fn adjusted_current_node_present_but_not_in_html_namespace(&mut self) -> bool {
        self.foreign.get()
    }
}

fn parse(source: &str) -> Handle {
    let active = Rc::new(RefCell::new(ActiveToken::default()));
    let text_range = Rc::new(RefCell::new(0..0));
    let sink = SourceTree {
        document: Node::special("#document"),
        active: active.clone(),
        text_range: text_range.clone(),
    };
    let context = Node::new(
        QualName::new(None, "http://www.w3.org/1999/xhtml".into(), "body".into()),
        Vec::new(),
        None,
        false,
    );
    let tree = TreeBuilder::new_for_fragment(sink, context, None, TreeBuilderOpts::default());
    let foreign = Rc::new(Cell::new(false));
    let emitter = SpannedEmitter {
        inner: DefaultEmitter::new_with_span(),
        foreign: foreign.clone(),
    };
    let mut tokenizer = Tokenizer::new_with_emitter(source, emitter);
    while let Some(result) = tokenizer.next() {
        let token = result.expect("string reader is infallible");
        let (token, current) = match token {
            html5gum::Token::StartTag(tag) => {
                let name = String::from_utf8_lossy(&tag.name).into_owned();
                let current = ActiveToken {
                    range: tag.span.start..tag.span.end,
                    name: name.clone(),
                    closing: false,
                };
                let attrs = tag
                    .attributes
                    .into_iter()
                    .map(|(key, value)| Attribute {
                        name: QualName::new(
                            None,
                            "".into(),
                            String::from_utf8_lossy(&key).as_ref().into(),
                        ),
                        value: StrTendril::from_slice(&String::from_utf8_lossy(&value)),
                    })
                    .collect();
                (
                    Token::TagToken(Tag {
                        kind: TagKind::StartTag,
                        name: name.into(),
                        self_closing: tag.self_closing,
                        attrs,
                    }),
                    current,
                )
            }
            html5gum::Token::EndTag(tag) => {
                let name = String::from_utf8_lossy(&tag.name).into_owned();
                (
                    Token::TagToken(Tag {
                        kind: TagKind::EndTag,
                        name: name.clone().into(),
                        self_closing: false,
                        attrs: Vec::new(),
                    }),
                    ActiveToken {
                        range: tag.span.start..tag.span.end,
                        name,
                        closing: true,
                    },
                )
            }
            html5gum::Token::String(text) => (
                Token::CharacterTokens(StrTendril::from_slice(&String::from_utf8_lossy(&text))),
                ActiveToken {
                    range: text.span.start..text.span.end,
                    ..Default::default()
                },
            ),
            html5gum::Token::Comment(_)
            | html5gum::Token::Doctype(_)
            | html5gum::Token::Error(_) => continue,
        };
        if matches!(token, Token::CharacterTokens(_)) {
            *text_range.borrow_mut() = current.range.clone();
        }
        // TreeSink::pop is not called by every HTML5 stack-removal algorithm.
        // A closing tag owns literal syntax only for a matching retained node
        // actually removed while that token is processed; ignored ends own none.
        let before = RetainedHandles::default();
        if current.closing {
            tree.trace_handles(&before);
        }
        *active.borrow_mut() = current.clone();
        match tree.process_token(token, 1) {
            TokenSinkResult::Continue => {}
            TokenSinkResult::Script(_) => tokenizer.set_state(State::Data),
            TokenSinkResult::Plaintext => tokenizer.set_state(State::PlainText),
            TokenSinkResult::RawData(kind) => tokenizer.set_state(match kind {
                RawKind::Rcdata => State::RcData,
                RawKind::Rawtext => State::RawText,
                RawKind::ScriptData | RawKind::ScriptDataEscaped(_) => State::ScriptData,
            }),
        }
        foreign.set(tree.adjusted_current_node_present_but_not_in_html_namespace());
        if current.closing {
            let after = RetainedHandles::default();
            tree.trace_handles(&after);
            for node in before.0.into_inner() {
                if node.name.local.as_ref() == current.name
                    && !after.0.borrow().iter().any(|n| Rc::ptr_eq(n, &node))
                {
                    *node.closing.borrow_mut() = Some(current.range.clone());
                }
            }
        }
    }
    *active.borrow_mut() = ActiveToken {
        range: source.len()..source.len(),
        ..Default::default()
    };
    let _ = tree.process_token(Token::EOFToken, 1);
    tree.end();
    tree.sink.finish()
}

struct NativeNode {
    kind: &'static str,
    attributes: Value,
    marks: Vec<Value>,
    text: String,
    source: Option<Handle>,
    children: Vec<usize>,
    parent: Option<usize>,
    maps: Vec<TextMapping>,
}

#[derive(Debug)]
struct DecodedAtom {
    raw: Range<usize>,
    text: String,
    mapping: &'static str,
}

struct TextAtoms<'a> {
    source: &'a str,
    position: usize,
    end: usize,
}
impl Iterator for TextAtoms<'_> {
    type Item = DecodedAtom;
    fn next(&mut self) -> Option<DecodedAtom> {
        if self.position == self.end {
            return None;
        }
        let start = self.position;
        let tail = &self.source[start..self.end];
        if let Some((bytes, text)) = tail.strip_prefix('&').and_then(entity) {
            self.position += bytes + 1;
            return Some(DecodedAtom {
                raw: start..self.position,
                text,
                mapping: "entity",
            });
        }
        let ch = tail.chars().next().expect("remaining scalar");
        self.position += ch.len_utf8();
        let (text, mapping) = if ch == '\r' {
            if self.source.as_bytes().get(self.position) == Some(&b'\n') && self.position < self.end
            {
                self.position += 1;
            }
            ("\n".into(), "normalized")
        } else if ch == '\0' {
            ("\u{fffd}".into(), "normalized")
        } else {
            (ch.to_string(), "identity")
        };
        Some(DecodedAtom {
            raw: start..self.position,
            text,
            mapping,
        })
    }
}

fn entity(tail: &str) -> Option<(usize, String)> {
    if let Some(numeric) = tail.strip_prefix('#') {
        let (digits, radix, offset) = match numeric.strip_prefix(['x', 'X']) {
            Some(hex) => (hex, 16, 2),
            None => (numeric, 10, 1),
        };
        let mut value = Some(0_u32);
        let mut consumed = 0;
        for byte in digits.bytes() {
            let Some(digit) = char::from(byte).to_digit(radix) else {
                break;
            };
            value = value.and_then(|v| v.checked_mul(radix)?.checked_add(digit));
            consumed += 1;
        }
        if consumed == 0 {
            return None;
        }
        let end = offset + consumed;
        let end = end + usize::from(tail.as_bytes().get(end) == Some(&b';'));
        let value = value.unwrap_or(0xfffd);
        let ch = if (0x80..=0x9f).contains(&value) {
            html5ever::data::C1_REPLACEMENTS[(value - 0x80) as usize]
                .unwrap_or_else(|| char::from_u32(value).expect("C1 scalar"))
        } else if value == 0 {
            '\u{fffd}'
        } else {
            char::from_u32(value).unwrap_or('\u{fffd}')
        };
        return Some((end, ch.to_string()));
    }
    // The HTML named-reference table has bounded names. Use the longest real
    // match, including the standard legacy names without a trailing semicolon.
    let limit = tail
        .bytes()
        .take(33)
        .take_while(|b| b.is_ascii_alphanumeric() || *b == b';')
        .count();
    for end in (1..=limit).rev() {
        if let Some(&(a, b)) = html5ever::data::NAMED_ENTITIES.get(&tail[..end]) {
            if a == 0 {
                continue;
            }
            let mut text = char::from_u32(a).expect("named entity scalar").to_string();
            if b != 0 {
                text.push(char::from_u32(b).expect("named entity second scalar"));
            }
            return Some((end, text));
        }
    }
    None
}

#[derive(Debug)]
struct TextMapping {
    raw: Range<usize>,
    rendered: Range<usize>,
    text: String,
    mapping: &'static str,
}

fn push_mapping(maps: &mut Vec<TextMapping>, map: TextMapping) {
    if let Some(last) = maps.last_mut() {
        if last.mapping == map.mapping
            && matches!(map.mapping, "identity" | "omitted")
            && last.raw.end == map.raw.start
            && last.rendered.end == map.rendered.start
            && last.text.len() + map.text.len() <= super::note_page_index::PIECE_BYTES
        {
            last.raw.end = map.raw.end;
            last.rendered.end = map.rendered.end;
            last.text.push_str(&map.text);
            return;
        }
    }
    maps.push(map);
}

fn normalized_text_maps(
    source: &str,
    ranges: &[Range<usize>],
    mut previous_space: bool,
    preserve: bool,
) -> (String, Vec<TextMapping>) {
    let mut output = String::new();
    let mut maps = Vec::new();
    let mut units = 0;
    let mut atoms = ranges
        .iter()
        .flat_map(|range| TextAtoms {
            source,
            position: range.start,
            end: range.end,
        })
        .peekable();
    while let Some(mut atom) = atoms.next() {
        let whitespace = |text: &str| {
            text.chars()
                .all(|ch| matches!(ch, ' ' | '\t' | '\n' | '\r' | '\x0c'))
        };
        let (text, mapping) = if !preserve && whitespace(&atom.text) {
            // Keep one normalization seam for the complete contiguous raw run.
            // A far source window inside it must still resolve the visible space.
            while atoms
                .peek()
                .is_some_and(|next| next.raw.start == atom.raw.end && whitespace(&next.text))
            {
                atom.raw.end = atoms.next().expect("peeked whitespace").raw.end;
            }
            let result = if previous_space {
                (String::new(), "omitted")
            } else {
                (
                    " ".to_owned(),
                    if &source[atom.raw.clone()] == " " {
                        "identity"
                    } else {
                        "normalized"
                    },
                )
            };
            previous_space = true;
            result
        } else {
            previous_space = false;
            (atom.text, atom.mapping)
        };
        let end = units + text.encode_utf16().count();
        output.push_str(&text);
        push_mapping(
            &mut maps,
            TextMapping {
                raw: atom.raw,
                rendered: units..end,
                text,
                mapping,
            },
        );
        units = end;
    }
    (output, maps)
}

struct NativeTree<'a> {
    nodes: Vec<NativeNode>,
    source: &'a str,
    omitted: Vec<TextMapping>,
}

impl<'a> NativeTree<'a> {
    fn new(source: &'a str) -> Self {
        let mut tree = Self {
            nodes: Vec::new(),
            source,
            omitted: Vec::new(),
        };
        tree.add(None, "doc", None, Value::Null);
        tree
    }
    fn add(
        &mut self,
        parent: Option<usize>,
        kind: &'static str,
        source: Option<Handle>,
        attributes: Value,
    ) -> usize {
        let id = self.nodes.len();
        self.nodes.push(NativeNode {
            kind,
            attributes,
            marks: Vec::new(),
            text: String::new(),
            source,
            children: Vec::new(),
            parent,
            maps: Vec::new(),
        });
        if let Some(parent) = parent {
            self.nodes[parent].children.push(id);
        }
        id
    }
    fn inline_parent(&mut self, parent: usize) -> usize {
        if matches!(
            self.nodes[parent].kind,
            "paragraph" | "heading" | "codeBlock"
        ) {
            return parent;
        }
        if let Some(last) = self.nodes[parent].children.last().copied() {
            if self.nodes[last].kind == "paragraph" && self.nodes[last].source.is_none() {
                return last;
            }
        }
        self.add(Some(parent), "paragraph", None, Value::Null)
    }
    fn text(&mut self, source: &Handle, parent: usize, marks: &[Value]) {
        let parent = self.inline_parent(parent);
        let previous = self.nodes[parent].children.last().copied();
        let space = previous.is_none_or(|id| {
            self.nodes[id].kind == "hardBreak" || self.nodes[id].text.ends_with(' ')
        });
        let preserve = self.nodes[parent].kind == "codeBlock";
        let (text, mut maps) =
            normalized_text_maps(self.source, &source.text_pieces.borrow(), space, preserve);
        if text.is_empty() {
            self.omitted.extend(maps);
            return;
        }
        if let Some(id) =
            previous.filter(|id| self.nodes[*id].kind == "text" && self.nodes[*id].marks == marks)
        {
            let offset = self.nodes[id].maps.last().map_or(0, |map| map.rendered.end);
            for map in &mut maps {
                map.rendered.start += offset;
                map.rendered.end += offset;
            }
            self.nodes[id].text.push_str(&text);
            self.nodes[id].maps.extend(maps);
        } else {
            let id = self.add(Some(parent), "text", Some(source.clone()), Value::Null);
            self.nodes[id].text = text;
            self.nodes[id].marks = marks.to_vec();
            self.nodes[id].maps = maps;
        }
    }
    fn finish_block(&mut self, parent: usize) {
        if self.nodes[parent].kind == "codeBlock" {
            return;
        }
        let Some(last) = self.nodes[parent].children.last().copied() else {
            return;
        };
        if self.nodes[last].kind == "text" {
            if self.nodes[last].text.ends_with(' ') {
                self.nodes[last].text.pop();
                let maps = &mut self.nodes[last].maps;
                let index = maps
                    .iter()
                    .rposition(|m| !m.text.is_empty())
                    .expect("text has mapping");
                let map = &mut maps[index];
                if map.mapping == "identity" && map.text.len() > 1 {
                    map.text.pop();
                    map.raw.end -= 1;
                    map.rendered.end -= 1;
                    let omitted = TextMapping {
                        raw: map.raw.end..map.raw.end + 1,
                        rendered: map.rendered.end..map.rendered.end,
                        text: String::new(),
                        mapping: "omitted",
                    };
                    maps.insert(index + 1, omitted);
                } else {
                    map.text.clear();
                    map.rendered.end = map.rendered.start;
                    map.mapping = "omitted";
                }
                // Trailing already-omitted source shares the final leaf endpoint.
                let end = self.nodes[last].text.encode_utf16().count();
                for map in &mut self.nodes[last].maps {
                    if map.text.is_empty() {
                        map.rendered.start = map.rendered.start.min(end);
                        map.rendered.end = map.rendered.start;
                    }
                }
            }
            if self.nodes[last].text.is_empty() {
                self.nodes[parent].children.pop();
                self.omitted.append(&mut self.nodes[last].maps);
            }
        } else if self.nodes[last].kind == "paragraph" {
            self.finish_block(last);
        }
    }
    fn visit(&mut self, node: &Handle, parent: usize, marks: &[Value]) {
        let tag = node.name.local.as_ref();
        if tag == "#text" {
            self.text(node, parent, marks);
            return;
        }
        // These elements lose their contents in the canonical sanitizer.
        if matches!(
            tag,
            "script"
                | "style"
                | "iframe"
                | "object"
                | "embed"
                | "noscript"
                | "template"
                | "#comment"
                | "#pi"
        ) {
            return;
        }
        let attr = |name: &str| {
            node.attrs
                .borrow()
                .iter()
                .find(|a| a.name.local.as_ref() == name)
                .map(|a| a.value.to_string())
        };
        let kind = match tag {
            "table" => Some("table"),
            "tr" => Some("tableRow"),
            "td" => Some("tableCell"),
            "th" => Some("tableHeader"),
            "p" => Some("paragraph"),
            "blockquote" => Some("blockquote"),
            "pre" => Some("codeBlock"),
            "ul" => Some("bulletList"),
            "ol" => Some("orderedList"),
            "li" => Some("listItem"),
            "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => Some("heading"),
            "img" => Some("image"),
            "hr" => Some("horizontalRule"),
            _ => None,
        };
        if tag == "br" {
            let parent = self.inline_parent(parent);
            self.add(Some(parent), "hardBreak", Some(node.clone()), Value::Null);
            return;
        }
        if let Some(kind) = kind {
            self.finish_block(parent);
            let attrs = match kind {
                "tableCell" | "tableHeader" => {
                    json!({"align":attr("align").filter(|a|matches!(a.as_str(),"left"|"center"|"right")),"colspan":1,"rowspan":1,"colwidth":null})
                }
                "heading" => json!({"level":tag.as_bytes()[1]-b'0'}),
                "orderedList" => {
                    json!({"start":attr("start").and_then(|n|n.parse::<i64>().ok()).unwrap_or(1),"type":null})
                }
                "codeBlock" => json!({"language":null}),
                "image" => {
                    json!({"src":attr("src").filter(|v|safe_url(v)),"alt":attr("alt"),"title":attr("title"),"width":null,"height":null,"mediaUnsupported":null})
                }
                _ => Value::Null,
            };
            let id = self.add(Some(parent), kind, Some(node.clone()), attrs);
            for child in node.children.borrow().iter() {
                self.visit(child, id, &[]);
            }
            if matches!(kind, "tableCell" | "tableHeader" | "listItem")
                && self.nodes[id].children.is_empty()
            {
                self.add(Some(id), "paragraph", None, Value::Null);
            }
            self.finish_block(id);
            return;
        }
        let mut next_marks = marks.to_vec();
        let mark = match tag {
            "strong"|"b" => Some(json!({"type":"bold"})),
            "em"|"i" => Some(json!({"type":"italic"})),
            "s"|"del"|"strike" => Some(json!({"type":"strike"})),
            "code" if self.nodes[parent].kind != "codeBlock" => Some(json!({"type":"code"})),
            "a" => attr("href").filter(|v|safe_url(v)).map(|href|json!({"type":"link","attrs":{"href":href,"target":attr("target").unwrap_or_else(||"_blank".into()),"rel":attr("rel").unwrap_or_else(||"noopener noreferrer nofollow".into()),"class":"text-primary-ink underline cursor-pointer"}})),
            _ => None,
        };
        if let Some(mark) = mark {
            next_marks.retain(|m| m["type"] != mark["type"]);
            next_marks.push(mark);
        }
        for child in node.children.borrow().iter() {
            self.visit(child, parent, &next_marks);
        }
    }
    #[cfg(test)]
    fn json(&self, id: usize) -> Value {
        let node = &self.nodes[id];
        let mut value = json!({"type":node.kind});
        if !node.attributes.is_null() {
            value["attrs"] = node.attributes.clone();
        }
        if !node.children.is_empty() {
            value["content"] = node.children.iter().map(|id| self.json(*id)).collect();
        }
        if !node.marks.is_empty() {
            value["marks"] = json!(node.marks);
        }
        if node.kind == "text" {
            value["text"] = json!(node.text);
        }
        value
    }
}

fn safe_url(value: &str) -> bool {
    let normalized: String = value
        .chars()
        .filter(|ch| !ch.is_ascii_control() && !ch.is_ascii_whitespace())
        .collect();
    match normalized.split_once(':') {
        Some((scheme, _)) => matches!(
            scheme.to_ascii_lowercase().as_str(),
            "http" | "https" | "mailto" | "tel" | "sms" | "intent"
        ),
        None => true,
    }
}

fn canonical(source: &str) -> NativeTree<'_> {
    let root = parse(source);
    let mut tree = NativeTree::new(source);
    tree.visit(&root, 0, &[]);
    tree.finish_block(0);
    tree
}

#[cfg(test)]
mod tests {
    use super::*;
    fn elements(root: &Handle, name: &str) -> Vec<Handle> {
        let mut pending = vec![root.clone()];
        let mut found = Vec::new();
        while let Some(node) = pending.pop() {
            if node.name.local.as_ref() == name {
                found.push(node.clone());
            }
            pending.extend(node.children.borrow().iter().rev().cloned());
        }
        found
    }
    #[test]
    fn canonical_html_matches_recorded_frontend_native_trees() {
        let cases: Value =
            serde_json::from_str(include_str!("tests/fixtures/note_html_native.json")).unwrap();
        for case in cases.as_array().unwrap() {
            let source = case["source"].as_str().unwrap();
            let actual = canonical(source);
            assert_eq!(actual.json(0), case["native"], "{}", case["case"]);
            for (id, node) in actual.nodes.iter().enumerate() {
                for child in &node.children {
                    assert_eq!(actual.nodes[*child].parent, Some(id));
                }
                if node.kind != "text" || node.text.is_empty() {
                    continue;
                }
                assert_eq!(
                    node.maps
                        .iter()
                        .map(|map| map.text.as_str())
                        .collect::<String>(),
                    node.text,
                    "{} text leaf {id}",
                    case["case"]
                );
                let mut offset = 0;
                for map in &node.maps {
                    assert_eq!(map.rendered.start, offset);
                    offset += map.text.encode_utf16().count();
                    assert_eq!(map.rendered.end, offset);
                    assert!(source.is_char_boundary(map.raw.start));
                    assert!(source.is_char_boundary(map.raw.end));
                    assert!(map.text.len() <= super::super::note_page_index::PIECE_BYTES);
                }
            }
        }
    }
    #[test]
    fn html_text_maps_keep_exact_entity_and_crlf_source_addresses() {
        let source = " A &amp; B&#x1f680;\r\n  C\t D&nbsp;E";
        let (text, maps) = normalized_text_maps(
            source,
            std::slice::from_ref(&(0..source.len())),
            true,
            false,
        );
        assert_eq!(text, "A & B🚀 C D\u{a0}E");
        assert_eq!(
            maps.iter().map(|m| m.text.as_str()).collect::<String>(),
            text
        );
        let entity = maps.iter().find(|m| m.text == "🚀").unwrap();
        assert_eq!(&source[entity.raw.clone()], "&#x1f680;");
        assert_eq!(entity.rendered.end - entity.rendered.start, 2);
        assert_eq!(entity.mapping, "entity");
        let crlf = maps
            .iter()
            .find(|m| &source[m.raw.clone()] == "\r\n  ")
            .unwrap();
        assert_eq!(crlf.text, " ");
        assert_eq!(crlf.mapping, "normalized");
        assert_eq!(maps.first().unwrap().mapping, "omitted");
        assert_eq!(
            maps.last().unwrap().rendered.end,
            text.encode_utf16().count()
        );
    }
    #[test]
    fn collapsed_whitespace_maps_keep_the_entire_far_seek_seam() {
        let source = format!("A{}B", " \r\n\t".repeat(20_000));
        let (text, maps) = normalized_text_maps(
            &source,
            std::slice::from_ref(&(0..source.len())),
            false,
            false,
        );
        assert_eq!(text, "A B");
        let seam = maps.iter().find(|m| m.raw.contains(&40_000)).unwrap();
        assert_eq!(seam.mapping, "normalized");
        assert_eq!(seam.raw, 1..source.len() - 1);
        assert_eq!(seam.rendered, 1..2);
        assert_eq!(seam.text, " ");
        assert_eq!(maps.len(), 3);
    }
    #[test]
    fn html_text_maps_bound_giant_identity_runs_and_decode_invalid_numeric_entities() {
        let source = format!("{}&#0;&#xD800;&#128;&notit;", "😀x".repeat(20_000));
        let (text, maps) = normalized_text_maps(
            &source,
            std::slice::from_ref(&(0..source.len())),
            false,
            false,
        );
        assert!(text.ends_with("\u{fffd}\u{fffd}€¬it;"));
        assert!(maps
            .iter()
            .all(|m| m.text.len() <= super::super::note_page_index::PIECE_BYTES));
        assert_eq!(
            maps.iter().map(|m| m.text.as_str()).collect::<String>(),
            text
        );
        for map in maps {
            assert!(source.is_char_boundary(map.raw.start));
            assert!(source.is_char_boundary(map.raw.end));
            assert_eq!(
                map.rendered.end - map.rendered.start,
                map.text.encode_utf16().count()
            );
        }
    }
    #[test]
    fn html_tree_repairs_implicit_rows_without_inventing_source_tags() {
        let source = "<table><tr><td>ONE<td>TWO<tr><td>THREE</table>";
        let root = parse(source);
        assert_eq!(elements(&root, "tbody").len(), 1);
        assert!(elements(&root, "tbody")[0].opening.is_none());
        assert_eq!(elements(&root, "tr").len(), 2);
        let cells = elements(&root, "td");
        assert_eq!(cells.len(), 3);
        for cell in &cells {
            assert_eq!(&source[cell.opening.clone().unwrap()], "<td>");
        }
        assert!(cells[0].closing.borrow().is_none());
        assert_eq!(
            &source[elements(&root, "table")[0]
                .closing
                .borrow()
                .clone()
                .unwrap()],
            "</table>"
        );
    }
    #[test]
    fn html_tree_foster_parenting_preserves_original_text_ranges() {
        let source = "<table>BEFORE<tr><td>A<table><tr><th>B</th></tr></table>C</td></tr></table>";
        let root = parse(source);
        assert_eq!(elements(&root, "table").len(), 2);
        let before = elements(&root, "#text")
            .into_iter()
            .find(|n| *n.text.borrow() == "BEFORE")
            .unwrap();
        assert_ne!(
            before
                .parent
                .borrow()
                .upgrade()
                .unwrap()
                .name
                .local
                .as_ref(),
            "table"
        );
        assert_eq!(&source[before.text_pieces.borrow()[0].clone()], "BEFORE");
    }
}
