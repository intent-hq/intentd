//! Canonical native primitive parsing after the existing HTML sanitizer rules.
//! Values here are schema attrs.code, before renderer decoding, not raw source.
use super::Handle;

fn removed(tag: &str) -> bool {
    matches!(
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
    )
}

fn js_space(ch: char) -> bool {
    matches!(ch, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}

fn attribute(node: &Handle, name: &str) -> Option<String> {
    let attrs = node.attrs.borrow();
    let value = attrs
        .iter()
        .find(|a| a.name.local.as_ref() == name)?
        .value
        .trim_matches(js_space);
    // DOMPurify SAFE_FOR_XML attribute guard. This runs after HTML entity
    // decoding, just as in the actual sanitizer; arrow payloads can be removed.
    let lower = value.to_ascii_lowercase();
    if value.contains("-->")
        || value.contains("--!>")
        || value.contains("]>")
        || [
            "style", "script", "title", "xmp", "textarea", "noscript", "iframe", "noembed",
            "noframes",
        ]
        .iter()
        .any(|tag| lower.contains(&format!("</{tag}")))
    {
        return None;
    }
    Some(value.into())
}

fn code_element(root: &Handle, language: &str) -> Option<Handle> {
    let mut pending: Vec<_> = root.children.borrow().iter().rev().cloned().collect();
    while let Some(node) = pending.pop() {
        if removed(node.name.local.as_ref()) {
            continue;
        }
        if node.name.local.as_ref() == "code"
            && attribute(&node, "class").is_some_and(|class| {
                class
                    .split_ascii_whitespace()
                    .any(|value| value == language)
            })
        {
            return Some(node);
        }
        pending.extend(node.children.borrow().iter().rev().cloned());
    }
    None
}

fn text_content(root: &Handle) -> String {
    let mut result = String::new();
    let mut pending = vec![root.clone()];
    while let Some(node) = pending.pop() {
        if removed(node.name.local.as_ref()) {
            continue;
        }
        if node.name.local.as_ref() == "#text" {
            result.push_str(&node.text.borrow());
        } else {
            pending.extend(node.children.borrow().iter().rev().cloned());
        }
    }
    result
}

pub(super) fn parse(node: &Handle) -> Option<(&'static str, String)> {
    if node.name.local.as_ref() == "div" {
        return match attribute(node, "data-type").as_deref() {
            Some("diff-block") => Some((
                "diffBlock",
                attribute(node, "data-diff-code").unwrap_or_default(),
            )),
            Some("mermaid-block") => Some((
                "mermaidBlock",
                attribute(node, "data-mermaid-code").unwrap_or_default(),
            )),
            _ => None,
        };
    }
    if node.name.local.as_ref() == "pre" {
        for (class, kind) in [
            ("language-diff", "diffBlock"),
            ("language-mermaid", "mermaidBlock"),
        ] {
            if let Some(code) = code_element(node, class) {
                return Some((kind, text_content(&code)));
            }
        }
    }
    None
}
