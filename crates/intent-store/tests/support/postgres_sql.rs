//! SQL scanning shared by the Postgres lints: table references in a
//! statement and the query each one belongs to. Text-level, for the shapes
//! the store writes; not a SQL parser.

/// One table reference: the keyword, its byte offset, the table and its alias.
pub struct TableRef {
    pub keyword: String,
    pub at: usize,
    pub table: String,
    pub alias: String,
}

/// Words of `stmt` with their byte offsets (split on whitespace and commas).
fn words_with_offsets(stmt: &str) -> Vec<(usize, &str)> {
    let mut out = Vec::new();
    let mut start = None;
    for (i, c) in stmt.char_indices() {
        let sep = c.is_whitespace() || c == ',';
        match (sep, start) {
            (true, Some(s)) => {
                out.push((s, &stmt[s..i]));
                start = None;
            }
            (false, None) => start = Some(i),
            _ => {}
        }
    }
    if let Some(s) = start {
        out.push((s, &stmt[s..]));
    }
    out
}

fn ident(w: &str) -> String {
    w.trim_matches(|c: char| !(c.is_alphanumeric() || c == '_'))
        .to_lowercase()
}

pub fn table_refs(stmt: &str) -> Vec<TableRef> {
    const NOT_ALIAS: &[&str] = &[
        "where",
        "set",
        "on",
        "join",
        "left",
        "inner",
        "group",
        "order",
        "limit",
        "union",
        "values",
        "select",
        "returning",
        "default",
    ];
    let words = words_with_offsets(stmt);
    let mut refs = Vec::new();
    for (i, (at, w)) in words.iter().enumerate() {
        let keyword = w.to_uppercase();
        if !matches!(keyword.as_str(), "FROM" | "JOIN" | "UPDATE" | "INTO") {
            continue;
        }
        let Some((_, raw)) = words.get(i + 1) else {
            continue;
        };
        if keyword == "UPDATE" && i > 0 && words[i - 1].1.eq_ignore_ascii_case("DO") {
            continue; // ON CONFLICT DO UPDATE SET
        }
        if raw.contains(['(', ')', '.']) || raw.starts_with(|c: char| c.is_ascii_digit()) {
            // subquery, table function, `IS DISTINCT FROM old.x`, SUBSTRING(x FROM n)
            continue;
        }
        let table = ident(raw);
        if table.is_empty() {
            continue;
        }
        let next = words.get(i + 2).map(|(_, w)| w.to_lowercase());
        let alias = match next.as_deref() {
            Some("as") => words.get(i + 3).map_or(table.clone(), |(_, a)| ident(a)),
            Some(n)
                if !NOT_ALIAS.contains(&n)
                    && n.chars().all(|c| c.is_alphanumeric() || c == '_') =>
            {
                n.to_owned()
            }
            _ => table.clone(),
        };
        refs.push(TableRef {
            keyword,
            at: *at,
            table,
            alias,
        });
    }
    refs
}

/// Text of the query that holds byte `at`: the parenthesized group around it
/// (the whole statement at depth 0) with nested subqueries — groups that open
/// with `select` — removed. Plain grouping parentheses stay, so a filter must
/// sit in the same query as the table it scopes, not only in a subquery.
pub fn same_level(s: &str, at: usize) -> String {
    let bytes = s.as_bytes();
    let mut depth = Vec::with_capacity(bytes.len());
    let mut d = 0usize;
    for &b in bytes {
        match b {
            b'(' => {
                depth.push(d);
                d += 1;
            }
            b')' => {
                d = d.saturating_sub(1);
                depth.push(d);
            }
            _ => depth.push(d),
        }
    }
    let level = depth[at];
    let mut lo = at;
    while lo > 0 && depth[lo - 1] >= level {
        lo -= 1;
    }
    let mut hi = at;
    while hi < bytes.len() && depth[hi] >= level {
        hi += 1;
    }
    let mut out = String::new();
    let mut i = lo;
    while i < hi {
        let opens_subquery =
            bytes[i] == b'(' && depth[i] >= level && s[i + 1..].trim_start().starts_with("select");
        if opens_subquery {
            let inner = depth[i];
            i += 1;
            while i < hi && !(bytes[i] == b')' && depth[i] == inner) {
                i += 1;
            }
            i += 1;
            continue;
        }
        out.push(char::from(bytes[i]));
        i += 1;
    }
    out
}
