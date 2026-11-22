//! In-source golden tables: a test that pins rows in a `const NAME: … = &[…];`
//! table rewrites them in place under `INTENTD_UPDATE_GOLDENS=1`, so a
//! deliberate change is one command and a reviewable diff.

use std::io::{Read as _, Seek as _, Write as _};
use std::path::Path;

pub const UPDATE_ENV: &str = "INTENTD_UPDATE_GOLDENS";

pub fn updating() -> bool {
    std::env::var_os(UPDATE_ENV).is_some_and(|value| value == "1")
}

/// One field of a table row.
pub enum Field<'a> {
    Str(&'a str),
    Count(usize),
}

/// `n` with `_` thousands separators, as the tables spell it.
pub fn grouped(n: usize) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push('_');
        }
        out.push(digit);
    }
    out
}

/// The table body in rustfmt's layout for tuples too long for one line,
/// which every row holding a SHA-256 is.
pub fn rows_source<'a>(rows: impl IntoIterator<Item = Vec<Field<'a>>>) -> String {
    let mut out = String::new();
    for row in rows {
        out.push_str("    (\n");
        for field in row {
            let text = match field {
                Field::Str(text) => format!("{text:?}"),
                Field::Count(n) => grouped(n),
            };
            out.push_str("        ");
            out.push_str(&text);
            out.push_str(",\n");
        }
        out.push_str("    ),\n");
    }
    out
}

/// `source` with the body of `const {name}: … = &[` … `];` replaced by `body`.
pub fn splice_table(source: &str, name: &str, body: &str) -> String {
    let declaration = format!("const {name}: ");
    let decl = source
        .find(&declaration)
        .unwrap_or_else(|| panic!("no `{declaration}` table"));
    let open = source[decl..]
        .find("= &[\n")
        .unwrap_or_else(|| panic!("`{name}` is not a multi-line `&[…]` table"))
        + decl
        + "= &[\n".len();
    let close = source[open - 1..]
        .find("\n];")
        .unwrap_or_else(|| panic!("`{name}` has no closing `];` line"))
        + open;
    format!("{}{body}{}", &source[..open], &source[close..])
}

/// Rewrites table `name` in `file` (relative to the crate root) with `body`.
/// The lock keeps tests in other processes that rewrite another table of the
/// same file from losing this update.
pub fn rewrite_table(file: &str, name: &str, body: &str) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(file);
    let mut handle = std::fs::File::options()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    handle.lock().expect("lock golden source");
    let mut source = String::new();
    handle
        .read_to_string(&mut source)
        .expect("read golden source");
    let updated = splice_table(&source, name, body);
    if updated != source {
        handle.set_len(0).expect("truncate golden source");
        handle.rewind().expect("rewind golden source");
        handle
            .write_all(updated.as_bytes())
            .expect("write golden source");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE: &str = "const GOLDEN_V2: &[(&str, usize)] = &[\n    (\n        \"b\",\n        2,\n    ),\n];\n\n/// Doc.\nconst GOLDEN: &[(&str, usize)] = &[\n    (\n        \"a\",\n        1,\n    ),\n];\n\nfn after() {}\n";

    #[test]
    fn splice_replaces_only_the_named_table() {
        let body = rows_source([vec![Field::Str("c\"d"), Field::Count(1_234_567)]]);
        let spliced = splice_table(SOURCE, "GOLDEN", &body);
        assert_eq!(
            spliced,
            SOURCE.replace(
                "        \"a\",\n        1,\n",
                "        \"c\\\"d\",\n        1_234_567,\n"
            )
        );
        assert_eq!(splice_table(&spliced, "GOLDEN", &body), spliced);
    }

    #[test]
    fn splice_can_empty_and_refill_a_table() {
        let emptied = splice_table(SOURCE, "GOLDEN_V2", "");
        assert!(emptied.starts_with("const GOLDEN_V2: &[(&str, usize)] = &[\n];\n"));
        let body = rows_source([vec![Field::Str("b"), Field::Count(2)]]);
        assert_eq!(splice_table(&emptied, "GOLDEN_V2", &body), SOURCE);
    }

    #[test]
    #[should_panic(expected = "no `const MISSING: ` table")]
    fn splice_names_a_missing_table() {
        splice_table(SOURCE, "MISSING", "");
    }

    #[test]
    fn grouped_spells_thousands() {
        assert_eq!(
            [0, 12, 999, 1_000, 448_018, 1_234_567].map(grouped),
            ["0", "12", "999", "1_000", "448_018", "1_234_567"]
        );
    }
}
