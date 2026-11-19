//! The SQL the Postgres backend runs, found by use rather than by text.
//!
//! A sink is the SQL argument of a sqlx call (`sqlx::query`, `query_as`,
//! `query_scalar`, their `_with` forms, `raw_sql`, `QueryBuilder::new`) or
//! the string of a `sqlx::query!`-style macro. Each sink's argument is
//! followed back to its text through references, `AssertSqlSafe`, `as_str`
//! and similar conversions, `if`/`match` branches, local bindings, `&str`
//! consts (`concat!` included), the arguments callers pass to a helper's
//! parameter, and the return values of a helper that builds the SQL. A
//! `format!` string is a template whose holes the caller resolves. An
//! argument the walk cannot follow (a struct field, a runtime fragment) is
//! [`Sql::Unresolved`], so it cannot pass unseen.
//!
//! Limit: text appended at runtime (`push_str`, a `format!` argument) is
//! not part of the statement checked here.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use quote::ToTokens;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{Expr, FnArg, Lit, Pat, Signature, Token, Type};

/// sqlx functions whose first argument is a statement.
const SINK_FNS: &[&str] = &[
    "query",
    "query_as",
    "query_scalar",
    "query_with",
    "query_as_with",
    "query_scalar_with",
    "raw_sql",
];

/// sqlx macros whose first string argument is a statement.
const SINK_MACROS: &[&str] = &[
    "query",
    "query_as",
    "query_scalar",
    "query_unchecked",
    "query_as_unchecked",
    "query_scalar_unchecked",
];

/// Conversions that keep the text: `sql.as_str()`, `sql.to_owned()`.
const PASS_THROUGH: &[&str] = &[
    "as_str",
    "as_ref",
    "as_deref",
    "clone",
    "into",
    "to_owned",
    "to_string",
];

const MAX_DEPTH: usize = 12;

pub fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The text a sink can run.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Sql {
    /// A literal, or a `format!` template when `template` is set. `args`
    /// holds the holes that resolve to exactly one text, by hole name
    /// (positional holes are `0`, `1`, ...).
    Text {
        text: String,
        template: bool,
        args: BTreeMap<String, Sql>,
    },
    /// An expression the walk cannot follow, as Rust tokens.
    Unresolved(String),
}

impl Sql {
    fn plain(text: String) -> Self {
        Self::Text {
            text,
            template: false,
            args: BTreeMap::new(),
        }
    }

    /// The statement as text pieces, with `None` for each unresolved hole.
    /// A resolved hole's text is spliced in, its own holes kept.
    pub fn pieces(&self) -> Vec<Option<String>> {
        let mut out = Vec::new();
        self.push_pieces(&mut out);
        out
    }

    fn push_pieces(&self, out: &mut Vec<Option<String>>) {
        match self {
            Self::Text {
                text,
                template: false,
                ..
            } => out.push(Some(text.clone())),
            Self::Text { text, args, .. } => {
                for piece in split_format(text) {
                    match piece {
                        Ok(t) => out.push(Some(t)),
                        Err(hole) => match args.get(&hole) {
                            Some(arg) => arg.push_pieces(out),
                            None => out.push(None),
                        },
                    }
                }
            }
            Self::Unresolved(_) => out.push(None),
        }
    }
}

/// A `format!` string as text (`Ok`, `{{` and `}}` undone) and holes
/// (`Err(name)`; the name before any `:`, positional holes numbered).
pub fn split_format(s: &str) -> Vec<Result<String, String>> {
    let mut out = Vec::new();
    let mut text = String::new();
    let mut positional = 0;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match (c, chars.peek()) {
            ('{', Some('{')) | ('}', Some('}')) => {
                chars.next();
                text.push(c);
            }
            ('{', _) => {
                let mut name = String::new();
                for n in chars.by_ref() {
                    if n == '}' {
                        break;
                    }
                    name.push(n);
                }
                let mut name = name.split(':').next().unwrap_or("").trim().to_owned();
                if name.is_empty() {
                    name = positional.to_string();
                    positional += 1;
                }
                out.push(Ok(std::mem::take(&mut text)));
                out.push(Err(name));
            }
            _ => text.push(c),
        }
    }
    out.push(Ok(text));
    out
}

/// One text that reaches a sink, where it is written.
#[derive(Clone, Debug)]
pub struct Source {
    pub file: PathBuf,
    pub line: usize,
    pub column: usize,
    pub sql: Sql,
}

/// One sink: the function that runs it and every text it can run.
pub struct Use {
    pub file: PathBuf,
    pub function: String,
    /// The function holds the tenant's writer lock: it calls
    /// `begin_write()` or holds a `WriteTx`, owned or `&mut`.
    pub locked: bool,
    pub sources: Vec<Source>,
}

/// What a local name is bound to.
#[derive(Clone)]
enum Binding {
    /// `let x = e` or `x = e`.
    Value(Expr),
    /// `for x in iter`, or the `path` field of a tuple pattern.
    Element { iter: Expr, path: Vec<usize> },
    /// Parameter `index` of the closure bound to `closure`.
    ClosureParam { closure: String, index: usize },
}

struct FnFacts {
    file: usize,
    name: String,
    has_self: bool,
    params: Vec<Option<String>>,
    locals: BTreeMap<String, Vec<Binding>>,
    returns: Vec<Expr>,
    locked: bool,
}

struct CallSite {
    name: String,
    caller: usize,
    method: bool,
    args: Vec<Expr>,
}

struct Sink {
    function: usize,
    arg: Expr,
}

/// Every function, call, `&str` const and sink of a set of Rust files.
#[derive(Default)]
pub struct Index {
    /// Functions that rewrite SQL text without adding a table: a call is
    /// followed into its first argument.
    rewriters: Vec<String>,
    files: Vec<PathBuf>,
    fns: Vec<FnFacts>,
    calls: Vec<CallSite>,
    consts: Vec<(usize, String, Expr)>,
    sinks: Vec<Sink>,
}

impl Index {
    /// Indexes the `(path, source)` pairs of `checked`, and only the `&str`
    /// consts of `consts_only`. Panics on a file syn cannot parse.
    pub fn new(checked: &[(PathBuf, String)], consts_only: &[(PathBuf, String)]) -> Self {
        let mut index = Self::default();
        let sets = [(checked, false), (consts_only, true)];
        for (path, text, consts_only) in sets
            .iter()
            .flat_map(|(set, only)| set.iter().map(move |(p, t)| (p, t, *only)))
        {
            let file = syn::parse_file(text).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            index.files.push(path.clone());
            let id = index.files.len() - 1;
            Items {
                index: &mut index,
                file: id,
                consts_only,
            }
            .visit_file(&file);
        }
        index
    }

    /// Follows calls to `name` into their first argument; see `rewriters`.
    #[must_use]
    pub fn with_rewriter(mut self, name: &str) -> Self {
        self.rewriters.push(name.to_owned());
        self
    }

    /// Every sink with the texts it can run.
    pub fn uses(&self) -> Vec<Use> {
        self.sinks
            .iter()
            .map(|s| {
                let f = &self.fns[s.function];
                let mut sources = Vec::new();
                let mut seen = BTreeSet::new();
                self.resolve(&s.arg, s.function, 0, &mut seen, &mut sources);
                let mut keys = BTreeSet::new();
                sources.retain(|src: &Source| {
                    keys.insert((src.file.clone(), src.line, src.column, src.sql.clone()))
                });
                Use {
                    file: self.files[f.file].clone(),
                    function: f.name.clone(),
                    locked: f.locked,
                    sources,
                }
            })
            .collect()
    }

    fn const_in(&self, file: usize, name: &str, depth: usize) -> Option<String> {
        if depth > MAX_DEPTH {
            return None;
        }
        let mut all = self.consts.iter().filter(|(_, n, _)| n == name);
        let local = self.consts.iter().find(|(f, n, _)| *f == file && n == name);
        let (f, _, value) = local.or_else(|| {
            let one = all.next()?;
            all.next().is_none().then_some(one)
        })?;
        self.const_value(*f, value, depth + 1)
    }

    fn const_value(&self, file: usize, e: &Expr, depth: usize) -> Option<String> {
        match e {
            Expr::Lit(l) => match &l.lit {
                Lit::Str(s) => Some(s.value()),
                _ => None,
            },
            Expr::Path(p) => {
                let name = p.path.segments.last()?.ident.to_string();
                self.const_in(file, &name, depth)
            }
            Expr::Macro(m) if last_segment(&m.mac.path) == "concat" => {
                let args = m
                    .mac
                    .parse_body_with(Punctuated::<Expr, Token![,]>::parse_terminated)
                    .ok()?;
                args.iter()
                    .map(|a| self.const_value(file, a, depth + 1))
                    .collect()
            }
            Expr::Group(g) => self.const_value(file, &g.expr, depth),
            Expr::Paren(p) => self.const_value(file, &p.expr, depth),
            _ => None,
        }
    }

    fn resolve(
        &self,
        e: &Expr,
        at: usize,
        depth: usize,
        seen: &mut BTreeSet<(usize, String)>,
        out: &mut Vec<Source>,
    ) {
        let file = self.fns[at].file;
        let unresolved = |out: &mut Vec<Source>, e: &Expr| {
            out.push(self.source(file, e, Sql::Unresolved(tokens(e))));
        };
        if depth > MAX_DEPTH {
            return unresolved(out, e);
        }
        let next = depth + 1;
        match e {
            Expr::Lit(l) => match &l.lit {
                Lit::Str(s) => out.push(self.source(file, e, Sql::plain(s.value()))),
                _ => unresolved(out, e),
            },
            Expr::Macro(m) => {
                let name = last_segment(&m.mac.path);
                let args = m
                    .mac
                    .parse_body_with(Punctuated::<Expr, Token![,]>::parse_terminated);
                match (name.as_str(), args) {
                    ("format", Ok(args)) => match args.first() {
                        Some(
                            lit @ Expr::Lit(syn::ExprLit {
                                lit: Lit::Str(s), ..
                            }),
                        ) => {
                            let text = s.value();
                            let args = self.format_args(&text, &args, at, next, seen);
                            out.push(self.source(
                                file,
                                lit,
                                Sql::Text {
                                    text,
                                    template: true,
                                    args,
                                },
                            ));
                        }
                        _ => unresolved(out, e),
                    },
                    ("concat", _) => match self.const_value(file, e, 0) {
                        Some(text) => out.push(self.source(file, e, Sql::plain(text))),
                        None => unresolved(out, e),
                    },
                    _ => unresolved(out, e),
                }
            }
            Expr::Reference(r) => self.resolve(&r.expr, at, next, seen, out),
            Expr::Paren(p) => self.resolve(&p.expr, at, next, seen, out),
            Expr::Group(g) => self.resolve(&g.expr, at, next, seen, out),
            Expr::Unary(u) if matches!(u.op, syn::UnOp::Deref(_)) => {
                self.resolve(&u.expr, at, next, seen, out);
            }
            Expr::MethodCall(m)
                if m.args.is_empty() && PASS_THROUGH.contains(&m.method.to_string().as_str()) =>
            {
                self.resolve(&m.receiver, at, next, seen, out);
            }
            // `self.build_sql(..)`: what that method returns.
            Expr::MethodCall(m) if is_self(&m.receiver) => {
                let name = m.method.to_string();
                if !self.resolve_returns(file, &name, next, seen, out) {
                    unresolved(out, e);
                }
            }
            Expr::Call(c) => {
                let Expr::Path(p) = &*c.func else {
                    return unresolved(out, e);
                };
                let name = last_segment(&p.path);
                let segs = &p.path.segments;
                let type_path = segs.len() > 1
                    && segs[segs.len() - 2]
                        .ident
                        .to_string()
                        .starts_with(|ch: char| ch.is_ascii_uppercase())
                    && segs[segs.len() - 2].ident != "Self";
                if (name == "AssertSqlSafe" || self.rewriters.contains(&name)) && !c.args.is_empty()
                {
                    self.resolve(&c.args[0], at, next, seen, out);
                } else if type_path || !self.resolve_returns(file, &name, next, seen, out) {
                    unresolved(out, e);
                }
            }
            Expr::Block(b) => match tail(&b.block) {
                Some(t) => self.resolve(t, at, next, seen, out),
                None => unresolved(out, e),
            },
            Expr::If(i) => {
                match tail(&i.then_branch) {
                    Some(t) => self.resolve(t, at, next, seen, out),
                    None => unresolved(out, e),
                }
                match &i.else_branch {
                    Some((_, els)) => self.resolve(els, at, next, seen, out),
                    None => unresolved(out, e),
                }
            }
            Expr::Match(m) => {
                for arm in &m.arms {
                    self.resolve(&arm.body, at, next, seen, out);
                }
            }
            Expr::Path(p) if p.path.segments.len() == 1 => {
                let name = p.path.segments[0].ident.to_string();
                if !seen.insert((at, name.clone())) {
                    return;
                }
                let f = &self.fns[at];
                if let Some(bindings) = f.locals.get(&name) {
                    for b in bindings {
                        self.resolve_binding(b, at, next, seen, out);
                    }
                } else if let Some(i) = f.params.iter().position(|p| p.as_deref() == Some(&name)) {
                    if !self.resolve_callers(at, i, next, seen, out) {
                        unresolved(out, e);
                    }
                } else if let Some(text) = self.const_in(file, &name, 0) {
                    out.push(self.source(file, e, Sql::plain(text)));
                } else {
                    unresolved(out, e);
                }
            }
            Expr::Path(p) => match self.const_in(file, &last_segment(&p.path), 0) {
                Some(text) => out.push(self.source(file, e, Sql::plain(text))),
                None => unresolved(out, e),
            },
            _ => unresolved(out, e),
        }
    }

    /// The holes of a `format!` string that resolve to exactly one text:
    /// positional and named arguments, and captured names.
    fn format_args(
        &self,
        text: &str,
        args: &Punctuated<Expr, Token![,]>,
        at: usize,
        depth: usize,
        seen: &BTreeSet<(usize, String)>,
    ) -> BTreeMap<String, Sql> {
        let mut by_name: BTreeMap<String, Expr> = BTreeMap::new();
        let mut positional = 0;
        for a in args.iter().skip(1) {
            match a {
                Expr::Assign(syn::ExprAssign { left, right, .. }) if matches!(&**left, Expr::Path(p) if p.path.get_ident().is_some()) =>
                {
                    by_name.insert(tokens(left), (**right).clone());
                }
                other => {
                    by_name.insert(positional.to_string(), other.clone());
                    positional += 1;
                }
            }
        }
        let mut out = BTreeMap::new();
        for hole in split_format(text).into_iter().filter_map(Result::err) {
            if out.contains_key(&hole) {
                continue;
            }
            let expr = match by_name.get(&hole) {
                Some(e) => e.clone(),
                None => match syn::parse_str::<Expr>(&hole) {
                    Ok(e) => e,
                    Err(_) => continue,
                },
            };
            let mut found = Vec::new();
            self.resolve(&expr, at, depth, &mut seen.clone(), &mut found);
            if let [one] = found.as_slice() {
                if matches!(one.sql, Sql::Text { .. }) {
                    out.insert(hole, one.sql.clone());
                }
            }
        }
        out
    }

    fn resolve_binding(
        &self,
        b: &Binding,
        at: usize,
        depth: usize,
        seen: &mut BTreeSet<(usize, String)>,
        out: &mut Vec<Source>,
    ) {
        match b {
            Binding::Value(e) => self.resolve(e, at, depth, seen, out),
            Binding::Element { iter, path } => {
                let Some(items) = elements(iter) else {
                    let file = self.fns[at].file;
                    return out.push(self.source(file, iter, Sql::Unresolved(tokens(iter))));
                };
                for item in items {
                    let mut e = item;
                    for &k in path {
                        e = match strip(e) {
                            Expr::Tuple(t) if k < t.elems.len() => &t.elems[k],
                            _ => break,
                        };
                    }
                    self.resolve(e, at, depth, seen, out);
                }
            }
            Binding::ClosureParam { closure, index } => {
                let calls = self
                    .calls
                    .iter()
                    .filter(|c| c.caller == at && !c.method && &c.name == closure);
                for c in calls {
                    if let Some(arg) = c.args.get(*index) {
                        self.resolve(arg, at, depth, seen, out);
                    }
                }
            }
        }
    }

    /// The functions named `name`: those in `file`, else all of them.
    fn fns_named(&self, file: usize, name: &str) -> Vec<usize> {
        let all: Vec<usize> = (0..self.fns.len())
            .filter(|&i| self.fns[i].name == name)
            .collect();
        let local: Vec<usize> = all
            .iter()
            .copied()
            .filter(|&i| self.fns[i].file == file)
            .collect();
        if local.is_empty() {
            all
        } else {
            local
        }
    }

    /// Resolves what each function named `name` returns; false if none.
    fn resolve_returns(
        &self,
        file: usize,
        name: &str,
        depth: usize,
        seen: &mut BTreeSet<(usize, String)>,
        out: &mut Vec<Source>,
    ) -> bool {
        let fns = self.fns_named(file, name);
        for &f in &fns {
            if !seen.insert((f, "<return>".to_owned())) {
                continue;
            }
            for r in &self.fns[f].returns {
                self.resolve(r, f, depth, seen, out);
            }
        }
        !fns.is_empty()
    }

    /// Resolves the argument every caller passes to parameter `i` of `f`;
    /// false if nothing calls it.
    fn resolve_callers(
        &self,
        f: usize,
        i: usize,
        depth: usize,
        seen: &mut BTreeSet<(usize, String)>,
        out: &mut Vec<Source>,
    ) -> bool {
        let callee = &self.fns[f];
        let mut any = false;
        for c in self.calls.iter().filter(|c| c.name == callee.name) {
            let at = if !c.method && callee.has_self {
                i + 1
            } else {
                i
            };
            if let Some(arg) = c.args.get(at) {
                any = true;
                self.resolve(arg, c.caller, depth, seen, out);
            }
        }
        any
    }

    fn source(&self, file: usize, e: &Expr, sql: Sql) -> Source {
        let start = e.span().start();
        Source {
            file: self.files[file].clone(),
            line: start.line,
            column: start.column,
            sql,
        }
    }
}

/// The items of a literal array iterated by a `for` loop
/// (`[a, b]`, `&[a, b]`, `[a, b].iter()`).
fn elements(e: &Expr) -> Option<&Punctuated<Expr, Token![,]>> {
    match strip(e) {
        Expr::Array(a) => Some(&a.elems),
        Expr::MethodCall(m)
            if m.args.is_empty()
                && ["iter", "into_iter", "copied", "cloned"]
                    .contains(&m.method.to_string().as_str()) =>
        {
            elements(&m.receiver)
        }
        _ => None,
    }
}

/// `e` without references, parentheses and groups.
fn strip(e: &Expr) -> &Expr {
    match e {
        Expr::Reference(r) => strip(&r.expr),
        Expr::Paren(p) => strip(&p.expr),
        Expr::Group(g) => strip(&g.expr),
        other => other,
    }
}

fn tokens(e: &Expr) -> String {
    e.to_token_stream().to_string()
}

fn last_segment(p: &syn::Path) -> String {
    p.segments
        .last()
        .map(|s| s.ident.to_string())
        .unwrap_or_default()
}

fn is_self(e: &Expr) -> bool {
    matches!(e, Expr::Path(p) if p.path.is_ident("self"))
}

fn tail(b: &syn::Block) -> Option<&Expr> {
    match b.stmts.last() {
        Some(syn::Stmt::Expr(e, None)) => Some(e),
        _ => None,
    }
}

/// `WriteTx`, owned or `&mut`, under any path.
fn is_write_tx(ty: &Type) -> bool {
    match ty {
        Type::Path(p) => last_segment(&p.path) == "WriteTx",
        Type::Reference(r) => r.mutability.is_some() && is_write_tx(&r.elem),
        Type::Paren(p) => is_write_tx(&p.elem),
        _ => false,
    }
}

/// The bound names of a `let` pattern that binds one name.
fn pat_ident(p: &Pat) -> Option<String> {
    match p {
        Pat::Ident(i) => Some(i.ident.to_string()),
        Pat::Type(t) => pat_ident(&t.pat),
        _ => None,
    }
}

/// Finds functions and consts at any depth.
struct Items<'i> {
    index: &'i mut Index,
    file: usize,
    consts_only: bool,
}

impl Items<'_> {
    fn add_fn(&mut self, sig: &Signature, block: &syn::Block) {
        if self.consts_only {
            return;
        }
        let mut facts = FnFacts {
            file: self.file,
            name: sig.ident.to_string(),
            has_self: false,
            params: Vec::new(),
            locals: BTreeMap::new(),
            returns: Vec::new(),
            locked: false,
        };
        for input in &sig.inputs {
            match input {
                FnArg::Receiver(_) => facts.has_self = true,
                FnArg::Typed(t) => {
                    facts.locked |= is_write_tx(&t.ty);
                    facts.params.push(pat_ident(&t.pat));
                }
            }
        }
        facts.returns.extend(tail(block).cloned());
        let id = self.index.fns.len();
        self.index.fns.push(facts);
        let mut body = Body {
            index: self.index,
            id,
        };
        body.visit_block(block);
    }
}

impl<'ast> Visit<'ast> for Items<'_> {
    fn visit_item_fn(&mut self, i: &'ast syn::ItemFn) {
        self.add_fn(&i.sig, &i.block);
        visit::visit_item_fn(self, i);
    }

    fn visit_impl_item_fn(&mut self, i: &'ast syn::ImplItemFn) {
        self.add_fn(&i.sig, &i.block);
        visit::visit_impl_item_fn(self, i);
    }

    fn visit_trait_item_fn(&mut self, i: &'ast syn::TraitItemFn) {
        if let Some(block) = &i.default {
            self.add_fn(&i.sig, block);
        }
        visit::visit_trait_item_fn(self, i);
    }

    fn visit_item_const(&mut self, i: &'ast syn::ItemConst) {
        self.index
            .consts
            .push((self.file, i.ident.to_string(), (*i.expr).clone()));
        visit::visit_item_const(self, i);
    }

    fn visit_impl_item_const(&mut self, i: &'ast syn::ImplItemConst) {
        self.index
            .consts
            .push((self.file, i.ident.to_string(), i.expr.clone()));
        visit::visit_impl_item_const(self, i);
    }
}

/// Walks one function body; nested items are their own functions.
struct Body<'i> {
    index: &'i mut Index,
    id: usize,
}

impl Body<'_> {
    fn facts(&mut self) -> &mut FnFacts {
        &mut self.index.fns[self.id]
    }
}

impl<'ast> Visit<'ast> for Body<'_> {
    fn visit_item(&mut self, _: &'ast syn::Item) {}

    fn visit_local(&mut self, l: &'ast syn::Local) {
        if let Pat::Type(t) = &l.pat {
            self.facts().locked |= is_write_tx(&t.ty);
        }
        if let (Some(name), Some(init)) = (pat_ident(&l.pat), &l.init) {
            if let Expr::Closure(c) = &*init.expr {
                for (index, input) in c.inputs.iter().enumerate() {
                    if let Some(param) = pat_ident(input) {
                        let b = Binding::ClosureParam {
                            closure: name.clone(),
                            index,
                        };
                        self.facts().locals.entry(param).or_default().push(b);
                    }
                }
            }
            let b = Binding::Value((*init.expr).clone());
            self.facts().locals.entry(name).or_default().push(b);
        }
        visit::visit_local(self, l);
    }

    fn visit_expr_for_loop(&mut self, f: &'ast syn::ExprForLoop) {
        let mut bind = |name: String, path: Vec<usize>| {
            let b = Binding::Element {
                iter: (*f.expr).clone(),
                path,
            };
            self.facts().locals.entry(name).or_default().push(b);
        };
        match &*f.pat {
            Pat::Tuple(t) => {
                for (k, p) in t.elems.iter().enumerate() {
                    if let Some(name) = pat_ident(p) {
                        bind(name, vec![k]);
                    }
                }
            }
            p => {
                if let Some(name) = pat_ident(p) {
                    bind(name, Vec::new());
                }
            }
        }
        visit::visit_expr_for_loop(self, f);
    }

    fn visit_expr_assign(&mut self, a: &'ast syn::ExprAssign) {
        if let Expr::Path(p) = &*a.left {
            if let Some(name) = p.path.get_ident() {
                let right = Binding::Value((*a.right).clone());
                self.facts()
                    .locals
                    .entry(name.to_string())
                    .or_default()
                    .push(right);
            }
        }
        visit::visit_expr_assign(self, a);
    }

    fn visit_expr_return(&mut self, r: &'ast syn::ExprReturn) {
        if let Some(e) = &r.expr {
            let e = (**e).clone();
            self.facts().returns.push(e);
        }
        visit::visit_expr_return(self, r);
    }

    fn visit_expr_method_call(&mut self, m: &'ast syn::ExprMethodCall) {
        let name = m.method.to_string();
        if name == "begin_write" {
            self.facts().locked = true;
        }
        let caller = self.id;
        self.index.calls.push(CallSite {
            name,
            caller,
            method: true,
            args: m.args.iter().cloned().collect(),
        });
        visit::visit_expr_method_call(self, m);
    }

    fn visit_expr_call(&mut self, c: &'ast syn::ExprCall) {
        if let Expr::Path(p) = &*c.func {
            let segs: Vec<String> = p
                .path
                .segments
                .iter()
                .map(|s| s.ident.to_string())
                .collect();
            let name = segs.last().cloned().unwrap_or_default();
            let sqlx = segs.len() == 1 || segs.iter().any(|s| s == "sqlx");
            let sink = (sqlx && SINK_FNS.contains(&name.as_str()))
                || (name == "new" && segs.iter().any(|s| s == "QueryBuilder"));
            if sink {
                if let Some(arg) = c.args.first() {
                    self.index.sinks.push(Sink {
                        function: self.id,
                        arg: arg.clone(),
                    });
                }
            } else {
                let caller = self.id;
                self.index.calls.push(CallSite {
                    name,
                    caller,
                    method: false,
                    args: c.args.iter().cloned().collect(),
                });
            }
        }
        visit::visit_expr_call(self, c);
    }

    /// Macro bodies are opaque to syn: a sqlx macro is a sink, and any
    /// macro whose body parses as expressions (`format!`, `vec!`) is
    /// walked for the calls inside it.
    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        let Ok(args) = m.parse_body_with(Punctuated::<Expr, Token![,]>::parse_terminated) else {
            return;
        };
        let segs: Vec<String> = m
            .path
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect();
        let name = segs.last().cloned().unwrap_or_default();
        if segs.iter().any(|s| s == "sqlx") && SINK_MACROS.contains(&name.as_str()) {
            let sql = args
                .iter()
                .find(|a| matches!(a, Expr::Lit(l) if matches!(l.lit, Lit::Str(_))));
            if let Some(sql) = sql {
                self.index.sinks.push(Sink {
                    function: self.id,
                    arg: sql.clone(),
                });
            }
        }
        for a in &args {
            // `format!("{x}", x = expr)`: the value, not a binding.
            match a {
                Expr::Assign(named) => self.visit_expr(&named.right),
                other => self.visit_expr(other),
            }
        }
    }
}
