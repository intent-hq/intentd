//! The SQL the Postgres backend runs, found by use rather than by text.
//!
//! A sink is where sqlx receives a statement:
//! - the first argument of `sqlx::query`, `query_as`, `query_scalar`, their
//!   `_with` forms, `raw_sql`, `QueryBuilder::new` and
//!   `QueryBuilder::with_arguments`, also under a `use ... as` alias;
//! - the string of a `query!`-style macro, with or without `sqlx::`;
//! - the string given to an executor method (`execute`, `fetch*`,
//!   `prepare`, `describe`) in method or `sqlx::Executor::` path form.
//!
//! Each sink's argument is followed back to its text through references,
//! `AssertSqlSafe`, `as_str` and similar conversions, `if`/`match`
//! branches, local bindings (also tuple and `for` patterns), `&str` consts
//! (`concat!` included), closure and helper parameters (every caller),
//! helper return values (`other::f()` in module `other`), and text a bound
//! name gains through `push_str`, `push`, `+=` or `write!`. A `format!`
//! string is a template: each hole keeps every alternative it resolves to,
//! and an integer hole reads as `1`.
//!
//! Fails closed: a sink argument or hole the walk cannot follow is
//! [`Sql::Unresolved`], and so is a sink that yields no text, a sink
//! function passed by value, and a macro (or `macro_rules!` body) that
//! names sqlx where its body cannot be read.
//!
//! The writer lock: [`Use::lock_problem`] is set unless every executor the
//! sink runs on derives from a `WriteTx` (`&mut *tx`, `tx.as_mut()`, a
//! helper parameter every caller fills that way). A sink inside a spawned
//! task never counts as locked.
//!
//! Limits: a sink whose executor is not visible (the query value leaves the
//! function) falls back to "the function holds a `WriteTx` or called
//! `begin_write()` before the sink". Combinations of alternatives are
//! capped at [`MAX_EXPANSIONS`] per statement. Text a struct field gains
//! (`self.sql.push_str`) is not followed: a hole naming the field is
//! unresolved.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use proc_macro2::{Delimiter, TokenStream, TokenTree};
use quote::ToTokens;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::{self, Visit};
use syn::{BinOp, Expr, FnArg, Lit, Pat, Signature, Token, Type};

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

/// `sqlx::Executor` methods; on a query value their argument is the
/// executor, on an executor it is the statement.
const EXEC_METHODS: &[&str] = &[
    "execute",
    "execute_many",
    "fetch",
    "fetch_all",
    "fetch_many",
    "fetch_one",
    "fetch_optional",
    "prepare",
    "prepare_with",
    "describe",
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

/// Conversions that keep an executor: `tx.as_mut()`.
const EXECUTOR_PASS_THROUGH: &[&str] = &["as_mut", "conn", "deref_mut", "borrow_mut"];

const SPAWNS: &[&str] = &["spawn", "spawn_blocking", "spawn_local"];

/// Calls that hand back the transaction an argument opens
/// (`timeout(d, self.begin())`).
const BEGIN_PASS_THROUGH: &[&str] = &["timeout"];

/// Methods and fields that name a connection pool (`self.pool()`,
/// `self.read_pool`), also through `.clone()`.
const POOL_METHODS: &[&str] = &["pool", "read_pool", "write_pool"];

/// Methods that end a transaction where it is bound.
const TX_END_METHODS: &[&str] = &["commit", "rollback"];

const INTEGER_TYPES: &[&str] = &[
    "i8", "i16", "i32", "i64", "i128", "isize", "u8", "u16", "u32", "u64", "u128", "usize",
];

const MAX_DEPTH: usize = 48;

/// The most combinations of hole alternatives checked per statement.
pub const MAX_EXPANSIONS: usize = 256;

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

/// A line (1-based) and column in a source file.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Pos {
    pub line: usize,
    pub column: usize,
}

impl From<proc_macro2::LineColumn> for Pos {
    fn from(l: proc_macro2::LineColumn) -> Self {
        Self {
            line: l.line,
            column: l.column,
        }
    }
}

/// A text a sink can run, where it is written.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Source {
    pub file: PathBuf,
    /// The function the text or expression is written in; empty for a
    /// const.
    pub function: String,
    pub start: Pos,
    pub end: Pos,
    pub sql: Sql,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Sql {
    /// A literal, or a `format!` template when `template` is set. `args`
    /// holds every alternative of each hole by name (positional holes are
    /// `0`, `1`, ...); `appends` the text appended later, each with the
    /// branch it is appended in.
    Text {
        text: String,
        template: bool,
        args: BTreeMap<String, Vec<Source>>,
        appends: Vec<Append>,
    },
    /// An expression the walk cannot follow, as Rust tokens.
    Unresolved(String),
}

/// Which arm of an `if`, `match` or loop an append runs in:
/// `(construct, arm, arms)`. A loop's arm 0 runs its body, arm 1 skips it;
/// an `if` without `else` has an empty arm 1.
pub type Guard = Vec<(usize, usize, usize)>;

/// Text appended to a bound name, in the branch `guard`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Append {
    pub guard: Guard,
    pub alts: Vec<Source>,
}

impl Sql {
    fn plain(text: String) -> Self {
        Self::Text {
            text,
            template: false,
            args: BTreeMap::new(),
            appends: Vec::new(),
        }
    }
}

/// An expression the walk cannot follow, where it is written.
#[derive(Clone, Debug)]
pub struct Unfollowed {
    pub file: PathBuf,
    pub function: String,
    pub start: Pos,
    pub expr: String,
}

/// One way a source reads at run time: text pieces with `None` for an
/// unresolved part, and those parts.
#[derive(Clone, Debug, Default)]
pub struct Expansion {
    pub pieces: Vec<Option<String>>,
    pub unresolved: Vec<Unfollowed>,
    /// When the statement has more than [`MAX_EXPANSIONS`] combinations
    /// and its parts were varied one at a time: how many there were.
    pub capped: usize,
}

impl Expansion {
    fn join(&self, other: &Self) -> Self {
        let mut out = self.clone();
        out.pieces.extend(other.pieces.iter().cloned());
        out.unresolved.extend(other.unresolved.iter().cloned());
        out.capped = out.capped.max(other.capped);
        out
    }
}

impl Source {
    /// The text, or the Rust expression of an unresolved source.
    pub fn text(&self) -> &str {
        match &self.sql {
            Sql::Text { text, .. } | Sql::Unresolved(text) => text,
        }
    }

    /// The ways this source reads: every combination of hole and append
    /// alternatives while there are at most [`MAX_EXPANSIONS`]; past that,
    /// each hole and append varies alone with the others at their first
    /// alternative, so every alternative is still seen.
    pub fn expansions(&self) -> Vec<Expansion> {
        let (text, template, args, appends) = match &self.sql {
            Sql::Unresolved(expr) => {
                return vec![Expansion {
                    pieces: vec![None],
                    unresolved: vec![Unfollowed {
                        file: self.file.clone(),
                        function: self.function.clone(),
                        start: self.start,
                        expr: expr.clone(),
                    }],
                    capped: 0,
                }];
            }
            Sql::Text {
                text,
                template,
                args,
                appends,
            } => (text, *template, args, appends),
        };
        let fixed = |t: String| {
            vec![Expansion {
                pieces: vec![Some(t)],
                unresolved: Vec::new(),
                capped: 0,
            }]
        };
        let mut head: Vec<Vec<Expansion>> = Vec::new();
        let slots = &mut head;
        if template {
            for piece in split_format(text) {
                match piece {
                    Ok(t) => slots.push(fixed(t)),
                    Err(hole) => {
                        let missing = [Source {
                            sql: Sql::Unresolved(hole.clone()),
                            ..self.clone()
                        }];
                        let alts = args.get(&hole).map_or(&missing[..], Vec::as_slice);
                        slots.push(alternatives(alts));
                    }
                }
            }
        } else {
            slots.push(fixed(text.clone()));
        }
        let mut out = Vec::new();
        let (choices, capped) = branch_choices(appends);
        for choice in choices {
            let mut slots = head.clone();
            for a in appends {
                if a.guard
                    .iter()
                    .all(|(id, arm, _)| choice.get(id) == Some(arm))
                {
                    slots.push(alternatives(&a.alts));
                }
            }
            out.extend(combine(&slots).into_iter().map(|mut e| {
                e.capped = e.capped.max(capped);
                e
            }));
        }
        out
    }

    /// This source and every source inside it (holes, appends).
    pub fn walk<'a>(&'a self, out: &mut Vec<&'a Source>) {
        out.push(self);
        if let Sql::Text { args, appends, .. } = &self.sql {
            for s in args.values().flatten() {
                s.walk(out);
            }
            for s in appends.iter().flat_map(|a| &a.alts) {
                s.walk(out);
            }
        }
    }
}

/// One arm per branch construct the appends sit in: every combination
/// while there are at most [`MAX_EXPANSIONS`], else each construct varied
/// alone with the others at arm 0.
fn branch_choices(appends: &[Append]) -> (Vec<BTreeMap<usize, usize>>, usize) {
    let arms: BTreeMap<usize, usize> = appends
        .iter()
        .flat_map(|a| a.guard.iter().map(|(id, _, n)| (*id, *n)))
        .collect();
    let total = arms
        .values()
        .try_fold(1usize, |t, n| t.checked_mul(*n))
        .unwrap_or(usize::MAX);
    let base: BTreeMap<usize, usize> = arms.keys().map(|id| (*id, 0)).collect();
    if total <= MAX_EXPANSIONS {
        let mut out = vec![base];
        for (id, n) in &arms {
            out = out
                .into_iter()
                .flat_map(|c| {
                    (0..*n).map(move |k| {
                        let mut c = c.clone();
                        c.insert(*id, k);
                        c
                    })
                })
                .collect();
        }
        return (out, 0);
    }
    let mut out = vec![base.clone()];
    for (id, n) in &arms {
        for k in 1..*n {
            let mut c = base.clone();
            c.insert(*id, k);
            out.push(c);
        }
    }
    (out, total)
}

/// The expansions of each alternative; an empty set is unresolved.
fn alternatives(alts: &[Source]) -> Vec<Expansion> {
    let out: Vec<Expansion> = alts.iter().flat_map(Source::expansions).collect();
    if out.is_empty() {
        return vec![Expansion {
            pieces: vec![None],
            unresolved: vec![Unfollowed {
                file: PathBuf::new(),
                function: String::new(),
                start: Pos { line: 0, column: 0 },
                expr: "<no text>".to_owned(),
            }],
            capped: 0,
        }];
    }
    out
}

/// Joins one expansion per slot: every combination while there are at
/// most [`MAX_EXPANSIONS`], else each slot varied alone.
fn combine(slots: &[Vec<Expansion>]) -> Vec<Expansion> {
    let total = slots
        .iter()
        .try_fold(1usize, |n, s| n.checked_mul(s.len()))
        .unwrap_or(usize::MAX);
    let join = |pick: &[usize]| {
        slots
            .iter()
            .zip(pick)
            .fold(Expansion::default(), |acc, (s, &k)| acc.join(&s[k]))
    };
    if total <= MAX_EXPANSIONS {
        let mut out = vec![Expansion::default()];
        for s in slots {
            out = out
                .iter()
                .flat_map(|a| s.iter().map(move |e| a.join(e)))
                .collect();
        }
        return out;
    }
    let base = vec![0; slots.len()];
    let mut out = vec![join(&base)];
    for (i, s) in slots.iter().enumerate() {
        for k in 1..s.len() {
            let mut pick = base.clone();
            pick[i] = k;
            out.push(join(&pick));
        }
    }
    for e in &mut out {
        e.capped = e.capped.max(total);
    }
    out
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

/// A string literal no sink reaches.
pub struct Unreached {
    pub file: PathBuf,
    pub function: String,
    pub start: Pos,
    pub value: String,
}

/// Why a sink does not run under the tenant's writer lock.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockProblem {
    /// The executor is not visible and no lock is held before the sink.
    Hidden,
    /// The sink runs on this executor, which is not the `WriteTx`.
    Executor(String),
    /// The sink runs in a spawned task.
    Spawned,
}

impl std::fmt::Display for LockProblem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hidden => write!(f, "has no WriteTx or begin_write() before it"),
            Self::Executor(e) => write!(f, "runs on `{e}`, not the WriteTx"),
            Self::Spawned => write!(f, "runs in a spawned task"),
        }
    }
}

/// One sink: where it runs and every text it can run.
pub struct Use {
    pub file: PathBuf,
    pub function: String,
    pub line: usize,
    pub sources: Vec<Source>,
    /// Why the sink does not run under the tenant's writer lock, if so.
    /// Only a write needs the lock; the caller decides that.
    pub lock_problem: Option<LockProblem>,
    /// The function takes the writer lock somewhere: a `WriteTx`
    /// parameter or binding, or a `begin_write()` call.
    pub takes_lock: bool,
}

/// What runs a read's statements outside one snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TxKind {
    /// A plain `begin()` transaction (READ COMMITTED).
    Begin,
    /// A connection checked out with `acquire()`: each statement commits
    /// on its own.
    Connection,
    /// The pool: each statement on whatever connection it checks out.
    Pool,
}

/// A plain `begin()` transaction, acquired connection or pool that runs
/// more than one sink, or one sink in a loop.
pub struct PlainTx {
    pub file: PathBuf,
    /// The function that opens it (for the pool, that names it).
    pub function: String,
    /// Where it is bound (for the pool, the function's start).
    pub line: usize,
    pub kind: TxKind,
    /// `function:line` of each sink it runs, `(in a loop)` when it runs
    /// more than once.
    pub sinks: Vec<String>,
}

/// A plain `begin()` transaction the lint cannot count statements for.
pub struct Escape {
    pub file: PathBuf,
    pub function: String,
    pub line: usize,
    /// What the function does with it.
    pub what: String,
}

/// A source span, as `(start, end)`.
type Span = (Pos, Pos);

/// Where an executor comes from, as `(kind, function, binding position)`.
type Origin = (TxKind, usize, Pos);

/// The call sites between an opener and a sink, as `(caller, call)`,
/// outermost first.
type Via = Vec<(usize, Pos)>;

/// An executor use [`Index::tx_origins`] follows back to its origin.
struct Reach {
    /// The function it is used in, and where (a sink or a call site).
    at: usize,
    used_at: Pos,
    /// It runs more than once in a function between.
    repeated: bool,
    via: Via,
}

/// A binding of a local name, where it is made. A `let` (or `for`,
/// closure parameter) shadows earlier bindings; an assignment adds one.
#[derive(Clone)]
struct Local {
    pos: Pos,
    is_let: bool,
    /// The binding holds a `WriteTx`: a typed `WriteTx` binding, or a value
    /// whose chain root is `begin_write()` in every branch.
    tx: bool,
    binding: Binding,
    /// The block the binding is made in: it is not seen outside it.
    scope: (Pos, Pos),
}

/// What a local name is bound to.
#[derive(Clone)]
enum Binding {
    /// `let x = e` or `x = e`.
    Value(Expr),
    /// Field `path` of the tuple `init` (`let (a, b) = init`).
    Field { init: Expr, path: Vec<usize> },
    /// `for x in iter`, or the `path` field of a tuple pattern.
    Element { iter: Expr, path: Vec<usize> },
    /// Parameter `index` of the closure bound to `closure`.
    ClosureParam { closure: String, index: usize },
}

#[derive(Default)]
struct FnFacts {
    file: usize,
    name: String,
    has_self: bool,
    params: Vec<Option<String>>,
    /// Parameters of an integer type.
    integers: BTreeSet<String>,
    locals: BTreeMap<String, Vec<Local>>,
    /// Text appended to a bound name (`push_str`, `push`, `+=`, `write!`).
    appends: BTreeMap<String, Vec<(Expr, Guard)>>,
    returns: Vec<Expr>,
    /// Parameters that hold a `WriteTx`.
    tx_params: BTreeSet<String>,
    /// Parameters of a pool type (`PgPool`, `Pool<..>`).
    pool_params: BTreeSet<String>,
    /// `let name;` without a value: where, and the block it is in.
    declared: BTreeMap<String, Vec<(Pos, Span)>>,
    /// Where the function first holds the writer lock: a `WriteTx`
    /// parameter (the start), a typed `WriteTx` binding or `begin_write()`.
    lock_from: Option<Pos>,
    /// Closures bound by `let`, with their span.
    closures: Vec<(String, Pos, Pos)>,
    /// `let name = <sink call chain>`: the sink's position.
    sink_names: Vec<(String, Pos)>,
    /// Where the function's signature starts.
    start: Pos,
    /// Every argument-less `.begin()` call (its method name's position).
    begins: Vec<Pos>,
    /// The `.begin()` calls a `let` or an assignment binds.
    bound_begins: BTreeSet<Pos>,
    /// The spans of loops: `while` and `loop` whole, a `for` loop's body.
    loops: Vec<(Pos, Pos)>,
    /// Every use of a single-identifier path.
    idents: Vec<(String, Pos)>,
    /// Identifier uses that keep a transaction in view: an executor, the
    /// receiver of `commit`/`rollback`, `drop`'s argument, or a `let`
    /// (or assignment) that renames or reborrows it.
    tx_ok: BTreeSet<Pos>,
    /// Identifier uses passed to a call: `(use, module, callee, method)`.
    arg_uses: Vec<(Pos, Option<String>, String, bool)>,
}

struct CallSite {
    /// The path's segments before the name (`other` in `other::f`).
    module: Option<String>,
    name: String,
    caller: usize,
    method: bool,
    args: Vec<Expr>,
    /// Where the call starts.
    pos: Pos,
}

struct Sink {
    function: usize,
    pos: Pos,
    arg: Expr,
    /// The executor, where the sink form names it.
    executor: Option<Expr>,
    /// An executor method's argument: a sink only if it resolves to text.
    candidate: bool,
    in_spawn: bool,
}

/// What an executor method runs on.
enum LinkTarget {
    Sink(Pos),
    Name(String),
    Closure(String),
}

struct ExecLink {
    function: usize,
    target: LinkTarget,
    executor: Expr,
}

struct Literal {
    file: usize,
    function: String,
    start: Pos,
    end: Pos,
    value: String,
}

/// Every function, call, `&str` const, sink and string literal of a set of
/// Rust files.
#[derive(Default)]
pub struct Index {
    /// Functions that rewrite SQL text without adding a table: a call is
    /// followed into its first argument.
    rewriters: Vec<String>,
    /// [`list_key`]s of expressions not to follow: they resolve to
    /// [`Sql::Unresolved`] at once.
    listed: BTreeSet<String>,
    files: Vec<PathBuf>,
    fns: Vec<FnFacts>,
    calls: Vec<CallSite>,
    consts: Vec<(usize, String, Expr)>,
    sinks: Vec<Sink>,
    links: Vec<ExecLink>,
    literals: Vec<Literal>,
    /// The branch constructs counted so far (ids for [`Guard`]s).
    constructs: usize,
    /// `use sqlx::query as q`: the alias names per file.
    aliases: BTreeMap<usize, BTreeSet<String>>,
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
            if !consts_only {
                let mut aliases = Aliases::default();
                aliases.visit_file(&file);
                index.aliases.insert(id, aliases.0);
                let mut lits = Literals {
                    index: &mut index,
                    file: id,
                    function: String::new(),
                };
                lits.visit_file(&file);
            }
            Items {
                index: &mut index,
                file: id,
                consts_only,
            }
            .visit_file(&file);
        }
        index
    }

    /// `text` with each `{NAME}` hole that names a `&str` const of one
    /// value (seen from `file`) replaced by that value.
    pub fn fill_consts(&self, file: &Path, text: &str) -> String {
        let Some(id) = self.files.iter().position(|f| f == file) else {
            return text.to_owned();
        };
        let mut out = String::new();
        for piece in split_format(text) {
            match piece {
                Ok(t) => out.push_str(&t),
                Err(hole) => {
                    let mut found = Vec::new();
                    for (f, _, v) in self.consts_named(id, None, &hole) {
                        self.resolve_const(*f, v, 0, &mut found);
                    }
                    if let [Source {
                        sql:
                            Sql::Text {
                                text,
                                template: false,
                                ..
                            },
                        ..
                    }] = found.as_slice()
                    {
                        out.push_str(text);
                    } else {
                        out.push('{');
                        out.push_str(&hole);
                        out.push('}');
                    }
                }
            }
        }
        out
    }

    /// Follows calls to `name` into their first argument; see `rewriters`.
    #[must_use]
    pub fn with_rewriter(mut self, name: &str) -> Self {
        self.rewriters.push(name.to_owned());
        self
    }

    /// Stops the walk at the expressions with these [`list_key`]s.
    #[must_use]
    pub fn with_listed<'a>(mut self, keys: impl IntoIterator<Item = &'a String>) -> Self {
        self.listed.extend(keys.into_iter().cloned());
        self
    }

    /// Every sink with the texts it can run.
    pub fn uses(&self) -> Vec<Use> {
        let mut out = Vec::new();
        for s in &self.sinks {
            let f = &self.fns[s.function];
            let mut sources = Vec::new();
            self.resolve(&s.arg, s.function, 0, &mut BTreeSet::new(), &mut sources);
            if s.candidate && !sources.iter().any(|x| matches!(x.sql, Sql::Text { .. })) {
                continue;
            }
            if let Some(name) = f
                .sink_names
                .iter()
                .find(|(_, p)| *p == s.pos)
                .map(|(n, _)| n)
            {
                self.attach_appends(name, s.function, &mut sources);
            }
            if sources.is_empty() {
                sources.push(self.src(s.function, &s.arg, Sql::Unresolved(tokens(&s.arg))));
            }
            let mut keys = BTreeSet::new();
            sources.retain(|x| keys.insert(x.clone()));
            out.push(Use {
                file: self.files[f.file].clone(),
                function: f.name.clone(),
                line: s.pos.line,
                sources,
                lock_problem: self.lock_problem(s),
                takes_lock: f.lock_from.is_some(),
            });
        }
        out
    }

    /// String literals outside `#[cfg(test)]` items that no sink reaches.
    pub fn unreached_literals(&self, uses: &[Use]) -> Vec<Unreached> {
        let mut reached: Vec<&Source> = Vec::new();
        for u in uses {
            for s in &u.sources {
                s.walk(&mut reached);
            }
        }
        self.literals
            .iter()
            .filter(|l| {
                let file = &self.files[l.file];
                !reached
                    .iter()
                    .any(|s| &s.file == file && s.start <= l.start && l.end <= s.end)
            })
            .map(|l| Unreached {
                file: self.files[l.file].clone(),
                function: l.function.clone(),
                start: l.start,
                value: l.value.clone(),
            })
            .collect()
    }

    fn lock_problem(&self, s: &Sink) -> Option<LockProblem> {
        if s.in_spawn {
            return Some(LockProblem::Spawned);
        }
        let f = &self.fns[s.function];
        let mut executors: Vec<&Expr> = s.executor.iter().collect();
        for l in self.links.iter().filter(|l| l.function == s.function) {
            let hit = match &l.target {
                LinkTarget::Sink(p) => *p == s.pos,
                LinkTarget::Name(n) => f.sink_names.iter().any(|(m, p)| m == n && *p == s.pos),
                LinkTarget::Closure(c) => f
                    .closures
                    .iter()
                    .any(|(name, a, b)| name == c && *a <= s.pos && s.pos <= *b),
            };
            if hit {
                executors.push(&l.executor);
            }
        }
        if executors.is_empty() {
            let held = f.lock_from.is_some_and(|b| b < s.pos);
            return (!held).then_some(LockProblem::Hidden);
        }
        executors
            .into_iter()
            .find(|e| !self.lock_derived(e, s.function, 0))
            .map(|e| LockProblem::Executor(tokens(e)))
    }

    /// Whether an executor expression derives from a `WriteTx`.
    fn lock_derived(&self, executor: &Expr, at: usize, depth: usize) -> bool {
        if depth > MAX_DEPTH {
            return false;
        }
        let Expr::Path(path) = strip_executor(executor) else {
            return false;
        };
        let Some(name) = path.path.get_ident().map(ToString::to_string) else {
            return false;
        };
        let facts = &self.fns[at];
        let used: Pos = path.span().start().into();
        let before: Vec<&Local> = facts
            .locals
            .get(&name)
            .map(|ls| ls.iter().filter(|l| l.pos < used).collect())
            .unwrap_or_default();
        if !before.is_empty() {
            let from = before.iter().rposition(|l| l.is_let).unwrap_or(0);
            return before[from..].iter().all(|l| {
                l.tx || matches!(&l.binding, Binding::Value(v) if self.lock_derived(v, at, depth + 1))
            });
        }
        if facts.tx_params.contains(&name) {
            return true;
        }
        let Some(index) = facts
            .params
            .iter()
            .position(|q| q.as_deref() == Some(&name))
        else {
            return false;
        };
        let mut any = false;
        for call in self.callers(at) {
            let arg_at = if !call.method && facts.has_self {
                index + 1
            } else {
                index
            };
            let Some(arg) = call.args.get(arg_at) else {
                continue;
            };
            if !self.lock_derived(arg, call.caller, depth + 1) {
                return false;
            }
            any = true;
        }
        any
    }

    /// Plain `begin()` transactions, acquired connections and pools that
    /// run more than one sink, directly or through helpers they are passed
    /// to, or one sink in a loop (in the opener, outside the loop, or in a
    /// helper). A sink counts once per chain of call sites that reaches it
    /// from the opener, so a helper called twice counts twice.
    pub fn multi_statement_reads(&self) -> Vec<PlainTx> {
        let mut by_origin: BTreeMap<Origin, BTreeMap<(usize, Pos, Via), bool>> = BTreeMap::new();
        for s in self.sinks.iter().filter(|s| !s.in_spawn) {
            if s.candidate {
                let mut sources = Vec::new();
                self.resolve(&s.arg, s.function, 0, &mut BTreeSet::new(), &mut sources);
                if !sources.iter().any(|x| matches!(x.sql, Sql::Text { .. })) {
                    continue;
                }
            }
            let f = &self.fns[s.function];
            let mut executors: Vec<&Expr> = s.executor.iter().collect();
            for l in self.links.iter().filter(|l| l.function == s.function) {
                let hit = match &l.target {
                    LinkTarget::Sink(p) => *p == s.pos,
                    LinkTarget::Name(n) => f.sink_names.iter().any(|(m, p)| m == n && *p == s.pos),
                    LinkTarget::Closure(c) => f
                        .closures
                        .iter()
                        .any(|(name, a, b)| name == c && *a <= s.pos && s.pos <= *b),
                };
                if hit {
                    executors.push(&l.executor);
                }
            }
            let mut origins = BTreeMap::new();
            for e in executors {
                let start = Reach {
                    at: s.function,
                    used_at: s.pos,
                    repeated: false,
                    via: Vec::new(),
                };
                self.tx_origins(e, start, 0, &mut origins);
            }
            for ((o, via), repeated) in origins {
                let seen = by_origin.entry(o).or_default();
                *seen.entry((s.function, s.pos, via)).or_default() |= repeated;
            }
        }
        by_origin
            .into_iter()
            .filter(|(_, sinks)| sinks.len() > 1 || sinks.values().any(|r| *r))
            .map(|((kind, at, pos), sinks)| PlainTx {
                file: self.files[self.fns[at].file].clone(),
                function: self.fns[at].name.clone(),
                line: if kind == TxKind::Pool {
                    self.fns[at].start.line
                } else {
                    pos.line
                },
                kind,
                sinks: sinks
                    .into_iter()
                    .map(|((f, p, via), repeated)| {
                        let looped = if repeated { " (in a loop)" } else { "" };
                        let from = via
                            .first()
                            .map(|(c, cp)| format!(" via {}:{}", self.fns[*c].name, cp.line))
                            .unwrap_or_default();
                        format!("{}:{}{from}{looped}", self.fns[f].name, p.line)
                    })
                    .collect(),
            })
            .collect()
    }

    /// Where an executor expression used as `reach` describes can come
    /// from: a plain `begin()` or `acquire()` binding, through locals, and
    /// parameters through every caller's argument; or a pool. Each origin,
    /// with the call sites between, maps to whether the use runs more than
    /// once in it: inside a loop that does not hold the binding, or a loop
    /// in a function between.
    fn tx_origins(
        &self,
        executor: &Expr,
        reach: Reach,
        depth: usize,
        out: &mut BTreeMap<(Origin, Via), bool>,
    ) {
        if depth > MAX_DEPTH {
            return;
        }
        let Reach {
            at,
            used_at,
            repeated,
            via,
        } = reach;
        let e = strip_executor(executor);
        if is_pool(e) {
            let r = repeated || self.in_loop(at, used_at, None);
            let key = ((TxKind::Pool, at, Pos::default()), via);
            *out.entry(key).or_default() |= r;
            return;
        }
        let Expr::Path(path) = e else {
            return;
        };
        let Some(name) = path.path.get_ident().map(ToString::to_string) else {
            return;
        };
        let used: Pos = path.span().start().into();
        let visible = self.in_scope(&name, used, at);
        if !visible.is_empty() {
            for l in visible {
                if let Binding::Value(v) = &l.binding {
                    let kind = if is_plain_begin(v) {
                        Some(TxKind::Begin)
                    } else if is_acquire(v) {
                        Some(TxKind::Connection)
                    } else {
                        None
                    };
                    if let Some(kind) = kind {
                        let r = repeated || self.in_loop(at, used_at, Some(l.pos));
                        *out.entry(((kind, at, l.pos), via.clone())).or_default() |= r;
                    } else {
                        let next = Reach {
                            at,
                            used_at,
                            repeated,
                            via: via.clone(),
                        };
                        self.tx_origins(v, next, depth + 1, out);
                    }
                }
            }
            return;
        }
        let facts = &self.fns[at];
        let Some(index) = facts
            .params
            .iter()
            .position(|q| q.as_deref() == Some(&name))
        else {
            return;
        };
        let r = repeated || self.in_loop(at, used_at, None);
        // A pool parameter no indexed caller passes is a pool of its own.
        if facts.pool_params.contains(&name) && self.callers(at).next().is_none() {
            let key = ((TxKind::Pool, at, Pos::default()), via);
            *out.entry(key).or_default() |= r;
            return;
        }
        for call in self.callers(at) {
            let arg_at = if !call.method && facts.has_self {
                index + 1
            } else {
                index
            };
            if let Some(arg) = call.args.get(arg_at) {
                let mut via = via.clone();
                via.insert(0, (call.caller, call.pos));
                let next = Reach {
                    at: call.caller,
                    used_at: call.pos,
                    repeated: r,
                    via,
                };
                self.tx_origins(arg, next, depth + 1, out);
            }
        }
    }

    /// The bindings of local `name` a use at `used` in function `at` can
    /// see: made before it in a block holding it, from the last `let` on.
    fn in_scope(&self, name: &str, used: Pos, at: usize) -> Vec<&Local> {
        let before: Vec<&Local> = self.fns[at]
            .locals
            .get(name)
            .map(|ls| {
                ls.iter()
                    .filter(|l| l.pos < used && l.scope.0 <= used && used <= l.scope.1)
                    .collect()
            })
            .unwrap_or_default();
        let from = before.iter().rposition(|l| l.is_let).unwrap_or(0);
        let mut seen: Vec<&Local> = before.get(from..).unwrap_or_default().to_vec();
        // A later `let name;` shadows them: only its assignments count.
        let declared = self.fns[at].declared.get(name).and_then(|ds| {
            ds.iter()
                .rev()
                .find(|(p, sc)| *p < used && sc.0 <= used && used <= sc.1)
        });
        if let Some((d, _)) = declared {
            if seen.first().is_none_or(|l| !l.is_let || l.pos < *d) {
                seen.retain(|l| l.pos > *d);
            }
        }
        seen
    }

    /// Whether `pos` in function `at` sits in a loop that does not also
    /// hold `binding`.
    /// A sink in a closure counts where the closure is called.
    fn in_loop(&self, at: usize, pos: Pos, binding: Option<Pos>) -> bool {
        let f = &self.fns[at];
        let looped = |pos: Pos| {
            f.loops.iter().any(|(a, b)| {
                *a <= pos && pos <= *b && binding.is_none_or(|p| !(*a <= p && p <= *b))
            })
        };
        looped(pos)
            || f.closures
                .iter()
                .filter(|(_, a, b)| *a <= pos && pos <= *b)
                .any(|(name, _, _)| {
                    self.calls
                        .iter()
                        .any(|c| c.caller == at && !c.method && c.name == *name && looped(c.pos))
                })
    }

    /// Plain `begin()` transactions whose statements the lint cannot count:
    /// a `.begin()` no `let` or assignment binds (through `.await`, `?`,
    /// methods on it and [`BEGIN_PASS_THROUGH`] calls), and a binding moved
    /// anywhere but an executor, a helper the index has, `commit`,
    /// `rollback`, `drop` or another binding.
    pub fn begin_escapes(&self) -> Vec<Escape> {
        let params = self.plain_params();
        let mut out = Vec::new();
        for (at, f) in self.fns.iter().enumerate() {
            let escape = |line: usize, what: String| Escape {
                file: self.files[f.file].clone(),
                function: f.name.clone(),
                line,
                what,
            };
            for b in f.begins.iter().filter(|b| !f.bound_begins.contains(b)) {
                out.push(escape(
                    b.line,
                    "calls begin() where no `let name =` binds the transaction".to_owned(),
                ));
            }
            for (name, pos) in &f.idents {
                if f.tx_ok.contains(pos) || !self.holds_plain_begin(name, *pos, at, &params) {
                    continue;
                }
                let helper = f.arg_uses.iter().find(|u| u.0 == *pos).is_some_and(
                    |(_, module, callee, method)| {
                        let named = self.fns_named(f.file, module.as_deref(), callee, *method);
                        // `Self::f` must be in this file, `Type::f` in module
                        // `Type`, as `callers` follows them.
                        let in_module = match module.as_deref() {
                            None | Some("super" | "crate") => true,
                            Some("Self" | "self") => {
                                named.iter().any(|&n| self.fns[n].file == f.file)
                            }
                            Some(m) => named.iter().any(|&n| self.in_module(self.fns[n].file, m)),
                        };
                        !f.closures.iter().any(|(c, _, _)| c == callee)
                            && !named.is_empty()
                            && in_module
                    },
                );
                if !helper {
                    out.push(escape(
                        pos.line,
                        format!(
                            "moves its plain begin() transaction `{name}` where the lint \
                             does not follow it (a struct, Option, field, return or closure)"
                        ),
                    ));
                }
            }
        }
        out
    }

    /// Whether `name` used at `used` in function `at` holds a plain
    /// `begin()` transaction: a local bound to one (or renaming one), or a
    /// parameter in `params` ([`Self::plain_params`]).
    fn holds_plain_begin(
        &self,
        name: &str,
        used: Pos,
        at: usize,
        params: &BTreeSet<(usize, String)>,
    ) -> bool {
        let visible = self.in_scope(name, used, at);
        if visible.is_empty() {
            return params.contains(&(at, name.to_owned()));
        }
        // A binding is made before its use, so renames end.
        visible.iter().any(|l| {
            let Binding::Value(v) = &l.binding else {
                return false;
            };
            is_plain_begin(v)
                || executor_ident(v)
                    .is_some_and(|(n, p)| p < used && self.holds_plain_begin(&n, p, at, params))
        })
    }

    /// The parameters, as `(function, name)`, some caller passes a plain
    /// `begin()` transaction, directly or through other such parameters
    /// (to a fixed point).
    fn plain_params(&self) -> BTreeSet<(usize, String)> {
        let mut by_name: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
        for (i, f) in self.fns.iter().enumerate() {
            by_name.entry(f.name.as_str()).or_default().push(i);
        }
        let mut set = BTreeSet::new();
        loop {
            let mut changed = false;
            for call in &self.calls {
                let Some(callees) = by_name.get(call.name.as_str()) else {
                    continue;
                };
                for &f in callees {
                    let facts = &self.fns[f];
                    if !self.reaches(call, f) {
                        continue;
                    }
                    for (index, param) in facts.params.iter().enumerate() {
                        let Some(param) = param else { continue };
                        let key = (f, param.clone());
                        if set.contains(&key) {
                            continue;
                        }
                        let arg_at = if !call.method && facts.has_self {
                            index + 1
                        } else {
                            index
                        };
                        let passes = call
                            .args
                            .get(arg_at)
                            .and_then(executor_ident)
                            .is_some_and(|(n, p)| self.holds_plain_begin(&n, p, call.caller, &set));
                        if passes {
                            set.insert(key);
                            changed = true;
                        }
                    }
                }
            }
            if !changed {
                return set;
            }
        }
    }

    fn callers(&self, f: usize) -> impl Iterator<Item = &CallSite> {
        let callee = &self.fns[f];
        self.calls
            .iter()
            .filter(move |c| c.name == callee.name && self.reaches(c, f))
    }

    /// Whether `call`'s path can name function `f`: no module, `Self::` or
    /// `self::` from `f`'s own file, or a module `f`'s file is.
    fn reaches(&self, call: &CallSite, f: usize) -> bool {
        let file = self.fns[f].file;
        match call.module.as_deref() {
            None => true,
            Some("Self" | "self") => self.fns[call.caller].file == file,
            Some(m) => self.in_module(file, m),
        }
    }

    fn attach_appends(&self, name: &str, at: usize, sources: &mut [Source]) {
        let Some(appends) = self.fns[at].appends.get(name) else {
            return;
        };
        let resolved: Vec<Append> = appends
            .iter()
            .map(|(a, guard)| {
                let mut alts = Vec::new();
                self.resolve(a, at, 1, &mut BTreeSet::new(), &mut alts);
                if alts.is_empty() {
                    alts.push(self.src(at, a, Sql::Unresolved(tokens(a))));
                }
                Append {
                    guard: guard.clone(),
                    alts,
                }
            })
            .collect();
        for s in sources {
            if let Sql::Text { appends, .. } = &mut s.sql {
                appends.extend(resolved.iter().cloned());
            }
        }
    }

    /// Whether `file` is module `module` (`module.rs` or `module/mod.rs`).
    fn in_module(&self, file: usize, module: &str) -> bool {
        let path = &self.files[file];
        let stem = path.file_stem().and_then(|s| s.to_str());
        let dir = path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str());
        stem == Some(module) || (stem == Some("mod") && dir == Some(module))
    }

    /// The consts named `name` seen from `file`: `module::NAME` in that
    /// module, else the file's own, else every const of that name.
    fn consts_named(
        &self,
        file: usize,
        module: Option<&str>,
        name: &str,
    ) -> Vec<&(usize, String, Expr)> {
        let all: Vec<_> = self.consts.iter().filter(|(_, n, _)| n == name).collect();
        if let Some(m) = module {
            let there: Vec<_> = all
                .iter()
                .copied()
                .filter(|(f, _, _)| self.in_module(*f, m))
                .collect();
            if !there.is_empty() {
                return there;
            }
        }
        let local: Vec<_> = all.iter().copied().filter(|(f, _, _)| *f == file).collect();
        if local.is_empty() {
            all
        } else {
            local
        }
    }

    /// The functions named `name` seen from `file`, by the same rule.
    fn fns_named(&self, file: usize, module: Option<&str>, name: &str, method: bool) -> Vec<usize> {
        let all: Vec<usize> = (0..self.fns.len())
            .filter(|&i| self.fns[i].name == name && (!method || self.fns[i].has_self))
            .collect();
        if let Some(m) = module {
            let there: Vec<usize> = all
                .iter()
                .copied()
                .filter(|&i| self.in_module(self.fns[i].file, m))
                .collect();
            if !there.is_empty() {
                return there;
            }
        }
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

    /// Resolves a const's value expression in its own file.
    fn resolve_const(&self, file: usize, e: &Expr, depth: usize, out: &mut Vec<Source>) {
        let unresolved =
            |out: &mut Vec<Source>| out.push(self.source(file, e, Sql::Unresolved(tokens(e))));
        if depth > MAX_DEPTH {
            return unresolved(out);
        }
        match e {
            Expr::Lit(syn::ExprLit {
                lit: Lit::Str(s), ..
            }) => {
                out.push(self.source(file, e, Sql::plain(s.value())));
            }
            Expr::Path(p) => {
                let (module, name) = split_path(&p.path);
                let found = self.consts_named(file, module.as_deref(), &name);
                if found.is_empty() {
                    return unresolved(out);
                }
                for (f, _, v) in found {
                    self.resolve_const(*f, v, depth + 1, out);
                }
            }
            Expr::Macro(m) if last_segment(&m.mac.path) == "concat" => {
                let Ok(args) = m
                    .mac
                    .parse_body_with(Punctuated::<Expr, Token![,]>::parse_terminated)
                else {
                    return unresolved(out);
                };
                let mut text = String::new();
                for a in &args {
                    let mut part = Vec::new();
                    self.resolve_const(file, a, depth + 1, &mut part);
                    match part.as_slice() {
                        [Source {
                            sql:
                                Sql::Text {
                                    text: t,
                                    template: false,
                                    ..
                                },
                            ..
                        }] => text.push_str(t),
                        _ => return unresolved(out),
                    }
                }
                out.push(self.source(file, e, Sql::plain(text)));
            }
            Expr::Group(g) => self.resolve_const(file, &g.expr, depth, out),
            Expr::Paren(p) => self.resolve_const(file, &p.expr, depth, out),
            _ => unresolved(out),
        }
    }

    #[expect(clippy::too_many_lines)]
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
            out.push(self.src(at, e, Sql::Unresolved(tokens(e))));
        };
        if depth > MAX_DEPTH {
            return unresolved(out, e);
        }
        let next = depth + 1;
        if !self.listed.is_empty()
            && !matches!(e, Expr::Lit(_))
            && self
                .listed
                .contains(&list_key(&self.files[file], &self.fns[at].name, &tokens(e)))
        {
            return unresolved(out, e);
        }
        if is_integer(e, &self.fns[at], 0) {
            return out.push(self.src(at, e, Sql::plain("1".to_owned())));
        }
        match e {
            Expr::Lit(syn::ExprLit {
                lit: Lit::Str(s), ..
            }) => {
                out.push(self.src(at, e, Sql::plain(s.value())));
            }
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
                            let holes = self.format_args(lit, &text, &args, at, next, seen);
                            out.push(self.src(
                                at,
                                lit,
                                Sql::Text {
                                    text,
                                    template: true,
                                    args: holes,
                                    appends: Vec::new(),
                                },
                            ));
                        }
                        _ => unresolved(out, e),
                    },
                    ("concat", _) => self.resolve_const(file, e, next, out),
                    _ => unresolved(out, e),
                }
            }
            Expr::Reference(r) => self.resolve(&r.expr, at, next, seen, out),
            Expr::Cast(c) => self.resolve(&c.expr, at, next, seen, out),
            // `head + &tail`: the two texts joined.
            Expr::Binary(b) if matches!(b.op, BinOp::Add(_)) => {
                let mut parts = BTreeMap::new();
                for (k, side) in [&b.left, &b.right].into_iter().enumerate() {
                    let mut alts = Vec::new();
                    self.resolve(side, at, next, &mut seen.clone(), &mut alts);
                    if alts.is_empty() {
                        alts.push(self.src(at, side, Sql::Unresolved(tokens(side))));
                    }
                    parts.insert(k.to_string(), alts);
                }
                out.push(self.src(
                    at,
                    e,
                    Sql::Text {
                        text: "{0}{1}".to_owned(),
                        template: true,
                        args: parts,
                        appends: Vec::new(),
                    },
                ));
            }
            // `f.sql` of a local that gained text: that text.
            Expr::Field(fe) if matches!(&*fe.base, Expr::Path(p) if p.path.get_ident().is_some_and(|i| self.fns[at].appends.contains_key(&i.to_string()))) =>
            {
                let Expr::Path(base) = &*fe.base else {
                    return unresolved(out, e);
                };
                let name = base.path.segments[0].ident.to_string();
                let start = out.len();
                out.push(self.src(at, e, Sql::plain(String::new())));
                self.attach_appends(&name, at, &mut out[start..]);
            }
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
            // `COLUMNS.replace(" content,", " '' AS content,")` on known text.
            Expr::MethodCall(m) if m.method == "replace" && m.args.len() == 2 => {
                let snapshot = seen.clone();
                let plain = |e: &Expr| {
                    let mut alts = Vec::new();
                    self.resolve(e, at, next, &mut snapshot.clone(), &mut alts);
                    match alts.as_slice() {
                        [Source {
                            sql:
                                Sql::Text {
                                    text,
                                    template: false,
                                    appends,
                                    ..
                                },
                            ..
                        }] if appends.is_empty() => Some(text.clone()),
                        _ => None,
                    }
                };
                let mut receivers = Vec::new();
                self.resolve(&m.receiver, at, next, seen, &mut receivers);
                let (Some(from), Some(to)) = (plain(&m.args[0]), plain(&m.args[1])) else {
                    return unresolved(out, e);
                };
                for r in receivers {
                    match &r.sql {
                        Sql::Text {
                            text,
                            template: false,
                            appends,
                            ..
                        } if appends.is_empty() => out.push(Source {
                            sql: Sql::plain(text.replace(&from, &to)),
                            ..r.clone()
                        }),
                        _ => unresolved(out, e),
                    }
                }
            }
            // `self.build_sql(..)` or `query.sql()`: what that method returns.
            Expr::MethodCall(m) => {
                let name = m.method.to_string();
                let fns = self.fns_named(file, None, &name, true);
                if fns.is_empty() {
                    unresolved(out, e);
                } else {
                    self.resolve_returns(&fns, next, seen, out);
                }
            }
            Expr::Call(c) => {
                let Expr::Path(p) = &*c.func else {
                    return unresolved(out, e);
                };
                let (module, name) = split_path(&p.path);
                if name == "AssertSqlSafe"
                    || self.rewriters.contains(&name)
                    || (module.as_deref() == Some("String") && name == "from")
                {
                    match c.args.first() {
                        Some(a) => self.resolve(a, at, next, seen, out),
                        None => unresolved(out, e),
                    }
                    return;
                }
                if module.as_deref() == Some("String") && (name == "new" || name == "with_capacity")
                {
                    return out.push(self.src(at, e, Sql::plain(String::new())));
                }
                let type_path = module.as_deref().is_some_and(|m| {
                    m.starts_with(|ch: char| ch.is_ascii_uppercase()) && m != "Self"
                });
                let fns = if type_path || self.is_sink_path(file, &p.path) {
                    Vec::new()
                } else {
                    self.fns_named(file, module.as_deref(), &name, false)
                };
                if fns.is_empty() {
                    unresolved(out, e);
                } else {
                    self.resolve_returns(&fns, next, seen, out);
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
                let used: Pos = e.span().start().into();
                if !seen.insert((at, format!("{name}@{}:{}", used.line, used.column))) {
                    return;
                }
                let f = &self.fns[at];
                if let Some(locals) = f.locals.get(&name) {
                    let start = out.len();
                    for l in visible(locals, used) {
                        self.resolve_binding(&l.binding, at, next, seen, out);
                    }
                    self.attach_appends(&name, at, &mut out[start..]);
                } else if let Some(i) = f.params.iter().position(|p| p.as_deref() == Some(&name)) {
                    if !self.resolve_callers(at, i, next, seen, out) {
                        unresolved(out, e);
                    }
                } else {
                    let found = self.consts_named(file, None, &name);
                    if found.is_empty() {
                        unresolved(out, e);
                    }
                    for (f, _, v) in found {
                        self.resolve_const(*f, v, next, out);
                    }
                }
            }
            Expr::Path(p) => {
                let (module, name) = split_path(&p.path);
                let found = self.consts_named(file, module.as_deref(), &name);
                if found.is_empty() {
                    unresolved(out, e);
                }
                for (f, _, v) in found {
                    self.resolve_const(*f, v, next, out);
                }
            }
            _ => unresolved(out, e),
        }
    }

    fn is_sink_path(&self, file: usize, p: &syn::Path) -> bool {
        let segs: Vec<String> = p.segments.iter().map(|s| s.ident.to_string()).collect();
        let name = segs.last().cloned().unwrap_or_default();
        let aliased = segs.len() == 1 && self.aliases.get(&file).is_some_and(|a| a.contains(&name));
        let sqlx = segs.len() == 1 || segs.iter().any(|s| s == "sqlx");
        aliased
            || (sqlx && SINK_FNS.contains(&name.as_str()))
            || (segs.iter().any(|s| s == "QueryBuilder")
                && (name == "new" || name == "with_arguments"))
    }

    /// Every alternative of each hole of a `format!` string: positional
    /// and named arguments, and captured names.
    fn format_args(
        &self,
        lit: &Expr,
        text: &str,
        args: &Punctuated<Expr, Token![,]>,
        at: usize,
        depth: usize,
        seen: &BTreeSet<(usize, String)>,
    ) -> BTreeMap<String, Vec<Source>> {
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
                // A captured name, read where the string is written.
                None => match syn::parse_str::<syn::Ident>(&hole) {
                    Ok(mut ident) => {
                        ident.set_span(lit.span());
                        Expr::Path(syn::ExprPath {
                            attrs: Vec::new(),
                            qself: None,
                            path: ident.into(),
                        })
                    }
                    Err(_) => Expr::Verbatim(hole.parse().unwrap_or_default()),
                },
            };
            let mut found = Vec::new();
            self.resolve(&expr, at, depth, &mut seen.clone(), &mut found);
            if found.is_empty() {
                let mut missing = self.src(at, lit, Sql::Unresolved(tokens(&expr)));
                missing.end = missing.start;
                found.push(missing);
            }
            out.insert(hole, found);
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
        let file = self.fns[at].file;
        match b {
            Binding::Value(e) => self.resolve(e, at, depth, seen, out),
            Binding::Field { init, path } => {
                let mut parts = Vec::new();
                self.project(init, path, at, 0, &mut parts);
                for (p, f) in parts {
                    match p {
                        Ok(e) => self.resolve(e, f, depth, seen, out),
                        Err(e) => {
                            out.push(self.src(f, e, Sql::Unresolved(tokens(e))));
                        }
                    }
                }
            }
            Binding::Element { iter, path } => {
                let Some(items) = self.elements(iter, file, 0) else {
                    return out.push(self.src(at, iter, Sql::Unresolved(tokens(iter))));
                };
                for item in items {
                    let mut parts = Vec::new();
                    self.project(item, path, at, 0, &mut parts);
                    for (p, f) in parts {
                        match p {
                            Ok(e) => self.resolve(e, f, depth, seen, out),
                            Err(e) => out.push(self.src(f, e, Sql::Unresolved(tokens(e)))),
                        }
                    }
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

    /// Resolves what each of `fns` returns.
    fn resolve_returns(
        &self,
        fns: &[usize],
        depth: usize,
        seen: &mut BTreeSet<(usize, String)>,
        out: &mut Vec<Source>,
    ) {
        for &f in fns {
            if !seen.insert((f, "<return>".to_owned())) {
                continue;
            }
            for r in &self.fns[f].returns {
                self.resolve(r, f, depth, seen, out);
            }
        }
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
        let has_self = self.fns[f].has_self;
        let mut any = false;
        for c in self.callers(f) {
            let k = if !c.method && has_self { i + 1 } else { i };
            if let Some(arg) = c.args.get(k) {
                any = true;
                self.resolve(arg, c.caller, depth, seen, out);
            }
        }
        any
    }

    /// The parts of `e` at tuple field `path`, through blocks, `if`,
    /// `match` and the return values of a called function, each with the
    /// function it is read in: `Ok` for an expression, `Err` for a shape it
    /// cannot see into.
    fn project<'a>(
        &'a self,
        e: &'a Expr,
        path: &[usize],
        at: usize,
        depth: usize,
        out: &mut Vec<(Result<&'a Expr, &'a Expr>, usize)>,
    ) {
        let Some((&k, rest)) = path.split_first() else {
            return out.push((Ok(e), at));
        };
        if depth > MAX_DEPTH {
            return out.push((Err(e), at));
        }
        let next = depth + 1;
        match strip(e) {
            Expr::Tuple(t) if k < t.elems.len() => self.project(&t.elems[k], rest, at, next, out),
            Expr::Block(b) => match tail(&b.block) {
                Some(t) => self.project(t, path, at, next, out),
                None => out.push((Err(e), at)),
            },
            Expr::If(i) => {
                match tail(&i.then_branch) {
                    Some(t) => self.project(t, path, at, next, out),
                    None => out.push((Err(e), at)),
                }
                match &i.else_branch {
                    Some((_, els)) => self.project(els, path, at, next, out),
                    None => out.push((Err(e), at)),
                }
            }
            Expr::Match(m) => {
                for arm in &m.arms {
                    self.project(&arm.body, path, at, next, out);
                }
            }
            Expr::Call(c) => {
                let Expr::Path(p) = &*c.func else {
                    return out.push((Err(e), at));
                };
                let (module, name) = split_path(&p.path);
                let fns = self.fns_named(self.fns[at].file, module.as_deref(), &name, false);
                if fns.is_empty() {
                    return out.push((Err(e), at));
                }
                for f in fns {
                    for r in &self.fns[f].returns {
                        self.project(r, path, f, next, out);
                    }
                }
            }
            _ => out.push((Err(e), at)),
        }
    }

    /// The items of a literal array iterated by a `for` loop (`[a, b]`,
    /// `&[a, b]`, `[a, b].iter()`, a const array).
    fn elements<'a>(&'a self, e: &'a Expr, file: usize, depth: usize) -> Option<Vec<&'a Expr>> {
        if depth > MAX_DEPTH {
            return None;
        }
        match strip(e) {
            Expr::Array(a) => Some(a.elems.iter().collect()),
            Expr::MethodCall(m)
                if m.args.is_empty()
                    && ["iter", "into_iter", "copied", "cloned"]
                        .contains(&m.method.to_string().as_str()) =>
            {
                self.elements(&m.receiver, file, depth + 1)
            }
            Expr::Path(p) => {
                let (module, name) = split_path(&p.path);
                let found = self.consts_named(file, module.as_deref(), &name);
                let mut out = Vec::new();
                for (f, _, v) in &found {
                    out.extend(self.elements(v, *f, depth + 1)?);
                }
                (!found.is_empty()).then_some(out)
            }
            _ => None,
        }
    }

    /// A source written in function `at`.
    fn src(&self, at: usize, e: &impl Spanned, sql: Sql) -> Source {
        Source {
            function: self.fns[at].name.clone(),
            ..self.source(self.fns[at].file, e, sql)
        }
    }

    /// A source written outside a function (a const).
    fn source(&self, file: usize, e: &impl Spanned, sql: Sql) -> Source {
        let span = e.span();
        Source {
            file: self.files[file].clone(),
            function: String::new(),
            start: span.start().into(),
            end: span.end().into(),
            sql,
        }
    }
}

/// Whether `e` is an integer: a literal, arithmetic on integers, a length,
/// a cast to an integer type, or a parameter or local of one.
fn is_integer(e: &Expr, f: &FnFacts, depth: usize) -> bool {
    if depth > MAX_DEPTH {
        return false;
    }
    let next = depth + 1;
    match strip(e) {
        Expr::Lit(syn::ExprLit {
            lit: Lit::Int(_), ..
        }) => true,
        Expr::Cast(c) => is_integer_type(&c.ty),
        Expr::Binary(b) => {
            matches!(
                b.op,
                BinOp::Add(_) | BinOp::Sub(_) | BinOp::Mul(_) | BinOp::Div(_) | BinOp::Rem(_)
            ) && is_integer(&b.left, f, next)
                && is_integer(&b.right, f, next)
        }
        Expr::MethodCall(m) => {
            m.args.is_empty() && ["len", "count"].contains(&m.method.to_string().as_str())
        }
        Expr::Path(p) => p.path.get_ident().is_some_and(|i| {
            let name = i.to_string();
            f.integers.contains(&name)
                || f.locals.get(&name).is_some_and(|ls| {
                    ls.iter()
                        .all(|l| matches!(&l.binding, Binding::Value(v) if is_integer(v, f, next)))
                })
        }),
        _ => false,
    }
}

/// Whether every value `e` can take comes from `begin_write()`, through
/// `.await`, `?`, method calls on it, blocks and every branch.
fn is_begin_write(e: &Expr) -> bool {
    match strip(e) {
        Expr::Try(t) => is_begin_write(&t.expr),
        Expr::Await(a) => is_begin_write(&a.base),
        Expr::MethodCall(m) => m.method == "begin_write" || is_begin_write(&m.receiver),
        Expr::Block(b) => tail(&b.block).is_some_and(is_begin_write),
        Expr::If(i) => {
            tail(&i.then_branch).is_some_and(is_begin_write)
                && i.else_branch
                    .as_ref()
                    .is_some_and(|(_, e)| is_begin_write(e))
        }
        Expr::Match(m) => !m.arms.is_empty() && m.arms.iter().all(|a| is_begin_write(&a.body)),
        _ => false,
    }
}

/// Whether `e` is a plain `begin()` transaction (not `begin_write()` or
/// `begin_read_snapshot()`), through `.await`, `?` and method calls on it.
fn is_plain_begin(e: &Expr) -> bool {
    match strip(e) {
        Expr::Try(t) => is_plain_begin(&t.expr),
        Expr::Await(a) => is_plain_begin(&a.base),
        Expr::MethodCall(m) => m.method == "begin" || is_plain_begin(&m.receiver),
        Expr::Call(c) if is_begin_pass_through(c) => c.args.iter().any(is_plain_begin),
        _ => false,
    }
}

/// `(module, name)` of a path: `other::f` is `(Some("other"), "f")`;
/// `self`, `super` and `crate` name no module.
/// Whether `c` is a [`BEGIN_PASS_THROUGH`] call.
fn is_begin_pass_through(c: &syn::ExprCall) -> bool {
    matches!(&*c.func, Expr::Path(p) if BEGIN_PASS_THROUGH.contains(&last_segment(&p.path).as_str()))
}

/// The `.begin()` calls `e` evaluates to (each method name's position),
/// through `.await`, `?`, methods on it and [`BEGIN_PASS_THROUGH`] calls.
fn begin_calls(e: &Expr, out: &mut Vec<Pos>) {
    match strip(e) {
        Expr::Try(t) => begin_calls(&t.expr, out),
        Expr::Await(a) => begin_calls(&a.base, out),
        Expr::MethodCall(m) if m.method == "begin" && m.args.is_empty() => {
            out.push(m.method.span().start().into());
        }
        Expr::MethodCall(m) => begin_calls(&m.receiver, out),
        Expr::Call(c) if is_begin_pass_through(c) => {
            for a in &c.args {
                begin_calls(a, out);
            }
        }
        _ => {}
    }
}

/// Whether `e` is a connection checked out with `acquire()`, through
/// `.await`, `?` and method calls on it.
fn is_acquire(e: &Expr) -> bool {
    match strip(e) {
        Expr::Try(t) => is_acquire(&t.expr),
        Expr::Await(a) => is_acquire(&a.base),
        Expr::MethodCall(m) => m.method == "acquire" || is_acquire(&m.receiver),
        _ => false,
    }
}

/// Whether an executor (already stripped) names a pool: a `pool` field or
/// a [`POOL_METHODS`] call.
fn is_pool(e: &Expr) -> bool {
    match e {
        Expr::Field(f) => matches!(&f.member,
            syn::Member::Named(n) if POOL_METHODS.contains(&n.to_string().as_str())),
        Expr::MethodCall(m) if m.args.is_empty() && m.method == "clone" => {
            is_pool(strip_executor(&m.receiver))
        }
        Expr::MethodCall(m) => {
            m.args.is_empty() && POOL_METHODS.contains(&m.method.to_string().as_str())
        }
        _ => false,
    }
}

/// The identifier an expression names once `&`, `*` and executor
/// conversions are stripped, with its position.
fn executor_ident(e: &Expr) -> Option<(String, Pos)> {
    match strip_executor(e) {
        Expr::Path(p) => p
            .path
            .get_ident()
            .map(|i| (i.to_string(), p.span().start().into())),
        _ => None,
    }
}

fn split_path(p: &syn::Path) -> (Option<String>, String) {
    let segs: Vec<String> = p.segments.iter().map(|s| s.ident.to_string()).collect();
    let name = segs.last().cloned().unwrap_or_default();
    let module = segs
        .len()
        .checked_sub(2)
        .map(|i| segs[i].clone())
        .filter(|m| !["self", "super", "crate"].contains(&m.as_str()));
    (module, name)
}

/// The bindings a use at `used` sees: the last `let` before it and the
/// assignments after that `let`; all of them when none comes before.
fn visible(locals: &[Local], used: Pos) -> Vec<&Local> {
    let before: Vec<&Local> = locals.iter().filter(|l| l.pos < used).collect();
    if before.is_empty() {
        return locals.iter().collect();
    }
    let from = before.iter().rposition(|l| l.is_let).unwrap_or(0);
    before[from..].to_vec()
}

/// The key an allowlist lists a text or expression under: its file name,
/// the function it is written in, and its first 80 characters with
/// whitespace collapsed.
pub fn list_key(file: &Path, function: &str, text: &str) -> String {
    let flat: String = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let name = file
        .file_name()
        .map(|f| f.to_string_lossy())
        .unwrap_or_default();
    let head: String = flat.chars().take(80).collect();
    format!("{name}::{function} | {}", head.trim_end())
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

/// An executor without `&`, `&mut`, `*` and executor conversions.
fn strip_executor(e: &Expr) -> &Expr {
    match strip(e) {
        Expr::Unary(u) if matches!(u.op, syn::UnOp::Deref(_)) => strip_executor(&u.expr),
        Expr::MethodCall(m)
            if m.args.is_empty()
                && EXECUTOR_PASS_THROUGH.contains(&m.method.to_string().as_str()) =>
        {
            strip_executor(&m.receiver)
        }
        other => other,
    }
}

fn tokens(e: &impl ToTokens) -> String {
    e.to_token_stream().to_string()
}

fn last_segment(p: &syn::Path) -> String {
    p.segments
        .last()
        .map(|s| s.ident.to_string())
        .unwrap_or_default()
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

/// Whether `ty` is a pool, through references: `PgPool` or `Pool<..>`.
fn is_pool_type(ty: &Type) -> bool {
    match ty {
        Type::Reference(r) => is_pool_type(&r.elem),
        Type::Path(p) => p
            .path
            .segments
            .last()
            .is_some_and(|s| s.ident == "PgPool" || s.ident == "Pool"),
        _ => false,
    }
}

fn is_integer_type(ty: &Type) -> bool {
    match ty {
        Type::Path(p) => p
            .path
            .get_ident()
            .is_some_and(|i| INTEGER_TYPES.contains(&i.to_string().as_str())),
        Type::Reference(r) => is_integer_type(&r.elem),
        _ => false,
    }
}

/// The name a pattern binds, if it binds one.
fn pat_ident(p: &Pat) -> Option<String> {
    match p {
        Pat::Ident(i) => Some(i.ident.to_string()),
        Pat::Type(t) => pat_ident(&t.pat),
        _ => None,
    }
}

/// The names a tuple pattern binds, with their field path.
fn tuple_idents(p: &Pat, path: &mut Vec<usize>, out: &mut Vec<(String, Vec<usize>)>) {
    match p {
        Pat::Tuple(t) => {
            for (k, q) in t.elems.iter().enumerate() {
                path.push(k);
                tuple_idents(q, path, out);
                path.pop();
            }
        }
        Pat::Type(t) => tuple_idents(&t.pat, path, out),
        other => {
            if let Some(name) = pat_ident(other) {
                out.push((name, path.clone()));
            }
        }
    }
}

/// Whether an item is compiled only for tests: `#[cfg(test)]`, or an
/// `any(..)`/`all(..)` that names `test`. `cfg(not(test))` is not.
fn has_cfg_test(attrs: &[syn::Attribute]) -> bool {
    fn test_only(m: &syn::Meta) -> bool {
        match m {
            syn::Meta::Path(p) => p.is_ident("test"),
            syn::Meta::List(l) if l.path.is_ident("any") || l.path.is_ident("all") => l
                .parse_args_with(Punctuated::<syn::Meta, Token![,]>::parse_terminated)
                .is_ok_and(|ms| ms.iter().any(test_only)),
            _ => false,
        }
    }
    attrs.iter().any(|a| {
        a.path().is_ident("cfg") && a.parse_args::<syn::Meta>().is_ok_and(|m| test_only(&m))
    })
}

fn mentions_sqlx(ts: &TokenStream) -> bool {
    ts.clone().into_iter().any(|t| match t {
        TokenTree::Ident(i) => i == "sqlx",
        TokenTree::Group(g) => mentions_sqlx(&g.stream()),
        _ => false,
    })
}

/// `use sqlx::query as q;`: the aliases of sink functions.
#[derive(Default)]
struct Aliases(BTreeSet<String>);

impl Aliases {
    fn tree(&mut self, t: &syn::UseTree, sqlx: bool) {
        match t {
            syn::UseTree::Path(p) => self.tree(&p.tree, sqlx || p.ident == "sqlx"),
            syn::UseTree::Rename(r) if sqlx && SINK_FNS.contains(&r.ident.to_string().as_str()) => {
                self.0.insert(r.rename.to_string());
            }
            syn::UseTree::Group(g) => {
                for t in &g.items {
                    self.tree(t, sqlx);
                }
            }
            _ => {}
        }
    }
}

impl<'ast> Visit<'ast> for Aliases {
    fn visit_item_use(&mut self, u: &'ast syn::ItemUse) {
        self.tree(&u.tree, false);
    }
}

/// Every string literal outside `#[cfg(test)]` items, also in macro
/// bodies that parse as expressions.
struct Literals<'i> {
    index: &'i mut Index,
    file: usize,
    /// The function being walked.
    function: String,
}

impl Literals<'_> {
    fn in_fn(&mut self, name: String, walk: impl FnOnce(&mut Self)) {
        let outer = std::mem::replace(&mut self.function, name);
        walk(self);
        self.function = outer;
    }
}

impl<'ast> Visit<'ast> for Literals<'_> {
    fn visit_item_mod(&mut self, m: &'ast syn::ItemMod) {
        if !has_cfg_test(&m.attrs) {
            visit::visit_item_mod(self, m);
        }
    }

    fn visit_item_fn(&mut self, f: &'ast syn::ItemFn) {
        if !has_cfg_test(&f.attrs) {
            self.in_fn(f.sig.ident.to_string(), |me| visit::visit_item_fn(me, f));
        }
    }

    fn visit_impl_item_fn(&mut self, f: &'ast syn::ImplItemFn) {
        if !has_cfg_test(&f.attrs) {
            self.in_fn(f.sig.ident.to_string(), |me| {
                visit::visit_impl_item_fn(me, f);
            });
        }
    }

    fn visit_item_const(&mut self, c: &'ast syn::ItemConst) {
        if !has_cfg_test(&c.attrs) {
            self.in_fn(c.ident.to_string(), |me| visit::visit_item_const(me, c));
        }
    }

    fn visit_attribute(&mut self, _: &'ast syn::Attribute) {}

    fn visit_lit_str(&mut self, l: &'ast syn::LitStr) {
        self.index.literals.push(Literal {
            function: self.function.clone(),
            file: self.file,
            start: l.span().start().into(),
            end: l.span().end().into(),
            value: l.value(),
        });
    }

    /// `concat!` of literals is one value; other macro bodies that parse
    /// as expressions are walked.
    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        let Ok(args) = m.parse_body_with(Punctuated::<Expr, Token![,]>::parse_terminated) else {
            return;
        };
        let parts: Option<Vec<String>> = args
            .iter()
            .map(|a| match a {
                Expr::Lit(syn::ExprLit {
                    lit: Lit::Str(s), ..
                }) => Some(s.value()),
                _ => None,
            })
            .collect();
        if let (true, Some(parts)) = (m.path.is_ident("concat"), parts) {
            self.index.literals.push(Literal {
                function: self.function.clone(),
                file: self.file,
                start: m.span().start().into(),
                end: m.span().end().into(),
                value: parts.concat(),
            });
            return;
        }
        for a in &args {
            self.visit_expr(a);
        }
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
            start: sig.span().start().into(),
            ..FnFacts::default()
        };
        for input in &sig.inputs {
            match input {
                FnArg::Receiver(_) => facts.has_self = true,
                FnArg::Typed(t) => {
                    let name = pat_ident(&t.pat);
                    if let Some(n) = &name {
                        if is_write_tx(&t.ty) {
                            facts.tx_params.insert(n.clone());
                            facts.lock_from = Some(Pos { line: 0, column: 0 });
                        }
                        if is_integer_type(&t.ty) {
                            facts.integers.insert(n.clone());
                        }
                        if is_pool_type(&t.ty) {
                            facts.pool_params.insert(n.clone());
                        }
                    }
                    facts.params.push(name);
                }
            }
        }
        facts.returns.extend(tail(block).cloned());
        let id = self.index.fns.len();
        self.index.fns.push(facts);
        Body {
            index: self.index,
            id,
            spawn_depth: 0,
            guard: Guard::new(),
            scopes: Vec::new(),
        }
        .visit_block(block);
    }

    /// A `macro_rules!` body that names sqlx: an opaque sink.
    fn macro_rules(&mut self, m: &syn::ItemMacro) {
        if self.consts_only || !m.mac.path.is_ident("macro_rules") || !mentions_sqlx(&m.mac.tokens)
        {
            return;
        }
        let name = m
            .ident
            .as_ref()
            .map_or_else(String::new, ToString::to_string);
        let id = self.index.fns.len();
        self.index.fns.push(FnFacts {
            file: self.file,
            name: format!("macro_rules! {name}"),
            ..FnFacts::default()
        });
        self.index.sinks.push(Sink {
            function: id,
            pos: m.span().start().into(),
            arg: Expr::Verbatim(m.mac.tokens.clone()),
            executor: None,
            candidate: false,
            in_spawn: false,
        });
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

    fn visit_item_macro(&mut self, m: &'ast syn::ItemMacro) {
        self.macro_rules(m);
        visit::visit_item_macro(self, m);
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
    /// Inside the arguments of a `spawn` call.
    spawn_depth: usize,
    /// The branch arms around the current expression.
    guard: Guard,
    /// The spans of the blocks around the current expression.
    scopes: Vec<(Pos, Pos)>,
}

impl Body<'_> {
    fn facts(&mut self) -> &mut FnFacts {
        &mut self.index.fns[self.id]
    }

    fn lock_at(&mut self, pos: Pos) {
        let f = self.facts();
        if f.lock_from.is_none_or(|p| pos < p) {
            f.lock_from = Some(pos);
        }
    }

    fn file(&self) -> usize {
        self.index.fns[self.id].file
    }

    fn sink(&mut self, pos: Pos, arg: Expr, executor: Option<Expr>, candidate: bool) {
        self.index.sinks.push(Sink {
            function: self.id,
            pos,
            arg,
            executor,
            candidate,
            in_spawn: self.spawn_depth > 0,
        });
    }

    fn bind(&mut self, name: String, pos: Pos, is_let: bool, binding: Binding) {
        let tx = matches!(&binding, Binding::Value(v) if is_begin_write(v));
        let whole = (
            Pos::default(),
            Pos {
                line: usize::MAX,
                column: usize::MAX,
            },
        );
        let here = self.scopes.last().copied().unwrap_or(whole);
        // An assignment updates the binding it assigns to, in that one's
        // block, not the block the assignment sits in.
        let scope = if is_let {
            here
        } else {
            let visible = |at: Pos, scope: (Pos, Pos)| at < pos && scope.0 <= pos && pos <= scope.1;
            let facts = &self.index.fns[self.id];
            let bound = facts.locals.get(&name).and_then(|ls| {
                ls.iter()
                    .rev()
                    .find(|l| l.is_let && visible(l.pos, l.scope))
                    .map(|l| (l.pos, l.scope))
            });
            let declared = facts
                .declared
                .get(&name)
                .and_then(|ds| ds.iter().rev().find(|(at, sc)| visible(*at, *sc)).copied());
            bound
                .into_iter()
                .chain(declared)
                .max()
                .map_or(here, |(_, sc)| sc)
        };
        self.facts().locals.entry(name).or_default().push(Local {
            pos,
            is_let,
            tx,
            binding,
            scope,
        });
    }

    /// A `let` or assignment of `e`: the `.begin()` calls it binds, and a
    /// transaction it renames or reborrows stays in view.
    fn bound(&mut self, e: &Expr) {
        let mut begins = Vec::new();
        begin_calls(e, &mut begins);
        let ok = executor_ident(e).map(|(_, p)| p);
        let f = self.facts();
        f.bound_begins.extend(begins);
        f.tx_ok.extend(ok);
    }

    fn append(&mut self, name: String, e: Expr) {
        let guard = self.guard.clone();
        self.facts()
            .appends
            .entry(name)
            .or_default()
            .push((e, guard));
    }

    /// A new branch construct with `arms` arms.
    fn construct(&mut self) -> usize {
        self.index.constructs += 1;
        self.index.constructs
    }

    /// Walks `walk` inside arm `arm` of construct `id`.
    fn in_arm(&mut self, id: usize, arm: usize, arms: usize, walk: impl FnOnce(&mut Self)) {
        self.guard.push((id, arm, arms));
        walk(self);
        self.guard.pop();
    }

    /// The sink call at the root of a method chain (`sqlx::query(..).bind(..)`),
    /// or the name or closure call there.
    fn chain_root(&self, mut e: &Expr) -> Option<LinkTarget> {
        loop {
            match strip(e) {
                Expr::MethodCall(m) => e = &m.receiver,
                Expr::Await(a) => e = &a.base,
                Expr::Try(t) => e = &t.expr,
                Expr::Call(c) => {
                    let Expr::Path(p) = &*c.func else { return None };
                    if self.index.is_sink_path(self.file(), &p.path) {
                        return Some(LinkTarget::Sink(c.span().start().into()));
                    }
                    return p
                        .path
                        .get_ident()
                        .map(|i| LinkTarget::Closure(i.to_string()));
                }
                Expr::Path(p) => {
                    return p.path.get_ident().map(|i| LinkTarget::Name(i.to_string()))
                }
                _ => return None,
            }
        }
    }

    /// Walks token trees a macro body does not parse into: brace groups
    /// that parse as statements are walked; a `sqlx` mention elsewhere is
    /// an opaque sink.
    fn opaque_tokens(&mut self, ts: &TokenStream) -> bool {
        let mut opaque = false;
        for t in ts.clone() {
            match t {
                TokenTree::Group(g) if g.delimiter() == Delimiter::Brace => {
                    match syn::parse::Parser::parse2(syn::Block::parse_within, g.stream()) {
                        Ok(stmts) => {
                            for s in &stmts {
                                self.visit_stmt(s);
                            }
                        }
                        Err(_) => opaque |= self.opaque_tokens(&g.stream()),
                    }
                }
                TokenTree::Group(g) => opaque |= self.opaque_tokens(&g.stream()),
                TokenTree::Ident(i) if i == "sqlx" => opaque = true,
                _ => {}
            }
        }
        opaque
    }
}

impl<'ast> Visit<'ast> for Body<'_> {
    fn visit_item(&mut self, _: &'ast syn::Item) {}

    fn visit_block(&mut self, b: &'ast syn::Block) {
        self.scopes
            .push((b.span().start().into(), b.span().end().into()));
        visit::visit_block(self, b);
        self.scopes.pop();
    }

    fn visit_local(&mut self, l: &'ast syn::Local) {
        let typed_tx = matches!(&l.pat, Pat::Type(t) if is_write_tx(&t.ty));
        if typed_tx {
            self.lock_at(l.span().start().into());
        }
        if l.init.is_none() {
            if let Some(name) = pat_ident(&l.pat) {
                let here = self.scopes.last().copied().unwrap_or_default();
                let at = l.span().end().into();
                self.facts()
                    .declared
                    .entry(name)
                    .or_default()
                    .push((at, here));
            }
        }
        if let Some(init) = &l.init {
            let init_expr = &*init.expr;
            if let Some(name) = pat_ident(&l.pat) {
                // A `begin()` under any other pattern (`let Ok(tx) = ..
                // else`, a tuple) is not bound: it fails as an escape.
                self.bound(init_expr);
                if let Expr::Closure(c) = init_expr {
                    let span = (c.span().start().into(), c.span().end().into());
                    self.facts().closures.push((name.clone(), span.0, span.1));
                    for (index, input) in c.inputs.iter().enumerate() {
                        if let Some(param) = pat_ident(input) {
                            let b = Binding::ClosureParam {
                                closure: name.clone(),
                                index,
                            };
                            self.bind(param, span.0, true, b);
                        }
                    }
                }
                if let Some(LinkTarget::Sink(pos)) = self.chain_root(init_expr) {
                    self.facts().sink_names.push((name.clone(), pos));
                }
                let b = Binding::Value(init_expr.clone());
                self.bind(name.clone(), l.span().end().into(), true, b);
                if typed_tx {
                    if let Some(last) = self
                        .facts()
                        .locals
                        .get_mut(&name)
                        .and_then(|v| v.last_mut())
                    {
                        last.tx = true;
                    }
                }
            } else {
                let mut names = Vec::new();
                tuple_idents(&l.pat, &mut Vec::new(), &mut names);
                for (name, path) in names {
                    let b = Binding::Field {
                        init: init_expr.clone(),
                        path,
                    };
                    self.bind(name, l.span().end().into(), true, b);
                }
            }
        }
        visit::visit_local(self, l);
    }

    fn visit_expr_if(&mut self, i: &'ast syn::ExprIf) {
        let id = self.construct();
        self.visit_expr(&i.cond);
        self.in_arm(id, 0, 2, |me| me.visit_block(&i.then_branch));
        if let Some((_, els)) = &i.else_branch {
            self.in_arm(id, 1, 2, |me| me.visit_expr(els));
        }
    }

    fn visit_expr_match(&mut self, m: &'ast syn::ExprMatch) {
        let id = self.construct();
        self.visit_expr(&m.expr);
        let arms = m.arms.len();
        for (k, arm) in m.arms.iter().enumerate() {
            self.in_arm(id, k, arms, |me| me.visit_arm(arm));
        }
    }

    fn visit_expr_while(&mut self, w: &'ast syn::ExprWhile) {
        let span = (w.span().start().into(), w.span().end().into());
        self.facts().loops.push(span);
        let id = self.construct();
        self.visit_expr(&w.cond);
        self.in_arm(id, 0, 2, |me| me.visit_block(&w.body));
    }

    fn visit_expr_loop(&mut self, l: &'ast syn::ExprLoop) {
        let span = (l.span().start().into(), l.span().end().into());
        self.facts().loops.push(span);
        let id = self.construct();
        self.in_arm(id, 0, 2, |me| me.visit_block(&l.body));
    }

    fn visit_expr_for_loop(&mut self, f: &'ast syn::ExprForLoop) {
        let mut names = Vec::new();
        tuple_idents(&f.pat, &mut Vec::new(), &mut names);
        for (name, path) in names {
            let b = Binding::Element {
                iter: (*f.expr).clone(),
                path,
            };
            self.bind(name, f.span().start().into(), true, b);
        }
        self.visit_expr(&f.expr);
        let span = (f.body.span().start().into(), f.body.span().end().into());
        self.facts().loops.push(span);
        let id = self.construct();
        self.in_arm(id, 0, 2, |me| me.visit_block(&f.body));
    }

    fn visit_expr_assign(&mut self, a: &'ast syn::ExprAssign) {
        // Only an assignment to a local renames a transaction; one to a
        // field or through `*` moves it out of view.
        if let Expr::Path(p) = &*a.left {
            if p.path.get_ident().is_some() {
                self.bound(&a.right);
                // The name assigned to is a binding, not a use.
                let pos = p.span().start().into();
                self.facts().tx_ok.insert(pos);
            }
        }
        if let Expr::Path(p) = &*a.left {
            if let Some(name) = p.path.get_ident() {
                let right = Binding::Value((*a.right).clone());
                self.bind(name.to_string(), a.span().end().into(), false, right);
            }
        }
        visit::visit_expr_assign(self, a);
    }

    fn visit_expr_binary(&mut self, b: &'ast syn::ExprBinary) {
        if let (BinOp::AddAssign(_), Expr::Path(p)) = (&b.op, &*b.left) {
            if let Some(name) = p.path.get_ident() {
                self.append(name.to_string(), (*b.right).clone());
            }
        }
        visit::visit_expr_binary(self, b);
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
            self.lock_at(m.span().start().into());
        }
        if name == "begin" && m.args.is_empty() {
            let pos = m.method.span().start().into();
            self.facts().begins.push(pos);
        }
        let exec = EXEC_METHODS.contains(&name.as_str());
        if exec || TX_END_METHODS.contains(&name.as_str()) {
            if let Some((_, p)) = executor_ident(&m.receiver) {
                self.facts().tx_ok.insert(p);
            }
        }
        for a in &m.args {
            if let Some((_, p)) = executor_ident(a) {
                if exec {
                    self.facts().tx_ok.insert(p);
                } else {
                    self.facts().arg_uses.push((p, None, name.clone(), true));
                }
            }
        }
        if let Expr::Path(p) = &*m.receiver {
            if let (Some(recv), Some(arg)) = (p.path.get_ident(), m.args.first()) {
                match name.as_str() {
                    "push_str" | "push" => self.append(recv.to_string(), arg.clone()),
                    "push_bind" => self.append(recv.to_string(), syn::parse_quote!("$1")),
                    _ => {}
                }
            }
        }
        if EXEC_METHODS.contains(&name.as_str()) {
            if let Some(arg) = m.args.first() {
                if let Some(target) = self.chain_root(&m.receiver) {
                    self.index.links.push(ExecLink {
                        function: self.id,
                        target,
                        executor: arg.clone(),
                    });
                }
                let pos = m.span().start().into();
                self.sink(pos, arg.clone(), Some((*m.receiver).clone()), true);
            }
        }
        let caller = self.id;
        self.index.calls.push(CallSite {
            module: None,
            name,
            caller,
            method: true,
            args: m.args.iter().cloned().collect(),
            pos: m.span().start().into(),
        });
        visit::visit_expr_method_call(self, m);
    }

    fn visit_expr_call(&mut self, c: &'ast syn::ExprCall) {
        let Expr::Path(p) = &*c.func else {
            return visit::visit_expr_call(self, c);
        };
        let (module, name) = split_path(&p.path);
        let pos: Pos = c.span().start().into();
        let executor_path = p.path.segments.iter().any(|s| s.ident == "Executor")
            && EXEC_METHODS.contains(&name.as_str());
        if executor_path {
            if let (Some(ex), Some(arg)) = (c.args.first(), c.args.get(1)) {
                self.sink(pos, arg.clone(), Some(ex.clone()), false);
            }
            if let Some((_, p)) = c.args.first().and_then(executor_ident) {
                self.facts().tx_ok.insert(p);
            }
        } else if self.index.is_sink_path(self.file(), &p.path) {
            if let Some(arg) = c.args.first() {
                self.sink(pos, arg.clone(), None, false);
            }
        } else {
            for a in &c.args {
                if let Some((_, p)) = executor_ident(a) {
                    if name == "drop" {
                        self.facts().tx_ok.insert(p);
                    } else {
                        let use_ = (p, module.clone(), name.clone(), false);
                        self.facts().arg_uses.push(use_);
                    }
                }
            }
            let caller = self.id;
            self.index.calls.push(CallSite {
                module,
                name: name.clone(),
                caller,
                method: false,
                args: c.args.iter().cloned().collect(),
                pos,
            });
        }
        let spawn = SPAWNS.contains(&name.as_str());
        self.spawn_depth += usize::from(spawn);
        for a in &c.args {
            self.visit_expr(a);
        }
        self.spawn_depth -= usize::from(spawn);
    }

    /// A sink function used as a value (`.map(sqlx::query)`).
    fn visit_expr_path(&mut self, p: &'ast syn::ExprPath) {
        if let Some(i) = p.path.get_ident() {
            let use_ = (i.to_string(), p.span().start().into());
            self.facts().idents.push(use_);
        }
        if p.path.segments.len() > 1 && self.index.is_sink_path(self.file(), &p.path) {
            let pos = p.span().start().into();
            self.sink(pos, Expr::Verbatim(p.to_token_stream()), None, false);
        }
    }

    /// Macro bodies are opaque to syn: a sqlx macro is a sink, a macro
    /// whose body parses as expressions (`format!`, `vec!`) is walked, and
    /// one that names sqlx where its body cannot be read is an opaque sink.
    fn visit_macro(&mut self, m: &'ast syn::Macro) {
        let segs: Vec<String> = m
            .path
            .segments
            .iter()
            .map(|s| s.ident.to_string())
            .collect();
        let name = segs.last().cloned().unwrap_or_default();
        let pos: Pos = m.span().start().into();
        let Ok(args) = m.parse_body_with(Punctuated::<Expr, Token![,]>::parse_terminated) else {
            if self.opaque_tokens(&m.tokens) {
                self.sink(pos, Expr::Verbatim(m.to_token_stream()), None, false);
            }
            return;
        };
        let sqlx_macro = (segs.len() == 1 || segs.iter().any(|s| s == "sqlx"))
            && SINK_MACROS.contains(&name.as_str());
        if sqlx_macro {
            let sql = args
                .iter()
                .find(|a| matches!(a, Expr::Lit(l) if matches!(l.lit, Lit::Str(_))));
            let arg = sql
                .cloned()
                .unwrap_or_else(|| Expr::Verbatim(m.tokens.clone()));
            self.sink(pos, arg, None, false);
        }
        if name == "write" || name == "writeln" {
            if let Some(Expr::Path(p)) = args.first().map(strip) {
                if let Some(target) = p.path.get_ident() {
                    let rest: Vec<&Expr> = args.iter().skip(1).collect();
                    let fmt: Expr = syn::parse_quote!(format!(#(#rest),*));
                    self.append(target.to_string(), fmt);
                }
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
