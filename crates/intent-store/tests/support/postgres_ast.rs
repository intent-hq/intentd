//! The Postgres tenant rule on parsed SQL (sqlparser, Postgres dialect).
//!
//! For every SELECT level, each table in FROM or JOIN that is not a CTE in
//! scope needs a `tenant_id = $n` (or `<alias>.tenant_id = $n`, either
//! operand order) conjunct in that level's WHERE, or in the ON of a join
//! that drops its unmatched rows (the right side of a LEFT JOIN, either
//! side of an inner join, the left side of a RIGHT JOIN; never a FULL
//! JOIN side). An INSERT's column list includes `tenant_id`, and so does its
//! ON CONFLICT target. An UPDATE or DELETE target needs the filter in its
//! WHERE; SET lists do not count. Subqueries anywhere in a statement are
//! checked as their own level. A CTE name is in scope after its body, and
//! in every sibling body under WITH RECURSIVE; an UPDATE or DELETE target is
//! always the real table. A statement or table shape the rule does not
//! model is a violation, not a pass.
//!
//! Limit: the rule sees placeholders, not what is bound to them. It
//! requires every tenant filter in a statement to use the same `$n`; that
//! the tenant is bound there is for the Postgres parity tests to show.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, ConflictTarget, Expr, FromTable, Insert, JoinConstraint, JoinOperator,
    ObjectName, OnConflictAction, OnInsert, Query, Select, SetExpr, Statement, TableAlias,
    TableFactor, TableObject, TableWithJoins, UpdateTableFromKind, Value, Visit, Visitor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

pub fn parse(sql: &str) -> Result<Vec<Statement>, String> {
    Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|e| e.to_string())
}

/// Fillers tried, in order, for a `format!` hole that no const resolves:
/// an expression, nothing (an optional clause), an identifier, a subquery
/// body, a VALUES row and an operator. A filler names no table, but it does
/// stand in for the hole's runtime text, which the rule never sees here:
/// that text is checked through its own literals, where it has any.
const FILLERS: &[&str] = &["true", "", "x", "select 1", "($1)", "=", "x = x"];

/// Schemas of the Postgres system catalogs, which hold no tenant rows.
const SYSTEM_SCHEMAS: &[&str] = &["pg_catalog", "information_schema"];

/// Parses SQL given as pieces: `Some(text)`, or `None` for a `format!`
/// hole the lint could not resolve. The holes try every [`FILLERS`]
/// combination (up to four holes; more get one filler each) until one
/// parses. The error is the first attempt's.
pub fn parse_pieces(pieces: &[Option<String>]) -> Result<Vec<Statement>, String> {
    let mut parts = vec![String::new()];
    for p in pieces {
        match p {
            Some(text) => parts.last_mut().unwrap().push_str(text),
            None => parts.push(String::new()),
        }
    }
    let holes = parts.len() - 1;
    let build = |choice: &[usize]| {
        let mut out = parts[0].clone();
        for (i, part) in parts[1..].iter().enumerate() {
            out.push(' ');
            out.push_str(FILLERS[choice[i]]);
            out.push(' ');
            out.push_str(part);
        }
        out
    };
    let first = parse(&build(&vec![0; holes]));
    if first.is_ok() || holes == 0 {
        return first;
    }
    let tries: Vec<Vec<usize>> = if holes <= 4 {
        (0..FILLERS.len().pow(u32::try_from(holes).unwrap()))
            .map(|mut n| {
                (0..holes)
                    .map(|_| {
                        let d = n % FILLERS.len();
                        n /= FILLERS.len();
                        d
                    })
                    .collect()
            })
            .collect()
    } else {
        (0..FILLERS.len()).map(|f| vec![f; holes]).collect()
    };
    tries
        .iter()
        .find_map(|c| parse(&build(c)).ok())
        .ok_or_else(|| first.unwrap_err())
}

/// The tenant violations in parsed statements, as `<KEYWORD> <table>`
/// (`FROM note`, `JOIN task`, `INTO settings`, `UPDATE hook`). Tables in
/// `exempt` belong to the schema, not a tenant, as do the system catalogs.
pub fn violations(stmts: &[Statement], exempt: &[&str]) -> Vec<String> {
    let mut rule = Rule {
        exempt,
        out: Vec::new(),
        params: BTreeSet::new(),
    };
    for s in stmts {
        rule.params.clear();
        rule.statement(s, &Scope::new());
        if rule.params.len() > 1 {
            let params: Vec<_> = rule.params.iter().map(String::as_str).collect();
            rule.out.push(format!("TENANT PARAMS {}", params.join(" ")));
        }
    }
    rule.out
}

/// Whether `sql` (a rendered SET statement) sets a `__neki.` setting to
/// a literal.
fn is_neki_setting(sql: &str) -> bool {
    let lower = sql.to_lowercase();
    let Some(rest) = lower.strip_prefix("set __neki.") else {
        return false;
    };
    let Some((name, value)) = rest.split_once('=') else {
        return false;
    };
    let value = value.trim();
    name.trim()
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        && value.len() >= 2
        && value.starts_with('\'')
        && value.ends_with('\'')
        && !value[1..value.len() - 1].contains('\'')
}

/// Whether `text` is a statement (SELECT, INSERT, UPDATE, DELETE, WITH,
/// MERGE, TRUNCATE), read as is or as a `format!` string with its holes
/// filled.
pub fn is_statement(text: &str) -> bool {
    let dml = |stmts: Vec<Statement>| {
        stmts.iter().any(|s| {
            matches!(
                s,
                Statement::Query(_)
                    | Statement::Insert(_)
                    | Statement::Update(_)
                    | Statement::Delete(_)
                    | Statement::Merge(_)
                    | Statement::Truncate(_)
            )
        })
    };
    let head = text.trim_start().to_lowercase();
    let keyword = [
        "select", "insert", "update", "delete", "with", "merge", "truncate",
    ]
    .iter()
    .any(|k| head.trim_start_matches(['(', ' ']).starts_with(k));
    if !keyword {
        return false;
    }
    if parse(text).is_ok_and(dml) {
        return true;
    }
    if !text.contains('{') {
        return false;
    }
    let mut pieces = Vec::new();
    let mut rest = text;
    while let Some(at) = rest.find('{') {
        pieces.push(Some(rest[..at].to_owned()));
        pieces.push(None);
        rest = rest[at..].find('}').map_or("", |end| &rest[at + end + 1..]);
    }
    pieces.push(Some(rest.to_owned()));
    parse_pieces(&pieces).is_ok_and(dml)
}

/// The tables the statements write (INSERT, UPDATE, DELETE, also inside a
/// data-modifying CTE), with MERGE and TRUNCATE as `merge` and `truncate`.
/// Names are lower-cased, unqualified ones bare.
pub fn written_tables(stmts: &[Statement]) -> Vec<String> {
    let mut w = Writes::default();
    for s in stmts {
        let _ = s.visit(&mut w);
    }
    w.tables
}

#[derive(Default)]
struct Writes {
    tables: Vec<String>,
}

/// The functions the statements call, anywhere in them (`SELECT f($1)`,
/// `FROM f($1)`, inside a CTE or a subquery). Names are lower-cased and
/// without their schema (`public.f` is `f`), as the migrations define them.
pub fn called_functions(stmts: &[Statement]) -> Vec<String> {
    let mut calls = Calls::default();
    for s in stmts {
        let _ = s.visit(&mut calls);
    }
    calls.names
}

#[derive(Default)]
struct Calls {
    names: Vec<String>,
}

impl Visitor for Calls {
    type Break = ();

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        if let Expr::Function(f) = e {
            self.names.push(bare_name(&f.name));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        match t {
            TableFactor::Function { name, .. }
            | TableFactor::Table {
                name,
                args: Some(_),
                ..
            } => self.names.push(bare_name(name)),
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

impl Visitor for Writes {
    type Break = ();

    fn pre_visit_statement(&mut self, s: &Statement) -> ControlFlow<()> {
        let mut refs = Refs::default();
        match s {
            Statement::Insert(i) => match &i.table {
                TableObject::TableName(name) => self.tables.push(display_name(name)),
                other => self.tables.push(other.to_string().to_lowercase()),
            },
            Statement::Update(u) => table_refs(&u.table, "UPDATE", &mut refs),
            Statement::Delete(d) => {
                let (FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from)) = &d.from;
                for t in from {
                    table_refs(t, "FROM", &mut refs);
                }
            }
            Statement::Merge(_) => self.tables.push("merge".to_owned()),
            Statement::Truncate(_) => self.tables.push("truncate".to_owned()),
            _ => {}
        }
        // An UPDATE writes only its first table; the rest are joined reads.
        let n = if matches!(s, Statement::Update(_)) {
            1
        } else {
            usize::MAX
        };
        self.tables
            .extend(refs.tables.iter().take(n).map(|r| display_name(r.name)));
        self.tables.extend(refs.unsupported);
        ControlFlow::Continue(())
    }
}

/// CTE names visible at a point of the statement.
type Scope = BTreeSet<String>;

struct Rule<'a> {
    exempt: &'a [&'a str],
    out: Vec<String>,
    /// The placeholders the current statement's tenant filters use.
    params: BTreeSet<String>,
}

/// The tables a level reads, and the table shapes it does not model.
#[derive(Default)]
struct Refs<'a> {
    tables: Vec<TableRef<'a>>,
    unsupported: Vec<String>,
}

/// A table this level reads, with the JOIN ON clauses that can filter it.
struct TableRef<'a> {
    keyword: &'static str,
    /// The table an UPDATE or DELETE changes: always a real table, even
    /// where a CTE of that name is in scope.
    target: bool,
    name: &'a ObjectName,
    alias: Option<&'a TableAlias>,
    on: Vec<&'a Expr>,
}

impl Rule<'_> {
    fn statement(&mut self, s: &Statement, scope: &Scope) {
        match s {
            Statement::Query(q) => self.query(q, scope),
            Statement::Insert(i) => self.insert(i, scope),
            Statement::Update(u) => {
                let mut refs = Refs::default();
                table_refs(&u.table, "UPDATE", &mut refs);
                if let Some(r) = refs.tables.first_mut() {
                    r.target = true;
                }
                if let Some(
                    UpdateTableFromKind::AfterSet(from) | UpdateTableFromKind::BeforeSet(from),
                ) = &u.from
                {
                    for t in from {
                        table_refs(t, "FROM", &mut refs);
                    }
                }
                self.check(refs, u.selection.as_ref(), scope);
                self.nested(u, scope);
            }
            Statement::Delete(d) => {
                let mut refs = Refs::default();
                let (FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from)) = &d.from;
                for t in from {
                    table_refs(t, "FROM", &mut refs);
                }
                for r in &mut refs.tables {
                    r.target = true;
                }
                for t in d.using.iter().flatten() {
                    table_refs(t, "USING", &mut refs);
                }
                self.check(refs, d.selection.as_ref(), scope);
                self.nested(d, scope);
            }
            // The migration ledger's own table: schema, not tenant, data.
            Statement::CreateTable(c) if self.is_exempt(&c.name) => {}
            // A Neki session setting (`SET __neki.tx_mode = ...`): it reads
            // and writes no table. Any other SET stays unsupported.
            Statement::Set(_) if is_neki_setting(&s.to_string()) => {}
            other => self.unsupported(&other.to_string()),
        }
    }

    /// A shape the rule does not model: reported, so it cannot pass unseen.
    fn unsupported(&mut self, sql: &str) {
        let head: Vec<&str> = sql.split_whitespace().take(2).collect();
        self.out.push(format!("UNSUPPORTED {}", head.join(" ")));
    }

    fn insert(&mut self, i: &Insert, scope: &Scope) {
        if let TableObject::TableName(name) = &i.table {
            let has_tenant = i
                .columns
                .iter()
                .any(|c| last_ident(c).is_some_and(|n| n == "tenant_id"));
            if !has_tenant && !self.is_exempt(name) {
                self.out.push(format!("INTO {}", display_name(name)));
            }
            // A conflict target without tenant_id matches other tenants'
            // rows. `ON CONFLICT DO NOTHING` with no target only skips.
            if let Some(OnInsert::OnConflict(c)) = &i.on {
                let scoped = match &c.conflict_target {
                    Some(ConflictTarget::Columns { columns, .. }) => columns
                        .iter()
                        .any(|c| c.value.eq_ignore_ascii_case("tenant_id")),
                    Some(ConflictTarget::OnConstraint(_)) => false,
                    None => matches!(c.action, OnConflictAction::DoNothing),
                };
                if !scoped && !self.is_exempt(name) {
                    self.out.push(format!("ON CONFLICT {}", display_name(name)));
                }
            }
        } else {
            self.unsupported(&i.to_string());
        }
        self.nested(i, scope);
    }

    fn query(&mut self, q: &Query, scope: &Scope) {
        let mut scope = scope.clone();
        if let Some(with) = &q.with {
            // WITH RECURSIVE makes every sibling visible in every body; a
            // plain WITH shows each name only after its own body.
            if with.recursive {
                scope.extend(
                    with.cte_tables
                        .iter()
                        .map(|c| c.alias.name.value.to_lowercase()),
                );
            }
            for cte in &with.cte_tables {
                self.query(&cte.query, &scope);
                scope.insert(cte.alias.name.value.to_lowercase());
            }
        }
        self.set_expr(&q.body, &scope);
        self.nested(&q.order_by, &scope);
        self.nested(&q.limit_clause, &scope);
    }

    fn set_expr(&mut self, body: &SetExpr, scope: &Scope) {
        match body {
            SetExpr::Select(s) => self.select(s, scope),
            SetExpr::Query(q) => self.query(q, scope),
            SetExpr::SetOperation { left, right, .. } => {
                self.set_expr(left, scope);
                self.set_expr(right, scope);
            }
            SetExpr::Insert(s) | SetExpr::Update(s) | SetExpr::Delete(s) | SetExpr::Merge(s) => {
                self.statement(s, scope);
            }
            SetExpr::Values(v) => self.nested(v, scope),
            // `TABLE note`: every row of the table, no filter possible.
            SetExpr::Table(t) => {
                let name = t.table_name.as_ref().map(|n| n.value.to_lowercase());
                let cte =
                    t.schema_name.is_none() && name.as_ref().is_some_and(|n| scope.contains(n));
                let system = t
                    .schema_name
                    .as_ref()
                    .is_some_and(|s| SYSTEM_SCHEMAS.contains(&s.value.to_lowercase().as_str()));
                let exempt = t.schema_name.is_none()
                    && name.as_deref().is_some_and(|n| self.exempt.contains(&n));
                if !(cte || system || exempt) {
                    self.out.push(format!(
                        "FROM {}",
                        t.to_string().to_lowercase().trim_start_matches("table ")
                    ));
                }
            }
        }
    }

    fn select(&mut self, s: &Select, scope: &Scope) {
        let mut refs = Refs::default();
        for t in &s.from {
            table_refs(t, "FROM", &mut refs);
        }
        self.check(refs, s.selection.as_ref(), scope);
        self.nested(s, scope);
    }

    /// Checks each table against the level's WHERE and its own JOIN ONs.
    /// A one-part name that a CTE in scope defines is that CTE.
    fn check(&mut self, refs: Refs<'_>, selection: Option<&Expr>, scope: &Scope) {
        for u in &refs.unsupported {
            self.unsupported(u);
        }
        for r in refs.tables {
            let cte = !r.target
                && r.name.0.len() == 1
                && last_ident(r.name).is_some_and(|n| scope.contains(&n));
            if cte || self.is_exempt(r.name) {
                continue;
            }
            let alias = r.alias.map_or_else(
                || last_ident(r.name).unwrap_or_default(),
                |a| a.name.value.to_lowercase(),
            );
            let param = tenant_param(selection, &alias)
                .or_else(|| r.on.iter().find_map(|on| tenant_param(Some(on), &alias)));
            let ok = param.is_some();
            self.params.extend(param);
            if !ok {
                self.out
                    .push(format!("{} {}", r.keyword, display_name(r.name)));
            }
        }
    }

    /// Checks every subquery directly inside `node` (not those nested in
    /// another subquery, which their parent checks) in `scope`.
    fn nested<V: Visit>(&mut self, node: &V, scope: &Scope) {
        let mut found = Subqueries::default();
        let _ = node.visit(&mut found);
        for q in &found.queries {
            self.query(q, scope);
        }
    }

    fn is_exempt(&self, name: &ObjectName) -> bool {
        let system = name.0.len() == 2
            && name.0[0]
                .as_ident()
                .is_some_and(|s| SYSTEM_SCHEMAS.contains(&s.value.to_lowercase().as_str()));
        system
            || (name.0.len() == 1
                && last_ident(name).is_some_and(|n| self.exempt.contains(&n.as_str())))
    }
}

/// The tables of one FROM item: its relation, its joins, and the tables of
/// parenthesized joins. Each join's ON is attached to the tables on the
/// side or sides whose unmatched rows that join drops.
fn table_refs<'a>(t: &'a TableWithJoins, keyword: &'static str, out: &mut Refs<'a>) {
    let start = out.tables.len();
    relation(&t.relation, keyword, out);
    for j in &t.joins {
        let right = out.tables.len();
        relation(&j.relation, "JOIN", out);
        let (filters_left, filters_right) = filtered_sides(&j.join_operator);
        if let Some(on) = join_on(&j.join_operator) {
            let left_side = start..right;
            let right_side = right..out.tables.len();
            for (i, r) in out.tables.iter_mut().enumerate().skip(start) {
                if (filters_left && left_side.contains(&i))
                    || (filters_right && right_side.contains(&i))
                {
                    r.on.push(on);
                }
            }
        }
    }
}

fn relation<'a>(f: &'a TableFactor, keyword: &'static str, out: &mut Refs<'a>) {
    match f {
        // Subqueries and table functions (`json_each($2)`,
        // `generate_series(..)`) name no table at this level;
        // `Rule::nested` checks the subqueries.
        TableFactor::Table { args: Some(_), .. }
        | TableFactor::Derived { .. }
        | TableFactor::TableFunction { .. }
        | TableFactor::Function { .. }
        | TableFactor::UNNEST { .. }
        | TableFactor::JsonTable { .. } => {}
        TableFactor::Table { name, alias, .. } => out.tables.push(TableRef {
            keyword,
            target: false,
            name,
            alias: alias.as_ref(),
            on: Vec::new(),
        }),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => table_refs(table_with_joins, keyword, out),
        // PIVOT, MATCH_RECOGNIZE and the like: not modelled.
        other => out.unsupported.push(other.to_string()),
    }
}

/// Which sides of a join lose their unmatched rows: (left, right).
fn filtered_sides(op: &JoinOperator) -> (bool, bool) {
    match op {
        JoinOperator::Join(_)
        | JoinOperator::Inner(_)
        | JoinOperator::CrossJoin(_)
        | JoinOperator::StraightJoin(_) => (true, true),
        JoinOperator::Left(_) | JoinOperator::LeftOuter(_) => (false, true),
        JoinOperator::Right(_) | JoinOperator::RightOuter(_) => (true, false),
        _ => (false, false),
    }
}

fn join_on(op: &JoinOperator) -> Option<&Expr> {
    let (JoinOperator::Join(c)
    | JoinOperator::Inner(c)
    | JoinOperator::Left(c)
    | JoinOperator::LeftOuter(c)
    | JoinOperator::Right(c)
    | JoinOperator::RightOuter(c)
    | JoinOperator::FullOuter(c)
    | JoinOperator::CrossJoin(c)
    | JoinOperator::Semi(c)
    | JoinOperator::LeftSemi(c)
    | JoinOperator::RightSemi(c)
    | JoinOperator::Anti(c)
    | JoinOperator::LeftAnti(c)
    | JoinOperator::RightAnti(c)
    | JoinOperator::StraightJoin(c)
    | JoinOperator::AsOf { constraint: c, .. }) = op
    else {
        return None;
    };
    match c {
        JoinConstraint::On(e) => Some(e),
        _ => None,
    }
}

/// The placeholder of a top-level AND conjunct of `e` that is
/// `tenant_id = $n` for `alias`.
fn tenant_param(e: Option<&Expr>, alias: &str) -> Option<String> {
    let mut stack: Vec<&Expr> = e.into_iter().collect();
    while let Some(e) = stack.pop() {
        match e {
            Expr::Nested(inner) => stack.push(inner),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                stack.push(left);
                stack.push(right);
            }
            Expr::BinaryOp {
                left,
                op: BinaryOperator::Eq,
                right,
            } => {
                let pair = |col: &Expr, val: &Expr| {
                    is_tenant_column(col, alias).then(|| param(val)).flatten()
                };
                if let Some(p) = pair(left, right).or_else(|| pair(right, left)) {
                    return Some(p);
                }
            }
            _ => {}
        }
    }
    None
}

fn is_tenant_column(e: &Expr, alias: &str) -> bool {
    match e {
        Expr::Identifier(i) => i.value.eq_ignore_ascii_case("tenant_id"),
        Expr::CompoundIdentifier(parts) => {
            parts.len() == 2
                && parts[0].value.eq_ignore_ascii_case(alias)
                && parts[1].value.eq_ignore_ascii_case("tenant_id")
        }
        Expr::Nested(inner) => is_tenant_column(inner, alias),
        _ => false,
    }
}

fn param(e: &Expr) -> Option<String> {
    match e {
        Expr::Value(v) => match &v.value {
            Value::Placeholder(p) if p.starts_with('$') => Some(p.clone()),
            _ => None,
        },
        Expr::Nested(inner) => param(inner),
        _ => None,
    }
}

fn last_ident(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.to_lowercase())
}

/// [`display_name`] without a schema qualifier.
fn bare_name(name: &ObjectName) -> String {
    let full = display_name(name);
    full.rsplit('.').next().unwrap_or(&full).to_owned()
}

fn display_name(name: &ObjectName) -> String {
    name.to_string().to_lowercase().replace('"', "")
}

/// Collects the outermost subqueries of a node.
#[derive(Default)]
struct Subqueries {
    depth: usize,
    queries: Vec<Query>,
}

impl Visitor for Subqueries {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<()> {
        if self.depth == 0 {
            self.queries.push(query.clone());
        }
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }
}
