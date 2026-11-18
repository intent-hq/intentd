//! The Postgres tenant rule on parsed SQL (sqlparser, Postgres dialect).
//!
//! For every SELECT level, each table in FROM or JOIN that is not a CTE in
//! scope needs a `tenant_id = $n` (or `<alias>.tenant_id = $n`, either
//! operand order) conjunct in that level's WHERE or in its own JOIN's ON.
//! An INSERT's column list includes `tenant_id`. An UPDATE or DELETE target
//! needs the filter in its WHERE; SET lists and ON CONFLICT targets do not
//! count. Subqueries anywhere in a statement are checked as their own level.

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, FromTable, Insert, JoinConstraint, JoinOperator, ObjectName, Query,
    Select, SetExpr, Statement, TableAlias, TableFactor, TableObject, TableWithJoins,
    UpdateTableFromKind, Value, Visit, Visitor,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

pub fn parse(sql: &str) -> Result<Vec<Statement>, String> {
    Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|e| e.to_string())
}

/// Fillers tried, in order, for a `format!` hole that no const resolves:
/// an expression, nothing (an optional clause), an identifier, a subquery
/// body, a VALUES row and an operator. A filler names no table, so it
/// cannot hide one.
const FILLERS: &[&str] = &["true", "", "x", "select 1", "($1)", "="];

/// Schemas of the Postgres system catalogs, which hold no tenant rows.
const SYSTEM_SCHEMAS: &[&str] = &["pg_catalog", "information_schema"];

/// Parses a `format!` template. Each `{NAME}` hole that `resolve` knows is
/// replaced by its text; the others try every [`FILLERS`] combination (up
/// to four holes; more get one filler each) until one parses. The error is
/// the first attempt's.
pub fn parse_template(
    sql: &str,
    resolve: &dyn Fn(&str) -> Option<String>,
) -> Result<Vec<Statement>, String> {
    let mut parts = vec![String::new()];
    let mut rest = sql;
    while let Some(at) = rest.find('{') {
        let Some(len) = rest[at..].find('}') else {
            break;
        };
        parts.last_mut().unwrap().push_str(&rest[..at]);
        let name = rest[at + 1..at + len].split(':').next().unwrap_or("");
        if let Some(text) = resolve(name) {
            let last = parts.last_mut().unwrap();
            last.push(' ');
            last.push_str(&text);
            last.push(' ');
        } else {
            parts.push(String::new());
        }
        rest = &rest[at + len + 1..];
    }
    parts.last_mut().unwrap().push_str(rest);
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
    };
    for s in stmts {
        rule.statement(s, &Scope::new());
    }
    rule.out
}

/// CTE names visible at a point of the statement.
type Scope = BTreeSet<String>;

struct Rule<'a> {
    exempt: &'a [&'a str],
    out: Vec<String>,
}

/// A table this level reads, with the JOIN ON clauses that may hold its filter.
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
                let mut refs = Vec::new();
                table_refs(&u.table, "UPDATE", &[], &mut refs);
                if let Some(r) = refs.first_mut() {
                    r.target = true;
                }
                if let Some(
                    UpdateTableFromKind::AfterSet(from) | UpdateTableFromKind::BeforeSet(from),
                ) = &u.from
                {
                    for t in from {
                        table_refs(t, "FROM", &[], &mut refs);
                    }
                }
                self.check(refs, u.selection.as_ref(), scope);
                self.nested(u, scope);
            }
            Statement::Delete(d) => {
                let mut refs = Vec::new();
                let (FromTable::WithFromKeyword(from) | FromTable::WithoutKeyword(from)) = &d.from;
                for t in from {
                    table_refs(t, "FROM", &[], &mut refs);
                }
                for r in &mut refs {
                    r.target = true;
                }
                for t in d.using.iter().flatten() {
                    table_refs(t, "USING", &[], &mut refs);
                }
                self.check(refs, d.selection.as_ref(), scope);
                self.nested(d, scope);
            }
            other => self.nested(other, scope),
        }
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
        }
        self.nested(i, scope);
    }

    fn query(&mut self, q: &Query, scope: &Scope) {
        let mut scope = scope.clone();
        if let Some(with) = &q.with {
            for cte in &with.cte_tables {
                let name = cte.alias.name.value.to_lowercase();
                if with.recursive {
                    scope.insert(name.clone());
                }
                self.query(&cte.query, &scope);
                scope.insert(name);
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
            other => self.nested(other, scope),
        }
    }

    fn select(&mut self, s: &Select, scope: &Scope) {
        let mut refs = Vec::new();
        for t in &s.from {
            table_refs(t, "FROM", &[], &mut refs);
        }
        self.check(refs, s.selection.as_ref(), scope);
        self.nested(s, scope);
    }

    /// Checks each table against the level's WHERE and its own JOIN ONs.
    /// A one-part name that a CTE in scope defines is that CTE.
    fn check(&mut self, refs: Vec<TableRef<'_>>, selection: Option<&Expr>, scope: &Scope) {
        for r in refs {
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
            let ok =
                has_filter(selection, &alias) || r.on.iter().any(|on| has_filter(Some(on), &alias));
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
/// parenthesized joins, each with the ON clauses that apply to it.
fn table_refs<'a>(
    t: &'a TableWithJoins,
    keyword: &'static str,
    outer_on: &[&'a Expr],
    out: &mut Vec<TableRef<'a>>,
) {
    relation(&t.relation, keyword, outer_on.to_vec(), out);
    for j in &t.joins {
        let mut on = outer_on.to_vec();
        on.extend(join_on(&j.join_operator));
        relation(&j.relation, "JOIN", on, out);
    }
}

fn relation<'a>(
    f: &'a TableFactor,
    keyword: &'static str,
    on: Vec<&'a Expr>,
    out: &mut Vec<TableRef<'a>>,
) {
    match f {
        // `json_each($2)`, `generate_series(..)`: a function, not a table.
        TableFactor::Table { args: Some(_), .. } => {}
        TableFactor::Table { name, alias, .. } => out.push(TableRef {
            keyword,
            target: false,
            name,
            alias: alias.as_ref(),
            on,
        }),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => table_refs(table_with_joins, keyword, &on, out),
        // Derived tables are subqueries, found by `Rule::nested`.
        _ => {}
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

/// Whether a top-level AND conjunct of `e` is `tenant_id = $n` for `alias`.
fn has_filter(e: Option<&Expr>, alias: &str) -> bool {
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
                let pair = |col: &Expr, val: &Expr| is_tenant_column(col, alias) && is_param(val);
                if pair(left, right) || pair(right, left) {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
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

fn is_param(e: &Expr) -> bool {
    match e {
        Expr::Value(v) => matches!(&v.value, Value::Placeholder(p) if p.starts_with('$')),
        Expr::Nested(inner) => is_param(inner),
        _ => false,
    }
}

fn last_ident(name: &ObjectName) -> Option<String> {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .map(|i| i.value.to_lowercase())
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
