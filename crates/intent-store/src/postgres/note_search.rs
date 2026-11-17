//! Ranked note search (Postgres). Mirrors `note_search_repo.rs` over the
//! generated `note.search_tsv` column. The FTS5 MATCH expression callers pass
//! (`intent_search::fts_match_expr`) is translated to a `tsquery` in which
//! each term matches through the built-in 'english' (stemmed) or 'simple'
//! (stopwords kept) configuration, the same two the column is built from.
//! Ranking is `ts_rank`, not bm25, so the order of hits can differ from
//! `SQLite`; the filters, boosts, per-workspace cap, cursor and limit are the
//! same.

use intent_core::{Error, Result, WorkspaceId};

use super::notes::col;
use super::{db, PgStore};
use crate::note_search_repo::{NoteFtsCursor, NoteFtsMatch, NoteFtsOptions};
use crate::tags_from_db;

/// Order of ranked hits; the same tie-breaks as `SQLite`, in byte order.
const ORDER: &str = "ORDER BY adjusted_rank, updated_at COLLATE \"C\" DESC, \
     workspace_id COLLATE \"C\", note_id COLLATE \"C\"";

/// Strictly after the cursor in [`ORDER`]. Binds `$7`..`$10`.
const AFTER_CURSOR: &str = " AND (adjusted_rank > $7 OR (adjusted_rank = $7 AND \
     (updated_at COLLATE \"C\" < $8 OR (updated_at = $8 AND (workspace_id COLLATE \"C\" > $9 \
     OR (workspace_id = $9 AND note_id COLLATE \"C\" > $10))))))";

/// The ranked candidate rows for `query` (a [`SearchQuery::sql`] tsquery
/// expression). Binds: `$1` tenant, `$2` preferred workspace, `$3` the query
/// terms, `$4` hard scope, `$5` allowed workspaces.
fn ranked_sql(options: &NoteFtsOptions<'_>, query: &str) -> String {
    let mut filters = String::new();
    if options.workspace_id.is_some() {
        filters.push_str(" AND n.workspace_id = $4");
    }
    if options.allowed_workspace_ids.is_some() {
        filters.push_str(" AND n.workspace_id = ANY($5)");
    }
    if !options.include_archived {
        filters.push_str(" AND n.is_archived = 0");
    }
    format!(
        "SELECT n.id AS note_id, n.workspace_id, n.updated_at, n.title, n.content, n.tags, \
             n.is_archived, w.archived AS workspace_archived, \
             (-ts_rank(n.search_tsv, q.query) \
               - CASE WHEN n.workspace_id = $2 THEN 1.0 ELSE 0.0 END \
               + CASE WHEN w.archived <> 0 THEN 1.0 ELSE 0.0 END)::float8 AS adjusted_rank \
         FROM note n \
         JOIN workspace w ON w.tenant_id = $1 AND w.id = n.workspace_id \
         CROSS JOIN (SELECT {query} AS query) q \
         WHERE n.tenant_id = $1 AND n.search_tsv @@ q.query{filters}"
    )
}

type PgQuery<'q> = sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>;

fn bind_ranked<'q>(
    store: &'q PgStore,
    sql: &'q str,
    terms: Vec<String>,
    options: &NoteFtsOptions<'q>,
) -> PgQuery<'q> {
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(store.tenant())
        .bind(options.prefer_workspace_id.map(WorkspaceId::as_str))
        .bind(terms)
        .bind(options.workspace_id.map(WorkspaceId::as_str))
        .bind(
            options
                .allowed_workspace_ids
                .map(|ids| ids.iter().map(|id| id.0.clone()).collect::<Vec<_>>()),
        )
}

impl PgStore {
    pub async fn note_fts_positions(
        &self,
        match_expr: &str,
        options: &NoteFtsOptions<'_>,
        notes: &[(String, String)],
    ) -> Result<Vec<NoteFtsCursor>> {
        if notes.is_empty()
            || options
                .allowed_workspace_ids
                .is_some_and(<[WorkspaceId]>::is_empty)
        {
            return Ok(Vec::new());
        }
        let Some(query) = SearchQuery::parse(match_expr) else {
            return Ok(Vec::new());
        };
        let sql = format!(
            "SELECT note_id, workspace_id, updated_at, adjusted_rank FROM ({}) r \
             WHERE (workspace_id, note_id) IN (SELECT * FROM unnest($6::text[], $7::text[]))",
            ranked_sql(options, &query.sql())
        );
        let (workspaces, note_ids): (Vec<String>, Vec<String>) = notes.iter().cloned().unzip();
        let mut tx = self.begin().await?;
        let rows = bind_ranked(self, &sql, query.terms, options)
            .bind(workspaces)
            .bind(note_ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("note search positions failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                Ok(NoteFtsCursor {
                    rank: col(row, "adjusted_rank")?,
                    updated_at: col(row, "updated_at")?,
                    workspace_id: col(row, "workspace_id")?,
                    note_id: col(row, "note_id")?,
                })
            })
            .collect()
    }

    pub async fn search_notes_fts(
        &self,
        match_expr: &str,
        options: &NoteFtsOptions<'_>,
    ) -> Result<Vec<NoteFtsMatch>> {
        if options.limit.is_some_and(|limit| limit < 0)
            || options.per_workspace_limit.is_some_and(|limit| limit < 0)
        {
            return Err(Error::InvalidParams(
                "note search limit must be nonnegative".into(),
            ));
        }
        if options.limit == Some(0)
            || options.per_workspace_limit == Some(0)
            || options
                .allowed_workspace_ids
                .is_some_and(<[WorkspaceId]>::is_empty)
        {
            return Ok(Vec::new());
        }
        let Some(query) = SearchQuery::parse(match_expr) else {
            return Ok(Vec::new());
        };
        let ranked = ranked_sql(options, &query.sql());
        let after = if options.after.is_some() {
            AFTER_CURSOR
        } else {
            ""
        };
        let sql = if options.per_workspace_limit.is_some() {
            format!(
                "SELECT * FROM ( \
                     SELECT r.*, ROW_NUMBER() OVER ( \
                         PARTITION BY r.workspace_id \
                         ORDER BY r.adjusted_rank, r.updated_at COLLATE \"C\" DESC, \
                             r.note_id COLLATE \"C\" \
                     ) AS workspace_position \
                     FROM ({ranked}) r \
                 ) p WHERE workspace_position <= $6{after} {ORDER} LIMIT $11"
            )
        } else {
            format!("SELECT * FROM ({ranked}) r WHERE true{after} {ORDER} LIMIT $11")
        };
        let after = options.after;
        let mut tx = self.begin().await?;
        let rows = bind_ranked(self, &sql, query.terms, options)
            .bind(options.per_workspace_limit)
            .bind(after.map(|a| a.rank))
            .bind(after.map(|a| a.updated_at.as_str()))
            .bind(after.map(|a| a.workspace_id.as_str()))
            .bind(after.map(|a| a.note_id.as_str()))
            .bind(options.limit)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| Error::Internal(format!("search notes failed: {e}")))?;
        tx.commit().await.map_err(db)?;
        rows.iter()
            .map(|row| {
                Ok(NoteFtsMatch {
                    note_id: col(row, "note_id")?,
                    workspace_id: col(row, "workspace_id")?,
                    title: col(row, "title")?,
                    content: col(row, "content")?,
                    tags: tags_from_db(&col::<String>(row, "tags")?)?,
                    updated_at: col(row, "updated_at")?,
                    is_archived: col::<i64>(row, "is_archived")? != 0,
                    workspace_archived: col::<i64>(row, "workspace_archived")? != 0,
                    rank: col(row, "adjusted_rank")?,
                })
            })
            .collect()
    }
}

/// A parsed FTS5 MATCH expression: the boolean shape plus its terms, which
/// are bound as the `$3` text array, never spliced into the SQL.
#[derive(Debug)]
pub(crate) struct SearchQuery {
    expr: Expr,
    /// `to_tsquery` inputs: words joined by `<->`, a prefix term ends `:*`.
    terms: Vec<String>,
}

#[derive(Debug)]
enum Expr {
    Term(usize),
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    AndNot(Box<Expr>, Box<Expr>),
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Open,
    Close,
    And,
    Or,
    Not,
    Term(String),
}

impl SearchQuery {
    /// Parse FTS5 syntax: quoted or bare terms, `*` prefixes, `AND`, `OR`,
    /// `NOT`, parentheses and implicit AND, with FTS5 precedence (NOT, then
    /// AND, then OR). Terms keep only letters and digits. Operators with a
    /// missing side are dropped. `None` when no term remains or the
    /// parentheses do not balance.
    pub(crate) fn parse(expr: &str) -> Option<Self> {
        let raw = lex(expr);
        let mut tokens = Vec::with_capacity(raw.len());
        for (i, token) in raw.iter().enumerate() {
            if matches!(token, Token::And | Token::Or | Token::Not) {
                let before = matches!(tokens.last(), Some(Token::Close | Token::Term(_)));
                let after = matches!(raw.get(i + 1), Some(Token::Open | Token::Term(_)));
                if !(before && after) {
                    continue;
                }
            }
            tokens.push(token.clone());
        }
        let mut parser = Parser {
            tokens,
            at: 0,
            terms: Vec::new(),
        };
        let expr = parser.or()?;
        (parser.at == parser.tokens.len()).then_some(Self {
            expr,
            terms: parser.terms,
        })
    }

    /// The tsquery SQL expression. Each term matches through 'english'
    /// (stemmed; stopwords dropped) or 'simple' (every word kept), the two
    /// configurations `search_tsv` is built from.
    pub(crate) fn sql(&self) -> String {
        render(&self.expr, &|i| {
            format!("(to_tsquery('english', $3[{i}]) || to_tsquery('simple', $3[{i}]))")
        })
    }
}

fn render(expr: &Expr, term: &dyn Fn(usize) -> String) -> String {
    match expr {
        Expr::Term(i) => term(*i),
        Expr::And(a, b) => format!("({} && {})", render(a, term), render(b, term)),
        Expr::Or(a, b) => format!("({} || {})", render(a, term), render(b, term)),
        Expr::AndNot(a, b) => format!("({} && !!{})", render(a, term), render(b, term)),
    }
}

struct Parser {
    tokens: Vec<Token>,
    at: usize,
    terms: Vec<String>,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.at)
    }

    fn or(&mut self) -> Option<Expr> {
        let mut left = self.and()?;
        while self.peek() == Some(&Token::Or) {
            self.at += 1;
            left = Expr::Or(Box::new(left), Box::new(self.and()?));
        }
        Some(left)
    }

    fn and(&mut self) -> Option<Expr> {
        let mut left = self.not()?;
        loop {
            match self.peek() {
                Some(Token::And) => self.at += 1,
                Some(Token::Open | Token::Term(_)) => {}
                _ => return Some(left),
            }
            left = Expr::And(Box::new(left), Box::new(self.not()?));
        }
    }

    fn not(&mut self) -> Option<Expr> {
        let mut left = self.primary()?;
        while self.peek() == Some(&Token::Not) {
            self.at += 1;
            left = Expr::AndNot(Box::new(left), Box::new(self.primary()?));
        }
        Some(left)
    }

    fn primary(&mut self) -> Option<Expr> {
        match self.tokens.get(self.at).cloned()? {
            Token::Term(term) => {
                self.at += 1;
                self.terms.push(term);
                Some(Expr::Term(self.terms.len()))
            }
            Token::Open => {
                self.at += 1;
                let inner = self.or()?;
                (self.peek() == Some(&Token::Close)).then(|| {
                    self.at += 1;
                    inner
                })
            }
            _ => None,
        }
    }
}

fn lex(expr: &str) -> Vec<Token> {
    let chars: Vec<char> = expr.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '(' || c == ')' {
            tokens.push(if c == '(' { Token::Open } else { Token::Close });
            i += 1;
            continue;
        }
        let text = if c == '"' {
            let mut text = String::new();
            i += 1;
            while i < chars.len() {
                if chars[i] == '"' {
                    if chars.get(i + 1) == Some(&'"') {
                        text.push('"');
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                text.push(chars[i]);
                i += 1;
            }
            text
        } else if c.is_alphanumeric() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            let operator = match word.as_str() {
                "AND" => Some(Token::And),
                "OR" => Some(Token::Or),
                "NOT" => Some(Token::Not),
                _ => None,
            };
            if let Some(operator) = operator {
                tokens.push(operator);
                continue;
            }
            word
        } else {
            i += 1;
            continue;
        };
        let prefix = chars.get(i) == Some(&'*');
        if prefix {
            i += 1;
        }
        let words: Vec<String> = text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
            .map(str::to_lowercase)
            .collect();
        if words.is_empty() {
            continue;
        }
        let mut term = words.join(" <-> ");
        if prefix {
            term.push_str(":*");
        }
        tokens.push(Token::Term(term));
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::{render, SearchQuery};

    /// The boolean shape with each term written inline.
    fn shape(fts5: &str) -> Option<String> {
        let q = SearchQuery::parse(fts5)?;
        Some(render(&q.expr, &|i| q.terms[i - 1].clone()))
    }

    #[test]
    fn translates_fts5_match_expressions() {
        for (fts5, expected) in [
            (
                r#""alpha" AND ("beta" OR "beta"*)"#,
                Some("(alpha && (beta || beta:*))"),
            ),
            (r#""two words""#, Some("two <-> words")),
            ("a b", Some("(a && b)")),
            ("a NOT b", Some("(a && !!b)")),
            // FTS5 precedence: NOT, then AND, then OR.
            ("a OR b AND c", Some("(a || (b && c))")),
            ("a AND b NOT c OR d", Some("((a && (b && !!c)) || d)")),
            (r#""it's" OR x"#, Some("(it <-> s || x)")),
            (r#"" " AND "x""#, Some("x")),
            ("AND", None),
            ("", None),
            ("(a", None),
            ("a)", None),
            ("x:&|!'", Some("x")),
        ] {
            assert_eq!(shape(fts5).as_deref(), expected, "{fts5}");
        }
    }

    #[test]
    fn terms_are_bound_not_spliced() {
        let q = SearchQuery::parse(r#""it's" OR x*"#).unwrap();
        assert_eq!(q.terms, ["it <-> s", "x:*"]);
        assert_eq!(
            q.sql(),
            "((to_tsquery('english', $3[1]) || to_tsquery('simple', $3[1])) || \
             (to_tsquery('english', $3[2]) || to_tsquery('simple', $3[2])))"
        );
    }
}
