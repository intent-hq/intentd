//! Test-only attribute macros for tests that call the service layer directly.
//!
//! Every `intent-services` capability gate refuses an unbound request
//! (`current_caller() == None`, fail-closed — multiplayer w3). Production
//! entry points bind a [`Caller`](intent_core::Caller) before any service
//! call; a unit test that drives `Services` directly has no entry point, so
//! it states which caller it runs as instead. [`daemon_test`] is the explicit
//! per-test binding — there is deliberately no `Services`-level default.
//!
//! The crate is `proc-macro = true` with no dependencies beyond `proc_macro`
//! itself and is only ever a `dev-dependency`.

use proc_macro::{Delimiter, Group, Ident, Punct, Spacing, Span, TokenStream, TokenTree};

/// Replace `#[tokio::test]` on an `async fn` test with
/// `#[intent_test_macros::daemon_test]` to run its body under
/// `intent_core::with_caller(Caller::Daemon, …)`. Attribute arguments are
/// forwarded to `tokio::test` unchanged
/// (`#[daemon_test(flavor = "multi_thread")]`); other attributes on the
/// function (`#[should_panic]`, `#[ignore]`, `#[expect(..)]`) are kept, and a
/// declared return type is preserved so `?` keeps inferring inside the body.
///
/// The expansion is
///
/// ```ignore
/// #[tokio::test(<args>)]
/// async fn name() -> Ret {
///     let out: Ret = ::intent_core::with_caller(::intent_core::Caller::Daemon, async move { <body> }).await;
///     out
/// }
/// ```
#[proc_macro_attribute]
pub fn daemon_test(args: TokenStream, item: TokenStream) -> TokenStream {
    let tokens: Vec<TokenTree> = item.into_iter().collect();
    let Some((TokenTree::Group(body), signature)) = tokens.split_last() else {
        return compile_error("daemon_test: expected an `async fn` item");
    };
    if body.delimiter() != Delimiter::Brace {
        return compile_error("daemon_test: expected an `async fn` item with a block body");
    }
    if !signature
        .windows(2)
        .any(|w| is_ident(&w[0], "async") && is_ident(&w[1], "fn"))
    {
        return compile_error("daemon_test: the item must be an `async fn`");
    }
    let return_type = declared_return_type(signature);

    let mut out = TokenStream::new();
    out.extend(tokio_test_attribute(args));
    out.extend(signature.iter().cloned());
    out.extend([TokenTree::Group(Group::new(
        Delimiter::Brace,
        bound_body(body.stream(), return_type),
    ))]);
    out
}

fn is_ident(tt: &TokenTree, name: &str) -> bool {
    matches!(tt, TokenTree::Ident(i) if i.to_string() == name)
}

/// The tokens after `->` in the signature (the body is already split off),
/// or `None` for a unit-returning test.
fn declared_return_type(signature: &[TokenTree]) -> Option<Vec<TokenTree>> {
    signature
        .windows(2)
        .position(|w| {
            matches!((&w[0], &w[1]), (TokenTree::Punct(p), TokenTree::Punct(next))
            if p.as_char() == '-' && p.spacing() == Spacing::Joint && next.as_char() == '>')
        })
        .map(|i| signature[i + 2..].to_vec())
}

/// `#[::tokio::test]` or `#[::tokio::test(<args>)]`.
fn tokio_test_attribute(args: TokenStream) -> TokenStream {
    let mut inner: TokenStream = "::tokio::test".parse().expect("static path");
    if !args.is_empty() {
        inner.extend([TokenTree::Group(Group::new(Delimiter::Parenthesis, args))]);
    }
    let mut attr = TokenStream::new();
    attr.extend([
        TokenTree::Punct(Punct::new('#', Spacing::Alone)),
        TokenTree::Group(Group::new(Delimiter::Bracket, inner)),
    ]);
    attr
}

/// `let out: Ret = ::intent_core::with_caller(::intent_core::Caller::Daemon, async move { body }).await; out`
/// — or the bare awaited scope for a unit-returning test.
fn bound_body(body: TokenStream, return_type: Option<Vec<TokenTree>>) -> TokenStream {
    let mut call: TokenStream = "::intent_core::with_caller".parse().expect("static path");
    let mut call_args: TokenStream = "::intent_core::Caller::Daemon,"
        .parse()
        .expect("static path");
    call_args.extend([
        TokenTree::Ident(Ident::new("async", Span::call_site())),
        TokenTree::Ident(Ident::new("move", Span::call_site())),
        TokenTree::Group(Group::new(Delimiter::Brace, body)),
    ]);
    call.extend([TokenTree::Group(Group::new(
        Delimiter::Parenthesis,
        call_args,
    ))]);
    call.extend(".await".parse::<TokenStream>().expect("static tokens"));

    let Some(return_type) = return_type else {
        return call;
    };
    let mut out: TokenStream = "let __daemon_test_out:".parse().expect("static tokens");
    out.extend(return_type);
    out.extend([TokenTree::Punct(Punct::new('=', Spacing::Alone))]);
    out.extend(call);
    out.extend([TokenTree::Punct(Punct::new(';', Spacing::Alone))]);
    out.extend([TokenTree::Ident(Ident::new(
        "__daemon_test_out",
        Span::call_site(),
    ))]);
    out
}

fn compile_error(message: &str) -> TokenStream {
    format!("compile_error!({message:?});")
        .parse()
        .expect("static compile_error")
}
