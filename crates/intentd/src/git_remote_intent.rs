//! `git-remote-intent`: when invoked under that name, intentd is git's
//! remote helper for `intent://` agent remotes on an execution node. The
//! node links the name to its intentd binary in the directory it puts first
//! on its git commands' `PATH`, and names its local git socket in
//! `INTENT_NODE_GIT_SOCKET`.

use std::path::Path;
use std::process::ExitCode;

use intent_git::node_git::{HELPER_NAME, SOCKET_ENV};

/// Run as the remote helper when `argv[0]` is `git-remote-intent`.
pub fn dispatch_argv0() -> Option<ExitCode> {
    let mut args = std::env::args_os();
    if Path::new(&args.next()?).file_name()? != HELPER_NAME {
        return None;
    }
    let url = args.nth(1).and_then(|url| url.into_string().ok());
    let socket = std::env::var_os(SOCKET_ENV);
    let (Some(url), Some(socket)) = (url, socket) else {
        eprintln!("fatal: {HELPER_NAME} <remote> <url> needs {SOCKET_ENV}");
        return Some(ExitCode::FAILURE);
    };
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| intent_core::Error::Internal(error.to_string()))
        .and_then(|runtime| {
            let result = runtime.block_on(intent_transport::node_link::run_git_remote_helper(
                &url,
                Path::new(&socket),
            ));
            // A session that ends before git has its answer leaves a stdin
            // read blocked while git waits for our output: dropping the
            // runtime would wait for that read forever, and hold the push.
            runtime.shutdown_background();
            result
        });
    Some(match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("fatal: {error}");
            ExitCode::FAILURE
        }
    })
}
