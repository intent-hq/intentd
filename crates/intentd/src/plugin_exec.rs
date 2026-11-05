//! `intentd plugin-exec`: start an Agent Plugins stdio MCP server with the
//! working directory its `mcp.json` names. The server inherits stdio and env.

use std::ffi::OsString;
use std::path::Path;
use std::process::ExitCode;

pub(crate) fn run(cwd: &Path, command: &[OsString]) -> ExitCode {
    let Some((program, args)) = command.split_first() else {
        eprintln!("plugin-exec: missing command");
        return ExitCode::from(2);
    };
    let mut child = std::process::Command::new(program);
    child.args(args).current_dir(cwd);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let error = child.exec();
        eprintln!(
            "plugin-exec: cannot start {}: {error}",
            program.to_string_lossy()
        );
        ExitCode::from(127)
    }
    #[cfg(not(unix))]
    match child.status() {
        Ok(status) => ExitCode::from(status.code().unwrap_or(1).clamp(0, 255) as u8),
        Err(error) => {
            eprintln!(
                "plugin-exec: cannot start {}: {error}",
                program.to_string_lossy()
            );
            ExitCode::from(127)
        }
    }
}
