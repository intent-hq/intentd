//! The shared fake `npx` for tests that launch a pinned npm package.
//!
//! The fake accepts only the argv [`npx_run_args`] builds for its package, so
//! a change to the production npx launch flags reaches every fake at once.
//! Do not hand-write an npx argv check in a test: build the fake here and put
//! the test-specific behavior in [`FakeNpx::then`].
//!
//! ```text
//! npx --version          -> the version probe (default: prints DEFAULT_VERSION)
//! npx <npx_run_args> ... -> the `then` body, with the original "$@"
//! anything else          -> exit REJECT_EXIT, argv on stderr
//! ```

use std::path::Path;

use crate::spawn::npx_run_args;

/// Exit status of the fake when its argv is not the production launch argv.
pub const REJECT_EXIT: i32 = 92;

/// What `npx --version` prints unless [`FakeNpx::version`] changes it: an npm
/// new enough for every daemon npx gate.
pub const DEFAULT_VERSION: &str = "11.0.0";

/// Builder for a `/bin/sh` fake `npx`. See the module docs.
#[derive(Debug, Clone)]
pub struct FakeNpx {
    package: String,
    version_probe: String,
    trailing_args: bool,
    body: String,
}

impl FakeNpx {
    /// A fake that accepts only the production npx launch of `package`.
    #[must_use]
    pub fn new(package: &str) -> Self {
        Self {
            package: package.to_owned(),
            version_probe: print_line(DEFAULT_VERSION),
            trailing_args: false,
            body: String::new(),
        }
    }

    /// `npx --version` prints `version`.
    #[must_use]
    pub fn version(mut self, version: &str) -> Self {
        self.version_probe = print_line(version);
        self
    }

    /// `npx --version` runs `script` instead. It exits 0 unless the script
    /// exits first.
    #[must_use]
    pub fn on_version(mut self, script: &str) -> Self {
        script.clone_into(&mut self.version_probe);
        self
    }

    /// Accept arguments after the package (the adapter's own arguments).
    /// Without this, the argv must be exactly the production argv.
    #[must_use]
    pub fn allow_trailing_args(mut self) -> Self {
        self.trailing_args = true;
        self
    }

    /// Shell lines that run after the argv check, with the original `"$@"`.
    /// `shift "$npx_prefix_len"` leaves only the arguments after the package.
    #[must_use]
    pub fn then(mut self, script: &str) -> Self {
        script.clone_into(&mut self.body);
        self
    }

    /// The argv the fake accepts (before any trailing arguments).
    #[must_use]
    pub fn expected_args(&self) -> Vec<String> {
        npx_run_args(&self.package)
    }

    /// [`Self::expected_args`] joined by spaces: what `printf '%s\n' "$*"`
    /// in a body records for a launch with no trailing arguments.
    #[must_use]
    pub fn command_line(&self) -> String {
        self.expected_args().join(" ")
    }

    /// The fake's `/bin/sh` source.
    #[must_use]
    pub fn script(&self) -> String {
        let expected = self.expected_args();
        let count = expected.len();
        let arity = if self.trailing_args { "-ge" } else { "=" };
        let mut checks = format!("[ \"$#\" {arity} {count} ] || npx_reject \"$@\"\n");
        for (index, arg) in expected.iter().enumerate() {
            checks.push_str(&format!(
                "[ \"${{{}}}\" = {} ] || npx_reject \"$@\"\n",
                index + 1,
                sh_quote(arg)
            ));
        }
        format!(
            "#!/bin/sh\n\
             if [ \"$#\" = 1 ] && [ \"$1\" = --version ]; then\n\
             {version}\n\
             exit 0\n\
             fi\n\
             npx_reject() {{\n\
             printf 'fake npx: not the production npx launch of %s: %s\\n' {package} \"$*\" >&2\n\
             exit {REJECT_EXIT}\n\
             }}\n\
             {checks}\
             npx_prefix_len={count}\n\
             {body}\n",
            version = self.version_probe,
            package = sh_quote(&self.package),
            body = self.body,
        )
    }

    /// Write the fake to `path` as an executable file.
    ///
    /// # Errors
    /// Returns the I/O error from writing the file or setting its mode.
    pub fn write(&self, path: &Path) -> std::io::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(path, self.script())?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
    }
}

/// `value` as one single-quoted `/bin/sh` word.
#[must_use]
pub fn sh_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', r"'\''"))
}

fn print_line(value: &str) -> String {
    format!("printf '%s\\n' {}", sh_quote(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spawn::{build_args, SpawnOptions};
    use std::process::{Command, Output};

    const PACKAGE: &str = "@scope/fixture-adapter@1.2.3";

    fn run(fake: &FakeNpx, args: &[String]) -> Output {
        let dir = tempfile::tempdir().unwrap();
        let npx = dir.path().join("npx");
        fake.write(&npx).unwrap();
        Command::new(&npx).args(args).output().unwrap()
    }

    fn stdout(output: &Output) -> String {
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    fn rejected(output: &Output) -> bool {
        output.status.code() == Some(REJECT_EXIT)
    }

    /// The one literal copy of the production npx launch flags. A change to
    /// `npx_run_args` or `npx_install_args` updates this test and no fake.
    #[test]
    fn production_npx_argv_is_pinned() {
        assert_eq!(
            npx_run_args(PACKAGE),
            [
                "--workspaces=false",
                "--ignore-scripts=false",
                "--include=optional",
                "--allow-scripts=@scope/fixture-adapter@1.2.3",
                "-y",
                "@scope/fixture-adapter@1.2.3",
            ]
        );
    }

    #[test]
    fn accepts_the_production_argv_and_runs_the_body_with_it() {
        let fake = FakeNpx::new(PACKAGE).then(r#"printf '%s|%s\n' "$npx_prefix_len" "$*""#);
        let output = run(&fake, &fake.expected_args());
        assert!(output.status.success(), "{output:?}");
        assert_eq!(
            stdout(&output),
            format!("{}|{}\n", fake.expected_args().len(), fake.command_line())
        );
    }

    #[test]
    fn rejects_every_changed_dropped_or_reordered_argument() {
        let fake = FakeNpx::new(PACKAGE).then("printf 'body ran\\n'");
        let expected = fake.expected_args();
        let mut variants = Vec::new();
        for index in 0..expected.len() {
            let mut dropped = expected.clone();
            dropped.remove(index);
            variants.push(dropped);
            let mut changed = expected.clone();
            changed[index].push('x');
            variants.push(changed);
        }
        let mut swapped = expected.clone();
        swapped.swap(0, 1);
        variants.push(swapped);
        variants.push(Vec::new());
        for args in variants {
            let output = run(&fake, &args);
            assert!(rejected(&output), "{args:?} was accepted: {output:?}");
            assert!(stdout(&output).is_empty(), "body ran for {args:?}");
            assert!(String::from_utf8_lossy(&output.stderr).contains(PACKAGE));
        }
    }

    #[test]
    fn trailing_arguments_need_opt_in_and_follow_the_prefix() {
        let mut args = FakeNpx::new(PACKAGE).expected_args();
        args.extend(["--acp".to_owned(), "two words".to_owned()]);
        assert!(rejected(&run(&FakeNpx::new(PACKAGE), &args)));

        let fake = FakeNpx::new(PACKAGE)
            .allow_trailing_args()
            .then(r#"shift "$npx_prefix_len"; printf '<%s>' "$@""#);
        let output = run(&fake, &args);
        assert!(output.status.success(), "{output:?}");
        assert_eq!(stdout(&output), "<--acp><two words>");
    }

    #[test]
    fn version_probe_never_reaches_the_body() {
        let probe = ["--version".to_owned()];
        let body = "printf 'body ran\\n'";
        assert_eq!(
            stdout(&run(&FakeNpx::new(PACKAGE).then(body), &probe)),
            format!("{DEFAULT_VERSION}\n")
        );
        assert_eq!(
            stdout(&run(&FakeNpx::new(PACKAGE).version("10.9.2").then(body), &probe)),
            "10.9.2\n"
        );
        let custom = FakeNpx::new(PACKAGE)
            .on_version("printf 'custom\\n'; exit 9")
            .then(body);
        let output = run(&custom, &probe);
        assert_eq!(output.status.code(), Some(9));
        assert_eq!(stdout(&output), "custom\n");
    }

    #[test]
    fn quotes_packages_the_shell_would_otherwise_split_or_expand() {
        let package = "it's $HOME @x/y@1";
        let fake = FakeNpx::new(package).then("printf 'ok\\n'");
        assert_eq!(stdout(&run(&fake, &fake.expected_args())), "ok\n");
    }

    /// Every provider's npx agent launch passes the fake: `build_args` puts
    /// the production argv first and only the provider's own args after it.
    #[test]
    fn accepts_every_provider_npx_launch_from_build_args() {
        let npx = Path::new("/usr/bin/npx");
        let mut launches = 0;
        for provider in intent_providers::ACP_PROVIDERS {
            let Some(package) = provider.npx_only_package.or(provider.fallback_npx_package)
            else {
                continue;
            };
            let mut opts = SpawnOptions::new(provider);
            opts.npx_fallback_binary = Some(npx);
            opts.npx_fallback_package = Some(package);
            let fake = FakeNpx::new(package).allow_trailing_args();
            let output = run(&fake, &build_args(&opts));
            assert!(output.status.success(), "{}: {output:?}", provider.id);
            launches += 1;
        }
        assert!(launches > 0, "no provider launches through npx");
    }
}
