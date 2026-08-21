#!/bin/sh
# One-line installer for the intentd sitter (macOS + Linux).
#
#   curl -fsSL https://github.com/intent-hq/intentd-releases/releases/download/sitter-latest/install.sh | sh
#
# Downloads the intentd-<triple>.tar.xz archive from the fixed sitter-latest
# release on the public intent-hq/intentd-releases mirror, verifies its
# .sha256 sidecar, and installs the `intentd` binary (the self-updating
# sitter) to, in order of preference:
#
#   1. $INTENTD_INSTALL_DIR, when set (created if missing)
#   2. /usr/local/bin, when it exists and is writable
#   3. ~/.local/bin (created if missing)
#
# After a successful install it offers to register intentd as a per-user
# service that starts at login and to start it now:
#
#   Linux  systemd user unit   ~/.config/systemd/user/intentd.service
#          (same unit the .deb ships, pointed at the installed binary)
#   macOS  launchd LaunchAgent ~/Library/LaunchAgents/com.intenthq.intentd.plist
#          (same shape as the Homebrew formula's brew-services plist)
#
# The prompt reads from /dev/tty, so it works under `curl ... | sh`; without
# a usable terminal it never hangs — it skips with a hint. Force either way:
#
#   INTENTD_INSTALL_SERVICE=1  (or --service, on direct runs)     set up
#   INTENTD_INSTALL_SERVICE=0  (or --no-service, on direct runs)  skip
#
# When a service is set up, the script also asks whether it should auto-resume
# interrupted agents at startup (the daemon setting
# agents.resumeInterruptedOnStart). The default `auto` resumes only on headless
# hosts, so answering auto — or having no terminal — writes nothing; on/off are
# applied via `intentd settings` once the daemon is up. Force an answer:
#
#   INTENTD_AUTO_RESUME=auto|on|off  (or --auto-resume=<value>, on direct runs)
#
# The prompt and both of those accept the obvious yes/no spellings for on/off
# (y/yes/true, n/no/false), in any case.
#
# The release channel the daemon is installed from — and updated on — defaults
# to stable. Choose another with:
#
#   INTENTD_CHANNEL=stable|beta|alpha  (or --channel=<value>, on direct runs)
#
# A non-default choice is pinned via `intentd sitter channel`, which writes the
# sitter's own config.toml, so the service keeps following it across restarts
# and updates. A default run writes no pin, leaving any channel you pinned
# earlier by hand untouched.
#
# INTENTD_SERVICE_NAME overrides the unit name / launchd label (testing).
# INTENTD_DATA_DIR, when set, is baked into the unit/plist so the service
# serves the same data dir the install-time CLI used.
#
# Idempotent: re-running replaces the installed binary atomically, and
# service setup restarts an already-registered service instead of
# duplicating it.
set -eu

BASE_URL="https://github.com/intent-hq/intentd-releases/releases/download/sitter-latest"

info() { printf '%s\n' "install.sh: $*"; }
warn() { printf '%s\n' "install.sh: warning: $*" >&2; }
fail() { printf '%s\n' "install.sh: error: $*" >&2; exit 1; }

# Escape a value for a double-quoted word in a systemd unit: backslash-escape
# \ and ", and double % (systemd specifier syntax).
systemd_escape() { printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' -e 's/%/%%/g'; }

# Escape a value for XML text content (launchd plist).
xml_escape() { printf '%s' "$1" | sed -e 's/&/\&amp;/g' -e 's/</\&lt;/g' -e 's/>/\&gt;/g'; }

tmpdir=""
staged=""
cleanup() {
  if [ -n "$staged" ]; then rm -f "$staged"; fi
  if [ -n "$tmpdir" ]; then rm -rf "$tmpdir"; fi
}

# Lines of service log quoted when the daemon never came up: enough to carry
# the daemon's own error (it inherits the sitter's stdio, so its message sits
# directly above the sitter's respawn lines) without dumping an unbounded log.
log_tail_lines=40

# Where this run's service output lands, plus how to bound it to this run so a
# diagnosis never quotes a previous run's crash. Set by note_log_start.
log_desc=""
log_file=""
log_offset=0
log_unit=""
log_since=""
restart_hint=""

# Record where the service log ends, and how to restart the service once a
# human has fixed the cause. Callers run this BEFORE starting/restarting the
# service: a daemon that fails the instant it starts must still land inside
# the boundary, so capturing it any later could exclude the very failure
# verify_daemon needs to quote.
#
# macOS: the LaunchAgent's StandardErrorPath, built from the same $HOME the
# plist writer interpolates (the label does not affect the log path).
# Linux: the unit sets no StandardError, so output goes to the journal.
note_log_start() {
  if [ "$os" = "Darwin" ]; then
    log_file="$HOME/Library/Logs/intentd.err.log"
    log_desc="$log_file"
    # Guarded: an unopenable file makes the *shell* report the failed
    # redirect, which `wc 2>/dev/null` cannot suppress — and a first install
    # has no log yet, so that would be noise on the happy path.
    log_offset=0
    if [ -r "$log_file" ]; then
      # The pipeline's status is tr's, so a failed wc surfaces as empty
      # output, not a failed command: check the value, not the status.
      log_offset=$(wc -c <"$log_file" | tr -d '[:space:]')
      [ -n "$log_offset" ] || log_offset=0
    fi
    restart_hint="launchctl kickstart -k gui/$(id -u)/${INTENTD_SERVICE_NAME:-com.intenthq.intentd}"
  else
    log_unit=${INTENTD_SERVICE_NAME:-intentd}
    log_desc="journalctl --user -u $log_unit"
    # Second granularity may pull in the last second of an earlier run; that
    # is close enough to keep the diagnosis about this start.
    log_since=$(date '+%Y-%m-%d %H:%M:%S' 2>/dev/null) || log_since=""
    restart_hint="systemctl --user restart $log_unit"
  fi
}

# Echo this run's service output, bounded to $log_tail_lines lines. Echoes
# nothing when there is nothing to read: the log may not exist yet, be empty
# or unreadable, or — on a Linux box without systemd — there may be no journal.
service_log_tail() {
  if [ -n "$log_file" ]; then
    [ -r "$log_file" ] || return 0
    size=$(wc -c <"$log_file" 2>/dev/null | tr -d '[:space:]') || size=""
    # A rotated or replaced log invalidates the offset; read it whole.
    if [ -z "$size" ] || [ "$size" -lt "$log_offset" ]; then
      log_offset=0
    fi
    # The offset is a byte count, so the first line can start mid-word when
    # the service was already writing as we took it; the lines that matter
    # come after it.
    tail -c "+$((log_offset + 1))" "$log_file" 2>/dev/null | tail -n "$log_tail_lines"
  elif [ -n "$log_unit" ] && command -v journalctl >/dev/null 2>&1; then
    if [ -n "$log_since" ]; then
      journalctl --user -u "$log_unit" --since "$log_since" -n "$log_tail_lines" --no-pager 2>/dev/null || true
    else
      journalctl --user -u "$log_unit" -n "$log_tail_lines" --no-pager 2>/dev/null || true
    fi
  fi
}

# Quote the captured log on stderr, ahead of the error that explains it (both
# on stderr so the two cannot interleave out of order).
report_daemon_log() {
  printf '%s\n' "install.sh: the service's output for this start (from $log_desc):" >&2
  printf '%s\n' "$1" | sed 's/^/  | /' >&2
}

# Poll `intentd status` until the daemon answers. First service start can be
# slow: the sitter downloads the real daemon before serving. The caller must
# have called note_log_start before starting the service, so the log window
# quoted on failure covers the whole start.
verify_daemon() {
  info "waiting for the daemon to respond (first start downloads the daemon binary)..."
  waited=0
  while [ "$waited" -lt 60 ]; do
    if "$install_dir/intentd" status >/dev/null 2>&1; then
      info "daemon is up — 'intentd status' responds"
      return 0
    fi
    sleep 2
    waited=$((waited + 2))
  done

  # Timing out is three different things. The sitter reports a daemon that
  # starts and dies on the service's stderr, so this run's log says whether it
  # has already given up (service stopped for good), is still respawning a
  # daemon that cannot start (it only gives up minutes after this 60s wait, so
  # this is the common case), or nothing crashed at all and the first download
  # is merely slow. Only the last of the three is a warning.
  # An explicit on/off auto-resume answer is applied only after this check
  # passes (apply_auto_resume runs after verify_daemon), so a failure here
  # drops it — say so instead of dropping it silently.
  auto_resume_note=""
  case "${auto_resume:-}" in
    on | off) auto_resume_note="
Your auto-resume choice ('$auto_resume') was not applied; once the daemon is up, apply it with:
  intentd settings agents.resumeInterruptedOnStart $auto_resume" ;;
  esac
  log_out=$(service_log_tail)
  case "$log_out" in
    *"times in a row without ever staying up"*)
      report_daemon_log "$log_out"
      fail "the daemon could not start and the sitter has given up — the service is stopped.
Fix the cause reported above, then start it again with:
  $restart_hint$auto_resume_note" ;;
    *"exited unexpectedly"* | *"failed to spawn"* | *"failed waiting on intentd"*)
      report_daemon_log "$log_out"
      fail "the daemon is failing to start and the sitter is still respawning it; it gives up in a few minutes and leaves the service stopped.
Fix the cause reported above, then start it again with:
  $restart_hint$auto_resume_note" ;;
  esac
  warn "daemon did not respond within 60s — it may still be downloading; check later with: intentd status"
}

# Normalize an auto-resume value from the flag, the env var, or the prompt:
# lowercase it so matching is case-insensitive, and fold the unambiguous
# yes/no spellings onto the canonical on/off — the question reads as a yes/no
# question, so `no` must mean off rather than being rejected in favour of the
# opposite default. Anything else passes through lowercased, for
# validate_auto_resume or the prompt to reject.
normalize_auto_resume() {
  auto_resume_lower=$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')
  case "$auto_resume_lower" in
    y | yes | true) printf 'on' ;;
    n | no | false) printf 'off' ;;
    *) printf '%s' "$auto_resume_lower" ;;
  esac
}

# Fail fast on a bogus auto-resume value from the flag or env var
# (already lowercased and folded via normalize_auto_resume).
validate_auto_resume() {
  case "$1" in
    auto | on | off) ;;
    *) fail "invalid auto-resume value '$1' (expected auto, on, or off)" ;;
  esac
}

# Apply an explicit on/off auto-resume answer via the settings CLI once the
# daemon is reachable (runs after verify_daemon). `auto` is the daemon default,
# so there is nothing to write. A failure is a warning, not a fatal install
# error: the setting can be changed later with the same command.
apply_auto_resume() {
  case "$auto_resume" in
    on | off) ;;
    *) return 0 ;;
  esac
  if "$install_dir/intentd" settings agents.resumeInterruptedOnStart "$auto_resume" >/dev/null 2>&1; then
    info "auto-resume on service start set to '$auto_resume' (agents.resumeInterruptedOnStart)"
  else
    warn "could not set agents.resumeInterruptedOnStart=$auto_resume — set it later with: intentd settings agents.resumeInterruptedOnStart $auto_resume"
  fi
}

# Lowercase a flag/env channel value so validation is case-insensitive,
# matching the auto-resume pair above.
normalize_channel() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

# Fail fast on a bogus channel value from the flag or env var (already
# lowercased via normalize_channel). Mirrors the sitter's own Channel::parse,
# so install.sh rejects exactly what the sitter would reject.
validate_channel() {
  case "$1" in
    stable | beta | alpha) ;;
    *) fail "invalid channel '$1' (expected stable, beta, or alpha)" ;;
  esac
}

# Pin a non-default channel in the sitter's config.toml via its own CLI.
#
# The config pin — not a --sitter-channel flag baked into the unit/plist — is
# what makes the choice durable: the sitter resolves flag > env > config.toml >
# stable, and service definitions deliberately pass neither flag nor env, so
# config.toml is the level built for exactly this. It also survives a unit
# being regenerated by a later .deb/brew install, and `intentd update` reports
# "the channel the service follows" by reading this pin alone.
#
# Runs before service setup: `sitter channel` only writes the pin and never
# touches a running daemon, so the service's very first daemon download already
# uses the chosen channel. A failure here is fatal — continuing would silently
# install stable after the user explicitly asked for something else.
pin_channel() {
  if pin_out=$("$install_dir/intentd" sitter channel "$channel" 2>&1); then
    info "update channel pinned to '$channel' — the service follows it across restarts and updates"
  else
    printf '%s\n' "$pin_out" | sed 's/^/  | /' >&2
    fail "could not pin the update channel to '$channel' (reported above); the daemon would install from stable instead"
  fi
}

setup_service_linux() {
  if ! command -v systemctl >/dev/null 2>&1; then
    warn "systemd not found — cannot register a service; start the daemon manually with: intentd serve"
    info "connecting from another machine (desktop/mobile app)? Once the daemon is running: intentd pair"
    return 0
  fi

  unit_name=${INTENTD_SERVICE_NAME:-intentd}
  unit_dir="$HOME/.config/systemd/user"
  mkdir -p "$unit_dir" || fail "cannot create $unit_dir"
  # Carry a custom data dir into the service so it serves the same data dir
  # the install-time CLI used.
  env_line=""
  if [ -n "${INTENTD_DATA_DIR:-}" ]; then
    env_line="Environment=\"INTENTD_DATA_DIR=$(systemd_escape "$INTENTD_DATA_DIR")\""
  fi
  # Same unit the .deb ships (packaging/deb/intentd.service), pointed at the
  # installed binary. ExecStart/ExecStop are quoted: install dirs can contain
  # spaces, and systemd honors quoted words; systemd_escape handles \ " %.
  exec_path=$(systemd_escape "$install_dir/intentd")
  cat >"$unit_dir/$unit_name.service" <<EOF
[Unit]
Description=Intent backend daemon (intentd)
After=network.target

# A systemd user unit does not inherit the graphical session's DISPLAY /
# WAYLAND_DISPLAY, so the daemon sees this service as headless: the default
# agents.resumeInterruptedOnStart=auto resumes interrupted agents at startup
# even on a desktop. Opt out with:
#   intentd settings agents.resumeInterruptedOnStart off
[Service]
Type=simple
ExecStart="$exec_path" serve
ExecStop="$exec_path" stop
Restart=on-failure
$env_line

[Install]
WantedBy=default.target
EOF

  # Written before this check so the hint below is actionable: from an
  # SSH/headless session without a user manager the unit still lands on disk.
  if ! systemctl --user show-environment >/dev/null 2>&1; then
    warn "unit written to $unit_dir/$unit_name.service, but cannot talk to the systemd user manager for this session — enable it later from a login session with:
  systemctl --user enable --now $unit_name"
    info "connecting from another machine (desktop/mobile app)? Once the daemon is running: intentd pair"
    return 0
  fi

  systemctl --user daemon-reload
  systemctl --user enable "$unit_name.service" 2>/dev/null \
    || warn "systemctl --user enable $unit_name failed — the service will not start at login"
  # Log boundary before the start: a failure during the restart itself must
  # land inside the window verify_daemon quotes.
  note_log_start
  # restart, not start: a re-run replaces the binary and must pick it up.
  systemctl --user restart "$unit_name.service" \
    || fail "systemctl --user restart $unit_name failed — inspect with: systemctl --user status $unit_name"
  info "systemd user unit installed and started: $unit_dir/$unit_name.service"

  # User units only run while the user has a session; lingering starts them
  # at boot — essential on headless boxes.
  if command -v loginctl >/dev/null 2>&1; then
    user=$(id -un)
    if [ "$(loginctl show-user "$user" --property=Linger 2>/dev/null)" != "Linger=yes" ]; then
      info "note: user services run only while you are logged in. To start intentd at boot (headless/server), enable lingering:
  sudo loginctl enable-linger $user"
    fi
  fi

  verify_daemon
  apply_auto_resume
  info "manage the service with: systemctl --user {status|stop|restart|disable} $unit_name"
  info "connecting from another machine (desktop/mobile app)? Run: intentd pair"
}

setup_service_macos() {
  label=${INTENTD_SERVICE_NAME:-com.intenthq.intentd}
  plist="$HOME/Library/LaunchAgents/$label.plist"
  mkdir -p "$HOME/Library/LaunchAgents" || fail "cannot create ~/Library/LaunchAgents"
  # Interpolated values are XML-escaped: a path with & or < would otherwise
  # produce an invalid plist and an opaque bootstrap failure.
  xml_bin=$(xml_escape "$install_dir/intentd")
  xml_home=$(xml_escape "$HOME")
  # Carry a custom data dir into the service so it serves the same data dir
  # the install-time CLI used.
  env_block=""
  if [ -n "${INTENTD_DATA_DIR:-}" ]; then
    env_block="	<key>EnvironmentVariables</key>
	<dict>
		<key>INTENTD_DATA_DIR</key>
		<string>$(xml_escape "$INTENTD_DATA_DIR")</string>
	</dict>
"
  fi
  # Same shape the Homebrew formula's `brew services` plist uses: RunAtLoad
  # for start-at-login, KeepAlive relaunches the sitter after a crash but not
  # after a clean exit (`intentd stop` stays stopped until next login).
  cat >"$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>$label</string>
	<key>ProgramArguments</key>
	<array>
		<string>$xml_bin</string>
		<string>serve</string>
	</array>
$env_block	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<dict>
		<key>Crashed</key>
		<true/>
		<key>SuccessfulExit</key>
		<false/>
	</dict>
	<key>StandardOutPath</key>
	<string>$xml_home/Library/Logs/intentd.log</string>
	<key>StandardErrorPath</key>
	<string>$xml_home/Library/Logs/intentd.err.log</string>
</dict>
</plist>
EOF

  uid=$(id -u)
  # bootout tears down any previous registration (idempotent re-runs);
  # bootstrap loads the fresh plist and RunAtLoad starts the agent. bootout
  # is asynchronous — bootstrapping while the old agent is still tearing
  # down fails with EIO — so wait until the label is gone.
  if launchctl bootout "gui/$uid/$label" 2>/dev/null; then
    waited=0
    while launchctl print "gui/$uid/$label" >/dev/null 2>&1; do
      [ "$waited" -lt 10 ] || break
      sleep 1
      waited=$((waited + 1))
    done
  fi
  # Log boundary after the old agent is fully torn down (its shutdown noise
  # stays out of the window) but before bootstrap starts the new one, so a
  # failure during the start itself lands inside the window verify_daemon
  # quotes.
  note_log_start
  launchctl bootstrap "gui/$uid" "$plist" \
    || fail "launchctl bootstrap failed for $plist"
  info "LaunchAgent installed and started: $plist"

  verify_daemon
  apply_auto_resume
  info "manage the service with: launchctl {print|bootout} gui/$uid/$label"
  info "connecting from another machine (desktop/mobile app)? Run: intentd pair"
}

main() {
  service_arg=""
  auto_resume_arg=""
  channel_arg=""
  for arg in "$@"; do
    case "$arg" in
      --service) service_arg="yes" ;;
      --no-service) service_arg="no" ;;
      --auto-resume=*)
        auto_resume_arg=$(normalize_auto_resume "${arg#--auto-resume=}")
        validate_auto_resume "$auto_resume_arg"
        ;;
      --channel=*)
        channel_arg=$(normalize_channel "${arg#--channel=}")
        validate_channel "$channel_arg"
        ;;
      *) fail "unknown option '$arg' (supported: --service, --no-service, --auto-resume=auto|on|off, --channel=stable|beta|alpha)" ;;
    esac
  done
  # Validated before any download so garbage fails fast, interactive or not.
  auto_resume_env=""
  if [ -n "${INTENTD_AUTO_RESUME:-}" ]; then
    auto_resume_env=$(normalize_auto_resume "$INTENTD_AUTO_RESUME")
    validate_auto_resume "$auto_resume_env"
  fi
  # Channel choice: flag beats the env var, same precedence as auto-resume.
  # Empty means "not asked for" — no pin is written and the sitter's own
  # stable default applies, so a re-install never clobbers a hand-set pin.
  channel="$channel_arg"
  if [ -z "$channel" ]; then
    if [ -n "${INTENTD_CHANNEL:-}" ]; then
      channel=$(normalize_channel "$INTENTD_CHANNEL")
      validate_channel "$channel"
    fi
  fi
  # Every intentd call below inherits this environment, and the sitter parses
  # INTENTD_CHANNEL case-sensitively (and lets the flag lose to it only when no
  # flag is given). Re-export the normalized, effective value so a `--channel`
  # flag is not undercut by a stale env var, and so a case-insensitive spelling
  # this script accepts cannot make the sitter reject its own environment.
  if [ -n "$channel" ]; then
    INTENTD_CHANNEL="$channel"
    export INTENTD_CHANNEL
  fi

  os=$(uname -s)
  case "$os" in
    Darwin) vendor_os="apple-darwin" ;;
    Linux) vendor_os="unknown-linux-musl" ;;
    MINGW* | MSYS* | CYGWIN* | Windows_NT)
      fail "Windows detected — use the PowerShell installer instead:
  powershell -c \"irm $BASE_URL/install.ps1 | iex\"" ;;
    *) fail "unsupported operating system '$os' (supported: Linux, Darwin/macOS)" ;;
  esac

  arch=$(uname -m)
  case "$arch" in
    x86_64 | amd64) cpu="x86_64" ;;
    aarch64 | arm64) cpu="aarch64" ;;
    *) fail "unsupported architecture '$arch' (supported: x86_64/amd64, aarch64/arm64)" ;;
  esac

  triple="$cpu-$vendor_os"
  archive="intentd-$triple.tar.xz"

  # download <url> <dest>. wget stays flag-minimal for busybox compatibility.
  if command -v curl >/dev/null 2>&1; then
    download() { curl --proto '=https' --tlsv1.2 --retry 3 -fsSL -o "$2" "$1"; }
  elif command -v wget >/dev/null 2>&1; then
    download() { wget -qO "$2" "$1"; }
  else
    fail "neither curl nor wget is available; install one and re-run"
  fi

  command -v tar >/dev/null 2>&1 || fail "tar is required but was not found"

  # extract <archive> <dir> — GNU tar shells out to the xz binary for .tar.xz,
  # so prefer piping through xz explicitly; macOS bsdtar decompresses xz
  # natively, so it works without the binary. Checked before downloading so a
  # missing tool fails fast.
  if command -v xz >/dev/null 2>&1; then
    extract() { xz -dc "$1" | tar -xf - -C "$2"; }
  elif [ "$os" = "Darwin" ]; then
    extract() { tar -xJf "$1" -C "$2"; }
  else
    fail "xz is required to extract the archive — install it first, e.g.:
  sudo apt install xz-utils   # Debian/Ubuntu (or your distro's equivalent)"
  fi

  # checksum <dir> <sidecar> — verify a "HASH *NAME" sha256 sidecar inside
  # <dir>. Short -c only: busybox sha256sum rejects GNU long options like
  # --check/--status. The sidecar is fetched from the same origin as the
  # archive, so this verifies download integrity, not publisher authenticity.
  if command -v sha256sum >/dev/null 2>&1; then
    checksum() { (cd "$1" && sha256sum -c "$2" >/dev/null 2>&1); }
  elif command -v shasum >/dev/null 2>&1; then
    checksum() { (cd "$1" && shasum -a 256 -c "$2" >/dev/null 2>&1); }
  else
    fail "neither sha256sum nor shasum is available; cannot verify the download"
  fi

  tmpdir=$(mktemp -d)
  trap cleanup EXIT
  trap 'exit 1' INT TERM

  info "downloading $archive from the sitter-latest release..."
  download "$BASE_URL/$archive" "$tmpdir/$archive" \
    || fail "download failed: $BASE_URL/$archive"
  download "$BASE_URL/$archive.sha256" "$tmpdir/$archive.sha256" \
    || fail "download failed: $BASE_URL/$archive.sha256"

  checksum "$tmpdir" "$archive.sha256" \
    || fail "sha256 verification failed for $archive"
  info "sha256 verified"

  extract "$tmpdir/$archive" "$tmpdir" \
    || fail "extraction failed for $archive"
  binary="$tmpdir/intentd-$triple/intentd"
  [ -f "$binary" ] || fail "archive did not contain intentd-$triple/intentd"

  if [ -n "${INTENTD_INSTALL_DIR:-}" ]; then
    install_dir="$INTENTD_INSTALL_DIR"
    mkdir -p "$install_dir" || fail "cannot create $install_dir"
  elif [ -d /usr/local/bin ] && [ -w /usr/local/bin ]; then
    install_dir="/usr/local/bin"
  else
    install_dir="$HOME/.local/bin"
    mkdir -p "$install_dir" || fail "cannot create $install_dir"
  fi

  # Stage next to the destination, then rename: atomic on the same filesystem,
  # and replacing a running binary via rename is safe where in-place copy is
  # not. The EXIT trap sweeps the staged file if we die between cp and mv.
  staged="$install_dir/.intentd.install.$$"
  cp "$binary" "$staged" || fail "cannot write to $install_dir"
  chmod 755 "$staged"
  mv -f "$staged" "$install_dir/intentd" \
    || fail "cannot install to $install_dir/intentd"
  staged=""

  version=$("$install_dir/intentd" --sitter-version 2>/dev/null) || version=""
  if [ -n "$version" ]; then
    info "installed $version to $install_dir/intentd"
  else
    info "installed intentd to $install_dir/intentd"
  fi

  case ":$PATH:" in
    *":$install_dir:"*) ;;
    *) warn "$install_dir is not on your PATH — add it, e.g.:
  export PATH=\"$install_dir:\$PATH\"" ;;
  esac

  # Before the service decision: a manual `intentd serve` should honour the
  # requested channel just as the service does.
  if [ -n "$channel" ]; then
    pin_channel
  fi

  # Service setup decision: flags beat the env var beats the prompt. The
  # prompt talks to /dev/tty because `curl | sh` occupies stdin; when no
  # terminal is available (CI, provisioning) it must never hang, so the
  # default there is to skip with a hint.
  service_mode="$service_arg"
  if [ -z "$service_mode" ]; then
    # Case-insensitive to match install.ps1 (FALSE/No must also mean skip).
    case "${INTENTD_INSTALL_SERVICE:-}" in
      '') ;;
      0 | [Ff][Aa][Ll][Ss][Ee] | [Nn][Oo]) service_mode="no" ;;
      *) service_mode="yes" ;;
    esac
  fi
  if [ -z "$service_mode" ]; then
    if (exec </dev/tty) 2>/dev/null; then
      printf 'Set up intentd to start at login and start it now? [Y/n] ' >/dev/tty
      reply=""
      read -r reply </dev/tty || reply=""
      case "$reply" in
        [nN]*) service_mode="no" ;;
        *) service_mode="yes" ;;
      esac
    else
      service_mode="skip"
    fi
  fi

  if [ "$service_mode" = "yes" ]; then
    # Auto-resume choice: flag beats the env var beats the prompt (both were
    # lowercased + validated above). Same /dev/tty pattern as the service
    # prompt; `auto` — or no terminal — is the daemon default and writes
    # nothing. Note the answer is applied only after the service's first
    # start (apply_auto_resume runs after verify_daemon), so on a re-install
    # over an existing data dir that first start still runs under the prior
    # effective setting.
    auto_resume="$auto_resume_arg"
    if [ -z "$auto_resume" ]; then
      auto_resume="$auto_resume_env"
    fi
    if [ -z "$auto_resume" ]; then
      if (exec </dev/tty) 2>/dev/null; then
        # Ask again on an answer we genuinely cannot read, rather than
        # silently installing the default the answer may have been rejecting.
        # Bounded, and an empty answer (including EOF, which read reports by
        # leaving reply empty) still takes the default, so a scripted or
        # garbage-fed terminal cannot hold the install open.
        auto_resume_tries=0
        while [ -z "$auto_resume" ] && [ "$auto_resume_tries" -lt 3 ]; do
          auto_resume_tries=$((auto_resume_tries + 1))
          printf 'Auto-resume interrupted agents when the service starts? [auto/on/off] (default auto) ' >/dev/tty
          reply=""
          read -r reply </dev/tty || reply=""
          case "$(normalize_auto_resume "$reply")" in
            '' | auto) auto_resume="auto" ;;
            on) auto_resume="on" ;;
            off) auto_resume="off" ;;
            *) warn "unrecognized answer '$reply' — answer auto, on, or off (yes/no also work)" ;;
          esac
        done
        if [ -z "$auto_resume" ]; then
          warn "still no recognized answer — keeping the default (auto)"
          auto_resume="auto"
        fi
      else
        auto_resume="auto"
      fi
    fi
    if [ "$os" = "Darwin" ]; then
      setup_service_macos
    else
      setup_service_linux
    fi
    return 0
  fi

  if [ "$service_mode" = "skip" ]; then
    info "skipping service setup (no interactive terminal detected). To set it up, re-run with INTENTD_INSTALL_SERVICE=1"
  fi
  printf '%s\n' "
Next steps:
  intentd serve   # start the daemon in the foreground (downloads the real daemon on first run)
  intentd pair    # once the daemon is running (another terminal): pairing info for remote clients (desktop/mobile app)

To run intentd at login as a background service, re-run this installer with
INTENTD_INSTALL_SERVICE=1, or use a package-manager install:
  Homebrew (macOS/Linux):  brew install intent-hq/tap/intentd && brew services start intentd
  Debian/Ubuntu (.deb):    installs a systemd user unit — systemctl --user enable --now intentd"
}

# Wrapped in main() so a truncated download can never execute a partial
# script; nothing runs until the closing line below has been parsed.
main "$@"
