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
# Idempotent: re-running replaces the installed binary atomically.
set -eu

BASE_URL="https://github.com/intent-hq/intentd-releases/releases/download/sitter-latest"

info() { printf '%s\n' "install.sh: $*"; }
warn() { printf '%s\n' "install.sh: warning: $*" >&2; }
fail() { printf '%s\n' "install.sh: error: $*" >&2; exit 1; }

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

# download <url> <dest>
if command -v curl >/dev/null 2>&1; then
  download() { curl -fsSL -o "$2" "$1"; }
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

# checksum <dir> <sidecar> — verify a "HASH *NAME" sha256 sidecar inside <dir>.
if command -v sha256sum >/dev/null 2>&1; then
  checksum() { (cd "$1" && sha256sum --check --status "$2"); }
elif command -v shasum >/dev/null 2>&1; then
  checksum() { (cd "$1" && shasum -a 256 --check --status "$2"); }
else
  fail "neither sha256sum nor shasum is available; cannot verify the download"
fi

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT INT TERM

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
# and replacing a running binary via rename is safe where in-place copy is not.
staged="$install_dir/.intentd.install.$$"
cp "$binary" "$staged" || fail "cannot write to $install_dir"
chmod 755 "$staged"
mv -f "$staged" "$install_dir/intentd" \
  || { rm -f "$staged"; fail "cannot install to $install_dir/intentd"; }

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

printf '%s\n' "
Next steps:
  intentd serve   # start the daemon in the foreground (downloads the real daemon on first run)

To run intentd as a managed background service instead, use a package-manager install:
  Homebrew (macOS/Linux):  brew install intent-hq/tap/intentd && brew services start intentd
  Debian/Ubuntu (.deb):    installs a systemd user unit — systemctl --user enable --now intentd"
