#!/usr/bin/env bash
# Builds the intent-passkey sidecar for the CPU of a daemon release leg and stages
# it as packaging/libexec/intent-passkey (plus its LGPL text), so dist's
# `include = ["packaging/libexec"]` packs it into that leg's archive. Non-Linux
# legs are a no-op.
#
# Why glibc and not the daemon's musl target: libwebauthn needs libdbus (BlueZ)
# and hidapi's hidraw backend, which links libudev; device passkeys check the
# PIN through libpam. None has a supported static musl build, and all are
# present on every desktop Linux that runs the Intent AppImage, so the sidecar
# links them dynamically against glibc. The
# x86_64 runner builds aarch64 with Debian multiarch (gcc cross + arm64 -dev
# packages from ports.ubuntu.com).
#
# Usage: scripts/build-intent-passkey.sh <dist target triple>
# Needs passwordless sudo for apt-get unless the -dev packages are installed.
set -euo pipefail

leg=${1:?usage: build-intent-passkey.sh <dist target triple>}
case "$leg" in
  x86_64-unknown-linux-*) arch=x86_64 deb_arch=amd64 ;;
  aarch64-unknown-linux-*) arch=aarch64 deb_arch=arm64 ;;
  *)
    echo "intent-passkey: nothing to build for $leg"
    exit 0
    ;;
esac

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
manifest="$root/intent-passkey/Cargo.toml"
dest="$root/packaging/libexec"
target="$arch-unknown-linux-gnu"
host_arch=$(uname -m)

apt_install() {
  local i
  for i in 1 2 3; do
    if sudo -n apt-get -o DPkg::Lock::Timeout=300 update -qq; then break; fi
    if [ "$i" -eq 3 ]; then return 1; fi
    sleep $((i * 15))
  done
  sudo -n apt-get -o DPkg::Lock::Timeout=300 install -y -qq --no-install-recommends "$@"
}

# Restricts the existing Ubuntu sources to the host architecture and adds the
# mirror that carries $deb_arch, so `apt-get install pkg:$deb_arch` resolves.
enable_foreign_arch() {
  local host_deb codename mirror suites
  host_deb=$(dpkg --print-architecture)
  codename=$(. /etc/os-release && echo "$VERSION_CODENAME")
  sudo -n dpkg --add-architecture "$deb_arch"
  if [ -f /etc/apt/sources.list.d/ubuntu.sources ] &&
    ! grep -q '^Architectures:' /etc/apt/sources.list.d/ubuntu.sources; then
    sudo -n sed -i "/^Types:/a Architectures: $host_deb" /etc/apt/sources.list.d/ubuntu.sources
  fi
  if [ -f /etc/apt/sources.list ]; then
    sudo -n sed -i -E "s/^deb (http|https|mirror)/deb [arch=$host_deb] \1/" /etc/apt/sources.list
  fi
  case "$deb_arch" in
    amd64 | i386) mirror=http://archive.ubuntu.com/ubuntu ;;
    *) mirror=http://ports.ubuntu.com/ubuntu-ports ;;
  esac
  suites="$codename $codename-updates $codename-security"
  printf 'Types: deb\nURIs: %s\nSuites: %s\nComponents: main universe\nArchitectures: %s\n' \
    "$mirror" "$suites" "$deb_arch" |
    sudo -n tee "/etc/apt/sources.list.d/intent-passkey-$deb_arch.sources" >/dev/null
}

# libpam0g-dev ships no pkg-config file, so look for its linker symlink.
has_pam() { [ -e "/usr/lib/$arch-linux-gnu/libpam.so" ]; }

if [ "$host_arch" = "$arch" ]; then
  if ! pkg-config --exists dbus-1 libudev || ! has_pam; then
    apt_install pkg-config libdbus-1-dev libudev-dev libpam0g-dev
  fi
else
  if ! PKG_CONFIG_LIBDIR="/usr/lib/$arch-linux-gnu/pkgconfig" pkg-config --exists dbus-1 libudev ||
    ! has_pam || ! command -v "$arch-linux-gnu-gcc" >/dev/null; then
    enable_foreign_arch
    apt_install pkg-config "gcc-${arch//_/-}-linux-gnu" "libc6-dev-$deb_arch-cross" \
      "libdbus-1-dev:$deb_arch" "libudev-dev:$deb_arch" "libpam0g-dev:$deb_arch"
  fi
  triple_env=$(echo "$target" | tr 'a-z-' 'A-Z_')
  export PKG_CONFIG_ALLOW_CROSS=1
  export PKG_CONFIG_LIBDIR="/usr/lib/$arch-linux-gnu/pkgconfig:/usr/share/pkgconfig"
  export "CARGO_TARGET_${triple_env}_LINKER=$arch-linux-gnu-gcc"
  export "CC_${target//-/_}=$arch-linux-gnu-gcc"
fi

if command -v rustup >/dev/null 2>&1; then
  rustup target add "$target" >/dev/null
fi
cargo build --release --locked --manifest-path "$manifest" --target "$target"

binary="$root/intent-passkey/target/$target/release/intent-passkey"
mkdir -p "$dest"
install -m 0755 "$binary" "$dest/intent-passkey"
libwebauthn_dir=$(cargo metadata --locked --format-version 1 --manifest-path "$manifest" --filter-platform "$target" |
  python3 -c 'import json, os, sys
meta = json.load(sys.stdin)
print(next(os.path.dirname(p["manifest_path"]) for p in meta["packages"] if p["name"] == "libwebauthn"))')
install -m 0644 "$libwebauthn_dir/COPYING" "$dest/intent-passkey.LGPL-2.1.txt"

glibc_floor=$(objdump -T "$binary" | grep -oE 'GLIBC_[0-9]+\.[0-9]+(\.[0-9]+)?' | sort -uV | tail -n1)
echo "intent-passkey: staged $target build in $dest (needs $glibc_floor, libdbus-1.so.3, libudev.so.1, libpam.so.0)"
