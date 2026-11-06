#!/usr/bin/env python3
"""Render the sitter formula, retaining truthful prior Intel metadata if absent."""
import argparse
import hashlib
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
INTEL = 'x86_64-apple-darwin'
VERSION = r'[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.]+)?'
BASE = 'https://github.com/intent-hq/intentd-releases/releases/download/sitter-v'


def intel_stanza(version, digest):
    return (f'    on_intel do\n'
            f'      version "{version}"\n'
            f'      url "{BASE}{version}/intentd-{INTEL}.tar.xz"\n'
            f'      sha256 "{digest}"\n'
            f'    end\n')


def previous_intel(previous):
    if previous is None or not previous.exists():
        return ''
    text = previous.read_text()
    mac = re.search(r'^  on_macos do\n(.*?)^  end$', text, re.M | re.S)
    if not mac:
        raise ValueError('previous formula has no recognizable macOS section')
    block = re.search(r'^    on_intel do\n(.*?)^    end$', mac[1], re.M | re.S)
    if not block:
        return ''
    url = re.search(r'^      url "' + re.escape(BASE) + '(' + VERSION + ')/intentd-' + re.escape(INTEL) + r'\.tar\.xz"$', block[1], re.M)
    digest = re.search(r'^      sha256 "([0-9a-f]{64})"$', block[1], re.M)
    versions = re.findall(r'^\s+version "(' + VERSION + ')"$', block[1], re.M)
    if not versions:
        versions = re.findall(r'^  version "(' + VERSION + ')"$', text, re.M)
    if not url or not digest or versions != [url[1]]:
        raise ValueError('previous Intel formula URL, version and checksum must agree')
    return intel_stanza(url[1], digest[1])


def render(version, artifacts, previous=None):
    if not re.fullmatch(VERSION, version):
        raise ValueError('invalid sitter release version')
    text = (ROOT / 'packaging/homebrew/intentd.rb.template').read_text().replace('{{VERSION}}', version)
    for target in ('aarch64-apple-darwin', 'aarch64-unknown-linux-musl', 'x86_64-unknown-linux-musl'):
        with (artifacts / f'intentd-{target}.tar.xz').open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        text = text.replace('{{SHA256_' + target.upper().replace('-', '_') + '}}', digest)
    intel = artifacts / f'intentd-{INTEL}.tar.xz'
    if intel.exists():
        with intel.open('rb') as stream:
            stanza = intel_stanza(version, hashlib.file_digest(stream, 'sha256').hexdigest())
    else:
        stanza = previous_intel(previous)
    text = text.replace('{{MACOS_INTEL}}\n', stanza)
    if re.search(r'\{\{[A-Z0-9_]+\}\}', text):
        raise ValueError('unrendered formula placeholders')
    return text


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('version')
    parser.add_argument('artifacts', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('previous', type=Path, nargs='?')
    args = parser.parse_args()
    args.output.write_text(render(args.version, args.artifacts, args.previous))
