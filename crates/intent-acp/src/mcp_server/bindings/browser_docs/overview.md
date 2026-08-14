# Browser API Overview

The browser API gives you programmatic access to embedded browser tabs via Chrome DevTools Protocol.
Actions are executed as a sequence of declarative operations.

## Basic Actions
- `{ action: "listTabs" }` - List all browser tabs
- `{ action: "getAccessibilityTree", tabId? }` - Get page structure as YAML
- `{ action: "screenshot", tabId? }` - Take a screenshot
- `{ action: "evaluate", expression, tabId? }` - Run JavaScript in the page
- `{ action: "focusTab", tabId? }` - Focus/remount a tab

## Capture Actions (for debugging)
- `{ action: "snapshot", workspaceId, name?, reload?, waitFor? }` - Quick point-in-time capture
- `{ action: "startSession", workspaceId, name? }` / `{ action: "endSession", sessionId }` - Multi-step capture
- `{ action: "startCapture", sessionId }` / `{ action: "endCapture", sessionId }` - Event capture
- `{ action: "captureStep", sessionId, stepName, reload?, waitFor? }` - Capture a named step
- `{ action: "startTrace", sessionId, traceName }` / `{ action: "stopTrace", sessionId, traceName }` - Performance traces

## Navigation
- `{ action: "navigate", url, tabId? }` - Navigate an existing tab to a new URL

## Recovery
- `{ action: "resetTab", tabId? }` - Force reset CDP connection (use if you get "already attached" errors)

## UI Control
- `{ action: "openTab", url, position? }` - Open a new browser tab in the UI
  - position: 'adjacent' (default), 'replace', or 'same'
- `{ action: "closeTab", tabId }` - Close a browser tab
  - tabId is REQUIRED; it does not fall back to the sequence-level default tabId

## Supported Protocols
The embedded browser supports http://, https://, and file:// URLs.
You can open local HTML files directly: `{ action: "openTab", url: "file:///path/to/index.html" }`

## Loopback URLs: daemon vs client machine

The embedded browser runs on the client (user's) machine, while servers you start (via
`ws.host.exec`, scripts, terminals) run on the daemon machine. In a remote-daemon setup
these are different machines, so `navigate`/`openTab` URLs support reserved hostnames:

- `daemon.localhost` — the machine where the daemon runs (where your servers run).
  Local setup: rewritten to `127.0.0.1`. Remote setup: rewritten to the daemon host.
- `client.localhost` — the user's machine (where the embedded browser runs). Always
  rewritten to `127.0.0.1`.
- Bare `127.0.0.1` / `localhost` / `[::1]` — ambiguous; assumed to mean the daemon
  machine (your frame of reference). Unchanged in a local setup; in a remote setup it is
  rewritten to the daemon host and the result carries a `warning` suggesting the explicit
  aliases.

Only the hostname is rewritten — scheme, port, path, query, and hash are preserved. Every
rewritten action's result echoes `{ requestedUrl, finalUrl, rewritten: true, reason, warning? }`
so you can see what was opened. Prefer `daemon.localhost` for servers you started.

Remote-daemon caveat: the client's browser reaches daemon-side servers over the network,
so prefer binding `0.0.0.0` and keeping the port reachable from the client machine. URLs
rewritten to a remote daemon host are reachability-probed before navigating. If the origin
cannot be reached, the Electron desktop client automatically forwards the port over the
daemon connection and opens the tunneled URL instead — the result carries `tunneled: true`
and the `reason` describes the forward — so servers bound to `127.0.0.1` on the daemon
still work. Web-browser clients cannot open a local tunnel listener, so there the action
fails with an explanatory error (pointing at `127.0.0.1`-only binding or a firewall)
instead of opening a broken page.

Use `browser_docs` with topic="capture" or topic="examples" for detailed usage.