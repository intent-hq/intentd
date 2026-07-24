# Changelog

All notable changes to this project will be documented in this file.

## [0.2.5] - 2026-07-24

### 🐛 Bug Fixes

- Reject bare-model/provider mismatch at agent creation and setModel ([#425](https://github.com/intent-hq/intentd/pull/425))
- Lift proposals when the provider collapses raw_output ([#427](https://github.com/intent-hq/intentd/pull/427))

### 🧪 Testing

- Deflake graceful_shutdown_allows_immediate_restart port contention (monorepo#466) ([#429](https://github.com/intent-hq/intentd/pull/429))


## [0.2.4] - 2026-07-24

### 🚀 Features

- Emit specialists:changed on specialist file changes ([#426](https://github.com/intent-hq/intentd/pull/426))

### 🐛 Bug Fixes

- Fail closed on nonexistent agent in agent.queueMessage and agent.watchCompletion (monorepo#568) ([#408](https://github.com/intent-hq/intentd/pull/408))
- Deliver workspace MCP servers to grok sessions ([#412](https://github.com/intent-hq/intentd/pull/412))
- Annotate stale queued-message redrives and keep delivered completion report ([#576](https://github.com/intent-hq/intentd/pull/576)) ([#413](https://github.com/intent-hq/intentd/pull/413))
- Drop the draft workspace FK so opaque draft keys work (PROTOCOL 5.16) ([#420](https://github.com/intent-hq/intentd/pull/420))
- Spawn chief agents in dedicated empty chief-cwd dir instead of /tmp ([#419](https://github.com/intent-hq/intentd/pull/419))

### 🧪 Testing

- Poll system.status for WSS port with bounded backoff ([#409](https://github.com/intent-hq/intentd/pull/409))
- Deflake uds_note_subscription frame/state awaits (monorepo#601)


## [0.2.3] - 2026-07-23

### 🐛 Bug Fixes

- Fail closed on nonexistent agent in agent.send and sender-watch paths ([#407](https://github.com/intent-hq/intentd/pull/407))


## [0.2.2] - 2026-07-23

### 🚀 Features

- Free-text query on github.issues.search / github.pulls.search ([#391](https://github.com/intent-hq/intentd/pull/391))
- NextToken pagination on linear.listIssues / linear.searchIssues ([#398](https://github.com/intent-hq/intentd/pull/398))
- NextToken pagination on sentry.listIssues / sentry.searchIssues ([#403](https://github.com/intent-hq/intentd/pull/403))

### 🐛 Bug Fixes

- Switch codex provider to @agentclientprotocol/codex-acp and FirstTurnPrepend injection ([#387](https://github.com/intent-hq/intentd/pull/387))

### 🧪 Testing

- Bounded retry for WSS e2e connection establishment (intent-hq/monorepo#553)
- Route e2e_wss_sentry_pagination connect through shared retry helper (intent-hq/monorepo#553) ([#405](https://github.com/intent-hq/intentd/pull/405))

### ⚙️ Miscellaneous Tasks

- Bump sysinfo from 0.36.1 to 0.39.6 ([#395](https://github.com/intent-hq/intentd/pull/395))

