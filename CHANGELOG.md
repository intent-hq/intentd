# Changelog

All notable changes to this project will be documented in this file.

## [0.2.11] - 2026-07-28

### 🚀 Features

- *(services)* Warn live unsloth agents before a model-switch restart ([#647](https://github.com/intent-hq/intentd/pull/647))
- *(services)* Port-conflict detection for the managed unsloth server ([#660](https://github.com/intent-hq/intentd/pull/660))
- *(unsloth)* Add unsloth.status and unsloth.stop daemon RPCs ([#623](https://github.com/intent-hq/intentd/pull/623))
- *(services)* Real event.subscribe delivery with batching and restart persistence (monorepo#937) ([#632](https://github.com/intent-hq/intentd/pull/632))
- Event-subscription introspection + workspace-delete cleanup (monorepo#947) ([#644](https://github.com/intent-hq/intentd/pull/644))
- Add isWaitingForOtherAgents to the agent:idle payload ([#651](https://github.com/intent-hq/intentd/pull/651))
- *(services)* Expose secondary-binary status in host.providerDiscovery ([#668](https://github.com/intent-hq/intentd/pull/668))
- Remove model.workspaceOverrides setting layer ([#669](https://github.com/intent-hq/intentd/pull/669))
- Workspace status screenshot — statusImageAssetId + ws.workspace.setStatusImage ([#671](https://github.com/intent-hq/intentd/pull/671))
- *(store)* Sweep high-churn state-notification event families (72h retention) ([#677](https://github.com/intent-hq/intentd/pull/677))

### 🐛 Bug Fixes

- Report the platform file manager as always installed on macOS/Windows ([#655](https://github.com/intent-hq/intentd/pull/655))
- Renumber duplicate migration version 0062 to 0063 ([#674](https://github.com/intent-hq/intentd/pull/674))
- Default interactive terminal TERM for Backspace erase ([#952](https://github.com/intent-hq/intentd/pull/952)) ([#638](https://github.com/intent-hq/intentd/pull/638))
- Demote agentCommit unattributed-dirty skip log to debug ([#645](https://github.com/intent-hq/intentd/pull/645))
- Pre-gate wakeOrCreate watch scope before side effects (monorepo#932)
- *(services)* Skip wakeOrCreate SUB-1 watch and pre-gate for a deleted caller ([#667](https://github.com/intent-hq/intentd/pull/667))
- *(metrics)* Stop double counting shared paths across agent attribution rows (monorepo#1009) ([#683](https://github.com/intent-hq/intentd/pull/683))
- *(usage-stats)* Stop re-recording shared-path growth on row updates (monorepo#1023) ([#689](https://github.com/intent-hq/intentd/pull/689))
- *(acp)* Unwrap codex nested MCP tool-call arguments in session mapping
- Stamp sender attribution on wakeOrCreate context message (monorepo#1015) ([#681](https://github.com/intent-hq/intentd/pull/681))
- Recreate ACP session on retry of poisoned session (monorepo#940)
- Decouple CoW sandbox provisioning from the delegate critical path ([#636](https://github.com/intent-hq/intentd/pull/636))
- Filter agent-initiated agentCommit to the agent's attributed paths (intent-hq/monorepo#939)
- Honor isNewRepo in workspace.create — initialize repository before provisioning
- Doctor names the actually-missing binary for dual-binary providers ([#653](https://github.com/intent-hq/intentd/pull/653))
- Multi-agent attribution rows + directory-rename attribution (monorepo#957) ([#670](https://github.com/intent-hq/intentd/pull/670))
- Deliver preempted message combined with interrupt on zero-output interrupt ([#685](https://github.com/intent-hq/intentd/pull/685))
- Annotate suspected-stall completions in parent wakes (monorepo#1016) ([#688](https://github.com/intent-hq/intentd/pull/688))

### 🔧 Refactor

- *(services)* Use UNKNOWN_PROVIDER alias for the stats provider fallback ([#654](https://github.com/intent-hq/intentd/pull/654))

### ⚡ Performance

- *(store)* Index + keys-only window for agent message projections (monorepo#1010) ([#673](https://github.com/intent-hq/intentd/pull/673))
- *(store)* Per-session message projection for agent.get (monorepo#981) ([#659](https://github.com/intent-hq/intentd/pull/659))
- *(services)* Run CoW sandbox clone on the blocking pool ([#656](https://github.com/intent-hq/intentd/pull/656))
- *(store)* Bound agent.list projection payload via SQL text-block extraction ([#679](https://github.com/intent-hq/intentd/pull/679))
- Eliminate multi-core CPU burn on large repos (diff rollup, adaptive TTL, pushed-check) ([#648](https://github.com/intent-hq/intentd/pull/648))
- Bound agent read paths — stop hydrating full transcripts (monorepo#958)
- Cap persisted agent:tool:call payloads at 16KiB and drop TTL to 6h ([#680](https://github.com/intent-hq/intentd/pull/680))

### 🧪 Testing

- Make flaky card-aggregates ordering and token-usage scan tests deterministic ([#658](https://github.com/intent-hq/intentd/pull/658))
- Make flaky provider-models CLI and agent-ops/unsloth timing tests deterministic under load ([#663](https://github.com/intent-hq/intentd/pull/663))
- *(intentd)* Quiesce activity before paired lastActivity reads (monorepo#1004) ([#682](https://github.com/intent-hq/intentd/pull/682))


## [0.2.10] - 2026-07-27

### 🚀 Features

- Daemon-managed Unsloth server lifecycle ([#597](https://github.com/intent-hq/intentd/pull/597))
- *(unsloth)* Select best-fitting GGUF quant variant at spawn time ([#610](https://github.com/intent-hq/intentd/pull/610))
- CoW provisioning phase timings + configurable clone exclusions ([#614](https://github.com/intent-hq/intentd/pull/614))
- Model-change transcript notice + cross-provider replay e2e coverage (monorepo#882) ([#598](https://github.com/intent-hq/intentd/pull/598))
- Inject scoped GitHub credential helper into terminal and agent spawn environments ([#601](https://github.com/intent-hq/intentd/pull/601))
- BE-owned Workspace.displayStatus with change event ([#600](https://github.com/intent-hq/intentd/pull/600))
- Background retry sweep for merge_pending sandboxes ([#608](https://github.com/intent-hq/intentd/pull/608))
- Daemon-backed git credential helper for terminal and agent spawns ([#618](https://github.com/intent-hq/intentd/pull/618))

### 🐛 Bug Fixes

- *(unsloth)* Preserve in-flight startup across mint timeouts and spawn retries (monorepo#878) ([#621](https://github.com/intent-hq/intentd/pull/621))
- *(providers)* Require unsloth CLI alongside opencode for unsloth provider discovery ([#622](https://github.com/intent-hq/intentd/pull/622))
- *(acp)* Hold stdin lines racing a pending mcp-bridge reconnect ([#620](https://github.com/intent-hq/intentd/pull/620))
- *(acp)* Make mcp-bridge resilient to daemon restarts and TCP drops ([#871](https://github.com/intent-hq/intentd/pull/871)) ([#595](https://github.com/intent-hq/intentd/pull/595))
- Allow cross-provider agent.setModel after first turn (monorepo#882) ([#604](https://github.com/intent-hq/intentd/pull/604))
- *(acp)* Buffer stdin during mcp-bridge initial connect window (monorepo#908) ([#611](https://github.com/intent-hq/intentd/pull/611))
- *(services)* Subscribe caller to completion on wakeOrCreate created_new branch ([#627](https://github.com/intent-hq/intentd/pull/627))
- Skip foreign session/load after a committed cross-provider setModel ([#625](https://github.com/intent-hq/intentd/pull/625))

### 🧪 Testing

- *(services)* Fix flaky dismiss_attention_idempotent event-order race (monorepo#905)
- *(unsloth)* Cover retry-attach across a model switch (monorepo#878) ([#628](https://github.com/intent-hq/intentd/pull/628))


## [0.2.9] - 2026-07-27

### 🚀 Features

- Unsloth provider registry entry + HF GGUF catalog with memory-fit filtering ([#593](https://github.com/intent-hq/intentd/pull/593))

### 🐛 Bug Fixes

- Robust sandbox merge-back and faster best-effort CoW clone ([#592](https://github.com/intent-hq/intentd/pull/592))

### ⚙️ Miscellaneous Tasks

- Fail PRs containing committed git conflict markers (#588 incident) ([#591](https://github.com/intent-hq/intentd/pull/591))


## [0.2.8] - 2026-07-27

### 🚀 Features

- Structured error.data for unresolvable base ref (monorepo#761) ([#525](https://github.com/intent-hq/intentd/pull/525))
- Local wall-clock bucketing for usage stats (D12) ([#544](https://github.com/intent-hq/intentd/pull/544))
- System.capabilities RPC exposing machine-level cowSupported (protocol 2.3) ([#549](https://github.com/intent-hq/intentd/pull/549))
- Github.repoConfig.get RPC — fetch .intent/config.json remotely (protocol 2.4) ([#557](https://github.com/intent-hq/intentd/pull/557))
- Ws.app.question.ask binding with AtTurnEnd question attachments (intent-hq/monorepo#732)
- Inject stored GitHub token into clones and classify auth failures (monorepo#825)
- Circuit breaker for provider-blocked (poisoned) agent sessions (monorepo#840)
- Skip daemon-managed manifests in legacy import ([#579](https://github.com/intent-hq/intentd/pull/579))
- Migrate parked queues and GC poisoned sessions in agent.wakeOrCreate (monorepo#847) ([#585](https://github.com/intent-hq/intentd/pull/585))
- Stream harness-wake session/updates as implicit agent-initiated turns ([#587](https://github.com/intent-hq/intentd/pull/587))

### 🐛 Bug Fixes

- Include archived workspaces in the workspace.subscribe snapshot
- Detect dead ACP child processes and recover transparently ([#764](https://github.com/intent-hq/intentd/pull/764)) ([#523](https://github.com/intent-hq/intentd/pull/523))
- Rename skipWorktree -> skipIsolation in workspace.update params ([#533](https://github.com/intent-hq/intentd/pull/533))
- CowSupported probe default-root fallback + CoW-to-worktree creation fallback ([#540](https://github.com/intent-hq/intentd/pull/540))
- Scrub phantom anchor markers and support overlapping comment ranges ([#541](https://github.com/intent-hq/intentd/pull/541))
- Emit completionReport alongside report on wake/idle payloads ([#548](https://github.com/intent-hq/intentd/pull/548))
- Keep tool title/name/input across sparse tool_call_update events ([#551](https://github.com/intent-hq/intentd/pull/551))
- Expand leading ~ in workspace.create and git.clone paths ([#554](https://github.com/intent-hq/intentd/pull/554))
- Classify workspace.create clone failures into typed errors with sanitized detail (monorepo#826)
- Emit trailing AtTurnEnd attachment blocks on agent:stream:end (intent-hq/monorepo#732)
- Make CoW cloning best-effort and handle git-worktree edge cases ([#574](https://github.com/intent-hq/intentd/pull/574))


## [0.2.7] - 2026-07-25

### 🚀 Features

- BaseRef-aware PR-workspace matching (intent-hq/monorepo#459)
- Add hidden flag to specialist definitions ([#471](https://github.com/intent-hq/intentd/pull/471))
- Turn-attachment registry for deterministic resource-block attach ([#482](https://github.com/intent-hq/intentd/pull/482))
- Carry stopReason and messageId on interrupt agent:stream:end ([#492](https://github.com/intent-hq/intentd/pull/492))
- Capture live token usage from ACP turn end ([#485](https://github.com/intent-hq/intentd/pull/485))
- Expose workspace archive/unarchive on the agent MCP bridge ([#733](https://github.com/intent-hq/intentd/pull/733)) ([#499](https://github.com/intent-hq/intentd/pull/499))
- Make comment.respond reply-anchoring contract explicit (monorepo#729) ([#496](https://github.com/intent-hq/intentd/pull/496))
- Carry isBackground on agent:idle payload ([#501](https://github.com/intent-hq/intentd/pull/501))
- Opt-in CoW workspace provisioning (cowIsolation checkouts, checkoutMode, sandboxes) ([#507](https://github.com/intent-hq/intentd/pull/507))
- Comment.add accepts optional client-supplied commentId ([#514](https://github.com/intent-hq/intentd/pull/514))

### 🐛 Bug Fixes

- Comment.getThread/resolveThread/list caller-input errors return -32602 (intent-hq/monorepo#649)
- *(test)* Scale fixed 5s daemon-read timeouts by the shared test budget (intent-hq/monorepo#615) ([#457](https://github.com/intent-hq/intentd/pull/457))
- Reject review-requested filter on github.issues.search (intent-hq/monorepo#551) ([#462](https://github.com/intent-hq/intentd/pull/462))
- Include script:output in ephemeral event retention sweep (monorepo#620) ([#432](https://github.com/intent-hq/intentd/pull/432))
- Inherit hidden flag across specialist tiers ([#480](https://github.com/intent-hq/intentd/pull/480))
- Use UTF-16 offset kind and recover poisoned CRDT sessions mutex (monorepo#721) ([#487](https://github.com/intent-hq/intentd/pull/487))
- Route line-attribution:updated through the transient publish path (monorepo#720) ([#488](https://github.com/intent-hq/intentd/pull/488))
- Auto-activate incremental auto_vacuum at daemon startup (monorepo#720) ([#500](https://github.com/intent-hq/intentd/pull/500))
- Emit full applied delta on workspace archive/unarchive ([#508](https://github.com/intent-hq/intentd/pull/508))
- Statically link vendored OpenSSL on macOS so packaged intentd runs without Homebrew (intent-hq/monorepo#776)

### ⚡ Performance

- Relax sweep cadences and pause between workspaces (monorepo#703) ([#465](https://github.com/intent-hq/intentd/pull/465))

### 🧪 Testing

- Deflake queue-drain event-order race in e2e_wss_agent_lifecycle (monorepo#456) ([#459](https://github.com/intent-hq/intentd/pull/459))


## [0.2.6] - 2026-07-24

### 🚀 Features

- Add recoverable legacy import RPC ([#423](https://github.com/intent-hq/intentd/pull/423))
- Extend bare-model ownership validation to cached dynamic catalogs ([#607](https://github.com/intent-hq/intentd/pull/607)) ([#433](https://github.com/intent-hq/intentd/pull/433))
- Accept optional authorType on comment.respond ([#434](https://github.com/intent-hq/intentd/pull/434))
- Comment.add echoes post-rewrite noteRev, commits atomically, and emits note:updated (intent-hq/monorepo#638) ([#447](https://github.com/intent-hq/intentd/pull/447))
- *(providers)* Deliver workspace MCP tools to pi via bundled extension ([#452](https://github.com/intent-hq/intentd/pull/452))

### 🐛 Bug Fixes

- Enrich host PATH from login shell ([#422](https://github.com/intent-hq/intentd/pull/422))
- Enforce UDS-only guard on system.shutdown (monorepo#630) ([#436](https://github.com/intent-hq/intentd/pull/436))
- Harden skills/specialists watchers and follow workspace lifecycle ([#439](https://github.com/intent-hq/intentd/pull/439))
- Comment.respond returns -32602 for all caller-input validation errors (intent-hq/monorepo#632) ([#445](https://github.com/intent-hq/intentd/pull/445))
- Survive load spikes on agent spawn — 30s initialize timeout + jittered retry backoff (monorepo#616)

### ⚡ Performance

- *(store)* Index-friendly event retention sweep, 24h agent:tool:call TTL, incremental vacuum

### 🧪 Testing

- Deflake script-runtime and WSS runtime-control tests under load (monorepo#515) ([#448](https://github.com/intent-hq/intentd/pull/448))

### ⚙️ Miscellaneous Tasks

- Add cargo-deny license policy (monorepo#420) ([#451](https://github.com/intent-hq/intentd/pull/451))


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

