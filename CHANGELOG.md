# Changelog

All notable changes to this project will be documented in this file.

## [0.8.0] - 2026-08-26

### 🚀 Features

- [**breaking**] Make the slim conversation projection the wire default (protocol 8.0) ([#1502](https://github.com/intent-hq/intentd/pull/1502))

### 🐛 Bug Fixes

- Evict pending-question registries on agent delete + self-contained dismiss events ([#1496](https://github.com/intent-hq/intentd/pull/1496))
- Compound/per-method rpc statement budgets for agent.sendMessage + workspace.unarchive ([#1493](https://github.com/intent-hq/intentd/pull/1493))

### 🧪 Testing

- *(intentd)* Deflake tunnel closed-port test by holding the port reservation ([#1492](https://github.com/intent-hq/intentd/pull/1492))


## [0.7.63] - 2026-08-26

### 🚀 Features

- *(services)* Reject unknown specialist ids at spawn and update seams ([#1498](https://github.com/intent-hq/intentd/pull/1498))

### ⚡ Performance

- *(store)* Keep message content out of the search.messages ranking phase ([#1494](https://github.com/intent-hq/intentd/pull/1494))
- *(store)* Batch agent queue write-through into chunked bulk inserts ([#1499](https://github.com/intent-hq/intentd/pull/1499))

### 🧪 Testing

- *(intent-git)* Poison hermetic config reads so CI exercises host-gitconfig isolation ([#1495](https://github.com/intent-hq/intentd/pull/1495))


## [0.7.62] - 2026-08-26

### 🚀 Features

- Add scoped sibling workspace proposals ([#1453](https://github.com/intent-hq/intentd/pull/1453))
- Harden agent coordination and runtime diagnostics ([#1484](https://github.com/intent-hq/intentd/pull/1484))


## [0.7.61] - 2026-08-26

### 🚀 Features

- *(specialists)* Alias support in the specialist schema (coordinator → spec-writer) ([#1488](https://github.com/intent-hq/intentd/pull/1488))

### 🐛 Bug Fixes

- *(services)* Schedule lastActivity only at turn boundaries ([#1489](https://github.com/intent-hq/intentd/pull/1489))


## [0.7.60] - 2026-08-26

### 🚀 Features

- *(providers)* Gate cortex and droid behind enable env vars ([#1482](https://github.com/intent-hq/intentd/pull/1482))

### 🐛 Bug Fixes

- *(services)* Suppress mid-turn stall advisory while a tool call is in flight ([#1481](https://github.com/intent-hq/intentd/pull/1481))

### 🧪 Testing

- *(intentd)* Deflake tunnel oversize 1009 close under parallel suite load ([#1486](https://github.com/intent-hq/intentd/pull/1486))


## [0.7.59] - 2026-08-26

### 🚀 Features

- Allow MCP tool calls from hooks via ws.mcp.* bindings ([#1483](https://github.com/intent-hq/intentd/pull/1483))
- *(providers)* Kills_child_on_interrupt quirk tears down leaking providers after cancel ([#1479](https://github.com/intent-hq/intentd/pull/1479))

### 🐛 Bug Fixes

- Return workspace-not-found from note.list instead of raw FK error on spec reseed ([#1473](https://github.com/intent-hq/intentd/pull/1473))


## [0.7.58] - 2026-08-25

### 🚀 Features

- *(specialists)* Role/teamAgents/icon picker-metadata frontmatter keys ([#1477](https://github.com/intent-hq/intentd/pull/1477))
- *(acp)* Add ws.agent.spawnPeer with top-level gating and target-side sender watch ([#1468](https://github.com/intent-hq/intentd/pull/1468))

### 🐛 Bug Fixes

- *(agents)* Fan attention wakes out to all active completion watches ([#1478](https://github.com/intent-hq/intentd/pull/1478))
- *(services)* Set parent_agent_id on wakeOrCreate-created agents ([#1476](https://github.com/intent-hq/intentd/pull/1476))


## [0.7.57] - 2026-08-25

### 🚀 Features

- *(acp)* Ws.chat.unread() unread digest behind opt-in unreadSummaries toggle ([#1469](https://github.com/intent-hq/intentd/pull/1469))
- Peer agent collaboration - agentFeatures settings, soft agent retire ([#1451](https://github.com/intent-hq/intentd/pull/1451))

### 🧪 Testing

- *(intentd)* Deflake sub-threshold dequeue-annotation WSS e2e via measured wait ([#1472](https://github.com/intent-hq/intentd/pull/1472))

### ⚙️ Miscellaneous Tasks

- Revert ws.chat.unread() unread digest ([#1469](https://github.com/intent-hq/intentd/pull/1469)) ([#1475](https://github.com/intent-hq/intentd/pull/1475))


## [0.7.56] - 2026-08-25

### 🚀 Features

- *(agents)* Reject explicit watches on an agent already waiting on the caller ([#1456](https://github.com/intent-hq/intentd/pull/1456))
- *(harness)* Mint harness v1.1 with rewritten feature sections ([#1464](https://github.com/intent-hq/intentd/pull/1464))
- *(acp)* Condensed ws.* docs rendering for the system prompt ([#1465](https://github.com/intent-hq/intentd/pull/1465))
- Emit stalled/resumed agent:stream:status events mid-turn ([#1462](https://github.com/intent-hq/intentd/pull/1462))
- Remove Ralph specialist and ralph-loop agent type ([#1458](https://github.com/intent-hq/intentd/pull/1458))
- Refresh expired MCP OAuth tokens at header build time ([#1463](https://github.com/intent-hq/intentd/pull/1463))
- *(settings)* TokenImpact annotations on agentFeatures definitions ([#1467](https://github.com/intent-hq/intentd/pull/1467))
- *(transport)* Expose prettyHostname on host.status, server.pairingInfo, and system.status ([#1466](https://github.com/intent-hq/intentd/pull/1466))

### 🐛 Bug Fixes

- Restore first-turn self-naming for user-created agents ([#1454](https://github.com/intent-hq/intentd/pull/1454))
- *(services)* Exclude idle children from runningSubAgents snapshot count ([#1457](https://github.com/intent-hq/intentd/pull/1457))
- *(events)* Cap event.query response size below the 1 MiB frame warn threshold

### 🧪 Testing

- *(intentd)* Deflake e2e_wss_auto_unarchive by keeping the autoUnarchive frame ([#1460](https://github.com/intent-hq/intentd/pull/1460))


## [0.7.55] - 2026-08-24

### 🚀 Features

- *(services)* Derive workspace unread from per-agent seen markers ([#1450](https://github.com/intent-hq/intentd/pull/1450))


## [0.7.54] - 2026-08-24

### 🚀 Features

- Add privacy-safe stream lifecycle telemetry ([#1352](https://github.com/intent-hq/intentd/pull/1352))


## [0.7.53] - 2026-08-24

### 📚 Documentation

- Teach agents inline workspace image embedding in chat ([#1448](https://github.com/intent-hq/intentd/pull/1448))


## [0.7.52] - 2026-08-24

### 🚀 Features

- *(acp)* Self-describing send outcomes for naive senders ([#1439](https://github.com/intent-hq/intentd/pull/1439))
- *(acp)* Atomic replacePending option on ws.agent.send / sendToTask
- *(agents)* Accept image-reference blocks ({type:'image', attachmentId}) in sendMessage/create ([#1446](https://github.com/intent-hq/intentd/pull/1446))

### 🐛 Bug Fixes

- *(services)* Surface zero-started batch delegate with summary, warning, and after_all advisory wake ([#1442](https://github.com/intent-hq/intentd/pull/1442))

### 📚 Documentation

- *(acp)* Clarify A2A send outcome shapes and single-pending-message rule ([#1440](https://github.com/intent-hq/intentd/pull/1440))

### ⚡ Performance

- *(git)* O(diff) hard reset in CoW checkout provisioning ([#1447](https://github.com/intent-hq/intentd/pull/1447))

### 🧪 Testing

- *(intent-git)* Isolate auth config-read tests from host gitconfig ([#1443](https://github.com/intent-hq/intentd/pull/1443))


## [0.7.51] - 2026-08-24

### 🐛 Bug Fixes

- *(sourcecontrol)* Robust gh discovery + enriched no-token error ([#1438](https://github.com/intent-hq/intentd/pull/1438))


## [0.7.50] - 2026-08-24

### 🚀 Features

- Specialist replacement directory at startup ([#1435](https://github.com/intent-hq/intentd/pull/1435))


## [0.7.49] - 2026-08-24

### 🚀 Features

- Multi-select endpoint picker for intentd pair + --select-endpoints ([#1433](https://github.com/intent-hq/intentd/pull/1433))

### 🐛 Bug Fixes

- *(voice)* Sanitize keyterms sent to ElevenLabs ([#1434](https://github.com/intent-hq/intentd/pull/1434))


## [0.7.48] - 2026-08-24

### 🚀 Features

- Support a list of IPs for server.bindAddress ([#1431](https://github.com/intent-hq/intentd/pull/1431))


## [0.7.47] - 2026-08-23

### 🚀 Features

- *(providers)* Flag grok and droid as truncating tool descriptions ([#1429](https://github.com/intent-hq/intentd/pull/1429))
- Always run startup resume sweep on update-triggered restarts ([#1428](https://github.com/intent-hq/intentd/pull/1428))


## [0.7.46] - 2026-08-23

### 🐛 Bug Fixes

- Lint macOS-gated code in CI and fix pedantic clippy warnings ([#1425](https://github.com/intent-hq/intentd/pull/1425))


## [0.7.45] - 2026-08-23

### 🐛 Bug Fixes

- *(services)* Always drop the ACP default pseudo-row when real rows exist ([#1419](https://github.com/intent-hq/intentd/pull/1419))


## [0.7.44] - 2026-08-23

### 🚀 Features

- *(acp)* Replace claude-code preset system prompt with assembled prompt ([#1421](https://github.com/intent-hq/intentd/pull/1421))

### ⚡ Performance

- *(services)* Cap agent.list row previews to a render-sized budget ([#1422](https://github.com/intent-hq/intentd/pull/1422))


## [0.7.43] - 2026-08-23

### 🐛 Bug Fixes

- Treat empty harness-wake responses as failed recovery, not completion ([#1418](https://github.com/intent-hq/intentd/pull/1418))


## [0.7.42] - 2026-08-23

### 🚀 Features

- *(acp)* Serve compact workspace_api description to truncating providers ([#1415](https://github.com/intent-hq/intentd/pull/1415))
- *(acp)* Send excludeDynamicSections in claude-code systemPrompt meta ([#1413](https://github.com/intent-hq/intentd/pull/1413))

### 🐛 Bug Fixes

- Close the LRU/cap eviction TOCTOU with claim-before-kill ([#1416](https://github.com/intent-hq/intentd/pull/1416))


## [0.7.41] - 2026-08-23

### 🐛 Bug Fixes

- Rate-limit backoff for pr refresh + git root sweeps ([#1412](https://github.com/intent-hq/intentd/pull/1412))


## [0.7.40] - 2026-08-22

### 🐛 Bug Fixes

- Default host.exec cwd to the workspace root and surface in-hook exec failures


## [0.7.39] - 2026-08-22

### 🚀 Features

- Add mcp.testConnection connection/auth probe RPC (protocol 7.3) ([#1408](https://github.com/intent-hq/intentd/pull/1408))
- *(rpc)* Add agent.listUserMessages returning a bounded user-message index ([#1407](https://github.com/intent-hq/intentd/pull/1407))


## [0.7.38] - 2026-08-22

### 🚀 Features

- *(settings)* Flip agentFeatures.taskGraph default to on ([#1405](https://github.com/intent-hq/intentd/pull/1405))


## [0.7.37] - 2026-08-22

### 🚀 Features

- *(agents)* Fail fast on disabled or unavailable providers at create/delegate ([#1403](https://github.com/intent-hq/intentd/pull/1403))

### 🐛 Bug Fixes

- *(services)* Re-resolve model.default when providers.active switches ([#1401](https://github.com/intent-hq/intentd/pull/1401))


## [0.7.36] - 2026-08-22

### 🐛 Bug Fixes

- *(providers)* Apply codex model via session/set_config_option ([#1395](https://github.com/intent-hq/intentd/pull/1395))
- Make pending question markers authoritative ([#1350](https://github.com/intent-hq/intentd/pull/1350))
- *(services)* Derive pr_ready only when the PR is truly mergeable ([#1402](https://github.com/intent-hq/intentd/pull/1402))


## [0.7.35] - 2026-08-22

### 🚀 Features

- Send task-derived agent name as session/new _meta.sessionTitle for codex ([#1392](https://github.com/intent-hq/intentd/pull/1392))
- Preserve Auggie legacy model metadata ([#1333](https://github.com/intent-hq/intentd/pull/1333))

### 🐛 Bug Fixes

- *(providers)* Bump codex-acp npx pin to 1.6.2 ([#1394](https://github.com/intent-hq/intentd/pull/1394))
- Harden stale-path staging and status refresh ([#1334](https://github.com/intent-hq/intentd/pull/1334))
- Make delegate tests hermetic — no real auggie dependency ([#1393](https://github.com/intent-hq/intentd/pull/1393))


## [0.7.34] - 2026-08-21

### 🐛 Bug Fixes

- *(services)* Re-fetch stale non-Merged PR pool entries during the git-root sweep ([#1390](https://github.com/intent-hq/intentd/pull/1390))


## [0.7.33] - 2026-08-21

### 🐛 Bug Fixes

- *(services)* Terminal PR verdicts override stale entries in the list PR pool ([#1387](https://github.com/intent-hq/intentd/pull/1387))


## [0.7.32] - 2026-08-21

### 📚 Documentation

- ShowTab reveals by activating in a visible panel without stealing focus ([#1384](https://github.com/intent-hq/intentd/pull/1384))


## [0.7.31] - 2026-08-21

### 🔧 Refactor

- Complete the pedantic clippy burndown, empty the temporary allowlist ([#1382](https://github.com/intent-hq/intentd/pull/1382))


## [0.7.30] - 2026-08-21

### 🔧 Refactor

- Burn down pedantic must_use, cast, and signature-adjacent lints ([#1379](https://github.com/intent-hq/intentd/pull/1379))

### ⚡ Performance

- Keep workspace.list/get enrichment off note bodies and duplicate session reads ([#1378](https://github.com/intent-hq/intentd/pull/1378))


## [0.7.29] - 2026-08-21

### 🚀 Features

- First-class provider param on agent.delegate ([#1375](https://github.com/intent-hq/intentd/pull/1375))

### 🐛 Bug Fixes

- *(git)* Order intentd's credential helper ahead of OS-default ones ([#1364](https://github.com/intent-hq/intentd/pull/1364))
- Add workspace.delete to the compound-op statement tier ([#3074](https://github.com/intent-hq/intentd/pull/3074))
- Self-heal default provider settings and remove positional auggie fallback ([#1373](https://github.com/intent-hq/intentd/pull/1373))
- Surface a client-visible terminal state for wedged turns ([#1374](https://github.com/intent-hq/intentd/pull/1374))

### 📚 Documentation

- Burn down pedantic missing_errors_doc and missing_panics_doc ([#1371](https://github.com/intent-hq/intentd/pull/1371))
- Burn down pedantic doc_markdown ([#1367](https://github.com/intent-hq/intentd/pull/1367))


## [0.7.28] - 2026-08-21

### 🔧 Refactor

- Burn down pedantic style and Option/format lints ([#1360](https://github.com/intent-hq/intentd/pull/1360))

### 🧪 Testing

- De-flake hook rehydration countdown tests racing the run loop


## [0.7.27] - 2026-08-21

### 🐛 Bug Fixes

- Make dev-env intent-services tests hermetic and macOS-safe ([#1355](https://github.com/intent-hq/intentd/pull/1355))
- *(acp)* Truncate oversized workspace_api output inline when file redirect fails ([#1354](https://github.com/intent-hq/intentd/pull/1354))
- Preserve structured browser.exec action-result errors on the agent surface ([#1363](https://github.com/intent-hq/intentd/pull/1363))
- Batch agent.getSubscriptions status reads into one IN-list query ([#1357](https://github.com/intent-hq/intentd/pull/1357))
- Emit agent:process:evicted with reason idle-ttl from the TTL idle sweep ([#1356](https://github.com/intent-hq/intentd/pull/1356))
- Stop serving tokenUsage on workspace.list rows and agentSummary on archived rows ([#1359](https://github.com/intent-hq/intentd/pull/1359))

### 📚 Documentation

- Document hidden-by-default tabs, showTab action, and listTabs visibility ([#1358](https://github.com/intent-hq/intentd/pull/1358))
- Workspace-inactive semantics for showTab/focusTab/openTab ([#1362](https://github.com/intent-hq/intentd/pull/1362))


## [0.7.26] - 2026-08-20

### ⚙️ Miscellaneous Tasks

- Add clippy pedantic ratchet scaffold ([#1351](https://github.com/intent-hq/intentd/pull/1351))


## [0.7.25] - 2026-08-20

### 🐛 Bug Fixes

- Retry transient provider fetch failures instead of failing the turn terminally ([#1347](https://github.com/intent-hq/intentd/pull/1347))


## [0.7.24] - 2026-08-20

### 🚀 Features

- Stamp agent-flipped completions as wake triggers ([#1340](https://github.com/intent-hq/intentd/pull/1340))

### 🐛 Bug Fixes

- *(providers)* Version-gate auggie spawn and honor auggie-path marker ([#1299](https://github.com/intent-hq/intentd/pull/1299))
- Reject agent.watch on idle target with no waiting reasons ([#1341](https://github.com/intent-hq/intentd/pull/1341))
- Resolve rpc_profile budget warnings for transfer.plan, create, and list ([#1344](https://github.com/intent-hq/intentd/pull/1344))


## [0.7.23] - 2026-08-20

### 🚀 Features

- *(services)* Merge git-root and monitor PRs into pullRequests on list emit paths ([#1330](https://github.com/intent-hq/intentd/pull/1330))
- Fold agent-monitored PRs into the displayStatus derivation ([#1329](https://github.com/intent-hq/intentd/pull/1329))
- *(services)* Stamp shared queueInfo.batchId on batch-flushed rows ([#1335](https://github.com/intent-hq/intentd/pull/1335))

### 🐛 Bug Fixes

- Preserve hook nextRunAt across daemon restarts ([#1327](https://github.com/intent-hq/intentd/pull/1327))
- Suppress same-settlement duplicate report delivery after reportToParent wake ([#1326](https://github.com/intent-hq/intentd/pull/1326))
- Default WSS bind to 127.0.0.1; bind-address picker in pair flow ([#1325](https://github.com/intent-hq/intentd/pull/1325))
- Omit metadata.initialMessage from agent.list rows (monorepo#2932) ([#1337](https://github.com/intent-hq/intentd/pull/1337))

### 📚 Documentation

- Teach ws.browser docs + help text the tab ownership ops ([#1319](https://github.com/intent-hq/intentd/pull/1319))

### ⚙️ Miscellaneous Tasks

- Tighten unused-externally pub items to pub(crate); delete dead code ([#1331](https://github.com/intent-hq/intentd/pull/1331))
- Remove unused crate dependencies and add cargo-shear CI gate ([#1323](https://github.com/intent-hq/intentd/pull/1323))
- Audit dead_code allows; delete dead code, justify keepers ([#1324](https://github.com/intent-hq/intentd/pull/1324))


## [0.7.22] - 2026-08-19

### 🐛 Bug Fixes

- Persist agent:failed wake dedup so restarts and re-arms never re-deliver historical failures ([#1316](https://github.com/intent-hq/intentd/pull/1316))
- Enrich agent:failed and agent:deleted wake events with agentName ([#1318](https://github.com/intent-hq/intentd/pull/1318))
- Auto-redrive suspected-truncated turns on delegated in-task agents ([#1317](https://github.com/intent-hq/intentd/pull/1317))

### ⚙️ Miscellaneous Tasks

- Bump sha2 to 0.11 ([#1321](https://github.com/intent-hq/intentd/pull/1321))


## [0.7.21] - 2026-08-19

### 🚀 Features

- Byte-budget slim conversation pages at ~512KB ([#1314](https://github.com/intent-hq/intentd/pull/1314))

### 🐛 Bug Fixes

- Dedup completion wakes per (watcher, completion) so restarts and re-arms never re-deliver ([#1313](https://github.com/intent-hq/intentd/pull/1313))


## [0.7.20] - 2026-08-18

### 📚 Documentation

- Teach agents to reuse existing browser tabs ([#1310](https://github.com/intent-hq/intentd/pull/1310))


## [0.7.19] - 2026-08-18

### 🚀 Features

- Agent.getMessageBlock — one full content block on demand (protocol v7.2) ([#1306](https://github.com/intent-hq/intentd/pull/1306))
- LastToolUse preview column + agent:last-message event ([#1307](https://github.com/intent-hq/intentd/pull/1307))


## [0.7.18] - 2026-08-18

### 🚀 Features

- Slim tool/image projection for conversation reads ([#1304](https://github.com/intent-hq/intentd/pull/1304))


## [0.7.17] - 2026-08-18

### 🐛 Bug Fixes

- Repo-cache create progress visibility, re-clone signal, freshness TTL, env-overridable timeouts ([#1301](https://github.com/intent-hq/intentd/pull/1301))


## [0.7.16] - 2026-08-17

### 🚀 Features

- Add aroundIndex ordinal seek to agent.getConversation ([#1297](https://github.com/intent-hq/intentd/pull/1297))

### 🐛 Bug Fixes

- *(services)* Fail closed on appends to a vanished agent session (intent-hq/monorepo#2762)


## [0.7.15] - 2026-08-17

### 🐛 Bug Fixes

- *(services)* Exclude soft-deleted sessions from agentSummary ([#1295](https://github.com/intent-hq/intentd/pull/1295))


## [0.7.14] - 2026-08-17

### 🚀 Features

- *(acp)* Instrument workspace_api dispatch stages with tracing ([#1287](https://github.com/intent-hq/intentd/pull/1287))
- *(services)* Silent-tail turn annotation + diagnostics exposure ([#1286](https://github.com/intent-hq/intentd/pull/1286))
- *(transport)* Opt-in incremental chat delta encoding (deltaEncoding param) ([#1289](https://github.com/intent-hq/intentd/pull/1289))
- *(services)* Raise hook TTL cap from 60 minutes to 24 hours ([#1290](https://github.com/intent-hq/intentd/pull/1290))
- *(agent)* Include outage duration in restart-resume continuation ([#1291](https://github.com/intent-hq/intentd/pull/1291))
- *(acp)* Default ws.agent.send / ws.agent.sendToTask to interrupt priority ([#1292](https://github.com/intent-hq/intentd/pull/1292))
- *(services)* Park automatic wakes in archived workspaces ([#1293](https://github.com/intent-hq/intentd/pull/1293))

### 🐛 Bug Fixes

- Bounded retry for transient event-batch insert failures in the event bus ([#1284](https://github.com/intent-hq/intentd/pull/1284))
- *(services)* Re-check drain after deliver_wake_message archived park ([#1294](https://github.com/intent-hq/intentd/pull/1294))
- *(acp)* Bridge dispatch watchdog guarantees a timeout response ([#1285](https://github.com/intent-hq/intentd/pull/1285))


## [0.7.13] - 2026-08-17

### 🚀 Features

- *(services)* Resolve and hide the "default" pseudo-row in ACP model catalogs ([#1278](https://github.com/intent-hq/intentd/pull/1278))
- *(services)* Pin the cached catalog default model at agent create time ([#1279](https://github.com/intent-hq/intentd/pull/1279))
- Freeze resolved specialist injection at agent creation ([#1280](https://github.com/intent-hq/intentd/pull/1280))
- *(transport)* Chat forwarder self-heals after upstream event loss ([#1281](https://github.com/intent-hq/intentd/pull/1281))

### 📚 Documentation

- Point protocol references at docs/protocol/ directory ([#1283](https://github.com/intent-hq/intentd/pull/1283))


## [0.7.12] - 2026-08-17

### 🚀 Features

- *(services)* Expose conversationBytes and large-conversation stuck-risk in agent.diagnostics ([#1276](https://github.com/intent-hq/intentd/pull/1276))

### 🐛 Bug Fixes

- *(transport)* Never silently skip the chat terminal frame on reconcile failure ([#1277](https://github.com/intent-hq/intentd/pull/1277))
- *(services)* Gate stateSnapshot injection on the session's captured feature snapshot ([#1273](https://github.com/intent-hq/intentd/pull/1273))
- Add compound-op statement-budget tier for workspace.create (39-40 stmts vs flat 25) ([#1275](https://github.com/intent-hq/intentd/pull/1275))


## [0.7.11] - 2026-08-16

### 🚀 Features

- *(host)* Report validated standard-directory favorites on host.listDirectory ([#1268](https://github.com/intent-hq/intentd/pull/1268))

### 🐛 Bug Fixes

- *(acp)* Register proposal turn attachments at binding call time ([#1270](https://github.com/intent-hq/intentd/pull/1270))

### 📚 Documentation

- *(acp)* Update browser_docs tunnel lifecycle for persistent forwards ([#1269](https://github.com/intent-hq/intentd/pull/1269))

### ⚙️ Miscellaneous Tasks

- *(core)* Stop seeding taskGraph into the first-boot config template ([#1271](https://github.com/intent-hq/intentd/pull/1271))


## [0.7.10] - 2026-08-16

### 🐛 Bug Fixes

- *(workspace)* Persist worktreePath for isNewRepo direct-mode creates and fall back to repositoryPath in spawn cwd ([#1265](https://github.com/intent-hq/intentd/pull/1265))


## [0.7.9] - 2026-08-16

### 🚀 Features

- *(mcp)* Probe http/sse MCP servers and report real status ([#1263](https://github.com/intent-hq/intentd/pull/1263))


## [0.7.8] - 2026-08-16

### 🚀 Features

- *(status)* Report workspaces-root disk space in system.status ([#1261](https://github.com/intent-hq/intentd/pull/1261))


## [0.7.7] - 2026-08-16

### 🚀 Features

- *(services)* Versioned doctrine bundles + per-session harness resolution (H2) ([#1259](https://github.com/intent-hq/intentd/pull/1259))


## [0.7.6] - 2026-08-16

### 🧪 Testing

- *(services)* Deflake lastActivity debounce persist waits with bounded polls ([#1256](https://github.com/intent-hq/intentd/pull/1256))


## [0.7.5] - 2026-08-16

### 🚀 Features

- *(agent)* Stamp harness version + captured agentFeatures on sessions ([#1255](https://github.com/intent-hq/intentd/pull/1255))

### 🔧 Refactor

- *(services)* Introduce versioned prompt harness module (H5) ([#1254](https://github.com/intent-hq/intentd/pull/1254))
- Migrate wakes, queue notes, and notices behind the harness trait ([#1258](https://github.com/intent-hq/intentd/pull/1258))

### 🧪 Testing

- Tolerate memory-sampler first-sample race in system.status e2e ([#1253](https://github.com/intent-hq/intentd/pull/1253))


## [0.7.4] - 2026-08-16

### 🐛 Bug Fixes

- *(agent)* Disclose reportToParent watch disarm and repair watch re-arm on interim-idle children ([#1250](https://github.com/intent-hq/intentd/pull/1250))
- *(agent)* Replay interrupted-turn tail on session/load resume ([#1249](https://github.com/intent-hq/intentd/pull/1249))

### 🧪 Testing

- Pin v1 golden fixtures for system-message surfaces ([#1251](https://github.com/intent-hq/intentd/pull/1251))


## [0.7.3] - 2026-08-15

### 🐛 Bug Fixes

- Gate worker end-of-turn drain on archived workspace ([#1244](https://github.com/intent-hq/intentd/pull/1244))

### 📚 Documentation

- *(browser)* Document tunnel actions and lifecycle in browser_docs ([#1247](https://github.com/intent-hq/intentd/pull/1247))


## [0.7.2] - 2026-08-15

### 🚀 Features

- Warn on JSON-RPC frames over 1 MiB ([#1241](https://github.com/intent-hq/intentd/pull/1241))
- *(cli)* Read sensitive settings from stdin or hidden prompt instead of argv ([#1243](https://github.com/intent-hq/intentd/pull/1243))

### 🐛 Bug Fixes

- Emit script definition change events ([#1170](https://github.com/intent-hq/intentd/pull/1170))
- Run startup resume sweep to completion before serving traffic ([#1246](https://github.com/intent-hq/intentd/pull/1246))


## [0.7.1] - 2026-08-15

### 🚀 Features

- Make startup auto-resume a setting with installer prompts ([#1238](https://github.com/intent-hq/intentd/pull/1238))
- *(delegate)* Annotate relation-less tasks in batch classification ([#1237](https://github.com/intent-hq/intentd/pull/1237))


## [0.7.0] - 2026-08-15

### 🚀 Features

- Add chunked binary file read method (file.readChunk) ([#1231](https://github.com/intent-hq/intentd/pull/1231))
- *(prompts)* Rework flag-ON task-graph teaching from doctrine to advisory ([#1229](https://github.com/intent-hq/intentd/pull/1229))
- *(git)* Add optional gitRootId param to git.commitDetails ([#1235](https://github.com/intent-hq/intentd/pull/1235))
- *(delegate)* [**breaking**] Per-task options in batch delegate; remove greedy param ([#1236](https://github.com/intent-hq/intentd/pull/1236))

### 🐛 Bug Fixes

- Keep session-level base64 imageBlocks out of the AgentLite projection ([#1230](https://github.com/intent-hq/intentd/pull/1230))


## [0.6.21] - 2026-08-14

### 🚀 Features

- Opt-in agentFeatures.taskGraph flag gating task-graph teaching ([#1226](https://github.com/intent-hq/intentd/pull/1226))

### 🐛 Bug Fixes

- Capture taskGraph for unblocked wakes per session


## [0.6.20] - 2026-08-14

### 🐛 Bug Fixes

- Refresh flipped specLinked flags on spec-body edits in task.subscribe ([#1224](https://github.com/intent-hq/intentd/pull/1224))


## [0.6.19] - 2026-08-14

### 🚀 Features

- Carry specLinked on task.subscribe snapshot and delta payloads ([#1218](https://github.com/intent-hq/intentd/pull/1218))
- Persist registered-commit SHA on workspace git roots ([#1220](https://github.com/intent-hq/intentd/pull/1220))

### 🐛 Bug Fixes

- Two-phase commit claim + just-in-time sweep recheck in workspace import ([#1219](https://github.com/intent-hq/intentd/pull/1219))
- Bound attachment upload sessions with a per-workspace cap and idle TTL ([#1217](https://github.com/intent-hq/intentd/pull/1217))

### 🧪 Testing

- Deflake one_shot prompt-timeout test with a private adapter bound ([#1222](https://github.com/intent-hq/intentd/pull/1222))


## [0.6.18] - 2026-08-14

### 🚀 Features

- *(agents)* Persist abnormal finishReason on the turn's assistant row ([#1211](https://github.com/intent-hq/intentd/pull/1211))
- Widen task.list to all workspace task notes, add specLinked to WorkspaceTask ([#1214](https://github.com/intent-hq/intentd/pull/1214))
- Auto-unarchive workspace on agent turn start ([#1216](https://github.com/intent-hq/intentd/pull/1216))

### 🐛 Bug Fixes

- Threshold-gate dequeue-wait annotation at 5s ([#1212](https://github.com/intent-hq/intentd/pull/1212))

### 🧪 Testing

- Regression coverage for nested repos/worktrees under untracked parent dirs in transfer WIP snapshot ([#1210](https://github.com/intent-hq/intentd/pull/1210))


## [0.6.17] - 2026-08-14

### 🚀 Features

- *(agents)* Turn-start memory budget re-check for idle-to-active transitions ([#1203](https://github.com/intent-hq/intentd/pull/1203))
- *(agents)* Stamp reason (slots | memory-budget) on agent:process:* events ([#1196](https://github.com/intent-hq/intentd/pull/1196))
- *(settings)* Auto/off semantics for agents.memoryBudgetMb ([#1195](https://github.com/intent-hq/intentd/pull/1195))
- Expose aggregate memory budget visibility on system.status ([#1198](https://github.com/intent-hq/intentd/pull/1198))
- Bucket descendant-tree RSS by nearest registered agent root ([#1197](https://github.com/intent-hq/intentd/pull/1197))
- *(agents)* Wire boot to resolve auto memory budget to recommended value
- *(services)* Expose subtreeMemoryBytes on agent.diagnostics rows ([#1201](https://github.com/intent-hq/intentd/pull/1201))
- *(agents)* Budget-triggered idle reap drains without a spawn ([#1202](https://github.com/intent-hq/intentd/pull/1202))
- *(transport)* /tunnel WS endpoint with stream mux + loopback relay ([#1205](https://github.com/intent-hq/intentd/pull/1205))
- Derive orthogonal workspace waiting flag and emit workspace:waiting-changed ([#1207](https://github.com/intent-hq/intentd/pull/1207))

### 🐛 Bug Fixes

- *(intentd)* Exclude Linux thread rows from the child-tree memory sampler ([#1209](https://github.com/intent-hq/intentd/pull/1209))

### 📚 Documentation

- *(acp)* Document daemon.localhost/client.localhost convention in browser docs ([#1206](https://github.com/intent-hq/intentd/pull/1206))
- *(acp)* Note automatic tunnel fallback for unreachable remote loopback URLs ([#1208](https://github.com/intent-hq/intentd/pull/1208))


## [0.6.16] - 2026-08-13

### 🐛 Bug Fixes

- Persist error status before terminal events on the streaming path ([#1191](https://github.com/intent-hq/intentd/pull/1191))
- Collapse Codex effort variants ([#1173](https://github.com/intent-hq/intentd/pull/1173))


## [0.6.15] - 2026-08-13

### 🚀 Features

- Multi git root tracking — persisted roots, gitRoot.list, scoped git reads (protocol v6.15) ([#1180](https://github.com/intent-hq/intentd/pull/1180))
- Defer workspace watcher start until setup script completes ([#1183](https://github.com/intent-hq/intentd/pull/1183))
- Remove unread from the displayStatus derivation ([#1186](https://github.com/intent-hq/intentd/pull/1186))
- Chunked attachment upload session (file.attachmentUpload.*) ([#1187](https://github.com/intent-hq/intentd/pull/1187))

### 🐛 Bug Fixes

- Close the idle reaper TOCTOU with claim-before-kill ([#1184](https://github.com/intent-hq/intentd/pull/1184))


## [0.6.14] - 2026-08-13

### 🚀 Features

- Add machine-readable watchStillArmed flag to agent-watch wake metadata ([#1179](https://github.com/intent-hq/intentd/pull/1179))

### 🐛 Bug Fixes

- Exclude workspace-owned checkouts from the known-repo registry ([#1181](https://github.com/intent-hq/intentd/pull/1181))


## [0.6.13] - 2026-08-13

### 🔧 Refactor

- *(core)* Unify Windows runnable-extension policy in path_utils ([#1176](https://github.com/intent-hq/intentd/pull/1176))


## [0.6.12] - 2026-08-13

### 🚀 Features

- Surface conversion createdTasks + warnings on note.create ([#1162](https://github.com/intent-hq/intentd/pull/1162))
- *(settings)* Bound memoryBudgetMb by real RAM and reap idle agents at 10min ([#1166](https://github.com/intent-hq/intentd/pull/1166))

### 🐛 Bug Fixes

- *(chat)* Flush the live-turn slot as of flush time, not the pre-abort clone ([#1157](https://github.com/intent-hq/intentd/pull/1157))
- Skip nested git repos in sandbox staging (add_all) ([#1168](https://github.com/intent-hq/intentd/pull/1168))
- Prefer newest nvm Node during discovery ([#1169](https://github.com/intent-hq/intentd/pull/1169))
- *(intent-context)* Require runnable extension in auggie discovery on Windows ([#1171](https://github.com/intent-hq/intentd/pull/1171))
- Skip nested git repos/worktrees when snapshotting WIP for transfer ([#1159](https://github.com/intent-hq/intentd/pull/1159))
- *(system.status)* Catch sub-5s bursts in childMemoryPeakBytes ([#1167](https://github.com/intent-hq/intentd/pull/1167))
- *(chat)* Serve orphaned live-turn content instead of hiding it ([#1161](https://github.com/intent-hq/intentd/pull/1161))
- Classify placeAttachment copy failures and log them at WARN ([#1165](https://github.com/intent-hq/intentd/pull/1165))


## [0.6.11] - 2026-08-12

### 🐛 Bug Fixes

- Patch crates-io tokio-tungstenite to the fork for cargo package verification ([#1158](https://github.com/intent-hq/intentd/pull/1158))
- Report criticalPathMinutes for mixed-estimate graphs ([#1160](https://github.com/intent-hq/intentd/pull/1160))

## [0.6.10] - 2026-08-12

### 🐛 Bug Fixes

- Drop fork-only deflate feature request so cargo package resolves against crates.io ([#1155](https://github.com/intent-hq/intentd/pull/1155))
- *(chat)* Heal a snapshot-missed turn at stream:end ([#1154](https://github.com/intent-hq/intentd/pull/1154))

### 📚 Documentation

- *(config)* Describe the agent memory knobs in the config template ([#1152](https://github.com/intent-hq/intentd/pull/1152))

## [0.6.9] - 2026-08-12

### 🐛 Bug Fixes

- *(chat)* Keep the live-turn slot published across the interrupt abort→flush gap ([#1150](https://github.com/intent-hq/intentd/pull/1150))
- Add version requirement to tokio-tungstenite git dep for release-plz packaging ([#1151](https://github.com/intent-hq/intentd/pull/1151))

## [0.6.8] - 2026-08-12

### 🚀 Features

- Git snapshot + transfer bundle builder ([#1097](https://github.com/intent-hq/intentd/pull/1097))
- Target git materialization from transfer bundle ([#1102](https://github.com/intent-hq/intentd/pull/1102))
- Parse optional header attributes on @@@task fence lines ([#1128](https://github.com/intent-hq/intentd/pull/1128))
- *(services)* Pure ready-set delta helper for delivery-time unblocked computation ([#1138](https://github.com/intent-hq/intentd/pull/1138))
- Optional prefix filter on github.branches.list via matching-refs ([#1081](https://github.com/intent-hq/intentd/pull/1081))
- *(core)* Surface isInitialAgent on the AgentLite metadata projection ([#1085](https://github.com/intent-hq/intentd/pull/1085))
- File.placeAttachment — daemon-mediated attachment placement (.intent/attachments/) ([#1090](https://github.com/intent-hq/intentd/pull/1090))
- Flag stale undelivered queue entries as stuck risks in agent.diagnostics ([#1084](https://github.com/intent-hq/intentd/pull/1084))
- Transfer manifest + workspace.transfer.plan RPC ([#1092](https://github.com/intent-hq/intentd/pull/1092))
- Gitlink-aware git.status/git.diffs payload + typed git.showFile not-a-file error ([#1095](https://github.com/intent-hq/intentd/pull/1095))
- Daemon-owned delete-undo grace window for workspaces and agents ([#1096](https://github.com/intent-hq/intentd/pull/1096))
- Negotiate permessage-deflate on the WSS listener ([#1099](https://github.com/intent-hq/intentd/pull/1099))
- *(task)* First-class dependsOn/conflictsWith relations + task.setRelations RPC ([#1100](https://github.com/intent-hq/intentd/pull/1100))
- Priority lanes for RPC responses on the outbound queue ([#1094](https://github.com/intent-hq/intentd/pull/1094))
- *(tasks)* Generalize task readiness over dependsOn edges ([#1104](https://github.com/intent-hq/intentd/pull/1104))
- Staged workspace import surface (workspace.import.*) ([#1101](https://github.com/intent-hq/intentd/pull/1101))
- *(repo)* Repo.warmCache RPC — opportunistic background repo cache refresh ([#1105](https://github.com/intent-hq/intentd/pull/1105))
- *(transport)* Chat.subscribe resume via sinceMessageId ([#1091](https://github.com/intent-hq/intentd/pull/1091))
- Materialize imported git payload in workspace.import.commit ([#1107](https://github.com/intent-hq/intentd/pull/1107))
- *(transport)* Lossless egress conflation of high-volume stream events under backpressure ([#1093](https://github.com/intent-hq/intentd/pull/1093))
- *(agents)* Widen agent.delegate with tasks[] + greedy (idempotent batch start) ([#1108](https://github.com/intent-hq/intentd/pull/1108))
- Add check param to prMonitor.flush for on-demand re-poll ([#1111](https://github.com/intent-hq/intentd/pull/1111))
- *(agents)* Effort parsing + critical-path greedy-off ordering in batch delegate ([#1112](https://github.com/intent-hq/intentd/pull/1112))
- *(notes)* Project computed unmetDependsOn onto note-shaped payloads ([#1109](https://github.com/intent-hq/intentd/pull/1109))
- Expose localIps and hostname on system.status (additive) ([#1115](https://github.com/intent-hq/intentd/pull/1115))
- Recompute ready set on dependsOn writes and dep-note deletion ([#1114](https://github.com/intent-hq/intentd/pull/1114))
- Workspace export pipeline (workspace.export.*) + transfer round-trip test ([#1118](https://github.com/intent-hq/intentd/pull/1118))
- Emit ready-set recompute when deleting a task note moves the ready set ([#1121](https://github.com/intent-hq/intentd/pull/1121))
- Name the pidfile holder in the contended data-dir lock error ([#1124](https://github.com/intent-hq/intentd/pull/1124))
- Include resolved provider in agent.delegate result ([#1126](https://github.com/intent-hq/intentd/pull/1126))
- *(tasks)* Resolve keys and seed relations at @@@task conversion time ([#1130](https://github.com/intent-hq/intentd/pull/1130))
- UUID attachment registry, attachment-reference file blocks, and ws.file.getAttachment MCP tool ([#1131](https://github.com/intent-hq/intentd/pull/1131))
- Return created tasks and warnings from conversion ([#1133](https://github.com/intent-hq/intentd/pull/1133))
- Carry .intent/attachments/ files and registry rows through workspace transfer ([#1140](https://github.com/intent-hq/intentd/pull/1140))
- *(services)* Delivery-time 'tasks now unblocked' hints in completion wakes ([#1144](https://github.com/intent-hq/intentd/pull/1144))
- *(system.status)* Report the daemon's whole child-process memory footprint ([#1139](https://github.com/intent-hq/intentd/pull/1139))
- *(agents)* Prototype an aggregate child-tree memory budget ([#1145](https://github.com/intent-hq/intentd/pull/1145))
- Bound concurrently live ephemeral ACP adapters ([#1146](https://github.com/intent-hq/intentd/pull/1146))
- *(pr-monitor)* Refresh workspace PR linkage on terminal wake ([#1148](https://github.com/intent-hq/intentd/pull/1148))

### 🐛 Bug Fixes

- *(intentd)* Prefer runnable Windows extensions in host.findBinary ([#1122](https://github.com/intent-hq/intentd/pull/1122))
- Persist spawn system prompt via narrow write to avoid clobbering concurrent agent.setModel ([#1086](https://github.com/intent-hq/intentd/pull/1086))
- Never append contradictory stall tail to after_all settlement wakes ([#1087](https://github.com/intent-hq/intentd/pull/1087))
- Bound PR-monitor sweep fetches with network + per-fetch timeouts ([#1110](https://github.com/intent-hq/intentd/pull/1110))
- *(hooks)* Persist hook expiry atomically and harden sleep-expiry test ([#1119](https://github.com/intent-hq/intentd/pull/1119))
- Make synthetic-idle suppression test wait for settlement idle deterministically ([#1120](https://github.com/intent-hq/intentd/pull/1120))
- Size common-dir watch delivery-confirmation budgets to LIVENESS ([#1123](https://github.com/intent-hq/intentd/pull/1123))
- Bound workspace PR-refresh sweep fetches with a per-fetch timeout ([#1149](https://github.com/intent-hq/intentd/pull/1149))
- Single-flight the ls-remote fallback in github.branches.listCached ([#1083](https://github.com/intent-hq/intentd/pull/1083))
- Persist zero-output stop-redelivery payload across daemon restart ([#1089](https://github.com/intent-hq/intentd/pull/1089))
- Reject dependsOn edges onto tree ancestors/descendants ([#1106](https://github.com/intent-hq/intentd/pull/1106))
- Switch commit-message generation to a JSON output contract ([#1137](https://github.com/intent-hq/intentd/pull/1137))
- Persist error status before terminal failure events ([#1136](https://github.com/intent-hq/intentd/pull/1136))
- State watch retirement and re-arm instruction in agent-watch wake ([#1141](https://github.com/intent-hq/intentd/pull/1141))
- Run first-boot legacy import in background with per-workspace isolation and resume ([#1143](https://github.com/intent-hq/intentd/pull/1143))
- *(chat)* Stamp real tool_result block ids on the live delta stream ([#1142](https://github.com/intent-hq/intentd/pull/1142))

### 🔧 Refactor

- Remove pr-shepherd bundled specialist ([#1117](https://github.com/intent-hq/intentd/pull/1117))

### 📚 Documentation

- Contract-first task splitting + batch delegation guidance in agent instructions ([#1116](https://github.com/intent-hq/intentd/pull/1116))
- Document inline @@@task relations in MCP help and task-breakdown instructions ([#1135](https://github.com/intent-hq/intentd/pull/1135))
- Document delivery-time 'tasks now unblocked' completion-wake hints ([#1147](https://github.com/intent-hq/intentd/pull/1147))

### 🧪 Testing

- Schema-parity tripwire for transfer tables ([#1132](https://github.com/intent-hq/intentd/pull/1132))
- *(services)* Deterministic refresher flush in archived-worktree common-dir watch test ([#1125](https://github.com/intent-hq/intentd/pull/1125))
- *(services)* Deracify delete grace window deadline tests ([#1127](https://github.com/intent-hq/intentd/pull/1127))
- Export build failure-injection coverage ([#1129](https://github.com/intent-hq/intentd/pull/1129))
- *(core)* Serialize fake-shell capture tests to fix ETXTBSY flake ([#1113](https://github.com/intent-hq/intentd/pull/1113))


## [0.6.7] - 2026-08-11

### 🚀 Features

- Suppress unread + notification signals for archived workspaces ([#1075](https://github.com/intent-hq/intentd/pull/1075))
- *(cli)* Merge token into pair with labeled pairing output ([#1074](https://github.com/intent-hq/intentd/pull/1074))

### 🐛 Bug Fixes

- CompletionReport idle settles after_all group without retiring PR monitors/hooks ([#1079](https://github.com/intent-hq/intentd/pull/1079))

### 📚 Documentation

- Add closeTab to the browser.exec action-catalog help text ([#1077](https://github.com/intent-hq/intentd/pull/1077))

### ⚙️ Miscellaneous Tasks

- Bump whoami from 1.6.1 to 2.1.2 ([#964](https://github.com/intent-hq/intentd/pull/964))
- Bump getrandom from 0.2.17 to 0.4.2 ([#962](https://github.com/intent-hq/intentd/pull/962))
- Update Cargo.toml dependencies


## [0.6.6] - 2026-08-10

### 🚀 Features

- Real streaming progress for cache ensure + submodules ([#1069](https://github.com/intent-hq/intentd/pull/1069))
- Remove the cortex feature-code gate ([#1068](https://github.com/intent-hq/intentd/pull/1068))
- Cancel active PR monitors in the workspace.archive sweep ([#1067](https://github.com/intent-hq/intentd/pull/1067))
- Machine-readable listener-down discriminator on pairing.getInfo ([#1065](https://github.com/intent-hq/intentd/pull/1065))
- Ls-remote fallback for github.branches.listCached ([#1072](https://github.com/intent-hq/intentd/pull/1072))

### 🐛 Bug Fixes

- Non-blocking cached-branch read during in-flight clone ([#1071](https://github.com/intent-hq/intentd/pull/1071))

### 🧪 Testing

- *(intentd)* WSS e2e for create-scoped progress frames ([#1073](https://github.com/intent-hq/intentd/pull/1073))


## [0.6.5] - 2026-08-10

### 🚀 Features

- Add uncached host.checkNode and host.checkGh fast-path methods ([#1064](https://github.com/intent-hq/intentd/pull/1064))
- ProgressId plumbing + unified provisioning progress for workspace.create ([#1062](https://github.com/intent-hq/intentd/pull/1062))
- Gate ws.app.question.ask to top-level agents ([#1063](https://github.com/intent-hq/intentd/pull/1063))
- Make workspace.create setupScript param execute-only (no config write) ([#1066](https://github.com/intent-hq/intentd/pull/1066))

### 🐛 Bug Fixes

- Redeliver a zero-output stopped turn's user message on the next turn ([#1058](https://github.com/intent-hq/intentd/pull/1058))
- Drain parked automatic queue entries FIFO with user deliveries under a question hold ([#1059](https://github.com/intent-hq/intentd/pull/1059))


## [0.6.4] - 2026-08-10

### 🚀 Features

- Add debug.sampleStacks RPC for in-process daemon stack sampling ([#1057](https://github.com/intent-hq/intentd/pull/1057))

### 🐛 Bug Fixes

- Spawn CLI auth probes with the enhanced PATH; treat exit 127 as probe failure ([#1056](https://github.com/intent-hq/intentd/pull/1056))
- Exit quietly on EPIPE when a one-shot CLI's stdout pipe closes early ([#1055](https://github.com/intent-hq/intentd/pull/1055))
- Batch script.list bootstrap persists to stay within SQL statement budget ([#1054](https://github.com/intent-hq/intentd/pull/1054))

### 🧪 Testing

- *(services)* Deterministic pause barrier for skills_watcher resume catch-up test ([#1053](https://github.com/intent-hq/intentd/pull/1053))
- *(intentd)* Pure-liveness deadlines for settings live-reload e2e ([#1052](https://github.com/intent-hq/intentd/pull/1052))


## [0.6.3] - 2026-08-09

### 🚀 Features

- *(services)* Batch agent teardown in workspace.delete for fast ack ([#1038](https://github.com/intent-hq/intentd/pull/1038))
- *(git)* Submodule-aware repo cache and hydration ([#1024](https://github.com/intent-hq/intentd/pull/1024))
- *(git)* Parallelize background workspace file removal ([#1046](https://github.com/intent-hq/intentd/pull/1046))
- *(hooks)* Shorten dispatch wake state notes, add hookStillActive metadata ([#1027](https://github.com/intent-hq/intentd/pull/1027))
- *(intentd)* Make 'intentd pair' enable the WSS listener on demand ([#1034](https://github.com/intent-hq/intentd/pull/1034))
- *(services)* Include url in pr_monitor_wake metadata ([#1033](https://github.com/intent-hq/intentd/pull/1033))
- Persist and serve thoughtTokens on stats.getUsage ([#1041](https://github.com/intent-hq/intentd/pull/1041))
- Project lastMessageId onto AgentLite ([#1039](https://github.com/intent-hq/intentd/pull/1039))
- *(github)* Add github.branches.listCached read-only RPC
- Gate the pi provider on the pi CLI version (>= 0.80.4) ([#1044](https://github.com/intent-hq/intentd/pull/1044))
- Gap-fill provider and host.exec spawn env with captured login-shell credential vars ([#1047](https://github.com/intent-hq/intentd/pull/1047))
- Coalesce PR monitor wakes into net baseline diffs ([#1049](https://github.com/intent-hq/intentd/pull/1049))

### 🐛 Bug Fixes

- *(services)* Apply report_delivered filter to waiting projections ([#1017](https://github.com/intent-hq/intentd/pull/1017))
- *(store,services)* Guard last_activity against full-row clobber and persist derived value ([#1018](https://github.com/intent-hq/intentd/pull/1018))
- Recognize any recent self-write in the config-watcher guard ([#999](https://github.com/intent-hq/intentd/pull/999))
- *(services)* Watch git metadata for gitfile linked-worktree workspaces ([#1048](https://github.com/intent-hq/intentd/pull/1048))
- *(git)* Prune stale submodule modules and clean CoW orphan work trees ([#1028](https://github.com/intent-hq/intentd/pull/1028))
- Discover CLIs across nvm node versions ([#1045](https://github.com/intent-hq/intentd/pull/1045))
- *(providers)* Apply Node heap cap on npx-fallback spawns ([#1042](https://github.com/intent-hq/intentd/pull/1042))
- *(services)* Derive delegation-group subscription linkage from grouped watches in diagnostics ([#1016](https://github.com/intent-hq/intentd/pull/1016))
- *(services)* Gate turn-end unread raise on top-level foreground agents ([#1021](https://github.com/intent-hq/intentd/pull/1021))
- Drain timed-out turn's late session/update tail before the idle-timeout warning turn ([#1032](https://github.com/intent-hq/intentd/pull/1032))
- *(services)* Fold active PR monitors into workspace displayStatus ([#1036](https://github.com/intent-hq/intentd/pull/1036))
- *(services)* Suppress per-check success lines in the PR monitor diff ([#1000](https://github.com/intent-hq/intentd/pull/1000))

### ⚡ Performance

- *(services)* Dedup pr.monitor forge fetches per (repo, pr) within a sweep ([#1020](https://github.com/intent-hq/intentd/pull/1020))

### 🧪 Testing

- *(events)* Use pure-liveness deadline for positive watcher-test waits ([#1030](https://github.com/intent-hq/intentd/pull/1030))
- *(script_ops)* Use pure-liveness run timeouts for output-capture tests ([#1043](https://github.com/intent-hq/intentd/pull/1043))
- *(intentd)* Deflake workspace lifecycle watcher e2e via retry-until-observed ([#997](https://github.com/intent-hq/intentd/pull/997))


## [0.6.2] - 2026-08-09

### 🚀 Features

- *(store)* Drop preview self-heal fallback, refuse newer-schema databases ([#1001](https://github.com/intent-hq/intentd/pull/1001))
- *(agents)* Unify PR-monitor waiting into external-wait classification ([#1002](https://github.com/intent-hq/intentd/pull/1002))
- *(intent-services)* Single-flight the full accept-changes.getStatus build ([#1008](https://github.com/intent-hq/intentd/pull/1008))
- Surface waitingOnPrMonitors on the wire (mirrors waitingOnHooks) ([#1007](https://github.com/intent-hq/intentd/pull/1007))
- Resolve quickActions.* daemon-side for agent.completeOnce ([#1012](https://github.com/intent-hq/intentd/pull/1012))
- Cap outstanding slow-path RPCs with -32011 overload rejection ([#1013](https://github.com/intent-hq/intentd/pull/1013))

### 🐛 Bug Fixes

- Tier rpc_profile duration budget for network-bound methods ([#1004](https://github.com/intent-hq/intentd/pull/1004))
- Never stage submodule-internal paths in the superproject (commit/stage guards) ([#1009](https://github.com/intent-hq/intentd/pull/1009))
- Rename backgroundAgents.* to quickActions.* and scope to quick actions ([#1010](https://github.com/intent-hq/intentd/pull/1010))
- Refuse submodule-internal paths in git.discard ([#1011](https://github.com/intent-hq/intentd/pull/1011))
- Remove RpcLimiter Default impl and warn when the overload cap is disabled ([#1014](https://github.com/intent-hq/intentd/pull/1014))

### 🔧 Refactor

- *(transport)* Share envelope-validity rules between router and dispatch pre-check ([#1015](https://github.com/intent-hq/intentd/pull/1015))


## [0.6.1] - 2026-08-08

### 🚀 Features

- Ws.pr.monitor — centralized PR monitoring with merge-requirements checklist
- Provider-neutral agent.completeOnce via ephemeral ACP sessions ([#991](https://github.com/intent-hq/intentd/pull/991))
- Persist and serve session-discovered reasoning-effort levels ([#992](https://github.com/intent-hq/intentd/pull/992))
- Cache provider discovery results and prewarm login-shell PATH ([#994](https://github.com/intent-hq/intentd/pull/994))

### 🐛 Bug Fixes

- *(git)* Resolve inherited origin in CoW checkout provisioning ([#996](https://github.com/intent-hq/intentd/pull/996))
- *(test)* Retry writes in gitignore-suppression e2e under load ([#998](https://github.com/intent-hq/intentd/pull/998))

### 🧪 Testing

- *(services)* Hermetic unit coverage for the unresolvable-adapter unavailable envelope ([#993](https://github.com/intent-hq/intentd/pull/993))


## [0.6.0] - 2026-08-07

### 🚀 Features

- Spread a turn's tokens across the minutes it ran ([#969](https://github.com/intent-hq/intentd/pull/969))
- *(context)* Honor ~/.augment/auggie-path marker in auggie discovery ([#939](https://github.com/intent-hq/intentd/pull/939))
- *(acp)* Derive tool names from Claude Code mcp__<server>__<tool> titles ([#935](https://github.com/intent-hq/intentd/pull/935))
- [**breaking**] Remove inert workspace.autoFetch setting ([#924](https://github.com/intent-hq/intentd/pull/924))
- Raise hook name cap to 50 chars for human-readable names ([#929](https://github.com/intent-hq/intentd/pull/929))
- *(script)* Persist was-running marker and expose previouslyRunning ([#932](https://github.com/intent-hq/intentd/pull/932))
- Status-neutral commit policy and clearer auto-commit-disabled rejection ([#926](https://github.com/intent-hq/intentd/pull/926))
- Kill all daemon-owned PTY sessions on graceful shutdown ([#940](https://github.com/intent-hq/intentd/pull/940))
- *(usage)* Capture ACP usage_update cost in TokenUsage
- Repo cache + cache-hydrated workspace creation ([#944](https://github.com/intent-hq/intentd/pull/944))
- First-class reasoningEffort session field with generic ACP application (protocol 5.2) ([#946](https://github.com/intent-hq/intentd/pull/946))
- Name specialist default model in delegate model-option hints ([#958](https://github.com/intent-hq/intentd/pull/958))
- Emit throttled activity pings for tool-call updates ([#957](https://github.com/intent-hq/intentd/pull/957))
- *(events)* Hybrid file:* persistence; remove event.recentFiles/directoryChanges
- Extend displayStatus into the BE-owned canonical rollup ([#945](https://github.com/intent-hq/intentd/pull/945))
- *(transport)* [**breaking**] Bump protocol version to 6.0 for event method removals
- *(agents)* Persistent question hold across plain user messages ([#965](https://github.com/intent-hq/intentd/pull/965))
- *(settings)* Add model.defaultReasoningEffort setting ([#970](https://github.com/intent-hq/intentd/pull/970))
- Surface ACP agent_thought_chunk as thinking blocks and thoughtTokens usage ([#973](https://github.com/intent-hq/intentd/pull/973))
- Auto-resume agent turns after host sleep ([#972](https://github.com/intent-hq/intentd/pull/972))
- *(services)* Apply model.defaultReasoningEffort at agent creation ([#974](https://github.com/intent-hq/intentd/pull/974))
- Carry thoughtTokens in per-minute usage rate history ([#976](https://github.com/intent-hq/intentd/pull/976))
- Per-turn agent state snapshot — ws.agent.snapshot op, MCP tool, turn-prompt injection, stateSnapshot setting ([#971](https://github.com/intent-hq/intentd/pull/971))
- *(providers)* Probe auggie auth via token print, drop checkAuggie version ([#977](https://github.com/intent-hq/intentd/pull/977))
- Perpetual background hooks that re-arm after dispatch until TTL ([#979](https://github.com/intent-hq/intentd/pull/979))
- *(events)* Emit task:created on every task creation path ([#978](https://github.com/intent-hq/intentd/pull/978))
- Serve model-catalog cache indefinitely; probe only on miss or forceRefresh ([#987](https://github.com/intent-hq/intentd/pull/987))
- Optional providerId param on agent.setModel ([#986](https://github.com/intent-hq/intentd/pull/986))

### 🐛 Bug Fixes

- *(store)* Bound the token-usage fallback message read ([#954](https://github.com/intent-hq/intentd/pull/954))
- *(services)* Reword questions-dismissed notice to informative-only ([#930](https://github.com/intent-hq/intentd/pull/930))
- *(services)* Persist D13 effective model to resolved_model instead of rewriting session.model ([#941](https://github.com/intent-hq/intentd/pull/941))
- *(events)* Defer OS watch registration off the caller's thread ([#952](https://github.com/intent-hq/intentd/pull/952))
- *(events)* Stop workspace watchers on archive, restart on unarchive
- Settle agent-waiting groups when a report_delivered watch retires ([#980](https://github.com/intent-hq/intentd/pull/980))
- *(acp)* Shell-wrap terminal/create for Grok-style packed commands
- Derive pr.snapshot review decision from reviewDecision, not mergeable_state
- *(sourcecontrol)* Stop double-unwrapping the GraphQL data envelope ([#949](https://github.com/intent-hq/intentd/pull/949))
- Close workspace displayStatus audit gaps (G1-G9) ([#928](https://github.com/intent-hq/intentd/pull/928))
- *(acp)* Answer delivered MCP bridge calls with non-retryable outcome-unknown error on TCP drop ([#937](https://github.com/intent-hq/intentd/pull/937))
- State terminal retirement in hook dispatch/eviction wake messages ([#933](https://github.com/intent-hq/intentd/pull/933))
- Support subscribe-style eventType globs in event.query ([#938](https://github.com/intent-hq/intentd/pull/938))
- Persist derived lastActivity in the debounce task ([#959](https://github.com/intent-hq/intentd/pull/959))
- Duplicate standalone-checkout workspaces as standalone checkouts ([#956](https://github.com/intent-hq/intentd/pull/956))
- Scope ws.hook.cancel to the owning agent ([#953](https://github.com/intent-hq/intentd/pull/953))
- *(intentd)* Bind listeners before slow startup initializations
- Exclude the calling agent from the archive interrupt sweep ([#950](https://github.com/intent-hq/intentd/pull/950))
- Run the archive post-persist tail detached so a hook-initiated archive still emits ([#968](https://github.com/intent-hq/intentd/pull/968))

### 🔧 Refactor

- *(services)* Extract central workspace_status module ([#925](https://github.com/intent-hq/intentd/pull/925))

### ⚡ Performance

- *(events)* Consolidate FSEvents streams into shared watchers with in-process demux
- Coalesce concurrent git status scans per worktree ([#982](https://github.com/intent-hq/intentd/pull/982))
- Event-invalidated per-worktree git.status cache

### 🧪 Testing

- *(services)* Make models.list legacy-path tests hermetic via fetch seam ([#934](https://github.com/intent-hq/intentd/pull/934))
- Split agent.watch lifecycle e2e and clamp budgets under nextest kill ([#947](https://github.com/intent-hq/intentd/pull/947))

### ⚙️ Miscellaneous Tasks

- Bump pinned ACP adapters (claude-agent-acp 0.66.0, codex-acp 1.1.14, pi-acp 0.0.33) ([#983](https://github.com/intent-hq/intentd/pull/983))


## [0.5.0] - 2026-08-06

### 🚀 Features

- Sync device-flow token into gh CLI on authorize ([#914](https://github.com/intent-hq/intentd/pull/914))
- *(acp)* Workspace_api discoverability — capability index, ws.help(), hook guidance, pr.snapshot repo override ([#911](https://github.com/intent-hq/intentd/pull/911))
- Clarify ws.app.question.ask hints and stabilize timing tests ([#917](https://github.com/intent-hq/intentd/pull/917))
- *(agents)* Add flushQueuedMessages mode setting (all/systemOnly/off)
- Interrupt agents and cancel hooks on workspace archive; eager hook-task abort on delete ([#896](https://github.com/intent-hq/intentd/pull/896))
- *(voice)* Structured voice-no-api-key error data on voice.transcribe ([#902](https://github.com/intent-hq/intentd/pull/902))
- ModelOptions frontmatter list on specialist definitions ([#900](https://github.com/intent-hq/intentd/pull/900))
- *(voice)* Add voice.language setting as transcription language fallback ([#901](https://github.com/intent-hq/intentd/pull/901))
- Surface specialist modelOptions in the workspace_api delegate docs ([#908](https://github.com/intent-hq/intentd/pull/908))
- *(agent)* Agent.markSeen per-conversation seen marker (protocol 4.5)
- *(events)* Suppress gitignored paths in the file watcher ([#903](https://github.com/intent-hq/intentd/pull/903))
- *(search)* Rank archived-workspace matches below active in search.messages ([#906](https://github.com/intent-hq/intentd/pull/906))
- Log gh CLI out of github.com on github.revoke when its token matches ([#915](https://github.com/intent-hq/intentd/pull/915))
- Shrink ws.pr.* to snapshot and ws.git.* to commit; point agents at gh/git CLIs ([#918](https://github.com/intent-hq/intentd/pull/918))
- Persist interruption reason on interrupted rows and stream:end ([#919](https://github.com/intent-hq/intentd/pull/919))
- [**breaking**] Remove 11 caller-less pr.* router methods; bump protocol to 5.0
- [**breaking**] Remove model tiers and default-provider designation ([#922](https://github.com/intent-hq/intentd/pull/922))
- Workspace-derived vocabulary for voice dictation ([#920](https://github.com/intent-hq/intentd/pull/920))

### 🐛 Bug Fixes

- *(services)* Publish subscriptions-changed on resume watch re-registration ([#904](https://github.com/intent-hq/intentd/pull/904))
- Stop bumping updated_at in markSeen/dismissAttention ([#905](https://github.com/intent-hq/intentd/pull/905))
- Seal after_all groups on queue-idle regardless of hooks ([#909](https://github.com/intent-hq/intentd/pull/909))
- Scope attention writes to the attention column ([#912](https://github.com/intent-hq/intentd/pull/912))
- Load nvm in interactive terminals ([#898](https://github.com/intent-hq/intentd/pull/898))
- Defer completion-watch settlement for idle children waiting on other agents ([#913](https://github.com/intent-hq/intentd/pull/913))
- Resolve agent.delegate provider from configured default, not Auggie ([#910](https://github.com/intent-hq/intentd/pull/910))

### 🧪 Testing

- Make opencode bridge-server test host-independent ([#899](https://github.com/intent-hq/intentd/pull/899))

### ⚙️ Miscellaneous Tasks

- Bump rand from 0.9.5 to 0.10.2 ([#771](https://github.com/intent-hq/intentd/pull/771))
- Bump rquickjs from 0.9.0 to 0.12.2 ([#770](https://github.com/intent-hq/intentd/pull/770))
- Bump tokio-tungstenite from 0.23.1 to 0.30.0 ([#769](https://github.com/intent-hq/intentd/pull/769))


## [0.4.2] - 2026-08-04

### 🚀 Features

- Promote displayStatus to in_progress for top-level agents with child completion watches ([#891](https://github.com/intent-hq/intentd/pull/891))
- Add ws.pr.snapshot for hook-based PR monitoring ([#887](https://github.com/intent-hq/intentd/pull/887))
- *(services)* Bridge file:* events to debounced changes:git-status ([#882](https://github.com/intent-hq/intentd/pull/882))
- *(intentd)* Warn on RPC dispatches exceeding statement or duration budgets ([#884](https://github.com/intent-hq/intentd/pull/884))
- On-demand workspace.diskUsage method; drop diskUsage from list/get ([#886](https://github.com/intent-hq/intentd/pull/886))
- Retire modelTier from specialist frontmatter and resolution ([#889](https://github.com/intent-hq/intentd/pull/889))
- Agent feature toggles in config.toml ([agentFeatures]) ([#890](https://github.com/intent-hq/intentd/pull/890))
- Voice.transcribe RPC with pluggable speech-to-text providers ([#893](https://github.com/intent-hq/intentd/pull/893))
- Notify model on agent.dismissQuestions ([#892](https://github.com/intent-hq/intentd/pull/892))

### ⚡ Performance

- Thin agent.list, add listActive, cap diskUsage walks ([#881](https://github.com/intent-hq/intentd/pull/881))


## [0.4.1] - 2026-08-03

### 🚀 Features

- Hint renderable chat blocks in agent instructions ([#874](https://github.com/intent-hq/intentd/pull/874))
- Hint nav-link blocks in agent instructions ([#877](https://github.com/intent-hq/intentd/pull/877))
- Flush queued messages into one combined turn on idle ([#876](https://github.com/intent-hq/intentd/pull/876))

### 🐛 Bug Fixes

- Derive tool name from dot-separated codex MCP titles ([#869](https://github.com/intent-hq/intentd/pull/869))
- Write per-agent config files under <data_dir>/agent-configs instead of the OS temp dir ([#871](https://github.com/intent-hq/intentd/pull/871))

### 🧪 Testing

- Eliminate remaining test temp residuals (RAII guards, sqlite sidecars, sockets, node caches) ([#872](https://github.com/intent-hq/intentd/pull/872))


## [0.4.0] - 2026-08-03

### 🚀 Features

- Windows named-pipe local transport (listener + CLI client) ([#855](https://github.com/intent-hq/intentd/pull/855))
- Add restarting script status for auto-restart backoff window ([#861](https://github.com/intent-hq/intentd/pull/861))
- [**breaking**] Terminal.list returns { terminals, daemonBootId } envelope ([#862](https://github.com/intent-hq/intentd/pull/862))
- Add structured error.data.code discriminator to -32602 errors ([#863](https://github.com/intent-hq/intentd/pull/863))
- Add host.createDirectory RPC for remote folder creation ([#864](https://github.com/intent-hq/intentd/pull/864))
- Add error.data.code discriminator to fast-path -32602 errors ([#865](https://github.com/intent-hq/intentd/pull/865))

### 🐛 Bug Fixes

- Keep displayStatus in_progress while workspace owns active hooks ([#856](https://github.com/intent-hq/intentd/pull/856))

### 📚 Documentation

- *(agents)* Explain turn idle timeout in hook guidance and timeout warning ([#858](https://github.com/intent-hq/intentd/pull/858))
- *(agents)* Advise estimating hook ttlMs instead of defaulting to the cap ([#860](https://github.com/intent-hq/intentd/pull/860))

### 🧪 Testing

- *(services)* Make in-flight hook TTL expiry test deterministic ([#866](https://github.com/intent-hq/intentd/pull/866))


## [0.3.0] - 2026-08-02

### 🚀 Features

- Queue-aware retire-on-completion delivery for ungrouped watches ([#836](https://github.com/intent-hq/intentd/pull/836))
- Register hardwareConsole.state settings bag key ([#853](https://github.com/intent-hq/intentd/pull/853))
- Stamp queueInfo metadata on drained queue entries ([#834](https://github.com/intent-hq/intentd/pull/834))
- Full-text search over agent chat transcripts ([#845](https://github.com/intent-hq/intentd/pull/845))
- Expose physical workspace disk usage on workspace.list/get ([#849](https://github.com/intent-hq/intentd/pull/849))
- Background hook scheduler with console capture ([#850](https://github.com/intent-hq/intentd/pull/850))
- Daemon-owned specialist default-model resolution with provider guards ([#852](https://github.com/intent-hq/intentd/pull/852))
- [**breaking**] Hook state, 60-min TTL, and hook-aware parent settlement ([#854](https://github.com/intent-hq/intentd/pull/854))

### 🐛 Bug Fixes

- *(services)* Redeliver stranded completion watch after queue retraction/edit ([#841](https://github.com/intent-hq/intentd/pull/841))
- Interim coordinator idle no longer seals the after_all group ([#842](https://github.com/intent-hq/intentd/pull/842))
- Busy-aware interim classification for completion delivery and sealing ([#846](https://github.com/intent-hq/intentd/pull/846))
- *(pty)* Escalate to SIGKILL when the process group is non-empty after grace ([#847](https://github.com/intent-hq/intentd/pull/847))
- *(services)* Reap group stragglers before recording a script exit ([#851](https://github.com/intent-hq/intentd/pull/851))

### 🔧 Refactor

- Remove one_shot from completion-watch registry, registration paths, and store ([#832](https://github.com/intent-hq/intentd/pull/832))
- Drop oneShot from subscription wire payloads and stale watch docs ([#837](https://github.com/intent-hq/intentd/pull/837))

### 🧪 Testing

- Deflake specialists/skills watcher drain helpers ([#839](https://github.com/intent-hq/intentd/pull/839))
- Stop leaking test temp dirs (/tmp) across the suite ([#843](https://github.com/intent-hq/intentd/pull/843))
- Fix residual temp-file leaks (intent-ctx/intent-host, sqlite sidecars, teardown races) ([#848](https://github.com/intent-hq/intentd/pull/848))


## [0.2.16] - 2026-08-01

### 🚀 Features

- Persist and serve lastMessageRole on AgentLite ([#807](https://github.com/intent-hq/intentd/pull/807))
- Sticky attention-state for child/background agents + failure timestamps ([#810](https://github.com/intent-hq/intentd/pull/810))
- Agent-facing queue visibility, single-pending-message guard, dequeue annotation ([#816](https://github.com/intent-hq/intentd/pull/816))
- Restrict agent event subscriptions and add ws.agent.watch/unwatch (intent-hq/monorepo#1229)
- Adjustable workspace_api output limit + TOON encoding ([#819](https://github.com/intent-hq/intentd/pull/819))
- Needs_attention workspace displayStatus ([#825](https://github.com/intent-hq/intentd/pull/825))

### 🐛 Bug Fixes

- Omit exited and script-owned PTYs from terminal.list ([#745](https://github.com/intent-hq/intentd/pull/745))

### 🧪 Testing

- Wss e2e for foreground automatic-delivery attention negative case (monorepo#1237)
- Make pool-contention stress budgets co-tenancy-safe (monorepo#1239) ([#818](https://github.com/intent-hq/intentd/pull/818))


## [0.2.15] - 2026-07-31

### 🚀 Features

- Honor workspace setup settings (worktrees location, per-workspace auto-commit, commit-policy prompts) ([#744](https://github.com/intent-hq/intentd/pull/744))
- *(transport)* Emit user-row deltas on chat.subscribe ([#747](https://github.com/intent-hq/intentd/pull/747))
- Warn-and-continue on prompt idle timeout instead of terminal failure
- Agent attention requests (requestDiscussion/reportBlocker, blocked task status) ([#754](https://github.com/intent-hq/intentd/pull/754))
- Hold automatic deliveries while an agent's question is pending ([#751](https://github.com/intent-hq/intentd/pull/751))
- Scoped cancel for agent.cancelSubscriptions ([#759](https://github.com/intent-hq/intentd/pull/759))
- Rename agent:stream:chunk broadcast to content-free agent:stream:activity with leading-edge 1s throttle ([#775](https://github.com/intent-hq/intentd/pull/775))
- Serve-time synthetic block ids + appMessageId on user-row chat deltas (monorepo#1114, monorepo#1157)
- Overlay live-turn text into AgentLite lastAgentResponse/digest ([#786](https://github.com/intent-hq/intentd/pull/786))
- Per-minute token-rate history and agentSummary parentAgentId (protocol 2.8/2.9) ([#789](https://github.com/intent-hq/intentd/pull/789))
- Carry optional parentAgentId on agent:attention-requested and agent:failed ([#788](https://github.com/intent-hq/intentd/pull/788))
- Carry live preview fields on agent:stream:activity ([#792](https://github.com/intent-hq/intentd/pull/792))
- Derive idle/running agent activity into displayStatus ([#793](https://github.com/intent-hq/intentd/pull/793))

### 🐛 Bug Fixes

- Renumber workspace auto-commit migration to 0067 (intent-hq/monorepo#1126) ([#752](https://github.com/intent-hq/intentd/pull/752))
- *(store)* Retry SQLITE_BUSY on note read path (monorepo#1139) ([#783](https://github.com/intent-hq/intentd/pull/783))
- Await supervisor teardown in script remove/upsert/start to prevent PTY orphans (monorepo#1180)
- Do not clip final text block closed by a tool-call boundary ([#796](https://github.com/intent-hq/intentd/pull/796))
- *(services)* Generation-stamp script registry entries to close the start remove+recreate identity-confusion race (monorepo#1194) ([#801](https://github.com/intent-hq/intentd/pull/801))
- Select Windows-native shells and provider shims (intent-hq/monorepo#1054)
- *(intent-git)* Use COPYFILE_CLONE_FORCE for per-file CoW clone (intent-hq/monorepo#1124) ([#782](https://github.com/intent-hq/intentd/pull/782))
- Normalize spaced workspace-mcp bridge path ([#736](https://github.com/intent-hq/intentd/pull/736))
- Deliver attention-request parent wake immediately in after_all groups ([#758](https://github.com/intent-hq/intentd/pull/758))
- Emit FILE_CHANGED before fs/write_text_file response to close attribution TOCTOU (intent-hq/monorepo#1144)
- Honor INTENTD_TCP_PORT=0 as ephemeral port for the secure WSS boot bind ([#737](https://github.com/intent-hq/intentd/pull/737))
- Stop workspace list CPU thrash and oversized git.diffs wire frames ([#743](https://github.com/intent-hq/intentd/pull/743))
- Enforce one active completion watch per (parent, child) ([#761](https://github.com/intent-hq/intentd/pull/761))
- Suppress SUB-1 child→parent auto-watch and carry row metadata on message deltas ([#773](https://github.com/intent-hq/intentd/pull/773))
- Make script.run cancellation-safe and guard concurrent runs (monorepo#1155) ([#777](https://github.com/intent-hq/intentd/pull/777))
- Agent.diagnostics taskNoteId filter matches assigned agents (monorepo#1150) ([#765](https://github.com/intent-hq/intentd/pull/765))
- Treat workspace prStatus as a PR-stage signal in displayStatus ([#760](https://github.com/intent-hq/intentd/pull/760))
- Guard agent.delegate and task.assignAgent against double-delegating an occupied task ([#774](https://github.com/intent-hq/intentd/pull/774))
- Retain pending attention request across automatic deliveries ([#785](https://github.com/intent-hq/intentd/pull/785))
- Single-flight git.diffs walks, rate-limit slow-walk warn, normalize absolute paths ([#790](https://github.com/intent-hq/intentd/pull/790))
- Clip mid-turn live previews at the last completed newline ([#795](https://github.com/intent-hq/intentd/pull/795))
- Poll session idle status in idle-timeout e2e test (monorepo#1164) ([#799](https://github.com/intent-hq/intentd/pull/799))
- Persist row-level messageMetadata on wake deliveries ([#802](https://github.com/intent-hq/intentd/pull/802))

### 📚 Documentation

- *(acp)* Document why prompt idle timeout must not be raised ([#740](https://github.com/intent-hq/intentd/pull/740))

### ⚡ Performance

- *(store)* Persist last-message previews at write time ([#742](https://github.com/intent-hq/intentd/pull/742))
- *(services)* Cache agent.list message projections until append ([#776](https://github.com/intent-hq/intentd/pull/776))
- *(git)* Use clonefile(2) for whole-tree CoW fast path (monorepo#1125)

### 🧪 Testing

- Add services-level literal paths tests for git.diffs (monorepo#1078) ([#734](https://github.com/intent-hq/intentd/pull/734))
- Adopt _logged WSS readiness pollers in e2e_wss_runtime_control ([#748](https://github.com/intent-hq/intentd/pull/748))
- Assert appMessageId on fresh chat.subscribe snapshot user rows (monorepo#1157) ([#791](https://github.com/intent-hq/intentd/pull/791))

### ⚙️ Miscellaneous Tasks

- Update Cargo.toml dependencies
- Remove dead agent stream event constants ([#756](https://github.com/intent-hq/intentd/pull/756))


## [0.2.14] - 2026-07-29

### 🔧 Refactor

- Rename sandbox.* wire surface to sandbox.cow.* ([#730](https://github.com/intent-hq/intentd/pull/730))


## [0.2.13] - 2026-07-29

### 🚀 Features

- Add ask-tool hint to top-level agent system prompts ([#721](https://github.com/intent-hq/intentd/pull/721))
- *(intent-git)* Single-pass index-to-workdir diff with hunks and pathspec pruning ([#705](https://github.com/intent-hq/intentd/pull/705))
- Derive omitted agent name from specialist display name ([#710](https://github.com/intent-hq/intentd/pull/710))
- *(transport)* Accept paths[] narrowing on git.diffs and prune the diff walk ([#715](https://github.com/intent-hq/intentd/pull/715))

### 🐛 Bug Fixes

- *(services)* ProviderAuthStatus install gate honors providers.paths overrides ([#725](https://github.com/intent-hq/intentd/pull/725))
- Retarget providers.paths[\unsloth\] to the unsloth CLI ([#707](https://github.com/intent-hq/intentd/pull/707))
- ProviderDiscovery installed status honors providers.paths overrides ([#717](https://github.com/intent-hq/intentd/pull/717))
- *(intent-services)* Flush event-bus writer immediately when idle ([#718](https://github.com/intent-hq/intentd/pull/718))

### 🔧 Refactor

- Remove unused file-tracking.sync/.init/.load wire methods ([#704](https://github.com/intent-hq/intentd/pull/704))

### 📚 Documentation

- *(intent-services)* Clarify exit/state vs chunk ordering doc comments ([#720](https://github.com/intent-hq/intentd/pull/720))

### ⚡ Performance

- *(services)* Adopt single-pass index-to-workdir diff in build_diffs and compute_and_store ([#709](https://github.com/intent-hq/intentd/pull/709))


## [0.2.12] - 2026-07-29

### 🚀 Features

- Add providers.catalog RPC serving the intent-providers registry (monorepo#928) ([#694](https://github.com/intent-hq/intentd/pull/694))
- Add atomic agent.sendQueuedMessageNow RPC; remove agent.forceMessage ([#696](https://github.com/intent-hq/intentd/pull/696))
- Turn correlation id (turnId) on agent lifecycle events, queue entries, and RPC responses ([#699](https://github.com/intent-hq/intentd/pull/699))
- Surface secondaryResolvedPath in host.providerDiscovery ([#701](https://github.com/intent-hq/intentd/pull/701))

### 🐛 Bug Fixes

- Thread combined-delivery prepend_* fields through enqueue_message (intent-hq/monorepo#1034) ([#693](https://github.com/intent-hq/intentd/pull/693))

### 🧪 Testing

- Add append-failure auto-queue prepend regression test ([#703](https://github.com/intent-hq/intentd/pull/703))


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

