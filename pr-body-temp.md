Add comprehensive e2e tests covering agent→BE MCP bindings for task, comment, event, git, file, agent read-side, and deepened note operations. All tests drive real MCP tools/call invocations through the mock ACP agent and assert BE state changes.

## Coverage impact (mcp_server/bindings/)

| File | Before | After | Improvement |
|------|--------|-------|-------------|
| task.rs | 0% | 11.54% | +11.54% |
| comment.rs | 0% | 36.45% | +36.45% |
| event.rs | 0% | 56.54% | +56.54% |
| git.rs | 0% | 30.96% | +30.96% |
| file.rs | 0% | 16.24% | +16.24% |
| agent.rs | 0% | 6.89% | +6.89% |
| note.rs | 11.14% | 35.79% | +24.65% |

**Overall e2e coverage: 36.93% → 41.17% (+4.24%)**

## Test files

- e2e_mock_agent_workspace_api_bindings.rs – task + comment bindings
- e2e_mock_agent_workspace_api_bindings2.rs – event, git, file, agent, note bindings

All tests follow the existing e2e_mock_agent.rs pattern using MOCK_AGENT_BEHAVIOR and real MCP tool calls.

## Verification

```bash
cd packages/intentd
cargo test -p intentd --test e2e_mock_agent_workspace_api_bindings*
./scripts/coverage-e2e.sh
```
