## Summary

Fixes specialist frontmatter `model` field resolution during agent creation, ensuring delegated/background agents use the specialist's declared model before falling back to settings.

## Changes

- **specialists.rs**: Added `SpecialistsService::resolve_model` to extract the `model` field from specialist frontmatter (3-tier resolution: project > user > bundled), with path-traversal validation via `validate_id`
- **agent_ops.rs**: Modified `agent_create_op` to capture `workspace_path` from `AgentCreateExtra` and consult specialist frontmatter before the settings chain
- **tests_specialist_frontmatter.rs**: Added regression test file with 4 tests covering precedence scenarios and path-traversal security

## Precedence

```
explicit model > specialist frontmatter model > settings chain > CLI default
```

## Security

`resolve_model` validates the specialist `id` parameter via `validate_id` before building file paths, preventing path traversal attacks.

## Testing

- 4 unit tests in `tests_specialist_frontmatter.rs`:
  - Specialist frontmatter model used when no explicit model
  - Missing/empty frontmatter falls through to settings
  - Explicit model beats specialist frontmatter
  - Malicious specialist id with path traversal is rejected
- Full `intent-services` test suite (1118 tests) green
- `cargo fmt --check` and `cargo clippy -- -D warnings` clean
