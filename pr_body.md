## Summary

Implements three observability features to make the next unexplained daemon death diagnosable from disk (addresses gaps identified in STAB-15 investigation):

1. **File logging with rotation**
2. **Panic hook with backtrace logging**  
3. **DB health section in intentd doctor**

## Changes

### File Logging
- Uses tracing-appender::rolling with rotation
- Logs to INTENTD_DATA_DIR/intentd.log
- Keeps ~5 files, ~10MB each
- INFO default, RUST_LOG override respected
- Dual output: stderr (interactive) + file (diagnostics)

### Panic Hook
- Installed via std::panic::set_hook
- Captures full backtrace using std::backtrace::Backtrace::force_capture()
- Logs to tracing (which writes to file) with location, message, and backtrace
- Also writes to stderr for immediate visibility

### DB Health Section
- Added report_db_health() function called from cmd_doctor()
- Runs PRAGMA integrity_check
- Runs PRAGMA wal_checkpoint(PASSIVE) and reports frame counts
- Reports connection pool stats (size, idle connections)
- All checks informational, never fail doctor

## Verification

```
make check && make test
```

**Doctor output:**
```
database health:
  [ok] integrity_check: ok
  [ok] wal_checkpoint(PASSIVE): busy=0, log=636 frames, checkpointed=636 frames
  [ok] pool: size=1, idle=0
```

## Files Changed

- crates/intentd/src/main.rs: Updated init_tracing(), added install_panic_hook(), added report_db_health()
- crates/intentd/Cargo.toml: Added tracing-appender, directories, sqlx dependencies
- Cargo.lock: Updated lockfile

## Notes

- Does NOT touch pool sizing, busy_timeout, or WAL settings (owned by sibling workspace)
- Log file appears with rotation on fresh run
- Forced panic writes backtrace to file
- Unit/integration coverage for doctor section feasible but not required (informational checks)
