#!/bin/sh
# Fake rtk shim for deterministic e2e testing.
# Prints a mock Commands section with a couple of subcommands, including one
# excluded name to prove filtering.

if [ "$1" = "help" ]; then
  cat <<'EOF'
rtk - compressed CLI output

Commands:
  ls             List directory contents (compressed)
  cat            Concatenate files (compressed)
  test           Run tests (excluded - shell builtin conflict)
  grep           Search file patterns (compressed)
  help           Show this help (excluded - internal command)

Options:
  --version      Show version
  --help         Show help
EOF
  exit 0
fi

echo "rtk: unknown command. Run 'rtk help' for usage."
exit 1
