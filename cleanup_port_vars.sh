#!/bin/bash
set -e

# Remove unused port variables and ensure tests read port from system.status

for file in crates/intentd/tests/e2e_wss_*.rs; do
    if [[ ! -f "$file" ]] || [[ "$file" == *"wss_port_setting"* ]]; then
        continue
    fi
    
    echo "Processing $file..."
    
    # Remove lines with: let port = free_port();
    perl -i -ne 'print unless /^\s*let port = free_port\(\);/' "$file"
    
    # Remove lines with: let port_s = port.to_string();
    perl -i -ne 'print unless /^\s*let port_s = port\.to_string\(\);/' "$file"
done

# Also process wss_integration.rs
if [[ -f "crates/intentd/tests/wss_integration.rs" ]]; then
    echo "Processing wss_integration.rs..."
    perl -i -ne 'print unless /^\s*let port = free_port\(\);/' "crates/intentd/tests/wss_integration.rs"
    perl -i -ne 'print unless /^\s*let port_s = port\.to_string\(\);/' "crates/intentd/tests/wss_integration.rs"
fi

echo "Done! Running cargo fmt..."
cargo fmt

echo "Checking which files still have free_port calls..."
rg -l "free_port\(\)" crates/intentd/tests/*.rs | grep -v wss_port_setting.rs || echo "All basic patterns converted!"
