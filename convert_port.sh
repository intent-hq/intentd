#!/bin/bash
set -e

# Simple script to convert ("INTENTD_TCP_PORT", &port_s) to ("INTENTD_TCP_PORT", "0")
# and remove let port_s = free_port().to_string(); lines

for file in crates/intentd/tests/e2e_wss_*.rs crates/intentd/tests/wss_integration.rs; do
    if [[ ! -f "$file" ]]; then
        continue
    fi
    
    # Skip wss_port_setting.rs
    if [[ "$file" == *"wss_port_setting"* ]]; then
        continue
    fi
    
    echo "Processing $file..."
    
    # Replace ("INTENTD_TCP_PORT", &port_s) with ("INTENTD_TCP_PORT", "0")
    perl -i -pe 's/\("INTENTD_TCP_PORT", &port_s\)/("INTENTD_TCP_PORT", "0")/g' "$file"
    
    # Remove lines: let port_s = free_port().to_string();
    perl -i -ne 'print unless /^\s*let port_s = free_port\(\)\.to_string\(\);/' "$file"
done

echo "Done! Running cargo fmt..."
cargo fmt

echo "Checking which files still have free_port..."
rg -l "free_port\(\)" crates/intentd/tests/*.rs | grep -v wss_port_setting.rs || echo "All converted!"
