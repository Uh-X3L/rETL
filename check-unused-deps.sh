#!/usr/bin/env bash
# Check for unused dependencies in all workspace crates (Bash/WSL version)
# Requires: cargo-udeps (will be installed if missing)

set -e

# Check for cargo-udeps, install if missing
if ! command -v cargo-udeps &> /dev/null; then
    echo 'cargo-udeps not found, installing...'
    cargo install cargo-udeps
fi

echo 'Running unused dependency check (requires nightly toolchain)...'
udeps_output=$(cargo +nightly udeps --all-targets --workspace 2>&1)
echo "$udeps_output"

echo 'Parsing unused dependencies and generating removal commands...'

# Parse output for unused dependencies and print cargo rm commands
# This works for typical cargo-udeps output, but always review before running
awk '/^unused dependencies:/ {
    deps = $0
    sub(/^unused dependencies: /, "", deps)
    n = split(deps, arr, ",");
    for (i = 1; i <= n; i++) {
        dep = arr[i];
        gsub(/^ +| +$/, "", dep)
        if (dep != "") {
            print dep
        }
    }
} /^crate:/ { crate = $2 } crate && /^unused dependencies:/ {
    for (i = 1; i <= n; i++) {
        if (arr[i] != "") {
            printf("cargo rm %s --manifest-path components/%s/Cargo.toml\n", arr[i], crate)
        }
    }
    crate = ""
}' <<< "$udeps_output"
