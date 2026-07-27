#!/usr/bin/env bash
# CI script for tpt-workflow - matches tpt-synapse convention
# Usage: ./scripts/ci.sh [check|test|build|fmt|clippy|all]

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CMD="${1:-all}"

case "$CMD" in
    check)
        echo "=== Running cargo check ==="
        cargo check --workspace
        ;;
    test)
        echo "=== Running cargo test ==="
        cargo test --workspace
        ;;
    build)
        echo "=== Running cargo build ==="
        cargo build --workspace --release
        ;;
    fmt)
        echo "=== Running cargo fmt ==="
        cargo fmt --all -- --check
        ;;
    clippy)
        echo "=== Running cargo clippy ==="
        cargo clippy --workspace -- -D warnings
        ;;
    all)
        echo "=== Running full CI pipeline ==="
        cargo fmt --all -- --check
        cargo clippy --workspace -- -D warnings
        cargo check --workspace
        cargo test --workspace
        cargo build --workspace --release
        ;;
    *)
        echo "Usage: $0 [check|test|build|fmt|clippy|all]"
        exit 1
        ;;
esac

echo "=== CI completed successfully ==="