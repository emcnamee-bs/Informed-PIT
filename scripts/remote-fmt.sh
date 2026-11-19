#!/usr/bin/env bash
# Format the working tree with rustfmt on mini-mac and copy the formatted src/ and tests/ back.
# Usage: bash scripts/remote-fmt.sh
set -euo pipefail
HOST="${PIT_BUILD_HOST:-emac@mini-mac}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
bash scripts/remote-cargo.sh fmt
ssh "$HOST" 'cd ~/pit-build && tar czf - src tests' | tar xzf -
