#!/usr/bin/env bash
# Run a cargo command on mini-mac against the current working tree.
# ermac has no Rust/C toolchain; mini-mac (the deploy target) does.
# Usage: bash scripts/remote-cargo.sh test --test db_eventlog
#        bash scripts/remote-cargo.sh clippy -- -D warnings
# The tree is copied (without target/.git/.claude/.superpowers) to ~/pit-build on mini-mac;
# the remote target/ dir is kept between runs for fast incremental builds.
set -euo pipefail
HOST="${PIT_BUILD_HOST:-emac@mini-mac}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
tar czf - --exclude=./target --exclude=./.git --exclude=./.claude --exclude=./.superpowers . \
  | ssh "$HOST" 'mkdir -p ~/pit-build && cd ~/pit-build && find . -mindepth 1 -maxdepth 1 ! -name target -exec rm -rf {} + && tar xzf -'
ssh "$HOST" "cd ~/pit-build && ~/.cargo/bin/cargo $*"
