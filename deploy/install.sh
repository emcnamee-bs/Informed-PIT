#!/usr/bin/env bash
# Install or update Informed-PIT on mini-mac (run there, from ~/Informed-PIT on branch deploy).
# - builds the release binary
# - installs the systemd user unit
# - creates the settings file (mode 600) with placeholders if it does not exist yet
#   (keys are filled in separately; this script never prints or copies secrets)
# - enables and (re)starts the service
set -euo pipefail
cd "$(dirname "$0")/.."
~/.cargo/bin/cargo build --release
mkdir -p ~/.config/systemd/user ~/.config/informed-pit ~/informed-pit-data
install -m 644 deploy/informed-pit.service ~/.config/systemd/user/informed-pit.service
CFG=~/.config/informed-pit/env
if [ ! -f "$CFG" ]; then
  umask 077
  printf '%s\n' '# Informed-PIT settings (mode 600). Fill in keys; never commit this file.' \
    'ANTHROPIC_API_KEY=' 'NEWSAPI_AI_KEY=' > "$CFG"
fi
chmod 600 "$CFG"
systemctl --user daemon-reload
systemctl --user enable informed-pit.service
systemctl --user restart informed-pit.service
systemctl --user --no-pager status informed-pit.service | head -5
