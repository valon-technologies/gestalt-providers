#!/usr/bin/env bash

set -euo pipefail

: "${PAT_TOKEN:?PAT_TOKEN is required}"

cat > "${HOME}/.netrc" <<EOF
machine github.com
  login x-access-token
  password ${PAT_TOKEN}
EOF
chmod 600 "${HOME}/.netrc"

git config --global url."https://github.com/".insteadOf "ssh://git@github.com/"
git config --global url."https://github.com/".insteadOf "git@github.com:"
