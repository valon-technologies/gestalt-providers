#!/bin/sh

set -eu

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <plugin-dir> <gestalt-repository> <gestalt-ref> <version>" >&2
  exit 2
fi

plugin_dir="$1"
gestalt_repository="$2"
gestalt_ref="$3"
version="$4"
go_version="${GO_VERSION:-1.26.1}"
pat_token="${PAT_TOKEN:-}"

if [ -z "$pat_token" ]; then
  echo "PAT_TOKEN is required to package musl Python plugins" >&2
  exit 1
fi

apk add --no-cache build-base ca-certificates curl git
curl -fsSL "https://go.dev/dl/go${go_version}.linux-amd64.tar.gz" | tar -C /usr/local -xz

printf "machine github.com\n  login x-access-token\n  password %s\n" "$pat_token" > "${HOME}/.netrc"
chmod 600 "${HOME}/.netrc"

git config --global url."https://github.com/".insteadOf "ssh://git@github.com/"
git config --global url."https://github.com/".insteadOf "git@github.com:"

mkdir -p /tmp/bin
export PATH="/usr/local/go/bin:/tmp/bin:$PATH"
/workspace/.github/scripts/install_gestaltd_from_ref.sh "$gestalt_repository" "$gestalt_ref" /tmp/bin/gestaltd

cd "$plugin_dir"
rm -rf .venv
uv sync
export GESTALT_PYTHON_LINUX_AMD64="$PWD/.venv/bin/python"
gestaltd plugin release --version "$version" --platform linux/amd64/musl
chmod -R a+rX dist
