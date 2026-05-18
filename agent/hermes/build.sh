#!/usr/bin/env sh
set -eu

output=".gestalt/build/provider"
goos="${GOOS:-}"
goarch="${GOARCH:-}"
if [ -z "$goos" ] && [ -n "${RELEASE_PLATFORM:-}" ]; then
  goos="${RELEASE_PLATFORM%%/*}"
  goarch="${RELEASE_PLATFORM#*/}"
fi
if [ -z "$goos" ]; then
  case "$(uname -s)" in
    Darwin) goos="darwin" ;;
    Linux) goos="linux" ;;
    *) goos="$(uname -s | tr '[:upper:]' '[:lower:]')" ;;
  esac
fi
if [ -z "$goarch" ]; then
  case "$(uname -m)" in
    x86_64|amd64) goarch="amd64" ;;
    arm64|aarch64) goarch="arm64" ;;
    *) goarch="$(uname -m)" ;;
  esac
fi

case "$goos/$goarch" in
  darwin/amd64) target="x86_64-apple-darwin" ;;
  darwin/arm64) target="aarch64-apple-darwin" ;;
  linux/amd64) target="x86_64-unknown-linux-musl" ;;
  linux/arm64) target="aarch64-unknown-linux-musl" ;;
  windows/amd64) target="x86_64-pc-windows-gnu" ;;
  *) echo "unsupported Rust target platform $goos/$goarch" >&2; exit 2 ;;
esac

mkdir -p "$(dirname "$output")"
cargo build --release --target "$target" --target-dir .gestalt/target --bin gestalt-agent-hermes
built=".gestalt/target/$target/release/gestalt-agent-hermes"
if [ "$goos" = "windows" ]; then
  built="$built.exe"
fi
cp "$built" "$output"
chmod 755 "$output"
