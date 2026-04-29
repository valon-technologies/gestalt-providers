#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 IMAGE" >&2
  exit 2
fi

image="$1"
platforms=(linux/amd64 linux/arm64)

inspect_output="$(docker buildx imagetools inspect "${image}")"
for platform in "${platforms[@]}"; do
  if ! grep -Eq "Platform:[[:space:]]*${platform}([[:space:]]|$)" <<<"${inspect_output}"; then
    echo "runtime image ${image} is missing ${platform}" >&2
    echo "${inspect_output}" >&2
    exit 1
  fi
done

for platform in "${platforms[@]}"; do
  docker run --rm \
    --platform "${platform}" \
    --workdir / \
    --entrypoint sh \
    "${image}" \
    -lc '
      set -eu
      test -x ./gestalt-plugin-simple
      test -x /app/gestalt-plugin-simple

      set +e
      timeout 5s ./gestalt-plugin-simple --help >/tmp/gestalt-plugin-simple.out 2>&1
      status=$?
      set -e

      case "${status}" in
        126|127)
          cat /tmp/gestalt-plugin-simple.out >&2
          exit "${status}"
          ;;
      esac
    '
done
