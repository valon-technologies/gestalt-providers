#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

unformatted="$(gofmt -l .)"
if [[ -n "$unformatted" ]]; then
	echo "gofmt required for:"
	echo "$unformatted"
	exit 1
fi

go vet .
go test .
