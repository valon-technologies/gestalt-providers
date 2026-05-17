#!/bin/sh
set -eu

cleanup() {
  rm -rf node_modules .next tsconfig.tsbuildinfo next-env.d.ts
}
trap cleanup EXIT

rm -rf .next out
npm ci
npm run check
npm run build:next
rm -rf out
npm run build:static
