#!/bin/sh
set -eu

rm -rf .next out
npm ci
npm run check
npm run build
