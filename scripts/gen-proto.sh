#!/usr/bin/env bash
# Regenerates Go code from proto/ and keeps gRPC stubs out of the
# WebAssembly build: the in-browser simulator only needs the message types,
# and the gRPC client/server code (with net/http and TLS) would triple the
# size of sim.wasm.
set -euo pipefail
cd "$(dirname "$0")/.."
buf lint
buf generate
for f in gen/streamforge/v1/*_grpc.pb.go; do
  printf '//go:build !js\n\n' | cat - "$f" > "$f.tmp" && mv "$f.tmp" "$f"
done
