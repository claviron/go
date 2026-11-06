#!/usr/bin/env bash
# Cross-compile only FreeBSD x64, in Docker. Other targets build on their own hosts.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [[ $# -ne 0 ]]; then echo 'Usage: scripts/prepare-freebsd.sh' >&2; exit 2; fi
if [[ "$(uname -s)/$(uname -m)" != Linux/x86_64 ]]; then
  echo 'The Docker cross builder requires a Linux x64 host.' >&2; exit 2
fi
for command in docker cargo rustc rustup python3; do command -v "$command" >/dev/null; done
toolchain="$(rustc --print sysroot)"
if [[ ! -d "$toolchain/lib/rustlib/x86_64-unknown-freebsd/lib" ]]; then
  echo 'Install the matching standard library first: rustup target add x86_64-unknown-freebsd' >&2
  exit 1
fi
project="$(pwd -P)"
cache="${CLAVIRON_FREEBSD_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/claviron-go-freebsd}"
mkdir -p "$cache/cargo" "$cache/target" prebuilds
image="claviron-go-freebsd-builder:14.4"
registry="${CARGO_HOME:-$HOME/.cargo}/registry"
jobs="${CARGO_BUILD_JOBS:-4}"
python3 -c 'import shutil, sys; sys.dont_write_bytecode = True; sys.path.insert(0, "scripts"); import release; shutil.copy2(release.ensure_lock(), "Cargo.lock")'
# Fetch through the operator's Cargo configuration; credentials never enter Docker.
cargo fetch --locked --target x86_64-unknown-freebsd
docker build --network "${CLAVIRON_FREEBSD_BUILD_NETWORK:-default}" --tag "$image" scripts/freebsd
# No host networking, privileged access, live SHM, credentials or exposed ports.
# Only the source tree is read; prebuilds/ is the sole writable source mount.
docker run --rm --network none --user "$(id -u):$(id -g)" --cap-drop ALL \
  --security-opt no-new-privileges --cpus "$jobs" --memory 8g \
  --mount "type=bind,src=$project,dst=/workspace/claviron-go,readonly" \
  --mount "type=bind,src=$project/prebuilds,dst=/workspace/claviron-go/prebuilds" \
  --mount "type=bind,src=$toolchain,dst=/opt/rust,readonly" \
  --mount "type=bind,src=$cache,dst=/cache" \
  --mount "type=bind,src=$registry,dst=/cache/cargo/registry,readonly" \
  --env CARGO_NET_OFFLINE=true --env CARGO_BUILD_JOBS="$jobs" "$image"
