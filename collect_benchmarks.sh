#!/usr/bin/env bash
set -euo pipefail

REPOSITORY_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

if [[ -n ${ORISUN_BENCHMARK_BINARY:-} ]]; then
  if [[ ! -x "$ORISUN_BENCHMARK_BINARY" ]]; then
    echo "ORISUN_BENCHMARK_BINARY is not executable: $ORISUN_BENCHMARK_BINARY" >&2
    exit 1
  fi
  exec "$ORISUN_BENCHMARK_BINARY" "$@"
fi

BENCHMARK_BIN_DIR=${ORISUN_BENCHMARK_BIN_DIR:-"$REPOSITORY_ROOT/tmp/benchmark-bin"}
BENCHMARK_BINARY="$BENCHMARK_BIN_DIR/orisun-bench"
BUILD_TIME=$(date -u +'%Y-%m-%dT%H:%M:%SZ')
GIT_COMMIT=$(git -C "$REPOSITORY_ROOT" rev-parse HEAD)
VERSION=$(git -C "$REPOSITORY_ROOT" describe --tags --always)
DIRTY=false
if [[ -n $(git -C "$REPOSITORY_ROOT" status --porcelain) ]]; then
  DIRTY=true
fi

mkdir -p "$BENCHMARK_BIN_DIR"

go build \
  -trimpath \
  -ldflags="-s -w \
    -X main.benchmarkVersion=$VERSION \
    -X main.benchmarkGitCommit=$GIT_COMMIT \
    -X main.benchmarkBuildTime=$BUILD_TIME \
    -X main.benchmarkDirty=$DIRTY" \
  -o "$BENCHMARK_BINARY" \
  "$REPOSITORY_ROOT/cmd/orisun-bench"

exec "$BENCHMARK_BINARY" "$@"
