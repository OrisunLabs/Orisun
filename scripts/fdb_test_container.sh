#!/usr/bin/env bash
# Compile and test the FoundationDB backend inside a Linux container with a
# throwaway single-node fdbserver.
# Usage: scripts/fdb_test_container.sh [go-test-args...]
#        TEST_PKGS=./cmd/ scripts/fdb_test_container.sh -run TestE2E_LedgerWorkload_FoundationDB -v
set -euo pipefail

FDB_VERSION="${FDB_VERSION:-7.3.77}"
GO_IMAGE="${GO_IMAGE:-golang:1.27.1-bookworm}"
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ARCH="$(uname -m)"
case "$ARCH" in
  arm64|aarch64) DEB_ARCH=aarch64 ;;
  x86_64|amd64) DEB_ARCH=amd64 ;;
  *) echo "unsupported arch: $ARCH" >&2; exit 1 ;;
esac

TEST_ARGS="$*"
if [ -z "$TEST_ARGS" ]; then
  TEST_ARGS="-run TestFoundationDB -v"
fi
TEST_PKGS="${TEST_PKGS:-./foundationdb/}"

docker run --rm \
  -v "$REPO_DIR":/src \
  -v "${GOMODCACHE:-$HOME/go/pkg/mod}":/go/pkg/mod \
  -w /src \
  -e FDB_VERSION="$FDB_VERSION" \
  -e DEB_ARCH="$DEB_ARCH" \
  -e TEST_ARGS="$TEST_ARGS" \
  -e TEST_PKGS="$TEST_PKGS" \
  -e ORISUN_FDB_SOAK="${ORISUN_FDB_SOAK:-}" \
  "$GO_IMAGE" bash -ec '
    base="https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}"
    curl_args=(-fsSL --retry 5 --retry-all-errors --retry-delay 2 --retry-max-time 120 --connect-timeout 30)
    curl "${curl_args[@]}" -o /tmp/clients.deb "${base}/foundationdb-clients_${FDB_VERSION}-1_${DEB_ARCH}.deb"
    curl "${curl_args[@]}" -o /tmp/server.deb  "${base}/foundationdb-server_${FDB_VERSION}-1_${DEB_ARCH}.deb"
    dpkg -i /tmp/clients.deb >/dev/null
    # The server postinst tries to start a service; install files only and run manually.
    dpkg --unpack /tmp/server.deb >/dev/null 2>&1 || true
    rm -f /var/lib/dpkg/info/foundationdb-server.postinst
    dpkg --configure foundationdb-server >/dev/null 2>&1 || true

    mkdir -p /var/fdb/data /var/fdb/logs /etc/foundationdb
    echo "docker:docker@127.0.0.1:4500" > /etc/foundationdb/fdb.cluster
    /usr/sbin/fdbserver -p 127.0.0.1:4500 \
      -d /var/fdb/data -L /var/fdb/logs \
      -C /etc/foundationdb/fdb.cluster &
    for i in $(seq 1 30); do
      if fdbcli -C /etc/foundationdb/fdb.cluster --exec "configure new single memory" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    fdbcli -C /etc/foundationdb/fdb.cluster --exec "status minimal"

    export GOFLAGS=-buildvcs=false
    export ORISUN_FDB_TEST_CLUSTER_FILE=/etc/foundationdb/fdb.cluster
    go vet -tags foundationdb ./foundationdb/
    # $TEST_ARGS/$TEST_PKGS expand after parsing: metacharacters in them (e.g.
    # | in -bench regexes) are not reinterpreted as shell operators.
    go test -tags foundationdb $TEST_PKGS $TEST_ARGS
  '
