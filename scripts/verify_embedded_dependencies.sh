#!/usr/bin/env bash

set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_dir}"

targets=(
  ./orisun
  ./sqlite
)

dependencies="$(go list -tags=orisun_embedded -deps "${targets[@]}")"
for forbidden in \
  '^google\.golang\.org/grpc($|/)' \
  '^github\.com/nats-io($|/)' \
  '^github\.com/OrisunLabs/Orisun/(server|nats|postgres|embedded/postgres|orisun/grpcapi)($|/)'
do
  if matches="$(printf '%s\n' "${dependencies}" | grep -E "${forbidden}")"; then
    echo "embedded dependency guard rejected transport/server packages:" >&2
    printf '%s\n' "${matches}" >&2
    exit 1
  fi
done

embedded_packages=(
  ./embedded/sqlite
  ./embedded/postgres
)
embedded_dependencies="$(go list -deps "${embedded_packages[@]}")"
if matches="$(printf '%s\n' "${embedded_dependencies}" | grep -E '^google\.golang\.org/(grpc|protobuf)($|/)')"; then
  echo "embedded package dependency guard rejected gRPC/protobuf packages:" >&2
  printf '%s\n' "${matches}" >&2
  exit 1
fi

generated_files=(
  orisun/grpcapi/*.pb.go
)
if matches="$(grep -l -E '^//go:build' "${generated_files[@]}" || true)"; [[ -n "${matches}" ]]; then
  echo "generated protobuf files must not contain handwritten build constraints:" >&2
  printf '%s\n' "${matches}" >&2
  exit 1
fi

echo "Embedded core and runtimes are gRPC/protobuf-free."
