---
title: Development
description: Build, test, document, and package Orisun.
---

## Requirements

- Go 1.26.5+
- Docker for integration tests
- `task` for optional development workflows
- Bun 1.3.13 for the documentation site

## Common Commands

```bash
go test ./...
go build ./...
go fmt ./...
go vet ./...
go run ./cmd
```

## Taskfile Helpers

```bash
task tools
task build
task build:pg
task build:sqlite
task run:sqlite
task live
task debug
```

## Docker

Build locally:

```bash
docker build -t orisun:local .
```

Run the local Docker smoke test:

```bash
./scripts/test_docker.sh
```

## Benchmarks

```bash
go test -bench=. -benchtime=3s ./cmd/benchmark_test.go
go test -run='^$' -bench=BenchmarkConsistencyCheck -benchtime=5s ./postgres/...
./collect_benchmarks.sh
```

## Documentation

Run the documentation site locally:

```bash
cd docs
bun install
bun run start
```

Build the static site:

```bash
cd docs
bun run build
```

The GitHub Pages workflow runs the same frozen install and build:

```bash
bun install --frozen-lockfile
bun run build
```
