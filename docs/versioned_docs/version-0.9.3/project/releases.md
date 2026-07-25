---
title: Releases
description: Publish binaries, Docker images, and curated release notes.
---

Create a normal release from `main`:

```bash
./scripts/release.sh 1.2.3
```

The script is the release path for this repository. It validates the tree, creates the annotated tag, pushes it, asks the Go module proxy and pkg.go.dev to fetch the new version, and lets the GitHub release workflow publish binaries, Docker images, and the GitHub release.

Attach curated release notes to the GitHub release:

```bash
./scripts/release.sh 1.2.3 --notes release-notes.md
```

The release script stores notes verbatim in the annotated git tag, including markdown headings.

If proxy.golang.org or pkg.go.dev is unavailable, rerun the release script only if
the tag was not pushed. After a successful tag push, the Go index sync is
best-effort and can be skipped with `SKIP_GO_INDEX_SYNC=1`.

The GitHub release workflow uses release notes in this order:

1. Manual `release_notes` input from `workflow_dispatch`.
2. Annotated tag message from `scripts/release.sh --notes`.
3. Generated commit log since the previous tag.

## Release checklist

Before tagging:

1. Run the Go test suite.
2. Build the docs site with `bun run build` from `docs/`.
3. Confirm release notes describe user-facing behavior, migrations, and image tags.
4. Create and push the annotated tag with `scripts/release.sh`.
5. Confirm pkg.go.dev has rendered the new version.
6. Confirm the GitHub Actions release workflow completed and the GitHub release exists.

Do not create release tags manually unless you are repairing a failed release and understand which script checks you are bypassing.

## Breaking Releases

Use a minor-version bump for storage or API changes that require operator action. For example, the PostgreSQL position migration that changes public `commit_position` values from PostgreSQL internal transaction IDs to Orisun logical positions is released as `0.3.1`, not another `0.2.x` patch.

Breaking release notes should include:

- what changed for existing deployments,
- whether startup migrations run automatically,
- backup and rollback expectations,
- any required one-node-first rollout steps for clustered deployments,
- whether external consumers need to remap stored positions.

## Binary Assets

Each release publishes standalone binaries for Linux, macOS, and Windows. Use these when deploying Orisun directly as a process instead of through Docker.

| Asset pattern | Backend |
| --- | --- |
| `orisun-pg-<os>-<arch>` | PostgreSQL-compatible backends: PostgreSQL and YugabyteDB |
| `orisun-sqlite-<os>-<arch>` | SQLite only |
| `orisun-fdb-linux-<arch>` | FoundationDB only; beta; Linux only |

Linux and macOS binaries should be marked executable after download:

```bash
chmod +x ./orisun-sqlite
```

## Docker Tags

The release workflow publishes the same tags to Docker Hub (`orisunlabs/orisun`) and GitHub Container Registry (`ghcr.io/orisunlabs/orisun`).

| Tag | Backend |
| --- | --- |
| `orisunlabs/orisun:pg` | PostgreSQL-compatible backends: PostgreSQL and YugabyteDB |
| `orisunlabs/orisun:sqlite` | SQLite only |
| `orisunlabs/orisun:fdb` | FoundationDB only, beta, includes the FDB client library |
| `orisunlabs/orisun:<version>-pg` | PostgreSQL-compatible release version |
| `orisunlabs/orisun:<version>-sqlite` | SQLite-only release version |
| `orisunlabs/orisun:<version>-fdb` | FoundationDB-only release version |

Use the same suffixes with `ghcr.io/orisunlabs/orisun` when you prefer GHCR.
