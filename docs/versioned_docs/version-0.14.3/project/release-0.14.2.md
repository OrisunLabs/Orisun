---
title: Release v0.14.2
description: Go 1.27.3 across server builds, container images, CI, and the Go client.
---

## Changes

Server builds, release binaries, Docker images, and GitHub Actions now use Go
1.27.3. The server and pinned Go client modules require Go 1.27.3, and the Go
client workflows use the same version.

This release contains only the Go toolchain upgrade and its documentation.
Public APIs, configuration, and storage formats are unchanged from v0.14.1.
Upgrading from v0.14.1 requires no new storage migration or checkpoint changes.
Source builds require Go 1.27.3 or newer.

## Distribution

PostgreSQL-only and SQLite-only binaries are available for Linux, macOS, and
Windows. Docker Hub and GHCR publish `0.14.2-pg` and `0.14.2-sqlite` images,
alongside the `pg` and `sqlite` channel tags.
