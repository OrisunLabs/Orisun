<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/static/img/logo-dark.svg">
    <img src="docs/static/img/logo.svg" alt="Orisun" width="96" height="96">
  </picture>
</p>

# Orisun

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![CI](https://github.com/OrisunLabs/Orisun/actions/workflows/ci.yml/badge.svg)](https://github.com/OrisunLabs/Orisun/actions/workflows/ci.yml)
[![Release](https://github.com/OrisunLabs/Orisun/actions/workflows/release.yml/badge.svg)](https://github.com/OrisunLabs/Orisun/actions/workflows/release.yml)
[![Latest Release](https://img.shields.io/github/v/release/OrisunLabs/Orisun?label=release)](https://github.com/OrisunLabs/Orisun/releases/latest)

Orisun is an open-source event database built for Command Context Consistency (CCC).

Applications declare the event context behind a decision with content-based queries. Orisun commits new events only if that context is still current, preserving the complete event history without forcing consistency rules into fixed streams or aggregates.

Orisun provides:

- Transactional event storage with SQLite, PostgreSQL, and FoundationDB (beta).
- Query-driven optimistic concurrency with content-scoped consistency checks.
- Per-boundary ordered catch-up and live subscriptions through embedded NATS JetStream.
- Event-backed boundary creation and import at runtime, without a startup boundary list.
- gRPC APIs, generated clients, server binaries, Docker images, and embedded runtimes.

## Documentation

See the [Orisun documentation](https://orisunlabs.github.io/Orisun/) for concepts, setup, the
[Admin boundary API](https://orisunlabs.github.io/Orisun/docs/api/admin#boundary-lifecycle),
client libraries, embedding, migration, and operations. Existing deployments
should follow the dedicated
[0.7.0 to 0.8.0 upgrade guide](https://orisunlabs.github.io/Orisun/docs/operations/upgrading-0.7-to-0.8)
before changing their server version.

## License

[MIT](LICENSE)
