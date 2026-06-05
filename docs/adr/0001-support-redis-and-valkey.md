# ADR-0001: Support Redis and Valkey

## Status

Accepted, 2026-06-05

## Context

Senna stores queue, schedule, batch, uniqueness, retry, and rate limit state in
Redis-compatible data structures. Redis and Valkey are both common production
choices for this role, and Senna's public API should not force users to choose a
different package or runtime model based on which compatible server they run.

Senna currently supports Redis 6.2+ and Valkey 7.2+. That version floor lets the
worker implementation rely on compatible blocking list operations while keeping
the supported backend set small enough to test and document clearly.

## Decision

Senna will support Redis and Valkey as first-class backends for every core
feature.

New features must either work correctly on both supported backends or explicitly
reject unsupported server capabilities before exposing user-visible behavior.
Backend-specific behavior belongs behind small compatibility boundaries and must
be documented when it affects operators or application code.

## Consequences

Users can deploy Senna with either Redis or Valkey without changing Senna APIs or
architectural assumptions.

Compatibility work becomes part of feature design. Tests, documentation, and Lua
scripts must consider both backends before relying on a command, reply shape, or
server behavior.

Senna should avoid adding features that only work on one backend unless the
feature can degrade clearly or fail during configuration.

## Alternatives Considered

Redis-only support would reduce the compatibility surface, but it would exclude
Valkey deployments that satisfy Senna's persistence and atomicity requirements.

An arbitrary backend abstraction would create a larger design surface before the
project needs it. Senna is intentionally Redis-compatible, not a general storage
abstraction.
