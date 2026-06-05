# ADR-0004: Remain brokerless

## Status

Accepted, 2026-06-05

## Context

Senna is a Go library. Clients and workers connect directly to Redis or Valkey,
and coordination happens through server-side data structures, Lua scripts,
leases, heartbeats, and worker state.

A required Senna broker, scheduler daemon, or coordinator service would change
the deployment model. It would also introduce another process that operators
must run, upgrade, observe, and keep highly available.

## Decision

Senna will remain brokerless.

Application processes may enqueue and process jobs directly through the library.
Redis or Valkey is the shared coordination point. Senna must not require a
separate Senna-owned broker, scheduler, or coordinator service for core
functionality.

## Consequences

Deployments stay simple: operators run application workers plus Redis or Valkey,
not a required Senna control plane.

Coordination logic must remain correct inside library code, Lua scripts, and
Redis or Valkey data structures.

Operational visibility should come from library APIs, metrics, logs, and backend
state rather than a mandatory broker UI.

Features that require a central always-on Senna process are outside the core
architecture. Optional tooling is acceptable only when core semantics do not
depend on it.

## Alternatives Considered

A dedicated broker service could centralize scheduling, coordination, and
visibility, but it would make Senna operationally heavier and introduce a new
availability dependency.

An optional broker mode would split behavior between brokered and brokerless
deployments. That split would make correctness, tests, and documentation harder
to maintain.
