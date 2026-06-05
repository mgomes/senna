# ADR-0003: Provide at-least-once delivery

## Status

Accepted, 2026-06-05

## Context

Senna moves jobs through Redis and Valkey state so workers can claim, execute,
acknowledge, retry, and recover jobs after interruption. A worker can crash after
a handler performs user-visible work but before Senna records the job as
finished. Network failures, server failover, process shutdown, and finalization
errors can create the same uncertainty.

Preventing duplicate execution would require Senna to coordinate with each
handler's external side effects. The library cannot make a database write, email
send, API call, or filesystem change exactly-once on behalf of application code.

## Decision

Senna guarantees at-least-once delivery, not exactly-once delivery.

A job that has been handed to a handler may be delivered again after failures or
recovery. Job handlers must therefore be idempotent or use application-level
deduplication when duplicate side effects matter.

## Consequences

Senna's reliability model favors avoiding job loss over avoiding duplicate
execution.

Retry and orphan-recovery behavior remain core parts of the worker design.

Documentation and public APIs must not promise exactly-once delivery. Examples
should encourage idempotent handlers for side-effecting jobs.

Operators and application authors must account for duplicates when jobs perform
external side effects.

## Alternatives Considered

At-most-once delivery would avoid duplicate handler calls, but it would allow job
loss when a worker dies or a network failure interrupts finalization.

Exactly-once delivery is not a realistic library-level guarantee for arbitrary
application side effects.
