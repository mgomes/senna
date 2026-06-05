# ADR-0002: Store Lua scripts as separate files

## Status

Accepted, 2026-06-05

## Context

Senna uses Lua for atomic Redis and Valkey operations that cannot be expressed as
safe multi-command client sequences. These scripts cover worker fetch,
acknowledgement, retry, orphan recovery, batch completion, unique enqueue,
periodic enqueue, and distributed rate limiters.

The scripts are production code. They need normal review, editor support,
diffable changes, and ownership near the package that uses them. Inline Go
string literals make that code harder to read and easier to accidentally damage
while editing unrelated Go code.

## Decision

Production Lua scripts will live in dedicated `.lua` files and be embedded or
loaded through small Go registries.

Go code may define typed handles and wrappers around scripts, but it must not
store production Lua bodies in inline string constants.

## Consequences

Lua changes are easier to review, format, search, and test independently from Go
control flow.

Scripts can live near the package that owns the behavior while still sharing a
consistent loading pattern.

Maintainers must keep script file names, embed declarations, and exported script
handles synchronized. Even small production scripts get separate files.

## Alternatives Considered

Inline Go strings would reduce the number of files, but they make script logic
harder to read, diff, and validate.

Generated Lua could help if Senna later needs a real script DSL, but generation
would add build complexity without solving a current problem.
