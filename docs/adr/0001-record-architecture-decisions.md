# 1. Record architecture decisions

- Status: accepted
- Date: 2026-09-24

## Context

Meridian has several non-obvious design choices (data layout in Redis, delivery
guarantees, how locks work). A reader of the code sees *what* was built, but not
*why*, or which alternatives were rejected.

## Decision

Significant decisions are recorded as Architecture Decision Records in `docs/adr`,
following Michael Nygard's format: context, decision, consequences. An ADR is never
edited after acceptance. When a decision changes, a new ADR supersedes it.

## Consequences

- Design reasoning lives next to the code and is reviewed in the same PRs.
- Writing an ADR adds some effort to each significant change.
