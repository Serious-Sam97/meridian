# 2. Redis data model and atomic state transitions

- Status: accepted
- Date: 2026-09-24

## Context

A job moves through these states: `waiting` → `active` → `completed` | `failed`,
with detours through `delayed` (scheduled or backing off before a retry). Every
transition touches several keys. If a process crashes between two commands, the
job must never be lost, duplicated in two states, or left in an unknown state.

## Decision

Every key of a queue shares the prefix `meridian:{<queue>}:`. The braces are a
Redis Cluster hash tag, so all keys of one queue live in the same slot and can be
used together in a single script.

| Key | Type | Purpose |
|---|---|---|
| `id` | string | `INCR` counter for job ids and FIFO ordering |
| `job:<id>` | hash | Job payload, options and state metadata |
| `job:<id>:lock` | string | Lock token, with a TTL, held by the worker processing the job |
| `wait` | sorted set | Ready jobs. Score = `priority * 2^32 + seq`, so lower priority numbers run first and jobs with the same priority run FIFO |
| `delayed` | sorted set | Scheduled jobs. Score = timestamp (ms) at which the job becomes ready |
| `active` | sorted set | Jobs being processed. Score = time the job started |
| `completed` / `failed` | sorted set | Finished jobs. Score = finish time, used for retention trimming |
| `marker` | list | Wake-up signal for idle workers (see below) |
| `meta` | hash | Queue flags such as `paused` |
| `events` | stream | Job lifecycle events for the dashboard, capped with `MAXLEN ~` |

Every state transition is **one Lua script**. Scripts run atomically, so a
transition either happens completely or not at all.

**Blocking fetch.** A Lua script cannot block, and `BZPOPMIN` on `wait` would take
a job without locking it. Workers therefore use a two-step loop:

1. Run `moveToActive`. It promotes due delayed jobs, pops the head of `wait`,
   moves it to `active` and sets the lock in one step. When there is no job, it
   returns the time of the next delayed job.
2. When there is no job, block on `BLPOP marker` with a timeout of
   `min(blockTimeout, nextDelayedAt - now)`. `add` pushes to `marker`, so an idle
   worker wakes as soon as work arrives.

A marker is only a hint, never a job. Spurious wake-ups are harmless, and the list
is trimmed so it cannot grow without bound.

**Clock.** Scripts read time from `redis.call('TIME')`, not from the client.
Delays, lock expiry and stalled detection then use a single clock, and clock skew
between worker machines does not matter.

## Alternatives considered

- **Lists + `BLMOVE wait active`** (the classic reliable-queue pattern). This
  blocks natively, but has no priorities. The lock is also set in a second
  command after the move, so a crash between the two leaves a job in `active`
  with no lock. The stalled-job checker would recover it, but only after a delay.
- **Postgres with `SELECT ... FOR UPDATE SKIP LOCKED`** (pg-boss). This gives
  stronger durability, but lower throughput and slower wake-ups. It could be
  added later as a second backend behind the same interface.

## Consequences

- Each transition is atomic and costs one round trip.
- Queue logic lives partly in Lua, which is harder to debug and needs integration
  tests against a real Redis. There are no mocks in the test suite for this reason.
- A single queue cannot be spread across cluster nodes. To scale out, use more
  queues, not bigger ones.
