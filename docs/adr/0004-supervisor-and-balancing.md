# 4. Supervisor: process pools balanced by workload

- Status: accepted
- Date: 2026-09-24

## Context

A `Worker` runs jobs concurrently within one process, which suits I/O-bound handlers.
Production deployments also need:

- **Isolation**: a handler that leaks memory, crashes, or blocks the event loop must
  not take down the others.
- **Scaling with demand**: a burst of 50,000 emails should get more processes than a
  queue that receives one job an hour, without someone tuning the numbers by hand.

Laravel Horizon solves this with supervisors that manage pools of worker processes
and rebalance them between queues. Meridian follows the same model.

## Decision

`@meridian/supervisor` runs one **pool of child processes per queue**. Each child
imports a processor module and runs a single `Worker` with a configured concurrency.
The supervisor never runs job code itself.

**Balancing** runs every `balanceInterval` and is a pure function
`balance(queues, current, options) → allocation`, so it can be unit tested without
processes or Redis:

- `simple` splits `maxProcesses` evenly between queues.
- `auto` gives each queue `minProcesses`, then splits the remaining capacity in
  proportion to each queue's **time to clear**: `waiting × avgRuntime`, using the
  last minutes of metrics. A busy queue with slow jobs gets more processes than a
  busy queue with fast ones. When every queue is idle, all of them shrink to
  `minProcesses`.
- A queue changes by at most `maxShift` processes per interval. This damps
  oscillation, and it matters because starting a process is not free.
- The total never exceeds `maxProcesses`. Scale-downs are applied before
  scale-ups, so capacity freed by one queue can go to another in the same tick.

**Process lifecycle**

- To scale down, the supervisor sends a `shutdown` message. The child calls
  `worker.close({ timeout })`, which finishes or releases its jobs (ADR 0003), then
  exits. The child is killed if it has not exited after a grace period.
- A child that exits unexpectedly is restarted with exponential backoff. Its jobs
  are recovered by the stalled-job checker.
- A child also exits when its IPC channel closes, so orphans do not keep running
  after the supervisor is killed.

**Visibility**: the supervisor writes a heartbeat with its pools and process counts
to `<prefix>:supervisors`. The dashboard shows supervisors whose heartbeat is recent.

## Alternatives considered

- **Worker threads instead of processes**: cheaper to start, but a native crash or an
  out-of-memory error takes down the whole process, and memory limits per thread are
  weaker. Processes match Horizon's isolation model.
- **Scaling concurrency within one process**: no isolation, and it does not help
  CPU-bound handlers.
- **Leaving scaling to Kubernetes HPA**: complementary. HPA scales machines or pods on
  coarse signals, while the supervisor divides a pod's capacity between queues using
  queue-level signals the platform cannot see.

## Consequences

- Processor code must live in a module the child can import. It cannot be passed as
  a closure. This is the same constraint Horizon has with job classes.
- The supervisor process is a single point of management for its pool, but not for
  the jobs. If it dies, the children exit and the jobs are recovered by any other
  worker.
