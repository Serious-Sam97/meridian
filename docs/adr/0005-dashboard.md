# 5. Dashboard: a framework-free handler with SSE and no build step

- Status: accepted
- Date: 2026-09-24

## Context

The dashboard has to be embeddable in whatever HTTP stack an application already uses
(Express, Fastify, plain `node:http`), and also runnable on its own. It shows job
payloads and can delete jobs, so it is a sensitive surface. It must stay cheap: people
leave it open in a tab all day.

## Decision

**Server**

- `createDashboard()` returns a plain `(req, res, next?)` handler, with no framework
  dependency. The same function works with `http.createServer` and as Express or
  Connect middleware. Routes are relative, so the dashboard can be mounted under any
  path.
- A tiny router of about 80 lines handles `/:param` paths and JSON responses.

**Live updates use Server-Sent Events, not WebSockets**

- Updates flow in one direction only (server to browser). SSE works over plain HTTP,
  reconnects on its own, passes through proxies with `x-accel-buffering: no`, and
  needs no extra dependency.
- One `XREAD BLOCK` loop on a dedicated connection tails the events stream of every
  queue and fans the events out to all browsers. It runs only while at least one
  browser is connected. Each stream starts at an explicit id taken from the Redis
  clock, not `$`, because `XREAD` re-evaluates `$` on every call and would drop events
  added between two reads.

**Browser**

- Plain ES modules, with no bundler and no framework. The UI is three files, served
  as they are.
- Every piece of dynamic content goes through `textContent`, never `innerHTML`, because
  job payloads are user data. A test asserts this.
- A strict CSP: `script-src 'self'`, with no inline scripts and no third-party origins.

**Protection**

- An `authorize(req)` hook runs for embedded use. The CLI offers HTTP Basic auth,
  compared in constant time, and binds to `127.0.0.1` by default.
- Mutating requests must carry `x-meridian-request: 1`. Cross-origin forms and
  `fetch` without a CORS preflight cannot set custom headers, and the dashboard never
  answers a preflight, so this blocks CSRF without tokens or sessions.

## Consequences

- Nothing needs to be built or installed for the UI, and it has no dependencies to
  keep updated.
- A richer UI would eventually outgrow hand-written DOM code. A framework could then
  be introduced behind the same JSON API without changing the server.
- Supervisors show up only while their heartbeat is fresh (15s by default). A
  supervisor that was just killed disappears after that delay, not immediately.
