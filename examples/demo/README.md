# Demo

A supervisor, the dashboard and a producer that generates realistic traffic:

- **emails**: a steady 10 jobs/s, with occasional transient SMTP errors (retried with
  exponential backoff) and invalid addresses (`UnrecoverableError`, failed immediately)
- **images**: a burst of 150 slow jobs every 30 seconds
- **reports**: a slow delayed job every 15 seconds

```bash
npm run redis:up
npm run demo        # builds the packages, then starts everything
```

Open http://127.0.0.1:3000. On each image burst, the supervisor moves `images` up to 6
processes, one per balancing round, then back down to 1 once the burst is done:

```
[supervisor] processes: emails=1 images=2 reports=1
[supervisor] processes: emails=1 images=3 reports=1
...
[supervisor] processes: emails=1 images=6 reports=1
[supervisor] processes: emails=1 images=5 reports=1
```

Press Ctrl+C to stop. The supervisor lets running jobs finish before exiting.
