# Audit and validation

## Source
Original ZIP: Convexity100_Ready_to_Deploy.zip. Original scanner.py, audit.py and tests reviewed; amended directly. Original implementation already had Massive WebSocket discovery, calls/puts, raw ask-to-bid target math, SQLite and simple Telegram delivery.

## Fixed
- Duplicate/updating minute bars previously inflated observation counts.
- Quote size was ignored; now minimum bid/ask sizes required.
- Quote age tightened from 75 seconds to configurable 5 seconds.
- NaN prices, crossed quotes, replayed and out-of-order quotes rejected.
- Cost-adjusted +100/+200/+300 tracking added with slippage and per-contract fees.
- Target and stop events now create Telegram outbox messages.
- Telegram delivery failures retained and retried without logging token-containing exceptions.
- Successful +100 records now settle at cutoff rather than remaining active indefinitely.
- Subscription admission bounded before enqueueing; stale reconnect candidate state cleared.
- Holiday and early-close cutoff uses XNYS calendar.
- Persistent outbox and database schema migration added.
- Accumulation path adds discovery before the default momentum gate.
- Non-root Docker image, Railway config, health/readiness endpoints, pinned dependencies and GitHub CI added.
- Audit denominator excludes still-active HIT_100 records.

## Verified locally
11 unit/integration tests passed on Python 3.12:
- contract parser; calls and puts;
- signal confirmation, gross and cost-adjusted return barriers;
- all three targets and durable records after reopening database;
- mocked Telegram delivery of entry and target events;
- flat-price accumulation discovery;
- duplicate aggregates and capacity admission;
- stale/zero-size/NaN/out-of-order quote rejection and stop-before-target;
- Thanksgiving holiday and half-day close;
- HTTP liveness/readiness separation;
- mocked Massive WebSocket authentication, AM.* subscription, selective Q subscription and signal persistence;
- wide-spread rejection and no repeated alerts.
Demo and Python compile checks passed. External calls are mocked in integration tests. Docker binary unavailable, so the container was not built or run. A Railway deployment was not performed.

## External blockers
- GitHub connector cannot create repositories. Browser sign-in was started through secure authentication; it timed out and subsequent browser attachment failed. Final authentication state unknown. Repository does not exist as a verified result of this run.
- MASSIVE_API_KEY missing; real-time options entitlement and production bandwidth untested.
- TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID missing; actual API delivery/chat recipient untested.
- No Railway session/token available; no deployed service, persistent-volume mount or public endpoint verified.

## Capability limitations
- No historical NBBO dataset, all-market mover audit, walk-forward validation, calibrated probabilities or verified forecast hit rate. No evidence of 24–48-hour advance detection accuracy.
- No Greeks/IV/OI/underlying-relative-strength enrichment, news catalysts, earnings calendar or event anticipation. Current detection is intraday volume/price-based.
- Minute bars need three baseline observations; startup gaps and rapid first-minute jumps can be missed. Fixed thresholds and the ≤75% open-gain gate can exclude continuation movers.
- Only 0–10 calendar-day expirations and configured price/volume bands are scanned. Market-wide minute discovery is broader than selective quote coverage; active tracking is subscription-limited.
- No sequence-gap backfill across feed outages. Outcomes during disconnects are unobserved; TIMEOUT cannot be treated as proof a barrier was never touched. Readiness measures recent connection traffic, not instrument-level quote completeness.
- One record per ticker permanently suppresses further episodes. A future migration should introduce signal IDs for repeated episodes.
- No fill simulation with order book depth, execution latency, adjusted deliverables or position quantity beyond minimum quote-size filtering. Standard 100-share multiplier is assumed.
- Signal write and outbox enqueue are separate transactions; a process crash between them can leave a recorded event without a corresponding alert. Delivery after enqueue is durable at least once, with possible duplicates.
- No automatic capture retention, disk limits, remote metrics, alert health escalation, live schema verification, or stress-test throughput benchmark.
- Minute aggregate revisions replace observations but are not independently re-evaluated within that same timestamp. Monotonic ordering avoids double counting at the expense of missing an intraminute revision trigger.
- Market calendar is XNYS, not contract-specific options exchange hours. After-hours/extended-hours option contracts are not independently modeled.

## Acceptance before operational use
Verify private GitHub repo and remote CI; build/run container; securely configure provider secrets; confirm Massive permissions and real-time messages; test the intended Telegram chat; restart with mounted persistence; record instrument-level feed gaps; run load/latency checks during active trading. Collect forward out-of-sample alerts and complete bid/ask paths, then calculate target-before-adverse-move rates with uncertainty and missed-mover coverage. Do not present priority scores as probabilities.

No live trades were placed. No brokerage credentials were requested or used.
