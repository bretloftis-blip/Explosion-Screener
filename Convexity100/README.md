# Convexity100

Experimental call/put discovery and auditable paper return tracking. No brokerage integration or trading code. Started from Convexity100_Ready_to_Deploy.zip supplied in the prior conversation.

## What runs

One authenticated TLS Massive options WebSocket subscribes to market-wide minute aggregates (`AM.*`). It detects volume bursts in 0–10 calendar-day contracts, then selectively subscribes to sized NBBO quotes (`Q.O:...`). Both calls and puts use the same premium-upside logic: a put opportunity is a rising put premium, not a negative put return.

Two discovery paths:
- Early ignition: at least 2.5x recent minute volume and 8% premium momentum.
- Accumulation: at least 4x recent minute volume with premium momentum between -3% and +8%, seeking discovery before strong price acceleration.

Defaults require three prior recent observations, 35 contracts/minute, 120 daily contracts, premium $0.15–$8, spread/midpoint ≤40%, and premium gain from official open ≤75%. These are heuristic parameters, not fitted or validated predictions. No alert promises a large subsequent move; unsignaled, illiquid, instantaneous, and already-extended moves can be missed.

Fresh positive sized bid/ask observations confirm candidates. Locked quotes are accepted, crossed/zero-size/nonfinite/stale/out-of-order quotes rejected. Duplicate minute timestamps are replaced, not counted as independent observations. Discovery does not query private information.

## Executable-return convention

Paper entry cost per option share = observed entry ask + configured slippage + entry fee/100. Paper liquidation proceeds = observed bid - configured slippage - exit fee/100. Return = proceeds / entry cost - 1. +100%, +200%, +300% require respectively 2x, 3x, 4x cost-adjusted proceeds. Assumes one standard 100-share contract; adjusted contract deliverables are not validated.

Targets are observed after the alert and before its cutoff. The paper adverse barrier is -50% before the first +100% target. Once +100% hits, higher targets are tracked until cutoff. A target crossing is based on available quote size, not a confirmed order fill. Latency, fill priority, cancellations and discontinuous prices may prevent execution. The raw target levels in alerts are gross reference levels; the appended cost-adjusted barriers govern stored outcomes.

XNYS session calendar determines next-session cutoff and holiday/early-close behavior; expiration can shorten the window. Late quotes do not rewrite earlier results. `STOP_FIRST`, `TIMEOUT`, and `COMPLETE` settle the record. SQLite retains target timestamps and maximum cost-adjusted observed return. One record per contract prevents duplicated alerts across restarts; repeated same-contract episodes are not supported yet.

## Local verification

Use Python 3.12:

```sh
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python -m unittest discover -s tests -v
python scanner.py --demo
```

The demo uses synthetic data and a separate database. It does not call Telegram or Massive. Normal start requires `MASSIVE_API_KEY`; set Telegram variables to enable delivery:

```sh
python scanner.py
python scanner.py --summary
python audit.py --db data/scanner.sqlite --csv data/audit.csv
```

## Private GitHub repository

The connected GitHub identity is `bretloftis-blip`. The available connector supports existing repository writes, but does not expose repository creation. The secure browser sign-in attempt timed out and authentication could not be verified. No Convexity100 repository was created or pushed in this run. The existing public Explosion-Screener repository was not modified.

Create a **private** repository named `Convexity100`, and allow the existing GitHub connection access to that repository. Upload the contents of this directory (not the containing ZIP) or use authenticated git:

```sh
git init -b main
git add .
git commit -m "Implement Convexity100 scanner and paper return tracking"
git remote add origin git@github.com:bretloftis-blip/Convexity100.git
git push -u origin main
```

Use GitHub's normal OAuth/SSH authentication. Never commit tokens or send credentials in chat. A test workflow is included; its remote execution is unverified until pushed.

## Railway deployment

1. Connect the private repository to Railway and create one service. Dockerfile and railway.json provide the build/start/restart configuration.
2. Attach a persistent volume at `/app/data`. Run **one replica** because SQLite and quote subscription ownership are local. Confirm the mounted directory is writable by container UID 10001; if not, fix volume permissions in the service setup.
3. In Railway Variables configure secrets `MASSIVE_API_KEY` and `TELEGRAM_BOT_TOKEN`, plus `TELEGRAM_CHAT_ID`. Set `FEED=realtime`, `DB_PATH=/app/data/scanner.sqlite`, and optionally `CAPTURE_PATH=/app/data/capture`. Keep tokens out of git, build arguments, public URLs and logs. Use `.env.example` for tunable thresholds only.
4. Massive account must be entitled to real-time options minute aggregates AND option quotes. Business wildcard discovery may require a Full Market expansion. Current published quote subscription limit is 1,000/connection; this application defaults to 500 and caps configuration at 950.
5. Create a Telegram bot using Telegram's official BotFather, start the bot from your intended chat, and set the bot token and correct chat ID securely in Railway. No credentials were supplied in this run; no real message has been sent.
6. Deploy. `/healthz` reports process liveness; `/readyz` returns 200 only while connected with a recent feed message. Readiness can be 503 outside market hours. Health endpoints reveal no signals or credentials and there are no web-admin endpoints.
7. Verify logs show auth_success, AM.* subscription, real events during market hours, and accepted quote subscriptions. If subscriptions are rejected, correct entitlement before relying on signals. Send a controlled test alert to verify the intended chat. Restart and confirm persistent signal/outbox records remain.
8. Back up SQLite using its online backup API, not a bare copy of a live WAL database. Monitor disk utilization and archive/delete old captures under your data retention policy. Set Railway budget alerts yourself; no hosting subscription or payment was initiated.

Telegram uses a persistent outbox, nonblocking delivery and retry backoff. Delivery is at least once: a crash after Telegram accepts a message but before the sent flag commits may duplicate it. Stored signals are read-only paper outcomes.

## Verification and remaining capabilities

See AUDIT.md for findings, tests, and exact limitations. Production connectivity and predictive performance are unverified. This implementation is a forward paper-data collection foundation, not an established 24–48-hour forecasting model.

Official references:
- https://massive.com/docs/websocket/options/quotes
- https://massive.com/docs/websocket/options/aggregates-per-minute
- https://core.telegram.org/bots/api
- https://docs.railway.com/builds/dockerfiles
- https://docs.railway.com/deployments/healthchecks
- https://docs.railway.com/volumes
