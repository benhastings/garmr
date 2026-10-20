# garmr design: the ClickHouse-native incident layer

Status: v0 accepted, 2026-10-19. This is the design doc that
[garmr#3](https://github.com/benhastings/garmr/issues/3) asks for. The research behind it is in
[clickhouse-observability-stack#149](https://github.com/benhastings/clickhouse-observability-stack/issues/149), and the
state-store spike is in [garmr#2](https://github.com/benhastings/garmr/issues/2).

## What garmr is

Alertmanager (or later vmalert or Grafana directly) sends garmr alerts. garmr turns them into **incidents**,
attaches what ClickHouse already knows about the failing service, keeps **one Slack thread per incident**, and
writes every alert, incident change and notification to ClickHouse, so alert quality is a SQL query.

It doesn't evaluate rules (vmalert does), page people or hold on-call schedules (GoAlert or PagerDuty do), or
use ML.

## Milestones

| | Ingest | Grouping and silences | Incidents | Notification | Enrichment |
| - | ------ | --------------------- | --------- | ------------ | ---------- |
| **v0** (this doc) | Alertmanager webhook | Alertmanager's | one per Alertmanager group | Slack thread, with acknowledge, silence and resolve | top new error logs, an example failing trace, the anomaly baseline |
| v0.1 | + generic webhook | | | + email | + deploy markers |
| v1 | + Alertmanager v2 `POST /api/v2/alerts`, so vmalert and Grafana point at garmr | garmr's own: fingerprints, CEL-style rules, service-graph proximity | correlated across groups | + hand-off to GoAlert or PagerDuty | |

v0 sits **behind** Alertmanager. Alertmanager keeps routing, grouping, inhibition and silences, and garmr adds
what it lacks. The rest of this doc is v0, plus the decisions v1 must not be blocked by.

## Architecture

```text
vmalert ──▶ Alertmanager ──webhook──▶ garmr serve ──▶ ClickHouse   garmr.* state (KeeperMap) and history (MergeTree)
                 ▲                      │   │    ╰──▶ ClickHouse   otel.*, otel_features.*, otel_baseline.* (enrichment, read only)
                 │                      │   ╰──────▶ Slack Web API (chat.postMessage, chat.update)
                 ╰─── silences ─────────┴◀────────── Slack Socket Mode (button clicks)
```

`garmr serve` is one Go binary. ClickHouse is its only datastore. It needs no inbound connection from the
internet: Slack's button clicks arrive over **Socket Mode**, a WebSocket that garmr opens to Slack, so the stack
needs no ingress or public URL for v0. Slack's HTTP interactivity endpoint can replace Socket Mode later
without changing anything else.

### Ingest

`POST /api/v1/alertmanager` takes Alertmanager's webhook payload (version 4). Alertmanager routes to it with
`send_resolved: true`, grouped by `alertname` and `service_name`, the same groups as the stack's Slack route
today.

garmr answers 200 once the notification is applied: state written, history queued, Slack updated. A failure
answers 500, and Alertmanager retries. Applying a notification is idempotent (a retry that finds the incident
already in that state changes nothing), so retries are safe. Enrichment runs after the response, so a slow
query never delays the webhook.

### Incidents

**Identity.** An incident is one Alertmanager group (`groupKey`) from the moment its first alert fires to the
moment it resolves. The incident's ID is a fresh UUID. If the same group fires again later, that's a new
incident with a new thread.

**States.**

```text
            ack button             resolve button, or Alertmanager says resolved
  firing ─────────────▶ acknowledged ─────────────────────────────▶ resolved (deleted from state, kept in history)
     ╰────────────────────────────────────────────────────────────▶
```

Silence isn't a state. The silence button creates an Alertmanager silence for the group's labels (1 hour by
default), so Alertmanager stops notifying. The incident stays open until its alerts resolve or someone
resolves it. When someone resolves it by hand while its alerts still fire, Alertmanager's next notification
for the group opens a new incident. That's deliberate: the problem came back.

**Concurrency.** Each replica serializes the notifications and clicks for one group in memory. Across
replicas, every state write is guarded:

- **Opening** is a strict-mode KeeperMap `INSERT` of the group key. If two replicas race, one insert lands and
  the other is refused (`Node exists`), so exactly one replica posts the Slack thread.
- **Every other change** is a compare-and-set: `ALTER TABLE ... UPDATE ..., version = version + 1, op = {op} WHERE
  group_key = {k} AND version = {v}`, then a read back of `op`. A stale writer matches no row, sees someone
  else's `op`, re-reads and retries.

### Data model

The schema is [`schema/garmr.sql`](../schema/garmr.sql). garmr never creates tables: the stack's schema Job
applies that file as `otel_admin`, as it does every other table, and garmr's tests apply it to a throwaway
server.

**State, in KeeperMap** (strict mode, `async_insert = 0`). It's small and mutable, and point reads take 2 ms:

| Table | Key | Holds |
| ----- | --- | ----- |
| `garmr.open_incidents` | `group_key` | incident ID, status, title, service, group labels, firing alerts, who acknowledged it, the Slack channel and thread `ts`, version |

When an incident resolves, garmr writes its final row to history and deletes it from `open_incidents`, so the
table holds only open incidents, and `keys_limit` caps it at 10,000.

**History, in MergeTree** (append only, through async inserts):

| Table | One row per | Answers |
| ----- | ----------- | ------- |
| `garmr.alert_events` | alert in each notification garmr receives | what fired, how often, which rules flap |
| `garmr.incident_events` | incident change: opened, updated, acknowledged, silenced, resolved | who did what, when |
| `garmr.incidents` | resolved incident | time to acknowledge, time to resolve, incidents nobody acknowledged |
| `garmr.notifications` | Slack call | delivery failures and latency |
| `garmr.enrichments` | enrichment query run for an incident | what responders were shown, which queries fail or are slow |

### Enrichment

When an incident opens and has a `service_name` label, garmr runs these queries for that service and posts the
results as the first reply in the thread. Each query has a 10 s timeout, and a failure is recorded in
`garmr.enrichments` and left out of the message, so a missing table (an environment without baselines) costs
one line and doesn't fail the incident.

| Enrichment | Source | Shows |
| ---------- | ------ | ----- |
| New error logs | `otel_features.log_templates` | the error and warning templates logged in the last 15 minutes that weren't logged in the 24 hours before, by count, with an example line; or, if none are new, the most frequent ones |
| Failing trace | `otel.otel_traces` | the latest server span with `StatusCode = 'Error'` in the last 15 minutes: trace ID, span name, status message and duration, linked to Grafana when `-trace-url` is set |
| Baseline | `otel_baseline.anomaly` | each signal's observed value against its median and robust z-score |

Deploy markers wait for v0.1, since the stack doesn't record deploys yet.

### Slack

A message has four parts. The **root message**: status, title, service, firing count, each alert's summary, who
acknowledged it, and the buttons. It's edited on every change, so a glance at the channel shows the current
state. **Thread replies** record each change: more alerts firing, some resolved, acknowledged by, silenced
until, resolved after. **The first reply** is the enrichment. **Buttons**: Acknowledge, Silence 1h, Resolve.
Each carries the incident ID, so a button on an old incident's message can't act on a newer one.

Slack is optional. Without tokens, garmr still keeps incidents and history, which is what the stack's e2e
test runs.

### Self-monitoring

- `GET /metrics` exposes `garmr_notifications_received_total`, `garmr_incidents_opened_total`,
  `garmr_slack_requests_total{method,outcome}`, `garmr_enrichments_total{name,outcome}` and
  `garmr_open_incidents`.
- `/-/healthy` is liveness. `/-/ready` means ClickHouse answers a ping and, when Slack is on, Socket Mode is
  connected.
- A Slack failure answers the webhook 500, so Alertmanager's own `alertmanager_notifications_failed_total` for
  the garmr receiver goes up. The stack already alerts on that.

## Running it

**In clickhouse-observability-stack**, garmr is a node named `garmr`, beside `garmr-sql-adapter`. It needs these
stack changes, tracked there:

- **A Keeper.** The embedded Keeper in the `ClickHouseInstallation`, about 70 MiB, plus
  `keeper_map_path_prefix`. A `ClickHouseKeeperInstallation` is the HA option.
- **The schema** in the schema Job: `schema/garmr.sql`.
- **A user, `garmr`**: `SELECT, INSERT, ALTER UPDATE, ALTER DELETE ON garmr.*`, plus `SELECT` on `otel.*`,
  `otel_features.*` and `otel_baseline.*`, with its password in the Secret `clickhouse-garmr`.
- **Alertmanager** routes every alert but the Watchdog to garmr's webhook, with `continue: true`. Alertmanager's
  own Slack receiver stays as an alternative, but an environment turns on one or the other.
- **Slack tokens** in the Secret `garmr-slack` (keys `bot-token` and `app-token`), which nothing in git creates,
  as with `alertmanager-slack`.

**Footprint:** a 64 MiB memory limit, like the SQL adapter, plus the Keeper's 70 MiB in ClickHouse.

**HA:** v0 runs one replica. The claim and compare-and-set make two replicas safe for state, but both would
hold a Socket Mode connection, and Slack sends each click to only one of them. That's fine, because either can
act on any incident. Running two replicas is a v1 test, not a v0 promise.

**Upgrades:** history tables only gain columns (`ADD COLUMN IF NOT EXISTS`). `open_incidents` can be dropped
and recreated at any upgrade, losing only the link between open incidents and their threads. The next
notification then opens a new thread.

**The stack's e2e test asserts** that an alert posted to Alertmanager opens an incident in
`garmr.open_incidents` with its alert events, and that resolving it moves it to `garmr.incidents`.

## Decisions

- **Socket Mode for Slack in v0:** no ingress, no public URL, no request signing to get right. HTTP
  interactivity comes with the stack's ingress.
- **One incident per Alertmanager group** in v0: Alertmanager has already grouped the alerts, and correlating
  across groups is v1's job.
- **Resolving by hand closes the incident**, even if alerts still fire; the next notification opens a new
  incident. The alternative, a resolved incident that reopens, hides how often a problem comes back.
- **Silence through Alertmanager** while it owns silences; garmr only records who silenced what, for how long.
- **ClickHouse is the only store** (garmr#2): KeeperMap for state, MergeTree for history.
- **The stack owns the schema**; garmr's repo holds the file and tests against it.
