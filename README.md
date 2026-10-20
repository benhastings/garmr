# garmr

A ClickHouse-native incident layer. In Norse myth, Garmr is the hound that howls before things go wrong.

Rule evaluators like vmalert, Grafana and Alertmanager send garmr their alerts. garmr turns them into incidents, enriches each one from the telemetry already in ClickHouse, and opens one Slack thread per incident (plus email). Every alert, notification and acknowledgement is stored in ClickHouse, next to the signals that caused it.

garmr is built for [clickhouse-observability-stack](https://github.com/benhastings/clickhouse-observability-stack) and installs there as one node. Cerberus answers the queries; garmr raises the incidents.

> **Status:** v0. `garmr serve` sits behind Alertmanager: it keeps an incident per alert group in ClickHouse,
> one Slack thread each with acknowledge, silence and resolve, and enrichment from the telemetry. The SQL
> adapter for vmalert works too. The design, milestones and decisions are in [docs/design.md](docs/design.md); the
> research is in [clickhouse-observability-stack#149](https://github.com/benhastings/clickhouse-observability-stack/issues/149).

## Scope

**In:**

- **Ingest:** Alertmanager's v2 alerts API, so vmalert and Grafana switch with a URL change; Alertmanager's webhook, for the first version, which sits behind Alertmanager; and a generic webhook.
- **Dedup and correlation:** alerts become incidents through fingerprints, rules, and service-graph proximity read from ClickHouse.
- **Enrichment:** a new incident gets ClickHouse queries attached: the top new error log, an example failing trace, the anomaly baseline, and recent deploys.
- **Notification:** one Slack thread per incident, kept up to date, with acknowledge, silence and resolve. Email for the same incident.
- **History in ClickHouse:** alert events, incidents and notifications, so alert quality is a SQL query.
- **The vmalert SQL adapter:** a Prometheus-API endpoint that runs ClickHouse SQL rules, so vmalert rules can use ClickHouse functions and precomputed baselines.

**Out:**

- **Rule evaluation:** vmalert, or ClickHouse natively when it ships rules.
- **Paging and on-call schedules:** hand off to GoAlert or PagerDuty.
- **ML:** not until the statistical rules are exhausted.

## `garmr serve`: incidents from Alertmanager

Point an Alertmanager webhook receiver at `/api/v1/alertmanager`, with `send_resolved: true`:

```yaml
receivers:
  - name: garmr
    webhook_configs:
      - url: http://garmr:8080/api/v1/alertmanager
        send_resolved: true
route:
  routes:
    - receiver: garmr
      group_by: [alertname, service_name]
      continue: true
```

**What it does:**

- **One incident per Alertmanager group**, from its first firing alert until none fire. It's stored in the
  KeeperMap table `garmr.open_incidents`, and opening it claims the group, so even two replicas post only
  one thread. When the group fires again later, that's a new incident.
- **One Slack thread per incident.** The first message shows the incident as it is now, and garmr edits it on
  every change. Each change also gets a reply in the thread: alerts started or resolved, acknowledged,
  silenced, resolved and how long it lasted.
- **Buttons**, over Slack's Socket Mode, so garmr needs no public URL:
  - **Acknowledge** records who acknowledged it.
  - **Silence 1h** creates an Alertmanager silence on the group's labels.
  - **Resolve** closes the incident.
- **Enrichment.** When an incident with a `service_name` opens, garmr replies in its thread with that service's
  new error-log templates, its latest failing server trace, and its signals against their baseline, all read from
  the telemetry in ClickHouse. A query that fails, for instance because there's no baseline in this environment,
  is left out of the reply.
- **History** in ClickHouse: `garmr.alert_events`, `incident_events`, `incidents` (one row per resolved incident),
  `notifications` and `enrichments`, so time to acknowledge or resolve is a query:

  ```sql
  SELECT service_name, count() AS incidents,
         quantile(0.5)(dateDiff('minute', opened_at, acknowledged_at)) AS p50_minutes_to_ack,
         quantile(0.5)(dateDiff('minute', opened_at, resolved_at)) AS p50_minutes_to_resolve,
         countIf(acknowledged_at IS NULL) AS never_acknowledged
  FROM garmr.incidents WHERE opened_at > now() - INTERVAL 7 DAY
  GROUP BY service_name
  ```

Without Slack tokens, garmr still keeps incidents and history; it just doesn't post them.

**What it needs:**

- **ClickHouse with a Keeper** and `keeper_map_path_prefix` set. [`testdata/clickhouse/keeper.xml`](testdata/clickhouse/keeper.xml)
  configures an embedded one.
- **The tables in [`schema/garmr.sql`](schema/garmr.sql).** garmr never creates tables; apply the file yourself, as
  clickhouse-observability-stack's schema Job does.
- **A user** with `SELECT, INSERT, ALTER UPDATE, ALTER DELETE ON garmr.*`, plus `SELECT` on the telemetry it
  enriches from: `otel.*`, `otel_features.*` and `otel_baseline.*`.
- **For Slack, an app** with a bot token (`chat:write`, invited to the channel) and an app-level token
  (`connections:write`) with Socket Mode and Interactivity on. This manifest creates one:

  ```yaml
  display_information:
    name: garmr
  features:
    bot_user:
      display_name: garmr
      always_online: true
  oauth_config:
    scopes:
      bot: [chat:write]
  settings:
    interactivity:
      is_enabled: true
    socket_mode_enabled: true
  ```

**Flags** (each one can also be set with the environment variable in brackets):

| Flag | Default | |
| ---- | ------- | - |
| `-listen-address` (`GARMR_LISTEN_ADDRESS`) | `:8080` | serves `/api/v1/alertmanager`, `/metrics`, `/-/healthy` and `/-/ready`; ready means ClickHouse answers and, with Slack on, Socket Mode is connected |
| `-clickhouse-addr` (`CLICKHOUSE_ADDR`) | `localhost:9000` | native protocol |
| `-clickhouse-user` (`CLICKHOUSE_USER`) | `garmr` | |
| `-clickhouse-password-file` (`CLICKHOUSE_PASSWORD_FILE`) | | otherwise `CLICKHOUSE_PASSWORD` is read |
| `-database` (`GARMR_DATABASE`) | `garmr` | where `schema/garmr.sql`'s tables are |
| `-slack-bot-token-file` (`SLACK_BOT_TOKEN_FILE`) | | `xoxb-`; otherwise `SLACK_BOT_TOKEN` is read. Without one, nothing goes to Slack |
| `-slack-app-token-file` (`SLACK_APP_TOKEN_FILE`) | | `xapp-`; otherwise `SLACK_APP_TOKEN` is read |
| `-slack-channel` (`SLACK_CHANNEL`) | | the channel ID, `C0…` |
| `-alertmanager-url` (`GARMR_ALERTMANAGER_URL`) | | for the silence button; without it the button reports an error |
| `-trace-url` (`GARMR_TRACE_URL`) | | links the failing trace: `{trace_id}` is replaced |
| `-silence-duration` | `1h` | |
| `-log-format` (`GARMR_LOG_FORMAT`) | `json` | or `text` |

**Metrics:** `garmr_notifications_received_total{outcome}`, `garmr_incidents_opened_total`,
`garmr_incidents_resolved_total{by}`, `garmr_slack_requests_total{method,outcome}`,
`garmr_enrichments_total{name,outcome}`, `garmr_actions_total{action,outcome}` and `garmr_open_incidents`.
A failure answers the webhook with 500, so Alertmanager retries it and counts it in `alertmanager_notifications_failed_total`.

## `garmr sql-adapter`: ClickHouse SQL rules for vmalert

vmalert evaluates rules through the Prometheus HTTP API. `garmr sql-adapter` serves that API (`/api/v1/query` and `/api/v1/query_range`) but answers each query by running it as **ClickHouse SQL**. A rule can then use anything ClickHouse can compute, such as quantiles over any column, `seriesOutliersDetectTukey`, or precomputed baselines, while vmalert still owns `for:`, labels, annotation templates, sending to Alertmanager, and alert state.

Run vmalert with `-rule.validateExpressions=false`, so it passes the SQL through instead of parsing it as PromQL, and point `-datasource.url` at the adapter:

```yaml
groups:
  - name: sql
    interval: 1m
    rules:
      - alert: ServerErrorRatioHigh
        expr: |
          SELECT ServiceName AS service_name,
                 countIf(StatusCode = 'Error') / count() AS value
          FROM otel.otel_traces
          WHERE SpanKind = 'Server'
            AND Timestamp BETWEEN {time:DateTime64(3, 'UTC')} - INTERVAL 5 MINUTE AND {time:DateTime64(3, 'UTC')}
          GROUP BY service_name
          HAVING count() > 10 AND value > 0.05
        for: 10m
        annotations:
          summary: "{{ $labels.service_name }} errors on {{ $value | humanizePercentage }} of server spans"
```

**The rule contract:**

- **Value and labels.** The column named `value` is the sample value. It must be numeric: any int, float, decimal or bool type, Nullable or not. Every other column is a label, and a NULL or empty label is dropped. A row whose value is NULL is no sample.
- **Unique label sets.** Two rows with the same labels are an error, so group by every label column.
- **Parameters.** The evaluation time and resolution are ClickHouse query parameters, bound by the server and never spliced into the text: `{time:DateTime64(3, 'UTC')}` and `{step:UInt32}` (seconds). A rule may use either, both or neither.
- **Read only.** Every query runs with `readonly=2`, so neither writes nor DDL are possible whatever the user's grants are, and a rule can't turn this off. Give the adapter a SELECT-only user anyway; in clickhouse-observability-stack that's `otel_reader`.
- **Timeouts.** Every evaluation is cancelled after `-query-timeout` (30s by default), and the server also stops it a few seconds later.
- **Errors.** A ClickHouse error is answered as a Prometheus `execution` error (422), a malformed result as `bad_data` (400), and a timeout as `timeout` (503). vmalert then marks the rule `health: err` with the message, and increments `vmalert_alerting_rules_errors_total`, so a broken rule is visible and can be alerted on.
- **Replay.** `query_range` runs the query once per step, up to 11,000 points, which is enough for vmalert's `-replay` mode.

**Flags** (each one can also be set with the environment variable in brackets):

| Flag | Default | |
| ---- | ------- | - |
| `-listen-address` (`GARMR_LISTEN_ADDRESS`) | `:8080` | serves `/api/v1/query`, `/api/v1/query_range`, `/-/healthy` and `/-/ready`; ready means ClickHouse answers a ping |
| `-clickhouse-addr` (`CLICKHOUSE_ADDR`) | `localhost:9000` | native protocol |
| `-clickhouse-database` (`CLICKHOUSE_DATABASE`) | `otel` | the default database for unqualified table names |
| `-clickhouse-user` (`CLICKHOUSE_USER`) | `otel_reader` | |
| `-clickhouse-password-file` (`CLICKHOUSE_PASSWORD_FILE`) | | otherwise `CLICKHOUSE_PASSWORD` is read |
| `-query-timeout` | `30s` | |
| `-log-format` (`GARMR_LOG_FORMAT`) | `json` | or `text` |

## Development

```sh
go test ./...                                         # unit tests
docker run -d --rm -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$PWD/testdata/clickhouse/keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
  clickhouse/clickhouse-server:26.9.5.2
GARMR_TEST_CLICKHOUSE_ADDR=localhost:9000 go test ./...   # plus the ClickHouse integration tests
```

The integration tests give each test its own copy of `schema/garmr.sql` in a database of its own. CI runs both, against the ClickHouse version clickhouse-observability-stack runs. Pushing a `v*` tag publishes `ghcr.io/benhastings/garmr:<tag>`.

## Decisions so far

The full list, with the reasoning, is in [docs/design.md](docs/design.md#decisions). In short:

- **Build rather than adopt Keep.** Keep is the reference design. Its single sign-on through Keycloak is enterprise-licensed, it needs its own database, and it doesn't store state in ClickHouse.
- **Written in Go**, as small binaries sized for a laptop.
- **ClickHouse is the only datastore:** KeeperMap for open incidents, MergeTree for history ([#2](https://github.com/benhastings/garmr/issues/2)).
- **v0 sits behind Alertmanager**, which keeps grouping and silences while garmr adds incidents, enrichment and Slack threads. **v1** takes alerts directly.
- **Slack's Socket Mode** for the buttons, so v0 needs no ingress.

## License

[MIT](LICENSE)
