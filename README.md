# garmr

A ClickHouse-native incident layer. In Norse myth, Garmr is the hound that howls before things go wrong.

Rule evaluators like vmalert, Grafana and Alertmanager send garmr their alerts. garmr turns them into incidents, enriches each one from the telemetry already in ClickHouse, and opens one Slack thread per incident (plus email). Every alert, notification and acknowledgement is stored in ClickHouse, next to the signals that caused it.

garmr is built for [clickhouse-observability-stack](https://github.com/benhastings/clickhouse-observability-stack) and installs there as one node. Cerberus answers the queries; garmr raises the incidents.

> **Status:** early. The SQL adapter for vmalert works (below), and the incident layer is in design.
> The background and the research are in
> [clickhouse-observability-stack#149](https://github.com/benhastings/clickhouse-observability-stack/issues/149).

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
docker run -d --rm -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server:26.9.5.2
GARMR_TEST_CLICKHOUSE_ADDR=localhost:9000 go test ./...   # plus the ClickHouse integration tests
```

CI runs both, against the ClickHouse version clickhouse-observability-stack runs. Pushing a `v*` tag publishes `ghcr.io/benhastings/garmr:<tag>`.

## Decisions so far

- **Build rather than adopt Keep.** Keep is the reference design. Its single sign-on through Keycloak is enterprise-licensed, it needs its own database, and it doesn't store state in ClickHouse.
- **Written in Go**, as small binaries sized for a laptop.
- **First version (v0) sits behind Alertmanager**, which keeps grouping and silences while garmr adds incidents, enrichment and Slack threads. **v1** takes alerts directly.

## License

[MIT](LICENSE)
