# garmr

A ClickHouse-native incident layer. In Norse myth, Garmr is the hound that howls before things go wrong.

Rule evaluators like vmalert, Grafana and Alertmanager send garmr their alerts. garmr turns them into incidents, enriches each one from the telemetry already in ClickHouse, and opens one Slack thread per incident (plus email). Every alert, notification and acknowledgement is stored in ClickHouse, next to the signals that caused it.

garmr is built for [clickhouse-observability-stack](https://github.com/benhastings/clickhouse-observability-stack) and installs there as one node. Cerberus answers the queries; garmr raises the incidents.

> **Status:** design. There is no code yet. The background and the research are in
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

## Decisions so far

- **Build rather than adopt Keep.** Keep is the reference design. Its single sign-on through Keycloak is enterprise-licensed, it needs its own database, and it doesn't store state in ClickHouse.
- **Written in Go**, as small binaries sized for a laptop.
- **First version (v0) sits behind Alertmanager**, which keeps grouping and silences while garmr adds incidents, enrichment and Slack threads. **v1** takes alerts directly.

## License

[MIT](LICENSE)
