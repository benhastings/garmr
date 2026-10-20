-- garmr's tables. garmr never creates them: clickhouse-observability-stack's schema Job applies this file as
-- otel_admin, and garmr's integration tests apply it to a throwaway server. Every statement is idempotent.
--
-- State lives in KeeperMap (the server needs a Keeper and keeper_map_path_prefix), which garmr writes in strict
-- mode: an INSERT of a key that exists is refused, and every update is a compare-and-set on version. History is
-- append-only MergeTree.
--
-- garmr's user needs SELECT, INSERT, ALTER UPDATE and ALTER DELETE on garmr.*.

CREATE DATABASE IF NOT EXISTS garmr;

-- One row per open incident, keyed by its Alertmanager group. Deleted when the incident resolves, after its
-- final row is written to garmr.incidents.
CREATE TABLE IF NOT EXISTS garmr.open_incidents
(
    `group_key` String,
    `incident_id` UUID,
    `status` LowCardinality(String),
    `title` String,
    `service_name` String,
    `receiver` String,
    `group_labels` Map(String, String),
    `common_labels` Map(String, String),
    -- The firing alerts, as JSON, for the Slack message.
    `alerts` String,
    `firing` UInt32,
    `opened_at` DateTime64(3, 'UTC'),
    `updated_at` DateTime64(3, 'UTC'),
    `acknowledged_by` String,
    `acknowledged_at` DateTime64(3, 'UTC'),
    `silenced_until` DateTime64(3, 'UTC'),
    `slack_channel` String,
    `slack_ts` String,
    `version` UInt64,
    -- The ID of the write that set version, so a compare-and-set can tell whether it won.
    `op` String
)
ENGINE = KeeperMap('/garmr/open_incidents', 10000)
PRIMARY KEY group_key;

-- Every alert in every notification garmr receives.
CREATE TABLE IF NOT EXISTS garmr.alert_events
(
    `received_at` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `incident_id` UUID,
    `group_key` String CODEC(ZSTD(1)),
    `receiver` LowCardinality(String),
    `status` LowCardinality(String),
    `alertname` LowCardinality(String),
    `service_name` LowCardinality(String),
    `fingerprint` String CODEC(ZSTD(1)),
    `labels` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `annotations` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `starts_at` DateTime64(3, 'UTC'),
    `ends_at` DateTime64(3, 'UTC'),
    `generator_url` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(received_at)
ORDER BY (alertname, received_at);

-- Every change to an incident: opened, updated, acknowledged, silenced, resolved.
CREATE TABLE IF NOT EXISTS garmr.incident_events
(
    `at` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `incident_id` UUID,
    `group_key` String CODEC(ZSTD(1)),
    `event` LowCardinality(String),
    `status` LowCardinality(String),
    -- Who caused it: alertmanager, or slack:<user ID>.
    `actor` String,
    `title` String CODEC(ZSTD(1)),
    `service_name` LowCardinality(String),
    `firing` UInt32,
    `detail` String CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(at)
ORDER BY (incident_id, at);

-- One row per resolved incident, for time to acknowledge and time to resolve.
CREATE TABLE IF NOT EXISTS garmr.incidents
(
    `incident_id` UUID,
    `group_key` String CODEC(ZSTD(1)),
    `title` String CODEC(ZSTD(1)),
    `service_name` LowCardinality(String),
    `receiver` LowCardinality(String),
    `group_labels` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    `opened_at` DateTime64(3, 'UTC'),
    `acknowledged_at` Nullable(DateTime64(3, 'UTC')),
    `acknowledged_by` String,
    `resolved_at` DateTime64(3, 'UTC'),
    -- alertmanager when its alerts resolved, slack:<user ID> when someone resolved it.
    `resolved_by` String,
    `max_firing` UInt32,
    `slack_channel` String,
    `slack_ts` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(opened_at)
ORDER BY (service_name, opened_at);

-- Every Slack API call garmr makes.
CREATE TABLE IF NOT EXISTS garmr.notifications
(
    `at` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `incident_id` UUID,
    `integration` LowCardinality(String),
    `method` LowCardinality(String),
    `channel` String,
    `ts` String,
    `ok` Bool,
    `error` String,
    `duration_ms` UInt32
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(at)
ORDER BY (incident_id, at);

-- Every enrichment query garmr runs for an incident, and what it found.
CREATE TABLE IF NOT EXISTS garmr.enrichments
(
    `at` DateTime64(3, 'UTC') CODEC(Delta(8), ZSTD(1)),
    `incident_id` UUID,
    `service_name` LowCardinality(String),
    `name` LowCardinality(String),
    `ok` Bool,
    `error` String,
    `duration_ms` UInt32,
    -- The rows it found, as JSON.
    `result` String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(at)
ORDER BY (incident_id, at);
