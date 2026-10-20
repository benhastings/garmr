package enrich

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/benhastings/garmr/internal/chtest"
	"github.com/benhastings/garmr/internal/incident"
)

var at = time.Date(2026, 10, 19, 14, 0, 0, 0, time.UTC)

// fixtures creates the columns the queries read from clickhouse-observability-stack's tables, in the test's
// own database, with payments failing and checkout healthy.
func fixtures(t *testing.T) (*Enricher, string) {
	t.Helper()
	conn := chtest.Conn(t)
	db := chtest.Database(t, conn)
	chtest.Exec(t, conn, strings.ReplaceAll(`
		CREATE TABLE db.log_templates (Minute DateTime, ServiceName LowCardinality(String), SeverityText LowCardinality(String),
			TemplateHash UInt64, Template String, Count UInt64, Example String)
		ENGINE = ReplacingMergeTree ORDER BY (ServiceName, Minute, SeverityText, TemplateHash);
		INSERT INTO db.log_templates VALUES
			('2026-10-19 13:50:00', 'payments', 'ERROR', 1, 'card <num> declined', 40, 'card 4242 declined'),
			('2026-10-19 13:51:00', 'payments', 'ERROR', 2, 'timeout after <num>ms', 9, 'timeout after 3000ms'),
			('2026-10-19 13:52:00', 'payments', 'INFO', 3, 'charged <num>', 500, 'charged 12'),
			('2026-10-19 12:00:00', 'payments', 'ERROR', 1, 'card <num> declined', 2, 'card 1111 declined'),
			('2026-10-19 13:50:00', 'checkout', 'ERROR', 4, 'cart empty', 1, 'cart empty');

		CREATE TABLE db.otel_traces (Timestamp DateTime64(9), TraceId String, SpanName LowCardinality(String),
			SpanKind LowCardinality(String), ServiceName LowCardinality(String), Duration UInt64,
			StatusCode LowCardinality(String), StatusMessage String)
		ENGINE = MergeTree ORDER BY (ServiceName, SpanName, toDateTime(Timestamp));
		INSERT INTO db.otel_traces VALUES
			('2026-10-19 13:55:00', 'old', 'POST /charge', 'Server', 'payments', 120000000, 'Error', 'card declined'),
			('2026-10-19 13:58:30', 'newest', 'POST /charge', 'Server', 'payments', 3100000000, 'Error', 'upstream timeout'),
			('2026-10-19 13:59:00', 'client', 'POST /bank', 'Client', 'payments', 1, 'Error', ''),
			('2026-10-19 14:05:00', 'future', 'POST /charge', 'Server', 'payments', 1, 'Error', ''),
			('2026-10-19 13:30:00', 'too-old', 'POST /charge', 'Server', 'payments', 1, 'Error', '');

		CREATE VIEW db.anomaly AS
		SELECT * FROM (
			SELECT 'payments' AS service_name, 'error_ratio' AS signal, 0.91 AS observed, 0.25 AS median, 17.8 AS z,
			       'last_hour' AS source, {at:DateTime64(3, 'UTC')} AS evaluated
			UNION ALL
			SELECT 'payments', 'latency_p95_ms', 210., 180., 0.9, 'last_hour', {at:DateTime64(3, 'UTC')}
		)`, "db.", db+"."))
	return &Enricher{
		Conn:     conn,
		Tables:   Tables{Traces: db + ".otel_traces", LogTemplates: db + ".log_templates", Anomaly: db + ".anomaly"},
		TraceURL: "https://grafana.example/explore?trace={trace_id}",
		Now:      func() time.Time { return at },
	}, db
}

func run(t *testing.T, e *Enricher, service string) map[string]incident.EnrichmentResult {
	t.Helper()
	got := map[string]incident.EnrichmentResult{}
	for _, r := range e.Enrich(context.Background(), &incident.Incident{ID: "i", Service: service}) {
		got[r.Name] = r
	}
	if len(got) != len(queries) {
		t.Fatalf("%d results", len(got))
	}
	return got
}

func lines(r incident.EnrichmentResult) string {
	if r.Finding == nil {
		return "<none>"
	}
	return r.Finding.Title + "\n" + strings.Join(r.Finding.Lines, "\n")
}

func TestEnrichesAFailingService(t *testing.T) {
	t.Parallel()
	e, _ := fixtures(t)
	got := run(t, e, "payments")
	for name, r := range got {
		if r.Err != nil {
			t.Fatalf("%s: %v", name, r.Err)
		}
	}

	logs := lines(got["new_error_logs"])
	want := "Error logs from payments, last 15m\n9× `ERROR` `timeout after 3000ms` _new_\n40× `ERROR` `card 4242 declined`"
	if logs != want {
		t.Errorf("logs:\n%s\nwant:\n%s", logs, want)
	}

	tr := lines(got["failing_trace"])
	want = "A failing trace\n<https://grafana.example/explore?trace=newest|newest> · `POST /charge` · 3100 ms · 13:58:30 UTC\n`upstream timeout`"
	if tr != want {
		t.Errorf("trace:\n%s\nwant:\n%s", tr, want)
	}

	base := lines(got["baseline"])
	want = "Against its baseline\n🔴 *error_ratio*: 91.0%, normally 25.0% (z +17.8)\n⚪ *latency_p95_ms*: 210 ms, normally 180 ms (z +0.9)"
	if base != want {
		t.Errorf("baseline:\n%s\nwant:\n%s", base, want)
	}
}

func TestAServiceWithNothingToShowHasNoFindings(t *testing.T) {
	t.Parallel()
	e, _ := fixtures(t)
	got := run(t, e, "inventory")
	for name, r := range got {
		if r.Err != nil || r.Finding != nil {
			t.Errorf("%s: %v, %+v", name, r.Err, r.Finding)
		}
	}
}

func TestAMissingTableIsAnErrorForThatQueryOnly(t *testing.T) {
	t.Parallel()
	e, db := fixtures(t)
	e.Tables.Anomaly = db + ".no_such_view"
	got := run(t, e, "payments")
	if got["baseline"].Err == nil {
		t.Fatal("want an error for the missing view")
	}
	if got["failing_trace"].Err != nil || got["failing_trace"].Finding == nil {
		t.Fatalf("the trace query should be unaffected: %v", got["failing_trace"].Err)
	}
}

func TestTheServiceIsBoundNotSpliced(t *testing.T) {
	t.Parallel()
	e, _ := fixtures(t)
	for name, r := range run(t, e, "payments' OR 1=1 --\n\\") {
		if r.Err != nil || r.Finding != nil {
			t.Errorf("%s: %v, %s", name, r.Err, fmt.Sprint(r.Finding))
		}
	}
}
