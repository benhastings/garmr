// Package enrich runs the ClickHouse queries attached to a new incident: what its service logged that it
// hadn't before, a failing trace, and how far its signals are from their baseline. Each query is for the
// incident's service_name, binds it as a query parameter, and runs read-only with its own timeout. One that
// fails, say because an environment has no baselines, is recorded and left out of the message.
package enrich

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/benhastings/garmr/internal/incident"
)

// Tables names the tables the queries read. The defaults are clickhouse-observability-stack's.
type Tables struct {
	Traces       string // otel.otel_traces
	LogTemplates string // otel_features.log_templates
	Anomaly      string // otel_baseline.anomaly, a parameterized view taking at
}

func (t Tables) withDefaults() Tables {
	if t.Traces == "" {
		t.Traces = "otel.otel_traces"
	}
	if t.LogTemplates == "" {
		t.LogTemplates = "otel_features.log_templates"
	}
	if t.Anomaly == "" {
		t.Anomaly = "otel_baseline.anomaly"
	}
	return t
}

type Enricher struct {
	Conn   driver.Conn
	Tables Tables
	// Timeout bounds each query; 10s when zero.
	Timeout time.Duration
	// TraceURL links a trace ID to a trace view: {trace_id} in it is replaced. Empty prints the ID alone.
	TraceURL string
	// Window is how far back the queries look from the incident's opening; 15 minutes when zero.
	Window time.Duration
	Now    func() time.Time
}

var _ incident.Enricher = (*Enricher)(nil)

type query struct {
	name string
	run  func(ctx context.Context, e *Enricher, t Tables, inc *incident.Incident, at time.Time) (rows any, f *incident.Finding, err error)
}

// The order the findings appear in.
var queries = []query{
	{"new_error_logs", newErrorLogs},
	{"failing_trace", failingTrace},
	{"baseline", baseline},
}

func (e *Enricher) Enrich(ctx context.Context, inc *incident.Incident) []incident.EnrichmentResult {
	t := e.Tables.withDefaults()
	at := time.Now().UTC()
	if e.Now != nil {
		at = e.Now().UTC()
	}
	timeout := e.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	results := make([]incident.EnrichmentResult, len(queries))
	var wg sync.WaitGroup
	for i, q := range queries {
		wg.Go(func() {
			qctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			start := time.Now()
			rows, f, err := q.run(qctx, e, t, inc, at)
			results[i] = incident.EnrichmentResult{Name: q.name, At: at, Duration: time.Since(start), Err: err, Rows: rows, Finding: f}
		})
	}
	wg.Wait()
	return results
}

func (e *Enricher) window() time.Duration {
	if e.Window > 0 {
		return e.Window
	}
	return 15 * time.Minute
}

// queryCtx binds the parameters every query takes and makes the query read-only.
func (e *Enricher) queryCtx(ctx context.Context, inc *incident.Incident, at time.Time) context.Context {
	return clickhouse.Context(ctx,
		clickhouse.WithSettings(clickhouse.Settings{"readonly": 2}),
		clickhouse.WithParameters(clickhouse.Parameters{
			"service": paramEscaper.Replace(inc.Service),
			"at":      at.Format("2006-01-02 15:04:05.000"),
			"window":  fmt.Sprint(int(e.window() / time.Second)),
		}))
}

var paramEscaper = strings.NewReplacer(`\`, `\\`, "\t", `\t`, "\n", `\n`, "\r", `\r`, "\x00", `\0`)

type logTemplate struct {
	Template string `json:"template"`
	Severity string `json:"severity"`
	Count    uint64 `json:"count"`
	Example  string `json:"example"`
	New      bool   `json:"new"`
}

// newErrorLogs finds the warning and error templates the service logged in the window, the ones it hadn't
// logged in the 24 hours before first. log_templates is two minutes behind, so the window ends then.
func newErrorLogs(ctx context.Context, e *Enricher, t Tables, inc *incident.Incident, at time.Time) (any, *incident.Finding, error) {
	sql := fmt.Sprintf(`
		WITH {at:DateTime64(3, 'UTC')} AS t, toIntervalSecond({window:UInt32}) AS w
		SELECT any(Template) AS template, any(SeverityText) AS severity, sum(Count) AS count, any(Example) AS example,
		       TemplateHash NOT IN (
		           SELECT TemplateHash FROM %[1]s
		           WHERE ServiceName = {service:String} AND Minute >= t - w - INTERVAL 1 DAY AND Minute < t - w - INTERVAL 2 MINUTE
		       ) AS new
		FROM %[1]s FINAL
		WHERE ServiceName = {service:String} AND Minute >= t - w - INTERVAL 2 MINUTE AND Minute <= t
		  AND upper(SeverityText) IN ('WARN', 'WARNING', 'ERROR', 'CRITICAL', 'FATAL', 'ALERT', 'EMERGENCY')
		GROUP BY TemplateHash
		ORDER BY new DESC, count DESC
		LIMIT 3`, t.LogTemplates)
	rows, err := e.Conn.Query(e.queryCtx(ctx, inc, at), sql)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var out []logTemplate
	for rows.Next() {
		var l logTemplate
		if err := rows.Scan(&l.Template, &l.Severity, &l.Count, &l.Example, &l.New); err != nil {
			return nil, nil, err
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil || len(out) == 0 {
		return out, nil, err
	}
	// Sorted new first, so the first says whether any is new and the last whether all are.
	anyNew, allNew := out[0].New, out[len(out)-1].New
	title := "Error logs"
	switch {
	case allNew:
		title = "New error logs"
	case !anyNew:
		title = "Most frequent error logs"
	}
	f := &incident.Finding{Title: fmt.Sprintf("%s from %s, last %s", title, inc.Service, shortDuration(e.window()))}
	for _, l := range out {
		mark := ""
		if l.New && !allNew {
			mark = " _new_"
		}
		f.Lines = append(f.Lines, fmt.Sprintf("%d× `%s` %s%s", l.Count, l.Severity, code(l.Example), mark))
	}
	return out, f, nil
}

type trace struct {
	TraceID    string    `json:"trace_id"`
	SpanName   string    `json:"span_name"`
	Status     string    `json:"status_message"`
	DurationMs float64   `json:"duration_ms"`
	At         time.Time `json:"at"`
}

// failingTrace finds the service's latest failed server span in the window.
func failingTrace(ctx context.Context, e *Enricher, t Tables, inc *incident.Incident, at time.Time) (any, *incident.Finding, error) {
	sql := fmt.Sprintf(`
		WITH {at:DateTime64(3, 'UTC')} AS t
		SELECT TraceId, SpanName, StatusMessage, Duration / 1e6, toDateTime64(Timestamp, 3, 'UTC')
		FROM %s
		WHERE ServiceName = {service:String} AND SpanKind = 'Server' AND StatusCode = 'Error'
		  AND Timestamp > t - toIntervalSecond({window:UInt32}) AND Timestamp <= t
		ORDER BY Timestamp DESC
		LIMIT 1`, t.Traces)
	var tr trace
	err := e.Conn.QueryRow(e.queryCtx(ctx, inc, at), sql).Scan(&tr.TraceID, &tr.SpanName, &tr.Status, &tr.DurationMs, &tr.At)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	id := "`" + tr.TraceID + "`"
	if e.TraceURL != "" {
		id = fmt.Sprintf("<%s|%s>", strings.ReplaceAll(e.TraceURL, "{trace_id}", tr.TraceID), tr.TraceID)
	}
	line := fmt.Sprintf("%s · `%s` · %.0f ms · %s UTC", id, tr.SpanName, tr.DurationMs, tr.At.Format("15:04:05"))
	f := &incident.Finding{Title: "A failing trace", Lines: []string{line}}
	if tr.Status != "" {
		f.Lines = append(f.Lines, code(tr.Status))
	}
	return tr, f, nil
}

type signal struct {
	Signal   string  `json:"signal"`
	Observed float64 `json:"observed"`
	Median   float64 `json:"median"`
	Z        float64 `json:"z"`
	Source   string  `json:"source"`
}

// baseline compares each of the service's signals now with its normal, as the anomaly rules do.
func baseline(ctx context.Context, e *Enricher, t Tables, inc *incident.Incident, at time.Time) (any, *incident.Finding, error) {
	sql := fmt.Sprintf(`
		SELECT signal, observed, median, z, source
		FROM %s(at = {at:DateTime64(3, 'UTC')})
		WHERE service_name = {service:String}
		ORDER BY abs(z) DESC`, t.Anomaly)
	rows, err := e.Conn.Query(e.queryCtx(ctx, inc, at), sql)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var out []signal
	for rows.Next() {
		var s signal
		if err := rows.Scan(&s.Signal, &s.Observed, &s.Median, &s.Z, &s.Source); err != nil {
			return nil, nil, err
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil || len(out) == 0 {
		return out, nil, err
	}
	f := &incident.Finding{Title: "Against its baseline"}
	for _, s := range out {
		f.Lines = append(f.Lines, fmt.Sprintf("%s *%s*: %s, normally %s (z %+.1f)",
			zMark(s.Z), s.Signal, formatSignal(s.Signal, s.Observed), formatSignal(s.Signal, s.Median), s.Z))
	}
	return out, f, nil
}

func zMark(z float64) string {
	switch a := math.Abs(z); {
	case a >= 6:
		return "🔴"
	case a >= 3:
		return "🟠"
	}
	return "⚪"
}

func formatSignal(name string, v float64) string {
	switch {
	case strings.HasSuffix(name, "_ratio"):
		return fmt.Sprintf("%.1f%%", v*100)
	case strings.HasSuffix(name, "_ms"):
		return fmt.Sprintf("%.0f ms", v)
	case strings.HasSuffix(name, "_per_minute"):
		return fmt.Sprintf("%.1f/min", v)
	}
	return fmt.Sprintf("%.3g", v)
}

// code quotes s as inline code for Slack, on one line and at most 200 characters.
func code(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > 200 {
		s = string(r[:200]) + "…"
	}
	return "`" + strings.ReplaceAll(s, "`", "'") + "`"
}

func shortDuration(d time.Duration) string {
	if d%time.Hour == 0 {
		return fmt.Sprintf("%dh", int(d/time.Hour))
	}
	return fmt.Sprintf("%dm", int(d/time.Minute))
}
