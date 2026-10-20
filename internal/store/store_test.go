package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/benhastings/garmr/internal/alertmanager"
	"github.com/benhastings/garmr/internal/chtest"
	"github.com/benhastings/garmr/internal/incident"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	conn := chtest.Conn(t)
	return &Store{Conn: conn, Database: chtest.Database(t, conn)}
}

var opened = time.Date(2026, 10, 19, 14, 2, 0, 0, time.UTC)

func newIncident() *incident.Incident {
	return &incident.Incident{
		GroupKey:     `{}/{}:{alertname="ErrorRatioAnomaly", service_name="payments"}`,
		ID:           uuid.NewString(),
		Status:       incident.Firing,
		Title:        "ErrorRatioAnomaly · payments",
		Service:      "payments",
		Receiver:     "garmr",
		GroupLabels:  map[string]string{"alertname": "ErrorRatioAnomaly", "service_name": "payments"},
		CommonLabels: map[string]string{"alertname": "ErrorRatioAnomaly", "service_name": "payments", "severity": "warning"},
		Alerts: []alertmanager.Alert{{
			Status:      "firing",
			Labels:      map[string]string{"alertname": "ErrorRatioAnomaly", "service_name": "payments"},
			Annotations: map[string]string{"summary": "payments: error ratio is 17.8 deviations above normal", "description": "line one\nline two\t'quoted' \\ backslash"},
			StartsAt:    opened.Add(-time.Minute),
			Fingerprint: "a1",
		}},
		MaxFiring: 1,
		OpenedAt:  opened,
		UpdatedAt: opened,
	}
}

func TestCreateThenGetRoundTripsAnIncident(t *testing.T) {
	t.Parallel()
	s, ctx := newStore(t), context.Background()
	want := newIncident()
	if err := s.Create(ctx, want); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, want.GroupKey)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.ID != want.ID || got.Title != want.Title || got.Status != incident.Firing ||
		got.GroupLabels["service_name"] != "payments" || len(got.Alerts) != 1 ||
		got.Alerts[0].Annotations["description"] != want.Alerts[0].Annotations["description"] ||
		!got.OpenedAt.Equal(opened) || !got.AcknowledgedAt.IsZero() || got.Version != 0 {
		t.Fatalf("got %+v", got)
	}
}

func TestGetReturnsNilForAGroupWithNoIncident(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	got, err := s.Get(context.Background(), "nothing")
	if err != nil || got != nil {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestOnlyOneOfTwoConcurrentCreatesClaimsTheGroup(t *testing.T) {
	t.Parallel()
	s, ctx := newStore(t), context.Background()
	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() { errs[i] = s.Create(ctx, newIncident()) })
	}
	wg.Wait()
	won := 0
	for _, err := range errs {
		switch {
		case err == nil:
			won++
		case !errors.Is(err, incident.ErrExists):
			t.Fatalf("want ErrExists, got %v", err)
		}
	}
	if won != 1 {
		t.Fatalf("%d creates succeeded, want 1", won)
	}
}

func TestUpdateAppliesFromTheCurrentVersionOnly(t *testing.T) {
	t.Parallel()
	s, ctx := newStore(t), context.Background()
	inc := newIncident()
	if err := s.Create(ctx, inc); err != nil {
		t.Fatal(err)
	}
	stale := *inc

	inc.Status = incident.Acknowledged
	inc.AcknowledgedBy = "U123"
	inc.AcknowledgedAt = opened.Add(5 * time.Minute)
	inc.SlackChannel, inc.SlackTS = "C1", "1760882520.000100"
	if err := s.Update(ctx, inc); err != nil {
		t.Fatal(err)
	}
	if inc.Version != 1 {
		t.Fatalf("version %d after one update", inc.Version)
	}

	stale.Status = incident.Firing
	if err := s.Update(ctx, &stale); !errors.Is(err, incident.ErrConflict) {
		t.Fatalf("a stale update: want ErrConflict, got %v", err)
	}

	got, err := s.Get(ctx, inc.GroupKey)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != incident.Acknowledged || got.AcknowledgedBy != "U123" || !got.AcknowledgedAt.Equal(inc.AcknowledgedAt) ||
		got.SlackTS != "1760882520.000100" || got.Version != 1 {
		t.Fatalf("got %+v", got)
	}
}

func TestConcurrentUpdatesNeverLoseOne(t *testing.T) {
	t.Parallel()
	s, ctx := newStore(t), context.Background()
	inc := newIncident()
	if err := s.Create(ctx, inc); err != nil {
		t.Fatal(err)
	}
	const n = 8
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		mine := *inc
		mine.AcknowledgedBy = uuid.NewString()
		wg.Go(func() { errs[i] = s.Update(ctx, &mine) })
	}
	wg.Wait()
	won := 0
	for _, err := range errs {
		switch {
		case err == nil:
			won++
		case !errors.Is(err, incident.ErrConflict):
			t.Fatal(err)
		}
	}
	got, err := s.Get(ctx, inc.GroupKey)
	if err != nil {
		t.Fatal(err)
	}
	if won != 1 || got.Version != 1 {
		t.Fatalf("%d updates won and the version is %d; want exactly one of each", won, got.Version)
	}
}

func TestDeleteRemovesTheIncidentAtItsVersionOnly(t *testing.T) {
	t.Parallel()
	s, ctx := newStore(t), context.Background()
	inc := newIncident()
	if err := s.Create(ctx, inc); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(ctx, inc); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, inc.GroupKey, 0); !errors.Is(err, incident.ErrConflict) {
		t.Fatalf("a stale delete: want ErrConflict, got %v", err)
	}
	if err := s.Delete(ctx, inc.GroupKey, inc.Version); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, inc.GroupKey); err != nil || got != nil {
		t.Fatalf("after delete: %+v, %v", got, err)
	}
	// The group can open again.
	if err := s.Create(ctx, newIncident()); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryIsWritten(t *testing.T) {
	t.Parallel()
	s, ctx := newStore(t), context.Background()
	inc := newIncident()
	inc.AcknowledgedAt = opened.Add(3 * time.Minute)
	inc.ResolvedAt = opened.Add(14 * time.Minute)
	inc.ResolvedBy = "alertmanager"
	m := &alertmanager.Message{Version: "4", GroupKey: inc.GroupKey, Receiver: "garmr", Alerts: inc.Alerts}

	if err := s.RecordAlerts(ctx, opened, inc.ID, m); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordEvent(ctx, incident.Event{At: opened, Incident: inc, Name: "opened", Actor: "alertmanager"}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordResolved(ctx, inc); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordNotification(ctx, Notification{At: opened, IncidentID: inc.ID, Integration: "slack", Method: "chat.postMessage", Err: errors.New("channel_not_found")}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordEnrichments(ctx, inc, []incident.EnrichmentResult{{Name: "failing_trace", At: opened, Rows: []map[string]any{{"trace_id": "abc"}}}}); err != nil {
		t.Fatal(err)
	}

	var ttr, tta int64
	byID := clickhouse.Context(ctx, clickhouse.WithParameters(clickhouse.Parameters{"id": inc.ID}))
	if err := s.Conn.QueryRow(byID, "SELECT dateDiff('second', opened_at, resolved_at), dateDiff('second', opened_at, assumeNotNull(acknowledged_at)) FROM "+
		s.table("incidents")+" WHERE incident_id = {id:UUID}").Scan(&ttr, &tta); err != nil {
		t.Fatal(err)
	}
	if ttr != 840 || tta != 180 {
		t.Fatalf("time to resolve %v, to acknowledge %v", ttr, tta)
	}
	for table, want := range map[string]uint64{"alert_events": 1, "incident_events": 1, "notifications": 1, "enrichments": 1} {
		var n uint64
		if err := s.Conn.QueryRow(ctx, "SELECT count() FROM "+s.table(table)).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Errorf("%s has %d rows, want %d", table, n, want)
		}
	}
}
