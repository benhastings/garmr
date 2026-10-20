package incident

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benhastings/garmr/internal/alertmanager"
)

// memStore is a Store in memory with the same claim and compare-and-set rules as ClickHouse's.
type memStore struct {
	mu          sync.Mutex
	open        map[string]Incident
	alertRows   int
	events      []Event
	resolved    []Incident
	enrichments []EnrichmentResult
	failGet     error
}

func newMemStore() *memStore { return &memStore{open: map[string]Incident{}} }

func (s *memStore) Get(_ context.Context, k string) (*Incident, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failGet != nil {
		return nil, s.failGet
	}
	inc, ok := s.open[k]
	if !ok {
		return nil, nil
	}
	return &inc, nil
}

func (s *memStore) Create(_ context.Context, inc *Incident) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.open[inc.GroupKey]; ok {
		return ErrExists
	}
	inc.Version = 0
	s.open[inc.GroupKey] = *inc
	return nil
}

func (s *memStore) Update(_ context.Context, inc *Incident) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.open[inc.GroupKey]
	if !ok || cur.Version != inc.Version {
		return ErrConflict
	}
	inc.Version++
	s.open[inc.GroupKey] = *inc
	return nil
}

func (s *memStore) Delete(_ context.Context, k string, v uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur, ok := s.open[k]; ok && cur.Version != v {
		return ErrConflict
	}
	delete(s.open, k)
	return nil
}

func (s *memStore) RecordAlerts(_ context.Context, _ time.Time, _ string, m *alertmanager.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.alertRows += len(m.Alerts)
	return nil
}

func (s *memStore) RecordEvent(_ context.Context, e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	return nil
}

func (s *memStore) RecordResolved(_ context.Context, inc *Incident) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resolved = append(s.resolved, *inc)
	return nil
}

func (s *memStore) RecordEnrichments(_ context.Context, _ *Incident, r []EnrichmentResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enrichments = append(s.enrichments, r...)
	return nil
}

func (s *memStore) eventNames() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var names []string
	for _, e := range s.events {
		names = append(names, e.Name)
	}
	return strings.Join(names, ",")
}

// fakeSlack records what the engine sends.
type fakeSlack struct {
	mu       sync.Mutex
	posts    int
	updates  []string // the notes, "" for a redraw only
	statuses []Status
	enriched [][]Finding
	failOpen error
}

func (f *fakeSlack) Open(_ context.Context, inc *Incident) (string, string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failOpen != nil {
		return "", "", f.failOpen
	}
	f.posts++
	return "C1", "100.000" + string(rune('0'+f.posts)), nil
}

func (f *fakeSlack) Update(_ context.Context, inc *Incident, note string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, note)
	f.statuses = append(f.statuses, inc.Status)
	return nil
}

func (f *fakeSlack) Enrichment(_ context.Context, _ *Incident, findings []Finding) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.enriched = append(f.enriched, findings)
	return nil
}

type fakeEnricher struct{ calls int }

func (f *fakeEnricher) Enrich(_ context.Context, inc *Incident) []EnrichmentResult {
	f.calls++
	return []EnrichmentResult{
		{Name: "failing_trace", Finding: &Finding{Title: "Failing trace", Lines: []string{"abc"}}},
		{Name: "baseline", Err: errors.New("no such table")},
	}
}

type fakeSilencer struct {
	labels map[string]string
	until  time.Time
	calls  int
}

func (f *fakeSilencer) Silence(_ context.Context, labels map[string]string, until time.Time, _, _ string) (string, error) {
	f.calls++
	f.labels, f.until = labels, until
	return "sil-1", nil
}

const groupKey = `{}/{}:{alertname="ErrorRatioAnomaly", service_name="payments"}`

var t0 = time.Date(2026, 10, 19, 14, 0, 0, 0, time.UTC)

func alert(fp, status string) alertmanager.Alert {
	return alertmanager.Alert{
		Status:      status,
		Labels:      map[string]string{"alertname": "ErrorRatioAnomaly", "service_name": "payments", "pod": fp},
		Annotations: map[string]string{"summary": "payments errors on " + fp},
		Fingerprint: fp,
	}
}

func message(alerts ...alertmanager.Alert) *alertmanager.Message {
	return &alertmanager.Message{
		Version:      "4",
		GroupKey:     groupKey,
		Receiver:     "garmr",
		GroupLabels:  map[string]string{"alertname": "ErrorRatioAnomaly", "service_name": "payments"},
		CommonLabels: map[string]string{"alertname": "ErrorRatioAnomaly", "service_name": "payments"},
		Alerts:       alerts,
	}
}

type harness struct {
	*Engine
	store    *memStore
	slack    *fakeSlack
	enricher *fakeEnricher
	silencer *fakeSilencer
	now      time.Time
}

func newHarness() *harness {
	h := &harness{store: newMemStore(), slack: &fakeSlack{}, enricher: &fakeEnricher{}, silencer: &fakeSilencer{}, now: t0}
	h.Engine = &Engine{Store: h.store, Notifier: h.slack, Enricher: h.enricher, Silencer: h.silencer, Now: func() time.Time { return h.now },
		Log: slog.New(slog.DiscardHandler)}
	return h
}

func (h *harness) notify(t *testing.T, m *alertmanager.Message) {
	t.Helper()
	if err := h.Notify(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	h.Wait()
}

func (h *harness) open(t *testing.T) *Incident {
	t.Helper()
	inc, _ := h.store.Get(context.Background(), groupKey)
	if inc == nil {
		t.Fatal("no open incident")
	}
	return inc
}

func TestTheFirstFiringAlertOpensAnIncidentAndItsThread(t *testing.T) {
	h := newHarness()
	h.notify(t, message(alert("a", "firing")))

	inc := h.open(t)
	if inc.Title != "ErrorRatioAnomaly · payments" || inc.Service != "payments" || inc.Status != Firing ||
		inc.SlackChannel != "C1" || inc.SlackTS == "" || len(inc.Alerts) != 1 {
		t.Fatalf("%+v", inc)
	}
	if h.slack.posts != 1 || h.enricher.calls != 1 || len(h.slack.enriched) != 1 || len(h.slack.enriched[0]) != 1 {
		t.Fatalf("posts %d, enrichments %d, enrichment replies %v", h.slack.posts, h.enricher.calls, h.slack.enriched)
	}
	if len(h.store.enrichments) != 2 {
		t.Fatalf("recorded %d enrichment runs, want both, the failed one too", len(h.store.enrichments))
	}
	if got := h.store.eventNames(); got != "opened" {
		t.Fatalf("events %s", got)
	}
	if h.store.alertRows != 1 {
		t.Fatalf("%d alert rows", h.store.alertRows)
	}
}

func TestARepeatNotificationOnlyRedrawsTheMessage(t *testing.T) {
	h := newHarness()
	h.notify(t, message(alert("a", "firing")))
	h.notify(t, message(alert("a", "firing")))

	if h.slack.posts != 1 || len(h.slack.updates) != 1 || h.slack.updates[0] != "" {
		t.Fatalf("posts %d, updates %q", h.slack.posts, h.slack.updates)
	}
	if got := h.store.eventNames(); got != "opened" {
		t.Fatalf("events %s", got)
	}
	if v := h.open(t).Version; v != 1 { // the thread link only
		t.Fatalf("version %d", v)
	}
}

func TestChangedAlertsAreRepliedInTheThread(t *testing.T) {
	h := newHarness()
	h.notify(t, message(alert("a", "firing")))
	h.notify(t, message(alert("a", "firing"), alert("b", "firing"), alert("c", "firing")))
	h.notify(t, message(alert("a", "resolved"), alert("b", "firing"), alert("c", "firing")))

	want := []string{
		"2 alerts started firing: payments errors on b; payments errors on c.",
		"1 alert resolved: payments errors on a.",
	}
	if strings.Join(h.slack.updates, "|") != strings.Join(want, "|") {
		t.Fatalf("notes %q", h.slack.updates)
	}
	inc := h.open(t)
	if len(inc.Alerts) != 2 || inc.MaxFiring != 3 {
		t.Fatalf("%d firing, max %d", len(inc.Alerts), inc.MaxFiring)
	}
}

func TestAllAlertsResolvedClosesTheIncident(t *testing.T) {
	h := newHarness()
	h.notify(t, message(alert("a", "firing")))
	h.now = t0.Add(14 * time.Minute)
	h.notify(t, message(alert("a", "resolved")))

	if inc, _ := h.store.Get(context.Background(), groupKey); inc != nil {
		t.Fatalf("still open: %+v", inc)
	}
	if len(h.store.resolved) != 1 || h.store.resolved[0].ResolvedBy != "alertmanager" || !h.store.resolved[0].ResolvedAt.Equal(h.now) {
		t.Fatalf("history %+v", h.store.resolved)
	}
	if last := h.slack.updates[len(h.slack.updates)-1]; last != "All alerts resolved after 14m." {
		t.Fatalf("note %q", last)
	}
	if h.slack.statuses[len(h.slack.statuses)-1] != Resolved {
		t.Fatal("the message wasn't redrawn as resolved")
	}

	// The next firing alert opens a new incident with a new thread.
	h.notify(t, message(alert("a", "firing")))
	if h.slack.posts != 2 {
		t.Fatalf("%d posts", h.slack.posts)
	}
}

func TestAResolvedNotificationWithNoOpenIncidentIsOnlyRecorded(t *testing.T) {
	h := newHarness()
	h.notify(t, message(alert("a", "resolved")))
	if h.slack.posts != 0 || len(h.slack.updates) != 0 || h.store.alertRows != 1 {
		t.Fatalf("posts %d, updates %d, alert rows %d", h.slack.posts, len(h.slack.updates), h.store.alertRows)
	}
}

func TestAFailedPostIsRetriedByTheNextNotification(t *testing.T) {
	h := newHarness()
	h.slack.failOpen = errors.New("ratelimited")
	if err := h.Notify(context.Background(), message(alert("a", "firing"))); err == nil {
		t.Fatal("want the Slack error, so Alertmanager retries")
	}
	if h.store.alertRows != 0 {
		t.Fatal("a failed notification's alerts were recorded; the retry records them")
	}
	h.slack.failOpen = nil
	h.notify(t, message(alert("a", "firing")))
	if inc := h.open(t); inc.SlackTS == "" || h.slack.posts != 1 {
		t.Fatalf("ts %q, posts %d", inc.SlackTS, h.slack.posts)
	}
	if h.enricher.calls != 1 {
		t.Fatalf("%d enrichments", h.enricher.calls)
	}
}

func TestIncidentsAreKeptWithoutSlack(t *testing.T) {
	h := newHarness()
	h.Notifier = nil
	h.notify(t, message(alert("a", "firing")))
	if inc := h.open(t); inc.SlackTS != "" {
		t.Fatal(inc.SlackTS)
	}
	if h.enricher.calls != 1 {
		t.Fatal("enrichment should still run and be recorded")
	}
	h.notify(t, message(alert("a", "resolved")))
	if len(h.store.resolved) != 1 {
		t.Fatal("not resolved")
	}
}

func action(t *testing.T, h *harness, kind ActionKind) Action {
	return Action{Kind: kind, IncidentID: h.open(t).ID, GroupKey: groupKey, UserID: "U42", UserName: "ben"}
}

func TestAcknowledge(t *testing.T) {
	h := newHarness()
	h.notify(t, message(alert("a", "firing")))
	h.now = t0.Add(3 * time.Minute)
	if err := h.Act(context.Background(), action(t, h, Acknowledge)); err != nil {
		t.Fatal(err)
	}
	inc := h.open(t)
	if inc.Status != Acknowledged || inc.AcknowledgedBy != "U42" || !inc.AcknowledgedAt.Equal(h.now) {
		t.Fatalf("%+v", inc)
	}
	if last := h.slack.updates[len(h.slack.updates)-1]; last != "Acknowledged by <@U42>." {
		t.Fatal(last)
	}
	// Twice is once.
	if err := h.Act(context.Background(), action(t, h, Acknowledge)); err != nil || len(h.slack.updates) != 1 {
		t.Fatalf("%v, %d updates", err, len(h.slack.updates))
	}
	// A later notification keeps the acknowledgement.
	h.notify(t, message(alert("a", "firing"), alert("b", "firing")))
	if inc := h.open(t); inc.Status != Acknowledged {
		t.Fatal(inc.Status)
	}
}

func TestSilenceSilencesTheGroupInAlertmanager(t *testing.T) {
	h := newHarness()
	h.notify(t, message(alert("a", "firing")))
	if err := h.Act(context.Background(), action(t, h, Silence)); err != nil {
		t.Fatal(err)
	}
	if h.silencer.calls != 1 || h.silencer.labels["service_name"] != "payments" || len(h.silencer.labels) != 2 ||
		!h.silencer.until.Equal(t0.Add(time.Hour)) {
		t.Fatalf("%+v", h.silencer)
	}
	inc := h.open(t)
	if !inc.Silenced(t0.Add(59*time.Minute)) || inc.Silenced(t0.Add(61*time.Minute)) {
		t.Fatalf("silenced until %v", inc.SilencedUntil)
	}
	if last := h.slack.updates[len(h.slack.updates)-1]; last != "Silenced for 1h by <@U42>, until 15:00 UTC." {
		t.Fatal(last)
	}
	if got := h.store.eventNames(); got != "opened,silenced" {
		t.Fatal(got)
	}
}

func TestResolveByHandClosesTheIncident(t *testing.T) {
	h := newHarness()
	h.notify(t, message(alert("a", "firing")))
	h.now = t0.Add(90 * time.Minute)
	if err := h.Act(context.Background(), action(t, h, Resolve)); err != nil {
		t.Fatal(err)
	}
	if len(h.store.resolved) != 1 || h.store.resolved[0].ResolvedBy != "slack:U42" {
		t.Fatalf("%+v", h.store.resolved)
	}
	if last := h.slack.updates[len(h.slack.updates)-1]; last != "Resolved by <@U42> after 1h 30m." {
		t.Fatal(last)
	}
}

func TestAButtonOnAClosedIncidentIsGone(t *testing.T) {
	h := newHarness()
	h.notify(t, message(alert("a", "firing")))
	old := action(t, h, Acknowledge)
	h.notify(t, message(alert("a", "resolved")))
	if err := h.Act(context.Background(), old); !errors.Is(err, ErrGone) {
		t.Fatalf("closed: want ErrGone, got %v", err)
	}
	// Nor does it act on the group's next incident.
	h.notify(t, message(alert("a", "firing")))
	if err := h.Act(context.Background(), old); !errors.Is(err, ErrGone) {
		t.Fatalf("reopened: want ErrGone, got %v", err)
	}
	if h.open(t).Status != Firing {
		t.Fatal("the old button acknowledged the new incident")
	}
}

func TestTitle(t *testing.T) {
	for _, c := range []struct {
		labels map[string]string
		want   string
	}{
		{map[string]string{"alertname": "A", "service_name": "s", "env": "prod"}, "A · prod · s"},
		{map[string]string{"service_name": "s"}, "s"},
		{nil, "alerts"},
	} {
		if got := Title(c.labels); got != c.want {
			t.Errorf("Title(%v) = %q, want %q", c.labels, got, c.want)
		}
	}
}

func TestHumanDuration(t *testing.T) {
	for d, want := range map[time.Duration]string{
		45 * time.Second:                "45s",
		14*time.Minute + 20*time.Second: "14m",
		2 * time.Hour:                   "2h",
		2*time.Hour + 5*time.Minute:     "2h 5m",
	} {
		if got := HumanDuration(d); got != want {
			t.Errorf("HumanDuration(%v) = %q, want %q", d, got, want)
		}
	}
}
