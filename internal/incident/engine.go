package incident

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/benhastings/garmr/internal/alertmanager"
	"github.com/benhastings/garmr/internal/metrics"
)

// maxAttempts bounds the retries after ErrConflict or ErrExists, which only another replica writing the same
// incident at the same moment causes.
const maxAttempts = 5

// ActorAlertmanager is the actor of every change a notification makes.
const ActorAlertmanager = "alertmanager"

// Engine applies notifications and actions to incidents. Store is required; Notifier, Enricher and Silencer
// may be nil.
type Engine struct {
	Store    Store
	Notifier Notifier
	Enricher Enricher
	Silencer Silencer
	Log      *slog.Logger
	Now      func() time.Time
	// SilenceFor is how long the silence button silences a group; an hour when zero.
	SilenceFor time.Duration
	// EnrichTimeout bounds all of one incident's enrichment queries together; 30s when zero.
	EnrichTimeout time.Duration

	locks keyedMutex
	wg    sync.WaitGroup
}

func (e *Engine) now() time.Time {
	if e.Now != nil {
		return e.Now().UTC()
	}
	return time.Now().UTC()
}

func (e *Engine) log() *slog.Logger {
	if e.Log != nil {
		return e.Log
	}
	return slog.Default()
}

// Wait blocks until every enrichment started so far has finished.
func (e *Engine) Wait() { e.wg.Wait() }

// Notify applies one Alertmanager notification: it opens the group's incident on its first firing alert,
// updates it while alerts change, and resolves it when none fire. A retry of a notification already applied
// changes nothing but the Slack message, which is redrawn.
func (e *Engine) Notify(ctx context.Context, m *alertmanager.Message) error {
	unlock := e.locks.lock(m.GroupKey)
	defer unlock()
	now := e.now()

	var inc *Incident
	var err error
	for range maxAttempts {
		inc, err = e.apply(ctx, m, now)
		if !errors.Is(err, ErrConflict) && !errors.Is(err, ErrExists) {
			break
		}
	}
	if err != nil {
		return err
	}
	id := ""
	if inc != nil {
		id = inc.ID
	}
	return e.Store.RecordAlerts(ctx, now, id, m)
}

func (e *Engine) apply(ctx context.Context, m *alertmanager.Message, now time.Time) (*Incident, error) {
	cur, err := e.Store.Get(ctx, m.GroupKey)
	if err != nil {
		return nil, err
	}
	firing := m.Firing()
	switch {
	case cur == nil && len(firing) == 0:
		return nil, nil // resolved, but its incident is already closed: someone resolved it by hand
	case cur == nil:
		return e.open(ctx, m, firing, now)
	case len(firing) == 0:
		return cur, e.resolve(ctx, cur, now, ActorAlertmanager, "All alerts resolved")
	default:
		return e.update(ctx, cur, m, firing, now)
	}
}

func (e *Engine) open(ctx context.Context, m *alertmanager.Message, firing []alertmanager.Alert, now time.Time) (*Incident, error) {
	service := m.GroupLabels["service_name"]
	if service == "" {
		service = m.CommonLabels["service_name"]
	}
	inc := &Incident{
		GroupKey:     m.GroupKey,
		ID:           uuid.NewString(),
		Status:       Firing,
		Title:        Title(m.GroupLabels),
		Service:      service,
		Receiver:     m.Receiver,
		GroupLabels:  m.GroupLabels,
		CommonLabels: m.CommonLabels,
		Alerts:       firing,
		MaxFiring:    len(firing),
		OpenedAt:     now,
		UpdatedAt:    now,
	}
	if err := e.Store.Create(ctx, inc); err != nil {
		return nil, err
	}
	metrics.IncidentsOpened.Inc()
	e.event(ctx, Event{At: now, Incident: inc, Name: "opened", Actor: ActorAlertmanager, Detail: fmt.Sprintf("%d firing", len(firing))})
	e.log().Info("incident opened", "incident", inc.ID, "title", inc.Title, "firing", len(firing))
	if e.Notifier == nil {
		e.enrich(inc)
		return inc, nil
	}
	return inc, e.post(ctx, inc)
}

// post opens the incident's Slack thread, records it on the incident, and starts the enrichment, which
// replies in that thread.
func (e *Engine) post(ctx context.Context, inc *Incident) error {
	channel, ts, err := e.Notifier.Open(ctx, inc)
	if err != nil {
		return fmt.Errorf("posting the incident to Slack: %w", err)
	}
	// The message is out, so keep retrying the write that links it, rather than let the caller's retry post a
	// second thread.
	for range maxAttempts {
		inc.SlackChannel, inc.SlackTS = channel, ts
		if err = e.Store.Update(ctx, inc); !errors.Is(err, ErrConflict) {
			break
		}
		cur, gerr := e.Store.Get(ctx, inc.GroupKey)
		if gerr != nil || cur == nil || cur.ID != inc.ID {
			return errors.Join(err, gerr)
		}
		*inc = *cur
	}
	if err != nil {
		return err
	}
	e.enrich(inc)
	return nil
}

func (e *Engine) update(ctx context.Context, cur *Incident, m *alertmanager.Message, firing []alertmanager.Alert, now time.Time) (*Incident, error) {
	next := *cur
	note := ""
	if !sameAlerts(cur.Alerts, firing) {
		note = changeNote(cur.Alerts, firing)
		next.Alerts = firing
		next.MaxFiring = max(cur.MaxFiring, len(firing))
		next.CommonLabels = m.CommonLabels
		next.UpdatedAt = now
		if err := e.Store.Update(ctx, &next); err != nil {
			return nil, err
		}
		e.event(ctx, Event{At: now, Incident: &next, Name: "updated", Actor: ActorAlertmanager, Detail: note})
	}
	if e.Notifier == nil {
		return &next, nil
	}
	if next.SlackTS == "" { // an earlier post failed
		return &next, e.post(ctx, &next)
	}
	return &next, e.Notifier.Update(ctx, &next, note)
}

// resolve closes cur: its final row goes to history and it leaves the open incidents, so the group's next
// firing alert opens a new incident.
func (e *Engine) resolve(ctx context.Context, cur *Incident, now time.Time, actor, note string) error {
	next := *cur
	next.Status = Resolved
	next.ResolvedAt = now
	next.ResolvedBy = actor
	next.UpdatedAt = now
	if err := e.Store.Delete(ctx, cur.GroupKey, cur.Version); err != nil {
		return err
	}
	by := "alertmanager"
	if actor != ActorAlertmanager {
		by = "slack"
	}
	metrics.IncidentsResolved.WithLabelValues(by).Inc()
	note = fmt.Sprintf("%s after %s.", note, HumanDuration(now.Sub(cur.OpenedAt)))
	var err error
	for range 3 {
		if err = e.Store.RecordResolved(ctx, &next); err == nil {
			break
		}
	}
	if err != nil {
		e.log().Error("the resolved incident's history row is lost", "incident", cur.ID, "err", err)
	}
	e.event(ctx, Event{At: now, Incident: &next, Name: "resolved", Actor: actor, Detail: note})
	e.log().Info("incident resolved", "incident", cur.ID, "title", cur.Title, "by", actor)
	if e.Notifier == nil || next.SlackTS == "" {
		return nil
	}
	return e.Notifier.Update(ctx, &next, note)
}

func (e *Engine) event(ctx context.Context, ev Event) {
	if err := e.Store.RecordEvent(ctx, ev); err != nil {
		e.log().Error("recording an incident event", "incident", ev.Incident.ID, "event", ev.Name, "err", err)
	}
}

func (e *Engine) enrich(inc *Incident) {
	if e.Enricher == nil || inc.Service == "" {
		return
	}
	snapshot := *inc
	timeout := e.EnrichTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		results := e.Enricher.Enrich(ctx, &snapshot)
		var findings []Finding
		for _, r := range results {
			outcome := "ok"
			switch {
			case r.Err != nil:
				outcome = "error"
				e.log().Warn("enrichment failed", "incident", snapshot.ID, "enrichment", r.Name, "err", r.Err)
			case r.Finding == nil:
				outcome = "empty"
			default:
				findings = append(findings, *r.Finding)
			}
			metrics.Enrichments.WithLabelValues(r.Name, outcome).Inc()
		}
		if err := e.Store.RecordEnrichments(ctx, &snapshot, results); err != nil {
			e.log().Error("recording enrichments", "incident", snapshot.ID, "err", err)
		}
		if e.Notifier == nil || snapshot.SlackTS == "" || len(findings) == 0 {
			return
		}
		if err := e.Notifier.Enrichment(ctx, &snapshot, findings); err != nil {
			e.log().Error("posting the enrichment to Slack", "incident", snapshot.ID, "err", err)
		}
	}()
}

// ActionKind is a button on the incident's message.
type ActionKind string

const (
	Acknowledge ActionKind = "acknowledge"
	Silence     ActionKind = "silence"
	Resolve     ActionKind = "resolve"
)

// Action is a responder's click on one incident's message.
type Action struct {
	Kind       ActionKind
	IncidentID string
	GroupKey   string
	UserID     string // Slack's user ID, for mentions and history
	UserName   string // for the silence's createdBy
}

// Act applies a. It returns ErrGone when the incident the button belongs to is no longer open, which the
// caller tells the user about.
func (e *Engine) Act(ctx context.Context, a Action) error {
	err := e.act(ctx, a)
	outcome := "ok"
	switch {
	case errors.Is(err, ErrGone):
		outcome = "gone"
	case err != nil:
		outcome = "error"
	}
	metrics.Actions.WithLabelValues(string(a.Kind), outcome).Inc()
	return err
}

func (e *Engine) act(ctx context.Context, a Action) error {
	unlock := e.locks.lock(a.GroupKey)
	defer unlock()
	actor := "slack:" + a.UserID
	mention := fmt.Sprintf("<@%s>", a.UserID)

	// The silence is created once, before the retries, so a conflict can't create a second one.
	var silenceID string
	var until time.Time
	if a.Kind == Silence {
		cur, err := e.current(ctx, a)
		if err != nil {
			return err
		}
		if e.Silencer == nil {
			return errors.New("silencing needs garmr's -alertmanager-url")
		}
		until = e.now().Add(e.silenceFor())
		silenceID, err = e.Silencer.Silence(ctx, cur.GroupLabels, until, a.UserName,
			fmt.Sprintf("garmr incident %s: %s", cur.ID, cur.Title))
		if err != nil {
			return fmt.Errorf("creating the silence: %w", err)
		}
	}

	var err error
	for range maxAttempts {
		if err = e.actOnce(ctx, a, actor, mention, silenceID, until); !errors.Is(err, ErrConflict) {
			break
		}
	}
	return err
}

func (e *Engine) actOnce(ctx context.Context, a Action, actor, mention, silenceID string, until time.Time) error {
	cur, err := e.current(ctx, a)
	if err != nil {
		return err
	}
	now := e.now()
	next := *cur
	next.UpdatedAt = now
	var note, detail string
	switch a.Kind {
	case Acknowledge:
		if cur.Status == Acknowledged {
			return nil
		}
		next.Status = Acknowledged
		next.AcknowledgedBy = a.UserID
		next.AcknowledgedAt = now
		note = "Acknowledged by " + mention + "."
	case Silence:
		next.SilencedUntil = until
		note = fmt.Sprintf("Silenced for %s by %s, until %s UTC.", HumanDuration(until.Sub(now)), mention, until.Format("15:04"))
		detail = "silence " + silenceID + " until " + until.Format(time.RFC3339)
	case Resolve:
		return e.resolve(ctx, cur, now, actor, "Resolved by "+mention)
	default:
		return fmt.Errorf("unknown action %q", a.Kind)
	}
	if err := e.Store.Update(ctx, &next); err != nil {
		return err
	}
	if detail == "" {
		detail = note
	}
	e.event(ctx, Event{At: now, Incident: &next, Name: string(eventFor(a.Kind)), Actor: actor, Detail: detail})
	if e.Notifier == nil || next.SlackTS == "" {
		return nil
	}
	return e.Notifier.Update(ctx, &next, note)
}

func (e *Engine) current(ctx context.Context, a Action) (*Incident, error) {
	cur, err := e.Store.Get(ctx, a.GroupKey)
	if err != nil {
		return nil, err
	}
	if cur == nil || cur.ID != a.IncidentID {
		return nil, ErrGone
	}
	return cur, nil
}

func (e *Engine) silenceFor() time.Duration {
	if e.SilenceFor > 0 {
		return e.SilenceFor
	}
	return time.Hour
}

func eventFor(k ActionKind) string {
	switch k {
	case Acknowledge:
		return "acknowledged"
	case Silence:
		return "silenced"
	}
	return string(k)
}

// changeNote describes what changed between two notifications' firing alerts.
func changeNote(before, after []alertmanager.Alert) string {
	was := make(map[string]bool, len(before))
	for _, a := range before {
		was[a.Fingerprint] = true
	}
	is := make(map[string]bool, len(after))
	var started []alertmanager.Alert
	for _, a := range after {
		is[a.Fingerprint] = true
		if !was[a.Fingerprint] {
			started = append(started, a)
		}
	}
	var stopped []alertmanager.Alert
	for _, a := range before {
		if !is[a.Fingerprint] {
			stopped = append(stopped, a)
		}
	}
	var parts []string
	if len(started) > 0 {
		parts = append(parts, fmt.Sprintf("%s started firing: %s", plural(len(started), "alert"), describe(started)))
	}
	if len(stopped) > 0 {
		parts = append(parts, fmt.Sprintf("%s resolved: %s", plural(len(stopped), "alert"), describe(stopped)))
	}
	if len(parts) == 0 {
		return "The alerts' details changed."
	}
	return strings.Join(parts, ". ") + "."
}

func describe(alerts []alertmanager.Alert) string {
	const most = 3
	var parts []string
	for i, a := range alerts {
		if i == most {
			parts = append(parts, fmt.Sprintf("and %d more", len(alerts)-most))
			break
		}
		parts = append(parts, AlertLine(a))
	}
	return strings.Join(parts, "; ")
}

// AlertLine is one line for an alert: its summary annotation, or its name and labels.
func AlertLine(a alertmanager.Alert) string {
	if s := a.Annotations["summary"]; s != "" {
		return s
	}
	labels := make([]string, 0, len(a.Labels))
	for k, v := range a.Labels {
		if k != "alertname" {
			labels = append(labels, k+"="+v)
		}
	}
	sort.Strings(labels)
	return strings.TrimSpace(a.Labels["alertname"] + " " + strings.Join(labels, ", "))
}

func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

// HumanDuration rounds d for a person: 45s, 14m, 2h 5m.
func HumanDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Round(time.Second)/time.Second))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Round(time.Minute)/time.Minute))
	}
	d = d.Round(time.Minute)
	h, m := int(d/time.Hour), int(d%time.Hour/time.Minute)
	if m == 0 {
		return fmt.Sprintf("%dh", h)
	}
	return fmt.Sprintf("%dh %dm", h, m)
}

// keyedMutex serializes the work on one group within this replica.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyLock
}

type keyLock struct {
	sync.Mutex
	users int
}

func (k *keyedMutex) lock(key string) (unlock func()) {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = map[string]*keyLock{}
	}
	l := k.locks[key]
	if l == nil {
		l = &keyLock{}
		k.locks[key] = l
	}
	l.users++
	k.mu.Unlock()
	l.Lock()
	return func() {
		l.Unlock()
		k.mu.Lock()
		if l.users--; l.users == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}
