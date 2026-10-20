// Package incident turns Alertmanager notifications into incidents, one per alert group from its first firing
// alert until it resolves, and applies what responders do to them: acknowledge, silence, resolve. It owns the
// rules; storage, Slack, enrichment and silences are interfaces that other packages implement.
package incident

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/benhastings/garmr/internal/alertmanager"
)

type Status string

const (
	Firing       Status = "firing"
	Acknowledged Status = "acknowledged"
	Resolved     Status = "resolved"
)

// Incident is one open incident, as garmr.open_incidents holds it.
type Incident struct {
	GroupKey       string
	ID             string // a UUID
	Status         Status
	Title          string
	Service        string
	Receiver       string
	GroupLabels    map[string]string
	CommonLabels   map[string]string
	Alerts         []alertmanager.Alert // the firing alerts, ordered by fingerprint
	MaxFiring      int
	OpenedAt       time.Time
	UpdatedAt      time.Time
	AcknowledgedBy string // a Slack user ID
	AcknowledgedAt time.Time
	SilencedUntil  time.Time
	SlackChannel   string
	SlackTS        string
	// Version counts the writes; Store.Update only applies a write made from the current version.
	Version uint64

	// Set on the copy a resolution produces, for the history row and the message; never stored.
	ResolvedAt time.Time
	ResolvedBy string
}

// Silenced reports whether a silence garmr created is still running at now.
func (i *Incident) Silenced(now time.Time) bool { return now.Before(i.SilencedUntil) }

var (
	// ErrExists is Store.Create's answer when the group already has an open incident.
	ErrExists = errors.New("the group already has an open incident")
	// ErrConflict is Store.Update's and Store.Delete's answer when the incident changed since it was read.
	ErrConflict = errors.New("the incident changed since it was read")
	// ErrGone is Engine.Act's answer when the incident a button belongs to is no longer open.
	ErrGone = errors.New("the incident is no longer open")
)

// Store holds open incidents and appends history.
type Store interface {
	// Get returns the group's open incident, or nil when it has none.
	Get(ctx context.Context, groupKey string) (*Incident, error)
	// Create stores a new incident, or returns ErrExists.
	Create(ctx context.Context, inc *Incident) error
	// Update stores inc if the stored incident is still at inc.Version, and increments inc.Version; otherwise
	// it returns ErrConflict.
	Update(ctx context.Context, inc *Incident) error
	// Delete removes the group's incident if it is still at version, or returns ErrConflict.
	Delete(ctx context.Context, groupKey string, version uint64) error

	RecordAlerts(ctx context.Context, at time.Time, incidentID string, m *alertmanager.Message) error
	RecordEvent(ctx context.Context, e Event) error
	RecordResolved(ctx context.Context, inc *Incident) error
	RecordEnrichments(ctx context.Context, inc *Incident, results []EnrichmentResult) error
}

// Event is one row of garmr.incident_events.
type Event struct {
	At       time.Time
	Incident *Incident
	Name     string // opened, updated, acknowledged, silenced, resolved
	Actor    string // alertmanager, or slack:<user ID>
	Detail   string
}

// Notifier keeps an incident's Slack thread. A nil Notifier means incidents are kept but not sent anywhere.
type Notifier interface {
	// Open posts the incident's first message and returns the channel ID and message ts.
	Open(ctx context.Context, inc *Incident) (channel, ts string, err error)
	// Update edits the first message to inc's current state and, when note isn't empty, replies with it in the
	// thread.
	Update(ctx context.Context, inc *Incident, note string) error
	// Enrichment replies in the thread with what the enrichment queries found.
	Enrichment(ctx context.Context, inc *Incident, findings []Finding) error
}

// Finding is one enrichment's result, ready to show: a title and a few lines of Slack mrkdwn.
type Finding struct {
	Title string
	Lines []string
}

// EnrichmentResult is one enrichment query's run, for garmr.enrichments.
type EnrichmentResult struct {
	Name     string
	At       time.Time
	Duration time.Duration
	Err      error
	Rows     any // marshalled to JSON for the history row
	Finding  *Finding
}

// Enricher runs the enrichment queries for a new incident.
type Enricher interface {
	Enrich(ctx context.Context, inc *Incident) []EnrichmentResult
}

// Silencer creates an Alertmanager silence.
type Silencer interface {
	Silence(ctx context.Context, labels map[string]string, until time.Time, createdBy, comment string) (string, error)
}

// Title names an incident after its group labels, alertname first, as Alertmanager's own Slack template does:
// "ErrorRatioAnomaly · payments".
func Title(groupLabels map[string]string) string {
	keys := make([]string, 0, len(groupLabels))
	for k := range groupLabels {
		if k != "alertname" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(groupLabels))
	if v := groupLabels["alertname"]; v != "" {
		parts = append(parts, v)
	}
	for _, k := range keys {
		if v := groupLabels[k]; v != "" {
			parts = append(parts, v)
		}
	}
	if len(parts) == 0 {
		return "alerts"
	}
	return strings.Join(parts, " · ")
}

// sameAlerts reports whether a and b, both ordered by fingerprint, are the same alerts with the same
// annotations, so a repeat notification changes nothing.
func sameAlerts(a, b []alertmanager.Alert) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Fingerprint != b[i].Fingerprint || !equalMaps(a[i].Annotations, b[i].Annotations) {
			return false
		}
	}
	return true
}

func equalMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}
