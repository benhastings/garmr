// Package store keeps incidents in ClickHouse, in the tables schema/garmr.sql defines: open incidents in the
// KeeperMap table garmr.open_incidents, and history in MergeTree tables.
//
// Every state write runs with keeper_map_strict_mode = 1. A strict INSERT of a key that exists is refused,
// which is how opening an incident claims its group. An update is a compare-and-set: it matches only the
// version it read, sets a fresh op, and reads op back to learn whether it won.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/benhastings/garmr/internal/alertmanager"
	"github.com/benhastings/garmr/internal/incident"
)

type Store struct {
	Conn driver.Conn
	// Database holds garmr's tables; garmr when empty.
	Database string
}

var _ incident.Store = (*Store)(nil)

func (s *Store) table(name string) string {
	db := s.Database
	if db == "" {
		db = "garmr"
	}
	return quoteIdent(db) + "." + quoteIdent(name)
}

func quoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

// stateCtx is the context for a state read or write: strict mode, and no async insert, whose buffer would
// hide a refused claim.
func stateCtx(ctx context.Context, params clickhouse.Parameters) context.Context {
	return clickhouse.Context(ctx,
		clickhouse.WithSettings(clickhouse.Settings{"keeper_map_strict_mode": 1, "async_insert": 0}),
		clickhouse.WithParameters(params))
}

// The parameter text ClickHouse parses for a String parameter is in its escaped format, so a backslash, tab
// or newline in the value must be escaped.
var paramEscaper = strings.NewReplacer(`\`, `\\`, "\t", `\t`, "\n", `\n`, "\r", `\r`, "\x00", `\0`)

func param(s string) string { return paramEscaper.Replace(s) }

// A time goes in as Unix milliseconds, read with fromUnixTimestamp64Milli: KeeperMap rewrites an ALTER UPDATE
// in a way that can't parse a DateTime64 parameter's text.
func timeParam(t time.Time) string { return strconv.FormatInt(t.UnixMilli(), 10) }

const stateColumns = `group_key, toString(incident_id), status, title, service_name, receiver, group_labels, common_labels,
	alerts, firing, opened_at, updated_at, acknowledged_by, acknowledged_at, silenced_until, slack_channel, slack_ts, version, op`

func (s *Store) Get(ctx context.Context, groupKey string) (*incident.Incident, error) {
	inc, _, err := s.get(ctx, groupKey)
	return inc, err
}

func (s *Store) get(ctx context.Context, groupKey string) (*incident.Incident, string, error) {
	rows, err := s.Conn.Query(stateCtx(ctx, clickhouse.Parameters{"k": param(groupKey)}),
		"SELECT "+stateColumns+" FROM "+s.table("open_incidents")+" WHERE group_key = {k:String}")
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, "", rows.Err()
	}
	var (
		inc          incident.Incident
		status       string
		alerts, op   string
		firing       uint32
		ackAt, silAt time.Time
	)
	if err := rows.Scan(&inc.GroupKey, &inc.ID, &status, &inc.Title, &inc.Service, &inc.Receiver, &inc.GroupLabels,
		&inc.CommonLabels, &alerts, &firing, &inc.OpenedAt, &inc.UpdatedAt, &inc.AcknowledgedBy, &ackAt, &silAt,
		&inc.SlackChannel, &inc.SlackTS, &inc.Version, &op); err != nil {
		return nil, "", err
	}
	inc.Status = incident.Status(status)
	inc.AcknowledgedAt = fromZero(ackAt)
	inc.SilencedUntil = fromZero(silAt)
	var st storedAlerts
	if err := json.Unmarshal([]byte(alerts), &st); err != nil {
		return nil, "", fmt.Errorf("incident %s: its alerts: %w", inc.ID, err)
	}
	inc.Alerts, inc.MaxFiring = st.Alerts, st.MaxFiring
	return &inc, op, rows.Err()
}

// storedAlerts is the alerts column: the firing alerts, plus the most that fired at once, which only the
// history row needs.
type storedAlerts struct {
	Alerts    []alertmanager.Alert `json:"alerts"`
	MaxFiring int                  `json:"max_firing"`
}

func alertsJSON(inc *incident.Incident) (string, error) {
	b, err := json.Marshal(storedAlerts{Alerts: inc.Alerts, MaxFiring: inc.MaxFiring})
	return string(b), err
}

// DateTime64 columns can't be NULL here, so the zero time is stored as the epoch and read back as zero.
func fromZero(t time.Time) time.Time {
	if t.Unix() <= 0 {
		return time.Time{}
	}
	return t
}

func toZero(t time.Time) time.Time {
	if t.IsZero() {
		return time.Unix(0, 0).UTC()
	}
	return t.UTC()
}

func (s *Store) Create(ctx context.Context, inc *incident.Incident) error {
	alerts, err := alertsJSON(inc)
	if err != nil {
		return err
	}
	op := uuid.NewString()
	batch, err := s.Conn.PrepareBatch(stateCtx(ctx, nil), "INSERT INTO "+s.table("open_incidents"))
	if err != nil {
		return err
	}
	defer batch.Close()
	if err := batch.Append(inc.GroupKey, inc.ID, string(inc.Status), inc.Title, inc.Service, inc.Receiver,
		nonNil(inc.GroupLabels), nonNil(inc.CommonLabels), alerts, uint32(len(inc.Alerts)), inc.OpenedAt.UTC(),
		inc.UpdatedAt.UTC(), inc.AcknowledgedBy, toZero(inc.AcknowledgedAt), toZero(inc.SilencedUntil),
		inc.SlackChannel, inc.SlackTS, uint64(0), op); err != nil {
		return err
	}
	if err := batch.Send(); err != nil {
		if isNodeExists(err) {
			return incident.ErrExists
		}
		return err
	}
	inc.Version = 0
	return nil
}

// isNodeExists recognizes Keeper's refusal of a strict-mode INSERT whose key exists.
func isNodeExists(err error) bool { return isKeeperRefusal(err, "Node exists") }

// isBadVersion recognizes Keeper's refusal of a strict-mode ALTER whose key another write changed while it
// ran. Like a stale version that matches no row, it means someone else's write won.
func isBadVersion(err error) bool { return isKeeperRefusal(err, "Bad version") }

func isKeeperRefusal(err error, what string) bool {
	var ex *clickhouse.Exception
	return errors.As(err, &ex) && strings.Contains(ex.Message, "Transaction failed ("+what+")")
}

func (s *Store) Update(ctx context.Context, inc *incident.Incident) error {
	alerts, err := alertsJSON(inc)
	if err != nil {
		return err
	}
	op := uuid.NewString()
	params := clickhouse.Parameters{
		"k":       param(inc.GroupKey),
		"v":       strconv.FormatUint(inc.Version, 10),
		"op":      op,
		"status":  param(string(inc.Status)),
		"alerts":  param(alerts),
		"firing":  strconv.Itoa(len(inc.Alerts)),
		"updated": timeParam(inc.UpdatedAt),
		"ack_by":  param(inc.AcknowledgedBy),
		"ack_at":  timeParam(toZero(inc.AcknowledgedAt)),
		"sil":     timeParam(toZero(inc.SilencedUntil)),
		"channel": param(inc.SlackChannel),
		"ts":      param(inc.SlackTS),
	}
	// common_labels changes as alerts come and go, but it's a Map, whose parameter text is awkward to escape;
	// the alerts carry every label, so it is set once, when the incident opens.
	err = s.Conn.Exec(stateCtx(ctx, params), "ALTER TABLE "+s.table("open_incidents")+` UPDATE
		status = {status:String}, alerts = {alerts:String}, firing = {firing:UInt32},
		updated_at = fromUnixTimestamp64Milli({updated:Int64}, 'UTC'), acknowledged_by = {ack_by:String},
		acknowledged_at = fromUnixTimestamp64Milli({ack_at:Int64}, 'UTC'), silenced_until = fromUnixTimestamp64Milli({sil:Int64}, 'UTC'),
		slack_channel = {channel:String}, slack_ts = {ts:String}, version = version + 1, op = {op:String}
		WHERE group_key = {k:String} AND version = {v:UInt64}`)
	if isBadVersion(err) {
		return incident.ErrConflict
	}
	if err != nil {
		return err
	}
	_, got, err := s.get(ctx, inc.GroupKey)
	if err != nil {
		return err
	}
	if got != op {
		return incident.ErrConflict
	}
	inc.Version++
	return nil
}

func (s *Store) Delete(ctx context.Context, groupKey string, version uint64) error {
	params := clickhouse.Parameters{"k": param(groupKey), "v": strconv.FormatUint(version, 10)}
	if err := s.Conn.Exec(stateCtx(ctx, params),
		"ALTER TABLE "+s.table("open_incidents")+" DELETE WHERE group_key = {k:String} AND version = {v:UInt64}"); err != nil {
		if isBadVersion(err) {
			return incident.ErrConflict
		}
		return err
	}
	cur, _, err := s.get(ctx, groupKey)
	if err != nil {
		return err
	}
	if cur != nil {
		return incident.ErrConflict
	}
	return nil
}

// Count returns the number of open incidents.
func (s *Store) Count(ctx context.Context) (uint64, error) {
	var n uint64
	err := s.Conn.QueryRow(ctx, "SELECT count() FROM "+s.table("open_incidents")).Scan(&n)
	return n, err
}

func (s *Store) RecordAlerts(ctx context.Context, at time.Time, incidentID string, m *alertmanager.Message) error {
	if len(m.Alerts) == 0 {
		return nil
	}
	return s.insert(ctx, "alert_events", func(b driver.Batch) error {
		for _, a := range m.Alerts {
			if err := b.Append(at.UTC(), uuidOrNil(incidentID), m.GroupKey, m.Receiver, a.Status, a.Labels["alertname"],
				a.Labels["service_name"], a.Fingerprint, nonNil(a.Labels), nonNil(a.Annotations), toZero(a.StartsAt),
				toZero(a.EndsAt), a.GeneratorURL); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) RecordEvent(ctx context.Context, e incident.Event) error {
	inc := e.Incident
	return s.insert(ctx, "incident_events", func(b driver.Batch) error {
		return b.Append(e.At.UTC(), inc.ID, inc.GroupKey, e.Name, string(inc.Status), e.Actor, inc.Title, inc.Service,
			uint32(len(inc.Alerts)), e.Detail)
	})
}

func (s *Store) RecordResolved(ctx context.Context, inc *incident.Incident) error {
	var ackAt *time.Time
	if !inc.AcknowledgedAt.IsZero() {
		t := inc.AcknowledgedAt.UTC()
		ackAt = &t
	}
	return s.insert(ctx, "incidents", func(b driver.Batch) error {
		return b.Append(inc.ID, inc.GroupKey, inc.Title, inc.Service, inc.Receiver, nonNil(inc.GroupLabels),
			inc.OpenedAt.UTC(), ackAt, inc.AcknowledgedBy, inc.ResolvedAt.UTC(), inc.ResolvedBy, uint32(inc.MaxFiring),
			inc.SlackChannel, inc.SlackTS)
	})
}

// Notification is one Slack API call, for garmr.notifications.
type Notification struct {
	At          time.Time
	IncidentID  string
	Integration string
	Method      string
	Channel     string
	TS          string
	Err         error
	Duration    time.Duration
}

func (s *Store) RecordNotification(ctx context.Context, n Notification) error {
	errText := ""
	if n.Err != nil {
		errText = n.Err.Error()
	}
	return s.insert(ctx, "notifications", func(b driver.Batch) error {
		return b.Append(n.At.UTC(), uuidOrNil(n.IncidentID), n.Integration, n.Method, n.Channel, n.TS, n.Err == nil, errText,
			uint32(n.Duration.Milliseconds()))
	})
}

func (s *Store) RecordEnrichments(ctx context.Context, inc *incident.Incident, results []incident.EnrichmentResult) error {
	if len(results) == 0 {
		return nil
	}
	return s.insert(ctx, "enrichments", func(b driver.Batch) error {
		for _, r := range results {
			errText, rows := "", ""
			if r.Err != nil {
				errText = r.Err.Error()
			}
			if r.Rows != nil {
				j, err := json.Marshal(r.Rows)
				if err != nil {
					return err
				}
				rows = string(j)
			}
			if err := b.Append(r.At.UTC(), inc.ID, inc.Service, r.Name, r.Err == nil, errText,
				uint32(r.Duration.Milliseconds()), rows); err != nil {
				return err
			}
		}
		return nil
	})
}

// insert appends one batch of history rows. History is append-only, so it goes through the native batch
// insert without strict mode; a few rows per notification don't need async inserts.
func (s *Store) insert(ctx context.Context, table string, rows func(driver.Batch) error) error {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"async_insert": 0}))
	batch, err := s.Conn.PrepareBatch(ctx, "INSERT INTO "+s.table(table))
	if err != nil {
		return fmt.Errorf("%s: %w", table, err)
	}
	defer batch.Close()
	if err := rows(batch); err != nil {
		return fmt.Errorf("%s: %w", table, err)
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("%s: %w", table, err)
	}
	return nil
}

func uuidOrNil(id string) string {
	if id == "" {
		return uuid.Nil.String()
	}
	return id
}

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}
