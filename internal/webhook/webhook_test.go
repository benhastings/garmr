package webhook

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/benhastings/garmr/internal/alertmanager"
)

type fakeNotifier struct {
	got []*alertmanager.Message
	err error
}

func (f *fakeNotifier) Notify(ctx context.Context, m *alertmanager.Message) error {
	f.got = append(f.got, m)
	return f.err
}

// A notification as Alertmanager v0.34 sends it.
const payload = `{
  "receiver": "garmr",
  "status": "firing",
  "alerts": [{
    "status": "firing",
    "labels": {"alertname": "ErrorRatioAnomaly", "service_name": "payments", "severity": "warning"},
    "annotations": {"summary": "payments: error ratio is 17.8 deviations above normal"},
    "startsAt": "2026-10-19T14:01:30.123Z",
    "endsAt": "0001-01-01T00:00:00Z",
    "generatorURL": "http://vmalert:8880/vmalert/alert?group_id=1&alert_id=2",
    "fingerprint": "6f5d2b0c3a1e4f77"
  }],
  "groupLabels": {"alertname": "ErrorRatioAnomaly", "service_name": "payments"},
  "commonLabels": {"alertname": "ErrorRatioAnomaly", "service_name": "payments", "severity": "warning"},
  "commonAnnotations": {},
  "externalURL": "http://alertmanager:9093",
  "version": "4",
  "groupKey": "{}/{alertname!=\"Watchdog\"}:{alertname=\"ErrorRatioAnomaly\", service_name=\"payments\"}",
  "truncatedAlerts": 0
}`

func post(h *Handler, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/alertmanager", strings.NewReader(body)))
	return rec
}

func handler(n Notifier) *Handler {
	return &Handler{Notifier: n, Log: slog.New(slog.DiscardHandler), Ready: func(context.Context) error { return nil }}
}

func TestANotificationIsApplied(t *testing.T) {
	n := &fakeNotifier{}
	if rec := post(handler(n), payload); rec.Code != http.StatusOK {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	if len(n.got) != 1 {
		t.Fatal(len(n.got))
	}
	m := n.got[0]
	if m.GroupLabels["service_name"] != "payments" || len(m.Alerts) != 1 || m.Alerts[0].Fingerprint != "6f5d2b0c3a1e4f77" ||
		m.Alerts[0].StartsAt.Second() != 30 || !m.Alerts[0].EndsAt.IsZero() {
		t.Fatalf("%+v", m)
	}
}

func TestAFailureAnswers500SoAlertmanagerRetries(t *testing.T) {
	if rec := post(handler(&fakeNotifier{err: errors.New("clickhouse is down")}), payload); rec.Code != http.StatusInternalServerError {
		t.Fatal(rec.Code)
	}
}

func TestAnythingElseIsRefused(t *testing.T) {
	n := &fakeNotifier{}
	for _, body := range []string{
		`not json`,
		`{"version": "3", "groupKey": "g"}`,
		`{"version": "4", "groupKey": ""}`,
		`{"version": "4", "groupKey": "g", "alerts": [{"status": "pending"}]}`,
	} {
		if rec := post(handler(n), body); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d", body, rec.Code)
		}
	}
	if len(n.got) != 0 {
		t.Fatal("applied an invalid notification")
	}
}

func TestReadyFollowsItsCheck(t *testing.T) {
	h := handler(&fakeNotifier{})
	h.Ready = func(context.Context) error { return errors.New("slack socket mode is not connected") }
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/-/ready", nil))
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "socket mode") {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}

func TestMetricsAreServed(t *testing.T) {
	h := handler(&fakeNotifier{})
	post(h, payload)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(rec.Body.String(), `garmr_notifications_received_total{outcome="ok"}`) {
		t.Fatal(rec.Body.String())
	}
}
