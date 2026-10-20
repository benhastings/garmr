package alertmanager

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSilenceMatchesExactlyTheGroupLabels(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v2/silences" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		_, _ = w.Write([]byte(`{"silenceID":"8a6c7f3e"}`))
	}))
	defer srv.Close()

	until := time.Date(2026, 10, 19, 15, 0, 0, 0, time.UTC)
	id, err := (&Silencer{URL: srv.URL + "/"}).Silence(context.Background(),
		map[string]string{"service_name": "payments", "alertname": "ErrorRatioAnomaly"}, until, "ben", "garmr incident 1")
	if err != nil || id != "8a6c7f3e" {
		t.Fatalf("%q %v", id, err)
	}
	b, _ := json.Marshal(got["matchers"])
	want := `[{"isEqual":true,"isRegex":false,"name":"alertname","value":"ErrorRatioAnomaly"},{"isEqual":true,"isRegex":false,"name":"service_name","value":"payments"}]`
	if string(b) != want {
		t.Errorf("matchers %s", b)
	}
	if got["endsAt"] != "2026-10-19T15:00:00Z" || got["createdBy"] != "ben" {
		t.Errorf("%v", got)
	}
}

func TestSilenceRefusesToSilenceEverything(t *testing.T) {
	if _, err := (&Silencer{URL: "http://unused"}).Silence(context.Background(), nil, time.Now(), "ben", ""); err == nil {
		t.Fatal("want an error for no labels")
	}
}

func TestSilenceReportsAlertmanagersError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "silence invalid: start time must be before end time", http.StatusBadRequest)
	}))
	defer srv.Close()
	_, err := (&Silencer{URL: srv.URL}).Silence(context.Background(), map[string]string{"a": "b"}, time.Now(), "ben", "")
	if err == nil || !strings.Contains(err.Error(), "start time must be before end time") {
		t.Fatal(err)
	}
}

func TestFiringIsOrderedByFingerprint(t *testing.T) {
	m := Message{Alerts: []Alert{{Status: "firing", Fingerprint: "b"}, {Status: "resolved", Fingerprint: "c"}, {Status: "firing", Fingerprint: "a"}}}
	f := m.Firing()
	if len(f) != 2 || f[0].Fingerprint != "a" || f[1].Fingerprint != "b" {
		t.Fatalf("%+v", f)
	}
}
