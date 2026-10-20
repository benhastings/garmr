package slack

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/slack-go/slack"

	"github.com/benhastings/garmr/internal/alertmanager"
	"github.com/benhastings/garmr/internal/incident"
	"github.com/benhastings/garmr/internal/store"
)

// fakeAPI answers Slack's Web API methods garmr calls and keeps what it was sent.
type fakeAPI struct {
	mu    sync.Mutex
	calls []call
	fail  string // a method to answer with an error
}

type call struct {
	method string
	form   map[string]string
}

func (f *fakeAPI) server(t *testing.T) *slack.Client {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		method := strings.TrimPrefix(r.URL.Path, "/")
		form := map[string]string{}
		for k := range r.Form {
			form[k] = r.Form.Get(k)
		}
		f.mu.Lock()
		f.calls = append(f.calls, call{method, form})
		fail := f.fail == method
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if fail {
			_, _ = w.Write([]byte(`{"ok":false,"error":"channel_not_found"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"channel":"C0DEMO","ts":"1760882520.000100","message_ts":"1760882520.000200"}`))
	}))
	t.Cleanup(srv.Close)
	return slack.New("xoxb-test", slack.OptionAPIURL(srv.URL+"/"))
}

type recorder struct {
	mu sync.Mutex
	n  []store.Notification
}

func (r *recorder) RecordNotification(_ context.Context, n store.Notification) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.n = append(r.n, n)
	return nil
}

var opened = time.Date(2026, 10, 19, 14, 2, 0, 0, time.UTC)

func newIncident() *incident.Incident {
	return &incident.Incident{
		GroupKey: `{}:{alertname="ErrorRatioAnomaly", service_name="payments"}`,
		ID:       "9b0d7c1e-0000-4000-8000-000000000001",
		Status:   incident.Firing,
		Title:    "ErrorRatioAnomaly · payments",
		Service:  "payments",
		Alerts: []alertmanager.Alert{{
			Labels:       map[string]string{"alertname": "ErrorRatioAnomaly"},
			Annotations:  map[string]string{"summary": "payments: error ratio is 17.8 deviations above normal", "description": "payments errors on 91% of server spans; normally 25% <b>"},
			GeneratorURL: "http://vmalert/rule",
			Fingerprint:  "a",
		}},
		OpenedAt:     opened,
		SlackChannel: "C0DEMO",
		SlackTS:      "1760882520.000100",
	}
}

func blocksJSON(t *testing.T, inc *incident.Incident, now time.Time) string {
	t.Helper()
	var b strings.Builder
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(Blocks(inc, now)); err != nil {
		t.Fatal(err)
	}
	// slack-go's own MarshalJSON for some blocks escapes HTML regardless.
	return strings.NewReplacer(`\u003c`, "<", `\u003e`, ">", `\u0026`, "&").Replace(b.String())
}

func TestAFiringIncidentHasEveryButton(t *testing.T) {
	got := blocksJSON(t, newIncident(), opened)
	for _, want := range []string{
		`🔴 *Firing* · *ErrorRatioAnomaly · payments*`,
		`• *payments: error ratio is 17.8 deviations above normal*`,
		`normally 25% &lt;b&gt;`, // alert text is escaped
		`<http://vmalert/rule|rule>`,
		`1 firing · opened 14:02 UTC Oct 19`,
		`"action_id":"garmr_acknowledge"`,
		`"action_id":"garmr_silence"`,
		`"action_id":"garmr_resolve"`,
		`\"i\":\"9b0d7c1e-0000-4000-8000-000000000001\"`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in\n%s", want, got)
		}
	}
}

func TestAnAcknowledgedSilencedIncidentDropsThoseButtons(t *testing.T) {
	inc := newIncident()
	inc.Status = incident.Acknowledged
	inc.AcknowledgedBy = "U42"
	inc.AcknowledgedAt = opened.Add(3 * time.Minute)
	inc.SilencedUntil = opened.Add(time.Hour)
	got := blocksJSON(t, inc, opened.Add(10*time.Minute))
	for _, want := range []string{"🟡 *Acknowledged* by <@U42>", "🔕 silenced until 15:02 UTC", "acknowledged 14:05 UTC", "garmr_resolve"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in\n%s", want, got)
		}
	}
	for _, unwanted := range []string{"garmr_acknowledge", "garmr_silence"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("has %s", unwanted)
		}
	}
	// Once the silence ends, the button is back.
	if got := blocksJSON(t, inc, opened.Add(2*time.Hour)); !strings.Contains(got, "garmr_silence") {
		t.Error("no silence button after the silence ended")
	}
}

func TestAResolvedIncidentHasNoButtons(t *testing.T) {
	inc := newIncident()
	inc.Status = incident.Resolved
	inc.ResolvedAt = opened.Add(14 * time.Minute)
	inc.ResolvedBy = "slack:U42"
	got := blocksJSON(t, inc, inc.ResolvedAt)
	if !strings.Contains(got, "✅ *Resolved*") || !strings.Contains(got, "Lasted 14m, resolved by <@U42>") {
		t.Errorf("%s", got)
	}
	if strings.Contains(got, "garmr_") || strings.Contains(got, "• *") {
		t.Errorf("a resolved incident lists buttons or alerts: %s", got)
	}
	if Fallback(inc) != "[RESOLVED] ErrorRatioAnomaly · payments" {
		t.Error(Fallback(inc))
	}
}

func TestOpenPostsAndUpdateEditsThenReplies(t *testing.T) {
	api := &fakeAPI{}
	rec := &recorder{}
	n := &Notifier{API: api.server(t), Channel: "#demo", Recorder: rec, Now: func() time.Time { return opened }}
	inc := newIncident()

	channel, ts, err := n.Open(context.Background(), inc)
	if err != nil || channel != "C0DEMO" || ts != "1760882520.000100" {
		t.Fatalf("%q %q %v", channel, ts, err)
	}
	if err := n.Update(context.Background(), inc, "Acknowledged by <@U42>."); err != nil {
		t.Fatal(err)
	}
	if err := n.Enrichment(context.Background(), inc, []incident.Finding{{Title: "A failing trace", Lines: []string{"`abc`"}}}); err != nil {
		t.Fatal(err)
	}

	var methods []string
	for _, c := range api.calls {
		methods = append(methods, c.method)
	}
	if got := strings.Join(methods, ","); got != "chat.postMessage,chat.update,chat.postMessage,chat.postMessage" {
		t.Fatal(got)
	}
	if c := api.calls[0]; c.form["channel"] != "#demo" || c.form["thread_ts"] != "" || c.form["text"] != "[FIRING:1] ErrorRatioAnomaly · payments" {
		t.Errorf("post: %v", c.form)
	}
	if c := api.calls[1]; c.form["channel"] != "C0DEMO" || c.form["ts"] != inc.SlackTS {
		t.Errorf("update: %v", c.form)
	}
	if c := api.calls[2]; c.form["thread_ts"] != inc.SlackTS || c.form["text"] != "Acknowledged by <@U42>." {
		t.Errorf("reply: %v", c.form)
	}
	if c := api.calls[3]; c.form["thread_ts"] != inc.SlackTS || !strings.Contains(c.form["blocks"], "*A failing trace*") {
		t.Errorf("enrichment: %v", c.form)
	}
	if len(rec.n) != 4 || rec.n[0].Method != "chat.postMessage" || rec.n[0].Err != nil {
		t.Errorf("recorded %+v", rec.n)
	}
}

func TestAFailedCallIsReturnedAndRecorded(t *testing.T) {
	api := &fakeAPI{fail: "chat.postMessage"}
	rec := &recorder{}
	n := &Notifier{API: api.server(t), Channel: "#gone", Recorder: rec}
	if _, _, err := n.Open(context.Background(), newIncident()); err == nil || !strings.Contains(err.Error(), "channel_not_found") {
		t.Fatalf("got %v", err)
	}
	if len(rec.n) != 1 || rec.n[0].Err == nil {
		t.Fatalf("%+v", rec.n)
	}
}

type fakeActor struct {
	got []incident.Action
	err error
}

func (f *fakeActor) Act(_ context.Context, a incident.Action) error {
	f.got = append(f.got, a)
	return f.err
}

func click(actionID string, value string) slack.InteractionCallback {
	var cb slack.InteractionCallback
	cb.Type = slack.InteractionTypeBlockActions
	cb.User = slack.User{ID: "U42", Name: "ben"}
	cb.Channel.ID = "C0DEMO"
	cb.ActionCallback.BlockActions = []*slack.BlockAction{{ActionID: actionID, Value: value}}
	return cb
}

func TestAClickBecomesAnAction(t *testing.T) {
	actor := &fakeActor{}
	l := &Listener{Actor: actor, Log: slog.New(slog.DiscardHandler)}
	value, _ := json.Marshal(buttonValue{IncidentID: "inc-1", GroupKey: "g"})
	l.Interaction(context.Background(), click(actionSilence, string(value)))
	l.Interaction(context.Background(), click("someone_elses_button", string(value)))
	if len(actor.got) != 1 {
		t.Fatalf("%+v", actor.got)
	}
	if a := actor.got[0]; a.Kind != incident.Silence || a.IncidentID != "inc-1" || a.GroupKey != "g" || a.UserID != "U42" || a.UserName != "ben" {
		t.Fatalf("%+v", a)
	}
}

func TestAClickOnAClosedIncidentTellsTheUser(t *testing.T) {
	api := &fakeAPI{}
	actor := &fakeActor{err: incident.ErrGone}
	l := &Listener{Actor: actor, API: api.server(t), Log: slog.New(slog.DiscardHandler)}
	value, _ := json.Marshal(buttonValue{IncidentID: "inc-1", GroupKey: "g"})
	l.Interaction(context.Background(), click(actionAcknowledge, string(value)))
	if len(api.calls) != 1 || api.calls[0].method != "chat.postEphemeral" || api.calls[0].form["user"] != "U42" ||
		api.calls[0].form["text"] != "This incident is already closed." {
		t.Fatalf("%+v", api.calls)
	}
	actor.err = errors.New("boom")
	l.Interaction(context.Background(), click(actionAcknowledge, string(value)))
	if len(api.calls) != 2 || !strings.Contains(api.calls[1].form["text"], "couldn't acknowledge this incident: boom") {
		t.Fatalf("%+v", api.calls)
	}
}
