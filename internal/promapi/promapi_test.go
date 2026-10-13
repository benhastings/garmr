package promapi

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type call struct {
	query string
	at    time.Time
	step  time.Duration
}

type fakeQuerier struct {
	calls   []call
	samples func(at time.Time) []Sample
	err     error
}

func (f *fakeQuerier) Query(_ context.Context, q string, at time.Time, step time.Duration) ([]Sample, error) {
	f.calls = append(f.calls, call{q, at, step})
	if f.err != nil {
		return nil, f.err
	}
	return f.samples(at), nil
}

func (f *fakeQuerier) Ping(context.Context) error { return f.err }

type body struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	Data      struct {
		ResultType string            `json:"resultType"`
		Result     []json.RawMessage `json:"result"`
	} `json:"data"`
}

func do(t *testing.T, h *Handler, method, path string, params url.Values) (int, body) {
	t.Helper()
	req := httptest.NewRequest(method, path+"?"+params.Encode(), nil)
	// vmalert posts with the parameters in the URL, an empty body and this content type.
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)
	var b body
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatalf("response is not JSON: %s", rec.Body.String())
	}
	return rec.Code, b
}

const rule = "SELECT service_name, z AS value\nFROM otel.zscores WHERE ts = {time:DateTime64(3, 'UTC')}"

func TestQueryPassesTheRuleThroughWithItsTimeAndStep(t *testing.T) {
	f := &fakeQuerier{samples: func(time.Time) []Sample {
		return []Sample{{Labels: map[string]string{"service_name": "payments"}, Value: 4.2}}
	}}
	code, b := do(t, &Handler{Querier: f}, http.MethodPost, "/api/v1/query",
		url.Values{"query": {rule}, "time": {"2026-10-10T19:13:30Z"}, "step": {"300s"}})

	if code != http.StatusOK || b.Status != "success" || b.Data.ResultType != "vector" {
		t.Fatalf("got %d %+v", code, b)
	}
	if len(f.calls) != 1 || f.calls[0].query != rule {
		t.Fatalf("the rule did not reach the querier verbatim: %+v", f.calls)
	}
	if want := time.Date(2026, 10, 10, 19, 13, 30, 0, time.UTC); !f.calls[0].at.Equal(want) || f.calls[0].step != 5*time.Minute {
		t.Fatalf("time or step: %+v", f.calls[0])
	}
	if got := string(b.Data.Result[0]); got != `{"metric":{"service_name":"payments"},"value":[1791659610,"4.2"]}` {
		t.Fatalf("sample: %s", got)
	}
}

func TestQueryDefaultsToNowAndFiveMinutes(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	f := &fakeQuerier{samples: func(time.Time) []Sample { return nil }}
	code, b := do(t, &Handler{Querier: f, Now: func() time.Time { return now }}, http.MethodGet, "/api/v1/query",
		url.Values{"query": {"SELECT 1 AS value"}})
	if code != http.StatusOK || len(b.Data.Result) != 0 {
		t.Fatalf("got %d %+v", code, b)
	}
	if !f.calls[0].at.Equal(now) || f.calls[0].step != DefaultStep {
		t.Fatalf("%+v", f.calls[0])
	}
}

func TestQueryReportsErrorsSoVmalertMarksTheRuleUnhealthy(t *testing.T) {
	for name, tc := range map[string]struct {
		err      error
		params   url.Values
		code     int
		kind     string
		contains string
	}{
		"ClickHouse rejects the SQL": {
			err: errors.New("code: 47, message: Unknown identifier"), params: url.Values{"query": {"SELECT nope"}},
			code: http.StatusUnprocessableEntity, kind: "execution", contains: "Unknown identifier",
		},
		"the rule has no value column": {
			err: &BadQueryError{Msg: "the rule returns no column named value"}, params: url.Values{"query": {"SELECT 1"}},
			code: http.StatusBadRequest, kind: "bad_data", contains: "named value",
		},
		"the query times out": {
			err: context.DeadlineExceeded, params: url.Values{"query": {"SELECT sleep(3)"}},
			code: http.StatusServiceUnavailable, kind: "timeout",
		},
		"no query": {params: url.Values{}, code: http.StatusBadRequest, kind: "bad_data", contains: "required"},
		"a bad time": {
			params: url.Values{"query": {"SELECT 1 AS value"}, "time": {"yesterday"}},
			code:   http.StatusBadRequest, kind: "bad_data", contains: "time",
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeQuerier{err: tc.err, samples: func(time.Time) []Sample { return nil }}
			code, b := do(t, &Handler{Querier: f}, http.MethodPost, "/api/v1/query", tc.params)
			if code != tc.code || b.Status != "error" || b.ErrorType != tc.kind || !strings.Contains(b.Error, tc.contains) {
				t.Fatalf("got %d %+v", code, b)
			}
		})
	}
}

func TestQueryRejectsTwoRowsWithTheSameLabels(t *testing.T) {
	f := &fakeQuerier{samples: func(time.Time) []Sample {
		return []Sample{
			{Labels: map[string]string{"service_name": "checkout"}, Value: 1},
			{Labels: map[string]string{"service_name": "checkout"}, Value: 2},
		}
	}}
	code, b := do(t, &Handler{Querier: f}, http.MethodPost, "/api/v1/query", url.Values{"query": {"SELECT ..."}})
	if code != http.StatusBadRequest || !strings.Contains(b.Error, "same labels") {
		t.Fatalf("got %d %+v", code, b)
	}
}

func TestQueryRangeEvaluatesOncePerStep(t *testing.T) {
	f := &fakeQuerier{samples: func(at time.Time) []Sample {
		return []Sample{{Labels: map[string]string{"service_name": "checkout"}, Value: float64(at.Minute())}}
	}}
	code, b := do(t, &Handler{Querier: f}, http.MethodPost, "/api/v1/query_range", url.Values{
		"query": {rule}, "start": {"2026-10-10T10:00:00Z"}, "end": {"2026-10-10T10:02:00Z"}, "step": {"60s"}, "nocache": {"1"},
	})
	if code != http.StatusOK || b.Data.ResultType != "matrix" || len(f.calls) != 3 {
		t.Fatalf("got %d %+v after %d calls", code, b, len(f.calls))
	}
	if got := string(b.Data.Result[0]); got != `{"metric":{"service_name":"checkout"},"values":[[1791626400,"0"],[1791626460,"1"],[1791626520,"2"]]}` {
		t.Fatalf("series: %s", got)
	}
}

func TestQueryRangeRefusesTooManyPoints(t *testing.T) {
	f := &fakeQuerier{samples: func(time.Time) []Sample { return nil }}
	code, b := do(t, &Handler{Querier: f}, http.MethodGet, "/api/v1/query_range", url.Values{
		"query": {"SELECT 1 AS value"}, "start": {"0"}, "end": {"86400"}, "step": {"1"},
	})
	if code != http.StatusBadRequest || len(f.calls) != 0 || !strings.Contains(b.Error, "exceeds") {
		t.Fatalf("got %d %+v after %d calls", code, b, len(f.calls))
	}
}

func TestParseTimeAcceptsBothPrometheusForms(t *testing.T) {
	want := time.Date(2026, 10, 10, 19, 13, 30, 500_000_000, time.UTC)
	for _, s := range []string{"2026-10-10T19:13:30.5Z", "1791659610.5", "2026-10-10T21:13:30.5+02:00"} {
		got, err := ParseTime(s)
		if err != nil || !got.Equal(want) {
			t.Errorf("ParseTime(%q) = %v, %v", s, got, err)
		}
	}
}

func TestFormatValueSpellsSpecialValuesLikePrometheus(t *testing.T) {
	for v, want := range map[float64]string{math.Inf(1): "+Inf", math.Inf(-1): "-Inf", 0.1: "0.1", 1e21: "1000000000000000000000"} {
		if got := FormatValue(v); got != want {
			t.Errorf("FormatValue(%v) = %q, want %q", v, got, want)
		}
	}
	if got := FormatValue(math.NaN()); got != "NaN" {
		t.Errorf("NaN: %q", got)
	}
}

func TestReadyFollowsClickHouse(t *testing.T) {
	rec := httptest.NewRecorder()
	(&Handler{Querier: &fakeQuerier{err: errors.New("connection refused")}}).Routes().
		ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/-/ready", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("ready with ClickHouse down: %d", rec.Code)
	}
}
