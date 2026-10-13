// Package promapi serves the slice of the Prometheus HTTP API that vmalert calls, /api/v1/query and
// /api/v1/query_range, and answers each query by handing its text to a Querier. The text is not PromQL: in
// garmr it is a ClickHouse SQL rule, which vmalert passes through unparsed with -rule.validateExpressions=false.
package promapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Sample is one result row: every column but the value is a label.
type Sample struct {
	Labels map[string]string
	Value  float64
}

// Querier runs one rule evaluation at time at, with step as the query resolution.
type Querier interface {
	Query(ctx context.Context, query string, at time.Time, step time.Duration) ([]Sample, error)
	Ping(ctx context.Context) error
}

// BadQueryError marks a result that can't be a Prometheus vector, such as a missing value column. It is the
// rule's fault, so it is reported as bad_data rather than as an execution error.
type BadQueryError struct{ Msg string }

func (e *BadQueryError) Error() string { return e.Msg }

// MaxRangePoints caps query_range like Prometheus does, since each point is one query.
const MaxRangePoints = 11000

// DefaultStep is what vmalert sends as step unless -datasource.queryStep says otherwise.
const DefaultStep = 5 * time.Minute

type Handler struct {
	Querier Querier
	Log     *slog.Logger
	Now     func() time.Time
}

func (h *Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/query", h.query)
	mux.HandleFunc("/api/v1/query_range", h.queryRange)
	mux.HandleFunc("/-/healthy", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/-/ready", h.ready)
	return mux
}

func (h *Handler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

func (h *Handler) query(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	q := r.Form.Get("query")
	if strings.TrimSpace(q) == "" {
		writeError(w, http.StatusBadRequest, "bad_data", "query is required")
		return
	}
	at := h.now()
	if v := r.Form.Get("time"); v != "" {
		t, err := ParseTime(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_data", "time: "+err.Error())
			return
		}
		at = t
	}
	step := DefaultStep
	if v := r.Form.Get("step"); v != "" {
		d, err := ParseDuration(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "bad_data", "step: "+err.Error())
			return
		}
		step = d
	}
	samples, err := h.Querier.Query(r.Context(), q, at, step)
	if err != nil {
		h.queryError(w, q, err)
		return
	}
	if err := checkUnique(samples); err != nil {
		h.queryError(w, q, err)
		return
	}
	result := make([]vectorSample, 0, len(samples))
	for _, s := range samples {
		result = append(result, vectorSample{Metric: s.Labels, Value: point{at, s.Value}})
	}
	sortByLabels(result, func(v vectorSample) map[string]string { return v.Metric })
	writeJSON(w, http.StatusOK, response{Status: "success", Data: &data{ResultType: "vector", Result: result}})
}

func (h *Handler) queryRange(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", err.Error())
		return
	}
	q := r.Form.Get("query")
	if strings.TrimSpace(q) == "" {
		writeError(w, http.StatusBadRequest, "bad_data", "query is required")
		return
	}
	start, err := ParseTime(r.Form.Get("start"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", "start: "+err.Error())
		return
	}
	end, err := ParseTime(r.Form.Get("end"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad_data", "end: "+err.Error())
		return
	}
	step, err := ParseDuration(r.Form.Get("step"))
	if err != nil || step <= 0 {
		writeError(w, http.StatusBadRequest, "bad_data", "step must be a positive duration")
		return
	}
	if end.Before(start) {
		writeError(w, http.StatusBadRequest, "bad_data", "end is before start")
		return
	}
	if points := end.Sub(start)/step + 1; points > MaxRangePoints {
		writeError(w, http.StatusBadRequest, "bad_data",
			fmt.Sprintf("%d points exceeds the maximum of %d; raise step or shorten the range", points, MaxRangePoints))
		return
	}

	series := map[string]*matrixSeries{}
	for at := start; !at.After(end); at = at.Add(step) {
		samples, err := h.Querier.Query(r.Context(), q, at, step)
		if err == nil {
			err = checkUnique(samples)
		}
		if err != nil {
			h.queryError(w, q, err)
			return
		}
		for _, s := range samples {
			key := labelKey(s.Labels)
			if series[key] == nil {
				series[key] = &matrixSeries{Metric: s.Labels}
			}
			series[key].Values = append(series[key].Values, point{at, s.Value})
		}
	}
	result := make([]matrixSeries, 0, len(series))
	for _, s := range series {
		result = append(result, *s)
	}
	sortByLabels(result, func(m matrixSeries) map[string]string { return m.Metric })
	writeJSON(w, http.StatusOK, response{Status: "success", Data: &data{ResultType: "matrix", Result: result}})
}

func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := h.Querier.Ping(ctx); err != nil {
		http.Error(w, "clickhouse: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ok\n"))
}

func (h *Handler) queryError(w http.ResponseWriter, q string, err error) {
	var bad *BadQueryError
	switch {
	case errors.As(err, &bad):
		writeError(w, http.StatusBadRequest, "bad_data", err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusServiceUnavailable, "timeout", err.Error())
	case errors.Is(err, context.Canceled):
		writeError(w, http.StatusServiceUnavailable, "canceled", err.Error())
	default:
		writeError(w, http.StatusUnprocessableEntity, "execution", err.Error())
	}
	if h.Log != nil {
		h.Log.Warn("query failed", "error", err, "query", q)
	}
}

// checkUnique rejects two rows with the same label set, which Prometheus never returns in one vector and
// which would make vmalert's alerts ambiguous.
func checkUnique(samples []Sample) error {
	seen := make(map[string]struct{}, len(samples))
	for _, s := range samples {
		key := labelKey(s.Labels)
		if _, dup := seen[key]; dup {
			return &BadQueryError{Msg: "two rows have the same labels " + key + "; group by every label column"}
		}
		seen[key] = struct{}{}
	}
	return nil
}

func labelKey(labels map[string]string) string {
	names := make([]string, 0, len(labels))
	for n := range labels {
		names = append(names, n)
	}
	sort.Strings(names)
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
		b.WriteString("=")
		b.WriteString(strconv.Quote(labels[n]))
	}
	b.WriteByte('}')
	return b.String()
}

func sortByLabels[T any](items []T, labels func(T) map[string]string) {
	sort.Slice(items, func(i, j int) bool { return labelKey(labels(items[i])) < labelKey(labels(items[j])) })
}

// ParseTime accepts the two forms the Prometheus API allows: RFC 3339 and Unix seconds with a fraction.
func ParseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, errors.New("missing")
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		sec, frac := math.Modf(f)
		return time.Unix(int64(sec), int64(math.Round(frac*1e9))).UTC(), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q is neither RFC 3339 nor Unix seconds", s)
	}
	return t.UTC(), nil
}

// ParseDuration accepts a Go or Prometheus duration ("300s", "5m", "1h") or a number of seconds.
func ParseDuration(s string) (time.Duration, error) {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Duration(f * float64(time.Second)), nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("%q is not a duration", s)
	}
	return d, nil
}

type response struct {
	Status    string `json:"status"`
	Data      *data  `json:"data,omitempty"`
	ErrorType string `json:"errorType,omitempty"`
	Error     string `json:"error,omitempty"`
}

type data struct {
	ResultType string `json:"resultType"`
	Result     any    `json:"result"`
}

type vectorSample struct {
	Metric map[string]string `json:"metric"`
	Value  point             `json:"value"`
}

type matrixSeries struct {
	Metric map[string]string `json:"metric"`
	Values []point           `json:"values"`
}

// point marshals as Prometheus does: [<unix seconds>, "<value>"].
type point struct {
	T time.Time
	V float64
}

func (p point) MarshalJSON() ([]byte, error) {
	ts := float64(p.T.UnixMilli()) / 1000
	return json.Marshal([]any{ts, FormatValue(p.V)})
}

// FormatValue writes a sample value the way Prometheus does, including its spellings of NaN and infinity.
func FormatValue(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func writeError(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, response{Status: "error", ErrorType: kind, Error: msg})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
