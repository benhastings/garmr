// Package webhook serves garmr's HTTP API: Alertmanager's webhook, health, readiness and metrics.
package webhook

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/benhastings/garmr/internal/alertmanager"
	"github.com/benhastings/garmr/internal/metrics"
)

// Notifier applies a notification; incident.Engine is one.
type Notifier interface {
	Notify(ctx context.Context, m *alertmanager.Message) error
}

// maxBody bounds a notification. Alertmanager's max_alerts caps the alerts in one, and the default receiver
// config sends every alert in the group, so this is generous.
const maxBody = 8 << 20

type Handler struct {
	Notifier Notifier
	// Ready reports whether garmr can serve: ClickHouse answers and, with Slack on, Socket Mode is up.
	Ready func(ctx context.Context) error
	Log   *slog.Logger
	// Timeout bounds applying one notification; 30s when zero. It doesn't end with the request, so a
	// notification Alertmanager stops waiting for is still applied whole.
	Timeout time.Duration
}

func (h *Handler) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/alertmanager", h.alertmanager)
	mux.HandleFunc("/-/healthy", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/-/ready", h.ready)
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func (h *Handler) alertmanager(w http.ResponseWriter, r *http.Request) {
	var m alertmanager.Message
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	if err := dec.Decode(&m); err != nil {
		h.invalid(w, "the body isn't an Alertmanager webhook payload: "+err.Error())
		return
	}
	if err := m.Validate(); err != nil {
		h.invalid(w, err.Error())
		return
	}
	timeout := h.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), timeout)
	defer cancel()
	if err := h.Notifier.Notify(ctx, &m); err != nil {
		metrics.NotificationsReceived.WithLabelValues("error").Inc()
		h.Log.Error("applying a notification", "group", m.GroupKey, "status", m.Status, "err", err)
		// 500, so Alertmanager retries and counts the failure in alertmanager_notifications_failed_total.
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	metrics.NotificationsReceived.WithLabelValues("ok").Inc()
	w.WriteHeader(http.StatusOK)
}

func (h *Handler) invalid(w http.ResponseWriter, msg string) {
	metrics.NotificationsReceived.WithLabelValues("invalid").Inc()
	h.Log.Warn("refused a notification", "err", msg)
	http.Error(w, msg, http.StatusBadRequest)
}

func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := h.Ready(ctx); err != nil {
		http.Error(w, "not ready: "+err.Error(), http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ok\n"))
}
