// Package metrics holds garmr's Prometheus metrics, served on /metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	NotificationsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "garmr_notifications_received_total",
		Help: "Alertmanager notifications received, by outcome: ok, invalid or error.",
	}, []string{"outcome"})
	IncidentsOpened = promauto.NewCounter(prometheus.CounterOpts{
		Name: "garmr_incidents_opened_total",
		Help: "Incidents opened.",
	})
	IncidentsResolved = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "garmr_incidents_resolved_total",
		Help: "Incidents resolved, by who resolved them: alertmanager or slack.",
	}, []string{"by"})
	SlackRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "garmr_slack_requests_total",
		Help: "Slack Web API calls, by method and outcome: ok or error.",
	}, []string{"method", "outcome"})
	Enrichments = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "garmr_enrichments_total",
		Help: "Enrichment queries run, by name and outcome: ok, empty or error.",
	}, []string{"name", "outcome"})
	Actions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "garmr_actions_total",
		Help: "Slack button clicks, by action and outcome: ok, gone or error.",
	}, []string{"action", "outcome"})
	OpenIncidents = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "garmr_open_incidents",
		Help: "Open incidents, as this replica last counted them.",
	})
)
