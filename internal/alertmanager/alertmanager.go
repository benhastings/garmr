// Package alertmanager holds what garmr exchanges with Alertmanager: the webhook payload it receives, and the
// v2 silences API it calls when someone silences an incident.
package alertmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// Message is Alertmanager's webhook payload, version 4: one notification for one aggregation group.
type Message struct {
	Version           string            `json:"version"`
	GroupKey          string            `json:"groupKey"`
	TruncatedAlerts   int               `json:"truncatedAlerts"`
	Status            string            `json:"status"`
	Receiver          string            `json:"receiver"`
	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	ExternalURL       string            `json:"externalURL"`
	Alerts            []Alert           `json:"alerts"`
}

type Alert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

const (
	StatusFiring   = "firing"
	StatusResolved = "resolved"
)

// Validate rejects a payload that can't be a notification, so a misdirected POST isn't taken for an empty
// group.
func (m *Message) Validate() error {
	if m.Version != "4" {
		return fmt.Errorf("unsupported webhook version %q, want 4", m.Version)
	}
	if m.GroupKey == "" {
		return fmt.Errorf("groupKey is empty")
	}
	for i, a := range m.Alerts {
		if a.Status != StatusFiring && a.Status != StatusResolved {
			return fmt.Errorf("alerts[%d]: status %q", i, a.Status)
		}
	}
	return nil
}

// Firing returns the alerts that are firing, ordered by fingerprint so two notifications with the same alerts
// compare equal.
func (m *Message) Firing() []Alert {
	var out []Alert
	for _, a := range m.Alerts {
		if a.Status == StatusFiring {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Fingerprint < out[j].Fingerprint })
	return out
}

// Silencer creates silences through Alertmanager's v2 API.
type Silencer struct {
	URL    string // Alertmanager's base URL, such as http://alertmanager:9093
	Client *http.Client
}

// Silence silences every alert that has all of labels, from now until until, and returns the silence's ID.
func (s *Silencer) Silence(ctx context.Context, labels map[string]string, until time.Time, createdBy, comment string) (string, error) {
	if len(labels) == 0 {
		return "", fmt.Errorf("no labels to match, which would silence everything")
	}
	type matcher struct {
		Name    string `json:"name"`
		Value   string `json:"value"`
		IsRegex bool   `json:"isRegex"`
		IsEqual bool   `json:"isEqual"`
	}
	body := struct {
		Matchers  []matcher `json:"matchers"`
		StartsAt  time.Time `json:"startsAt"`
		EndsAt    time.Time `json:"endsAt"`
		CreatedBy string    `json:"createdBy"`
		Comment   string    `json:"comment"`
	}{StartsAt: time.Now().UTC(), EndsAt: until.UTC(), CreatedBy: createdBy, Comment: comment}
	for _, name := range sortedKeys(labels) {
		body.Matchers = append(body.Matchers, matcher{Name: name, Value: labels[name], IsEqual: true})
	}
	b, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(s.URL, "/")+"/api/v2/silences", bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("alertmanager answered %s: %s", resp.Status, strings.TrimSpace(string(rb)))
	}
	var out struct {
		SilenceID string `json:"silenceID"`
	}
	if err := json.Unmarshal(rb, &out); err != nil {
		return "", fmt.Errorf("alertmanager's answer: %w", err)
	}
	return out.SilenceID, nil
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
