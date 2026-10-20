// Package slack keeps one Slack thread per incident, and takes the clicks on its buttons over Socket Mode.
//
// The thread's first message is the incident as it is now: status, alerts, who acknowledged it, and the
// Acknowledge, Silence and Resolve buttons. garmr edits it on every change, and replies in the thread with
// what changed, so the channel shows each incident's current state and the thread its history.
package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/slack-go/slack"

	"github.com/benhastings/garmr/internal/alertmanager"
	"github.com/benhastings/garmr/internal/incident"
	"github.com/benhastings/garmr/internal/metrics"
	"github.com/benhastings/garmr/internal/store"
)

// Recorder keeps a row for every Slack call.
type Recorder interface {
	RecordNotification(ctx context.Context, n store.Notification) error
}

// Notifier posts and edits incidents' messages with a bot token (chat:write).
type Notifier struct {
	API      *slack.Client
	Channel  string // the channel ID, or a name the bot can post to
	Recorder Recorder
	Log      *slog.Logger
	Now      func() time.Time
}

var _ incident.Notifier = (*Notifier)(nil)

func (n *Notifier) now() time.Time {
	if n.Now != nil {
		return n.Now().UTC()
	}
	return time.Now().UTC()
}

func (n *Notifier) Open(ctx context.Context, inc *incident.Incident) (string, string, error) {
	start := time.Now()
	channel, ts, err := n.API.PostMessageContext(ctx, n.Channel, n.message(inc)...)
	n.record(ctx, inc, "chat.postMessage", channel, ts, start, err)
	return channel, ts, err
}

func (n *Notifier) Update(ctx context.Context, inc *incident.Incident, note string) error {
	start := time.Now()
	_, _, _, err := n.API.UpdateMessageContext(ctx, inc.SlackChannel, inc.SlackTS, n.message(inc)...)
	n.record(ctx, inc, "chat.update", inc.SlackChannel, inc.SlackTS, start, err)
	if err != nil || note == "" {
		return err
	}
	return n.reply(ctx, inc, slack.MsgOptionText(note, false))
}

func (n *Notifier) Enrichment(ctx context.Context, inc *incident.Incident, findings []incident.Finding) error {
	blocks := make([]slack.Block, 0, len(findings))
	var fallback []string
	for _, f := range findings {
		text := "*" + f.Title + "*\n" + strings.Join(f.Lines, "\n")
		blocks = append(blocks, slack.NewSectionBlock(mrkdwn(truncate(text, 3000)), nil, nil))
		fallback = append(fallback, f.Title)
	}
	return n.reply(ctx, inc, slack.MsgOptionBlocks(blocks...),
		slack.MsgOptionText(strings.Join(fallback, " · "), false))
}

func (n *Notifier) reply(ctx context.Context, inc *incident.Incident, opts ...slack.MsgOption) error {
	start := time.Now()
	opts = append(opts, slack.MsgOptionTS(inc.SlackTS), slack.MsgOptionDisableLinkUnfurl())
	_, ts, err := n.API.PostMessageContext(ctx, inc.SlackChannel, opts...)
	n.record(ctx, inc, "chat.postMessage", inc.SlackChannel, ts, start, err)
	return err
}

func (n *Notifier) record(ctx context.Context, inc *incident.Incident, method, channel, ts string, start time.Time, err error) {
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	metrics.SlackRequests.WithLabelValues(method, outcome).Inc()
	if n.Recorder == nil {
		return
	}
	rerr := n.Recorder.RecordNotification(ctx, store.Notification{
		At: n.now(), IncidentID: inc.ID, Integration: "slack", Method: method, Channel: channel, TS: ts, Err: err,
		Duration: time.Since(start),
	})
	if rerr != nil && n.Log != nil {
		n.Log.Error("recording a Slack call", "incident", inc.ID, "err", rerr)
	}
}

// The action IDs of the buttons.
const (
	actionAcknowledge = "garmr_acknowledge"
	actionSilence     = "garmr_silence"
	actionResolve     = "garmr_resolve"
)

// buttonValue is what each button carries: enough to find the incident, and to refuse a click on an older
// incident of the same group.
type buttonValue struct {
	IncidentID string `json:"i"`
	GroupKey   string `json:"g"`
}

func (n *Notifier) message(inc *incident.Incident) []slack.MsgOption {
	return []slack.MsgOption{
		slack.MsgOptionText(Fallback(inc), false),
		slack.MsgOptionBlocks(Blocks(inc, n.now())...),
		slack.MsgOptionDisableLinkUnfurl(),
	}
}

// Fallback is the message's plain text, for notifications: "[FIRING:2] ErrorRatioAnomaly · payments".
func Fallback(inc *incident.Incident) string {
	switch inc.Status {
	case incident.Resolved:
		return "[RESOLVED] " + inc.Title
	case incident.Acknowledged:
		return fmt.Sprintf("[ACKNOWLEDGED:%d] %s", len(inc.Alerts), inc.Title)
	}
	return fmt.Sprintf("[FIRING:%d] %s", len(inc.Alerts), inc.Title)
}

// maxAlerts is how many alerts the message lists; the rest are counted.
const maxAlerts = 5

// Blocks draws the incident's first message as it is at now.
func Blocks(inc *incident.Incident, now time.Time) []slack.Block {
	var head string
	switch inc.Status {
	case incident.Resolved:
		head = "✅ *Resolved*"
	case incident.Acknowledged:
		head = fmt.Sprintf("🟡 *Acknowledged* by <@%s>", inc.AcknowledgedBy)
	default:
		head = "🔴 *Firing*"
	}
	blocks := []slack.Block{slack.NewSectionBlock(mrkdwn(head+" · *"+escape(inc.Title)+"*"), nil, nil)}

	if inc.Status != incident.Resolved && len(inc.Alerts) > 0 {
		var lines []string
		for i, a := range inc.Alerts {
			if i == maxAlerts {
				lines = append(lines, fmt.Sprintf("…and %d more", len(inc.Alerts)-maxAlerts))
				break
			}
			lines = append(lines, alertLines(a))
		}
		blocks = append(blocks, slack.NewSectionBlock(mrkdwn(truncate(strings.Join(lines, "\n"), 3000)), nil, nil))
	}

	var facts []string
	if inc.Status == incident.Resolved {
		by := "its alerts resolved"
		if u, ok := strings.CutPrefix(inc.ResolvedBy, "slack:"); ok {
			by = fmt.Sprintf("by <@%s>", u)
		}
		facts = append(facts, fmt.Sprintf("Lasted %s, resolved %s", incident.HumanDuration(inc.ResolvedAt.Sub(inc.OpenedAt)), by))
	} else {
		facts = append(facts, fmt.Sprintf("%d firing", len(inc.Alerts)))
	}
	facts = append(facts, "opened "+inc.OpenedAt.UTC().Format("15:04 UTC Jan 2"))
	if inc.Status != incident.Resolved && inc.Silenced(now) {
		facts = append(facts, "🔕 silenced until "+inc.SilencedUntil.UTC().Format("15:04 UTC"))
	}
	if inc.Status == incident.Acknowledged {
		facts = append(facts, "acknowledged "+inc.AcknowledgedAt.UTC().Format("15:04 UTC"))
	}
	if inc.Service != "" {
		facts = append(facts, "service `"+escape(inc.Service)+"`")
	}
	blocks = append(blocks, slack.NewContextBlock("", mrkdwn(strings.Join(facts, " · "))))

	if inc.Status == incident.Resolved {
		return blocks
	}
	value, _ := json.Marshal(buttonValue{IncidentID: inc.ID, GroupKey: inc.GroupKey})
	var buttons []slack.BlockElement
	if inc.Status == incident.Firing {
		buttons = append(buttons, slack.NewButtonBlockElement(actionAcknowledge, string(value), plain("Acknowledge")).WithStyle(slack.StylePrimary))
	}
	if !inc.Silenced(now) {
		buttons = append(buttons, slack.NewButtonBlockElement(actionSilence, string(value), plain("Silence 1h")))
	}
	buttons = append(buttons, slack.NewButtonBlockElement(actionResolve, string(value), plain("Resolve")).
		WithConfirm(slack.NewConfirmationBlockObject(plain("Resolve this incident?"),
			mrkdwn("If its alerts are still firing, Alertmanager's next notification opens a new incident."),
			plain("Resolve"), plain("Cancel"))))
	return append(blocks, slack.NewActionBlock("garmr_actions", buttons...))
}

// alertLines is one alert: its summary in bold, then its description.
func alertLines(a alertmanager.Alert) string {
	line := "• *" + escape(incident.AlertLine(a)) + "*"
	if d := strings.TrimSpace(a.Annotations["description"]); d != "" {
		line += "\n   " + escape(truncate(strings.Join(strings.Fields(d), " "), 300))
	}
	if a.GeneratorURL != "" {
		line += fmt.Sprintf(" <%s|rule>", a.GeneratorURL)
	}
	return line
}

func mrkdwn(s string) *slack.TextBlockObject {
	return slack.NewTextBlockObject(slack.MarkdownType, s, false, false)
}
func plain(s string) *slack.TextBlockObject {
	return slack.NewTextBlockObject(slack.PlainTextType, s, false, false)
}

// escape makes text from alerts safe for mrkdwn: Slack reads &, < and > as markup.
var escaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

func escape(s string) string { return escaper.Replace(s) }

func truncate(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}
