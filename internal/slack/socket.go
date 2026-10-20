package slack

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"

	"github.com/benhastings/garmr/internal/incident"
)

// Actor applies a button click; incident.Engine is one.
type Actor interface {
	Act(ctx context.Context, a incident.Action) error
}

// Listener takes the clicks on incidents' buttons over Socket Mode, which needs an app-level token
// (connections:write) and no public URL.
type Listener struct {
	Socket *socketmode.Client
	API    *slack.Client
	Actor  Actor
	Log    *slog.Logger

	connected atomic.Bool
}

// Connected reports whether the Socket Mode connection is up, for readiness.
func (l *Listener) Connected() bool { return l.connected.Load() }

// Run holds the connection, reconnecting as needed, until ctx is done.
func (l *Listener) Run(ctx context.Context) error {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evt := <-l.Socket.Events:
				l.handle(ctx, evt)
			}
		}
	}()
	err := l.Socket.RunContext(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (l *Listener) handle(ctx context.Context, evt socketmode.Event) {
	switch evt.Type {
	case socketmode.EventTypeConnected:
		l.connected.Store(true)
		l.Log.Info("connected to Slack over Socket Mode")
	case socketmode.EventTypeConnecting:
		l.connected.Store(false)
	case socketmode.EventTypeConnectionError, socketmode.EventTypeDisconnect:
		l.connected.Store(false)
		l.Log.Warn("Slack's Socket Mode connection dropped; reconnecting", "event", evt.Type)
	case socketmode.EventTypeInvalidAuth:
		l.connected.Store(false)
		l.Log.Error("Slack refused the app-level token")
	case socketmode.EventTypeInteractive:
		cb, ok := evt.Data.(slack.InteractionCallback)
		if evt.Request != nil {
			// Slack wants the ack within 3 seconds, before any work.
			if err := l.Socket.Ack(*evt.Request); err != nil {
				l.Log.Warn("acknowledging a Slack interaction", "err", err)
			}
		}
		if ok {
			go l.Interaction(ctx, cb)
		}
	}
}

// Interaction applies every incident button in one block_actions callback.
func (l *Listener) Interaction(ctx context.Context, cb slack.InteractionCallback) {
	if cb.Type != slack.InteractionTypeBlockActions {
		return
	}
	for _, ba := range cb.ActionCallback.BlockActions {
		kind, ok := map[string]incident.ActionKind{
			actionAcknowledge: incident.Acknowledge,
			actionSilence:     incident.Silence,
			actionResolve:     incident.Resolve,
		}[ba.ActionID]
		if !ok {
			continue
		}
		var v buttonValue
		if err := json.Unmarshal([]byte(ba.Value), &v); err != nil {
			l.Log.Warn("a button with a value garmr didn't write", "action", ba.ActionID, "err", err)
			continue
		}
		actx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := l.Actor.Act(actx, incident.Action{Kind: kind, IncidentID: v.IncidentID, GroupKey: v.GroupKey, UserID: cb.User.ID, UserName: userName(cb.User)})
		cancel()
		switch {
		case err == nil:
			l.Log.Info("incident action", "action", kind, "incident", v.IncidentID, "user", cb.User.ID)
		case errors.Is(err, incident.ErrGone):
			l.tell(ctx, cb, "This incident is already closed.")
		default:
			l.Log.Error("incident action failed", "action", kind, "incident", v.IncidentID, "user", cb.User.ID, "err", err)
			l.tell(ctx, cb, fmt.Sprintf("garmr couldn't %s this incident: %v", kind, err))
		}
	}
}

func userName(u slack.User) string {
	if u.Name != "" {
		return u.Name
	}
	return u.ID
}

// tell answers the user who clicked with a message only they see.
func (l *Listener) tell(ctx context.Context, cb slack.InteractionCallback, text string) {
	if l.API == nil {
		return
	}
	if _, err := l.API.PostEphemeralContext(ctx, cb.Channel.ID, cb.User.ID, slack.MsgOptionText(text, false)); err != nil {
		l.Log.Warn("telling the user", "user", cb.User.ID, "err", err)
	}
}
