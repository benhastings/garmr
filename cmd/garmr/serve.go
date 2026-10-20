package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	slackapi "github.com/slack-go/slack"
	"github.com/slack-go/slack/socketmode"

	"github.com/benhastings/garmr/internal/alertmanager"
	"github.com/benhastings/garmr/internal/enrich"
	"github.com/benhastings/garmr/internal/incident"
	"github.com/benhastings/garmr/internal/metrics"
	"github.com/benhastings/garmr/internal/slack"
	"github.com/benhastings/garmr/internal/store"
	"github.com/benhastings/garmr/internal/webhook"
)

func serve(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen-address", envOr("GARMR_LISTEN_ADDRESS", ":8080"), "address to serve the webhook, health and metrics on")
	addr := fs.String("clickhouse-addr", envOr("CLICKHOUSE_ADDR", "localhost:9000"), "ClickHouse native protocol address")
	database := fs.String("database", envOr("GARMR_DATABASE", "garmr"), "the database holding garmr's tables (schema/garmr.sql)")
	user := fs.String("clickhouse-user", envOr("CLICKHOUSE_USER", "garmr"), "ClickHouse user; it writes garmr.* and reads the telemetry")
	passwordFile := fs.String("clickhouse-password-file", envOr("CLICKHOUSE_PASSWORD_FILE", ""),
		"file holding the password; otherwise CLICKHOUSE_PASSWORD is read")
	botTokenFile := fs.String("slack-bot-token-file", envOr("SLACK_BOT_TOKEN_FILE", ""),
		"file holding the Slack bot token (xoxb-, chat:write); otherwise SLACK_BOT_TOKEN is read. Without one, nothing goes to Slack")
	appTokenFile := fs.String("slack-app-token-file", envOr("SLACK_APP_TOKEN_FILE", ""),
		"file holding the Slack app-level token (xapp-, connections:write) for the buttons; otherwise SLACK_APP_TOKEN is read")
	channel := fs.String("slack-channel", envOr("SLACK_CHANNEL", ""), "the Slack channel ID incidents are posted to")
	alertmanagerURL := fs.String("alertmanager-url", envOr("GARMR_ALERTMANAGER_URL", ""), "Alertmanager's URL, for the silence button")
	traceURL := fs.String("trace-url", envOr("GARMR_TRACE_URL", ""), "a link to a trace, with {trace_id} in it, for the failing-trace enrichment")
	silenceFor := fs.Duration("silence-duration", time.Hour, "how long the silence button silences an incident's group")
	logFormat := fs.String("log-format", envOr("GARMR_LOG_FORMAT", "json"), "json or text")
	_ = fs.Parse(args)

	log := newLogger(*logFormat)
	conn, err := openClickHouse(*addr, *database, *user, *passwordFile, "garmr")
	if err != nil {
		return err
	}
	defer conn.Close()

	st := &store.Store{Conn: conn, Database: *database}
	engine := &incident.Engine{
		Store:      st,
		Enricher:   &enrich.Enricher{Conn: conn, TraceURL: *traceURL},
		Log:        log,
		SilenceFor: *silenceFor,
	}
	if *alertmanagerURL != "" {
		engine.Silencer = &alertmanager.Silencer{URL: *alertmanagerURL, Client: &http.Client{Timeout: 10 * time.Second}}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	listener, err := slackFromFlags(ctx, log, engine, st, *botTokenFile, *appTokenFile, *channel)
	if err != nil {
		return err
	}
	errs := make(chan error, 2)
	if listener != nil {
		go func() {
			if err := listener.Run(ctx); err != nil {
				errs <- fmt.Errorf("slack socket mode: %w", err)
			}
		}()
	}

	h := &webhook.Handler{
		Notifier: engine,
		Log:      log,
		Ready: func(ctx context.Context) error {
			if err := conn.Ping(ctx); err != nil {
				return fmt.Errorf("clickhouse: %w", err)
			}
			if listener != nil && !listener.Connected() {
				return errors.New("slack socket mode is not connected")
			}
			return nil
		},
	}
	srv := &http.Server{Addr: *listen, Handler: h.Routes(), ReadHeaderTimeout: 10 * time.Second, WriteTimeout: time.Minute}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()
	go countOpen(ctx, st, log)

	log.Info("serving", "listen", *listen, "clickhouse", *addr, "user", *user, "database", *database,
		"slack", listener != nil, "silences", engine.Silencer != nil, "version", version)
	select {
	case <-ctx.Done():
	case err = <-errs:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	engine.Wait()
	return err
}

// slackFromFlags turns Slack on when a bot token is given. It checks the token, so a wrong one fails at
// start rather than at the first incident, and returns the Socket Mode listener for the buttons.
func slackFromFlags(ctx context.Context, log *slog.Logger, engine *incident.Engine, st *store.Store, botTokenFile, appTokenFile, channel string) (*slack.Listener, error) {
	bot, err := secret(botTokenFile, "SLACK_BOT_TOKEN")
	if err != nil {
		return nil, err
	}
	app, err := secret(appTokenFile, "SLACK_APP_TOKEN")
	if err != nil {
		return nil, err
	}
	if bot == "" && app == "" {
		log.Warn("no Slack bot token: incidents are kept in ClickHouse but not posted")
		return nil, nil
	}
	switch {
	case bot == "":
		return nil, errors.New("a Slack app-level token without a bot token")
	case app == "":
		return nil, errors.New("a Slack bot token without an app-level token, which the buttons need")
	case channel == "":
		return nil, errors.New("-slack-channel is required with Slack")
	}
	api := slackapi.New(bot, slackapi.OptionAppLevelToken(app))
	actx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	auth, err := api.AuthTestContext(actx)
	if err != nil {
		return nil, fmt.Errorf("checking the Slack bot token: %w", err)
	}
	log.Info("slack", "team", auth.Team, "bot", auth.User, "channel", channel)
	engine.Notifier = &slack.Notifier{API: api, Channel: channel, Recorder: st, Log: log}
	return &slack.Listener{Socket: socketmode.New(api), API: api, Actor: engine, Log: log}, nil
}

// countOpen keeps garmr_open_incidents current.
func countOpen(ctx context.Context, st *store.Store, log *slog.Logger) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		n, err := st.Count(ctx)
		if err == nil {
			metrics.OpenIncidents.Set(float64(n))
		} else if ctx.Err() == nil {
			log.Warn("counting open incidents", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
