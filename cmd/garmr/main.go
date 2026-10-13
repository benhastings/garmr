// Command garmr is the ClickHouse-native incident layer. Today it has one subcommand:
//
//	garmr sql-adapter   serve vmalert's Prometheus-API queries by running ClickHouse SQL rules
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/benhastings/garmr/internal/chquery"
	"github.com/benhastings/garmr/internal/promapi"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `garmr: a ClickHouse-native incident layer

Usage:
  garmr sql-adapter [flags]   serve vmalert's queries by running ClickHouse SQL rules
  garmr version

Run "garmr sql-adapter -h" for its flags.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "sql-adapter":
		if err := sqlAdapter(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "garmr sql-adapter:", err)
			os.Exit(1)
		}
	case "version":
		fmt.Println(version)
	case "-h", "--help", "help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "garmr: unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
}

func sqlAdapter(args []string) error {
	fs := flag.NewFlagSet("sql-adapter", flag.ExitOnError)
	listen := fs.String("listen-address", envOr("GARMR_LISTEN_ADDRESS", ":8080"), "address to serve the Prometheus API on")
	addr := fs.String("clickhouse-addr", envOr("CLICKHOUSE_ADDR", "localhost:9000"), "ClickHouse native protocol address")
	database := fs.String("clickhouse-database", envOr("CLICKHOUSE_DATABASE", "otel"), "default database for rules")
	user := fs.String("clickhouse-user", envOr("CLICKHOUSE_USER", "otel_reader"), "ClickHouse user; give it SELECT only")
	passwordFile := fs.String("clickhouse-password-file", envOr("CLICKHOUSE_PASSWORD_FILE", ""),
		"file holding the password; otherwise CLICKHOUSE_PASSWORD is read")
	timeout := fs.Duration("query-timeout", 30*time.Second, "max_execution_time for each rule evaluation")
	logFormat := fs.String("log-format", envOr("GARMR_LOG_FORMAT", "json"), "json or text")
	_ = fs.Parse(args)

	log := newLogger(*logFormat)
	password := os.Getenv("CLICKHOUSE_PASSWORD")
	if *passwordFile != "" {
		b, err := os.ReadFile(*passwordFile)
		if err != nil {
			return err
		}
		password = strings.TrimSpace(string(b))
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{*addr},
		Auth:        clickhouse.Auth{Database: *database, Username: *user, Password: password},
		ClientInfo:  clickhouse.ClientInfo{Products: []struct{ Name, Version string }{{Name: "garmr-sql-adapter", Version: version}}},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	h := &promapi.Handler{Querier: &chquery.Querier{Conn: conn, MaxExecutionTime: *timeout}, Log: log}
	srv := &http.Server{
		Addr:              *listen,
		Handler:           h.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
		WriteTimeout:      *timeout + 30*time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()

	log.Info("serving", "listen", *listen, "clickhouse", *addr, "user", *user, "database", *database, "version", version)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func newLogger(format string) *slog.Logger {
	if format == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, nil))
}
