// Command garmr is the ClickHouse-native incident layer:
//
//	garmr serve         turn Alertmanager's notifications into incidents, with a Slack thread each
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
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/benhastings/garmr/internal/chquery"
	"github.com/benhastings/garmr/internal/promapi"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

const usage = `garmr: a ClickHouse-native incident layer

Usage:
  garmr serve [flags]         turn Alertmanager's notifications into incidents, with a Slack thread each
  garmr sql-adapter [flags]   serve vmalert's queries by running ClickHouse SQL rules
  garmr version

Run "garmr <command> -h" for its flags.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		if err := serve(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "garmr serve:", err)
			os.Exit(1)
		}
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
	conn, err := openClickHouse(*addr, *database, *user, *passwordFile, "garmr-sql-adapter")
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

// openClickHouse connects with the password from passwordFile or, without one, CLICKHOUSE_PASSWORD.
func openClickHouse(addr, database, user, passwordFile, product string) (driver.Conn, error) {
	password, err := secret(passwordFile, "CLICKHOUSE_PASSWORD")
	if err != nil {
		return nil, err
	}
	return clickhouse.Open(&clickhouse.Options{
		Addr:        []string{addr},
		Auth:        clickhouse.Auth{Database: database, Username: user, Password: password},
		ClientInfo:  clickhouse.ClientInfo{Products: []struct{ Name, Version string }{{Name: product, Version: version}}},
		DialTimeout: 10 * time.Second,
	})
}

// secret reads file, trimmed, or the environment variable env when file is empty.
func secret(file, env string) (string, error) {
	if file == "" {
		return os.Getenv(env), nil
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
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
