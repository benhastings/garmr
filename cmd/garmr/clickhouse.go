package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// clickhouseFlags are the connection settings serve and sql-adapter share. user and database differ between
// the two commands, so each declares its own.
type clickhouseFlags struct {
	addr          *string
	passwordFile  *string
	tls           *string
	tlsCAFile     *string
	tlsServerName *string
}

// addClickHouseFlags declares the address, password and TLS flags on fs, each defaulting to its
// environment variable.
func addClickHouseFlags(fs *flag.FlagSet) clickhouseFlags {
	return clickhouseFlags{
		addr: fs.String("clickhouse-addr", envOr("CLICKHOUSE_ADDR", "localhost:9000"), "ClickHouse native protocol address"),
		passwordFile: fs.String("clickhouse-password-file", envOr("CLICKHOUSE_PASSWORD_FILE", ""),
			"file holding the password; otherwise CLICKHOUSE_PASSWORD is read"),
		tls: fs.String("clickhouse-tls", envOr("CLICKHOUSE_TLS_ENABLED", "false"),
			"true connects over TLS and verifies the server's certificate; there is no option to skip verification"),
		tlsCAFile: fs.String("clickhouse-tls-ca-file", envOr("CLICKHOUSE_TLS_CA_FILE", ""),
			"PEM CA bundle that signed the server's certificate; empty uses the system roots"),
		tlsServerName: fs.String("clickhouse-tls-server-name", envOr("CLICKHOUSE_TLS_SERVER_NAME", ""),
			"the name the server's certificate must carry, also sent as SNI; empty uses the host in -clickhouse-addr"),
	}
}

// openClickHouse connects with the password from passwordFile or, without one, CLICKHOUSE_PASSWORD, over
// TLS when the flags ask for it.
func openClickHouse(f clickhouseFlags, database, user, product string) (driver.Conn, error) {
	opts, err := clickhouseOptions(f, database, user, product)
	if err != nil {
		return nil, err
	}
	return clickhouse.Open(opts)
}

func clickhouseOptions(f clickhouseFlags, database, user, product string) (*clickhouse.Options, error) {
	password, err := secret(*f.passwordFile, "CLICKHOUSE_PASSWORD")
	if err != nil {
		return nil, err
	}
	tlsConfig, err := clickhouseTLS(*f.tls, *f.tlsCAFile, *f.tlsServerName)
	if err != nil {
		return nil, err
	}
	return &clickhouse.Options{
		Addr:        []string{*f.addr},
		Auth:        clickhouse.Auth{Database: database, Username: user, Password: password},
		ClientInfo:  clickhouse.ClientInfo{Products: []struct{ Name, Version string }{{Name: product, Version: version}}},
		DialTimeout: 10 * time.Second,
		TLS:         tlsConfig,
	}, nil
}

// clickhouseTLS returns the TLS config for the ClickHouse connection, or nil for plaintext. The server's
// certificate is always verified: against the bundle in caFile (the system roots when empty), and against
// serverName (the dialled host when empty). No client certificate is presented. A CA file or server name
// without TLS enabled is an error rather than a setting silently ignored.
func clickhouseTLS(enabled, caFile, serverName string) (*tls.Config, error) {
	on, err := strconv.ParseBool(enabled)
	if err != nil {
		return nil, fmt.Errorf("CLICKHOUSE_TLS_ENABLED (-clickhouse-tls): %q is not true or false", enabled)
	}
	if !on {
		if caFile != "" || serverName != "" {
			return nil, errors.New("CLICKHOUSE_TLS_CA_FILE or CLICKHOUSE_TLS_SERVER_NAME is set but CLICKHOUSE_TLS_ENABLED is not true; enable TLS or unset them")
		}
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("CLICKHOUSE_TLS_CA_FILE: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CLICKHOUSE_TLS_CA_FILE: no PEM certificate in %s", caFile)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}
