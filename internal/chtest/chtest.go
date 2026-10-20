// Package chtest connects tests to a throwaway ClickHouse server, and gives each test its own copy of
// garmr's schema.
//
// The tests need GARMR_TEST_CLICKHOUSE_ADDR (localhost:9000, say), and the server needs a Keeper and
// keeper_map_path_prefix for garmr's KeeperMap table: testdata/clickhouse/keeper.xml configures both. User
// and password come from GARMR_TEST_CLICKHOUSE_USER and GARMR_TEST_CLICKHOUSE_PASSWORD, default and empty
// otherwise.
package chtest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Conn connects to the test server, or skips the test when there is none.
func Conn(t *testing.T) driver.Conn {
	t.Helper()
	addr := os.Getenv("GARMR_TEST_CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("GARMR_TEST_CLICKHOUSE_ADDR is not set")
	}
	user := os.Getenv("GARMR_TEST_CLICKHOUSE_USER")
	if user == "" {
		user = "default"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{Username: user, Password: os.Getenv("GARMR_TEST_CLICKHOUSE_PASSWORD")},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// Database creates a database for this test alone with garmr's schema in it, and drops it afterwards. It
// returns the database's name.
func Database(t *testing.T, conn driver.Conn) string {
	t.Helper()
	db := fmt.Sprintf("garmr_test_%d", time.Now().UnixNano())
	_, file, _, _ := runtime.Caller(0)
	b, err := os.ReadFile(filepath.Join(filepath.Dir(file), "..", "..", "schema", "garmr.sql"))
	if err != nil {
		t.Fatal(err)
	}
	schema := strings.NewReplacer(
		"DATABASE IF NOT EXISTS garmr;", "DATABASE IF NOT EXISTS "+db+";",
		"garmr.", db+".",
		"'/garmr/", "'/"+db+"/",
	).Replace(string(b))
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC") })
	Exec(t, conn, schema)
	return db
}

// Exec runs each statement of sql, a script of ;-terminated statements without ; inside them.
func Exec(t *testing.T, conn driver.Conn, sql string) {
	t.Helper()
	for _, stmt := range strings.Split(stripComments(sql), ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if err := conn.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("%v\n%s", err, stmt)
		}
	}
}

func stripComments(sql string) string {
	var out []string
	for _, line := range strings.Split(sql, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}
