package chquery

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"github.com/benhastings/garmr/internal/promapi"
)

// These tests need a ClickHouse server: GARMR_TEST_CLICKHOUSE_ADDR=localhost:9000 go test ./...
// CI starts one as a service container. User and password come from GARMR_TEST_CLICKHOUSE_USER and
// GARMR_TEST_CLICKHOUSE_PASSWORD, default and empty otherwise.
func querier(t *testing.T) *Querier {
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
	return &Querier{Conn: conn, MaxExecutionTime: 2 * time.Second}
}

var at = time.Date(2026, 10, 10, 19, 13, 30, 0, time.UTC)

func query(t *testing.T, q *Querier, sql string) ([]promapi.Sample, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return q.Query(ctx, sql, at, 5*time.Minute)
}

func TestBindsTheEvaluationTimeAndStepAsQueryParameters(t *testing.T) {
	got, err := query(t, querier(t), `SELECT toString({time:DateTime64(3, 'UTC')}) AS t, {step:UInt32} AS value`)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Labels["t"] != "2026-10-10 19:13:30.000" || got[0].Value != 300 {
		t.Fatalf("%+v", got)
	}
}

func TestRunsARuleThatUsesNoParameters(t *testing.T) {
	got, err := query(t, querier(t), `SELECT 'checkout' AS service_name, 1.5 AS value`)
	if err != nil || len(got) != 1 || got[0].Value != 1.5 || got[0].Labels["service_name"] != "checkout" {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestEveryColumnButValueIsALabel(t *testing.T) {
	got, err := query(t, querier(t), `
		SELECT
			'payments'                              AS service_name,
			toLowCardinality('SERVER')              AS span_kind,
			CAST(NULL AS Nullable(String))          AS absent,
			''                                      AS empty,
			toUInt16(503)                           AS status,
			toDateTime64('2026-10-10 19:00:00', 3, 'UTC') AS since,
			toDecimal64(2.25, 2)                    AS value`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"service_name": "payments", "span_kind": "SERVER", "status": "503", "since": "2026-10-10T19:00:00Z"}
	if len(got) != 1 || got[0].Value != 2.25 || len(got[0].Labels) != len(want) {
		t.Fatalf("%+v", got)
	}
	for k, v := range want {
		if got[0].Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, got[0].Labels[k], v)
		}
	}
}

func TestAcceptsEveryNumericValueType(t *testing.T) {
	for _, expr := range []string{"toUInt64(7)", "toInt8(7)", "toFloat32(7)", "toInt128(7)", "toUInt256(7)", "toDecimal128(7, 3)", "true + 6", "toNullable(7)"} {
		got, err := query(t, querier(t), "SELECT "+expr+" AS value")
		if err != nil || len(got) != 1 || got[0].Value != 7 {
			t.Errorf("%s: %+v %v", expr, got, err)
		}
	}
}

func TestDropsRowsWithANullValue(t *testing.T) {
	got, err := query(t, querier(t), `SELECT number AS n, if(number = 1, NULL, number) AS value FROM numbers(3)`)
	if err != nil || len(got) != 2 {
		t.Fatalf("%+v %v", got, err)
	}
}

func TestRejectsARuleWithoutANumericValueColumn(t *testing.T) {
	q := querier(t)
	for sql, want := range map[string]string{
		`SELECT 1 AS z`:          "no column named value",
		`SELECT 'high' AS value`: "must be numeric",
	} {
		_, err := query(t, q, sql)
		var bad *promapi.BadQueryError
		if !errors.As(err, &bad) || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v", sql, err)
		}
	}
}

func TestIsReadOnlyWhateverTheUserMayDo(t *testing.T) {
	q := querier(t)
	// The test user may write, so a write refused here is refused by the querier's settings.
	if err := q.Conn.Exec(context.Background(), `CREATE TABLE IF NOT EXISTS garmr_readonly (a UInt8) ENGINE = MergeTree ORDER BY a`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = q.Conn.Exec(context.Background(), `DROP TABLE IF EXISTS garmr_readonly`)
		_ = q.Conn.Exec(context.Background(), `DROP TABLE IF EXISTS garmr_test`)
	})
	for _, sql := range []string{
		`CREATE TABLE garmr_test (a UInt8) ENGINE = Memory`,
		`INSERT INTO garmr_readonly SELECT 1`,
		`TRUNCATE TABLE garmr_readonly`,
		`SELECT 1 AS value SETTINGS readonly = 0`,
	} {
		if _, err := query(t, q, sql); err == nil || !strings.Contains(strings.ToLower(err.Error()), "readonly") {
			t.Errorf("%s ran: %v", sql, err)
		}
	}
}

func TestStopsAQueryAtMaxExecutionTime(t *testing.T) {
	start := time.Now()
	_, err := query(t, querier(t), `SELECT count() AS value FROM numbers_mt(1e13)`)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 5*time.Second {
		t.Fatalf("after %s: %v", time.Since(start), err)
	}
}
