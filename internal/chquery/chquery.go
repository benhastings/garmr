// Package chquery runs a SQL rule against ClickHouse and turns its rows into samples: the column named value
// is the sample value, and every other column is a label.
//
// The evaluation time and step reach the SQL as ClickHouse query parameters, bound by the server rather than
// spliced into the text:
//
//	{time:DateTime64(3, 'UTC')}   the evaluation time
//	{step:UInt32}                 the query resolution in seconds
//
// A rule may use either, both or neither.
package chquery

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strconv"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/benhastings/garmr/internal/promapi"
)

// ValueColumn is the column that holds the sample value.
const ValueColumn = "value"

type Querier struct {
	Conn driver.Conn
	// MaxExecutionTime bounds every rule evaluation. The client cancels the query at this deadline, and
	// clickhouse-go also sends it to the server as max_execution_time (plus 5s), so a query whose client
	// went away still stops.
	MaxExecutionTime time.Duration
}

func (q *Querier) Ping(ctx context.Context) error { return q.Conn.Ping(ctx) }

func (q *Querier) Query(ctx context.Context, query string, at time.Time, step time.Duration) ([]promapi.Sample, error) {
	if q.MaxExecutionTime > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, q.MaxExecutionTime)
		defer cancel()
	}
	samples, err := q.query(ctx, query, at, step)
	// At the deadline clickhouse-go reports either the context's error or the socket's i/o timeout, which can
	// fire a moment before the context's own timer. Report both as the deadline, so the caller answers with a
	// timeout rather than an execution error.
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && pastDeadline(ctx) {
		err = fmt.Errorf("%w: %v", context.DeadlineExceeded, err)
	}
	return samples, err
}

func pastDeadline(ctx context.Context) bool {
	deadline, ok := ctx.Deadline()
	return ok && !time.Now().Before(deadline)
}

func (q *Querier) query(ctx context.Context, query string, at time.Time, step time.Duration) ([]promapi.Sample, error) {
	// readonly=2: no writes and no DDL, whatever the user may do, while clickhouse-go may still set
	// max_execution_time. A rule can't lower it, since readonly can only be raised.
	settings := clickhouse.Settings{"readonly": 2}
	ctx = clickhouse.Context(ctx,
		clickhouse.WithParameters(clickhouse.Parameters{
			"time": at.UTC().Format("2006-01-02 15:04:05.000"),
			"step": strconv.FormatInt(int64(step/time.Second), 10),
		}),
		clickhouse.WithSettings(settings),
	)
	rows, err := q.Conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	types := rows.ColumnTypes()
	valueAt := -1
	for i, ct := range types {
		if ct.Name() == ValueColumn {
			valueAt = i
		}
	}
	if valueAt < 0 {
		return nil, &promapi.BadQueryError{Msg: "the rule returns no column named " + ValueColumn}
	}

	var samples []promapi.Sample
	for rows.Next() {
		dest := make([]any, len(types))
		for i, ct := range types {
			dest[i] = reflect.New(ct.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		value, ok, err := toFloat(dest[valueAt])
		if err != nil {
			return nil, &promapi.BadQueryError{Msg: fmt.Sprintf("column %s (%s): %v", ValueColumn, types[valueAt].DatabaseTypeName(), err)}
		}
		if !ok {
			continue // a NULL value is no sample, as an absent series is in Prometheus
		}
		labels := make(map[string]string, len(types)-1)
		for i, ct := range types {
			if i == valueAt {
				continue
			}
			if s, ok := toLabel(dest[i]); ok && s != "" {
				labels[ct.Name()] = s
			}
		}
		samples = append(samples, promapi.Sample{Labels: labels, Value: value})
	}
	return samples, rows.Err()
}

// deref follows the pointers Scan fills: one for the column, and one more for a Nullable column. ok is false
// for NULL.
func deref(v any) (reflect.Value, bool) {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return rv, false
		}
		rv = rv.Elem()
	}
	return rv, true
}

func toFloat(v any) (float64, bool, error) {
	rv, ok := deref(v)
	if !ok {
		return 0, false, nil
	}
	switch rv.Kind() {
	case reflect.Float32, reflect.Float64:
		return rv.Float(), true, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(rv.Int()), true, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(rv.Uint()), true, nil
	case reflect.Bool:
		if rv.Bool() {
			return 1, true, nil
		}
		return 0, true, nil
	}
	switch x := rv.Interface().(type) {
	case big.Int: // Int128, Int256 and the unsigned ones
		f, _ := new(big.Float).SetInt(&x).Float64()
		return f, true, nil
	case fmt.Stringer: // Decimal
		f, err := strconv.ParseFloat(x.String(), 64)
		if err != nil {
			return 0, false, fmt.Errorf("not a number: %q", x.String())
		}
		return f, true, nil
	}
	return 0, false, fmt.Errorf("must be numeric, got %s", rv.Type())
}

func toLabel(v any) (string, bool) {
	rv, ok := deref(v)
	if !ok {
		return "", false
	}
	switch x := rv.Interface().(type) {
	case string:
		return x, true
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), true
	case big.Int:
		return x.String(), true
	case fmt.Stringer:
		return x.String(), true
	}
	switch rv.Kind() {
	case reflect.Float32, reflect.Float64:
		return promapi.FormatValue(rv.Float()), true
	}
	return fmt.Sprint(rv.Interface()), true
}
