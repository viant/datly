package sqlite

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	sqlxread "github.com/viant/sqlx/io/read"
)

// Query keeps a verification statement and its bound arguments together.
type Query struct {
	SQL  string
	Args []any
}

// AssertQuery reads typed rows through SQLX and compares them in query order.
// Expected must be a slice of structs or pointers to structs. Specify ORDER BY
// in the query when deterministic ordering is part of the assertion.
func (h *Harness) AssertQuery(t testing.TB, ctx context.Context, query Query, expected any) {
	t.Helper()
	actual, err := h.ReadQuery(ctx, query, reflect.TypeOf(expected))
	if err != nil {
		t.Fatalf("verify query %q: %v", query.SQL, err)
	}
	want := reflect.ValueOf(expected)
	// No rows has one database meaning regardless of nil/empty Go allocation.
	if want.Len() == 0 && reflect.ValueOf(actual).Len() == 0 {
		return
	}
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("query %q rows differ:\nwant: %#v\n got: %#v", query.SQL, expected, actual)
	}
}

// ReadQuery returns a typed slice for tests that need additional assertions.
// SQLX remains the row-mapping owner; this harness only collects its typed rows.
func (h *Harness) ReadQuery(ctx context.Context, query Query, sliceType reflect.Type) (any, error) {
	if h == nil || h.DB == nil {
		return nil, fmt.Errorf("SQLite harness database is required")
	}
	if sliceType == nil || sliceType.Kind() != reflect.Slice {
		return nil, fmt.Errorf("expected row type must be a slice")
	}
	element := sliceType.Elem()
	rowType := element
	if rowType.Kind() == reflect.Pointer {
		rowType = rowType.Elem()
	}
	if rowType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected rows must be structs or struct pointers, got %s", element)
	}
	reader, err := sqlxread.New(ctx, h.DB, query.SQL, func() any { return reflect.New(rowType).Interface() })
	if err != nil {
		return nil, err
	}
	defer func() {
		if stmt := reader.Stmt(); stmt != nil {
			_ = stmt.Close()
		}
	}()
	rows := reflect.MakeSlice(sliceType, 0, 0)
	err = reader.QueryAll(ctx, func(row any) error {
		item := reflect.ValueOf(row)
		if element.Kind() != reflect.Pointer {
			item = item.Elem()
		}
		rows = reflect.Append(rows, item)
		return nil
	}, query.Args...)
	if err != nil {
		return nil, err
	}
	return rows.Interface(), nil
}
