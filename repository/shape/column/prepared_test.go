package column

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func preparedID() PreparedColumn {
	return PreparedColumn{Name: "RecordID", Type: reflect.TypeOf(int64(0)), Tag: `sqlx:"RecordID,primaryKey"`}
}
func preparedName() PreparedColumn {
	return PreparedColumn{Name: "NAME", Type: reflect.TypeOf(""), Nullable: true}
}

func TestResolvePreparedProjectionValidation(t *testing.T) {
	tests := []struct {
		name, sql, table, want string
		cols                   []PreparedColumn
	}{
		{name: "closed", sql: "SELECT t.ID AS RecordID, t.NAME FROM RECORDS t", cols: []PreparedColumn{preparedID(), preparedName()}, table: "RECORDS"},
		{name: "missing", sql: "SELECT ID AS RecordID, NAME FROM RECORDS", cols: []PreparedColumn{preparedID()}, want: "SQL requires 2"},
		{name: "wider", sql: "SELECT ID AS RecordID FROM RECORDS", cols: []PreparedColumn{preparedID(), preparedName()}, want: "SQL requires 1"},
		{name: "order", sql: "SELECT ID AS RecordID, NAME FROM RECORDS", cols: []PreparedColumn{preparedName(), preparedID()}, want: "in that order"},
		{name: "alias", sql: "SELECT ID AS DifferentID FROM RECORDS", cols: []PreparedColumn{preparedID()}, want: "DifferentID"},
		{name: "duplicate", sql: "SELECT * FROM RECORDS", cols: []PreparedColumn{preparedID(), preparedID()}, want: "duplicate"},
		{name: "empty name", sql: "SELECT * FROM RECORDS", cols: []PreparedColumn{{Type: reflect.TypeOf(0)}}, want: "empty"},
		{name: "untyped", sql: "SELECT * FROM RECORDS", cols: []PreparedColumn{{Name: "ID"}}, want: "untyped"},
		{name: "interface", sql: "SELECT * FROM RECORDS", cols: []PreparedColumn{{Name: "ID", Type: reflect.TypeOf((*any)(nil)).Elem()}}, want: "untyped"},
		{name: "missing result", sql: "SELECT * FROM RECORDS", want: "missing"},
		{name: "table drift", sql: "SELECT ID AS RecordID FROM RECORDS", cols: []PreparedColumn{preparedID()}, table: "OTHER", want: "direct SQL source"},
		{name: "join table claim", sql: "SELECT t.ID AS RecordID FROM RECORDS t JOIN OTHER o ON o.ID = t.ID", cols: []PreparedColumn{preparedID()}, table: "RECORDS", want: "direct SQL source"},
		{name: "closed subquery", sql: "SELECT * FROM (SELECT ID AS RecordID FROM RECORDS) t", cols: []PreparedColumn{preparedID(), preparedName()}, want: "SQL requires 1"},
		{name: "closed cte", sql: "WITH source AS (SELECT ID AS RecordID FROM RECORDS) SELECT * FROM source", cols: []PreparedColumn{preparedID(), preparedName()}, want: "SQL requires 1"},
		{name: "mixed wildcard", sql: "SELECT *, NAME FROM RECORDS", cols: []PreparedColumn{preparedID(), preparedName()}, want: "mixed wildcard"},
		{name: "wildcard", sql: "SELECT * FROM RECORDS", cols: []PreparedColumn{preparedID(), preparedName()}},
		{name: "predicate", sql: "SELECT ID AS RecordID FROM RECORDS WHERE ID = $Unsafe.ID", cols: []PreparedColumn{preparedID()}},
		{name: "dynamic projection", sql: "SELECT $Unsafe.Column AS RecordID FROM RECORDS", cols: []PreparedColumn{preparedID()}, want: "dynamic projection"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ResolvePrepared(context.Background(), PreparedRequest{View: "records", SQL: test.sql}, func(context.Context, PreparedRequest) (*PreparedResult, error) {
				return &PreparedResult{Columns: test.cols, Table: test.table}, nil
			})
			if test.want != "" {
				require.ErrorContains(t, err, test.want)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.cols, got)
		})
	}
}

func TestResolvePreparedErrorsAndIsolation(t *testing.T) {
	source := "SELECT ID AS RecordID FROM RECORDS"
	result := &PreparedResult{Columns: []PreparedColumn{preparedID()}}
	columns, err := ResolvePrepared(context.Background(), PreparedRequest{SQL: source}, func(_ context.Context, request PreparedRequest) (*PreparedResult, error) {
		request.SQL = "overwritten"
		return result, nil
	})
	require.NoError(t, err)
	result.Columns[0].Name = "changed"
	require.Equal(t, "RecordID", columns[0].Name)
	runtime := MaterializePrepared(columns)
	runtime[0].Tag = "changed"
	require.Equal(t, `sqlx:"RecordID,primaryKey"`, columns[0].Tag)
	sentinel := errors.New("model unavailable")
	_, err = ResolvePrepared(context.Background(), PreparedRequest{SQL: source}, func(context.Context, PreparedRequest) (*PreparedResult, error) { return nil, sentinel })
	require.ErrorIs(t, err, sentinel)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = ResolvePrepared(ctx, PreparedRequest{SQL: source}, func(context.Context, PreparedRequest) (*PreparedResult, error) {
		t.Fatal("called after cancellation")
		return nil, nil
	})
	require.ErrorIs(t, err, context.Canceled)
}
