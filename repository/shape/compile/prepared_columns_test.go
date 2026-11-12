package compile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/column"
	"github.com/viant/datly/repository/shape/plan"
)

func TestPreparedColumnsRejectMixedDiscovery(t *testing.T) {
	_, err := New().Compile(context.Background(), &shape.Source{DQL: "SELECT * FROM RECORDS"}, shape.WithColumnDiscoveryMode(shape.CompileColumnDiscoveryOn), shape.WithPreparedColumns(func(context.Context, column.PreparedRequest) (*column.PreparedResult, error) {
		t.Fatal("provider called")
		return nil, nil
	}))
	require.ErrorContains(t, err, "mutually exclusive")
}

func TestPreparedColumnsExpandedResourcesAndPredicates(t *testing.T) {
	root := t.TempDir()
	embedded := "SELECT ID AS RecordID FROM RECORDS WHERE ID > $Unsafe.MinID"
	require.NoError(t, os.WriteFile(filepath.Join(root, "records.sql"), []byte(embedded), 0600))
	executable := "SELECT * FROM (${embed:records.sql}) t WHERE RecordID < 100"
	p := &plan.Result{Views: []*plan.View{{Name: "records", Mode: "SQLQuery", SQL: executable, Connector: "offline"}}}
	source := &shape.Source{Name: "component", Path: filepath.Join(root, "records.dql")}
	err := applyPreparedColumns(context.Background(), source, p, func(_ context.Context, request column.PreparedRequest) (*column.PreparedResult, error) {
		require.Equal(t, executable, request.ExecutableSQL)
		require.Contains(t, request.SQL, embedded)
		require.Equal(t, "offline", request.Connector)
		request.SQL = "overwritten"
		return &column.PreparedResult{Columns: []column.PreparedColumn{{Name: "RecordID", Type: reflect.TypeOf(int64(0))}}}, nil
	})
	require.NoError(t, err)
	require.Equal(t, executable, p.Views[0].SQL)
	data, err := os.ReadFile(filepath.Join(root, "records.sql"))
	require.NoError(t, err)
	require.Equal(t, embedded, string(data))
}

func TestPreparedColumnsErrorsNeverFallBack(t *testing.T) {
	sentinel := errors.New("authoritative model unavailable")
	cases := []struct {
		name, sql string
		want      string
	}{
		{"provider", "SELECT * FROM RECORDS", "authoritative model unavailable"},
		{"missing embed", "SELECT * FROM (${embed:absent.sql}) t", "embedded SQL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New().Compile(context.Background(), &shape.Source{Name: "records", Path: filepath.Join(t.TempDir(), "records.dql"), DQL: tc.sql}, shape.WithPreparedColumns(func(context.Context, column.PreparedRequest) (*column.PreparedResult, error) { return nil, sentinel }))
			require.ErrorContains(t, err, tc.want)
		})
	}
	p := &plan.Result{Views: []*plan.View{{Name: "records", SQL: "SELECT ID FROM RECORDS", Declaration: &plan.ViewDeclaration{ColumnsConfig: map[string]*plan.ViewColumnConfig{"Missing": {DataType: "string"}}}}}}
	err := applyPreparedColumns(context.Background(), &shape.Source{}, p, func(context.Context, column.PreparedRequest) (*column.PreparedResult, error) {
		return &column.PreparedResult{Columns: []column.PreparedColumn{{Name: "ID", Type: reflect.TypeOf(0)}}}, nil
	})
	require.ErrorContains(t, err, "annotation target Missing is absent")
}

func TestPreparedColumnsEmbeddedCycle(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "cycle.sql"), []byte("${embed:cycle.sql}"), 0600))
	_, err := expandPreparedSQL(&shape.Source{Path: root}, "${embed:cycle.sql}", map[string]bool{})
	require.ErrorContains(t, err, "cyclic")
}
