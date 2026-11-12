package load_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/column"
	"github.com/viant/datly/repository/shape/compile"
	"github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/repository/shape/plan"
)

func TestPreparedColumnsCompileAndLoadOffline(t *testing.T) {
	for _, sql := range []string{"SELECT t.ID AS RecordID, t.NAME FROM RECORDS t WHERE t.ID > 0", "SELECT * FROM RECORDS t"} {
		t.Run(sql, func(t *testing.T) {
			provided := []column.PreparedColumn{{Name: "RecordID", Type: reflect.TypeOf(int64(0)), Tag: `sqlx:"RecordID,primaryKey"`}, {Name: "NAME", Type: reflect.TypeOf(""), Nullable: true}}
			calls := 0
			resolve := func(_ context.Context, request column.PreparedRequest) (*column.PreparedResult, error) {
				calls++
				require.Equal(t, "offline", request.Component)
				require.Equal(t, "no-such-connector", request.Connector)
				return &column.PreparedResult{Columns: provided}, nil
			}
			compiled, err := compile.New().Compile(context.Background(), &shape.Source{Name: "offline", Connector: "no-such-connector", DQL: sql}, shape.WithPreparedColumns(resolve), shape.WithColumnDiscoveryMode(shape.CompileColumnDiscoveryOff))
			require.NoError(t, err)
			p, ok := plan.ResultFrom(compiled)
			require.True(t, ok)
			require.False(t, p.ColumnsDiscovery)
			require.False(t, p.Views[0].ColumnsDiscovery)
			executable := p.Views[0].SQL
			require.Contains(t, executable, "FROM RECORDS")
			provided[0].Name = "mutated"
			artifacts, err := load.New().LoadComponent(context.Background(), compiled)
			require.NoError(t, err)
			require.Equal(t, 1, calls)
			require.Empty(t, artifacts.Resource.Connectors)
			got := artifacts.Resource.Views[0]
			require.Len(t, got.Columns, 2)
			require.Equal(t, "RecordID", got.Columns[0].Name)
			require.Equal(t, reflect.TypeOf(int64(0)), got.Columns[0].ColumnType())
			require.True(t, got.Columns[1].Nullable)
			require.Equal(t, executable, got.Template.Source)
			row := got.Schema.Type()
			for row.Kind() == reflect.Ptr || row.Kind() == reflect.Slice {
				row = row.Elem()
			}
			require.Equal(t, 2, row.NumField())
			require.Equal(t, reflect.TypeOf(int64(0)), row.Field(0).Type)
			// Reusing the plan creates detached runtime metadata.
			got.Columns[0].Name = "changed"
			again, err := load.New().LoadViews(context.Background(), compiled)
			require.NoError(t, err)
			require.Equal(t, "RecordID", again.Views[0].Columns[0].Name)
		})
	}
}

func TestPreparedColumnsEngineOption(t *testing.T) {
	called := false
	engine := shape.New(shape.WithCompiler(compile.New()), shape.WithLoader(load.New()), shape.WithPreparedColumnsDefault(func(context.Context, column.PreparedRequest) (*column.PreparedResult, error) {
		called = true
		return &column.PreparedResult{Columns: []column.PreparedColumn{{Name: "ID", Type: reflect.TypeOf(0)}}}, nil
	}))
	artifact, err := engine.LoadDQLViews(context.Background(), "SELECT * FROM RECORDS")
	require.NoError(t, err)
	require.True(t, called)
	require.Len(t, artifact.Views[0].Columns, 1)
}

func TestPreparedColumnsDoNotWidenFromLinkedSchema(t *testing.T) {
	type wider struct {
		ID     int
		Secret string
	}
	compiled, err := compile.New().Compile(context.Background(), &shape.Source{Name: "records", Type: reflect.TypeOf(wider{}), DQL: "SELECT ID FROM RECORDS records"}, shape.WithPreparedColumns(func(context.Context, column.PreparedRequest) (*column.PreparedResult, error) {
		return &column.PreparedResult{Columns: []column.PreparedColumn{{Name: "ID", Type: reflect.TypeOf(int64(0))}}}, nil
	}))
	require.NoError(t, err)
	artifact, err := load.New().LoadViews(context.Background(), compiled)
	require.NoError(t, err)
	require.Len(t, artifact.Views[0].Columns, 1)
	row := artifact.Views[0].Schema.Type()
	for row.Kind() == reflect.Ptr || row.Kind() == reflect.Slice {
		row = row.Elem()
	}
	require.Equal(t, 1, row.NumField())
	require.Equal(t, reflect.TypeOf(int64(0)), row.Field(0).Type)
}
