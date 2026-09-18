package reader_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	"github.com/viant/xdatly/response"
)

type statusRow struct{ ID int }
type statusEnvelope struct {
	Data            []*statusRow
	response.Status `parameter:",kind=output,in=status"`
}

func TestReaderSuccessStatusSQLite(t *testing.T) {
	h := sqlite.New(t)
	ctx := context.Background()
	require.NoError(t, h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1)"))
	for _, tc := range []struct {
		name, sql string
		fail      bool
	}{
		{"populated", "SELECT id FROM records", false},
		{"empty", "SELECT id FROM records WHERE 1=0", false},
		{"failed", "SELECT id FROM missing_table", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component := &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: tc.sql}}}
			inputType, outputType := reflect.TypeFor[struct{}](), reflect.TypeFor[statusEnvelope]()
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: inputType, OutputType: outputType, DirectViewField: "Data"})
			require.NoError(t, err)
			require.Equal(t, "Status", plan.OutputStatusField)
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: inputType, OutputType: outputType, Plan: plan, SQL: &dsql.SQLComponent{DB: h.DB}})
			require.NoError(t, err)
			out, err := execution.Read(ctx, &struct{}{}, nil, nil)
			if tc.fail {
				require.Error(t, err)
				require.Nil(t, out)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "ok", out.(*statusEnvelope).Status.Status)
			if tc.name == "empty" {
				require.Empty(t, out.(*statusEnvelope).Data)
			}
		})
	}
}
