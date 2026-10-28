package reader_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	"github.com/viant/datly/sql/reader/compiler"
	xh "github.com/viant/xdatly/handler"
)

type consumedRow struct {
	ID    int     `sqlx:"id"`
	Label *string `sqlx:"label"`
}

func (r *consumedRow) OnFetch(context.Context) error {
	if r.Label != nil {
		value := strings.ToUpper(*r.Label)
		r.Label = &value
	}
	return nil
}

type consumedOutput struct{ Rows []*consumedRow }

func TestConsumeRowsSQLite(t *testing.T) {
	type useCase struct {
		description string
		stopAt      int
		expected    int
		failure     error
		cancelAt    int
	}
	for _, tc := range []useCase{{"all rows without retaining output", 0, 3, nil, 0}, {"consumer failure stops once", 2, 2, errors.New("invalid connection: workbook consumer failed"), 0}, {"wrapped EOF is failure", 2, 2, fmt.Errorf("destination closed: %w", io.EOF), 0}, {"cancellation closes cursor", 0, 2, context.Canceled, 2}} {
		t.Run(tc.description, func(t *testing.T) {
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER,label TEXT)", "INSERT INTO records VALUES(1,'one'),(2,NULL),(3,'three')"))
			component := &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id,label FROM records ORDER BY id"}}}
			in, out := reflect.TypeFor[struct{}](), reflect.TypeFor[consumedOutput]()
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: in, OutputType: out, DirectViewField: "Rows"})
			require.NoError(t, err)
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: in, OutputType: out, Plan: plan, SQL: &dsql.SQLComponent{DB: db.DB}})
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var seen []*consumedRow
			stopped := tc.failure
			consumer := xh.ConsumeRows(func(ctx context.Context, row *consumedRow) error {
				seen = append(seen, row)
				if row.ID == tc.cancelAt {
					cancel()
				}
				if row.ID == tc.stopAt {
					return stopped
				}
				return nil
			})
			actual, err := execution.ReadRows(ctx, &struct{}{}, nil, nil, consumer)
			if tc.failure != nil {
				require.ErrorIs(t, err, stopped)
			} else {
				require.NoError(t, err)
				require.Empty(t, actual.(*consumedOutput).Rows)
			}
			require.Len(t, seen, tc.expected)
			require.Equal(t, 1, seen[0].ID)
			require.Equal(t, "ONE", *seen[0].Label)
			require.Nil(t, seen[1].Label)
			require.NotSame(t, seen[0], seen[1])
			require.Zero(t, db.DB.Stats().InUse)
			ordinary, err := execution.Read(t.Context(), &struct{}{}, nil, nil)
			require.NoError(t, err)
			require.Len(t, ordinary.(*consumedOutput).Rows, 3)
		})
	}
}

func TestConsumeRowsAdmissionSQLite(t *testing.T) {
	type useCase struct {
		description string
		consumer    *xh.RowConsumer
		transaction bool
		singleton   bool
		expected    string
	}
	for _, tc := range []useCase{
		{description: "singleton envelope", consumer: xh.ConsumeRows(func(context.Context, *consumedRow) error { return nil }), singleton: true, expected: "collection result"},
		{description: "missing callback", consumer: xh.ConsumeRows[*consumedRow](nil), expected: "callback is required"},
		{description: "wrong generated type", consumer: xh.ConsumeRows(func(context.Context, *struct{}) error { return nil }), expected: "does not match generated row"},
		{description: "transaction retains ordinary drain semantics", consumer: xh.ConsumeRows(func(context.Context, *consumedRow) error { return nil }), transaction: true, expected: "active transaction"},
	} {
		t.Run(tc.description, func(t *testing.T) {
			db := sqlite.New(t)
			component := &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT id,label FROM missing_records"}}}
			in, out := reflect.TypeFor[struct{}](), reflect.TypeFor[consumedOutput]()
			if tc.singleton {
				out = reflect.TypeFor[struct{ Rows *consumedRow }]()
			}
			plan, err := compiler.Compile(compiler.Input{Component: component, InputType: in, OutputType: out, DirectViewField: "Rows"})
			require.NoError(t, err)
			source := &dsql.SQLComponent{DB: db.DB}
			if tc.transaction {
				source.Tx, err = db.DB.BeginTx(t.Context(), nil)
				require.NoError(t, err)
				defer source.Tx.Rollback()
			}
			execution, err := reader.NewExecution(reader.Config{Component: component, InputType: in, OutputType: out, Plan: plan, SQL: source})
			require.NoError(t, err)
			_, err = execution.ReadRows(t.Context(), &struct{}{}, nil, nil, tc.consumer)
			require.ErrorContains(t, err, tc.expected)
		})
	}
}
