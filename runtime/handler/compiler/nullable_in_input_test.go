package compiler

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/predicate/velty"
	"github.com/viant/datly/spec"
	xhandler "github.com/viant/xdatly/handler"
)

type nullableSelectionBinder struct{ input any }

func (b nullableSelectionBinder) Bind(context.Context, any) error { return nil }
func (b nullableSelectionBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	return b.input, key == xhandler.InputKey, nil
}

// This exercises the native request binder, rather than encoding NULL as a
// query string token. Pointer elements preserve NULL separately from "null".
func TestNullableSelectionBodyInput(t *testing.T) {
	type input struct {
		Segments []*string `parameter:"Segments,kind=body,in=segments,required=false"`
	}
	compiled, err := New(Input{Component: &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/records"}}}, InputType: reflect.TypeFor[input]()}).Compile()
	require.NoError(t, err)
	program, err := velty.Compile(velty.CompileInput{Component: &spec.Component{Parameters: []*spec.Parameter{{Name: "Segments", Predicates: []*spec.Predicate{{Name: "nullable_in", Args: []string{"r", "segment"}}}}}}, InputType: reflect.TypeFor[input]()})
	require.NoError(t, err)
	type row struct {
		ID int `sqlx:"id"`
	}
	h := sqlite.New(t)
	require.NoError(t, h.ExecStatements(t.Context(), "CREATE TABLE records(id INTEGER, tenant INTEGER, segment TEXT)", "INSERT INTO records VALUES(1,7,NULL),(2,7,'Cedar'),(3,7,'null'),(4,8,NULL)"))
	route, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/records"})
	require.True(t, ok)
	injector, err := bindly.NewInjector()
	require.NoError(t, err)
	for _, tc := range []struct {
		wire      string
		size      int
		firstNull bool
		last      string
		want      []row
	}{
		{`{}`, 0, false, "", []row{{1}, {2}, {3}}},
		{`{"segments":null}`, 0, false, "", []row{{1}, {2}, {3}}},
		{`{"segments":[]}`, 0, false, "", []row{{1}, {2}, {3}}},
		{`{"segments":[null]}`, 1, true, "", []row{{1}}},
		{`{"segments":[null,"null"]}`, 2, true, "null", []row{{1}, {3}}},
		{`{"segments":["Cedar"]}`, 1, false, "Cedar", []row{{2}}},
	} {
		t.Run(tc.wire, func(t *testing.T) {
			request := httptest.NewRequest("POST", "/records", strings.NewReader(tc.wire))
			request.Header.Set("Content-Type", "application/json")
			providers, err := requestprovider.New(request)
			require.NoError(t, err)
			defer providers.Close()
			scope, err := injector.ForScope(providers.Providers()...)
			require.NoError(t, err)
			var actual input
			require.NoError(t, scope.Bind(t.Context(), &actual, bindly.WithPlan(route.Plan())))
			require.Len(t, actual.Segments, tc.size)
			if tc.firstNull {
				require.Nil(t, actual.Segments[0])
			}
			if tc.last != "" {
				require.NotNil(t, actual.Segments[tc.size-1])
				require.Equal(t, tc.last, *actual.Segments[tc.size-1])
			}
			var args []any
			bound, err := program.NewContext(t.Context(), nullableSelectionBinder{input: &actual}, func(values ...any) { args = append(args, values...) })
			require.NoError(t, err)
			clause, err := bound.(*velty.Context).FilterGroup(0, "AND")
			require.NoError(t, err)
			query := "SELECT r.id FROM records r WHERE r.tenant=7"
			if clause != "" {
				query += " AND " + clause
			}
			h.AssertQuery(t, t.Context(), sqlite.Query{SQL: query + " ORDER BY r.id", Args: args}, tc.want)
		})
	}
}
