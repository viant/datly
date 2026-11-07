package engine_test

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/handler/compiler"
	engine "github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	writer "github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
)

type jsonxWriteRow struct {
	ID   int                      `sqlx:"id,primaryKey"`
	Flag *int                     `sqlx:"flag" jsonx:"boolToInt,null=omit"`
	Has  *struct{ ID, Flag bool } `setMarker:"true" json:"-" sqlx:"-"`
}
type jsonxWriteInput struct {
	Rows        []*jsonxWriteRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=flags"`
	CurrentRows []*jsonxWriteRow `parameter:"CurrentRows,kind=view,in=CurrentRows" view:"CurrentRows,table=flags"`
}
type jsonxWriteOutput struct {
	Data []*jsonxWriteRow `parameter:"Data,kind=output,in=body"`
}

func TestJSONXNativeHTTPWriterSQLite(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		want          int
		failure       bool
	}{
		{"true", `{"data":[{"ID":1,"Flag":true}]}`, 1, false},
		{"false", `{"data":[{"ID":1,"Flag":false}]}`, 0, false},
		{"integer", `{"data":[{"ID":1,"Flag":2}]}`, 2, false},
		{"negative", `{"data":[{"ID":1,"Flag":-1}]}`, -1, false},
		{"null omitted", `{"data":[{"ID":1,"Flag":null}]}`, 7, false},
		{"omitted forged presence", `{"data":[{"ID":1,"Has":{"Flag":true}}]}`, 7, false},
		{"fraction rejected", `{"data":[{"ID":1,"Flag":1.5}]}`, 7, true},
		{"string rejected", `{"data":[{"ID":1,"Flag":"1"}]}`, 7, true},
		{"late failure rollback", `{"data":[{"ID":1,"Flag":true},{"ID":2,"Flag":false}]}`, 7, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := sqlite.New(t)
			ctx := context.Background()
			if err := h.ExecStatements(ctx, `CREATE TABLE flags(id INTEGER PRIMARY KEY,flag INTEGER)`, `INSERT INTO flags VALUES(1,7),(2,7)`); err != nil {
				t.Fatal(err)
			}
			if tc.name == "late failure rollback" {
				if err := h.ExecStatements(ctx, `CREATE TRIGGER late_flag BEFORE UPDATE ON flags WHEN OLD.id=2 BEGIN SELECT RAISE(ABORT,'fixture late flag'); END`); err != nil {
					t.Fatal(err)
				}
			}
			component := &spec.Component{Settings: &spec.Settings{}, Routes: []*spec.Route{{Method: "PATCH", Path: "/flags"}}, RootView: &spec.View{Name: "flags", Source: &spec.ViewSource{Table: "flags"}}}
			compiled, err := compiler.New(compiler.Input{Component: component, InputType: reflect.TypeFor[jsonxWriteInput]()}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			input, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/flags"})
			if !ok {
				t.Fatal("missing route")
			}
			native, err := writer.New(component, reflect.TypeFor[jsonxWriteInput](), reflect.TypeFor[jsonxWriteOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			prior := 7
			previous := []*jsonxWriteRow{{ID: 1, Flag: &prior}, {ID: 2, Flag: &prior}}
			provider := handlerprovider.Named("view", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
				if name == "CurrentRows" {
					return previous, true, nil
				}
				return nil, false, nil
			})
			request := httptest.NewRequest("PATCH", "/flags", strings.NewReader(tc.payload))
			request.Header.Set("Content-Type", "application/json")
			scope, err := requestprovider.New(request)
			if err != nil {
				t.Fatal(err)
			}
			defer scope.Close()
			_, err = engine.New().Execute(ctx, engine.Request{Input: input, Handler: native, Scope: scope, Providers: []locator.Provider{provider}, DataSource: dml.Source{DB: h.DB}})
			if (err != nil) != tc.failure {
				t.Fatalf("error=%v expected failure=%v", err, tc.failure)
			}
			var actual int
			if err := h.DB.QueryRowContext(ctx, "SELECT flag FROM flags WHERE id=1").Scan(&actual); err != nil {
				t.Fatal(err)
			}
			if actual != tc.want {
				t.Fatalf("stored=%d want=%d", actual, tc.want)
			}
		})
	}
}
