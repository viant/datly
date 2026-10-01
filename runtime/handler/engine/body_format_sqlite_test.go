package engine_test

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/handler/compiler"
	engine "github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	writer "github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
)

type formattedWriteHas struct{ ID, Stamp, Disabled bool }
type formattedWriteRow struct {
	ID       int                `sqlx:"id,primaryKey"`
	Stamp    *time.Time         `sqlx:"stamp" format:"timeLayout=2006-01-02T15:04:05Z07"`
	Disabled *bool              `sqlx:"disabled"`
	Has      *formattedWriteHas `setMarker:"true" json:"-" sqlx:"-"`
}
type formattedWriteInput struct {
	Rows        []*formattedWriteRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=events"`
	CurrentRows []*formattedWriteRow `parameter:"CurrentRows,kind=view,in=CurrentRows" view:"CurrentRows,table=events"`
}
type formattedWriteOutput struct {
	Data []*formattedWriteRow `parameter:"Data,kind=output,in=body"`
}

func TestAuthoredBodyDateNativeSQLitePresenceAndRollback(t *testing.T) {
	for _, tc := range []struct {
		description, input string
		failure            bool
		expectedStamp      string
		expectedDisabled   bool
	}{
		{"source short offset sparse false", `{"data":[{"ID":1,"Stamp":"2026-10-01T04:20:27+00","Disabled":false}]}`, false, "2026-10-01T04:20:27Z", false},
		{"omitted stamp preserves prior value", `{"data":[{"ID":1,"Disabled":false,"Has":{"Stamp":true}}]}`, false, "2026-01-01T00:00:00Z", false},
		{"explicit null clears stamp", `{"data":[{"ID":1,"Stamp":null}]}`, false, "", true},
		{"bad nested date does not mutate", `{"data":[{"ID":1,"Stamp":"bad","Disabled":false}]}`, true, "2026-01-01T00:00:00Z", true},
		{"late statement failure rolls back earlier date", `{"data":[{"ID":1,"Stamp":"2026-10-01T04:20:27+00","Disabled":false},{"ID":2,"Stamp":"2026-10-01T04:20:27Z","Disabled":false}]}`, true, "2026-01-01T00:00:00Z", true},
	} {
		t.Run(tc.description, func(t *testing.T) {
			h := sqlite.New(t)
			ctx := context.Background()
			if err := h.ExecStatements(ctx, `CREATE TABLE events(id INTEGER PRIMARY KEY,stamp DATETIME,disabled BOOLEAN)`, `INSERT INTO events VALUES(1,'2026-01-01T00:00:00Z',1),(2,'2026-01-01T00:00:00Z',1)`); err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(tc.description, "late") {
				if err := h.ExecStatements(ctx, `CREATE TRIGGER late_formatted_date BEFORE UPDATE ON events WHEN OLD.id=2 BEGIN SELECT RAISE(ABORT,'fixture late date'); END`); err != nil {
					t.Fatal(err)
				}
			}
			component := &spec.Component{Settings: &spec.Settings{}, Routes: []*spec.Route{{Method: "PATCH", Path: "/events"}}, RootView: &spec.View{Name: "events", Source: &spec.ViewSource{Table: "events"}}}
			compiled, err := compiler.New(compiler.Input{Component: component, InputType: reflect.TypeFor[formattedWriteInput]()}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			input, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/events"})
			if !ok {
				t.Fatal("missing route")
			}
			native, err := writer.New(component, reflect.TypeFor[formattedWriteInput](), reflect.TypeFor[formattedWriteOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			stamp := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			disabled := true
			previous := []*formattedWriteRow{{ID: 1, Stamp: &stamp, Disabled: &disabled}, {ID: 2, Stamp: &stamp, Disabled: &disabled}}
			provider := handlerprovider.Named("view", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
				if name == "CurrentRows" {
					return previous, true, nil
				}
				return nil, false, nil
			})
			request := httptest.NewRequest("PATCH", "/events", strings.NewReader(tc.input))
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
			var actualStamp *time.Time
			var actualDisabled bool
			if err := h.DB.QueryRowContext(ctx, "SELECT stamp,disabled FROM events WHERE id=1").Scan(&actualStamp, &actualDisabled); err != nil {
				t.Fatal(err)
			}
			if actualDisabled != tc.expectedDisabled {
				t.Fatalf("disabled=%v expected=%v", actualDisabled, tc.expectedDisabled)
			}
			if tc.expectedStamp == "" {
				if actualStamp != nil {
					t.Fatal("explicit null not persisted")
				}
			} else if actualStamp == nil || actualStamp.UTC().Format(time.RFC3339) != tc.expectedStamp {
				t.Fatalf("stamp=%v expected=%s", actualStamp, tc.expectedStamp)
			}
		})
	}
}
