package engine_test

import (
	"context"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/viant/bindly/locator"
	requestprovider "github.com/viant/bindly/provider/request"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/runtime/handler/engine"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/runtime/handler/writer"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	h "github.com/viant/xdatly/handler"
)

type nativeEligibilityHas struct{ ID, Name, Locked, RefID bool }
type nativeEligibilityRow struct {
	ID     *int                  `sqlx:"id,primaryKey,autoincrement"`
	Name   *string               `sqlx:"name" validate:"required"`
	RefID  *int                  `sqlx:"ref_id,refTable=eligible,refColumn=id"`
	Locked bool                  `sqlx:"-"`
	Has    *nativeEligibilityHas `setMarker:"true" json:"-" sqlx:"-"`
}
type nativeEligibilityInput struct {
	Rows    []*nativeEligibilityRow `parameter:"Rows,kind=body,in=data" view:"Rows,table=eligible"`
	Current []*nativeEligibilityRow `parameter:"Current,kind=view,in=Current" view:"Current,table=eligible"`
}
type nativeEligibilityOutput struct {
	Data []*nativeEligibilityRow `parameter:"Data,kind=output,in=body"`
}
type nativeEligibilityHooks struct{}
type nativeEligibilityCounterKey struct{}

var nativeEligibilityLinked = reflect.TypeFor[nativeEligibilityHooks]()

func (*nativeEligibilityHooks) WriteEligible(ctx context.Context, row *nativeEligibilityRow, _ h.LifecycleContext[nativeEligibilityRow, h.NoParent, nativeEligibilityOutput], _ h.WriteAction) (bool, error) {
	if counter, ok := ctx.Value(nativeEligibilityCounterKey{}).(*atomic.Int32); ok {
		counter.Add(1)
	}
	return !row.Locked, nil
}
func TestNativeWriterWriteEligibilitySQLite(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		failure    bool
		expected   []string
	}{
		{"allocate excluded insert and retain response", `{"data":[{"Name":"excluded","Locked":true},{"Name":"inserted"}]}`, false, []string{"old", "inserted"}},
		{"excluded update preserves stored row", `{"data":[{"ID":1,"Name":"excluded","Locked":true},{"Name":"inserted"}]}`, false, []string{"old", "inserted"}},
		{"excluded invalid insert still fails validation", `{"data":[{"Locked":true},{"Name":"inserted"}]}`, true, []string{"old"}},
		{"excluded insert cannot satisfy eligible FK", `{"data":[{"ID":9,"Name":"excluded","Locked":true},{"Name":"invalid reference","RefID":9}]}`, true, []string{"old"}},
		{"late eligible statement rolls back prior eligible insert", `{"data":[{"Name":"queued"},{"Name":"blocked"}]}`, true, []string{"old"}},
		{"identity only excluded update retains body", `{"data":[{"ID":1,"Locked":true}]}`, false, []string{"old"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := sqlite.New(t)
			counter := &atomic.Int32{}
			ctx := context.WithValue(context.Background(), nativeEligibilityCounterKey{}, counter)
			if err := db.ExecStatements(ctx, `CREATE TABLE eligible(id INTEGER PRIMARY KEY AUTOINCREMENT,name TEXT NOT NULL,ref_id INTEGER REFERENCES eligible(id))`, `INSERT INTO eligible(id,name) VALUES(1,'old')`); err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(tc.name, "late") {
				if err := db.ExecStatements(ctx, `CREATE TRIGGER fail_eligible BEFORE INSERT ON eligible WHEN NEW.name='blocked' BEGIN SELECT RAISE(ABORT,'late eligible fixture'); END`); err != nil {
					t.Fatal(err)
				}
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: nativeEligibilityLinked.PkgPath(), Name: "Rows"}, Settings: &spec.Settings{}, Routes: []*spec.Route{{Method: "PATCH", Path: "/eligible"}}, RootView: &spec.View{Name: "Rows", EntityHooks: "nativeEligibilityHooks", Source: &spec.ViewSource{Table: "eligible"}}}
			compiled, err := compiler.New(compiler.Input{Component: component, InputType: reflect.TypeFor[nativeEligibilityInput]()}).Compile()
			if err != nil {
				t.Fatal(err)
			}
			input, ok := compiled.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/eligible"})
			if !ok {
				t.Fatal("route missing")
			}
			native, err := writer.New(component, reflect.TypeFor[nativeEligibilityInput](), reflect.TypeFor[nativeEligibilityOutput](), "patch")
			if err != nil {
				t.Fatal(err)
			}
			id, name := 1, "old"
			provider := handlerprovider.Named("view", func(_ context.Context, _ reflect.Type, key string) (any, bool, error) {
				if key == "Current" {
					return []*nativeEligibilityRow{{ID: &id, Name: &name}}, true, nil
				}
				return nil, false, nil
			})
			request := httptest.NewRequest("PATCH", "/eligible", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			scope, err := requestprovider.New(request)
			if err != nil {
				t.Fatal(err)
			}
			defer scope.Close()
			output, err := engine.New().Execute(ctx, engine.Request{Input: input, Handler: native, Scope: scope, Providers: []locator.Provider{provider}, DataSource: dml.Source{DB: db.DB}})
			if (err != nil) != tc.failure {
				t.Fatalf("error=%v expected failure=%v", err, tc.failure)
			}
			if strings.HasPrefix(tc.name, "excluded insert cannot") {
				if counter.Load() != 2 || err == nil || !strings.Contains(err.Error(), "validate writer") {
					t.Fatalf("excluded producer failure did not occur at post-eligibility validation: calls=%d err=%v", counter.Load(), err)
				}
			}
			if strings.HasPrefix(tc.name, "excluded invalid") && counter.Load() != 0 {
				t.Fatal("invalid row reached eligibility")
			}
			rows, err := db.DB.QueryContext(ctx, "SELECT name FROM eligible ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var actual []string
			for rows.Next() {
				var value string
				if err := rows.Scan(&value); err != nil {
					t.Fatal(err)
				}
				actual = append(actual, value)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, tc.expected) {
				t.Fatalf("rows=%v want=%v", actual, tc.expected)
			}
			if !tc.failure {
				data := output.(*nativeEligibilityOutput).Data
				if len(data) == 0 {
					t.Fatal("response missing")
				}
				for _, row := range data {
					if row.ID == nil || *row.ID == 0 {
						t.Fatal("excluded/eligible identity not allocated")
					}
				}
				if strings.HasPrefix(tc.name, "allocate") && len(data) != 2 {
					t.Fatal("excluded row dropped")
				}
			}
		})
	}
}
