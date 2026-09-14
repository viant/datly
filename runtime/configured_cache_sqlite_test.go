package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/provider/values"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx/io/read/cache/aerospike"
)

type configuredCacheInput struct{ Tenant int }
type configuredCacheRow struct {
	ID   int    `sqlx:"id"`
	Name string `sqlx:"name"`
}
type configuredCacheOutput struct{ Rows []configuredCacheRow }
type configuredCacheFixture struct {
	settings *spec.CacheSettings
	pool     aerospike.Pool
	identity string
}

func (f *configuredCacheFixture) runtime(t *testing.T, sql *dsql.SQLComponent, suffix, connector string) (*Runtime, dexec.ComponentTarget) {
	t.Helper()
	required := true
	component := &spec.Component{
		Key: spec.Key{Kind: spec.KindComponent, Name: f.identity + suffix}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}},
		Settings:   &spec.Settings{Cache: f.settings},
		Parameters: []*spec.Parameter{{Name: "Tenant", TypeExpr: "int", Required: &required, Source: spec.BindSource{Kind: "query", Name: "tenant"}}, {Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}},
		RootView:   &spec.View{Name: "records", Source: &spec.ViewSource{Bindings: &spec.ViewBindings{Connector: connector}, SQL: "SELECT id,name FROM records WHERE tenant=:Tenant ORDER BY id"}},
	}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(configuredCacheInput{}), OutputType: reflect.TypeOf(configuredCacheOutput{}), DirectViewField: "Rows"})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: sql, Aerospike: &f.pool})
	if err != nil {
		t.Fatal(err)
	}
	rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(configuredCacheOutput{}), Reader: reader}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })
	return rt, dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/records"}}
}

func (f *configuredCacheFixture) read(t *testing.T, rt *Runtime, target dexec.ComponentTarget, tenant int, refresh bool, want []configuredCacheRow) {
	t.Helper()
	ctx := (dexec.ReaderOptions{RefreshCache: refresh}).Context(context.Background())
	got, err := rt.InvokeComponent(ctx, dexec.ComponentRequest{Target: target, Providers: []locator.Provider{values.New("query", map[string]any{"tenant": tenant})}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.(*configuredCacheOutput).Rows, want) {
		t.Fatalf("rows=%+v want=%+v", got, want)
	}
}

func TestConfiguredCacheDatlySQLite(t *testing.T) {
	for _, provider := range []string{"afs", "aerospike"} {
		t.Run(provider, func(t *testing.T) {
			uri, location := "afs", t.TempDir()
			if provider == "aerospike" {
				uri = os.Getenv("DATLY_TEST_AEROSPIKE")
				if uri == "" {
					t.Skip("DATLY_TEST_AEROSPIKE is unset; use dedicated validation container")
				}
				location = "datly_validation"
			}
			for _, mode := range []string{"lazy", "warmup", "ttl"} {
				t.Run(mode, func(t *testing.T) {
					ctx := context.Background()
					db := sqlite.New(t)
					if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,tenant INTEGER,name TEXT)", "INSERT INTO records VALUES(11,1,'before'),(22,2,'two')"); err != nil {
						t.Fatal(err)
					}
					raw, err := json.Marshal(map[string]any{"enabled": true, "provider": uri, "location": location, "ttl": "30s", "totalTimeoutInMs": 1000, "socketTimeoutInMs": 500, "failedRequestLimit": 3, "resetFailuresInMs": 100})
					if err != nil {
						t.Fatal(err)
					}
					f := &configuredCacheFixture{identity: fmt.Sprintf("cache_%d", time.Now().UnixNano())}
					if err := json.Unmarshal(raw, &f.settings); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = f.pool.Close() })
					if mode == "ttl" {
						f.settings.TTL = "2s"
					}
					if mode == "warmup" {
						f.settings.Warmup = &spec.CacheWarmupSettings{Cases: []*spec.CacheWarmupCase{{Set: []*spec.CacheWarmupParam{{Name: "Tenant", Values: []string{"1", "3"}}}}}}
					}
					source := &dsql.SQLComponent{DB: db.DB}
					rt, target := f.runtime(t, source, "", "")
					if mode == "warmup" {
						if count, err := rt.Warmup(ctx, target); err != nil || count != 2 {
							t.Fatalf("warmup=%d,%v", count, err)
						}
					}
					f.read(t, rt, target, 1, false, []configuredCacheRow{{11, "before"}})
					if err := db.ExecStatements(ctx, "UPDATE records SET name='after' WHERE tenant=1", "INSERT INTO records VALUES(33,3,'new')"); err != nil {
						t.Fatal(err)
					}
					f.read(t, rt, target, 1, false, []configuredCacheRow{{11, "before"}})
					// Same SQL under a different component or connector must miss the first view.
					other, otherTarget := f.runtime(t, source, "_other", "")
					f.read(t, other, otherTarget, 1, false, []configuredCacheRow{{11, "after"}})
					connector, connectorTarget := f.runtime(t, source, "", "other")
					f.read(t, connector, connectorTarget, 1, false, []configuredCacheRow{{11, "after"}})
					f.read(t, rt, target, 2, false, []configuredCacheRow{{22, "two"}})
					if mode == "ttl" {
						time.Sleep(3100 * time.Millisecond)
						f.read(t, rt, target, 1, false, []configuredCacheRow{{11, "after"}})
					} else {
						f.read(t, rt, target, 1, true, []configuredCacheRow{{11, "after"}})
					}
					if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
						t.Fatal(err)
					}
					f.read(t, rt, target, 1, false, []configuredCacheRow{{11, "after"}})
					if mode == "warmup" {
						f.read(t, rt, target, 3, false, []configuredCacheRow{})
					}
				})
			}
		})
	}
}
