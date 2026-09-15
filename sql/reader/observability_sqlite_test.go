package reader_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability/otel"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/sqlx/io/read/cache/afs"
	xexec "github.com/viant/xdatly/exec"
	"github.com/viant/xdatly/response"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type observationRow struct {
	ID   int    `sqlx:"id"`
	Name string `sqlx:"name"`
}
type observationOutput struct {
	Rows    []*observationRow `parameter:"Rows,kind=output,in=view" view:"records" sql:"SELECT id,name FROM records ORDER BY id"`
	Metrics response.Metrics  `parameter:"Metrics,kind=output,in=metrics"`
}
type observationChild struct {
	ID       int `sqlx:"id"`
	ParentID int `sqlx:"parent_id"`
}
type observationParent struct {
	ID       int                 `sqlx:"id"`
	Children []*observationChild `sqlx:"-" view:"children,batch=1,batchConcurrency=2" on:"ID:id=ParentID:parent_id" sql:"SELECT id,parent_id FROM children WHERE $COLUMN_IN"`
}
type observationRelations struct {
	Rows []*observationParent `parameter:"Rows,kind=output,in=view" view:"records" sql:"SELECT id FROM records ORDER BY id"`
}
type quietObservationLog struct{}

func (quietObservationLog) Debug(string, ...any) {}
func (quietObservationLog) Info(string, ...any)  {}
func (quietObservationLog) Warn(string, ...any)  {}
func (quietObservationLog) Error(string, ...any) {}

type readerExportProbe struct {
	gate    chan struct{}
	started chan struct{}
	once    sync.Once
	mu      sync.Mutex
	spans   int
}

func (p *readerExportProbe) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	p.once.Do(func() {
		if p.started != nil {
			close(p.started)
		}
	})
	if p.gate != nil {
		select {
		case <-p.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p.mu.Lock()
	p.spans += len(spans)
	p.mu.Unlock()
	return nil
}
func (*readerExportProbe) Shutdown(context.Context) error { return nil }
func TestReaderObservabilitySQLite(t *testing.T) {
	for _, mode := range []string{"rows", "empty", "SQL-error", "relations", "cache"} {
		t.Run(mode, func(t *testing.T) {
			db := sqlite.New(t)
			ctx := context.Background()
			if mode != "SQL-error" {
				if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT)", "CREATE TABLE children(id INTEGER,parent_id INTEGER)"); err != nil {
					t.Fatal(err)
				}
				if mode != "empty" {
					if err := db.ExecStatements(ctx, "INSERT INTO records VALUES(1,'one'),(2,'two')", "INSERT INTO children VALUES(10,1),(20,2)"); err != nil {
						t.Fatal(err)
					}
				}
			}
			output := reflect.TypeOf(observationOutput{})
			if mode == "relations" {
				output = reflect.TypeOf(observationRelations{})
			}
			f := typedGraphFixture{db: db, component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Observed"}, Name: "Observed", Routes: []*spec.Route{{Method: "GET", Path: "/graph"}}}, output: output}
			if mode == "cache" {
				service, err := afs.NewCache(t.TempDir(), time.Minute, "observed", nil)
				if err != nil {
					t.Fatal(err)
				}
				f.caches = func(a *bootstrap.Artifact) map[*data.View]cache.Cache {
					return map[*data.View]cache.Cache{a.Reader.Root.View: service}
				}
			}
			var callbackMu sync.Mutex
			var callbacks []readingCallbackResult
			app, artifact, err := f.compile(runtime.WithObservability(runtime.ObservabilityConfig{Logger: quietObservationLog{}, ReadingData: func(view string, duration time.Duration, query string, read int, params []any, failure error) {
				callbackMu.Lock()
				defer callbackMu.Unlock()
				callbacks = append(callbacks, readingCallbackResult{view: view, duration: duration, query: query, rows: read, failed: failure != nil})
			}}))
			if err != nil {
				t.Fatal(err)
			}
			passes := 1
			if mode == "cache" {
				passes = 2
			}
			for pass := 0; pass < passes; pass++ {
				callbacks = nil
				ec := xexec.New()
				actual, err := app.InvokeComponent(xexec.WithContext(ctx, ec), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/graph"}}, Input: &struct{}{}})
				if (err != nil) != (mode == "SQL-error") {
					t.Fatalf("invoke: %v", err)
				}
				expectedCallbacks := 1
				if mode == "relations" {
					expectedCallbacks = 3
				}
				if len(callbacks) != expectedCallbacks {
					t.Fatalf("callbacks=%d want=%d", len(callbacks), expectedCallbacks)
				}
				callbackRows := 0
				for _, callback := range callbacks {
					if callback.view == "" || callback.duration <= 0 || callback.query == "" || callback.failed != (mode == "SQL-error") {
						t.Fatalf("invalid callback metadata: %+v", callback)
					}
					callbackRows += callback.rows
				}
				expectedRows := 2
				if mode == "empty" || mode == "SQL-error" {
					expectedRows = 0
				}
				if mode == "relations" {
					expectedRows = 4
				}
				if callbackRows != expectedRows {
					t.Fatalf("callback rows=%d want=%d", callbackRows, expectedRows)
				}
				want := 1
				if mode == "relations" {
					want = 2
				}
				if len(ec.Metrics) != want {
					t.Fatalf("metrics=%d want=%d", len(ec.Metrics), want)
				}
				for _, m := range ec.Metrics {
					if m.ID == "" || m.StartTime.IsZero() || m.EndTime.Before(m.StartTime) || len(m.Executions) == 0 {
						t.Fatalf("bad metric %+v", m)
					}
					e := m.Executions[0]
					if e.ID == "" || e.StartTime.IsZero() || e.EndTime.Before(e.StartTime) {
						t.Fatal("SQL timing missing")
					}
					if mode == "SQL-error" {
						if m.Error == "" || e.Error == "" {
							t.Fatal("error missing")
						}
					} else if mode != "empty" && mode != "relations" && e.Rows != 2 {
						t.Fatalf("rows=%d", e.Rows)
					}
					if mode == "relations" && m.View == "children" {
						if len(m.Executions) != 2 || m.Executions[0].ID == m.Executions[1].ID {
							t.Fatal("batch execution identity lost")
						}
						if e.ParentID == "" || e.ParentID != ec.Metrics[0].Executions[0].ID {
							t.Fatal("relation parentage lost")
						}
					}
					if mode == "cache" {
						if e.CacheStats == nil {
							t.Fatal("cache stats missing")
						}
						if pass == 1 && !e.CacheStats.FoundLazy {
							t.Fatalf("cache replay stats %+v", e.CacheStats)
						}
					}
				}
				if out, ok := actual.(*observationOutput); ok && len(out.Metrics) != 1 {
					t.Fatal("declared metrics output not populated")
				}
				if mode == "cache" && pass == 0 {
					if err := db.ExecStatements(ctx, "DROP TABLE records"); err != nil {
						t.Fatal(err)
					}
				}
			}
			key := artifact.Component.Key.String() + "/records"
			values := app.Observability().Recorder.Values(key)
			status := "Success"
			if mode == "SQL-error" {
				status = "Error"
			}
			if values[status] != int64(passes) || values["Pending"] != 0 {
				t.Fatalf("counter %s %+v", key, values)
			}
		})
	}
}
func TestReaderExportDropsPreserveMetricsSQLite(t *testing.T) {
	db := sqlite.New(t)
	ctx := context.Background()
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,'one')"); err != nil {
		t.Fatal(err)
	}
	probe := &readerExportProbe{gate: make(chan struct{}), started: make(chan struct{})}
	f := typedGraphFixture{db: db, component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Exported"}, Name: "Exported", Routes: []*spec.Route{{Method: "GET", Path: "/graph"}}}, output: reflect.TypeOf(observationOutput{})}
	app, artifact, err := f.compile(runtime.WithObservability(runtime.ObservabilityConfig{Logger: quietObservationLog{}, OTel: &otel.Config{Enabled: true, Exporter: probe, QueueSize: 1}}))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		ec := xexec.New()
		_, err := app.InvokeComponent(xexec.WithContext(ctx, ec), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/graph"}}, Input: &struct{}{}})
		if err != nil {
			t.Fatal(err)
		}
		if len(ec.Metrics) != 1 || ec.Metrics[0].Rows != 1 {
			t.Fatal("native metric lost")
		}
		if i == 0 {
			select {
			case <-probe.started:
			case <-time.After(time.Second):
				t.Fatal("worker did not export")
			}
		}
	}
	close(probe.gate)
	if err := app.Observability().Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := app.Observability().ExportStats(); got.Accepted != 1 || got.Exported != 1 || got.Dropped != 1 {
		t.Fatal(fmt.Sprint(got))
	}
}

type observationFailRow struct {
	ID   int    `sqlx:"id"`
	Name string `sqlx:"name"`
}

type observationPanicKey struct{}

func (r *observationFailRow) OnFetch(ctx context.Context) error {
	if r.ID == 2 {
		if ctx.Value(observationPanicKey{}) == true {
			panic("private panic payload")
		}
		return fmt.Errorf("synthetic row hook failure")
	}
	return nil
}

type observationFailOutput struct {
	Rows []*observationFailRow `parameter:"Rows,kind=output,in=view" view:"records" sql:"SELECT id,name FROM records ORDER BY id"`
}

func TestReaderPartialErrorRetainsCollectorCountsSQLite(t *testing.T) {
	db := sqlite.New(t)
	ctx := context.Background()
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT)", "INSERT INTO records VALUES(1,'one'),(2,'two'),(3,'three')"); err != nil {
		t.Fatal(err)
	}
	f := typedGraphFixture{db: db, component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "Partial"}, Name: "Partial", Routes: []*spec.Route{{Method: "GET", Path: "/graph"}}}, output: reflect.TypeOf(observationFailOutput{})}
	app, artifact, err := f.compile(runtime.WithObservability(runtime.ObservabilityConfig{Logger: quietObservationLog{}}))
	if err != nil {
		t.Fatal(err)
	}
	ec := xexec.New()
	_, err = app.InvokeComponent(xexec.WithContext(ctx, ec), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/graph"}}, Input: &struct{}{}})
	if err == nil || len(ec.Metrics) != 1 {
		t.Fatal("missing partial read error")
	}
	m := ec.Metrics[0]
	if m.Rows != 2 || m.Executions[0].Rows != 2 || m.Error == "" || m.Executions[0].Error == "" {
		t.Fatalf("native collector counts lost: %+v, %+v", m, m.Executions[0])
	}
	panicked := xexec.New()
	var caught any
	func() {
		defer func() { caught = recover() }()
		_, err = app.InvokeComponent(context.WithValue(xexec.WithContext(ctx, panicked), observationPanicKey{}, true), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/graph"}}, Input: &struct{}{}})
	}()
	if caught != nil || err == nil || err.Error() != "handler invocation panicked" || len(panicked.Metrics) != 1 || panicked.Metrics[0].Error != "reader panicked" || panicked.Metrics[0].Executions[0].Error != "reader panicked" {
		t.Fatalf("panic contract or failure metadata lost: caught=%v err=%v metrics=%+v", caught, err, panicked.Metrics)
	}
}

// ReadingData receives row delivery counts, including native cache replay, at
// the real reader owner. This stores no SQL arguments or request payloads.
type readingCallbackResult struct {
	view, query string
	duration    time.Duration
	rows        int
	failed      bool
}
