package standalone

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/bindly/locator"
	dexec "github.com/viant/datly/exec"
	ndiffer "github.com/viant/datly/runtime/differ"
	"github.com/viant/datly/runtime/handler/provider"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/standalone/config"
	fixture "github.com/viant/datly/standalone/testdata/loggerapp"
	"github.com/viant/datly/standalone/testdata/loggerapp/components"
	xdiffer "github.com/viant/xdatly/differ"
	h "github.com/viant/xdatly/handler"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func loggerServer(t *testing.T, log *fixture.Logger) *Server {
	t.Helper()
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	options := Options{Config: cfg, Holders: []any{components.Component{}}, RequireLinked: true}
	if log != nil {
		options.InvocationLogger = log
	}
	server, err := New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
	if err = server.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	return server
}
func TestHostInvocationLoggerBindsReaderOutputChildAndWriterLifecycle(t *testing.T) {
	log := &fixture.Logger{}
	server := loggerServer(t, log)
	send := func(method, path, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		server.manager.ServeHTTP(rec, req)
		return rec
	}
	child := send("GET", "/parent/1", "")
	if child.Code != 200 {
		t.Fatalf("child %d %s", child.Code, child.Body.String())
	}
	var out components.ParentOutput
	if err := json.Unmarshal(child.Body.Bytes(), &out); err != nil || len(out.Rows) != 1 || out.Rows[0].Name != "first" {
		t.Fatalf("child result %#v %v", out, err)
	}
	if strings.Contains(child.Body.String(), "Logger") || strings.Contains(child.Body.String(), "logger") {
		t.Fatal("host capability leaked into body")
	}
	expected := []fixture.Entry{{Level: "info", Message: "parent.begin", Attributes: []any{"id", 1}}, {Level: "debug", Message: "reader.input", Attributes: []any{"id", 1}}, {Level: "info", Message: "parent.end", Attributes: []any{"rows", 1}}, {Level: "warn", Message: "reader.output", Attributes: []any{"rows", 1}}}
	if actual := log.Snapshot(); !reflect.DeepEqual(actual, expected) {
		t.Fatalf("configured logger/order %#v", actual)
	}
	created := send("POST", "/logged", `{"data":[{"id":2,"name":"created"}]}`)
	if created.Code != 200 {
		t.Fatalf("writer %d %s", created.Code, created.Body.String())
	}
	entries := log.Snapshot()
	if len(entries) != 6 || entries[4].Message != "writer.init" || entries[5].Message != "writer.finalize" || entries[4].Level != "info" || entries[5].Level != "info" {
		t.Fatalf("lifecycle host logger %#v", entries)
	}
	var name string
	if err := server.source.connections.SQL.DB.QueryRow("SELECT name FROM records WHERE id=2").Scan(&name); err != nil || name != "created" {
		t.Fatalf("native writer persistence %q %v", name, err)
	}
}
func TestNilHostLoggerRetainsMissingCapabilityFailure(t *testing.T) {
	server := loggerServer(t, nil)
	for _, path := range []string{"/logged/1", "/parent/1"} {
		rec := httptest.NewRecorder()
		server.manager.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		if rec.Code != 500 {
			t.Fatalf("missing logger %s %d %s", path, rec.Code, rec.Body.String())
		}
	}
	req := httptest.NewRequest("POST", "/logged", strings.NewReader(`{"data":[{"id":2,"name":"must-not-write"}]}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	server.manager.ServeHTTP(rec, req)
	if rec.Code != 500 {
		t.Fatalf("missing lifecycle logger %d %s", rec.Code, rec.Body.String())
	}
	var count int
	if err := server.source.connections.SQL.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 1 {
		t.Fatalf("missing logger persisted writer rows %d %v", count, err)
	}

}
func TestHostLoggerDoesNotAuthorizeSpoofedApplicationProvider(t *testing.T) {
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(context.Background(), Options{Config: cfg, Holders: []any{components.Component{}}, InvocationLogger: &fixture.Logger{}, Providers: []locator.Provider{provider.Static(h.ValueKey("logger"), &fixture.Logger{})}})
	if err == nil {
		defer server.Shutdown(context.Background())
		err = server.Reload(context.Background(), 1)
		if err == nil {
			_, err = server.manager.InvokeComponent(context.Background(), dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: components.Package, Name: "Read"}, Route: spec.RouteRef{Method: "GET", Path: "/logged/{id}"}}, Input: &components.ReadInput{ID: 1}})
		}
	}
	if err == nil {
		t.Fatal("spoofed provider was accepted")
	}

	if !strings.Contains(err.Error(), "protected") && !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("unexpected spoofing rejection %v", err)
	}
}

func differServer(t *testing.T, mode string, differ xdiffer.Differ, providers ...locator.Provider) *Server {
	t.Helper()
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	cfg.GoBootstrap.LinkedOnly = mode == "linked"
	cfg.GoBootstrap.EagerComponents = mode == "eager"
	server, err := New(context.Background(), Options{Config: cfg, Holders: []any{components.Component{}}, RequireLinked: true, InvocationDiffer: differ, Providers: providers})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
	if err = server.Reload(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if server.source.invocationDiffer != differ {
		t.Fatal("host Differ identity changed")
	}
	return server
}

func invokeDiffer(t *testing.T, server *Server, ctx context.Context, rows ...*components.Record) (*components.PatchOutput, error) {
	t.Helper()
	value, err := server.manager.InvokeComponent(ctx, dexec.ComponentRequest{Target: dexec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: components.Package, Name: "Patch"}, Route: spec.RouteRef{Method: "PATCH", Path: "/differ"}}, Input: &components.PatchInput{Rows: rows}})
	if err != nil {
		return nil, err
	}
	result, ok := value.(*components.PatchOutput)
	if !ok {
		t.Fatalf("writer output %T", value)
	}
	return result, nil
}

func TestHostInvocationDifferNativeWriterAllCompilationModes(t *testing.T) {
	for _, mode := range []string{"linked", "indexed", "eager"} {
		t.Run(mode, func(t *testing.T) {
			differ := ndiffer.New()
			server := differServer(t, mode, differ)
			for _, reload := range []bool{false, true} {
				if reload {
					if err := server.Reload(context.Background(), 2); err != nil {
						t.Fatal(err)
					}
				}
				name := "changed"
				if reload {
					name = "reloaded"
				}
				result, err := invokeDiffer(t, server, context.Background(), &components.Record{ID: 1, Name: name, Has: &components.RecordHas{ID: true, Name: true}})
				if err != nil {
					t.Fatal(err)
				}
				previous := "first"
				if reload {
					previous = "changed"
				}
				if result.BoundDiffer != differ || !reflect.DeepEqual(result.PreviousNames, []string{previous}) || len(result.Changes) != 1 || result.Changes[0].Change != "update" || result.Changes[0].Path != "Name" || result.Changes[0].From != previous || result.Changes[0].To != name {
					t.Fatalf("native update %#v", result)
				}
				var stored string
				if err := server.source.connections.SQL.DB.QueryRow("SELECT name FROM records WHERE id=1").Scan(&stored); err != nil || stored != name {
					t.Fatalf("persisted %q: %v", stored, err)
				}
			}
			result, err := invokeDiffer(t, server, context.Background(), &components.Record{ID: 2, Name: "insert", Has: &components.RecordHas{ID: true, Name: true}})
			if err != nil {
				t.Fatal(err)
			}
			if result.BoundDiffer != differ || len(result.PreviousNames) != 0 || len(result.Changes) != 2 {
				t.Fatalf("native insert %#v", result)
			}
			inserted := map[string]any{}
			for _, change := range result.Changes {
				inserted[change.Path] = change.To
				if change.Change != "update" || change.From != nil {
					t.Fatalf("insert change %#v", change)
				}
			}
			if !reflect.DeepEqual(inserted, map[string]any{"ID": 2, "Name": "insert"}) {
				t.Fatalf("insert log %v", inserted)
			}
			var insertedName string
			if err := server.source.connections.SQL.DB.QueryRow("SELECT name FROM records WHERE id=2").Scan(&insertedName); err != nil || insertedName != "insert" {
				t.Fatalf("insert persistence %q %v", insertedName, err)
			}
			// Sparse writer input keeps omitted Name. Native comparator marker
			// semantics are unchanged when the loaded Previous has no marker.
			result, err = invokeDiffer(t, server, context.Background(), &components.Record{ID: 1, Has: &components.RecordHas{ID: true}})
			if err != nil {
				t.Fatal(err)
			}
			canonical, err := differ.Diff(context.Background(), &components.Record{ID: 1, Name: "reloaded"}, &components.Record{ID: 1, Has: &components.RecordHas{ID: true}}, xdiffer.WithShallow(true), xdiffer.WithSetMarker(true))
			if err != nil || !reflect.DeepEqual(result.Changes, canonical.ToChangeRecords()) {
				t.Fatalf("sparse native comparator output differs: %#v %v", result.Changes, err)
			}
			var stored string
			if err := server.source.connections.SQL.DB.QueryRow("SELECT name FROM records WHERE id=1").Scan(&stored); err != nil || stored != "reloaded" {
				t.Fatalf("sparse writer persisted %q %v", stored, err)
			}
		})
	}
}

func TestNilAndTypedNilHostDifferRetainMissingCapabilityFailure(t *testing.T) {
	var typedNil *ndiffer.Service
	for _, differ := range []xdiffer.Differ{nil, typedNil} {
		server := differServer(t, "linked", differ)
		_, err := invokeDiffer(t, server, context.Background(), &components.Record{ID: 1, Name: "must-not-write", Has: &components.RecordHas{ID: true, Name: true}})
		if err == nil || !strings.Contains(err.Error(), "differ") {
			t.Fatalf("missing capability %v", err)
		}
		var name string
		if err := server.source.connections.SQL.DB.QueryRow("SELECT name FROM records WHERE id=1").Scan(&name); err != nil || name != "first" {
			t.Fatalf("missing Differ persisted %q %v", name, err)
		}
	}
}

func TestHostDifferDoesNotAuthorizeSpoofedApplicationProvider(t *testing.T) {
	f := fixture.New(t)
	cfg, err := (config.Loader{}).Load(context.Background(), f.Config)
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(context.Background(), Options{Config: cfg, Holders: []any{components.Component{}}, InvocationDiffer: ndiffer.New(), Providers: []locator.Provider{provider.Static(h.DifferKey, ndiffer.New())}})
	if err == nil {
		defer server.Shutdown(context.Background())
		err = server.Reload(context.Background(), 1)
		if err == nil {
			_, err = invokeDiffer(t, server, context.Background(), &components.Record{ID: 1})
		}
	}
	if err == nil || !strings.Contains(err.Error(), "protected") && !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("spoofed Differ accepted: %v", err)
	}
}

type failingHostDiffer struct{ calls int }

func (d *failingHostDiffer) Diff(context.Context, any, any, ...xdiffer.Option) (*xdiffer.ChangeLog, error) {
	d.calls++
	return nil, fmt.Errorf("host comparison failed")
}
func TestHostDifferFailureRollsBackNativeWriter(t *testing.T) {
	differ := &failingHostDiffer{}
	server := differServer(t, "linked", differ)
	_, err := invokeDiffer(t, server, context.Background(), &components.Record{ID: 1, Name: "must-not-write", Has: &components.RecordHas{ID: true, Name: true}}, &components.Record{ID: 2, Name: "must-not-insert", Has: &components.RecordHas{ID: true, Name: true}})
	if err == nil || !strings.Contains(err.Error(), "host comparison failed") || differ.calls != 1 {
		t.Fatalf("comparison failure calls=%d err=%v", differ.calls, err)
	}
	var name string
	var count int
	if err := server.source.connections.SQL.DB.QueryRow("SELECT name FROM records WHERE id=1").Scan(&name); err != nil || name != "first" {
		t.Fatalf("rollback name=%q err=%v", name, err)
	}
	if err := server.source.connections.SQL.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 1 {
		t.Fatalf("rollback count=%d err=%v", count, err)
	}
}

type differInvocationKey struct{}
type contextHostDiffer struct{ native xdiffer.Differ }

func (d *contextHostDiffer) Diff(ctx context.Context, from, to any, opts ...xdiffer.Option) (*xdiffer.ChangeLog, error) {
	row := to.(*components.Record)
	if ctx.Value(differInvocationKey{}) != row.ID {
		return nil, fmt.Errorf("Differ received another invocation's context")
	}
	var options xdiffer.Options
	options.Apply(opts...)
	if !options.WithShallow || !options.WithSetMarker {
		return nil, fmt.Errorf("Differ invocation options lost")
	}
	return d.native.Diff(ctx, from, to, opts...)
}
func TestHostDifferConcurrentNativeWriterContextIsolation(t *testing.T) {
	differ := &contextHostDiffer{native: ndiffer.New()}
	server := differServer(t, "linked", differ)
	// Serialize SQLite transactions; binder scopes and comparison calls retain
	// independent contexts while invocations overlap outside the DB connection.
	server.source.connections.SQL.DB.SetMaxOpenConns(1)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ctx := context.WithValue(context.Background(), differInvocationKey{}, id)
			result, err := invokeDiffer(t, server, ctx, &components.Record{ID: id, Name: fmt.Sprintf("row-%d", id), Has: &components.RecordHas{ID: true, Name: true}})
			if err != nil {
				t.Error(err)
				return
			}
			if result.BoundDiffer != differ || len(result.Changes) != 2 {
				t.Errorf("invocation %d result %#v", id, result)
			}
		}(i + 2)
	}
	wg.Wait()
	var count int
	if err := server.source.connections.SQL.DB.QueryRow("SELECT COUNT(*) FROM records").Scan(&count); err != nil || count != 17 {
		t.Fatalf("concurrent writes %d %v", count, err)
	}
}
