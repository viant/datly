package application_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/viant/bindly/resource"
	"github.com/viant/datly/application"
	"github.com/viant/datly/runtime"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/authorization"
)

func TestReloadRejectsStaleAndFailedStagesSQLite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f := &reloadFixture{}
	f.init(t)
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	var first *application.Build
	if err := manager.Reload(ctx, application.Request{Revision: 1, Compile: func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
		var err error
		first, err = f.compile(1)(ctx, types)
		return first, err
	}}); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name     string
		revision uint64
		compile  func(context.Context, *typecatalog.Catalog) (*application.Build, error)
	}{
		{"same revision", 1, f.compile(2)},
		{"invalid DQL", 2, f.compile(3)},
		{"nil build", 2, func(context.Context, *typecatalog.Catalog) (*application.Build, error) { return nil, nil }},
		{"active component reuse", 2, func(context.Context, *typecatalog.Catalog) (*application.Build, error) { return first, nil }},
		{"invalid MCP policy", 2, func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
			built, err := f.compile(2)(ctx, types)
			if err == nil {
				built.MCP.Authorization = &authorization.Policy{Tools: map[string]*authorization.Authorization{"unknown": {}}}
			}
			return built, err
		}},
		{"conflicting runtime resources", 2, func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
			built, err := f.compile(2)(ctx, types)
			if err == nil {
				built.Resources = resource.New()
				built.RuntimeOptions = []runtime.Option{runtime.WithResources(resource.New())}
			}
			return built, err
		}},
		{"conflicting MCP resources", 2, func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
			built, err := f.compile(2)(ctx, types)
			if err == nil {
				built.Resources = resource.New()
				built.MCP.Resources = resource.New()
			}
			return built, err
		}},
		{"mixed FS and staged store", 2, func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
			built, err := f.compile(2)(ctx, types)
			if err == nil {
				built.Resources = resource.New()
				built.RuntimeOptions = []runtime.Option{runtime.WithResourceFS("extra", fstest.MapFS{"query.sql": {Data: []byte("SELECT 1")}})}
			}
			return built, err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := manager.Reload(ctx, application.Request{Revision: test.revision, Compile: test.compile}); err == nil {
				t.Fatal("invalid stage accepted")
			}
			if manager.Revision() != 1 {
				t.Fatal("failed stage published")
			}
			catalog, err := manager.Types(ctx)
			if err != nil {
				t.Fatal(err)
			}
			typ, found, err := catalog.Resolve(typecatalog.PackageAuthority, "example.com/reload.Row")
			if err != nil || !found || typ.Type.Field(0).Type.Kind() != reflect.Int {
				t.Fatalf("old type lost: %v %v %v", typ, found, err)
			}
		})
	}
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- manager.Reload(ctx, application.Request{Revision: 2, Compile: func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
			built, err := f.compile(1)(ctx, types)
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return built, err
		}})
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("stage did not start")
	}
	if err := manager.Reload(ctx, application.Request{Revision: 3, Compile: f.compile(2)}); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; !errors.Is(err, application.ErrStale) {
		t.Fatalf("stale error=%v", err)
	}
	if manager.Revision() != 3 {
		t.Fatal("stale stage overwrote newer publication")
	}
	recorder := httptest.NewRecorder()
	manager.ServeHTTP(recorder, httptest.NewRequest("GET", "/records", nil))
	if recorder.Code != 200 || strings.TrimSpace(recorder.Body.String()) != `{"rows":[{"value":"new"}]}` {
		t.Fatalf("active=%d %s", recorder.Code, recorder.Body.String())
	}
}
