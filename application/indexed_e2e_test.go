package application_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	bootstrapindex "github.com/viant/datly/bootstrap/index"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/mcp"
	mcpserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/runtime/handler/custom"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
)

type indexedInput struct {
	Name string `parameter:"Name,kind=query,in=name"`
}

type indexedToolWithAuthInput struct {
	Name string         `parameter:"Name,kind=query,in=name"`
	Auth *indexedOutput `parameter:"Auth,kind=component,in=GET:/auth"`
}

type indexedAuthInput struct {
	Token string `parameter:"Token,kind=query,in=token"`
}

type lifecycleMaterializer struct {
	started chan struct{}
	gate    chan struct{}
	release chan struct{}
	once    sync.Once
}

func (m *lifecycleMaterializer) Materialize(_ context.Context, entry *bootstrapindex.Entry, _ bootstrapindex.Resolver) (*bootstrapindex.Loaded, error) {
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: entry.Component, InputType: reflect.TypeFor[indexedInput](), OutputType: reflect.TypeFor[indexedOutput]()})
	if err != nil {
		return nil, err
	}
	handler := custom.NewFunc[indexedInput, indexedOutput](func(_ context.Context, input *indexedInput) (*indexedOutput, error) {
		if m.started != nil {
			m.once.Do(func() { close(m.started) })
		}
		if m.gate != nil {
			<-m.gate
		}
		return &indexedOutput{Name: input.Name}, nil
	})
	registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[indexedOutput](), Handler: handler}
	return &bootstrapindex.Loaded{Registration: registered, Release: func(context.Context) error { close(m.release); return nil }}, nil
}

type indexedOutput struct{ Name string }

func writeFixture(t testing.TB, root, name, body string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

type countingMaterializer struct {
	mu    sync.Mutex
	calls map[string]int
	regs  map[string]*registry.RegisteredComponent
}

func (m *countingMaterializer) Materialize(_ context.Context, entry *bootstrapindex.Entry, _ bootstrapindex.Resolver) (*bootstrapindex.Loaded, error) {
	m.mu.Lock()
	m.calls[entry.Key().Name]++
	m.mu.Unlock()
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: entry.Component, InputType: reflect.TypeFor[indexedInput](), OutputType: reflect.TypeFor[indexedOutput]()})
	if err != nil {
		return nil, err
	}
	registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[indexedOutput](), Handler: custom.NewFunc[indexedInput, indexedOutput](func(_ context.Context, input *indexedInput) (*indexedOutput, error) {
		return &indexedOutput{Name: input.Name}, nil
	})}
	m.mu.Lock()
	m.regs[entry.Key().Name] = registered
	m.mu.Unlock()
	return &bootstrapindex.Loaded{Registration: registered}, nil
}

func (m *countingMaterializer) count(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[name]
}

func TestIndexedApplicationHTTPAndMCPMaterializeOnlyUsedComponents(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/indexed"}).Write(t, root)
	for _, fixture := range []struct {
		pkg, name, path string
		mcp             bool
	}{{"http", "HTTP", "/http", false}, {"tool", "Tool", "/tool", true}, {"unused", "Unused", "/unused", false}} {
		extra := ""
		if fixture.mcp {
			extra = ` mcp:"[{\"kind\":\"tool\",\"name\":\"indexed.tool\"}]"`
		}
		writeFixture(t, root, fixture.pkg+"/holder.go", `package `+fixture.pkg+`
import xdatly "github.com/viant/xdatly"
type Input struct { Name string `+"`"+`parameter:"Name,kind=query,in=name"`+"`"+` }
type Output struct { Name string }
type Holder struct { Route xdatly.Component[Input,Output] `+"`"+`component:"`+fixture.name+`,path=`+fixture.path+`,method=GET"`+extra+"`"+` }
`)
	}
	snapshot, err := (bootstrapindex.Builder{Config: bootstrapindex.Config{BaseDir: root, Include: []string{"example.com/indexed/..."}}}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	materializer := &countingMaterializer{calls: map[string]int{}, regs: map[string]*registry.RegisteredComponent{}}
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Reload(context.Background(), application.Request{Revision: 1, Compile: func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
		return &application.Build{Index: snapshot, Materializer: materializer}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	if materializer.count("HTTP")+materializer.count("Tool")+materializer.count("Unused") != 0 {
		t.Fatal("bootstrap materialized a component")
	}
	request := httptest.NewRequest(http.MethodGet, "/http?name=Ada", nil)
	response := httptest.NewRecorder()
	manager.ServeHTTP(response, request)
	if response.Code != http.StatusOK || materializer.count("HTTP") != 1 || materializer.count("Tool") != 0 || materializer.count("Unused") != 0 {
		t.Fatalf("HTTP status=%d calls=%v", response.Code, materializer.calls)
	}
	pinned, service, err := manager.Pin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer mcpserver.Release(pinned)
	prepared := service.(interface {
		PrepareTool(context.Context, string) error
	})
	if err := prepared.PrepareTool(pinned, "indexed.tool"); err != nil {
		t.Fatal(err)
	}
	if materializer.count("Tool") != 1 || materializer.count("Unused") != 0 {
		t.Fatalf("MCP calls=%v", materializer.calls)
	}
	entry, ok := service.Registry().ToolRegistry.Get("indexed.tool")
	if !ok || len(entry.Metadata.InputSchema.Properties) == 0 {
		t.Fatalf("indexed tool metadata=%+v", entry)
	}
	toolReg := materializer.regs["Tool"]
	eager, err := mcp.New(mcp.Config{Components: []*registry.RegisteredComponent{toolReg}, Invoker: manager})
	if err != nil {
		t.Fatal(err)
	}
	eagerEntry, _ := eager.Registry().ToolRegistry.Get("indexed.tool")
	if !reflect.DeepEqual(entry.Metadata, eagerEntry.Metadata) {
		t.Fatalf("indexed metadata differs from eager:\n%+v\n%+v", entry.Metadata, eagerEntry.Metadata)
	}
	if _, protocolErr := entry.Handler(pinned, &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: "indexed.tool"}}); protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if materializer.count("Tool") != 1 || materializer.count("Unused") != 0 {
		t.Fatalf("cached MCP calls=%v", materializer.calls)
	}
	mcpserver.Release(pinned)
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestIndexedApplicationMCPPreparesTransitiveInputDependencies(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/indexedauth"}).Write(t, root)
	for _, fixture := range []struct {
		pkg, name, path string
		mcp             bool
	}{{"tool", "Tool", "/tool", true}, {"auth", "Auth", "/auth", false}, {"unused", "Unused", "/unused", false}} {
		extra := ""
		if fixture.mcp {
			extra = ` mcp:"[{\"kind\":\"tool\",\"name\":\"indexed.auth.tool\"}]"`
		}
		writeFixture(t, root, fixture.pkg+"/holder.go", `package `+fixture.pkg+`
import xdatly "github.com/viant/xdatly"
type Input struct{}
type Output struct{}
type Holder struct { Route xdatly.Component[Input,Output] `+"`"+`component:"`+fixture.name+`,path=`+fixture.path+`,method=GET"`+extra+"`"+` }
`)
	}
	snapshot, err := (bootstrapindex.Builder{Config: bootstrapindex.Config{BaseDir: root, Include: []string{"example.com/indexedauth/..."}}}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	materializer := &countingMaterializer{calls: map[string]int{}, regs: map[string]*registry.RegisteredComponent{}}
	materializerForAuth := bootstrapindex.MaterializeFunc(func(ctx context.Context, entry *bootstrapindex.Entry, resolver bootstrapindex.Resolver) (*bootstrapindex.Loaded, error) {
		materializer.mu.Lock()
		materializer.calls[entry.Key().Name]++
		materializer.mu.Unlock()
		var inputType reflect.Type
		switch entry.Key().Name {
		case "Tool":
			inputType = reflect.TypeFor[indexedToolWithAuthInput]()
		case "Auth":
			inputType = reflect.TypeFor[indexedAuthInput]()
		default:
			inputType = reflect.TypeFor[indexedInput]()
		}
		artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: entry.Component, InputType: inputType, OutputType: reflect.TypeFor[indexedOutput]()})
		if err != nil {
			return nil, err
		}
		registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: reflect.TypeFor[indexedOutput](), Handler: custom.NewFunc[indexedInput, indexedOutput](func(_ context.Context, input *indexedInput) (*indexedOutput, error) {
			return &indexedOutput{Name: input.Name}, nil
		})}
		materializer.mu.Lock()
		materializer.regs[entry.Key().Name] = registered
		materializer.mu.Unlock()
		return &bootstrapindex.Loaded{Registration: registered}, nil
	})
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Reload(context.Background(), application.Request{Revision: 1, Compile: func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
		return &application.Build{Index: snapshot, Materializer: materializerForAuth}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	pinned, service, err := manager.Pin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer mcpserver.Release(pinned)
	if err := service.(interface {
		PrepareTool(context.Context, string) error
	}).PrepareTool(pinned, "indexed.auth.tool"); err != nil {
		t.Fatal(err)
	}
	if materializer.count("Tool") != 1 || materializer.count("Auth") != 1 || materializer.count("Unused") != 0 {
		t.Fatalf("MCP transitive preparation calls=%v", materializer.calls)
	}
	entry, ok := service.Registry().ToolRegistry.Get("indexed.auth.tool")
	if !ok {
		t.Fatal("indexed tool was not prepared")
	}
	if entry.Metadata.InputSchema.Properties["Token"] == nil {
		t.Fatalf("transitive auth input missing from schema: %+v", entry.Metadata.InputSchema.Properties)
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestIndexedApplicationOpenAPILoadsOnFirstDefaultDocumentRequest(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/openapi"}).Write(t, root)
	for _, fixture := range []struct{ pkg, name, path string }{{"one", "One", "/one"}, {"two", "Two", "/two"}} {
		writeFixture(t, root, fixture.pkg+"/holder.go", `package `+fixture.pkg+`
import xdatly "github.com/viant/xdatly"
type Input struct { Name string `+"`"+`parameter:"Name,kind=query,in=name"`+"`"+` }
type Output struct { Name string }
type Holder struct { Route xdatly.Component[Input,Output] `+"`"+`component:"`+fixture.name+`,path=`+fixture.path+`,method=GET"`+"`"+` }
`)
	}
	snapshot, err := (bootstrapindex.Builder{Config: bootstrapindex.Config{BaseDir: root, Include: []string{"example.com/openapi/..."}}}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	materializer := &countingMaterializer{calls: map[string]int{}, regs: map[string]*registry.RegisteredComponent{}}
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Reload(context.Background(), application.Request{Revision: 1, Compile: func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
		return &application.Build{Index: snapshot, Materializer: materializer, HTTP: gateway.Config{OpenAPI: &gateway.OpenAPIConfig{Info: openapi3.Info{Title: "Indexed", Version: "1"}}}}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	if materializer.count("One")+materializer.count("Two") != 0 {
		t.Fatal("OpenAPI configuration materialized components at bootstrap")
	}
	normal := httptest.NewRecorder()
	manager.ServeHTTP(normal, httptest.NewRequest(http.MethodGet, "/one?name=Ada", nil))
	if normal.Code != http.StatusOK || materializer.count("One") != 1 || materializer.count("Two") != 0 {
		t.Fatalf("normal request status=%d calls=%v", normal.Code, materializer.calls)
	}
	for attempt := 0; attempt < 2; attempt++ {
		document := httptest.NewRecorder()
		manager.ServeHTTP(document, httptest.NewRequest(http.MethodGet, gateway.DefaultOpenAPIURI, nil))
		if document.Code != http.StatusOK {
			t.Fatalf("document status=%d body=%s", document.Code, document.Body.String())
		}
	}
	if materializer.count("One") != 1 || materializer.count("Two") != 1 {
		t.Fatalf("OpenAPI component cache calls=%v", materializer.calls)
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestIndexedApplicationReloadReleasesOldGenerationAfterRequest(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/lifecycle"}).Write(t, root)
	writeFixture(t, root, "api/holder.go", `package api
import xdatly "github.com/viant/xdatly"
type Input struct { Name string `+"`"+`parameter:"Name,kind=query,in=name"`+"`"+` }
type Output struct { Name string }
type Holder struct { Route xdatly.Component[Input,Output] `+"`"+`component:"Lifecycle,path=/lifecycle,method=GET"`+"`"+` }
`)
	snapshot, err := (bootstrapindex.Builder{Config: bootstrapindex.Config{BaseDir: root, Include: []string{"example.com/lifecycle/api"}}}).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	manager, err := application.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	first := &lifecycleMaterializer{started: make(chan struct{}), gate: make(chan struct{}), release: make(chan struct{})}
	reload := func(revision uint64, materializer bootstrapindex.Materializer) {
		t.Helper()
		if err := manager.Reload(context.Background(), application.Request{Revision: revision, Compile: func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
			return &application.Build{Index: snapshot, Materializer: materializer}, nil
		}}); err != nil {
			t.Fatal(err)
		}
	}
	reload(1, first)
	done := make(chan struct{})
	go func() {
		manager.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/lifecycle", nil))
		close(done)
	}()
	<-first.started
	second := &lifecycleMaterializer{release: make(chan struct{})}
	reload(2, second)
	select {
	case <-first.release:
		t.Fatal("old generation released during its in-flight request")
	default:
	}
	close(first.gate)
	<-done
	select {
	case <-first.release:
	case <-time.After(time.Second):
		t.Fatal("old generation was not released after request completion")
	}
	// Materialize generation two, then replace it without an active request.
	manager.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/lifecycle", nil))
	pinned, _, err := manager.Pin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	third := &lifecycleMaterializer{release: make(chan struct{})}
	reload(3, third)
	select {
	case <-second.release:
		t.Fatal("explicitly pinned generation was released before its pin")
	default:
	}
	mcpserver.Release(pinned)
	select {
	case <-second.release:
	case <-time.After(time.Second):
		t.Fatal("released explicit pin retained the replaced generation")
	}
	select {
	case <-third.release:
		t.Fatal("current generation released before shutdown")
	default:
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkIndexedCachedHTTPRequest(b *testing.B) {
	root := b.TempDir()
	(testharness.GeneratedModule{Path: "example.com/httpbench"}).Write(b, root)
	writeFixture(b, root, "api/holder.go", `package api
import xdatly "github.com/viant/xdatly"
type Input struct { Name string `+"`"+`parameter:"Name,kind=query,in=name"`+"`"+` }
type Output struct { Name string }
type Holder struct { Route xdatly.Component[Input,Output] `+"`"+`component:"HTTP,path=/http,method=GET"`+"`"+` }
`)
	snapshot, err := (bootstrapindex.Builder{Config: bootstrapindex.Config{BaseDir: root, Include: []string{"example.com/httpbench/api"}}}).Build(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	materializer := &countingMaterializer{calls: map[string]int{}, regs: map[string]*registry.RegisteredComponent{}}
	manager, err := application.New(nil)
	if err != nil {
		b.Fatal(err)
	}
	if err = manager.Reload(context.Background(), application.Request{Revision: 1, Compile: func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
		return &application.Build{Index: snapshot, Materializer: materializer}, nil
	}}); err != nil {
		b.Fatal(err)
	}
	manager.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/http?name=Ada", nil))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		manager.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/http?name=Ada", nil))
	}
	b.StopTimer()
	if err := manager.Shutdown(context.Background()); err != nil {
		b.Fatal(err)
	}
}
