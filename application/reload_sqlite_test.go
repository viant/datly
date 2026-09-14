package application_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/application"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/observability"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/sqlx"
	"github.com/viant/x"
	xshape "github.com/viant/x/shape"
	xhandler "github.com/viant/xdatly/handler"
)

type storedSource struct {
	Revision int
	DQL      string
	Kind     string
}
type reloadFixture struct {
	db      *sqlite.Harness
	started chan struct{}
	release chan struct{}
	block   bool
}

func (f *reloadFixture) init(t *testing.T) {
	t.Helper()
	f.db = sqlite.New(t)
	f.started = make(chan struct{}, 1)
	f.release = make(chan struct{})
	if err := f.db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER,name TEXT)", "INSERT INTO records VALUES(7,'new')", "CREATE TABLE sources(revision INTEGER,dql TEXT,kind TEXT)", "INSERT INTO sources VALUES(1,'SELECT id AS value FROM records','int'),(2,'SELECT name AS value FROM records','string'),(3,'#setting($_ = $route('''' , ''''GET''''))','bool')"); err != nil {
		t.Fatal(err)
	}
}

func (f *reloadFixture) compile(revision int) func(context.Context, *typecatalog.Catalog) (*application.Build, error) {
	return func(ctx context.Context, types *typecatalog.Catalog) (*application.Build, error) {
		rows, err := f.db.ReadQuery(ctx, sqlite.Query{SQL: "SELECT revision,dql,kind FROM sources WHERE revision=?", Args: []any{revision}}, reflect.TypeOf([]storedSource{}))
		if err != nil {
			return nil, err
		}
		sources := rows.([]storedSource)
		if len(sources) != 1 {
			return nil, fmt.Errorf("missing source revision")
		}
		source := sources[0]
		rowType, err := (xshape.Runtime{}).Struct([]xshape.RuntimeField{{Name: "Value", TypeExpr: source.Kind, Tag: `sqlx:"value" json:"value"`}})
		if err != nil {
			return nil, err
		}
		if err := types.Register(typecatalog.TypeOriginDQL, &x.Type{PkgPath: "example.com/reload", Name: "Row", Type: rowType}); err != nil {
			return nil, err
		}
		outputType, err := (xshape.Runtime{}).Struct([]xshape.RuntimeField{{Name: "Rows", Type: reflect.SliceOf(rowType), Tag: `json:"rows"`}})
		if err != nil {
			return nil, err
		}
		result := &application.Build{}
		for _, name := range []string{"Child", "Records"} {
			path := "/" + strings.ToLower(name)
			text := fmt.Sprintf("#setting($_ = $route('%s','GET'))\n", path)
			if name == "Records" {
				text += fmt.Sprintf("#setting($_ = $mcp('records.v%d'))\n", revision)
			}
			text += source.DQL
			compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{Scope: "example.com/reload", Name: name, Text: text, Types: types})
			if err != nil {
				return nil, err
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, Types: types, InputType: reflect.TypeOf(struct{}{}), OutputType: outputType, DirectViewField: "Rows"})
			if err != nil {
				return nil, err
			}
			reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: f.db.DB}})
			if err != nil {
				return nil, err
			}
			if name == "Records" && revision == 1 && f.block {
				reader = &nestedReader{Reader: reader, started: f.started, release: f.release}
			}
			result.Components = append(result.Components, &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, Output: artifact.Output, OutputType: outputType, Reader: reader})
		}
		return result, nil
	}
}

type nestedReader struct {
	exec.Reader
	started chan struct{}
	release chan struct{}
}

// The blocking test decorator forwards the concrete reader's application
// service wiring so it exercises the same recorder as unwrapped executions.
func (r *nestedReader) WithRecorder(recorder *observability.Recorder) exec.Reader {
	result := *r
	owner, ok := r.Reader.(interface {
		WithRecorder(*observability.Recorder) exec.Reader
	})
	if !ok {
		panic("nested reader fixture requires recorder wiring")
	}
	result.Reader = owner.WithRecorder(recorder)
	return &result
}

func (r *nestedReader) Read(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (any, error) {
	r.started <- struct{}{}
	select {
	case <-r.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	value, found, err := binder.Lookup(ctx, exec.ComponentInvokerKey)
	if err != nil || !found {
		return nil, fmt.Errorf("nested invoker missing: %v", err)
	}
	nested, err := value.(exec.ComponentInvoker).InvokeComponent(ctx, exec.ComponentRequest{Target: exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/reload", Name: "Child"}, Route: spec.RouteRef{Method: "GET", Path: "/child"}}})
	if err != nil {
		return nil, err
	}
	actual, err := r.Reader.Read(ctx, input, binder, resolver)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(nested, actual) {
		return nil, fmt.Errorf("nested invocation crossed generation: %T vs %T", nested, actual)
	}
	return actual, nil
}

func TestDBDQLReloadPinsConcurrentInvocationsSQLite(t *testing.T) {
	for _, protocol := range []string{"http", "mcp-session", "mcp-stateless"} {
		t.Run(protocol, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			f := &reloadFixture{block: true}
			f.init(t)
			manager, err := application.New(nil)
			if err != nil {
				t.Fatal(err)
			}
			if err = manager.Reload(ctx, application.Request{Revision: 1, Compile: f.compile(1)}); err != nil {
				t.Fatal(err)
			}
			call := func(version int) (string, error) {
				response := httptest.NewRecorder()
				manager.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/records", nil).WithContext(ctx))
				if response.Code != 200 {
					return "", fmt.Errorf("HTTP status %d: %s", response.Code, response.Body.String())
				}
				return strings.TrimSpace(response.Body.String()), nil
			}
			if protocol != "http" {
				version := schema.LegacyProtocolVersion
				if protocol == "mcp-stateless" {
					version = schema.LatestProtocolVersion
				}
				client := (mcpclient.Config{Source: manager, ProtocolVersion: version}).New(t)
				call = func(revision int) (string, error) {
					result, err := client.CallTool(ctx, &schema.CallToolRequestParams{Name: fmt.Sprintf("records.v%d", revision)})
					if err != nil {
						return "", err
					}
					if result.IsError != nil && *result.IsError {
						return "", fmt.Errorf("tool error: %+v", result)
					}
					data, err := json.Marshal(result.StructuredContent)
					return string(data), err
				}
			}
			type response struct {
				body string
				err  error
			}
			old := make(chan response, 1)
			go func() { body, err := call(1); old <- response{body, err} }()
			select {
			case <-f.started:
			case <-ctx.Done():
				t.Fatal("old request did not enter reader")
			}
			if err := manager.Reload(ctx, application.Request{Revision: 2, Compile: f.compile(2)}); err != nil {
				t.Fatal(err)
			}
			if actual, err := call(2); err != nil || actual != `{"rows":[{"value":"new"}]}` {
				t.Fatalf("new body=%s err=%v", actual, err)
			}
			close(f.release)
			select {
			case result := <-old:
				if result.err != nil || result.body != `{"rows":[{"value":7}]}` {
					t.Fatalf("old=%+v", result)
				}
			case <-ctx.Done():
				t.Fatal("old request did not finish")
			}
			if protocol != "http" {
				if _, err := call(1); err == nil {
					t.Fatal("removed tool remained on old session")
				}
			}
			catalog, err := manager.Types(ctx)
			if err != nil {
				t.Fatal(err)
			}
			descriptor, found, err := catalog.Resolve(typecatalog.PackageAuthority, "example.com/reload.Row")
			if err != nil || !found || descriptor.Type.Field(0).Type.Kind() != reflect.String {
				t.Fatalf("published type=%+v found=%v err=%v", descriptor, found, err)
			}
			if err := manager.Reload(ctx, application.Request{Revision: 3, Compile: f.compile(3)}); err == nil {
				t.Fatal("invalid reload accepted")
			}
			if manager.Revision() != 2 {
				t.Fatal("failed reload published")
			}
			if actual, err := call(2); err != nil || actual != `{"rows":[{"value":"new"}]}` {
				t.Fatalf("retained body=%s err=%v", actual, err)
			}
			f.db.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT revision,dql,kind FROM sources WHERE revision=2"}, []storedSource{{2, "SELECT name AS value FROM records", "string"}})
		})
	}
}
