package reader

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	rhandler "github.com/viant/datly/runtime/handler"
	handlerengine "github.com/viant/datly/runtime/handler/engine"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io/read/cache"
	xhandler "github.com/viant/xdatly/handler"
)

// Session is test-only assembly input for SQL reader behavior tests. Production
// runtime registration receives a preconfigured exec.Reader instead.
type Session struct {
	Component    *spec.Component
	Input        *registry.RouteInputContract
	OutputType   reflect.Type
	Artifact     *sqlreader.Plan
	SQL          *dsql.SQLComponent
	Scope        dexec.ProviderScope
	ReadCaches   map[*data.View]cache.Cache
	Capabilities rhandler.InvocationCapabilities
	Providers    []locator.Provider
}

var readerTestRoute = spec.RouteRef{Method: "TEST", Path: "/reader"}

func buildArtifact(input bootstrap.ArtifactInput) (*bootstrap.Artifact, error) {
	component := input.Component.Clone()
	if component == nil {
		component = &spec.Component{}
	}
	component.Routes = []*spec.Route{{Method: readerTestRoute.Method, Path: readerTestRoute.Path}}
	input.Component = component
	return bootstrap.BuildArtifact(input)
}

func routeInput(t *testing.T, artifact *bootstrap.Artifact) *registry.RouteInputContract {
	t.Helper()
	if artifact == nil || artifact.Input == nil {
		t.Fatal("reader artifact input contract is required")
	}
	result, ok := artifact.Input.ForRoute(readerTestRoute)
	if !ok {
		t.Fatal("reader test route input contract was not found")
	}
	return result
}

func minimalPlan(component *spec.Component) *sqlreader.Plan {
	view := data.FromComponent(component)
	plan, err := sqlreader.NewPlan(sqlreader.PlanConfig{
		RootView: view, ViewIndex: sqlreader.NewViewIndex(component, view),
	})
	if err != nil {
		panic(err)
	}
	return plan
}

type Service struct {
	relationFetchConcurrency int
}

func NewService() *Service {
	return &Service{}
}

func (s *Service) Read(ctx context.Context, session *Session) (any, error) {
	execution, err := s.execution(session)
	if err != nil {
		return nil, err
	}
	return handlerengine.New().Execute(ctx, handlerengine.Request{
		Input:        session.Input,
		Scope:        session.Scope,
		Capabilities: session.Capabilities,
		Providers:    session.Providers,
		Handler:      NewHandler(execution, session.Input),
	})
}

func (s *Service) ReadBound(ctx context.Context, session *Session, input any) (any, error) {
	return s.readBoundWithBinder(ctx, session, input, boundInputBinder{input: input})
}

type boundInputBinder struct {
	input any
}

func (b boundInputBinder) Bind(context.Context, any) error { return nil }

func (b boundInputBinder) Lookup(_ context.Context, key xhandler.ValueKey) (any, bool, error) {
	if key != xhandler.InputKey {
		return nil, false, nil
	}
	return b.input, true, nil
}

func (s *Service) readBoundWithBinder(ctx context.Context, session *Session, input any, binder xhandler.Binder) (any, error) {
	execution, err := s.execution(session)
	if err != nil {
		return nil, err
	}
	resolver := sqlx.ParameterResolver(session.Input.Resolver(input))
	return execution.Read(ctx, input, binder, resolver)
}

func (s *Service) execution(session *Session) (*sqlreader.Execution, error) {
	if session == nil {
		return nil, fmt.Errorf("reader session is required")
	}
	if session.Input == nil {
		return nil, fmt.Errorf("reader route input contract is required")
	}
	options := []sqlreader.Option(nil)
	if s != nil && s.relationFetchConcurrency > 0 {
		options = append(options, sqlreader.WithRelationFetchConcurrency(s.relationFetchConcurrency))
	}
	return sqlreader.NewExecution(sqlreader.Config{
		Component:  session.Component,
		InputType:  session.Input.Type(),
		OutputType: session.OutputType,
		Plan:       session.Artifact,
		SQL:        session.SQL,
		ReadCaches: session.ReadCaches,
	}, options...)
}
