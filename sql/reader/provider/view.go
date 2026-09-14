// Package provider adapts SQL reader dependencies to Bindly providers.
package provider

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/data"
	dsql "github.com/viant/datly/sql"
	sqlreader "github.com/viant/datly/sql/reader"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io/read/cache"
	"github.com/viant/structology"
	xhandler "github.com/viant/xdatly/handler"
)

// Config supplies execution dependencies for independent views.
type Config struct {
	Dependencies []*sqlreader.ViewDependency
	Input        InputResolver
	SQL          *dsql.SQLComponent
	ReadCaches   map[string]map[*data.View]cache.Cache
}

type viewProvider struct {
	views map[string]*boundView
	input InputResolver
}

type boundView struct {
	targetType reflect.Type
	execution  *sqlreader.Execution
}

type viewLocator struct {
	views    map[string]*boundView
	contract InputResolver
	input    any
}

// InputResolver is the narrow canonical-input port needed by independent view
// DI. Runtime input contracts satisfy it without exposing their projection.
type InputResolver interface {
	Resolver(target any) bindly.ValueResolver
}

// New creates the Bindly provider for typed, DI-backed view reads.
func New(config Config) (locator.Provider, error) {
	if len(config.Dependencies) == 0 {
		return nil, fmt.Errorf("view dependencies are required")
	}
	if config.SQL == nil {
		return nil, fmt.Errorf("view provider SQL component is required")
	}
	provider := &viewProvider{
		views: make(map[string]*boundView, len(config.Dependencies)), input: config.Input,
	}
	for _, dependency := range config.Dependencies {
		if dependency == nil {
			return nil, fmt.Errorf("view dependency is required")
		}
		name := strings.TrimSpace(dependency.Name)
		if name == "" {
			return nil, fmt.Errorf("view dependency name is required")
		}
		if _, ok := provider.views[name]; ok {
			return nil, fmt.Errorf("duplicate view dependency %q", name)
		}
		if dependency.Component == nil {
			return nil, fmt.Errorf("view dependency %s component is required", name)
		}
		if config.Input == nil {
			return nil, fmt.Errorf("view provider input contract is required")
		}
		execution, err := sqlreader.NewExecution(sqlreader.Config{
			Component:  dependency.Component,
			InputType:  dependency.InputType,
			OutputType: dependency.TargetType,
			Plan:       dependency.Plan,
			SQL:        config.SQL,
			ReadCaches: config.ReadCaches[name],
		})
		if err != nil {
			return nil, fmt.Errorf("initialize view dependency %s: %w", name, err)
		}
		provider.views[name] = &boundView{targetType: dependency.TargetType, execution: execution}
	}
	return provider, nil
}

func (p *viewProvider) Kind() string           { return sqlreader.ViewDependencyKind }
func (p *viewProvider) Priority() int          { return locator.PriorityDependent }
func (p *viewProvider) DefaultCacheable() bool { return false }
func (p *viewProvider) Locate(state *structology.State) locator.Locator {
	if state == nil {
		return nil
	}
	input := state.StatePtr()
	if input == nil {
		input = state.State()
	}
	return &viewLocator{views: p.views, contract: p.input, input: input}
}

func (l *viewLocator) Kind() string { return sqlreader.ViewDependencyKind }

func (l *viewLocator) Value(context.Context, reflect.Type, string) (any, bool, error) {
	return nil, false, fmt.Errorf("view locator requires an active Bindly scope")
}

func (l *viewLocator) ValueInScope(ctx context.Context, scope locator.Scope, targetType reflect.Type, name string) (any, bool, error) {
	view := l.views[name]
	if view == nil {
		return nil, false, nil
	}
	if targetType != nil && targetType != view.targetType {
		return nil, false, fmt.Errorf("view %s targets %s, not %s", name, view.targetType, targetType)
	}
	resolver := sqlx.ParameterResolver(l.contract.Resolver(l.input))
	var output any
	var projection *readProjection
	var err error
	if requested, ok := scope.(locator.MetadataScope); ok && requested.MetadataRequested() {
		var result *sqlreader.ReadResult
		result, err = view.execution.ReadResult(ctx, l.input, scopeBinder{scope: scope}, resolver)
		if err == nil {
			output = result.Data
			projection = &readProjection{result: result.Projection}
		}
	} else {
		output, err = view.execution.Read(ctx, l.input, scopeBinder{scope: scope}, resolver)
	}
	if err != nil {
		return nil, false, err
	}
	if output != nil && reflect.TypeOf(output) != view.targetType {
		return nil, false, fmt.Errorf("view %s returned %T, want %s", name, output, view.targetType)
	}
	if projection != nil {
		return locator.ValueWithMetadata{Value: output, Metadata: projection}, true, nil
	}
	return output, true, nil
}

type scopeBinder struct {
	scope locator.Scope
}

func (b scopeBinder) Bind(ctx context.Context, target any) error {
	if b.scope == nil {
		return fmt.Errorf("binding scope is required")
	}
	return b.scope.BindTarget(ctx, target)
}

func (b scopeBinder) Lookup(ctx context.Context, key xhandler.ValueKey) (any, bool, error) {
	// An independent input view is not part of the component's output graph.
	// Source-root projections (including report selectors) must not be applied
	// to its unrelated columns. Its own compiled selector bindings still apply.
	if key == xhandler.SelectorsKey {
		return nil, false, nil
	}
	if b.scope == nil {
		return nil, false, fmt.Errorf("binding scope is required")
	}
	return b.scope.Value(ctx, &bindstate.Location{Kind: string(key)})
}

var _ locator.ScopedLocator = (*viewLocator)(nil)
var _ xhandler.Binder = scopeBinder{}
