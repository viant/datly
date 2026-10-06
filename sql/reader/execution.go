package reader

import (
	"context"
	"fmt"
	"github.com/viant/datly/observability"
	"reflect"
	"strings"

	"github.com/viant/datly/data"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/sqlx"
	"github.com/viant/sqlx/io/read/cache"
	xhandler "github.com/viant/xdatly/handler"
)

// Config contains immutable dependencies for a registered reader execution.
type Config struct {
	metricScope     string
	observations    map[*data.View]observability.Resolution
	observationErr  error
	Component       *spec.Component
	InputType       reflect.Type
	OutputType      reflect.Type
	Plan            *Plan
	SQL             *dsql.SQLComponent
	ReadCaches      map[*data.View]cache.Cache
	outputAccessors *outputAccessors
}

// Execution is a concurrency-safe registered reader. Mutable output and
// relation state are allocated in a fresh Session for every invocation.
type Execution struct {
	config  Config
	service *Service
}

var _ dexec.Reader = (*Execution)(nil)
var _ dexec.ReaderWarmupTargeter = (*Execution)(nil)

func NewExecution(config Config, options ...Option) (*Execution, error) {
	execution := &Execution{
		config: Config{
			Component:  config.Component,
			InputType:  config.InputType,
			OutputType: config.OutputType,
			Plan:       config.Plan,
			SQL:        config.SQL,
			ReadCaches: cloneCaches(config.ReadCaches),
		},
		service: NewService(options...),
	}
	session := execution.session()
	if err := session.Init(); err != nil {
		return nil, err
	}
	execution.config.outputAccessors = session.outputAccessors
	execution.config.metricScope = config.Component.Key.String()
	if err := execution.config.Plan.Validate(execution.config.OutputType); err != nil {
		return nil, err
	}
	if err := validatePlanWarmups(execution.config.Plan); err != nil {
		return nil, err
	}
	return execution, nil
}

// validatePlanWarmups fails initialization for duplicate effective warmup
// identities or unresolved shared case references instead of overwriting or
// ambiguously matching at request time.
func validatePlanWarmups(plan *Plan) error {
	if plan == nil || plan.Root == nil {
		return nil
	}
	visited := map[*ViewPlan]bool{}
	var visit func(*ViewPlan) error
	visit = func(item *ViewPlan) error {
		if item == nil || visited[item] {
			return nil
		}
		visited[item] = true
		if item.View != nil && item.View.Cache != nil {
			if _, err := item.View.Cache.EffectiveWarmups(); err != nil {
				return fmt.Errorf("view %q warmup: %w", item.View.Spec.Name, err)
			}
		}
		for _, relation := range item.Relations {
			if relation == nil {
				continue
			}
			if err := visit(relation.Target); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(plan.Root)
}

func (e *Execution) Read(ctx context.Context, input any, binder xhandler.Binder, resolver sqlx.ParameterResolver) (any, error) {
	if e == nil || e.service == nil {
		return nil, fmt.Errorf("reader execution is not initialized")
	}
	session := e.session()
	session.Parameters = resolver
	session.applyReadOptions(ctx)
	return e.service.Read(ctx, session, input, binder)
}

func (e *Execution) WarmupTargets() []dexec.ReaderWarmupTarget {
	if e == nil || e.config.Plan == nil || e.config.Plan.Root == nil {
		return nil
	}
	var result []dexec.ReaderWarmupTarget
	visited := map[*ViewPlan]bool{}
	var visit func(*ViewPlan, bool)
	visit = func(plan *ViewPlan, root bool) {
		if plan == nil || visited[plan] {
			return
		}
		visited[plan] = true
		if plan.View != nil && plan.View.Cache != nil {
			viewName := warmupTargetViewName(plan.View)
			if root {
				viewName = ""
			}
			// Initialization already rejected invalid warmup declarations; every
			// effective warmup becomes its own target so generated work retains
			// its originating warmup.
			warmups, err := plan.View.Cache.EffectiveWarmups()
			if err == nil {
				for _, settings := range warmups {
					result = append(result, dexec.ReaderWarmupTarget{View: viewName, Settings: settings})
				}
			}
		}
		for _, relation := range plan.Relations {
			if relation != nil {
				visit(relation.Target, false)
			}
		}
	}
	visit(e.config.Plan.Root, true)
	return result
}

func warmupTargetViewName(view *data.View) string {
	if view == nil {
		return ""
	}
	if name := strings.TrimSpace(view.Spec.Key.Name); name != "" {
		return name
	}
	return strings.TrimSpace(view.Spec.Name)
}

func (e *Execution) session() *Session {
	if e == nil {
		return nil
	}
	return &Session{
		Component:       e.config.Component,
		metricScope:     e.config.metricScope,
		observations:    e.config.observations,
		observationErr:  e.config.observationErr,
		InputType:       e.config.InputType,
		OutputType:      e.config.OutputType,
		Artifact:        e.config.Plan,
		SQL:             e.config.SQL,
		ReadCaches:      e.config.ReadCaches,
		outputAccessors: e.config.outputAccessors,
		Output:          Output{DataType: e.config.OutputType},
	}
}

func cloneCaches(source map[*data.View]cache.Cache) map[*data.View]cache.Cache {
	if len(source) == 0 {
		return nil
	}
	result := make(map[*data.View]cache.Cache, len(source))
	for view, service := range source {
		result[view] = service
	}
	return result
}

// WithRecorder returns a detached execution using an application-owned native
// metric service. Metadata and typed read plans remain immutable and shared.
func (e *Execution) WithRecorder(recorder *observability.Recorder) dexec.Reader {
	if e == nil {
		return e
	}
	result := *e
	service := *e.service
	service.recorder = recorder
	result.service = &service
	result.config.observationErr = nil
	if recorder == nil {
		result.config.observations = nil
		return &result
	}
	result.config.observations = map[*data.View]observability.Resolution{}
	for _, view := range e.observationViews() {
		resolved, err := recorder.Resolve(e.config.Component.Key, &view.Spec)
		if err != nil {
			result.config.observationErr = err
			break
		}
		result.config.observations[view] = resolved
	}
	return &result
}

func (e *Execution) observationViews() []*data.View {
	if e == nil || e.config.Plan == nil {
		return nil
	}
	var result []*data.View
	seen := map[*ViewPlan]bool{}
	var visit func(*ViewPlan)
	visit = func(plan *ViewPlan) {
		if plan == nil || seen[plan] {
			return
		}
		seen[plan] = true
		if plan.View != nil {
			result = append(result, plan.View)
		}
		for _, relation := range plan.Relations {
			if relation != nil {
				visit(relation.Target)
			}
		}
	}
	visit(e.config.Plan.Root)
	return result
}
func (e *Execution) ObservationTargets() []observability.ViewTarget {
	if e == nil || e.config.Component == nil {
		return nil
	}
	var result []observability.ViewTarget
	for _, view := range e.observationViews() {
		result = append(result, observability.ViewTarget{Component: e.config.Component.Key, View: &view.Spec})
	}
	return result
}
