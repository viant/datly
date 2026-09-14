package reader

import (
	"context"
	"fmt"
	"github.com/viant/datly/observability"
	"reflect"

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
	return execution, nil
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

func (e *Execution) session() *Session {
	if e == nil {
		return nil
	}
	return &Session{
		Component:       e.config.Component,
		metricScope:     e.config.metricScope,
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
	return &result
}
