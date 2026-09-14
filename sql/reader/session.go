package reader

import (
	"context"
	"fmt"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/observability"
	"github.com/viant/xdatly/response"
	"reflect"
	"sync"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader/readmeta"
	"github.com/viant/sqlx"
	sqlxread "github.com/viant/sqlx/io/read"
	"github.com/viant/sqlx/io/read/cache"
)

// Output carries typed reader output storage for one execution.
type Output struct {
	Metrics    response.Metrics
	Data       any
	DataType   reflect.Type
	DataPtr    any
	Projection *readmeta.Result
}

// Session contains only SQL reader execution state. Input binding, transport,
// handler capabilities, and registration remain runtime concerns.
type Session struct {
	metricScope       string
	pendingScope      string
	recorder          *observability.Recorder
	metricsMu         sync.Mutex
	rootRead          *viewRead
	Component         *spec.Component
	InputType         reflect.Type
	OutputType        reflect.Type
	Artifact          *Plan
	SQL               *dsql.SQLComponent
	ReadCaches        map[*data.View]cache.Cache
	Parameters        sqlx.ParameterResolver
	CollectProjection bool
	DryRun            bool
	RefreshCache      bool
	CacheOnly         bool
	QueryScope        *sqlxread.QueryScope
	outputSlots       map[string][]*readmeta.Record
	outputAccessors   *outputAccessors
	Output
}

func (s *Session) Init() error {
	if s == nil {
		return fmt.Errorf("reader session is required")
	}
	if s.Component == nil {
		return fmt.Errorf("reader session component is required")
	}
	if s.InputType == nil {
		return fmt.Errorf("reader session input type is required")
	}
	if s.Artifact == nil {
		return fmt.Errorf("reader session plan is required")
	}
	if s.SQL == nil {
		return fmt.Errorf("reader session SQL component is required")
	}
	// Resources are resolved once in the compiled plan. The component retains
	// authored URI/embed metadata and is not the executable source authority.
	if s.Artifact.Root == nil || s.Artifact.Root.View == nil {
		return fmt.Errorf("reader plan root view is required")
	}
	source := s.Artifact.Root.View.Spec.Source
	if source == nil || (source.SQL == "" && source.Table == "") {
		return fmt.Errorf("reader session root SQL or table is required")
	}
	if s.outputAccessors == nil {
		var err error
		s.outputAccessors, err = s.Artifact.compileOutputAccessors(s.OutputType)
		if err == nil {
			err = s.outputAccessors.compileMetrics(s.Artifact.OutputMetricsField, s.OutputType)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// applyReadOptions carries the same native query/cache policy into both ordinary
// reads and reads that expose column metadata to canonical dependency binding.
func (s *Session) applyReadOptions(ctx context.Context) {
	options := dexec.ReaderOptionsFromContext(ctx)
	s.RefreshCache = options.RefreshCache
	s.CacheOnly = options.CacheOnly
	s.QueryScope = options.QueryScope
}
