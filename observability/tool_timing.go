package observability

import (
	"context"
	"sync"
	"time"
)

// ToolTimingMetaKey is an opt-in MCP result metadata field. Values are fixed
// numeric measurements; it never contains SQL, arguments, identities, or rows.
const ToolTimingMetaKey = "io.viant/timing/v1"

const (
	maxToolTimingDuration = 10 * time.Minute
	maxToolTimingCalls    = 65535
)

// ToolTimingPhase names the bounded phases available to a tool invocation.
// Durations are inclusive and may overlap. In particular, QueryAll includes
// SQLX cache, statement preparation, query, scan, and visitor work.
type ToolTimingPhase uint8

const (
	ToolTimingCatalog ToolTimingPhase = iota
	ToolTimingAdmission
	ToolTimingInput
	ToolTimingScope
	ToolTimingAuthorization
	ToolTimingScopeValidation
	ToolTimingQueryTransformInclusive
	ToolTimingResponse
	ToolTimingRevalidation
	ToolTimingMetadataBinding
	ToolTimingSchemaCompile
	ToolTimingReaderPrepare
	ToolTimingReaderBind
	ToolTimingReaderInvoke
	ToolTimingReaderTransform
	ToolTimingConnectorResolve
	ToolTimingQueryAllInclusive
	toolTimingPhaseCount
)

var toolTimingPhaseNames = [...]string{
	"catalog", "admission", "input", "scope", "authorization",
	"scope_validation", "query_transform_inclusive", "response", "revalidation",
	"metadata_binding", "schema_compile", "reader_prepare", "reader_bind",
	"reader_invoke", "reader_transform", "connector_resolve", "queryall_inclusive",
}

// ToolTimingMetric is cumulative within one invocation, including concurrent
// calls of the same phase. Counts describe observations, not authorization.
type ToolTimingMetric struct {
	DurationMs float64 `json:"durationMs"`
	Calls      uint64  `json:"calls"`
}

type toolTimingKey struct{}

// ToolTiming belongs to one opted-in HTTP invocation. The fixed array bounds
// its size independently of reader count or caller-controlled input.
type ToolTiming struct {
	mu        sync.Mutex
	durations [toolTimingPhaseCount]time.Duration
	calls     [toolTimingPhaseCount]uint64
}

// WithToolTiming creates a fresh invocation-local collector. Ordinary calls
// never allocate one and therefore emit no timing metadata.
func WithToolTiming(ctx context.Context) context.Context {
	return context.WithValue(ctx, toolTimingKey{}, &ToolTiming{})
}

func toolTimingFromContext(ctx context.Context) *ToolTiming {
	if ctx == nil {
		return nil
	}
	result, _ := ctx.Value(toolTimingKey{}).(*ToolTiming)
	return result
}

// StartToolTiming returns a no-op when the invocation did not opt in. The
// returned completion function is safe to invoke repeatedly and on error.
func StartToolTiming(ctx context.Context, phase ToolTimingPhase) func() {
	collector := toolTimingFromContext(ctx)
	if collector == nil || phase >= toolTimingPhaseCount {
		return func() {}
	}
	started := time.Now()
	var once sync.Once
	return func() {
		once.Do(func() {
			elapsed := time.Since(started)
			collector.mu.Lock()
			// Bound even a pathological recursive invocation. These diagnostic
			// caps never alter execution or turn a failure into a success.
			if remaining := maxToolTimingDuration - collector.durations[phase]; remaining > 0 {
				if elapsed > remaining {
					elapsed = remaining
				}
				collector.durations[phase] += elapsed
			}
			if collector.calls[phase] < maxToolTimingCalls {
				collector.calls[phase]++
			}
			collector.mu.Unlock()
		})
	}
}

// ToolTimingSnapshot returns a detached, fixed-name numeric map. Nil means
// the request had no opt-in collector or no observed phases.
func ToolTimingSnapshot(ctx context.Context) map[string]ToolTimingMetric {
	collector := toolTimingFromContext(ctx)
	if collector == nil {
		return nil
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	var result map[string]ToolTimingMetric
	for phase, name := range toolTimingPhaseNames {
		if collector.calls[phase] == 0 {
			continue
		}
		if result == nil {
			result = make(map[string]ToolTimingMetric, len(toolTimingPhaseNames))
		}
		result[name] = ToolTimingMetric{
			DurationMs: float64(collector.durations[phase]) / float64(time.Millisecond),
			Calls:      collector.calls[phase],
		}
	}
	return result
}
