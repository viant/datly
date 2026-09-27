package invocation

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/spec"
	xexec "github.com/viant/xdatly/exec"
	"github.com/viant/xdatly/response"
	"github.com/viant/xdatly/tracing"
)

type tracingInvoker struct{}

func (tracingInvoker) InvokeComponent(ctx context.Context, _ exec.ComponentRequest) (any, error) {
	current := xexec.GetContext(ctx)
	current.Header["local"] = "value"
	current.SetValue("local", true)
	current.StatusCode = 201
	current.AppendMetrics(&response.Metric{View: "local"})
	span := tracing.NewSpan("local", "INTERNAL", nil, time.Now(), time.Now())
	current.Trace.Append(&span)
	return nil, nil
}

func TestMCPInvocationTraceIsolation(t *testing.T) {
	for _, mode := range []string{"independent", "parent id", "parent trace", "empty parent"} {
		t.Run(mode, func(t *testing.T) {
			parent := xexec.New(xexec.WithMethod("parent"), xexec.WithURI("/parent"), xexec.WithTraceResource("parent", "1"))
			parent.Header["parent"] = "untouched"
			parent.SetValue("parent", true)
			if mode == "parent trace" {
				parent.TraceID = ""
			} else if mode == "empty parent" {
				parent.TraceID, parent.Trace = "", nil
			}
			before := parent.SnapshotForLogging()
			base, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			ctx := base
			if mode != "independent" {
				ctx = xexec.WithContext(ctx, parent)
			}
			target := exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "Test"}, Route: spec.RouteRef{Method: "GET", Path: "/test"}}
			invoker := New(Config{Invoker: tracingInvoker{}})
			const calls = 8
			results := make([]*Execution, calls)
			var wg sync.WaitGroup
			for index := range results {
				wg.Add(1)
				go func() {
					defer wg.Done()
					result, err := invoker.Execute(ctx, Request{Target: target, Method: "tools/call", URI: target.String()})
					if err != nil {
						t.Errorf("execute: %v", err)
						return
					}
					results[index] = result
				}()
			}
			wg.Wait()
			ids := map[string]bool{}
			contexts := map[*xexec.Context]bool{}
			traces := map[*tracing.Trace]bool{}
			for _, result := range results {
				require.NotNil(t, result)
				current := result.context
				require.NotSame(t, parent, current)
				require.NotSame(t, parent.Trace, current.Trace)
				require.NotEmpty(t, current.TraceID)
				require.Equal(t, current.TraceID, current.Trace.TraceID)
				require.Equal(t, "tools/call", current.Method)
				require.Equal(t, target.String(), current.URI)
				require.Len(t, current.Metrics, 1)
				require.Len(t, current.Trace.Spans, 1)
				require.Equal(t, map[string]string{"local": "value"}, current.Header)
				_, inherited := current.Value("parent")
				require.False(t, inherited)
				deadline, ok := result.encodingContext.Deadline()
				wantDeadline, _ := ctx.Deadline()
				require.True(t, ok)
				require.Equal(t, wantDeadline, deadline)
				ids[current.TraceID], contexts[current], traces[current.Trace] = true, true, true
			}
			require.Len(t, contexts, calls)
			require.Len(t, traces, calls)
			if mode == "parent id" || mode == "parent trace" {
				require.Equal(t, map[string]bool{parent.Trace.TraceID: true}, ids)
			} else {
				require.Len(t, ids, calls)
			}
			after := parent.SnapshotForLogging()
			before.ElapsedMs, after.ElapsedMs = 0, 0
			require.Equal(t, before, after, "parent execution context must not be mutated")
			cancel()
			for _, result := range results {
				require.ErrorIs(t, result.encodingContext.Err(), context.Canceled)
			}
		})
	}
}
