package runtime

import (
	"context"
	dexec "github.com/viant/datly/exec"
	xexec "github.com/viant/xdatly/exec"
	"testing"
	"time"
)

func TestDryRunObservationDoesNotReportExecutedSQL(t *testing.T) {
	fixture := newAsyncReaderFixture(t)
	callbacks := 0
	var components []*RegisteredComponent
	for _, component := range fixture.runtime.registered {
		components = append(components, component)
	}
	configured, err := NewRuntime(components, WithObservability(ObservabilityConfig{ReadingData: func(string, time.Duration, string, int, []any, error) { callbacks++ }}))
	if err != nil {
		t.Fatal(err)
	}
	fixture.runtime = configured
	captured := xexec.New()
	// The application table deliberately does not exist. Planning must not read it.
	result, err := fixture.runtime.InvokeComponent(xexec.WithContext(context.Background(), captured), dexec.ComponentRequest{Target: fixture.target, Input: &asyncReaderInput{ID: 7, Tenant: 1}, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := result.(*dexec.ReadPlan); !ok {
		t.Fatalf("result=%T", result)
	}
	if len(captured.Metrics) != 1 {
		t.Fatalf("planning metrics=%d", len(captured.Metrics))
	}
	if callbacks != 0 {
		t.Fatal("planning emitted ReadingData")
	}
	metric := captured.Metrics[0]
	if metric.Rows != 0 || len(metric.Executions) != 0 || metric.Error != "" {
		t.Fatalf("planning reported SQL execution: %+v", metric)
	}
}
