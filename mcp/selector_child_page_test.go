package mcp

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	fixture "github.com/viant/datly/bootstrap/testdata/companionchildpage"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/mcp-protocol/schema"
	"reflect"
	"sync"
	"testing"
)

func TestMCPChildPageDoesNotBecomeDeferredRootPage(t *testing.T) {
	var observed *selectorInvocationObserver
	service := selectorSideService(t, reflect.TypeFor[fixture.Component](), reflect.TypeFor[fixture.Input](), reflect.TypeFor[fixture.Output](), "ReadNestedPage", func(config *Config) {
		observed = &selectorInvocationObserver{ComponentInvoker: config.Invoker}
		config.Invoker = observed
	})
	client := mcpclient.New(t, service, schema.LatestProtocolVersion)
	for _, tc := range []struct{ page, children int }{{1, 1}, {2, 0}} {
		result, err := client.CallTool(t.Context(), &schema.CallToolRequestParams{Name: "records.select", Arguments: map[string]any{"page": 1, "childPage": tc.page, "childLimit": 1}})
		require.NoError(t, err)
		require.False(t, result.IsError != nil && *result.IsError, "native cause: %v", observed.cause())
		wire, err := json.Marshal(result.StructuredContent)
		require.NoError(t, err)
		var output fixture.Output
		require.NoError(t, json.Unmarshal(wire, &output))
		require.Len(t, output.Data, 2, "child paging must not page the parent")
		for _, row := range output.Data {
			require.Len(t, row.Children, tc.children)
		}
	}
}

type selectorInvocationObserver struct {
	dexec.ComponentInvoker
	err error
	mu  sync.Mutex
}

func (o *selectorInvocationObserver) InvokeComponent(ctx context.Context, request dexec.ComponentRequest) (any, error) {
	value, err := o.ComponentInvoker.InvokeComponent(ctx, request)
	o.mu.Lock()
	o.err = err
	o.mu.Unlock()
	return value, err
}

func (o *selectorInvocationObserver) cause() error { o.mu.Lock(); defer o.mu.Unlock(); return o.err }
