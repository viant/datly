package extension

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view/state"
	"github.com/viant/jsonrpc"
	"github.com/viant/jsonrpc/transport"
	pclient "github.com/viant/mcp-protocol/client"
	"github.com/viant/mcp-protocol/schema"
	serverproto "github.com/viant/mcp-protocol/server"
)

type fakeClientOps struct{}

func (f *fakeClientOps) Notify(ctx context.Context, notification *jsonrpc.Notification) error {
	return nil
}

func (f *fakeClientOps) NextRequestID() jsonrpc.RequestId {
	return 1
}

func (f *fakeClientOps) LastRequestID() jsonrpc.RequestId {
	return 1
}

func (f *fakeClientOps) ListRoots(ctx context.Context, request *jsonrpc.TypedRequest[*schema.ListRootsRequest]) (*schema.ListRootsResult, *jsonrpc.Error) {
	return &schema.ListRootsResult{}, nil
}

func (f *fakeClientOps) CreateMessage(ctx context.Context, request *jsonrpc.TypedRequest[*schema.CreateMessageRequest]) (*schema.CreateMessageResult, *jsonrpc.Error) {
	return &schema.CreateMessageResult{}, nil
}

func (f *fakeClientOps) Elicit(ctx context.Context, request *jsonrpc.TypedRequest[*schema.ElicitRequest]) (*schema.ElicitResult, *jsonrpc.Error) {
	return &schema.ElicitResult{}, nil
}

func (f *fakeClientOps) Implements(method string) bool {
	switch method {
	case schema.MethodElicitationCreate, schema.MethodSamplingCreateMessage:
		return true
	default:
		return false
	}
}

func (f *fakeClientOps) Init(ctx context.Context, capabilities *schema.ClientCapabilities) {}

var _ pclient.Operations = (*fakeClientOps)(nil)
var _ transport.Notifier = (*fakeClientOps)(nil)
var _ transport.Sequencer = (*fakeClientOps)(nil)

func TestHandler_CallTool_InjectsMCPContext(t *testing.T) {
	registry := serverproto.NewRegistry()
	registry.RegisterTool(&serverproto.ToolEntry{
		Metadata: schema.Tool{Name: "test"},
		Handler: func(ctx context.Context, request *schema.CallToolRequest) (*schema.CallToolResult, *jsonrpc.Error) {
			mcp, ok := state.LookupMCPContext(ctx)
			require.True(t, ok)
			require.NotNil(t, mcp)
			require.NotNil(t, mcp.Client())
			assert.True(t, mcp.Client().CanElicit())
			assert.True(t, mcp.Client().CanGenerateContent())
			return &schema.CallToolResult{}, nil
		},
	})

	newHandler := New(registry)
	actual, err := newHandler(context.Background(), nil, nil, &fakeClientOps{})
	require.NoError(t, err)

	typed := &jsonrpc.TypedRequest[*schema.CallToolRequest]{
		Request: &schema.CallToolRequest{
			Method: schema.MethodToolsCall,
			Params: schema.CallToolRequestParams{Name: "test"},
		},
	}

	result, rpcErr := actual.CallTool(context.Background(), typed)
	require.Nil(t, rpcErr)
	require.NotNil(t, result)
}
