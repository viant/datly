package invocation

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"github.com/viant/datly/internal/testharness"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/viant/datly/exec"
	mcpinput "github.com/viant/datly/mcp/input"
	"github.com/viant/datly/spec"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	xexec "github.com/viant/xdatly/exec"
	xmcp "github.com/viant/xdatly/handler/mcp"
	"github.com/viant/xdatly/response"
)

type captureInvoker struct {
	request exec.ComponentRequest
	result  interface{}
	err     error
	check   func(context.Context)
}

func (i *captureInvoker) InvokeComponent(ctx context.Context, request exec.ComponentRequest) (interface{}, error) {
	i.request = request
	if i.check != nil {
		i.check(ctx)
	}
	return i.result, i.err
}

type statusError struct {
	status int
}

func (e statusError) Error() string   { return "validation failed" }
func (e statusError) StatusCode() int { return e.status }

func TestInvokerUsesExactProviderModeAndMCPContext(t *testing.T) {
	target := exec.ComponentTarget{
		Component: spec.Key{Kind: spec.KindComponent, Scope: "example.com/app", Name: "Search"},
		Route:     spec.RouteRef{Method: "POST", Path: "/search"},
	}
	captured := &captureInvoker{result: map[string]interface{}{"id": 7}}
	captured.check = func(ctx context.Context) {
		if value, ok := xmcp.LookupContext(ctx); !ok || value == nil {
			t.Fatal("MCP context was not attached")
		}
		if value := xexec.GetContext(ctx); value == nil || value.Method != schema.MethodToolsCall || value.URI != target.String() {
			t.Fatalf("execution context = %+v", value)
		}
	}
	result, protocolErr := invokeTool(context.Background(), New(Config{Invoker: captured}), Request{Target: target})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if captured.request.Target != target || captured.request.Input != nil || testharness.StructuredObject(t, result.StructuredContent)["id"] != float64(7) {
		t.Fatalf("request=%+v result=%+v", captured.request, result)
	}
	text := result.Content[0].(schema.TextContent)
	if text.Text != `{"id":7}` || result.IsError != nil {
		t.Fatalf("result = %+v", result)
	}
}

func TestInvokerPreservesProtocolMCPContext(t *testing.T) {
	target := exec.ComponentTarget{
		Component: spec.Key{Kind: spec.KindComponent, Name: "Search"},
		Route:     spec.RouteRef{Method: "POST", Path: "/search"},
	}
	protocolContext := &requestContext{}
	captured := &captureInvoker{result: map[string]interface{}{"ok": true}}
	captured.check = func(ctx context.Context) {
		actual, ok := xmcp.LookupContext(ctx)
		if !ok || actual != protocolContext {
			t.Fatalf("MCP context = %#v, want protocol context", actual)
		}
	}
	ctx := xmcp.WithContext(context.Background(), protocolContext)
	if _, protocolErr := invokeTool(ctx, New(Config{Invoker: captured}), Request{Target: target}); protocolErr != nil {
		t.Fatal(protocolErr)
	}
}

func TestInvokerReturnsComponentFailuresAsToolResults(t *testing.T) {
	target := exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "Test"}, Route: spec.RouteRef{Method: "POST", Path: "/test"}}
	result, protocolErr := invokeTool(context.Background(), New(Config{Invoker: &captureInvoker{err: statusError{status: 422}}}), Request{Target: target})
	if protocolErr != nil || result.IsError == nil || !*result.IsError || testharness.StructuredObject(t, result.StructuredContent)["status"] != 422 {
		t.Fatalf("result=%+v protocolErr=%v", result, protocolErr)
	}
	if testharness.StructuredObject(t, result.StructuredContent)["message"] != "validation failed" {
		t.Fatalf("error payload = %+v", result.StructuredContent)
	}
}

func TestInvokerKeepsArraySuccessUnstructured(t *testing.T) {
	target := exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "Test"}, Route: spec.RouteRef{Method: "GET", Path: "/test"}}
	result, protocolErr := invokeTool(context.Background(), New(Config{Invoker: &captureInvoker{result: []int{1, 2}}}), Request{Target: target})
	if protocolErr != nil || result.StructuredContent != nil || result.Content[0].(schema.TextContent).Text != `[1,2]` {
		t.Fatalf("result=%+v protocolErr=%v", result, protocolErr)
	}
}

func TestInvokerReadsAndDecompressesTransportResponseOnce(t *testing.T) {
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	_, _ = writer.Write([]byte(`{"ok":true}`))
	_ = writer.Close()
	transport := &singleBodyResponse{payload: compressed.Bytes(), status: http.StatusCreated, compression: "gzip"}
	target := exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "Test"}, Route: spec.RouteRef{Method: "GET", Path: "/test"}}
	result, protocolErr := invokeTool(context.Background(), New(Config{Invoker: &captureInvoker{result: transport}}), Request{Target: target})
	if protocolErr != nil || transport.reads != 1 || result.StructuredContent != nil {
		t.Fatalf("result=%+v reads=%d protocolErr=%v", result, transport.reads, protocolErr)
	}
	payload, err := base64.StdEncoding.DecodeString(result.Content[0].(schema.EmbeddedResource).Resource.Blob)
	if err != nil || string(payload) != `{"ok":true}` {
		t.Fatalf("raw transport payload=%q err=%v", payload, err)
	}
}

func TestInvokerUnavailableIsProtocolError(t *testing.T) {
	result, protocolErr := invokeTool(context.Background(), New(Config{}), Request{})
	if result != nil || protocolErr == nil {
		t.Fatalf("result=%+v protocolErr=%v", result, protocolErr)
	}
}

func TestInvokerDoesNotExposeInternalFailureDetails(t *testing.T) {
	target := exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "Test"}, Route: spec.RouteRef{Method: "POST", Path: "/test"}}
	result, protocolErr := invokeTool(context.Background(), New(Config{Invoker: &captureInvoker{err: errors.New("SQL password=secret")}}), Request{Target: target})
	if protocolErr != nil || testharness.StructuredObject(t, result.StructuredContent)["message"] != http.StatusText(http.StatusInternalServerError) {
		t.Fatalf("result=%+v protocolErr=%v", result, protocolErr)
	}
}

func TestInvokerAddsProtocolTokenAsImmutableAuthorizationProvider(t *testing.T) {
	target := exec.ComponentTarget{Component: spec.Key{Kind: spec.KindComponent, Name: "Test"}, Route: spec.RouteRef{Method: "POST", Path: "/test"}}
	plan, err := mcpinput.NewCompiler().Compile(nil)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := plan.Scope(mcpinput.Arguments(nil))
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		token string
		want  string
	}{
		{name: "raw bearer", token: "secret", want: "Bearer secret"},
		{name: "existing scheme", token: "Basic secret", want: "Basic secret"},
	} {
		t.Run(test.name, func(t *testing.T) {
			protocolToken := &authorization.Token{Token: test.token}
			captured := &captureInvoker{result: map[string]interface{}{"ok": true}}
			ctx := context.WithValue(context.Background(), authorization.TokenKey, protocolToken)
			_, protocolErr := invokeTool(ctx, New(Config{Invoker: captured}), Request{Target: target, Scope: scope})
			if protocolErr != nil {
				t.Fatal(protocolErr)
			}
			if protocolToken.Token != test.token {
				t.Fatalf("protocol token was mutated: %q", protocolToken.Token)
			}
			if len(captured.request.Providers) != 1 {
				t.Fatalf("providers = %+v", captured.request.Providers)
			}
			value, ok, locateErr := captured.request.Providers[0].Locate(nil).Value(context.Background(), reflect.TypeOf(""), "authorization")
			if locateErr != nil || !ok || value != test.want {
				t.Fatalf("authorization=%#v ok=%v err=%v", value, ok, locateErr)
			}
		})
	}
}

func invokeTool(ctx context.Context, invoker *Invoker, request Request) (*schema.CallToolResult, *jsonrpc.Error) {
	request.Method = schema.MethodToolsCall
	request.URI = request.Target.String()
	execution, protocolErr := invoker.Execute(ctx, request)
	if protocolErr != nil {
		return nil, protocolErr
	}
	return execution.ToolResult(), nil
}

type singleBodyResponse struct {
	payload     []byte
	status      int
	compression string
	reads       int
}

func (r *singleBodyResponse) Body() io.Reader {
	r.reads++
	return bytes.NewReader(r.payload)
}
func (r *singleBodyResponse) Headers() http.Header    { return nil }
func (r *singleBodyResponse) Size() int               { return len(r.payload) }
func (r *singleBodyResponse) StatusCode() int         { return r.status }
func (r *singleBodyResponse) SetStatusCode(value int) { r.status = value }
func (r *singleBodyResponse) CompressionType() string { return r.compression }

var _ response.Response = (*singleBodyResponse)(nil)
var _ response.Compressed = (*singleBodyResponse)(nil)
var _ xmcp.Context = (*requestContext)(nil)
