package resource

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/datly/spec"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
	xexec "github.com/viant/xdatly/exec"
	"github.com/viant/xdatly/response"
)

type resourceCaptureInvoker struct {
	request exec.ComponentRequest
	result  interface{}
	err     error
	check   func(context.Context)
}

func (i *resourceCaptureInvoker) InvokeComponent(ctx context.Context, request exec.ComponentRequest) (interface{}, error) {
	i.request = request
	if i.check != nil {
		i.check(ctx)
	}
	return i.result, i.err
}

func TestHandlerExecutesConcreteTemplateThroughExactInvocation(t *testing.T) {
	contract := newResourceContract(t, "/orders/{id}", []bindly.BindingSpec{
		{Path: "ID", Location: bindstate.Location{Kind: "path", In: "id"}},
	})
	compiler, _ := NewCompiler("datly://localhost")
	plan, err := compiler.Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Name: "Order"},
		Route:     &spec.Route{Method: http.MethodGet, Path: "/orders/{id}"},
		Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureResourceTemplate, Name: "orders", MIMEType: "application/json"},
		Contract:  contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	catalog, _ := NewCatalog([]*Plan{plan})
	captured := &resourceCaptureInvoker{result: map[string]interface{}{"ok": true}}
	captured.check = func(ctx context.Context) {
		actual := xexec.GetContext(ctx)
		if actual == nil || actual.Method != schema.MethodResourcesRead || actual.URI != "datly://localhost/orders/a%2Fb" {
			t.Fatalf("execution context = %+v", actual)
		}
	}
	handler := NewHandler(catalog, invocation.New(invocation.Config{Invoker: captured}))
	result, protocolErr := handler.Handle(context.Background(), &schema.ReadResourceRequest{
		Method: schema.MethodResourcesRead, Params: schema.ReadResourceRequestParams{Uri: "datly://localhost/orders/a%2Fb"},
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	if captured.request.Target != plan.Target() || captured.request.Input != nil || len(captured.request.Providers) != 1 {
		t.Fatalf("component request = %+v", captured.request)
	}
	value, found, locateErr := captured.request.Providers[0].Locate(nil).Value(context.Background(), reflect.TypeOf(""), "id")
	if locateErr != nil || !found || value != "a/b" {
		t.Fatalf("path value=%#v found=%v err=%v", value, found, locateErr)
	}
	content := result.Contents[0]
	if content.Uri != "datly://localhost/orders/a%2Fb" || content.Text != `{"ok":true}` || content.Blob != "" || content.MimeType == nil || *content.MimeType != "application/json" {
		t.Fatalf("content = %+v", content)
	}
	assertSerializedContent(t, content, `{"ok":true}`, "")
}

func TestHandlerEmitsBinaryBlobAndReadsResponseOnce(t *testing.T) {
	contract := newResourceContract(t, "/logo", nil)
	compiler, _ := NewCompiler("datly://localhost")
	plan, err := compiler.Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Name: "Logo"},
		Route:     &spec.Route{Method: http.MethodGet, Path: "/logo"},
		Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureResource, Name: "logo", MIMEType: "image/png"},
		Contract:  contract,
	})
	if err != nil {
		t.Fatal(err)
	}
	catalog, _ := NewCatalog([]*Plan{plan})
	transport := &resourceResponse{payload: []byte{0, 1, 2}, status: http.StatusOK}
	handler := NewHandler(catalog, invocation.New(invocation.Config{Invoker: &resourceCaptureInvoker{result: transport}}))
	result, protocolErr := handler.Handle(context.Background(), &schema.ReadResourceRequest{
		Method: schema.MethodResourcesRead, Params: schema.ReadResourceRequestParams{Uri: "datly://localhost/logo"},
	})
	if protocolErr != nil {
		t.Fatal(protocolErr)
	}
	content := result.Contents[0]
	if transport.reads != 1 || content.Blob != base64.StdEncoding.EncodeToString(transport.payload) || content.Text != "" {
		t.Fatalf("content=%+v reads=%d", content, transport.reads)
	}
	assertSerializedContent(t, content, "", base64.StdEncoding.EncodeToString(transport.payload))
}

func assertSerializedContent(t *testing.T, content schema.ReadResourceResultContentsElem, wantText, wantBlob string) {
	t.Helper()
	payload, err := json.Marshal(content)
	if err != nil {
		t.Fatalf("marshal content: %v", err)
	}
	actual := struct {
		Text *string `json:"text"`
		Blob *string `json:"blob"`
	}{}
	if err := json.Unmarshal(payload, &actual); err != nil {
		t.Fatalf("unmarshal content: %v", err)
	}
	if actual.Text == nil || *actual.Text != wantText || actual.Blob == nil || *actual.Blob != wantBlob {
		t.Fatalf("serialized content = %s, want text=%q blob=%q", payload, wantText, wantBlob)
	}
}

func TestHandlerClassifiesResolutionAndExecutionErrors(t *testing.T) {
	contract := newResourceContract(t, "/status", nil)
	compiler, _ := NewCompiler("datly://localhost")
	plan, _ := compiler.Compile(Input{
		Component: spec.Key{Kind: spec.KindComponent, Name: "Status"},
		Route:     &spec.Route{Method: http.MethodGet, Path: "/status"},
		Exposure:  &spec.MCPExposure{Kind: spec.MCPExposureResource, Name: "status"}, Contract: contract,
	})
	catalog, _ := NewCatalog([]*Plan{plan})
	for _, testCase := range []struct {
		name     string
		uri      string
		err      error
		wantCode int
	}{
		{name: "not found", uri: "datly://localhost/missing", wantCode: schema.ResourceNotFound},
		{name: "invalid URI", uri: "datly://localhost/%zz", wantCode: jsonrpc.InvalidParams},
		{name: "component validation", uri: "datly://localhost/status", err: resourceStatusError{status: 422}, wantCode: jsonrpc.InvalidParams},
		{name: "component internal", uri: "datly://localhost/status", err: errors.New("password=secret"), wantCode: jsonrpc.InternalError},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			handler := NewHandler(catalog, invocation.New(invocation.Config{Invoker: &resourceCaptureInvoker{err: testCase.err}}))
			result, protocolErr := handler.Handle(context.Background(), &schema.ReadResourceRequest{
				Method: schema.MethodResourcesRead, Params: schema.ReadResourceRequestParams{Uri: testCase.uri},
			})
			if result != nil || protocolErr == nil || protocolErr.Code != testCase.wantCode {
				t.Fatalf("result=%+v error=%+v", result, protocolErr)
			}
			if testCase.wantCode == jsonrpc.InternalError && protocolErr.Message != http.StatusText(http.StatusInternalServerError) {
				t.Fatalf("internal error leaked detail: %+v", protocolErr)
			}
		})
	}
}

type resourceStatusError struct{ status int }

func (e resourceStatusError) Error() string   { return "invalid resource input" }
func (e resourceStatusError) StatusCode() int { return e.status }

type resourceResponse struct {
	payload []byte
	status  int
	reads   int
}

func (r *resourceResponse) Body() io.Reader {
	r.reads++
	return bytes.NewReader(r.payload)
}
func (r *resourceResponse) Headers() http.Header    { return nil }
func (r *resourceResponse) Size() int               { return len(r.payload) }
func (r *resourceResponse) StatusCode() int         { return r.status }
func (r *resourceResponse) SetStatusCode(value int) { r.status = value }

var _ response.Response = (*resourceResponse)(nil)
