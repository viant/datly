package remote

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	core "github.com/viant/datly/runtime/remote"
	"github.com/viant/mcp-protocol/schema"
	xhttp "github.com/viant/xdatly/client/http"
	xmcp "github.com/viant/xdatly/client/mcp"
)

type httpProviderFunc func(context.Context, xhttp.Options) (xhttp.Client, error)

func (f httpProviderFunc) Client(ctx context.Context, options xhttp.Options) (xhttp.Client, error) {
	return f(ctx, options)
}

type httpClientFunc func(*http.Request) (*http.Response, error)

func (f httpClientFunc) Do(request *http.Request) (*http.Response, error) { return f(request) }

type mcpProviderFunc func(context.Context, xmcp.Options) (xmcp.Client, error)

func (f mcpProviderFunc) Client(ctx context.Context, options xmcp.Options) (xmcp.Client, error) {
	return f(ctx, options)
}

type mcpClientFunc func(context.Context, *schema.CallToolRequest, xmcp.CallOptions) (*schema.CallToolResult, error)

func (f mcpClientFunc) CallTool(ctx context.Context, request *schema.CallToolRequest, options xmcp.CallOptions) (*schema.CallToolResult, error) {
	return f(ctx, request, options)
}

func TestHandlerSelectsHTTPProvider(t *testing.T) {
	input := Input{Token: "Bearer alice", Remote: core.Config{
		Client:   core.ClientOptions{Transport: core.TransportHTTP, HTTP: &xhttp.Options{URL: "https://example.test/user", Method: http.MethodGet}},
		Request:  []core.RequestMapping{{Input: "Token", Header: "Authorization"}},
		Response: core.Response{Mappings: []core.ResponseMapping{{Path: "/id", Output: "ID"}}},
	}}
	handler := &Handler[struct{ ID int }]{Mapper: core.NewMapper()}
	handler.HTTP = httpProviderFunc(func(_ context.Context, options xhttp.Options) (xhttp.Client, error) {
		if options.URL != input.Remote.Client.HTTP.URL {
			t.Fatalf("HTTP options URL = %q", options.URL)
		}
		return httpClientFunc(func(request *http.Request) (*http.Response, error) {
			if got := request.Header.Get("Authorization"); got != input.Token {
				t.Fatalf("Authorization = %q", got)
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"id":7}`))}, nil
		}), nil
	})
	var output struct{ ID int }
	if err := handler.Exec(context.Background(), nil, &input, &output); err != nil {
		t.Fatal(err)
	}
	if output.ID != 7 {
		t.Fatalf("output ID = %d", output.ID)
	}
}

func TestHandlerSelectsMCPProvider(t *testing.T) {
	input := Input{Token: "Bearer bob", Remote: core.Config{
		Client:   core.ClientOptions{Transport: core.TransportMCP, MCP: &xmcp.Options{URL: "https://example.test/mcp", Tool: "whoami"}},
		Request:  []core.RequestMapping{{Input: "Token", Header: "Authorization"}},
		Response: core.Response{Mappings: []core.ResponseMapping{{Path: "/id", Output: "ID"}}},
	}}
	handler := &Handler[struct{ ID int }]{Mapper: core.NewMapper()}
	handler.MCP = mcpProviderFunc(func(_ context.Context, options xmcp.Options) (xmcp.Client, error) {
		if options.Tool != "whoami" {
			t.Fatalf("MCP tool = %q", options.Tool)
		}
		return mcpClientFunc(func(_ context.Context, request *schema.CallToolRequest, options xmcp.CallOptions) (*schema.CallToolResult, error) {
			if request.Params.Name != "whoami" || options.Header.Get("Authorization") != input.Token {
				t.Fatalf("tool = %q, Authorization = %q", request.Params.Name, options.Header.Get("Authorization"))
			}
			return &schema.CallToolResult{StructuredContent: map[string]any{"id": 9}}, nil
		}), nil
	})
	var output struct{ ID int }
	if err := handler.Exec(context.Background(), nil, &input, &output); err != nil {
		t.Fatal(err)
	}
	if output.ID != 9 {
		t.Fatalf("output ID = %d", output.ID)
	}
}

func TestOneHandlerAndMapperServeConcurrentInputs(t *testing.T) {
	config := core.Config{
		Client:   core.ClientOptions{Transport: core.TransportHTTP, HTTP: &xhttp.Options{URL: "https://example.test/user", Method: http.MethodGet}},
		Request:  []core.RequestMapping{{Input: "Token", Header: "Authorization"}},
		Response: core.Response{Mappings: []core.ResponseMapping{{Path: "/id", Output: "ID"}}},
	}
	handler := &Handler[struct{ ID int }]{Mapper: core.NewMapper()}
	handler.HTTP = httpProviderFunc(func(context.Context, xhttp.Options) (xhttp.Client, error) {
		return httpClientFunc(func(request *http.Request) (*http.Response, error) {
			id, err := strconv.Atoi(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer "))
			if err != nil {
				return nil, err
			}
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(fmt.Sprintf(`{"id":%d}`, id)))}, nil
		}), nil
	})
	const callers = 32
	var group sync.WaitGroup
	errors := make(chan error, callers)
	for id := 1; id <= callers; id++ {
		group.Add(1)
		go func(id int) {
			defer group.Done()
			input := Input{Token: fmt.Sprintf("Bearer %d", id), Remote: config}
			var output struct{ ID int }
			if err := handler.Exec(context.Background(), nil, &input, &output); err != nil {
				errors <- err
				return
			}
			if output.ID != id {
				errors <- fmt.Errorf("caller %d got ID %d", id, output.ID)
			}
		}(id)
	}
	group.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
}
