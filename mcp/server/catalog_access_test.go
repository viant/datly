package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/viant/authz"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/authorization"
	"github.com/viant/mcp-protocol/schema"
	protocol "github.com/viant/mcp-protocol/server"
)

type guardedCatalogService struct {
	handlerService
	denial error
}

func (s *guardedCatalogService) AuthorizeCatalogTool(_ context.Context, name, action string) error {
	if name == "private" && action == "describe" {
		if s.denial != nil {
			return s.denial
		}
		return fmt.Errorf("private metadata: %w", authz.ErrDenied)
	}
	return nil
}

func TestListToolsOmitsJoinedDenials(t *testing.T) {
	registry := protocol.NewRegistry()
	registry.RegisterTool(&protocol.ToolEntry{Metadata: schema.Tool{Name: "private"}})
	service := &guardedCatalogService{handlerService: handlerService{registry: registry}, denial: errors.Join(authz.ErrDenied, fmt.Errorf("identity: %w", authz.ErrIdentityDenied))}
	factory, err := NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := factory(context.Background(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.(*Handler).ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: "2025-11-25"}
	result, rpcErr := handler.ListTools(context.Background(), nil)
	if rpcErr != nil || result == nil || len(result.Tools) != 0 {
		t.Fatalf("result=%+v error=%v", result, rpcErr)
	}
}

func TestCatalogCallbackDiagnosticsUseSafeLogging(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	registry := protocol.NewRegistry()
	registry.RegisterTool(&protocol.ToolEntry{Metadata: schema.Tool{Name: "protected"}})
	failure := fmt.Errorf("scope lookup failed Authorization: Bearer bearer-secret password='password-secret'\r\nforged-record body={\"private\":\"body-secret\"} args=[arg-secret]")
	service := &failingCatalogService{handlerService: handlerService{registry: registry}, failure: failure, phase: "discover"}
	factory, err := NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := factory(context.Background(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.(*Handler).ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: "2025-11-25"}
	_, rpcErr := handler.ListTools(context.Background(), nil)
	if !errors.Is(rpcErr, failure) {
		t.Fatalf("lost original cause: %v", rpcErr)
	}
	got := output.String()
	for _, secret := range []string{"bearer-secret", "password-secret", "body-secret", "arg-secret"} {
		if strings.Contains(got, secret) {
			t.Fatalf("callback diagnostics exposed %s: %s", secret, got)
		}
	}
	if !strings.Contains(got, "scope lookup failed") || !strings.Contains(got, "request_id=0") || !strings.Contains(got, "elapsed=") || strings.Count(got, "\n") != 1 || strings.Contains(got, "\r") {
		t.Fatalf("unsafe or uncorrelated callback diagnostics: %q", got)
	}
}

type failingCatalogService struct {
	handlerService
	failure error
	phase   string
}

func (s *failingCatalogService) Authorization() *authorization.Policy { return nil }

func (s *failingCatalogService) AuthorizeCatalogTool(_ context.Context, name, action string) error {
	if name == "protected" && action == s.phase {
		return s.failure
	}
	return nil
}

// This transport regression proves a dependency outage is a protocol error,
// rather than a successful empty catalog. The callback owns identity/grants.
func TestCatalogFailureThroughStreamableHTTP(t *testing.T) {
	for _, failure := range []error{context.Canceled, authz.ErrUnavailable, fmt.Errorf("PRIVATE dependency diagnostic"), errors.Join(authz.ErrDenied, fmt.Errorf("PRIVATE joined dependency"))} {
		t.Run(failure.Error(), func(t *testing.T) {
			registry := protocol.NewRegistry()
			registry.RegisterTool(&protocol.ToolEntry{Metadata: schema.Tool{Name: "protected"}})
			service := &failingCatalogService{handlerService: handlerService{registry: registry}, failure: failure, phase: "discover"}
			server, err := New(Config{Service: service, Transport: TransportConfig{Kind: TransportStreamable}})
			if err != nil {
				t.Fatal(err)
			}
			httpServer, err := server.HTTP()
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 17, "method": schema.MethodToolsList, "params": map[string]any{"_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": schema.LatestProtocolVersion, "io.modelcontextprotocol/clientCapabilities": map[string]any{}}}})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest("POST", "/mcp", bytes.NewReader(data))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Accept", "application/json, text/event-stream")
			request.Header.Set(schema.HeaderProtocolVersion, schema.LatestProtocolVersion)
			request.Header.Set(schema.HeaderMethod, schema.MethodToolsList)
			response := httptest.NewRecorder()
			httpServer.Handler.ServeHTTP(response, request)
			body := response.Body.String()
			if response.Code != 200 || !strings.Contains(body, `"code":-32603`) || !strings.Contains(body, "MCP tool catalog is unavailable") || strings.Contains(body, "PRIVATE") || strings.Contains(body, `"tools"`) {
				t.Fatalf("HTTP %d: %s", response.Code, body)
			}
		})
	}
}

// Fixed causal cases exercise the protocol boundary and the private local cause.
func TestListToolsDoesNotConvertDependencyFailuresIntoSuccessfulOmission(t *testing.T) {
	for _, phase := range []string{"discover", "describe"} {
		for _, failure := range []error{context.Canceled, context.DeadlineExceeded, authz.ErrUnavailable, fmt.Errorf("private dependency diagnostic"), errors.Join(authz.ErrDenied, context.Canceled), errors.Join(authz.ErrDenied, authz.ErrUnavailable), errors.Join(authz.ErrDenied, fmt.Errorf("PRIVATE unexpected dependency")), errors.Join(fmt.Errorf("owner: %w", errors.Join(authz.ErrDenied, fmt.Errorf("PRIVATE nested dependency"))), authz.ErrIdentityDenied)} {
			t.Run(phase+"/"+failure.Error(), func(t *testing.T) {
				registry := protocol.NewRegistry()
				for _, name := range []string{"public", "protected"} {
					registry.RegisterTool(&protocol.ToolEntry{Metadata: schema.Tool{Name: name}})
				}
				service := &failingCatalogService{handlerService: handlerService{registry: registry}, failure: fmt.Errorf("catalog scope lookup: %w", failure), phase: phase}
				factory, err := NewHandler(service)
				if err != nil {
					t.Fatal(err)
				}
				handler, err := factory(context.Background(), nil, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				handler.(*Handler).ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: "2025-11-25"}
				result, rpcErr := handler.ListTools(context.Background(), nil)
				if result != nil || rpcErr == nil || rpcErr.Code != jsonrpc.InternalError || !errors.Is(rpcErr, failure) {
					t.Fatalf("result=%+v error=%v", result, rpcErr)
				}
				if rpcErr.Message != "MCP tool catalog is unavailable" || len(rpcErr.Data) != 0 {
					t.Fatalf("unsafe protocol diagnostic: %+v", rpcErr)
				}
				if len(registry.ListRegisteredTools()) != 2 {
					t.Fatal("authorization mutated published registry")
				}
			})
		}
	}
}
func TestListToolsRequiresDiscoveryAndDescriptionWithoutMutatingGeneration(t *testing.T) {
	registry := protocol.NewRegistry()
	for _, name := range []string{"public", "private"} {
		registry.RegisterTool(&protocol.ToolEntry{Metadata: schema.Tool{Name: name}})
	}
	service := &guardedCatalogService{handlerService: handlerService{registry: registry}}
	factory, err := NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := factory(context.Background(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler.(*Handler).ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: "2025-11-25"}
	list, e := handler.ListTools(context.Background(), nil)
	if e != nil || len(list.Tools) != 1 || list.Tools[0].Name != "public" {
		t.Fatalf("list=%+v err=%v", list, e)
	}
	if len(registry.ListRegisteredTools()) != 2 {
		t.Fatal("request filtering mutated the published registry")
	}
}
