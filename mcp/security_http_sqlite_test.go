package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	mcpserver "github.com/viant/datly/mcp/server"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly/response"
)

// Exercise real streamable HTTP JSON-RPC handling without requiring a socket.
func TestSecurityMCPHTTPFailuresSQLite(t *testing.T) {
	for _, mode := range []string{"panic", "unknown", "explicit", "invalid body"} {
		t.Run(mode, func(t *testing.T) {
			db := sqlite.New(t)
			ctx := context.Background()
			if err := db.ExecStatements(ctx, "CREATE TABLE records (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN, quantity INTEGER, note TEXT)", "INSERT INTO records VALUES (1,'keep',1,9,NULL)"); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "security", Name: "Patch"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/records", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "records.patch"}}}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[sparseInput](), OutputType: reflect.TypeFor[sparseOutput]()})
			if err != nil {
				t.Fatal(err)
			}
			cause := errors.New("PRIVATE DB or panic detail")
			handler := rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
				if _, err := NewRecordPatchHandler().Execute(ctx, inv); err != nil {
					return nil, err
				}
				if mode == "panic" {
					panic(cause)
				}
				if mode == "explicit" {
					return nil, &response.Error{Code: 500, Payload: map[string]any{"message": "intentional public failure", "error": nil}, Cause: cause}
				}
				return &sparseOutput{OK: true}, cause
			})
			registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[sparseOutput](), Handler: handler, DataSource: dml.Source{DB: db.DB}}
			server, err := mcpserver.New(mcpserver.Config{Service: runtimeToolService(t, registered), Transport: mcpserver.TransportConfig{Kind: mcpserver.TransportStreamable}})
			if err != nil {
				t.Fatal(err)
			}
			httpServer, err := server.HTTP()
			if err != nil {
				t.Fatal(err)
			}
			id := any(1)
			if mode == "invalid body" {
				id = "PRIVATE invalid ID"
			}
			data, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": schema.MethodToolsCall, "params": map[string]any{"name": "records.patch", "arguments": map[string]any{"patch": map[string]any{"id": id, "name": "changed"}}, "_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": schema.LatestProtocolVersion, "io.modelcontextprotocol/clientCapabilities": map[string]any{}}}})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest("POST", "/mcp", bytes.NewReader(data))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json, text/event-stream")
			req.Header.Set(schema.HeaderProtocolVersion, schema.LatestProtocolVersion)
			req.Header.Set(schema.HeaderMethod, schema.MethodToolsCall)
			req.Header.Set("Mcp-Name", "records.patch")
			res := httptest.NewRecorder()
			httpServer.Handler.ServeHTTP(res, req)
			body := res.Body.String()
			if res.Code != 200 || strings.Contains(body, "PRIVATE") || !strings.Contains(body, `"isError":true`) {
				t.Fatalf("%d %s", res.Code, body)
			}
			if mode == "explicit" {
				if !strings.Contains(body, "intentional public failure") {
					t.Fatal(body)
				}
			} else if mode != "invalid body" && !strings.Contains(body, "Internal Server Error") {
				t.Fatal(body)
			}
			if mode == "invalid body" && !strings.Contains(body, "Bad Request") {
				t.Fatal(body)
			}
			db.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id,name,active,quantity,note FROM records"}, []sparseRecord{{ID: 1, Name: "keep", Active: true, Quantity: 9}})
		})
	}
}
