package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"testing"

	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/logger"
	"github.com/viant/mcp-protocol/schema"
)

type preparationLoggingService struct {
	*transportTestService
	err error
}

func (s *preparationLoggingService) PrepareTools(context.Context) error        { return s.err }
func (s *preparationLoggingService) PrepareTool(context.Context, string) error { return s.err }

// Any use of the protocol logger would call a nil embedded method and fail the
// test: internal preparation errors must never become client notifications.
type protocolLogger interface{ logger.Logger }
type forbiddenProtocolLogger struct{ protocolLogger }

func TestHandlerPreparationLogging(t *testing.T) {
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	t.Cleanup(func() { log.SetOutput(previous) })

	for _, dynamic := range []bool{false, true} {
		mode := "static"
		if dynamic {
			mode = "pinned"
		}
		for _, operation := range []string{schema.MethodToolsList, schema.MethodToolsCall} {
			for _, failing := range []bool{false, true} {
				outcome := "success"
				if failing {
					outcome = "failure"
				}
				t.Run(mode+"/"+operation+"/"+outcome, func(t *testing.T) {
					logs.Reset()
					const cause = "bootstrap component source changed after indexing: /private/source.go"
					service := &preparationLoggingService{transportTestService: newTransportTestService(nil)}
					if failing {
						service.err = errors.New(cause)
					}
					var direct Service = service
					var binding *sourceBinding
					if dynamic {
						direct = nil
						binding = &sourceBinding{source: &testSource{service: service}}
					}
					factory, err := newHandler(direct, binding)
					if err != nil {
						t.Fatal(err)
					}
					handler, err := factory(context.Background(), nil, forbiddenProtocolLogger{}, nil)
					if err != nil {
						t.Fatal(err)
					}
					handler.(*Handler).ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: schema.LatestProtocolVersion}
					var protocolErr *jsonrpc.Error
					var result any
					message := "MCP tools are unavailable"
					if operation == schema.MethodToolsList {
						rows, callErr := handler.ListTools(context.Background(), &jsonrpc.TypedRequest[*schema.ListToolsRequest]{Id: 42})
						protocolErr = callErr
						if rows != nil {
							result = rows
						}
					} else {
						message = "MCP tool is unavailable"
						rows, callErr := handler.CallTool(context.Background(), &jsonrpc.TypedRequest[*schema.CallToolRequest]{
							Id: 42,
							Request: &schema.CallToolRequest{Method: operation, Params: schema.CallToolRequestParams{
								Name: "registered", Arguments: map[string]interface{}{"secret": "private-argument"},
							}},
						})
						protocolErr = callErr
						if rows != nil {
							result = rows
						}
					}
					if !failing {
						if protocolErr != nil || result == nil || logs.Len() != 0 {
							t.Fatalf("success: result=%v error=%v logs=%s", result, protocolErr, logs.String())
						}
						return
					}
					if result != nil || protocolErr == nil || protocolErr.Code != -32603 || protocolErr.Message != message {
						t.Fatalf("failure: result=%v error=%v", result, protocolErr)
					}
					encoded, err := json.Marshal(protocolErr)
					if err != nil {
						t.Fatal(err)
					}
					want, _ := json.Marshal(jsonrpc.NewInternalError(message, nil))
					if !bytes.Equal(encoded, want) {
						t.Fatalf("public error changed: %s", encoded)
					}
					for _, field := range []string{"MCP preparation failed", "operation=\"" + operation + "\"", "stage=prepare", "request_id=42", cause} {
						if !strings.Contains(logs.String(), field) {
							t.Errorf("log missing %q: %s", field, logs.String())
						}
					}
					if operation == schema.MethodToolsCall && !strings.Contains(logs.String(), "tool=\"registered\"") {
						t.Errorf("log missing tool: %s", logs.String())
					}
					if strings.Contains(logs.String(), "private-argument") || strings.Count(logs.String(), "MCP preparation failed") != 1 {
						t.Errorf("unexpected log contents: %s", logs.String())
					}
				})
			}
		}
	}
}
