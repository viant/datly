package developer_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/devapp"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp/developer"
	"github.com/viant/mcp-protocol/schema"
)

const developerWorkflowHTTPTimeout = 30 * time.Second

func TestNativeDeveloperFullWorkflow(t *testing.T) {
	f := devapp.New(t)
	cfg, err := devapp.Configuration(f.Root, f.DSN)
	if err != nil {
		t.Fatal(err)
	}
	s, err := developer.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	native := mcpclient.New(t, s, schema.LatestProtocolVersion)
	// The complete workflow includes generated Go validation and discovery.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	listing, err := native.ListResources(ctx, nil)
	if err != nil || len(listing.Resources) < 3 {
		t.Fatal(err)
	}
	if _, err = native.ReadResource(ctx, &schema.ReadResourceRequestParams{Uri: "skill://datly-reader/SKILL.md"}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ target, source string }{{"reader", devapp.ReadDQL}, {"writer", devapp.WriteDQL}} {
		result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: developer.TranscribeTool, Arguments: map[string]any{"target": tc.target, "source": tc.source}})
		if err != nil || *result.IsError {
			t.Fatalf("transcribe %+v %v", result, err)
		}
	}
	validated, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: developer.ValidationTool, Arguments: map[string]any{"target": "app"}})
	if err != nil || *validated.IsError {
		t.Fatalf("validate %+v %v", validated, err)
	}
	started, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: developer.RunTool, Arguments: map[string]any{"target": "app", "port": 0, "address": "127.0.0.1"}})
	if err != nil || *started.IsError {
		t.Fatalf("run %+v %v", started, err)
	}
	data, _ := json.Marshal(started.StructuredContent)
	var instance developer.Instance
	if err = json.Unmarshal(data, &instance); err != nil || instance.ID == "" {
		t.Fatal(err)
	}
	// WaitReady proves the listener is bound, while the first request can still
	// perform cold application work (notably under -race and full-suite load).
	// Keep every probe bounded without imposing a scheduler-sensitive 3s cap.
	client := &http.Client{Timeout: developerWorkflowHTTPTimeout}
	requestCtx, requestCancel := context.WithTimeout(ctx, developerWorkflowHTTPTimeout)
	res, err := client.Do(mustRequest(t, requestCtx, http.MethodGet, instance.Address+"/records/1", nil))
	if err != nil {
		requestCancel()
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	requestCancel()
	if res.StatusCode != 200 || !strings.Contains(string(body), "first") {
		t.Fatalf("read %d %s", res.StatusCode, body)
	}
	requestCtx, requestCancel = context.WithTimeout(ctx, developerWorkflowHTTPTimeout)
	request := mustRequest(t, requestCtx, http.MethodPost, instance.Address+"/records", strings.NewReader(`{"data":{"id":2,"name":"written"}}`))
	request.Header.Set("Content-Type", "application/json")
	res, err = client.Do(request)
	if err != nil {
		requestCancel()
		t.Fatal(err)
	}
	body, _ = io.ReadAll(res.Body)
	res.Body.Close()
	requestCancel()
	if res.StatusCode != 200 {
		t.Fatalf("write %d %s", res.StatusCode, body)
	}
	requestCtx, requestCancel = context.WithTimeout(ctx, developerWorkflowHTTPTimeout)
	res, err = client.Do(mustRequest(t, requestCtx, http.MethodGet, instance.Address+"/records/2", nil))
	if err != nil {
		requestCancel()
		t.Fatal(err)
	}
	body, _ = io.ReadAll(res.Body)
	res.Body.Close()
	requestCancel()
	if res.StatusCode != 200 || !strings.Contains(string(body), "written") {
		t.Fatalf("mutation roundtrip %d %s", res.StatusCode, body)
	}
	listed, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: developer.ComponentsTool, Arguments: map[string]any{"target": "app", "instanceId": instance.ID}})
	if err != nil || *listed.IsError {
		t.Fatalf("running metadata %+v %v", listed, err)
	}
	for i := 0; i < 2; i++ {
		stopped, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: developer.StopTool, Arguments: map[string]any{"instanceId": instance.ID}})
		if err != nil || *stopped.IsError {
			t.Fatalf("stop %+v %v", stopped, err)
		}
	}
	connection, err := net.DialTimeout("tcp", strings.TrimPrefix(instance.Address, "http://"), time.Second)
	if err == nil {
		connection.Close()
		t.Fatal("owned listener remained open")
	}
	if _, err = native.CallTool(ctx, &schema.CallToolRequestParams{Name: developer.StopTool, Arguments: map[string]any{"pid": 1}}); err == nil {
		t.Fatal("arbitrary process control accepted")
	}
}

func mustRequest(t *testing.T, ctx context.Context, method, uri string, body io.Reader) *http.Request {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, method, uri, body)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func TestNativeDeveloperPortCollisionAndCanceledRun(t *testing.T) {
	f := devapp.New(t)
	cfg, err := devapp.Configuration(f.Root, f.DSN)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	_, raw, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(raw)
	app := cfg.Applications["app"]
	app.Ports = []int{port}
	cfg.Applications["app"] = app
	s, err := developer.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Shutdown(context.Background())
	entry, _ := s.Registry().ToolRegistry.Get(developer.RunTool)
	request := &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: developer.RunTool, Arguments: map[string]any{"target": "app", "port": port}}}
	result, e := entry.Handler(context.Background(), request)
	if e == nil && !*result.IsError {
		t.Fatal("occupied port was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, e = entry.Handler(ctx, request)
	if e == nil && !*result.IsError {
		t.Fatal("canceled run was accepted")
	}
	if err = s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	connection, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal("unowned listener was stopped")
	}
	connection.Close()
}
