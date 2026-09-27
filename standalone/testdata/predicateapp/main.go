package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"time"

	_ "example.com/predicateapp/internal/dependencylink"
	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
	upstream "github.com/viant/mcp"
	"github.com/viant/mcp-protocol/schema"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := sql.Open("sqlite3", os.Args[3])
	if err != nil {
		return err
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS records(id INTEGER); DELETE FROM records; INSERT INTO records VALUES(1),(2),(3)")
	db.Close()
	if err != nil {
		return err
	}
	server, err := standalone.New(ctx, standalone.Options{Config: &config.Config{
		BaseDir: os.Args[1], Endpoint: config.Endpoint{Address: "127.0.0.1:0"},
		GoBootstrap: &config.Packages{Packages: []string{"example.com/predicateapp/records"}, EagerComponents: os.Args[2] == "eager"},
		Connectors:  []connector.Config{{Name: "main", Driver: "sqlite3", DSN: os.Args[3]}},
		MCP:         &config.MCP{Address: "127.0.0.1:0"},
	}})
	if err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, nil) }()
	defer func() { cancel(); <-done }()
	addresses, err := server.WaitReady(ctx)
	if err != nil {
		return err
	}
	metadata, err := server.Metadata(ctx)
	if err != nil {
		return err
	}
	if len(metadata.Components) != 2 {
		return fmt.Errorf("unexpected components: %d", len(metadata.Components))
	}
	native, err := upstream.NewClientWithContext(ctx, nil, &upstream.ClientOptions{
		Name: "predicate-test", Version: "1", ProtocolVersion: schema.LatestProtocolVersion,
		Transport: upstream.ClientTransport{Type: "streamable", ClientTransportHTTP: upstream.ClientTransportHTTP{URL: "http://" + addresses[1] + "/mcp"}},
	})
	if err != nil {
		return err
	}
	defer native.Close()
	// MCP is the first request, exercising lazy preparation rather than merely
	// reusing the registration materialized by an earlier HTTP read.
	for _, minimum := range []int{2, 3} {
		result, err := native.CallTool(ctx, &schema.CallToolRequestParams{Name: "Records", Arguments: map[string]any{"Minimum": minimum}})
		if err != nil {
			return err
		}
		if result.IsError != nil && *result.IsError {
			return fmt.Errorf("MCP failed: %+v", result)
		}
		body, err := json.Marshal(result.StructuredContent)
		if err != nil {
			return err
		}
		if err := check(body, minimum); err != nil {
			return err
		}
		recorder := httptest.NewRecorder()
		server.ServeHTTP(recorder, httptest.NewRequest("GET", fmt.Sprintf("/records?min=%d", minimum), nil).WithContext(ctx))
		if recorder.Code != 200 {
			return fmt.Errorf("HTTP %d: %s", recorder.Code, recorder.Body.String())
		}
		if err := check(recorder.Body.Bytes(), minimum); err != nil {
			return err
		}
		cube := fmt.Sprintf(`{"dimensions":{"id":true},"measures":{"total":true},"filters":{"minimum":%d}}`, minimum)
		recorder = httptest.NewRecorder()
		request := httptest.NewRequest("POST", "/records/cube", strings.NewReader(cube)).WithContext(ctx)
		request.Header.Set("Content-Type", "application/json")
		server.ServeHTTP(recorder, request)
		if recorder.Code != 200 {
			return fmt.Errorf("cube HTTP %d: %s", recorder.Code, recorder.Body.String())
		}
		if err := check(recorder.Body.Bytes(), minimum); err != nil {
			return err
		}
	}
	fmt.Println("predicate applied through HTTP and MCP")
	return nil
}

func check(body []byte, minimum int) error {
	var output struct {
		Data []struct {
			ID int `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &output); err != nil {
		return err
	}
	if len(output.Data) != 4-minimum {
		return fmt.Errorf("predicate not applied: %s", body)
	}
	for _, row := range output.Data {
		if row.ID < minimum {
			return fmt.Errorf("predicate leaked row: %s", body)
		}
	}
	return nil
}
