package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"time"

	_ "example.com/codecapp/internal/dependencylink"
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
	_, err = db.Exec("CREATE TABLE records(id INTEGER, label TEXT); INSERT INTO records VALUES(1,'child')")
	db.Close()
	if err != nil {
		return err
	}
	server, err := standalone.New(ctx, standalone.Options{Config: &config.Config{
		BaseDir: os.Args[1], Endpoint: config.Endpoint{Address: "127.0.0.1:0"},
		GoBootstrap: &config.Packages{Packages: []string{"example.com/codecapp/consumer"}, EagerComponents: os.Args[2] == "eager"},
		Connectors:  []connector.Config{{Name: "main", Driver: "sqlite3", DSN: os.Args[3]}}, MCP: &config.MCP{Address: "127.0.0.1:0"},
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
		return fmt.Errorf("unexpected component count: %d", len(metadata.Components))
	}
	client, err := upstream.NewClientWithContext(ctx, nil, &upstream.ClientOptions{Name: "codec-test", Version: "1", ProtocolVersion: schema.LatestProtocolVersion,
		Transport: upstream.ClientTransport{Type: "streamable", ClientTransportHTTP: upstream.ClientTransportHTTP{URL: "http://" + addresses[1] + "/mcp"}}})
	if err != nil {
		return err
	}
	defer client.Close()
	for _, tc := range []struct {
		tool, path, want string
		args             map[string]any
	}{
		{"CodecProbe", "/codec-probe?value=1,2&value=3", `{"values":[1,2,3]}`, map[string]any{"Values": []string{"1,2", "3"}}},
		{"CodecRows", "/codec-rows?suffix=!", `{"data":[{"id":1,"child":{"id":1,"label":"CHILD!"}}],"meta":{"label":"SUMMARY"}}`, map[string]any{"Suffix": "!"}},
	} {
		result, err := client.CallTool(ctx, &schema.CallToolRequestParams{Name: tc.tool, Arguments: tc.args})
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
		if err := check(body, tc.want); err != nil {
			return err
		}
		recorder := httptest.NewRecorder()
		server.ServeHTTP(recorder, httptest.NewRequest("GET", tc.path, nil).WithContext(ctx))
		if recorder.Code != 200 {
			return fmt.Errorf("HTTP %d: %s", recorder.Code, recorder.Body.String())
		}
		if err := check(recorder.Body.Bytes(), tc.want); err != nil {
			return err
		}
	}
	fmt.Println("parameter, child and summary codecs applied through HTTP and MCP")
	return nil
}

func check(body []byte, want string) error {
	var actualValue, expectedValue any
	if err := json.Unmarshal(body, &actualValue); err != nil {
		return err
	}
	if err := json.Unmarshal([]byte(want), &expectedValue); err != nil {
		return err
	}
	actual, _ := json.Marshal(actualValue)
	expected, _ := json.Marshal(expectedValue)
	if string(actual) != string(expected) {
		return fmt.Errorf("codec output: got %s, want %s", actual, expected)
	}
	return nil
}
