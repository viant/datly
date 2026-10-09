package command_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/cmd/command"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/mcp/tool"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/handler/custom"
	fixture "github.com/viant/datly/standalone/testdata/app"
	"github.com/viant/datly/standalone/testdata/app/handlerreport"
	"github.com/viant/datly/standalone/testdata/app/records"
	upstream "github.com/viant/mcp"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/xdatly"
)

// This explicit test-owned linked route reuses the real SQLite fixture writer.
// It adds MCP exposure for mutation-suppression evidence without altering the
// original fixture or generated source.
type commandArtifactWriter struct {
	Write xdatly.Component[records.WriteInput, records.WriteOutput] `component:"PinnedWrite,path=/pinned-records,method=POST,connector=main,handler=NewPinnedWrite" mcp:"[{\"kind\":\"tool\",\"name\":\"PinnedWrite\"}]"`
}

func (commandArtifactWriter) DatlyHandler(name string) func() (rhandler.TypedHandler, error) {
	if name == "NewPinnedWrite" {
		return custom.Factory(records.NewWrite)
	}
	return nil
}

// The fixture owner supplies explicit revision and provenance covering the
// actual running executable and its shipped resources, never a build label or
// generation counter. Production hosts supply their own approved manifest.
func commandFixtureArtifact(t *testing.T) *exec.LinkedArtifact {
	t.Helper()
	h := sha256.New()
	executable, err := os.Open(os.Args[0])
	require.NoError(t, err)
	_, err = io.Copy(h, executable)
	require.NoError(t, err)
	require.NoError(t, executable.Close())
	require.NoError(t, fs.WalkDir(fixture.Files, ".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		data, err := fs.ReadFile(fixture.Files, path)
		if err != nil {
			return err
		}
		fmt.Fprintf(h, "\n%s:%d\n", path, len(data))
		_, err = h.Write(data)
		return err
	}))
	return &exec.LinkedArtifact{Revision: "fixture-native-release", ContentFingerprint: hex.EncodeToString(h.Sum(nil))}
}

func TestCommandLinkedArtifactBindingRunStartLazyEager(t *testing.T) {
	for _, verb := range []string{"run", "start"} {
		for _, eager := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/eager=%v", verb, eager), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				f := fixture.New(t)
				require.NoError(t, f.DB.ExecStatements(ctx, "CREATE TABLE report_facts(country TEXT,region TEXT,site_id INTEGER,amount INTEGER,enabled INTEGER)", "INSERT INTO report_facts VALUES('US','MA',1,7,1)", "CREATE TABLE report_regions(country TEXT,region TEXT,label TEXT)", "CREATE TABLE report_sites(id INTEGER,label TEXT)"))
				f.WriteConfig(t, func(c map[string]any) {
					c["MCP"] = map[string]any{"Address": "127.0.0.1:0"}
					c["GoBootstrap"] = map[string]any{"Packages": []string{fixture.Module + "/handlerreport", reflect.TypeFor[commandArtifactWriter]().PkgPath()}, "EagerComponents": eager, "LinkedOnly": !eager}
				})
				exports, err := handlerreport.Exports()
				require.NoError(t, err)
				artifact := commandFixtureArtifact(t)
				var output, diagnostics testharness.Output
				finished := make(chan struct{})
				status := make(chan int, 1)
				go func() {
					defer close(finished)
					status <- (command.Service{Registry: exports, Holders: []any{commandArtifactWriter{}}, LinkedArtifact: artifact, RequireComponentBinding: true}).Run(ctx, []string{verb, "-conf", f.Config}, &output, &diagnostics)
				}()
				t.Cleanup(func() {
					cancel()
					select {
					case <-finished:
					case <-time.After(5 * time.Second):
						t.Error("command did not stop")
					}
				})
				address, err := output.WaitLine(ctx, "HTTP listening on ")
				require.NoError(t, err, diagnostics.String())
				mcpAddress, err := output.WaitLine(ctx, "MCP listening on ")
				require.NoError(t, err, diagnostics.String())
				res, err := http.Get("http://" + address + "/native-report?permit=true")
				require.NoError(t, err)
				body, err := io.ReadAll(res.Body)
				res.Body.Close()
				require.NoError(t, err)
				require.Equal(t, 200, res.StatusCode, string(body))
				require.Contains(t, string(body), `"amount":7`)
				native, err := upstream.NewClientWithContext(ctx, nil, &upstream.ClientOptions{Name: "command-artifact-test", Version: "1", ProtocolVersion: "2026-07-28", Transport: upstream.ClientTransport{Type: "streamable", ClientTransportHTTP: upstream.ClientTransportHTTP{URL: "http://" + mcpAddress + "/mcp"}}})
				require.NoError(t, err)
				defer native.Close()
				listing, err := native.ListTools(ctx, nil)
				require.NoError(t, err)
				var pin, writerPin exec.ComponentBinding
				for _, entry := range listing.Tools {
					if entry.Name == "PinnedWrite" {
						raw, err := json.Marshal(entry.Meta[tool.ComponentBindingMetaKey])
						require.NoError(t, err)
						require.NoError(t, json.Unmarshal(raw, &writerPin))
					}
					if entry.Name == "NativeReportCube" {
						raw, err := json.Marshal(entry.Meta[tool.ComponentBindingMetaKey])
						require.NoError(t, err)
						require.NoError(t, json.Unmarshal(raw, &pin))
					}
				}
				require.NoError(t, writerPin.Validate())
				require.NoError(t, pin.Validate())
				require.Equal(t, artifact.Revision, pin.Revision)
				require.Equal(t, artifact.ContentFingerprint, pin.ContentFingerprint)
				// Constructor copies the caller's manifest before publishing tool metadata.
				artifact.Revision = "caller-mutated"
				args := map[string]interface{}{"dimensions": map[string]interface{}{"country": true}, "measures": map[string]interface{}{"amount": true}, "filters": map[string]interface{}{"permit": true}}
				invoke := func(binding *exec.ComponentBinding) (*schema.CallToolResult, error) {
					params := &schema.CallToolRequestParams{Name: "NativeReportCube", Arguments: args}
					if binding != nil {
						params.Meta = schema.RequestMetaObject{AdditionalProperties: map[string]interface{}{tool.ComponentBindingMetaKey: *binding}}
					}
					return native.CallTool(ctx, params)
				}
				result, err := invoke(&pin)
				require.NoError(t, err)
				require.False(t, result.IsError != nil && *result.IsError)
				raw, err := json.Marshal(result.StructuredContent)
				require.NoError(t, err)
				require.Contains(t, string(raw), `"amount":7`)
				_, err = invoke(nil)
				require.ErrorContains(t, err, "exact component binding")
				for _, field := range []string{"revision", "content", "schema", "id"} {
					wrong := pin
					switch field {
					case "revision":
						wrong.Revision = "other-release"
					case "content":
						wrong.ContentFingerprint = strings.Repeat("b", 64)
					case "schema":
						wrong.SchemaFingerprint = strings.Repeat("c", 64)
					case "id":
						wrong.ID = "other-component"
					}
					_, err = invoke(&wrong)
					require.ErrorContains(t, err, "exact component binding", field)
				}
				// Rejected exact bindings never change the fixture and cannot execute using
				// a stale or unbound contract. The approved pin remains usable afterward.
				result, err = invoke(&pin)
				require.NoError(t, err)
				raw, err = json.Marshal(result.StructuredContent)
				require.NoError(t, err)
				require.Contains(t, string(raw), `"amount":7`)
				writeParams := &schema.CallToolRequestParams{Name: "PinnedWrite", Arguments: map[string]interface{}{"Record": map[string]interface{}{"id": 2, "name": "approved"}}}
				_, err = native.CallTool(ctx, writeParams)
				require.ErrorContains(t, err, "exact component binding")
				wrongWriter := writerPin
				wrongWriter.ContentFingerprint = strings.Repeat("d", 64)
				writeParams.Meta = schema.RequestMetaObject{AdditionalProperties: map[string]interface{}{tool.ComponentBindingMetaKey: wrongWriter}}
				_, err = native.CallTool(ctx, writeParams)
				require.ErrorContains(t, err, "exact component binding")
				var count int
				require.NoError(t, f.DB.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM records WHERE id=2").Scan(&count))
				require.Zero(t, count, "unbound or mismatched native write changed SQLite")
				writeParams.Meta = schema.RequestMetaObject{AdditionalProperties: map[string]interface{}{tool.ComponentBindingMetaKey: writerPin}}
				writeResult, err := native.CallTool(ctx, writeParams)
				require.NoError(t, err)
				require.False(t, writeResult.IsError != nil && *writeResult.IsError)
				var name string
				require.NoError(t, f.DB.DB.QueryRowContext(ctx, "SELECT name FROM records WHERE id=2").Scan(&name))
				require.Equal(t, "approved", name)
				t.Logf("native HTTP and MCP SQL amount=7; exact pin %s %s; unbound and four mismatches denied; native write denied without effect then approved pin inserted SQLite record", pin.Revision, pin.ContentFingerprint)
				native.Close()
				cancel()
				<-finished
				require.Equal(t, 0, <-status, diagnostics.String())
			})
		}
	}
}
