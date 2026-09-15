package developer_test

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/devapp"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/mcp/developer"
	dserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

const generationModule = "github.com/viant/datly/mcpgen"

func TestDeveloperHighLevelGenerationNativeMCP(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: generationModule}).Write(t, root)
	service, err := developer.New(developer.Config{
		Targets: map[string]transcribe.Validator{
			"get":           generationTarget(root, db),
			"patch":         generationTarget(root, db),
			"validate-only": {BaseDir: root, Include: []string{generationModule + "/api/orders/patch"}},
		},
		Authoring: map[string]transcribe.Request{
			"get":   generationRequest(root, "get"),
			"patch": generationRequest(root, "patch"),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Shutdown(ctx)
	handler := nativeDeveloperHandler(t, service)
	listing, rpcErr := handler.ListTools(ctx, &jsonrpc.TypedRequest[*schema.ListToolsRequest]{Request: &schema.ListToolsRequest{}})
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	tool := transcribeToolMetadata(t, listing.Tools)
	targetDescription := tool.InputSchema.Properties["target"]["description"].(string)
	for _, want := range []string{"get=generation:go:get", "patch=generation:go:patch", "validate-only=disabled"} {
		if !strings.Contains(targetDescription, want) {
			t.Fatalf("target capabilities missing %q: %s", want, targetDescription)
		}
	}
	metadata, _ := json.Marshal(tool.Meta["datly.authoringTargets"])
	if !strings.Contains(string(metadata), `"operation":"patch"`) || !strings.Contains(string(metadata), `"enabled":false`) {
		t.Fatalf("missing authoring metadata: %s", metadata)
	}
	getResult := callNativeTranscribe(t, handler, "get", namedGenerationDQL("GET", "api/orders/get"))
	if getResult.Mode != "generation" || getResult.Operation != "get" || getResult.Language != "go" {
		t.Fatalf("GET response did not report generation mode: %+v", getResult)
	}
	patchResult := callNativeTranscribe(t, handler, "patch", namedGenerationDQL("PATCH", "api/orders/patch"))
	if patchResult.Mode != "generation" || patchResult.Operation != "patch" || patchResult.Language != "go" {
		t.Fatalf("PATCH response did not report generation mode: %+v", patchResult)
	}
	patchDir := filepath.Join(root, "api/orders/patch")
	hooks := readFile(t, filepath.Join(patchDir, "lifecycle.go"))
	for _, want := range []string{"type OrdersViewLifecycle struct", "func (hooks *OrdersViewLifecycle) Init", "return nil"} {
		if !strings.Contains(hooks, want) {
			t.Fatalf("generated lifecycle missing %q:\n%s", want, hooks)
		}
	}
	generated := readGeneratedPackage(t, patchDir)
	for _, want := range []string{"type OrdersView struct", `invariant:"Interval"`, "HasIntervalChanges"} {
		if !strings.Contains(generated, want) {
			t.Fatalf("generated patch did not retain %q", want)
		}
	}
	compileGeneratedProject(t, root)
}

func TestDeveloperHighLevelGenerationDiscoversAndPreservesAuthoredHooks(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: generationModule}).Write(t, root)
	for directory, code := range map[string]string{"hooks/root": genpatch.RootHooks, "hooks/child": genpatch.ChildHooks} {
		writeSourceFile(t, root, filepath.Join(directory, "hooks.go"), strings.ReplaceAll(code, "github.com/viant/datly/genfixture", generationModule))
	}
	service, err := developer.New(developer.Config{
		Targets:   map[string]transcribe.Validator{"patch": generationTarget(root, db)},
		Authoring: map[string]transcribe.Request{"patch": generationRequest(root, "patch")},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Shutdown(ctx)
	handler := nativeDeveloperHandler(t, service)
	source := genpatch.DestinationDQL(generationModule)
	callNativeTranscribe(t, handler, "patch", source)
	for directory, code := range map[string]string{"hooks/root": genpatch.RootHooks, "hooks/child": genpatch.ChildHooks} {
		edited := strings.Replace(strings.ReplaceAll(code, "github.com/viant/datly/genfixture", generationModule), "const Version=1", "const Version=2", 1)
		writeSourceFile(t, root, filepath.Join(directory, "hooks.go"), edited+"\n// authored edit\n")
	}
	callNativeTranscribe(t, handler, "patch", source)
	for _, directory := range []string{"hooks/root", "hooks/child"} {
		if content := readFile(t, filepath.Join(root, directory, "hooks.go")); !strings.Contains(content, "const Version=2") {
			t.Fatalf("authored hook edit lost in %s:\n%s", directory, content)
		}
	}
	runtime := strings.ReplaceAll(genpatch.RuntimeSource, "github.com/viant/datly/genfixture", generationModule)
	start := strings.Index(runtime, "var lookupRead bool")
	end := strings.Index(runtime, "func TestGeneratedPatchRuntime")
	runtime = runtime[:start] + runtime[end:]
	runtime = strings.Replace(runtime, ` "fmt"`+"\n", "", 1)
	runtime = strings.Replace(runtime, ` "context"`+"\n", ` "context"`+"\n rows \""+generationModule+`/entities"`+"\n rh \""+generationModule+`/hooks/root"`+"\n ch \""+generationModule+`/hooks/child"`+"\n", 1)
	runtime = strings.NewReplacer("OrdersViewHas", "rows.OrderHas", "OrdersView", "rows.Order", "hookObservedOriginal", "rh.HookObservedOriginal", "lookupRead", "rh.LookupRead").Replace(runtime)
	runtime = strings.Replace(runtime, ` if !rh.LookupRead`, ` if rh.Version!=2||ch.Version!=2||rh.Calls==0||ch.Calls==0||rh.Completions==0{t.Fatal("authored hooks were not linked")}
 if !rh.LookupRead`, 1)
	genpatch.Run(t, root, filepath.Join(root, "api/orders"), runtime)
}

func TestDeveloperHighLevelGenerationFailuresAndCompatibility(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: generationModule}).Write(t, root)
	if _, err := developer.New(developer.Config{
		Targets:   map[string]transcribe.Validator{"bad": generationTarget(root, db)},
		Authoring: map[string]transcribe.Request{"bad": {Destination: root, Source: &transcribe.Source{Name: "Orders"}, Generation: transcribe.GenerationOptions{Operation: "delete"}}},
	}); err == nil {
		t.Fatal("unknown configured generation operation accepted")
	}
	service, err := developer.New(developer.Config{
		Targets: map[string]transcribe.Validator{
			"patch":    generationTarget(root, db),
			"disabled": {BaseDir: root, Include: []string{generationModule + "/api/orders"}},
		},
		Authoring: map[string]transcribe.Request{"patch": generationRequest(root, "patch")},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Shutdown(ctx)
	handler := nativeDeveloperHandler(t, service)
	for _, test := range []struct {
		name   string
		target string
		source string
		want   string
	}{
		{name: "missing package", target: "patch", source: strings.TrimPrefix(genpatch.DQL, genpatch.PackageDirective+"\n"), want: "explicit #package"},
		{name: "outside package", target: "patch", source: strings.Replace(genpatch.DQL, genpatch.PackageDirective, "#package('../escape')", 1), want: "escapes"},
		{name: "disabled", target: "disabled", source: genpatch.DQL, want: "not configured"},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := callNativeTool(t, handler, developer.TranscribeTool, map[string]any{"target": test.target, "source": test.source})
			if result.IsError == nil || !*result.IsError || !strings.Contains(result.Content[0].(schema.TextContent).Text, test.want) {
				t.Fatalf("expected %q failure, got %+v", test.want, result)
			}
		})
	}
	result, rpcErr := handler.CallTool(ctx, &jsonrpc.TypedRequest[*schema.CallToolRequest]{Request: &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: developer.TranscribeTool, Arguments: map[string]any{"target": "patch", "source": genpatch.DQL, "operation": "get"}}}})
	if rpcErr == nil && (result == nil || result.IsError == nil || !*result.IsError) {
		t.Fatal("client generation option injection accepted")
	}
	writeSourceFile(t, root, filepath.Join("badhooks", "bad.go"), "package badhooks\nfunc")
	malformed := "#package('api/orders')\n#import('bad','" + generationModule + "/badhooks')\n" + strings.TrimPrefix(genpatch.DQL, genpatch.PackageDirective+"\n")
	result = callNativeTool(t, handler, developer.TranscribeTool, map[string]any{"target": "patch", "source": malformed})
	if result.IsError == nil || !*result.IsError || !strings.Contains(result.Content[0].(schema.TextContent).Text, "bad.go") {
		t.Fatalf("malformed imported package boundary lost: %+v", result)
	}
	f := devapp.New(t)
	cfg, err := devapp.Configuration(f.Root, f.DSN)
	if err != nil {
		t.Fatal(err)
	}
	lowLevel, err := developer.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer lowLevel.Shutdown(ctx)
	low := callNativeTranscribe(t, nativeDeveloperHandler(t, lowLevel), "reader", devapp.ReadDQL)
	if low.Mode != "transcribe" || low.Operation != "" || low.Language != "" {
		t.Fatalf("lower-level authoring behavior changed: %+v", low)
	}
}

func generationTarget(root string, db *testharness.Harness) transcribe.Validator {
	return transcribe.Validator{BaseDir: root, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}
}

func generationRequest(root, operation string) transcribe.Request {
	return transcribe.Request{
		Destination: root,
		Source:      &transcribe.Source{Name: "Orders", Scope: generationModule + "/source/" + operation, Connector: "main"},
		Generation:  transcribe.GenerationOptions{Operation: operation},
	}
}

func namedGenerationDQL(method, pkg string) string {
	lifecycle := ""
	if method == "PATCH" {
		lifecycle = "lifecycle_type(orders, 'OrdersViewLifecycle'),\n"
	}
	return "#package('" + pkg + "')\n#setting($_ = $route('/orders','" + method + "'))\n" +
		"SELECT orders.*, Items.*, Kinds.*,\n" + lifecycle + `
 tag(orders.START,'invariant:"Interval"'),
 tag(orders.END,'invariant:"Interval" validate:"gtfield(Start)"')
FROM (SELECT o.* FROM ORDERS o) orders
JOIN (SELECT i.* FROM ITEMS i) Items ON Items.ORDER_ID = orders.ID
JOIN (SELECT k.* FROM (ORDER_KINDS) k) Kinds ON Kinds.ID = orders.KIND_ID`
}

func nativeDeveloperHandler(t *testing.T, service *developer.Service) *dserver.Handler {
	t.Helper()
	factory, err := dserver.NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := factory(context.Background(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	handler := raw.(*dserver.Handler)
	handler.ClientInitialize = &schema.InitializeRequestParams{ProtocolVersion: schema.LatestProtocolVersion}
	return handler
}

func callNativeTranscribe(t *testing.T, handler *dserver.Handler, target, source string) developer.Transcription {
	t.Helper()
	result := callNativeTool(t, handler, developer.TranscribeTool, map[string]any{"target": target, "source": source})
	if result.IsError != nil && *result.IsError {
		t.Fatalf("transcribe %s: %+v", target, result.Content)
	}
	data, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var response developer.Transcription
	if err = json.Unmarshal(data, &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func callNativeTool(t *testing.T, handler *dserver.Handler, name string, args map[string]any) *schema.CallToolResult {
	t.Helper()
	result, rpcErr := handler.CallTool(context.Background(), &jsonrpc.TypedRequest[*schema.CallToolRequest]{Request: &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: name, Arguments: args}}})
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	return result
}

func transcribeToolMetadata(t *testing.T, tools []schema.Tool) schema.Tool {
	t.Helper()
	for _, tool := range tools {
		if tool.Name == developer.TranscribeTool {
			return tool
		}
	}
	t.Fatal("transcribe tool missing")
	return schema.Tool{}
}

func writeSourceFile(t testing.TB, root, path, content string) {
	t.Helper()
	full := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(full), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func readFile(t testing.TB, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func readGeneratedPackage(t testing.TB, dir string) string {
	t.Helper()
	var builder strings.Builder
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".go" {
			return err
		}
		builder.WriteString(readFile(t, path))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return builder.String()
}

func compileGeneratedProject(t testing.TB, root string) {
	t.Helper()
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generated project compile: %v\n%s", err, output)
	}
}
