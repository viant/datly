package developer_test

import (
	"context"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp/developer"
	dserver "github.com/viant/datly/mcp/server"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/jsonrpc"
	"github.com/viant/mcp-protocol/schema"
)

var _ dserver.ServerService = (*developer.Service)(nil)

type validationFixture struct {
	base      string
	validator transcribe.Validator
}

func newValidationFixture(t *testing.T, query string) validationFixture {
	t.Helper()
	base := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/developerfixture"}).Write(t, base)
	fixture := validationFixture{base: base, validator: transcribe.Validator{BaseDir: base, Include: []string{"example.com/developerfixture/svc/records"}}}
	fixture.write(t, "svc/records/Records.dql", "#setting($_ = $route('/records','GET'))\n"+query)
	fixture.write(t, "svc/records/init.go", "package records\nfunc init(){panic(\"validation must not execute application init\")}\n")
	fixture.write(t, "generated/preserve.txt", "operator-owned content")
	return fixture
}

func (f validationFixture) write(t *testing.T, name, text string) {
	t.Helper()
	path := filepath.Join(f.base, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0644); err != nil {
		t.Fatal(err)
	}
}

func (f validationFixture) snapshot(t *testing.T) map[string]string {
	t.Helper()
	files := map[string]string{}
	if err := filepath.WalkDir(f.base, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		content, err := os.ReadFile(path)
		files[path] = string(content)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return files
}

func callValidation(t *testing.T, service *developer.Service, ctx context.Context, arguments map[string]any) (*schema.CallToolResult, *jsonrpc.Error) {
	t.Helper()
	entry, found := service.Registry().ToolRegistry.Get(developer.ValidationTool)
	if !found {
		t.Fatal("validation tool was not registered")
	}
	return entry.Handler(ctx, &schema.CallToolRequest{Params: schema.CallToolRequestParams{Name: developer.ValidationTool, Arguments: arguments}})
}

func assertReport(t *testing.T, result *schema.CallToolResult, expected *transcribe.ValidationReport) {
	t.Helper()
	if result == nil || result.IsError == nil || *result.IsError == expected.Valid {
		t.Fatalf("incorrect tool status: %+v", result)
	}
	var wire struct {
		Content []struct{ Text string }
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &wire); err != nil || len(wire.Content) != 1 {
		t.Fatalf("tool content: %s %v", data, err)
	}
	structured, err := json.Marshal(result.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range [][]byte{[]byte(wire.Content[0].Text), structured} {
		var report transcribe.ValidationReport
		if err := json.Unmarshal(payload, &report); err != nil || !reflect.DeepEqual(expected, &report) {
			t.Fatalf("report diverged from transcribe.Validator: %s\nwant=%+v error=%v", payload, expected, err)
		}
	}
}

func TestDeveloperValidationUsesCanonicalReportWithoutWrites(t *testing.T) {
	for _, query := range []string{"SELECT 1 AS ID", "SELECT ("} {
		t.Run(query, func(t *testing.T) {
			fixture := newValidationFixture(t, query)
			before := fixture.snapshot(t)
			expected, _ := fixture.validator.Validate(context.Background())
			service, err := developer.New(developer.Config{Targets: map[string]transcribe.Validator{"records": fixture.validator}})
			if err != nil {
				t.Fatal(err)
			}
			result, rpcErr := callValidation(t, service, context.Background(), map[string]any{"target": "records"})
			if rpcErr != nil {
				t.Fatal(rpcErr)
			}
			assertReport(t, result, expected)
			if !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("validation changed source or generated files")
			}
			if len(service.Registry().ListRegisteredTools()) != 7 {
				t.Fatal("developer service exposed business tools")
			}
		})
	}
}

func TestDeveloperValidationOperatorAuthorityAndSnapshot(t *testing.T) {
	fixture := newValidationFixture(t, "SELECT 1 AS ID")
	config := developer.Config{Targets: map[string]transcribe.Validator{"records": fixture.validator}}
	service, err := developer.New(config)
	if err != nil {
		t.Fatal(err)
	}
	expected, _ := fixture.validator.Validate(context.Background())
	config.Targets["records"] = transcribe.Validator{BaseDir: "/not-the-configured-project"}
	fixture.validator.Include[0] = "example.com/missing"
	result, rpcErr := callValidation(t, service, context.Background(), map[string]any{"target": "records"})
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	assertReport(t, result, expected)
	for _, arguments := range []map[string]any{
		nil, {}, {"target": 42}, {"target": "missing"},
		{"target": "records", "baseDir": "/tmp"}, {"target": "records", "moduleDirs": []string{"/tmp"}},
		{"target": "records", "dsn": "private"}, {"target": "records", "connector": "other"},
		{"target": "records", "schema": true}, {"target": "records", "include": []string{"./..."}},
	} {
		result, rpcErr := callValidation(t, service, context.Background(), arguments)
		if result != nil || rpcErr == nil || rpcErr.Code != jsonrpc.InvalidParams {
			t.Fatalf("unconfigured authority accepted: %v %v", result, rpcErr)
		}
	}
}

func TestDeveloperValidationCancellationReport(t *testing.T) {
	fixture := newValidationFixture(t, "SELECT 1 AS ID")
	service, err := developer.New(developer.Config{Targets: map[string]transcribe.Validator{"records": fixture.validator}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	expected, _ := fixture.validator.Validate(ctx)
	result, rpcErr := callValidation(t, service, ctx, map[string]any{"target": "records"})
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	assertReport(t, result, expected)
}

func TestDeveloperValidationExplicitSchemaAuthority(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	h.DB.SetMaxOpenConns(1)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER NOT NULL)", "INSERT INTO records VALUES(7)", "PRAGMA query_only=ON"); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"SELECT id FROM records", "SELECT absent FROM records"} {
		fixture := newValidationFixture(t, query)
		schemaValidator := fixture.validator
		schemaValidator.Connector = "main"
		schemaValidator.ColumnRefiner = column.New(column.Connections{"main": h.DB})
		service, err := developer.New(developer.Config{Targets: map[string]transcribe.Validator{"static": fixture.validator, "schema": schemaValidator}})
		if err != nil {
			t.Fatal(err)
		}
		before := fixture.snapshot(t)
		for name, validator := range map[string]transcribe.Validator{"static": fixture.validator, "schema": schemaValidator} {
			expected, _ := validator.Validate(ctx)
			result, rpcErr := callValidation(t, service, ctx, map[string]any{"target": name})
			if rpcErr != nil {
				t.Fatal(rpcErr)
			}
			assertReport(t, result, expected)
		}
		if !reflect.DeepEqual(before, fixture.snapshot(t)) {
			t.Fatal("schema validation changed project files")
		}
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id FROM records"}, []struct{ ID int }{{ID: 7}})
}

func TestDeveloperValidationConfigRequiresExplicitTargets(t *testing.T) {
	for _, config := range []developer.Config{{}, {Targets: map[string]transcribe.Validator{"": {BaseDir: "/tmp"}}}, {Targets: map[string]transcribe.Validator{"records": {}}}} {
		if _, err := developer.New(config); err == nil {
			t.Fatal("incomplete developer authority accepted")
		}
	}
	var absent *developer.Service
	if _, err := dserver.New(dserver.Config{Service: absent}); err == nil {
		t.Fatal("server accepted an absent developer service")
	}
}

func TestDeveloperValidationGoAndLinkedPackages(t *testing.T) {
	for _, linked := range []bool{false, true} {
		t.Run(map[bool]string{false: "Go package", true: "linked DQL"}[linked], func(t *testing.T) {
			fixture := newValidationFixture(t, "SELECT id FROM records")
			if !linked {
				if err := os.Remove(filepath.Join(fixture.base, "svc/records/Records.dql")); err != nil {
					t.Fatal(err)
				}
			}
			fixture.write(t, "svc/records/component.go", `package records
import xdatly "github.com/viant/xdatly"
type Row struct { ID int `+"`sqlx:\"id\"`"+` }
type Input struct {}
type Output struct { Data []*Row `+"`parameter:\"Data,kind=output,in=view\" view:\"Records,table=records\" sql:\"SELECT id FROM records\"`"+` }
type Component struct { Contract xdatly.Component[Input,Output] `+"`component:\"Records,path=/records,method=GET,view=Records\"`"+` }
`)
			before := fixture.snapshot(t)
			expected, err := fixture.validator.Validate(context.Background())
			if err != nil || !expected.Valid {
				t.Fatalf("canonical package validation: %+v %v", expected, err)
			}
			service, err := developer.New(developer.Config{Targets: map[string]transcribe.Validator{"records": fixture.validator}})
			if err != nil {
				t.Fatal(err)
			}
			result, rpcErr := callValidation(t, service, context.Background(), map[string]any{"target": "records"})
			if rpcErr != nil {
				t.Fatal(rpcErr)
			}
			assertReport(t, result, expected)
			if !reflect.DeepEqual(before, fixture.snapshot(t)) {
				t.Fatal("Go package validation changed files")
			}
		})
	}
}
