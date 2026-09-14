package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	mtool "github.com/viant/datly/mcp/tool"
	rhandler "github.com/viant/datly/runtime/handler"
	veltyhandler "github.com/viant/datly/runtime/handler/velty"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/dml"
	dtag "github.com/viant/datly/tag"
	"github.com/viant/mcp-protocol/schema"
	xdatly "github.com/viant/xdatly"
	xhandler "github.com/viant/xdatly/handler"
)

type sparseFields struct{ ID, Name, Active, Quantity, Note bool }
type sparseRecord struct {
	ID       int           `json:"id" sqlx:"id,primaryKey"`
	Name     string        `json:"name" sqlx:"name"`
	Active   bool          `json:"active" sqlx:"active"`
	Quantity int           `json:"quantity" sqlx:"quantity"`
	Note     *string       `json:"note" sqlx:"note"`
	Has      *sparseFields `sqlx:"-" setMarker:"true"`
}
type sparseInput struct {
	Patch *sparseRecord `parameter:"Patch,kind=body,in=patch,required" json:"patch"`
}
type sparseOutput struct {
	OK bool `json:"ok"`
}

type sparseComponents struct {
	RecordPatch xdatly.Component[sparseInput, sparseOutput] `component:"RecordPatch,path=/records,method=PATCH,connector=main,handler=NewRecordPatchHandler" mcp:"[{\"kind\":\"tool\",\"name\":\"records.patch\"}]"`
}

func NewRecordPatchHandler() rhandler.Handler {
	return rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
		input := invocation.Input.(*sparseInput)
		if input.Patch == nil || input.Patch.Has == nil || !input.Patch.Has.ID {
			return nil, fmt.Errorf("missing patch identity or internal presence")
		}
		value, _, err := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
		if err != nil {
			return nil, err
		}
		if err := value.(xhandler.DML).Update("records", input.Patch); err != nil {
			return nil, err
		}
		return &sparseOutput{OK: true}, nil
	})
}

func TestMCPSparseUpdateSQLite(t *testing.T) {
	note := "keep note"
	for _, target := range []struct{ kind, protocol string }{
		{"Go", schema.LegacyProtocolVersion}, {"Go", schema.LatestProtocolVersion},
		{"Velty", schema.LegacyProtocolVersion}, {"Velty", schema.LatestProtocolVersion},
	} {
		for _, tt := range []struct {
			name     string
			fields   map[string]any
			expected sparseRecord
		}{
			{"omitted fields", map[string]any{"name": "new"}, sparseRecord{ID: 1, Name: "new", Active: true, Quantity: 9, Note: &note}},
			{"explicit empty", map[string]any{"name": ""}, sparseRecord{ID: 1, Name: "", Active: true, Quantity: 9, Note: &note}},
			{"explicit false", map[string]any{"active": false}, sparseRecord{ID: 1, Name: "keep", Active: false, Quantity: 9, Note: &note}},
			{"explicit zero", map[string]any{"quantity": 0}, sparseRecord{ID: 1, Name: "keep", Active: true, Quantity: 0, Note: &note}},
			{"explicit null", map[string]any{"note": nil}, sparseRecord{ID: 1, Name: "keep", Active: true, Quantity: 9, Note: nil}},
			{"no field changes", map[string]any{}, sparseRecord{ID: 1, Name: "keep", Active: true, Quantity: 9, Note: &note}},
			{"client cannot set presence", map[string]any{"Has": map[string]any{"Name": true, "Quantity": true}}, sparseRecord{ID: 1, Name: "keep", Active: true, Quantity: 9, Note: &note}},
		} {
			t.Run(target.kind+"/"+target.protocol+"/"+tt.name, func(t *testing.T) {
				h := testharness.NewSQLiteHarness(t)
				ctx := context.Background()
				if err := h.ExecStatements(ctx, "CREATE TABLE records (id INTEGER PRIMARY KEY, name TEXT NOT NULL, active BOOLEAN NOT NULL, quantity INTEGER NOT NULL, note TEXT)", "INSERT INTO records VALUES (1,'keep',1,9,'keep note')"); err != nil {
					t.Fatal(err)
				}
				field := reflect.TypeOf(sparseComponents{}).Field(0)
				metadata, found, err := dtag.ParseComponent(field.Tag)
				if err != nil || !found {
					t.Fatalf("component declaration: %v", err)
				}
				if metadata.Handler != "NewRecordPatchHandler" {
					t.Fatal("handler factory metadata lost")
				}
				component, err := (&bootstrap.RouteSource{PackagePath: "example.com/sparse", FieldName: field.Name, Tag: metadata}).Resolve(reflect.TypeOf(sparseInput{}), reflect.TypeOf(sparseOutput{}))
				if err != nil {
					t.Fatal(err)
				}
				artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(sparseInput{}), OutputType: reflect.TypeOf(sparseOutput{})})
				if err != nil {
					t.Fatal(err)
				}
				contract, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "PATCH", Path: "/records"})
				if !ok {
					t.Fatal("missing route contract")
				}
				plan, err := mtool.NewCompiler().Compile(mtool.Input{Component: artifact.Component.Key, Exposure: component.Routes[0].MCP[0], Contract: contract})
				if err != nil {
					t.Fatal(err)
				}
				encoded, err := json.Marshal(plan.Metadata().InputSchema)
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Contains(bytes.ToLower(encoded), []byte(`"has"`)) || !bytes.Contains(encoded, []byte(`"quantity"`)) {
					t.Fatalf("incorrect MCP business schema: %s", encoded)
				}
				var handler rhandler.Handler = NewRecordPatchHandler()
				if target.kind == "Velty" {
					handler, err = veltyhandler.New[sparseInput, sparseOutput](veltyhandler.Config{Template: `$dml.Update("records", $Input.Patch)
#set($Output.OK = true)`})
					if err != nil {
						t.Fatal(err)
					}
				}
				registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(sparseOutput{}), Handler: handler, DataSource: dml.Source{DB: h.DB}}
				fields := map[string]any{"id": 1}
				for name, value := range tt.fields {
					fields[name] = value
				}
				_, wireSchema := executeNativeRuntimeTool(t, registered, "records.patch", map[string]any{"patch": fields}, target.protocol)
				wireJSON, err := json.Marshal(wireSchema.Properties)
				if err != nil {
					t.Fatal(err)
				}
				expectedProperties, err := json.Marshal(plan.Metadata().InputSchema.Properties)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(expectedProperties, wireJSON) || wireSchema.Type != "object" || !reflect.DeepEqual(wireSchema.Required, []string{"patch"}) {
					t.Fatalf("wire schema differs from compiled business schema: %s vs %s", wireJSON, expectedProperties)
				}
				properties := wireSchema.Properties["patch"]["properties"].(map[string]any)
				if !reflect.DeepEqual(properties["note"].(map[string]any)["type"], []any{"string", "null"}) {
					t.Fatal("nullable note is not advertised as nullable")
				}
				h.AssertQuery(t, ctx, sqlite.Query{SQL: "SELECT id,name,active,quantity,note FROM records ORDER BY id"}, []sparseRecord{tt.expected})
			})
		}
	}
}
