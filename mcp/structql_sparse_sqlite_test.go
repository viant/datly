package mcp

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/dml"
	viewprovider "github.com/viant/datly/sql/reader/provider"
	"github.com/viant/mcp-protocol/schema"
	xhandler "github.com/viant/xdatly/handler"
)

type sparseRecordIDs struct{ Values []int }
type structQLSparseInput struct {
	Records []*sparseRecord  `parameter:"Records,kind=body,in=data,required" json:"data"`
	IDs     *sparseRecordIDs `parameter:"IDs,kind=param,in=Records" codec:"structql,uri=fixtures:ids.sql" json:"-"`
	Current []*sparseRecord  `parameter:"Current,kind=view,in=Current" view:"Current,table=records" sql:"SELECT id,name,active,quantity,note FROM records WHERE $criteria.In(\"id\", $IDs.Values)" json:"-"`
}

func TestGoShapeStructQLDrivesSparseUpdateNativeMCP(t *testing.T) {
	for _, tt := range []struct {
		name   string
		fields map[string]any
		want   sparseRecord
	}{
		{"omitted fields", map[string]any{"name": "new"}, sparseRecord{ID: 1, Name: "new", Active: true, Quantity: 9}},
		{"explicit zero", map[string]any{"quantity": 0}, sparseRecord{ID: 1, Name: "keep", Active: true, Quantity: 0}},
		{"explicit false", map[string]any{"active": false}, sparseRecord{ID: 1, Name: "keep", Active: false, Quantity: 9}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			h := sqlite.New(t)
			if err := h.ExecStatements(context.Background(), "CREATE TABLE records (id INTEGER PRIMARY KEY, name TEXT, active BOOLEAN, quantity INTEGER, note TEXT)", "INSERT INTO records VALUES (1,'keep',1,9,NULL),(2,'untouched',1,8,NULL)"); err != nil {
				t.Fatal(err)
			}
			resources := resource.New()
			if err := resources.Register("fixtures", fstest.MapFS{"ids.sql": &fstest.MapFile{Data: []byte("? SELECT ARRAY_AGG(ID) AS Values FROM `/` LIMIT 1")}}); err != nil {
				t.Fatal(err)
			}
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/records", Name: "Patch"}, Routes: []*spec.Route{{Method: "PATCH", Path: "/records", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "records.patch"}}}}}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(structQLSparseInput{}), OutputType: reflect.TypeOf(sparseOutput{}), Resources: resources})
			if err != nil {
				t.Fatal(err)
			}
			views, err := viewprovider.New(viewprovider.Config{Dependencies: artifact.ViewDependencies, Input: artifact.Input, SQL: &dsql.SQLComponent{DB: h.DB}})
			if err != nil {
				t.Fatal(err)
			}
			handler := rhandler.HandlerFunc(func(ctx context.Context, invocation rhandler.Invocation) (any, error) {
				input := invocation.Input.(*structQLSparseInput)
				if input.IDs == nil || !reflect.DeepEqual(input.IDs.Values, []int{1}) || len(input.Current) != 1 || input.Current[0].ID != 1 {
					return nil, fmt.Errorf("StructQL/current-state chain failed: %+v", input)
				}
				if len(input.Records) != 1 || input.Records[0].Has == nil {
					return nil, fmt.Errorf("record presence lost")
				}
				value, _, err := invocation.Binder.Lookup(ctx, xhandler.DMLKey)
				if err != nil {
					return nil, err
				}
				if err := value.(xhandler.DML).Update("records", input.Records[0]); err != nil {
					return nil, err
				}
				return &sparseOutput{OK: true}, nil
			})
			registered := &registry.RegisteredComponent{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(sparseOutput{}), Handler: handler, Providers: []locator.Provider{views}, DataSource: dml.Source{DB: h.DB}}
			native := mcpclient.New(t, runtimeToolService(t, registered), schema.LatestProtocolVersion)
			fields := map[string]any{"id": 1}
			for name, value := range tt.fields {
				fields[name] = value
			}
			result, err := native.CallTool(context.Background(), &schema.CallToolRequestParams{Name: "records.patch", Arguments: map[string]any{"data": []any{fields}}})
			if err != nil || result == nil || result.IsError != nil && *result.IsError {
				t.Fatalf("MCP result=%+v error=%v", result, err)
			}
			h.AssertQuery(t, context.Background(), sqlite.Query{SQL: "SELECT id,name,active,quantity,note FROM records ORDER BY id"}, []sparseRecord{tt.want, {ID: 2, Name: "untouched", Active: true, Quantity: 8}})
		})
	}
}
