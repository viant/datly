package mcp_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/mcp-protocol/schema"
)

type caseFormatRelationInput struct {
	Fields        []string `parameter:"Fields,kind=query,in=fields" querySelector:"view=parents,property=fields"`
	OrderBy       string   `parameter:"OrderBy,kind=query,in=sort" querySelector:"view=parents,property=orderBy"`
	Criteria      string   `parameter:"Criteria,kind=query,in=criteria" querySelector:"view=parents,property=criteria"`
	Limit         int      `parameter:"Limit,kind=query,in=limit" querySelector:"view=parents,property=limit"`
	Offset        int      `parameter:"Offset,kind=query,in=offset" querySelector:"view=parents,property=offset"`
	Page          int      `parameter:"Page,kind=query,in=page" querySelector:"view=parents,property=page"`
	ChildFields   []string `parameter:"ChildFields,kind=query,in=child.fields" querySelector:"view=children,property=fields"`
	ChildOrderBy  string   `parameter:"ChildOrderBy,kind=query,in=child.sort" querySelector:"view=children,property=orderBy"`
	ChildCriteria string   `parameter:"ChildCriteria,kind=query,in=child.criteria" querySelector:"view=children,property=criteria"`
	ChildLimit    int      `parameter:"ChildLimit,kind=query,in=child.limit" querySelector:"view=children,property=limit"`
	ChildOffset   int      `parameter:"ChildOffset,kind=query,in=child.offset" querySelector:"view=children,property=offset"`
	ChildPage     int      `parameter:"ChildPage,kind=query,in=child.page" querySelector:"view=children,property=page"`
}
type caseFormatChild struct {
	ID              int    `sqlx:"id"`
	ParentID        int    `sqlx:"parent_id"`
	SampleSeen_1Day int    `sqlx:"sample_seen_1_day"`
	Secret          string `sqlx:"secret" json:"-" internal:"true"`
}
type caseFormatParent struct {
	ID              int `sqlx:"id"`
	SampleSeen_1Day int `sqlx:"sample_seen_1_day"`
	Children        []*caseFormatChild
}
type caseFormatRelationOutput struct {
	Data []*caseFormatParent `parameter:",kind=output,in=view"`
}

func TestExposureCaseFormatRootChildSelectorValues(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE parents(id INTEGER,sample_seen_1_day INTEGER)",
		"CREATE TABLE children(id INTEGER,parent_id INTEGER,sample_seen_1_day INTEGER,secret TEXT)",
		"INSERT INTO parents VALUES(1,10),(2,20),(3,30)",
		"INSERT INTO children VALUES(11,1,11,'s'),(12,1,12,'s'),(21,2,21,'s'),(22,2,22,'s'),(31,3,31,'s'),(32,3,32,'s')"))
	policy := func() *spec.Selector {
		return &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowCriteria: true, AllowLimit: true, AllowOffset: true, AllowPage: true, DefaultOrder: "id ASC", Orderable: []spec.FieldPath{"id", "sample_seen_1_day"}, Filterable: []spec.FieldPath{"SampleSeen_1Day"}}
	}
	child := &spec.View{Name: "children", Selector: policy(), Source: &spec.ViewSource{SQL: "SELECT id,parent_id,sample_seen_1_day FROM children"}}
	root := &spec.View{Name: "parents", Selector: policy(), Source: &spec.ViewSource{SQL: "SELECT id,sample_seen_1_day FROM parents"}, Relations: []*spec.Relation{{Name: "Children", Holder: "Children", Cardinality: spec.CardinalityMany, View: child, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}}}}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "CaseFormatRelations"}, RootView: root, Routes: []*spec.Route{{Method: "GET", Path: "/case-format-relations", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "CaseFormatRelations"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[caseFormatRelationInput](), OutputType: reflect.TypeFor[caseFormatRelationOutput](), DirectViewField: "Data"})
	require.NoError(t, err)
	reader, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}})
	require.NoError(t, err)
	registered, err := artifact.Registration(druntime.RegisteredComponent{Reader: reader})
	require.NoError(t, err)
	runtime, err := druntime.NewRuntime([]*druntime.RegisteredComponent{registered})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Shutdown(ctx)) })
	service, err := mcp.New(mcp.Config{Components: []*druntime.RegisteredComponent{registered}, Invoker: runtime})
	require.NoError(t, err)
	require.NoError(t, service.PrepareTool(ctx, "CaseFormatRelations"))
	tool, ok := service.Registry().ToolRegistry.Get("CaseFormatRelations")
	require.True(t, ok)
	for _, tc := range []struct {
		name     string
		args     map[string]any
		expected string
		bad      bool
	}{
		{"root and child values", map[string]any{"fields": []string{"sampleSeen1Day", "children"}, "orderBy": "sampleSeen1Day DESC", "criteria": "sampleSeen1Day >= 20", "limit": 1, "offset": 1, "childFields": []string{"sampleSeen1Day"}, "childOrderBy": "sampleSeen1Day DESC", "childCriteria": "sampleSeen1Day >= 21", "childLimit": 1, "childOffset": 1}, `{"data":[{"sampleSeen1Day":20,"children":[{"sampleSeen1Day":21}]}]}`, false},
		{"root and child pages", map[string]any{"fields": []string{"sampleSeen1Day", "children"}, "orderBy": "sampleSeen1Day DESC", "limit": 1, "page": 2, "childFields": []string{"sampleSeen1Day"}, "childOrderBy": "sampleSeen1Day DESC", "childLimit": 1, "childPage": 2}, `{"data":[{"sampleSeen1Day":20,"children":[{"sampleSeen1Day":21}]}]}`, false},
		{"quoted root and child fields", map[string]any{"fields": []string{`"sampleSeen1Day"`, "children"}, "orderBy": "sampleSeen1Day DESC", "limit": 1, "childFields": []string{`"sampleSeen1Day"`}, "childOrderBy": "sampleSeen1Day DESC", "childLimit": 1}, `{"data":[{"sampleSeen1Day":30,"children":[{"sampleSeen1Day":32}]}]}`, false},
		{"quoted root and child ordering", map[string]any{"fields": []string{"sampleSeen1Day", "children"}, "orderBy": `"sampleSeen1Day" DESC`, "limit": 1, "childFields": []string{"sampleSeen1Day"}, "childOrderBy": `"sampleSeen1Day" DESC`, "childLimit": 1}, `{"data":[{"sampleSeen1Day":30,"children":[{"sampleSeen1Day":32}]}]}`, false},

		{"child filter cannot authorize hidden field", map[string]any{"childCriteria": "secret = 's'"}, "", true},
		{"child filter preserves allowed list", map[string]any{"childCriteria": "parentId = 1"}, "", true},
		{"child order preserves allowed list", map[string]any{"childOrderBy": "parentId DESC"}, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, rpcErr := tool.Handler(ctx, &schema.CallToolRequest{Method: schema.MethodToolsCall, Params: schema.CallToolRequestParams{Name: "CaseFormatRelations", Arguments: tc.args}})
			if tc.bad {
				require.True(t, rpcErr != nil || result != nil && result.IsError != nil && *result.IsError)
				return
			}
			require.Nil(t, rpcErr)
			body, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.JSONEq(t, tc.expected, string(body))
		})
	}
}
