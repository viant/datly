package mcp_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
	gateway "github.com/viant/datly/gateway/http"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/mcp"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/mcp-protocol/schema"
)

// Public argument identities and HTTP names intentionally differ. Selector
// targets come from canonical metadata rather than suffix/name heuristics.
type relationSelectorsInput struct {
	Limit        int      `parameter:"Limit,kind=query,in=root_limit,required=false" querySelector:"view=parents,property=limit"`
	Offset       int      `parameter:"Offset,kind=query,in=root_offset,required=false" querySelector:"view=parents,property=offset"`
	Page         int      `parameter:"Page,kind=query,in=root_page,required=false" querySelector:"view=parents,property=page"`
	Fields       []string `parameter:"Fields,kind=query,in=root_fields,required=false" querySelector:"view=parents,property=fields"`
	OrderBy      []string `parameter:"OrderBy,kind=query,in=root_sort,required=false" querySelector:"view=parents,property=orderBy"`
	MatchLimit   int      `parameter:"MatchLimit,kind=query,in=matches.limit,required=false" querySelector:"view=matches,property=limit"`
	MatchOffset  int      `parameter:"MatchOffset,kind=query,in=matches.offset,required=false" querySelector:"view=matches,property=offset"`
	MatchPage    int      `parameter:"MatchPage,kind=query,in=matches.page,required=false" querySelector:"view=matches,property=page"`
	MatchFields  []string `parameter:"MatchFields,kind=query,in=matches.fields,required=false" querySelector:"view=matches,property=fields"`
	MatchOrderBy []string `parameter:"MatchOrderBy,kind=query,in=matches.sort,required=false" querySelector:"view=matches,property=orderBy"`
	DetailLimit  int      `parameter:"DetailLimit,kind=query,in=matches.details.limit,required=false" querySelector:"view=details,property=limit"`
	DetailOffset int      `parameter:"DetailOffset,kind=query,in=matches.details.offset,required=false" querySelector:"view=details,property=offset"`
	DetailFields []string `parameter:"DetailFields,kind=query,in=matches.details.fields,required=false" querySelector:"view=details,property=fields"`
}
type relationSelectorsDetail struct {
	ID      int    `sqlx:"id" json:"id"`
	MatchID int    `sqlx:"match_id" json:"matchId"`
	Label   string `sqlx:"label" json:"label"`
}
type relationSelectorsMatch struct {
	ID       int                        `sqlx:"id" json:"id"`
	ParentID int                        `sqlx:"parent_id" json:"parentId"`
	Label    string                     `sqlx:"label" json:"label"`
	Details  []*relationSelectorsDetail `json:"details"`
}
type relationSelectorsRow struct {
	ID      int                       `sqlx:"id" json:"id"`
	Name    string                    `sqlx:"name" json:"name"`
	Matches []*relationSelectorsMatch `json:"matches"`
}
type relationSelectorsOutput struct {
	Data []*relationSelectorsRow `parameter:",kind=output,in=view" json:"data"`
}

func TestRelationSelectorsNativeMCPMatchesHTTP(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	statements := []string{"CREATE TABLE parents(id INTEGER,name TEXT)", "CREATE TABLE matches(id INTEGER,parent_id INTEGER,label TEXT)", "CREATE TABLE details(id INTEGER,match_id INTEGER,label TEXT)"}
	for parent := 1; parent <= 3; parent++ {
		statements = append(statements, fmt.Sprintf("INSERT INTO parents VALUES(%d,'parent-%d')", parent, parent))
		for child := 1; child <= 3; child++ {
			id := parent*10 + child
			statements = append(statements, fmt.Sprintf("INSERT INTO matches VALUES(%d,%d,'match-%d')", id, parent, id))
			for detail := 1; detail <= 2; detail++ {
				statements = append(statements, fmt.Sprintf("INSERT INTO details VALUES(%d,%d,'detail-%d')", id*10+detail, id, id*10+detail))
			}
		}
	}
	require.NoError(t, db.ExecStatements(ctx, statements...))
	policy := func() *spec.Selector {
		return &spec.Selector{AllowFields: true, AllowOrderBy: true, AllowLimit: true, AllowOffset: true, AllowPage: true, DefaultOrder: "id ASC", Orderable: []spec.FieldPath{"id"}}
	}
	details := &spec.View{Name: "details", Selector: policy(), Source: &spec.ViewSource{SQL: "SELECT id,match_id,label FROM details"}}
	matches := &spec.View{Name: "matches", Selector: policy(), Source: &spec.ViewSource{SQL: "SELECT id,parent_id,label FROM matches"}, Relations: []*spec.Relation{{Name: "Details", Holder: "Details", Cardinality: spec.CardinalityMany, View: details, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "match_id"}}}}}
	root := &spec.View{Name: "parents", Selector: policy(), Source: &spec.ViewSource{SQL: "SELECT id,name FROM parents"}, Relations: []*spec.Relation{{Name: "Matches", Holder: "Matches", Cardinality: spec.CardinalityMany, View: matches, On: []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "parent_id"}}}}}
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "RelationSelectors"}, RootView: root, Routes: []*spec.Route{{Method: "GET", Path: "/relation-selectors", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "relations.select"}}}}}
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[relationSelectorsInput](), OutputType: reflect.TypeFor[relationSelectorsOutput](), DirectViewField: "Data"})
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
	native := mcpclient.New(t, service, schema.LatestProtocolVersion)
	listed, err := native.ListTools(t.Context(), nil)
	require.NoError(t, err)
	require.Len(t, listed.Tools, 1)
	encoded, err := json.Marshal(listed.Tools[0].InputSchema)
	require.NoError(t, err)
	var discovery struct {
		Properties map[string]struct {
			Type  string `json:"type"`
			Items struct {
				Type string `json:"type"`
			} `json:"items"`
		} `json:"properties"`
		Required []string `json:"required"`
	}
	require.NoError(t, json.Unmarshal(encoded, &discovery))
	require.Empty(t, discovery.Required)
	bindings := map[string]string{"Limit": "root_limit", "Offset": "root_offset", "Page": "root_page", "Fields": "root_fields", "OrderBy": "root_sort", "MatchLimit": "matches.limit", "MatchOffset": "matches.offset", "MatchPage": "matches.page", "MatchFields": "matches.fields", "MatchOrderBy": "matches.sort", "DetailLimit": "matches.details.limit", "DetailOffset": "matches.details.offset", "DetailFields": "matches.details.fields"}
	require.Len(t, discovery.Properties, len(bindings))
	for name := range bindings {
		prop, ok := discovery.Properties[name]
		require.True(t, ok, "missing %s: %s", name, encoded)
		if strings.HasSuffix(name, "Fields") || strings.HasSuffix(name, "OrderBy") {
			require.Equal(t, "array", prop.Type, name)
			require.Equal(t, "string", prop.Items.Type, name)
		} else {
			require.Equal(t, "integer", prop.Type, name)
		}
	}
	handler := gateway.NewHandler(runtime, nil, "test")
	for _, tc := range []struct {
		name                                string
		args                                map[string]any
		parents                             []int
		childCount, childIndex, detailCount int
		projected                           bool
	}{
		{name: "omitted optional selectors", args: map[string]any{}, parents: []int{1, 2, 3}, childCount: 3, detailCount: 2},
		{name: "child page does not page root", args: map[string]any{"MatchLimit": 1, "MatchPage": 2, "MatchOrderBy": []string{"id DESC"}}, parents: []int{1, 2, 3}, childCount: 1, childIndex: 2, detailCount: 2},
		{name: "child order is independent", args: map[string]any{"MatchLimit": 1, "MatchOrderBy": []string{"id DESC"}}, parents: []int{1, 2, 3}, childCount: 1, childIndex: 3, detailCount: 2},
		{name: "root order is independent", args: map[string]any{"Limit": 1, "OrderBy": []string{"id DESC"}, "MatchLimit": 1, "MatchPage": 1}, parents: []int{3}, childCount: 1, childIndex: 1, detailCount: 2},
		{name: "independent root and child offsets", args: map[string]any{"Limit": 1, "Offset": 1, "MatchLimit": 1, "MatchOffset": 2}, parents: []int{2}, childCount: 1, childIndex: 3, detailCount: 2},
		{name: "root page child first page", args: map[string]any{"Limit": 1, "Page": 3, "MatchLimit": 1, "MatchPage": 1}, parents: []int{3}, childCount: 1, childIndex: 1, detailCount: 2},
		{name: "array fields and nested selectors", args: map[string]any{"Fields": []string{"matches"}, "MatchFields": []string{"label", "details"}, "MatchLimit": 1, "MatchOffset": 1, "DetailFields": []string{"label"}, "DetailLimit": 1, "DetailOffset": 1}, parents: []int{1, 2, 3}, childCount: 1, childIndex: 2, detailCount: 1, projected: true},
		{name: "CSV fields and nested selectors", args: map[string]any{"Fields": "matches", "MatchFields": "label,details", "MatchLimit": 1, "MatchOffset": 1, "DetailFields": "label", "DetailLimit": 1, "DetailOffset": 1}, parents: []int{1, 2, 3}, childCount: 1, childIndex: 2, detailCount: 1, projected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := url.Values{}
			for name, value := range tc.args {
				switch typed := value.(type) {
				case []string:
					query[bindings[name]] = typed
				case int:
					query.Set(bindings[name], strconv.Itoa(typed))
				case string:
					query.Set(bindings[name], typed)
				default:
					t.Fatalf("unexpected test argument %T", value)
				}
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest("GET", "/relation-selectors?"+query.Encode(), nil))
			require.Equal(t, 200, rec.Code, rec.Body.String())
			result, err := native.CallTool(t.Context(), &schema.CallToolRequestParams{Name: "relations.select", Arguments: tc.args})
			require.NoError(t, err)
			require.NotNil(t, result)
			if result.IsError != nil {
				require.False(t, *result.IsError, "%+v", result)
			}
			body, err := json.Marshal(result.StructuredContent)
			require.NoError(t, err)
			require.JSONEq(t, rec.Body.String(), string(body))
			var output relationSelectorsOutput
			require.NoError(t, json.Unmarshal(body, &output))
			require.Len(t, output.Data, len(tc.parents))
			if tc.projected {
				// Projection hides unrequested keys on both transports, but the native
				// reader must retain link keys for every nested relation assembly.
				for _, row := range output.Data {
					require.Zero(t, row.ID)
					require.Empty(t, row.Name)
					for _, match := range row.Matches {
						require.Zero(t, match.ID)
						require.Zero(t, match.ParentID)
						require.NotEmpty(t, match.Details)
					}
				}
				scope, err := testharness.NewRequest("GET", "/relation-selectors").WithQuery(query).Scope()
				require.NoError(t, err)
				defer scope.Close()
				value, err := runtime.ExecuteRoute(t.Context(), "GET", "/relation-selectors", scope)
				require.NoError(t, err)
				output = *(value.(*relationSelectorsOutput))
			}
			for i, row := range output.Data {
				require.Equal(t, tc.parents[i], row.ID, "required relation parent key")
				require.Len(t, row.Matches, tc.childCount)
				if tc.projected {
					require.Empty(t, row.Name)
				}
				for _, match := range row.Matches {
					require.Equal(t, row.ID, match.ParentID, "required relation link key")
					if tc.childIndex > 0 {
						require.Equal(t, row.ID*10+tc.childIndex, match.ID, "required nested relation parent key")
					}
					require.Len(t, match.Details, tc.detailCount)
					for _, detail := range match.Details {
						require.Equal(t, match.ID, detail.MatchID)
						if tc.projected {
							require.Equal(t, fmt.Sprintf("detail-%d", match.ID*10+2), detail.Label)
						}
					}
				}
			}
		})
	}
}
