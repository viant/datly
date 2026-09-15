package reader

import (
	"context"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	handlerprovider "github.com/viant/datly/runtime/handler/provider"
	dsql "github.com/viant/datly/sql"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/datly/transcribe"
	"github.com/viant/scy/auth/jwt"
)

type OrderSearchInput struct {
	JWT         *jwt.Claims `json:"-"`
	Reference   string
	TenantID    int
	WorkspaceID int
	Search      string
	Minimum     int
	Maximum     int
	Has         *OrderSearchHas `setMarker:"true" json:"-"`
}
type OrderSearchHas struct{ TenantID, WorkspaceID, Search, Minimum, Maximum, JWT, Reference bool }
type Order struct {
	ID          int    `sqlx:"id" json:"id"`
	TenantID    int    `sqlx:"tenant_id" json:"tenantId"`
	WorkspaceID int    `sqlx:"workspace_id" json:"workspaceId"`
	Name        string `sqlx:"name" json:"name"`
	Reference   string `sqlx:"reference" json:"reference"`
	Total       int    `sqlx:"total" json:"total"`
}
type OrderSearchOutput struct {
	Data []*Order `parameter:"Data,kind=output,in=view" view:"orders" json:"data"`
}

// Observe the real template and bound input without replacing parsing, binding,
// predicate evaluation, or SQLite execution.
type predicateGroupsTrace struct {
	sqltemplate.Evaluator
	result sqltemplate.Result
	input  *OrderSearchInput
	calls  int
}

func (p *predicateGroupsTrace) Evaluate(ctx context.Context, invocation sqltemplate.Invocation) (sqltemplate.Result, error) {
	p.calls++
	input := invocation.Input
	for input.Kind() == reflect.Pointer {
		input = input.Elem()
	}
	copy := input.Interface().(OrderSearchInput)
	p.input = &copy
	result, err := p.Evaluator.Evaluate(ctx, invocation)
	p.result = result
	return result, err
}

func TestReaderPredicateGroupsDQLSQLite(t *testing.T) {
	ctx := context.Background()
	h := testharness.NewSQLiteHarness(t)
	require.NoError(t, h.ExecStatements(ctx,
		`CREATE TABLE purchase_orders (id INTEGER PRIMARY KEY, tenant_id INTEGER, workspace_id INTEGER, name TEXT, reference TEXT, total INTEGER)`,
		`INSERT INTO purchase_orders VALUES
          (1,7,70,'Alpha desk','D-1',-5),
          (2,7,70,'Beta chair','Alpha-2',0),
          (3,7,70,'Alpha lamp','L-3',20),
          (4,7,70,'Gamma rug','G-4',50),
          (5,8,70,'Alpha other tenant','Alpha-5',10),
          (6,7,71,'Alpha other workspace','Alpha-6',10),
          (7,8,80,'Alpha own order','A-7',10),
          (8,7,70,'Quote: x'' OR 1=1 --','Q-8',30)`))
	source, err := os.ReadFile("testdata/predicate_groups.sql")
	require.NoError(t, err)
	const builder = `${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND"), $predicate.FilterGroup(1, "OR"), $predicate.FilterGroup(2, "AND")).Build("WHERE")}`
	for _, mode := range []string{"where", "and after fixed condition", "optional AND after fixed scope", "chained", "nested", "repeated parameter across groups"} {
		t.Run(mode, func(t *testing.T) {
			dql := string(source)
			switch mode {
			case "and after fixed condition":
				dql = strings.Replace(dql, builder, `WHERE r.id > 0 `+strings.Replace(builder, `Build("WHERE")`, `Build("AND")`, 1), 1)
			case "optional AND after fixed scope":
				optional := strings.Replace(builder, `$predicate.FilterGroup(0, "AND"), `, "", 1)
				optional = strings.Replace(optional, `Build("WHERE")`, `Build("AND")`, 1)
				dql = strings.Replace(dql, builder, `WHERE r.tenant_id = :TenantID AND r.workspace_id = :WorkspaceID `+optional, 1)
			case "chained":
				dql = strings.Replace(dql, builder, `${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).And().CombineOr($predicate.FilterGroup(1, "OR")).And().CombineAnd($predicate.FilterGroup(2, "AND")).Build("WHERE")}`, 1)
			case "nested":
				dql = strings.Replace(dql, builder, `${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND"), $predicate.Builder().CombineAnd($predicate.FilterGroup(1, "OR"), $predicate.FilterGroup(2, "AND")).Build("")).Build("WHERE")}`, 1)
			case "repeated parameter across groups":
				// Reuse the same search value as a mandatory name refinement
				// as well as the name/reference OR search; bind it each time.
				dql = strings.Replace(dql, `.WithPredicate(1, 'contains', 'r', 'reference')`, `.WithPredicate(1, 'contains', 'r', 'reference').WithPredicate(3, 'contains', 'r', 'name')`, 1)
				dql = strings.Replace(dql, `$predicate.FilterGroup(2, "AND")`, `$predicate.FilterGroup(2, "AND"), $predicate.FilterGroup(3, "AND")`, 1)
			}
			types := testTypeCatalog(t, map[string]reflect.Type{
				"example.com/app/orders/read.OrderSearchInput":  reflect.TypeOf(OrderSearchInput{}),
				"example.com/app/orders/read.OrderSearchOutput": reflect.TypeOf(OrderSearchOutput{}),
				"example.com/app/orders/read.Order":             reflect.TypeOf(Order{}),
			})
			compiled, err := transcribe.NewCompiler().Compile(ctx, &transcribe.Source{Name: "orders", Text: dql, Types: types})
			require.NoError(t, err)
			artifact, err := buildArtifact(bootstrap.ArtifactInput{Component: compiled.Component, InputType: reflect.TypeOf(OrderSearchInput{}), OutputType: reflect.TypeOf(OrderSearchOutput{}), Types: types})
			require.NoError(t, err)
			require.Equal(t, "orders", artifact.Component.RootView.Name)
			require.NotNil(t, artifact.Reader.Root.Template)
			trace := &predicateGroupsTrace{Evaluator: artifact.Reader.Root.Template}
			artifact.Reader.Root.Template = trace
			for _, tc := range []struct {
				name, query       string
				tenant, workspace int
				ids               []int
				args              []any
				missingTenant     bool
			}{
				{name: "optional groups empty", tenant: 7, workspace: 70, ids: []int{1, 2, 3, 4, 8}, args: []any{7, 70}},
				{name: "search OR matches either column", query: "q=Alpha", tenant: 7, workspace: 70, ids: []int{1, 2, 3}, args: []any{7, 70, "%Alpha%", "%Alpha%"}},
				{name: "all groups enabled", query: "q=Alpha&min=0&max=20", tenant: 7, workspace: 70, ids: []int{2, 3}, args: []any{7, 70, "%Alpha%", "%Alpha%", 0, 20}},
				{name: "present zero maximum", query: "max=0", tenant: 7, workspace: 70, ids: []int{1, 2}, args: []any{7, 70, 0}},
				{name: "lower bound only", query: "min=0", tenant: 7, workspace: 70, ids: []int{2, 3, 4, 8}, args: []any{7, 70, 0}},
				{name: "bounds without search", query: "min=1&max=25", tenant: 7, workspace: 70, ids: []int{3}, args: []any{7, 70, 1, 25}},
				{name: "impossible bounds", query: "min=30&max=0", tenant: 7, workspace: 70, args: []any{7, 70, 30, 0}},
				{name: "other tenant scope", query: "q=Alpha", tenant: 8, workspace: 80, ids: []int{7}, args: []any{8, 80, "%Alpha%", "%Alpha%"}},
				{name: "scope cannot be overridden by query", query: "tenant=8&workspace=80&q=Alpha", tenant: 7, workspace: 70, ids: []int{1, 2, 3}, args: []any{7, 70, "%Alpha%", "%Alpha%"}},
				{name: "SQL text remains bound data", query: "q=" + url.QueryEscape("x' OR 1=1 --"), tenant: 7, workspace: 70, ids: []int{8}, args: []any{7, 70, "%x' OR 1=1 --%", "%x' OR 1=1 --%"}},
				{name: "missing trusted tenant", workspace: 70, missingTenant: true},
				{name: "present empty search", query: "q=", tenant: 7, workspace: 70, ids: []int{1, 2, 3, 4, 8}, args: []any{7, 70, "%%", "%%"}},
				{name: "empty groups again after populated calls", tenant: 7, workspace: 70, ids: []int{1, 2, 3, 4, 8}, args: []any{7, 70}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					values, err := url.ParseQuery(tc.query)
					require.NoError(t, err)
					providers := []locator.Provider{handlerprovider.Named("workspace", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
						return tc.workspace, name == "workspace", nil
					})}
					if !tc.missingTenant {
						providers = append(providers, handlerprovider.Named("tenant", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
							return tc.tenant, name == "tenant", nil
						}))
					}
					before := trace.calls
					actual, err := NewService().Read(ctx, &Session{Component: artifact.Component, OutputType: reflect.TypeOf(OrderSearchOutput{}), Input: routeInput(t, artifact), Artifact: artifact.Reader, SQL: &dsql.SQLComponent{DB: h.DB}, Scope: testharness.Request{}.WithQuery(values), Providers: providers})
					if tc.missingTenant {
						require.Error(t, err)
						require.Equal(t, before, trace.calls, "required scope must fail before SQL template evaluation")
						return
					}
					require.NoError(t, err)
					require.Equal(t, before+1, trace.calls)
					wantIDs, wantArgs := tc.ids, tc.args
					if mode == "optional AND after fixed scope" {
						// Named scope bindings are resolved downstream by the SQL builder.
						wantArgs = tc.args[2:]
						if len(wantArgs) == 0 {
							wantArgs = nil
						}
						require.NotContains(t, trace.result.SQL, "AND ()")
						require.NotContains(t, trace.result.SQL, "AND ( )")
					}
					if mode == "repeated parameter across groups" && values.Has("q") {
						wantArgs = append(append([]any(nil), wantArgs...), "%"+values.Get("q")+"%")
						wantIDs = nil
						for _, id := range tc.ids {
							if id != 2 || values.Get("q") == "" {
								wantIDs = append(wantIDs, id)
							}
						}
					}
					var ids []int
					for _, row := range actual.(*OrderSearchOutput).Data {
						require.Equal(t, tc.tenant, row.TenantID)
						require.Equal(t, tc.workspace, row.WorkspaceID)
						ids = append(ids, row.ID)
					}
					require.Equal(t, wantIDs, ids)
					require.Equal(t, wantArgs, trace.result.Args)
					require.Equal(t, len(wantArgs), strings.Count(trace.result.SQL, "?"))
					require.NotContains(t, trace.result.SQL, "Alpha")
					require.NotContains(t, trace.result.SQL, "x' OR 1=1 --")
					require.NotNil(t, trace.input.Has)
					require.Equal(t, values.Has("min"), trace.input.Has.Minimum)
					require.Equal(t, values.Has("max"), trace.input.Has.Maximum)
					require.Equal(t, values.Has("q"), trace.input.Has.Search)
				})
			}
		})
	}
}
