package readerbuilder_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	rb "github.com/viant/datly/authoring/readerbuilder"
	"github.com/viant/datly/bootstrap"
	dexec "github.com/viant/datly/exec"
	"github.com/viant/datly/internal/testharness/sqlite"
	druntime "github.com/viant/datly/runtime"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
)

func TestBuilderTargetedWarmupRoundtripAndNativeReplay(t *testing.T) {
	for _, rootPlan := range []bool{false, true} {
		t.Run(map[bool]string{false: "child-only", true: "root-and-child"}[rootPlan], func(t *testing.T) {
			source := `#setting($_ = $route('/records','GET'))
SELECT r.*, c.*, use_cache(c,'children'),cache_warmup(c,'policy')
FROM records r LEFT JOIN children c ON c.parent_id=r.ID`
			if rootPlan {
				source = strings.Replace(source, "SELECT r.*", "SELECT r.*,use_cache(r,'shared'),cache_warmup(r,'','Limit=0')", 1)
			}
			service := rootCacheWarmupService()
			args := []string{"c", "'parent_id'", "'Limit=0'"}
			response := service.Apply(t.Context(), rb.Request{DQL: source, Operation: rb.Operation{Type: rb.OperationUpdateFunction, Function: &rb.FunctionMutation{Name: "cache_warmup", Occurrence: map[bool]int{false: 0, true: 1}[rootPlan], ExpectedArgs: []string{"c", "'policy'"}, Args: args}}})
			require.True(t, response.Applied, "%+v", response.Diagnostics)
			require.NotContains(t, response.DQL, "$cache(")
			require.Contains(t, response.DQL, "use_cache(c,'children')")
			inspected := service.Apply(t.Context(), rb.Request{DQL: response.DQL, Operation: rb.Operation{Type: rb.OperationInspect}})
			require.Empty(t, inspected.Diagnostics)
			require.Equal(t, response.Structure.Component, inspected.Structure.Component)
			// Occurrence/expectedArgs checks still protect updates against stale source.
			rejected := service.Apply(t.Context(), rb.Request{DQL: response.DQL, Operation: rb.Operation{Type: rb.OperationRemoveFunction, Function: &rb.FunctionMutation{Name: "cache_warmup", Occurrence: map[bool]int{false: 0, true: 1}[rootPlan], ExpectedArgs: []string{"c", "'policy'"}}}})
			require.False(t, rejected.Applied)
			removed := service.Apply(t.Context(), rb.Request{DQL: response.DQL, Operation: rb.Operation{Type: rb.OperationRemoveFunction, Function: &rb.FunctionMutation{Name: "cache_warmup", Occurrence: map[bool]int{false: 0, true: 1}[rootPlan], ExpectedArgs: args}}})
			require.True(t, removed.Applied, "%+v", removed.Diagnostics)
			require.False(t, removed.Structure.Component.RootView.Relations[0].View.Source.Bindings.HasInlineWarmups())
			added := service.Apply(t.Context(), rb.Request{DQL: removed.DQL, Operation: rb.Operation{Type: rb.OperationAddFunction, Function: &rb.FunctionMutation{Name: "cache_warmup", Args: args}}})
			require.True(t, added.Applied, "%+v", added.Diagnostics)
			require.Equal(t, inspected.Structure.Component, added.Structure.Component)
			component := inspected.Structure.Component
			db := sqlite.New(t)
			require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE records(ID INTEGER)", "CREATE TABLE children(id INTEGER,parent_id INTEGER)", "INSERT INTO records VALUES(1)", "INSERT INTO children VALUES(11,1)"))
			host := map[string]*spec.CacheSettings{
				"shared":   {Enabled: true, Location: t.TempDir(), TTL: "1m"},
				"children": {Enabled: true, Location: t.TempDir(), TTL: "1m"},
				"policy":   {Warmup: &spec.CacheWarmupSettings{Name: "host-untouched", IndexColumn: "id"}},
			}
			before, err := json.Marshal(host)
			require.NoError(t, err)
			artifact := rootCacheWarmupArtifact(t, component)
			execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}, CacheSettings: host})
			require.NoError(t, err)
			targets := execution.(dexec.ReaderWarmupTargeter).WarmupTargets()
			expected := 1
			if rootPlan {
				expected = 2
			}
			require.Len(t, targets, expected)
			for _, target := range targets {
				require.Equal(t, "inline", target.Origin)
				require.Empty(t, target.PolicyRef)
				require.NotNil(t, target.Settings.Limit)
				require.Zero(t, *target.Settings.Limit)
				require.Nil(t, target.Settings.MaxCases)
				require.Len(t, target.Work, 1)
			}
			require.Equal(t, "c", targets[len(targets)-1].View)
			runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[rootCacheWarmupOutput](), Reader: execution}})
			require.NoError(t, err)
			count, err := runtime.Warmup(t.Context(), dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/records"}})
			require.NoError(t, err)
			require.Equal(t, expected, count)
			// Drop warmed tables before the first ordinary read: replay must use the
			// native populated entries, rather than a previously cached exact request.
			require.NoError(t, db.ExecStatements(t.Context(), "DROP TABLE children"))
			if rootPlan {
				require.NoError(t, db.ExecStatements(t.Context(), "DROP TABLE records"))
			}
			value, err := execution.Read(t.Context(), &struct{}{}, nil, nil)
			require.NoError(t, err)
			rows := value.(*rootCacheWarmupOutput).Rows
			require.Len(t, rows, 1)
			require.Equal(t, 1, rows[0].ID)
			require.Len(t, rows[0].C, 1)
			require.Equal(t, 11, rows[0].C[0].ID)
			after, err := json.Marshal(host)
			require.NoError(t, err)
			require.JSONEq(t, string(before), string(after))
		})
	}
}

type targetedCasesInput struct{ Period string }

type targetedNestedChild struct {
	ID       int                     `sqlx:"id"`
	ParentID int                     `sqlx:"parent_id"`
	G        []*rootCacheWarmupChild `sqlx:"-"`
}
type targetedNestedRow struct {
	ID int                     `sqlx:"ID"`
	C  []*targetedNestedChild  `sqlx:"-"`
	H  []*rootCacheWarmupChild `sqlx:"-"`
}
type targetedNestedOutput struct{ Rows []*targetedNestedRow }

func TestTargetedNestedAndHostWarmupsCoexistAndReplay(t *testing.T) {
	source := `#setting($_ = $route('/records','GET'))
#setting($_ = $cache_warmup_cases('recent','Period=lifetime','ExcludeDefault=Period'))
#define($_ = $Period<string>(query/period))
SELECT r.*,c.*,g.*,h.*,
use_cache(r,'shared'),cache_warmup(r,'','Limit=0'),
use_cache(c,'children'),cache_warmup(c,'parent_id','CaseRefs=recent','Limit=0'),
use_cache(g,'children'),cache_warmup(g,'parent_id','CaseRefs=recent','Limit=0'),
use_cache(h,'children'),cache_warmup(h,'host-policy')
FROM records r
LEFT JOIN children c ON c.parent_id=r.ID
LEFT JOIN grandchildren g ON g.parent_id=c.id
LEFT JOIN others h ON h.parent_id=r.ID`
	response := rootCacheWarmupService().Apply(t.Context(), rb.Request{DQL: source, Operation: rb.Operation{Type: rb.OperationInspect}})
	require.Empty(t, response.Diagnostics)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE records(ID INTEGER)", "INSERT INTO records VALUES(1)", "CREATE TABLE children(id INTEGER,parent_id INTEGER)", "INSERT INTO children VALUES(11,1)", "CREATE TABLE grandchildren(id INTEGER,parent_id INTEGER)", "INSERT INTO grandchildren VALUES(111,11)", "CREATE TABLE others(id INTEGER,parent_id INTEGER)", "INSERT INTO others VALUES(21,1)"))
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: response.Structure.Component, InputType: reflect.TypeFor[targetedCasesInput](), OutputType: reflect.TypeFor[targetedNestedOutput](), DirectViewField: "Rows"})
	require.NoError(t, err)
	host := map[string]*spec.CacheSettings{"shared": {Enabled: true, Location: t.TempDir(), TTL: "1m"}, "children": {Enabled: true, Location: t.TempDir(), TTL: "1m"}, "host-policy": {Warmup: &spec.CacheWarmupSettings{Name: "host", IndexColumn: "parent_id"}}}
	before, err := json.Marshal(host)
	require.NoError(t, err)
	execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}, CacheSettings: host})
	require.NoError(t, err)
	targets := execution.(dexec.ReaderWarmupTargeter).WarmupTargets()
	require.Len(t, targets, 4)
	origins := map[string]int{}
	for _, target := range targets {
		origins[target.Origin]++
		require.Len(t, target.Work, 1)
		if target.Origin == "host" {
			require.Equal(t, "host-policy", target.PolicyRef)
		} else {
			require.Empty(t, target.PolicyRef)
		}
	}
	require.Equal(t, map[string]int{"inline": 3, "host": 1}, origins)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[targetedNestedOutput](), Reader: execution}})
	require.NoError(t, err)
	count, err := runtime.Warmup(t.Context(), dexec.ComponentTarget{Component: artifact.Component.Key, Route: spec.RouteRef{Method: "GET", Path: "/records"}})
	require.NoError(t, err)
	require.Equal(t, 4, count)
	require.NoError(t, db.ExecStatements(t.Context(), "DROP TABLE records", "DROP TABLE children", "DROP TABLE grandchildren", "DROP TABLE others"))
	value, err := execution.Read(t.Context(), &targetedCasesInput{Period: "lifetime"}, nil, nil)
	require.NoError(t, err)
	rows := value.(*targetedNestedOutput).Rows
	require.Len(t, rows, 1)
	require.Len(t, rows[0].C, 1)
	require.Len(t, rows[0].C[0].G, 1)
	require.Equal(t, 111, rows[0].C[0].G[0].ID)
	require.Len(t, rows[0].H, 1)
	require.Equal(t, 21, rows[0].H[0].ID)
	after, err := json.Marshal(host)
	require.NoError(t, err)
	require.JSONEq(t, string(before), string(after))
}

func TestTargetedWarmupComponentCasesAndScope(t *testing.T) {
	const source = `#setting($_ = $route('/records','GET'))
#setting($_ = $cache_warmup_cases('recent','Period=lifetime','ExcludeDefault=Period'))
#define($_ = $Period<string>(query/period))
SELECT r.*, c.*,use_cache(c,'children'),cache_warmup(c,'parent_id','CaseRefs=recent','Limit=0')
FROM records r LEFT JOIN children c ON c.parent_id=r.ID`
	service := rootCacheWarmupService()
	response := service.Apply(t.Context(), rb.Request{DQL: source, Operation: rb.Operation{Type: rb.OperationInspect}})
	require.Empty(t, response.Diagnostics)
	component := response.Structure.Component
	require.False(t, component.Settings.Cache.Enabled, "case definitions alone must not activate root storage")
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeFor[targetedCasesInput](), OutputType: reflect.TypeFor[rootCacheWarmupOutput](), DirectViewField: "Rows"})
	require.NoError(t, err)
	db := sqlite.New(t)
	require.NoError(t, db.ExecStatements(t.Context(), "CREATE TABLE records(ID INTEGER)", "CREATE TABLE children(id INTEGER,parent_id INTEGER)", "INSERT INTO records VALUES(1)", "INSERT INTO children VALUES(11,1)"))
	execution, err := artifact.ReaderCompilation().NewExecution(bootstrap.ReaderRuntimeConfig{SQL: &dsql.SQLComponent{DB: db.DB}, CacheSettings: map[string]*spec.CacheSettings{"children": {Enabled: true, Location: t.TempDir(), TTL: "1m"}}})
	require.NoError(t, err)
	targets := execution.(dexec.ReaderWarmupTargeter).WarmupTargets()
	require.Len(t, targets, 1)
	require.Equal(t, "c", targets[0].View)
	policy := targets[0].Settings
	require.NotNil(t, policy.Limit)
	require.Zero(t, *policy.Limit)
	require.Nil(t, policy.MaxCases)
	require.Equal(t, []string{"lifetime"}, policy.Cases[0].Set[0].Values)
	require.True(t, policy.Cases[0].Set[0].ExcludeDefault)
	targets[0].Settings.Cases[0].Set[0].Values[0] = "changed"
	require.Equal(t, "lifetime", execution.(dexec.ReaderWarmupTargeter).WarmupTargets()[0].Settings.Cases[0].Set[0].Values[0])
	require.Equal(t, []string{"recent"}, component.RootView.Relations[0].View.Source.Bindings.InlineWarmups[0].CaseRefs)
	runtime, err := druntime.NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeFor[rootCacheWarmupOutput](), Reader: execution}})
	require.NoError(t, err)
	count, err := runtime.Warmup(t.Context(), dexec.ComponentTarget{Component: component.Key, Route: spec.RouteRef{Method: "GET", Path: "/records"}})
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.NoError(t, db.ExecStatements(t.Context(), "DROP TABLE children"))
	value, err := execution.Read(t.Context(), &targetedCasesInput{Period: "lifetime"}, nil, nil)
	require.NoError(t, err)
	require.Len(t, value.(*rootCacheWarmupOutput).Rows[0].C, 1)
	missing := service.Apply(t.Context(), rb.Request{DQL: strings.Replace(source, "CaseRefs=recent", "CaseRefs=host-only", 1), Operation: rb.Operation{Type: rb.OperationInspect}})
	require.NotEmpty(t, missing.Diagnostics, "inline case references must not search host scope")
}
