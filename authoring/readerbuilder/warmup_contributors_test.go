package readerbuilder_test

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	rb "github.com/viant/datly/authoring/readerbuilder"
	"github.com/viant/datly/spec"
)

const contributorDQL = `#setting($_ = $route('/records','GET'))
#setting($_ = $cache_warmup_cases('recent','Period=recent','ExcludeDefault=Period'))
#setting($_ = $cache_warmup('ID','Limit=77'))
#define($_ = $Period<string>(query/period).Optional())
SELECT r.*, c.*, use_cache(r,'shared'), cache_warmup(r,'','Limit=0'),
use_cache(c,'children'), cache_warmup(c,'policy'),
cache_warmup(c,'parent_id','Name=by_parent','Limit=0','CaseRefs=recent','Period=older','ExcludeDefault=Period'),
cache_warmup(c,'parent_id','Name=by_parent','Limit=0','Period=oldest'),
cache_warmup(c,'parent_id','Name=by_parent','Limit=0','Period=oldest'),
cache_warmup(c,'ID','Name=by_id')
FROM records r LEFT JOIN children c ON c.parent_id=r.ID`

func TestTargetedWarmupContributorsRoundtripAndBatchAtomicity(t *testing.T) {
	service := rootCacheWarmupService()
	inspect := func(source string) rb.Response {
		t.Helper()
		response := service.Apply(t.Context(), rb.Request{DQL: source, Operation: rb.Operation{Type: rb.OperationInspect}})
		require.Empty(t, response.Diagnostics)
		return response
	}
	response := inspect(contributorDQL)
	plans := response.Structure.TargetedWarmups
	require.Len(t, plans, 3)
	require.Equal(t, "r", plans[0].View)
	require.Equal(t, 0, plans[0].Contributors[0].Occurrence)
	plan := plans[1]
	require.Equal(t, "c", plan.View)
	require.Equal(t, []string{"recent"}, plan.Settings.CaseRefs)
	require.Len(t, plan.Settings.Cases, 3) // Shared cases must stay unexpanded.
	require.Len(t, plan.Contributors, 3)
	require.Equal(t, 2, plan.Contributors[0].Occurrence) // Host call participates in numbering.
	require.Equal(t, 3, plan.Contributors[1].Occurrence)
	require.Equal(t, 4, plan.Contributors[2].Occurrence)
	require.Equal(t, plan.Contributors[1].Args, plan.Contributors[2].Args)
	require.NotEqual(t, plan.Contributors[1].SourceSpan, plan.Contributors[2].SourceSpan)
	require.Equal(t, 5, plans[2].Contributors[0].Occurrence)
	require.True(t, plan.Editable)
	require.Empty(t, plan.Diagnostic)
	require.Nil(t, plan.Settings.MaxCases)
	require.NotNil(t, plan.Settings.Limit)
	require.Zero(t, *plan.Settings.Limit)
	require.Nil(t, plans[2].Settings.MaxCases)
	require.Nil(t, plans[2].Settings.Limit)
	child := response.Structure.Component.RootView.Relations[0].View
	require.Equal(t, child.Source.Bindings.InlineWarmups[0], plan.Settings)
	for _, call := range plan.Contributors {
		require.Equal(t, "cache_warmup", call.Name)
		require.Contains(t, contributorDQL[call.SourceSpan.Start:call.SourceSpan.End], "cache_warmup(c,")
	}
	operations := []rb.Operation{}
	for _, call := range plan.Contributors {
		args := append([]string(nil), call.Args...)
		for i, arg := range args {
			if arg == "'Name=by_parent'" {
				args[i] = "'Name=renamed_parent'"
			}
		}
		operations = append(operations, rb.Operation{Type: rb.OperationUpdateFunction, Function: &rb.FunctionMutation{Name: call.Name, Occurrence: call.Occurrence, ExpectedArgs: call.Args, Args: args}})
	}
	// The second operation fails after the first has already changed the candidate.
	invalid := append([]rb.Operation(nil), operations...)
	bad := *invalid[1].Function
	bad.ExpectedArgs = append([]string(nil), bad.ExpectedArgs...)
	bad.ExpectedArgs[0] = "other"
	invalid[1].Function = &bad
	rejected := service.Apply(t.Context(), rb.Request{DQL: contributorDQL, Operation: rb.Operation{Type: rb.OperationBatch, Operations: invalid}})
	require.False(t, rejected.Applied)
	require.Equal(t, contributorDQL, rejected.DQL)
	require.Equal(t, response.Structure.TargetedWarmups, rejected.Structure.TargetedWarmups)

	updated := service.Apply(t.Context(), rb.Request{DQL: contributorDQL, Operation: rb.Operation{Type: rb.OperationBatch, Operations: operations}})
	require.True(t, updated.Applied, "%+v", updated.Diagnostics)
	require.Empty(t, updated.Diagnostics)
	replay := inspect(updated.DQL)
	require.Equal(t, updated.Structure.TargetedWarmups, replay.Structure.TargetedWarmups)
	require.Len(t, replay.Structure.TargetedWarmups[1].Contributors, len(operations))
	for i, call := range replay.Structure.TargetedWarmups[1].Contributors {
		require.Equal(t, operations[i].Function.Args, call.Args, "preserve each authored definition, including duplicate case groups")
	}
	want := plan.Settings.Clone()
	want.Name = "renamed_parent"
	require.Equal(t, want, replay.Structure.TargetedWarmups[1].Settings)
	require.True(t, reflect.DeepEqual(plans[0].Settings, replay.Structure.TargetedWarmups[0].Settings))
	require.True(t, reflect.DeepEqual(plans[2].Settings, replay.Structure.TargetedWarmups[2].Settings))
	calls := append([]rb.FunctionOccurrence(nil), replay.Structure.TargetedWarmups[1].Contributors...)
	sort.Slice(calls, func(i, j int) bool { return calls[i].Occurrence > calls[j].Occurrence })
	operations = nil
	for _, call := range calls {
		operations = append(operations, rb.Operation{Type: rb.OperationRemoveFunction, Function: &rb.FunctionMutation{Name: call.Name, Occurrence: call.Occurrence, ExpectedArgs: call.Args}})
	}
	removed := service.Apply(t.Context(), rb.Request{DQL: updated.DQL, Operation: rb.Operation{Type: rb.OperationBatch, Operations: operations}})
	require.True(t, removed.Applied, "%+v", removed.Diagnostics)
	require.Len(t, removed.Structure.TargetedWarmups, 2)
	require.Contains(t, removed.DQL, "$cache_warmup_cases('recent'")
	require.Contains(t, removed.DQL, "$cache_warmup('ID','Limit=77')")
	require.Contains(t, removed.DQL, "cache_warmup(c,'policy')")
	require.Contains(t, removed.DQL, "use_cache(c,'children')")
	require.NotContains(t, removed.DQL, "Name=by_parent")
	require.NotContains(t, removed.DQL, "Name=renamed_parent")
	require.Equal(t, 2, removed.Structure.TargetedWarmups[1].Contributors[0].Occurrence)
	require.False(t, strings.Contains(removed.DQL, "Period=older"))
}

func TestTargetedWarmupIncompleteProvenanceRemainsReadOnly(t *testing.T) {
	for _, calls := range []string{
		"`cache_warmup`(r,'ID','Limit=0')",
		"cache_warmup(r,'ID','Limit=0') AS warmup",
		"cache_warmup(r,'ID','Limit=0') warmup",
		"cache_warmup(r,'ID','Period=older'), `cache_warmup`(r,'ID','Period=oldest')",
		"cache_warmup(r,'ID','Period=older'), cache_warmup(r,'ID','Period=oldest') AS warmup",
		// The omitted contributor may not change the merged settings at all.
		"cache_warmup(r,'ID','Limit=0'), `cache_warmup`(r,'ID','Limit=0')",
		"cache_warmup(r,'ID','Limit=0'), cache_warmup(r,'ID','Limit=0') AS warmup",
	} {
		t.Run(calls, func(t *testing.T) {
			source := "#setting($_ = $route('/records','GET'))\n#define($_ = $Period<string>(query/period).Optional())\nSELECT r.*, c.*, " + calls + ", cache_warmup(c,'ID','Limit=0') FROM records r LEFT JOIN children c ON c.parent_id=r.ID"
			service := rootCacheWarmupService()
			response := service.Apply(t.Context(), rb.Request{DQL: source, Operation: rb.Operation{Type: rb.OperationInspect}})
			require.Empty(t, response.Diagnostics, "provenance availability must not reject native compilation")
			require.Equal(t, "complete", response.Structure.Status)
			require.Len(t, response.Structure.TargetedWarmups, 2)
			plan := response.Structure.TargetedWarmups[0]
			require.Equal(t, response.Structure.Component.RootView.Source.Bindings.InlineWarmups[0], plan.Settings)
			require.False(t, plan.Editable)
			require.NotEmpty(t, plan.Diagnostic)
			require.Empty(t, plan.Contributors, "never publish a misleading partial removal recipe")
			wire, err := json.Marshal(plan)
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(wire, &fields))
			require.JSONEq(t, "[]", string(fields["contributors"]))
			require.Equal(t, "false", string(fields["editable"]))
			require.NotEmpty(t, fields["diagnostic"])
			plan.Settings.IndexColumn = "mutated"
			require.Equal(t, "ID", response.Structure.Component.RootView.Source.Bindings.InlineWarmups[0].IndexColumn, "inspection metadata is detached")
			require.True(t, response.Structure.TargetedWarmups[1].Editable, "unrelated view remains editable")
			// Other existing guarded edits still work on this valid component.
			updated := service.Apply(t.Context(), rb.Request{DQL: source, Operation: rb.Operation{Type: rb.OperationAddFunction, Function: &rb.FunctionMutation{Name: "set_limit", Args: []string{"r", "10"}}}})
			require.True(t, updated.Applied, "%+v", updated.Diagnostics)
			require.False(t, updated.Structure.TargetedWarmups[0].Editable)
		})
	}
}

func TestTargetedWarmupIdenticalPoliciesOnDifferentViewsStaySeparate(t *testing.T) {
	source := `#setting($_ = $route('/records','GET'))
SELECT r.*,c.*,cache_warmup(r,'ID','Name=common'),cache_warmup(c,'ID','Name=common'),cache_warmup(c,'ID','Name=common')
FROM records r LEFT JOIN children c ON c.parent_id=r.ID`
	service := rootCacheWarmupService()
	response := service.Apply(t.Context(), rb.Request{DQL: source, Operation: rb.Operation{Type: rb.OperationInspect}})
	require.Empty(t, response.Diagnostics)
	plans := response.Structure.TargetedWarmups
	require.Len(t, plans, 2)
	require.Equal(t, "r", plans[0].View)
	require.Equal(t, "c", plans[1].View)
	require.Equal(t, plans[0].Settings, plans[1].Settings)
	require.Len(t, plans[0].Contributors, 1)
	require.Len(t, plans[1].Contributors, 2)
	require.Equal(t, plans[1].Contributors[0].Args, plans[1].Contributors[1].Args)
	require.Equal(t, 1, plans[1].Contributors[0].Occurrence)
	require.Equal(t, 2, plans[1].Contributors[1].Occurrence)
	var operations []rb.Operation
	for _, call := range plans[1].Contributors {
		require.True(t, plans[1].Editable)
		args := append([]string(nil), call.Args...)
		args[2] = "'Name=child'"
		operations = append(operations, rb.Operation{Type: rb.OperationUpdateFunction, Function: &rb.FunctionMutation{Name: call.Name, Occurrence: call.Occurrence, ExpectedArgs: call.Args, Args: args}})
	}
	updated := service.Apply(t.Context(), rb.Request{DQL: source, Operation: rb.Operation{Type: rb.OperationBatch, Operations: operations}})
	require.True(t, updated.Applied, "%+v", updated.Diagnostics)
	require.Equal(t, plans[0].Settings, updated.Structure.TargetedWarmups[0].Settings)
	require.Equal(t, &spec.CacheWarmupSettings{Name: "child", IndexColumn: "ID"}, updated.Structure.TargetedWarmups[1].Settings)
	// Occurrence identities still cannot detect swapping identical declarations;
	// source revision ownership remains with the caller, not this metadata.
}
