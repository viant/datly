package readerbuilder

import (
	"reflect"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
	sqltext "github.com/viant/sqlparser/source"
)

const incompleteWarmupContributors = "Warmup contributors are not completely represented by the existing guarded outer-projection function operations."

// Compiled collections are authoritative. Never expose a partial source
// projection as the complete editable definition of a native plan.
func inspectTargetedWarmups(source string, component *spec.Component, functions []FunctionOccurrence) []TargetedWarmupPlan {
	if component == nil {
		return nil
	}
	groups, groupErr := groupEditableWarmupCalls(functions)
	byView := map[string][]TargetedWarmupPlan{}
	for _, group := range groups {
		key := strings.ToLower(group.View)
		byView[key] = append(byView[key], group)
	}
	uneditable, unavailable := uneditableWarmupViews(source, functions)
	var result []TargetedWarmupPlan
	seen := map[*spec.View]bool{}
	var visit func(*spec.View)
	visit = func(view *spec.View) {
		if view == nil || seen[view] {
			return
		}
		seen[view] = true
		if view.Source != nil && view.Source.Bindings.HasInlineWarmups() {
			key := strings.ToLower(strings.TrimSpace(view.Namespace))
			var policies []*spec.CacheWarmupSettings
			for _, policy := range view.Source.Bindings.InlineWarmups {
				if policy != nil {
					policies = append(policies, policy)
				}
			}
			candidates := byView[key]
			complete := groupErr == nil && !unavailable && !uneditable[key] && len(candidates) == len(policies)
			if complete {
				for i, policy := range policies {
					if !reflect.DeepEqual(policy, candidates[i].Settings) {
						complete = false
						break
					}
				}
			}
			for i, policy := range policies {
				plan := TargetedWarmupPlan{View: view.Namespace, Settings: policy.Clone(), Editable: complete, Contributors: []FunctionOccurrence{}}
				if complete {
					plan.Contributors = candidates[i].Contributors
				} else {
					plan.Diagnostic = incompleteWarmupContributors
				}
				result = append(result, plan)
			}
		}
		for _, relation := range view.Relations {
			if relation != nil {
				visit(relation.View)
			}
		}
	}
	visit(component.RootView)
	return result
}

// The native SQL parser sees aliases and quoted function names that the guarded
// function editor intentionally does not support. Even an identical omitted
// declaration must make provenance read-only; settings equality alone cannot
// establish that every contributing occurrence was accounted for.
func uneditableWarmupViews(source string, functions []FunctionOccurrence) (map[string]bool, bool) {
	result := map[string]bool{}
	projection, err := outerProjection(source)
	if err != nil {
		return result, true
	}
	editable := map[dql.SourceSpan]bool{}
	for _, call := range functions {
		if strings.EqualFold(call.Name, "cache_warmup") && len(call.Args) >= 3 {
			editable[call.SourceSpan] = true
		}
	}
	for _, span := range projection.items {
		// Do not mask source expressions here: an opaque/template projection
		// cannot establish complete occurrence coverage for guarded edits.
		parsed, err := sqlparser.ParseQuery("SELECT " + source[span.Start:span.End] + " FROM provenance_source")
		if err != nil || parsed == nil || len(parsed.List) != 1 {
			return result, true
		}
		call, ok := parsed.List[0].Expr.(*expr.Call)
		if !ok || call.X == nil || len(call.Args) < 3 {
			continue
		}
		name := strings.ToLower(strings.Trim(strings.TrimSpace(sqlparser.Stringify(call.X)), "`\"'"))
		if name != spec.ViewControlCacheWarmup {
			continue
		}
		if !editable[span] {
			key := strings.ToLower(strings.TrimSpace(sqlparser.Stringify(call.Args[0])))
			result[key] = true
		}
	}
	return result, false
}

// Only the existing editable outer-projection function occurrences are exposed.
// No function occurrence is synthesized from compiled/effective settings.
func groupEditableWarmupCalls(functions []FunctionOccurrence) ([]TargetedWarmupPlan, error) {
	type target struct {
		view  string
		calls []FunctionOccurrence
		args  [][]string
	}
	var targets []*target
	byView := map[string]*target{}
	for _, call := range functions {
		if !strings.EqualFold(call.Name, "cache_warmup") || len(call.Args) < 3 {
			continue
		}
		view := strings.TrimSpace(sqltext.TrimQuote(call.Args[0]))
		key := strings.ToLower(view)
		entry := byView[key]
		if entry == nil {
			entry = &target{view: view}
			byView[key] = entry
			targets = append(targets, entry)
		}
		call.Args = append([]string(nil), call.Args...)
		entry.calls = append(entry.calls, call)
		entry.args = append(entry.args, call.Args[1:])
	}
	var result []TargetedWarmupPlan
	for _, entry := range targets {
		groups, err := dql.GroupCacheWarmupDeclarations(entry.args)
		if err != nil {
			return nil, err
		}
		for _, group := range groups {
			plan := TargetedWarmupPlan{View: entry.view, Settings: group.Settings}
			for _, index := range group.Declarations {
				plan.Contributors = append(plan.Contributors, entry.calls[index])
			}
			result = append(result, plan)
		}
	}
	return result, nil
}
