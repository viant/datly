package generate

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

// ValidateLifecycleTarget checks dispatch ownership before artifact generation.
// Intermediate shape planning may precede mutation lowering; emitting an
// unsupported target must never silently retain an inert lifecycle declaration.
func (input *Input) ValidateLifecycleTarget(mutation bool) error {
	return input.validateLifecycleTarget(mutation, false)
}

func (input *Input) validateLifecycleTarget(mutation, pendingDiscovery bool) error {
	if input == nil || input.Component == nil {
		return nil
	}
	// Shape planning precedes explicit operation selection. An unset policy
	// may stage a declared DELETE graph; final emission still validates the
	// selected generated target and patch/put policy.
	policy := ""
	if input.Component.Settings != nil {
		policy = input.Component.Settings.Mutation
	}
	deleteRouteSupported := mutation && (policy == "" || policy == "patch" || policy == "put") && HasWritableDeleteMarker(input.Component.RootView)
	supported := mutation
	for _, route := range input.Component.Routes {
		if route != nil && !strings.EqualFold(route.Method, "POST") && !strings.EqualFold(route.Method, "PUT") && !strings.EqualFold(route.Method, "PATCH") && !(strings.EqualFold(route.Method, "DELETE") && deleteRouteSupported) {
			supported = false
		}
	}
	predicateSupported := mutation
	for _, route := range input.Component.Routes {
		if route != nil && !strings.EqualFold(route.Method, "PATCH") && !strings.EqualFold(route.Method, "PUT") && !(strings.EqualFold(route.Method, "DELETE") && deleteRouteSupported) {
			predicateSupported = false
		}
	}
	mutationViews := map[*spec.View]bool{}
	var markMutationViews func(*spec.View)
	markMutationViews = func(view *spec.View) {
		if view == nil || mutationViews[view] {
			return
		}
		mutationViews[view] = true
		for _, rel := range view.Relations {
			if rel != nil && rel.Kind != spec.RelationKindDerived {
				markMutationViews(rel.View)
			}
		}
	}
	markMutationViews(input.Component.RootView)
	visited := map[*spec.View]bool{}
	var check func(*spec.View) error
	check = func(view *spec.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		if view.NestedNullPolicy != "" {
			if view == input.Component.RootView || !supported || view.Cardinality == spec.CardinalityOne || view.NestedNullPolicy == "skip-auxiliary" && !mutationViews[view] || !((view.NestedNullPolicy == "initial-validation" && !view.Auxiliary) || (view.NestedNullPolicy == "skip-auxiliary" && (view.Auxiliary || pendingDiscovery && pendingAuxiliarySource(view)))) {
				return fmt.Errorf("nested_null_policy requires a generated writable collection relation and initial-validation")
			}
		}
		if view.RootNullPolicy != "" {
			if view != input.Component.RootView || !supported || view.Cardinality == spec.CardinalityOne || !((view.RootNullPolicy == "initial-validation" && !view.Auxiliary) || (view.RootNullPolicy == "skip-auxiliary" && (view.Auxiliary || pendingDiscovery && pendingAuxiliarySource(view)))) {
				return fmt.Errorf("root_null_policy requires a generated writable root and initial-validation")
			}
		}
		if view.WriterIdentityPolicy != "" {
			if view.WriterIdentityPolicy != "assigned-update" {
				return fmt.Errorf("writer_identity must be assigned-update")
			}
			if !mutation || view.Auxiliary {
				return fmt.Errorf("writer_identity requires a generated PATCH writable view")
			}
			if policy != "" && policy != "patch" {
				return fmt.Errorf("writer_identity requires PATCH operation policy")
			}
			for _, route := range input.Component.Routes {
				if route != nil && !strings.EqualFold(route.Method, "PATCH") {
					return fmt.Errorf("writer_identity requires PATCH routes")
				}
			}
			if len(view.Relations) > 0 {
				return fmt.Errorf("writer_identity assigned-update requires a leaf view")
			}
		}
		if view.OnDeleteNotFound != "" {
			if !predicateSupported || view.Auxiliary {
				return fmt.Errorf("delete_not_found requires a generated PATCH/PUT writable view")
			}
			if view.OnDeleteNotFound != "error" && view.OnDeleteNotFound != "ignore" {
				return fmt.Errorf("delete_not_found must be error or ignore")
			}
			if view.OnDeleteNotFound == "ignore" && len(view.Relations) > 0 {
				return fmt.Errorf("delete_not_found(ignore) requires a leaf view; missing parents cannot suppress descendant checks")
			}
			found := false
			for _, column := range view.Columns {
				if column != nil && column.DeleteMarker {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("delete_not_found requires delete_marker")
			}
		}
		if view.MutationPredicateGroup != nil {
			if !predicateSupported {
				return fmt.Errorf("view %s: mutation_predicate requires a generated PATCH/PUT writer", view.Name)
			}
			if view.Auxiliary || *view.MutationPredicateGroup < 0 {
				return fmt.Errorf("view %s: mutation_predicate requires a writable view and non-negative group", view.Name)
			}
			found := false
			for _, param := range input.Component.Parameters {
				if param != nil {
					for _, predicate := range param.Predicates {
						if predicate != nil && predicate.Group == *view.MutationPredicateGroup {
							found = true
						}
					}
				}
			}
			if !found {
				return fmt.Errorf("view %s: mutation_predicate group %d has no predicate inputs", view.Name, *view.MutationPredicateGroup)
			}
		}
		for _, column := range view.Columns {
			if !supported && column != nil && (column.DeleteMarker || column.ConcurrencyToken) {
				return fmt.Errorf("view %s: mutation markers require the generated Go mutation lifecycle", view.Name)
			}
		}
		if !supported && strings.TrimSpace(view.EntityHooks) != "" {
			return fmt.Errorf("view %s: lifecycle_type(%s, %q) requires the generated Go mutation lifecycle; readers use input_type OrdersInput.Init and output_type OrdersOutput.Finalize, with row OnFetch separate", view.Name, view.Name, view.EntityHooks)
		}
		for _, relation := range view.Relations {
			if relation != nil {
				if err := check(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := check(input.Component.RootView); err != nil {
		return err
	}
	for _, view := range input.Component.Views {
		if err := check(view); err != nil {
			return err
		}
	}
	return nil
}

// HasWritableDeleteMarker addresses explicit graph policy, not transport verbs.
func HasWritableDeleteMarker(root *spec.View) bool {
	seen := map[*spec.View]bool{}
	var has func(*spec.View) bool
	has = func(view *spec.View) bool {
		if view == nil || seen[view] || view.Auxiliary {
			return false
		}
		seen[view] = true
		for _, column := range view.Columns {
			if column != nil && column.DeleteMarker {
				return true
			}
		}
		for _, relation := range view.Relations {
			if relation != nil && has(relation.View) {
				return true
			}
		}
		return false
	}
	return has(root)
}

// Discovery needs an input type before expanding named SQL. This permits only
// provisional shape materialization, never artifact emission or dispatch.
func pendingAuxiliarySource(view *spec.View) bool {
	return view != nil && view.Source != nil && strings.TrimSpace(view.Source.Table) == "" && (len(view.Source.Embeds) > 0 || strings.TrimSpace(view.Source.URI) != "")
}
