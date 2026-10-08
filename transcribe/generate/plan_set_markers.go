package generate

import (
	"fmt"
	"sort"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

// applySetMarkerViews adds SQLX presence metadata only to generated view
// types selected by the target-neutral mutation plan. Package-linked types remain
// under package authority.
func (r *planResolver) applySetMarkerViews() error {
	if len(r.input.SetMarkerViews) == 0 {
		return nil
	}
	canonical, err := r.canonicalViewIndex()
	if err != nil {
		return err
	}
	borrowedEdges, err := r.borrowedAncestorPresenceEdges()
	if err != nil {
		return err
	}
	planned := make(map[string]*ViewPlan, len(r.plan.Views))
	linked := map[string]bool{}
	var markLinked func(*spec.View) error
	markLinked = func(view *spec.View) error {
		if view == nil {
			return nil
		}
		identity, identityErr := view.Identity()
		if identityErr != nil {
			return identityErr
		}
		if linked[identity] {
			return nil
		}
		linked[identity] = true
		for _, relation := range view.Relations {
			if relation != nil {
				if err := markLinked(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for index := range r.plan.Views {
		view := &r.plan.Views[index]
		if view.Identity != "" {
			planned[view.Identity] = view
		}
		if view.Ownership == ViewLinked {
			if err := markLinked(canonical[view.Identity]); err != nil {
				return err
			}
		}
	}
	identities := make([]string, 0, len(r.input.SetMarkerViews))
	for identity, enabled := range r.input.SetMarkerViews {
		if enabled {
			identities = append(identities, identity)
		}
	}
	sort.Strings(identities)
	for _, identity := range identities {
		view := canonical[identity]
		if view == nil {
			return fmt.Errorf("set-marker view %q is not canonical", identity)
		}
		viewPlan := planned[identity]
		if viewPlan == nil {
			if linked[identity] {
				continue
			}
			return fmt.Errorf("set-marker view %q has no generation plan", identity)
		}
		if viewPlan.Ownership == ViewLinked {
			continue
		}
		if err := addViewSetMarkerWithBorrowedEdges(viewPlan, view, borrowedEdges); err != nil {
			return err
		}
	}
	return nil
}

func (r *planResolver) canonicalViewIndex() (map[string]*spec.View, error) {
	result := map[string]*spec.View{}
	visited := map[*spec.View]bool{}
	var add func(*spec.View) error
	add = func(view *spec.View) error {
		if view == nil || visited[view] {
			return nil
		}
		visited[view] = true
		identity, err := view.Identity()
		if err != nil {
			return err
		}
		if previous := result[identity]; previous != nil && previous != view {
			return fmt.Errorf("canonical view identity %q is duplicated", identity)
		}
		result[identity] = view
		for _, relation := range view.Relations {
			if relation != nil {
				if err = add(relation.View); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if r.input.Component != nil {
		if err := add(r.input.Component.RootView); err != nil {
			return nil, err
		}
		for _, view := range r.input.Component.Views {
			if err := add(view); err != nil {
				return nil, err
			}
		}
	}
	return result, nil
}

func addViewSetMarker(plan *ViewPlan, view *spec.View) error {
	return addViewSetMarkerWithBorrowedEdges(plan, view, nil)
}

func addViewSetMarkerWithBorrowedEdges(plan *ViewPlan, view *spec.View, borrowedEdges map[borrowedPresenceEdge]bool) error {
	if plan == nil || view == nil {
		return fmt.Errorf("generated set-marker view is required")
	}
	fields := make(map[string]Field, len(plan.Fields))
	for _, field := range plan.Fields {
		fields[field.Name] = field
	}
	if _, ok := fields["Has"]; ok {
		return fmt.Errorf("generated view %q set marker collides with field Has", plan.Name)
	}
	markerFields := make([]string, 0, len(view.Columns))
	for _, column := range view.Columns {
		if column == nil {
			continue
		}
		name := typecatalog.FieldName(column.Name)
		_, ok := fields[name]
		if !ok {
			return fmt.Errorf("generated view %q set marker column %q has no field", plan.Name, column.Name)
		}
		// Scalar presence is independent of persistence and transport visibility.
		markerFields = append(markerFields, name)
	}
	for _, relation := range view.Relations {
		if relation == nil {
			continue
		}
		if relation.Kind == spec.RelationKindDerived {
			continue
		}
		holder := relation.Holder
		if holder == "" {
			holder = relation.Name
		}
		name := typecatalog.FieldName(holder)
		if relation.View != nil && relation.View.Auxiliary {
			if len(borrowedEdges) == 0 {
				continue
			}
			parent, err := view.Identity()
			if err != nil {
				return err
			}
			child, err := relation.View.Identity()
			if err != nil {
				return err
			}
			if !borrowedEdges[borrowedPresenceEdge{parent, name, child}] {
				continue
			}
		}
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("generated view %q relation marker %q has no field", plan.Name, name)
		}
		found := false
		for _, existing := range markerFields {
			if existing == name {
				found = true
				break
			}
		}
		if !found {
			markerFields = append(markerFields, name)
		}
	}
	if view.SelfReference != nil {
		name := typecatalog.FieldName(view.SelfReference.Holder)
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("generated view %q self marker %q has no field", plan.Name, name)
		}
		markerFields = append(markerFields, name)
	}
	if len(markerFields) == 0 {
		return fmt.Errorf("generated view %q set marker has no SQLX fields", plan.Name)
	}
	markerType := plan.Name + "Has"
	plan.Fields = append(plan.Fields, Field{
		Name: "Has", Type: "*" + markerType,
		Tag: `setMarker:"true" format:"-" sqlx:"-" diff:"-" json:"-" typeName:"` + markerType + `"`,
	})
	plan.SetMarkerFields = markerFields
	return nil
}

// This local key includes the generated holder as well as both canonical roles.
type borrowedPresenceEdge struct{ parent, holder, child string }

// borrowedAncestorPresenceEdges independently checks selected target context
// and the complete admitted path at marker emission. Ordinary marker selection
// and writable siblings cannot activate auxiliary relation presence.
func (r *planResolver) borrowedAncestorPresenceEdges() (map[borrowedPresenceEdge]bool, error) {
	edges := map[borrowedPresenceEdge]bool{}
	if !r.input.nativeMutationTargetSelected {
		return edges, nil
	}
	c := r.input.Component
	identities := make([]string, 0, len(r.input.Views))
	for identity := range r.input.Views {
		identities = append(identities, identity)
	}
	sort.Strings(identities)
	for _, identity := range identities {
		ref := r.input.Views[identity]
		if ref == nil || ref.Borrowed == nil {
			continue
		}
		proof := ref.Borrowed
		if c == nil || c.RootView == nil {
			return nil, fmt.Errorf("borrowed ancestor presence requires a canonical body graph")
		}
		parts := strings.Split(proof.Declaration.BodyPath, "/")
		rootName := typecatalog.FieldName(c.Name)
		bodyCount := 0
		for _, p := range spec.EffectiveParameters(c.Parameters) {
			if p != nil && strings.EqualFold(p.Source.Kind, "body") {
				bodyCount++
				rootName = typecatalog.FieldName(p.Name)
			}
		}
		if bodyCount > 1 || parts[0] != rootName {
			return nil, fmt.Errorf("borrowed ancestor presence body path does not select the exact canonical body holder")
		}
		view := c.RootView
		id, err := view.Identity()
		if err != nil {
			return nil, err
		}
		graph := []string{id}
		seen := map[*spec.View]bool{view: true}
		pathEdges := make([]borrowedPresenceEdge, 0, len(parts)-1)
		excluded := view.SelfReference != nil
		for _, holder := range parts[1:] {
			var found *spec.Relation
			for _, relation := range view.Relations {
				if relation == nil || relation.View == nil {
					return nil, fmt.Errorf("borrowed ancestor presence body relation is incomplete")
				}
				name := relation.Holder
				if name == "" {
					name = relation.Name
				}
				if typecatalog.FieldName(name) == holder {
					if found != nil {
						return nil, fmt.Errorf("borrowed ancestor presence holder is ambiguous")
					}
					found = relation
				}
			}
			if found == nil || seen[found.View] {
				return nil, fmt.Errorf("borrowed ancestor presence holder is missing or cyclic")
			}
			excluded = excluded || found.Kind == spec.RelationKindDerived || found.View.SelfReference != nil
			view = found.View
			seen[view] = true
			child, err := view.Identity()
			if err != nil {
				return nil, err
			}
			pathEdges = append(pathEdges, borrowedPresenceEdge{id, holder, child})
			id = child
			graph = append(graph, id)
		}
		if len(view.Relations) != 0 || view.SelfReference != nil {
			return nil, fmt.Errorf("borrowed ancestor presence target is not a leaf")
		}
		same := len(graph) == len(proof.BorrowerGraph)
		if same {
			for i := range graph {
				if graph[i] != proof.BorrowerGraph[i] {
					same = false
					break
				}
			}
		}
		if !same || id != identity || proof.Expected.Package != r.input.TargetPackage || ref.DescriptorKey != proof.Expected.Package+"."+proof.Expected.Name {
			return nil, fmt.Errorf("borrowed ancestor presence does not match its admitted canonical body path")
		}
		if excluded {
			continue
		}
		for _, edge := range pathEdges {
			edges[edge] = true
		}
	}
	return edges, nil
}
