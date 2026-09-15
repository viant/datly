package generate

import (
	"fmt"
	"reflect"
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
		if err := addViewSetMarker(viewPlan, view); err != nil {
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
		field, ok := fields[name]
		if !ok {
			return fmt.Errorf("generated view %q set marker column %q has no field", plan.Name, column.Name)
		}
		if ignoredSQLXField(field.Tag) && !column.DeleteMarker {
			continue
		}
		markerFields = append(markerFields, name)
	}
	for _, relation := range view.Relations {
		if relation == nil {
			continue
		}
		if relation.Kind == spec.RelationKindDerived || relation.View != nil && relation.View.Auxiliary {
			continue
		}
		holder := relation.Holder
		if holder == "" {
			holder = relation.Name
		}
		name := typecatalog.FieldName(holder)
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

func ignoredSQLXField(raw string) bool {
	value := reflect.StructTag(strings.TrimSpace(raw)).Get("sqlx")
	for _, option := range strings.Split(value, ",") {
		if strings.TrimSpace(option) == "-" {
			return true
		}
	}
	return false
}
