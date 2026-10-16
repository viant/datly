package reader

import (
	"reflect"
	"slices"
	"strings"

	"github.com/viant/bindly"
	bindinput "github.com/viant/bindly/input"
	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	legacy "github.com/viant/xdatly/handler/response"
)

type selectorOwner struct {
	view                 *data.View
	index                []int
	name, path, kind, in string
}

// compileSelectorOwner accepts only the already compiled root query selector
// and explicitly linked nominal output declarations. It never supplies defaults.
func (s *Session) compileSelectorOwner() *selectorOwner {
	if s.Component == nil || s.Artifact == nil || s.Artifact.Root == nil || s.Artifact.ViewIndex == nil || s.Component.RootView == nil || s.Artifact.DirectOutput || s.OutputType == nil {
		return nil
	}
	root := s.Artifact.Root.View
	if root == nil || root.Spec.Name == "" || root.Spec.Name != s.Component.RootView.Name || root.Spec.InMemory || root.Spec.Auxiliary || (s.Component.Settings != nil && s.Component.Settings.Mutation != "") {
		return nil
	}
	for _, route := range s.Component.Routes {
		if route != nil && route.Handler != "" {
			return nil
		}
	}
	var status, view *spec.Parameter
	for _, p := range spec.EffectiveParameters(s.Component.Parameters) {
		if p == nil || !(p.EmitOutput || strings.EqualFold(strings.TrimSpace(p.Source.Kind), "output")) {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(p.Source.Name)) {
		case "status":
			if status != nil {
				return nil
			}
			status = p
		case "view":
			if view != nil {
				return nil
			}
			view = p
		}
	}
	if status == nil || view == nil || status.Name != s.Artifact.OutputStatusField || view.Name != s.Artifact.OutputViewField {
		return nil
	}
	typ := s.OutputType
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil
	}
	field, ok := typ.FieldByName(status.Name)
	if !ok || !field.IsExported() || (field.Type != reflect.TypeFor[legacy.Status]() && field.Type != reflect.TypeFor[*legacy.Status]()) {
		return nil
	}
	if f, ok := typ.FieldByName(view.Name); !ok || !f.IsExported() {
		return nil
	}
	var selected *SelectorBindingPlan
	selectedIndex := -1
	for i := range s.Artifact.SelectorBindings {
		b := &s.Artifact.SelectorBindings[i]
		if b.Property != spec.SelectorPropertyFields {
			continue
		}
		if selected != nil || b.View != root {
			return nil
		}
		selected = b
		selectedIndex = i
	}
	if selected == nil {
		return nil
	}
	var owner *selectorOwner
	for _, p := range spec.EffectiveParameters(s.Component.Parameters) {
		if p == nil || p.QuerySelector == nil || p.QuerySelector.Property != spec.SelectorPropertyFields {
			continue
		}
		if owner != nil || p.Source.Kind != "query" || p.Source.Name == "" || p.Source.Name != strings.TrimSpace(p.Source.Name) {
			return nil
		}
		view, err := s.Artifact.ViewIndex.Resolve(p.QuerySelector.View)
		if err != nil || view != root {
			return nil
		}
		f, ok := typecatalog.FieldByName(s.InputType, p.Name)
		if s.Artifact.companionSelectors[selectedIndex] {
			if s.Artifact.selectorCompanion == nil || s.Artifact.selectorCompanion.InputType() != s.InputType || !s.Artifact.selectorCompanion.Matches(s.Component) {
				return nil
			}
			f, ok = s.Artifact.selectorCompanion.Field(p)
		}
		if !ok || !f.IsExported() || !slices.Equal(f.Index, selected.FieldIndex) {
			return nil
		}
		t := f.Type
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if t.Kind() != reflect.Slice || t.Elem().Kind() != reflect.String {
			return nil
		}
		// BuildBindingSpecs uses this exact canonical field for both Path and Name
		// when the effective declaration has QuerySelector metadata.
		owner = &selectorOwner{view: root, index: slices.Clone(f.Index), name: f.Name, path: f.Name, kind: p.Source.Kind, in: p.Source.Name}
	}
	return owner
}

func (s *Session) selectorError(view *data.View, cause error, owned bool) error {
	if !owned || s.outputAccessors == nil || s.outputAccessors.selectorOwner == nil || s.outputAccessors.selectorOwner.view != view {
		return nil
	}
	b := s.outputAccessors.selectorOwner
	result := &bindly.BindingError{Name: b.name, Path: b.path, Kind: b.kind, In: b.in, Code: 400, Cause: &bindinput.Error{Code: 400, Cause: cause}}
	s.selectorFailure = result
	return result
}
