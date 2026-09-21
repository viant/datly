package compiler

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/typecatalog"
)

func (c *compiler) currentView(component *spec.Component, param *spec.Parameter, boundIdentity string) (*spec.View, error) {
	identity := strings.TrimSpace(boundIdentity)
	if identity == "" {
		return nil, fmt.Errorf("patch current input %q requires an exact canonical view binding", param.Name)
	}
	for _, view := range component.Views {
		if view == nil {
			continue
		}
		actual, err := view.Identity()
		if err != nil {
			return nil, err
		}
		if actual == identity {
			return view, nil
		}
	}
	return nil, fmt.Errorf("patch current input %q targets unknown canonical view %q", param.Name, identity)
}

func matchCurrentKey(view *spec.View, root plan.KeyPart) (plan.KeyPart, error) {
	var matched *spec.Column
	for _, column := range view.Columns {
		if column == nil {
			continue
		}
		fieldMatch := typecatalog.FieldName(column.Name) == root.Field
		source := currentColumnOrigin(column)
		sourceMatch := root.Source != "" && strings.EqualFold(source, root.Source)
		if fieldMatch && root.Source != "" && source != "" && !sourceMatch {
			return plan.KeyPart{}, fmt.Errorf("patch current view %q key %q source %q does not match root source %q", view.CanonicalName(), column.Name, source, root.Source)
		}
		if !fieldMatch && !sourceMatch {
			continue
		}
		if matched != nil {
			return plan.KeyPart{}, fmt.Errorf("patch current view %q has ambiguous root key %q", view.CanonicalName(), root.Field)
		}
		matched = column
	}
	if matched == nil {
		return plan.KeyPart{}, fmt.Errorf("patch current view %q has no root key %q", view.CanonicalName(), root.Field)
	}
	matchedType := matched.EffectiveType()
	if !sameType(matchedType, root.Type) {
		return plan.KeyPart{}, fmt.Errorf("patch current view %q key %q type %s.%s does not match root type %s.%s",
			view.CanonicalName(), matched.Name, matched.Type.Package, matched.Type.Name, root.Type.Package, root.Type.Name)
	}
	field := typecatalog.FieldName(matched.Name)
	if field == "" {
		return plan.KeyPart{}, fmt.Errorf("patch current view %q key %q has no canonical Go field", view.CanonicalName(), matched.Name)
	}
	return plan.KeyPart{Field: field, Source: strings.TrimSpace(matched.Source), Type: matchedType}, nil
}

func sameType(left, right spec.TypeRef) bool {
	return strings.TrimSpace(left.Package) == strings.TrimSpace(right.Package) &&
		strings.TrimPrefix(strings.TrimSpace(left.Name), "*") == strings.TrimPrefix(strings.TrimSpace(right.Name), "*")
}
