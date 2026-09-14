package transcribe

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
)

func (l *componentLoader) mergeTypeContext(base, authored *spec.TypeContext, packageScope string) *spec.TypeContext {
	if base == nil {
		base = &spec.TypeContext{DefaultPackage: strings.TrimSpace(packageScope)}
	} else {
		base = base.Clone()
	}
	if authored == nil {
		return base
	}
	if strings.TrimSpace(authored.DefaultPackage) != "" {
		base.DefaultPackage = authored.DefaultPackage
	}
	seen := map[string]bool{}
	imports := make([]spec.ImportSpec, 0, len(authored.Imports)+len(base.Imports))
	for _, item := range append(append([]spec.ImportSpec(nil), authored.Imports...), base.Imports...) {
		key := strings.TrimSpace(item.Alias) + "|" + strings.TrimSpace(item.Package)
		if item.Alias != "" {
			key = strings.TrimSpace(item.Alias)
		}
		if key == "|" || seen[key] {
			continue
		}
		seen[key] = true
		imports = append(imports, item)
	}
	base.Imports = imports
	return base
}

func (l *componentLoader) mergeParams(base, authored []*spec.Parameter) ([]*spec.Parameter, error) {
	result := make([]*spec.Parameter, 0, len(authored)+len(base))
	baseByIdentity := map[string]*spec.Parameter{}
	for _, param := range base {
		if param == nil {
			continue
		}
		identity, err := l.paramIdentity(param)
		if err != nil {
			return nil, err
		}
		if baseByIdentity[identity] != nil {
			return nil, fmt.Errorf("package parameter %q is declared more than once", identity)
		}
		baseByIdentity[identity] = param
	}
	authoredIdentities := map[string]bool{}
	for _, param := range authored {
		if param == nil {
			continue
		}
		identity, err := l.paramIdentity(param)
		if err != nil {
			return nil, err
		}
		if authoredIdentities[identity] {
			return nil, fmt.Errorf("authored parameter %q is declared more than once", identity)
		}
		authoredIdentities[identity] = true
		result = append(result, overlayParam(baseByIdentity[identity], param))
	}
	for _, param := range base {
		if param == nil {
			continue
		}
		identity, err := l.paramIdentity(param)
		if err != nil {
			return nil, err
		}
		if !authoredIdentities[identity] {
			result = append(result, param)
		}
	}
	return result, nil
}

func overlayParam(base, authored *spec.Parameter) *spec.Parameter {
	if base == nil {
		return authored
	}
	result := base.Clone()
	authoredCopy := authored.Clone()
	result.Name = authored.Name
	result.Source = authored.Source
	result.Declaration = authored.Declaration
	result.Raw = authored.Raw
	if authored.TypeExpr != "" {
		result.TypeExpr = authored.TypeExpr
	}
	if authored.OutputTypeExpr != "" {
		result.OutputTypeExpr = authored.OutputTypeExpr
	}
	if authored.DeclarationSQL != "" {
		result.DeclarationSQL = authored.DeclarationSQL
	}
	if authored.Tag != "" {
		result.Tag = authored.Tag
	}
	if authored.Cardinality != "" {
		result.Cardinality = authored.Cardinality
	}
	if authored.Required != nil {
		result.Required = authoredCopy.Required
	}
	if authored.Cacheable != nil {
		result.Cacheable = authoredCopy.Cacheable
	}
	if authored.MinAllowedRecords != nil {
		result.MinAllowedRecords = authoredCopy.MinAllowedRecords
	}
	if authored.MaxAllowedRecords != nil {
		result.MaxAllowedRecords = authoredCopy.MaxAllowedRecords
	}
	if authored.ExpectedReturned != nil {
		result.ExpectedReturned = authoredCopy.ExpectedReturned
	}
	if authored.When != "" {
		result.When = authored.When
	}
	if authored.Scope != "" {
		result.Scope = authored.Scope
	}
	if authored.With != "" {
		result.With = authored.With
	}
	if authored.Activation != nil {
		result.Activation = authoredCopy.Activation
	}
	if authored.ResourceRef != "" {
		result.ResourceRef = authored.ResourceRef
	}
	if authored.Value != nil {
		result.Value = authoredCopy.Value
	}
	result.Async = result.Async || authored.Async
	if authored.ErrorStatusCode != 0 {
		result.ErrorStatusCode = authored.ErrorStatusCode
	}
	if authored.ErrorMessage != "" {
		result.ErrorMessage = authored.ErrorMessage
	}
	if authored.Description != "" {
		result.Description = authored.Description
	}
	if authored.Example != "" {
		result.Example = authored.Example
	}
	result.EmitOutput = result.EmitOutput || authored.EmitOutput
	if authored.MCP != nil {
		result.MCP = authoredCopy.MCP
	}
	if authored.PathMCP != nil {
		result.PathMCP = authoredCopy.PathMCP
	}
	if len(authored.Predicates) > 0 {
		result.Predicates = authoredCopy.Predicates
	}
	if authored.Codec != nil {
		result.Codec = authoredCopy.Codec
	}
	if authored.QuerySelector != nil {
		result.QuerySelector = authoredCopy.QuerySelector
	}
	return result
}

func (l *componentLoader) paramIdentity(param *spec.Parameter) (string, error) {
	if param == nil || strings.TrimSpace(param.Name) == "" {
		return "", fmt.Errorf("parameter name is required")
	}
	return param.Identity(), nil
}

func (l *componentLoader) mergeViews(base, authored []*spec.View) ([]*spec.View, error) {
	result := make([]*spec.View, 0, len(authored)+len(base))
	seen := map[string]bool{}
	for _, view := range authored {
		if view == nil {
			continue
		}
		identity, err := l.viewIdentity(view)
		if err != nil {
			return nil, err
		}
		if seen[identity] {
			return nil, fmt.Errorf("authored view %q is declared more than once", identity)
		}
		seen[identity] = true
		result = append(result, view)
	}
	baseSeen := map[string]bool{}
	for _, view := range base {
		if view == nil {
			continue
		}
		identity, err := l.viewIdentity(view)
		if err != nil {
			return nil, err
		}
		if baseSeen[identity] {
			return nil, fmt.Errorf("package view %q is declared more than once", identity)
		}
		baseSeen[identity] = true
		if seen[identity] {
			continue
		}
		result = append(result, view)
	}
	return result, nil
}

func (l *componentLoader) viewIdentity(view *spec.View) (string, error) {
	return view.Identity()
}

// normalizeIndependentViewParams adapts the original load-stage cardinality
// rule after package and DQL views have converged on the canonical component.
func (l *componentLoader) normalizeIndependentViewParams(component *spec.Component, declaredViews map[string]*spec.View) (map[string]string, error) {
	if component == nil {
		return nil, nil
	}
	bindings := map[string]string{}
	for _, param := range spec.EffectiveParameters(component.Parameters) {
		if param == nil || !strings.EqualFold(strings.TrimSpace(param.Source.Kind), "view") {
			continue
		}
		explicitCardinality := false
		switch strings.ToLower(strings.TrimSpace(param.Cardinality)) {
		case "one":
			param.Cardinality = string(spec.CardinalityOne)
			explicitCardinality = true
		case "many":
			param.Cardinality = string(spec.CardinalityMany)
			explicitCardinality = true
		case "":
		default:
			return nil, fmt.Errorf("view parameter %q has unsupported cardinality %q", param.Name, param.Cardinality)
		}

		matched := declaredViews[param.Identity()]
		viewName := strings.TrimSpace(param.Source.Name)
		if viewName == "" {
			viewName = strings.TrimSpace(param.Name)
		}
		if matched == nil {
			for _, view := range component.Views {
				if view == nil || !strings.EqualFold(strings.TrimSpace(view.CanonicalName()), viewName) {
					continue
				}
				if matched != nil {
					return nil, fmt.Errorf("view parameter %q matches more than one independent view named %q", param.Name, viewName)
				}
				matched = view
			}
		}
		if matched == nil {
			return nil, fmt.Errorf("view parameter %q has no canonical independent view %q", param.Name, viewName)
		}
		identity, err := matched.Identity()
		if err != nil {
			return nil, err
		}
		bindings[param.Identity()] = identity
		if explicitCardinality {
			continue
		}
		if param.Required != nil && *param.Required {
			param.Cardinality = string(spec.CardinalityOne)
			continue
		}
		switch matched.Cardinality {
		case spec.CardinalityOne:
			param.Cardinality = string(spec.CardinalityOne)
		case "", spec.CardinalityMany:
			param.Cardinality = string(spec.CardinalityMany)
		default:
			return nil, fmt.Errorf("independent view %q has unsupported cardinality %q", viewName, matched.Cardinality)
		}
	}
	if len(bindings) == 0 {
		return nil, nil
	}
	return bindings, nil
}
