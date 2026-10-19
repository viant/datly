package link

import (
	"fmt"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/tagly/format/text"
	"github.com/viant/tagly/tags"
)

func (l *Loader) mergeTypeContext(base, authored *spec.TypeContext, packageScope string) *spec.TypeContext {
	if base == nil {
		base = &spec.TypeContext{DefaultPackage: strings.TrimSpace(packageScope)}
	} else {
		base = base.Clone()
	}
	if authored == nil {
		return base
	}
	if authored.PackagePath != "" {
		base.PackagePath = authored.PackagePath
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

func (l *Loader) mergeParams(base, authored []*spec.Parameter) ([]*spec.Parameter, error) {
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
	consumedBase := map[string]bool{}
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
		matched := baseByIdentity[identity]
		matchedIdentity := identity
		if matched == nil {
			var err error
			matched, matchedIdentity, err = inheritedSelectorAlias(base, param)
			if err != nil {
				return nil, err
			}
		}
		if matched != nil {
			if consumedBase[matchedIdentity] {
				return nil, fmt.Errorf("package parameter %q matches more than one authored selector", matchedIdentity)
			}
			consumedBase[matchedIdentity] = true
		}
		result = append(result, overlayParam(matched, param))
	}
	for _, param := range base {
		if param == nil {
			continue
		}
		identity, err := l.paramIdentity(param)
		if err != nil {
			return nil, err
		}
		if !consumedBase[identity] {
			result = append(result, param)
		}
	}
	return result, nil
}

// An input field generated from a selector uses its transport source name.
// On package-linked regeneration that field may therefore be named Sort while
// the authored selector remains OrderBy(query/sort). Only reconcile that exact
// generated alias; unrelated parameters sharing a transport key stay distinct.
func inheritedSelectorAlias(base []*spec.Parameter, authored *spec.Parameter) (*spec.Parameter, string, error) {
	if authored.EmitOutput || authored.QuerySelector == nil || !strings.EqualFold(authored.Source.Kind, "query") {
		return nil, "", nil
	}
	property, canonical := spec.SelectorPropertyForParam(authored.Name)
	if !canonical || property != authored.QuerySelector.Property {
		return nil, "", nil
	}
	source := strings.TrimLeft(strings.TrimSpace(authored.Source.Name), "_")
	if source == "" {
		return nil, "", nil
	}
	alias := text.DetectCaseFormat(source).Format(source, text.CaseFormatUpperCamel)
	var matched *spec.Parameter
	var identity string
	for _, candidate := range base {
		if candidate == nil || candidate.EmitOutput || candidate.QuerySelector == nil || candidate.Name != alias ||
			!strings.EqualFold(candidate.Source.Kind, authored.Source.Kind) ||
			!strings.EqualFold(candidate.Source.Name, authored.Source.Name) ||
			candidate.QuerySelector.Property != property ||
			!strings.EqualFold(candidate.QuerySelector.View, authored.QuerySelector.View) {
			continue
		}
		if matched != nil {
			return nil, "", fmt.Errorf("authored selector %q matches more than one inherited generated alias", authored.Name)
		}
		matched = candidate
		identity = candidate.Identity()
	}
	return matched, identity, nil
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
	// A matched DQL declaration owns the complete scheduling policy, including
	// removing a group previously emitted into the linked input contract.
	result.ResolutionGroup = authoredCopy.ResolutionGroup
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
	if strings.EqualFold(strings.TrimSpace(authored.Source.Kind), "param") &&
		strings.TrimSpace(authored.DeclarationSQL) != "" &&
		strings.TrimSpace(authored.TypeExpr) == "" && strings.TrimSpace(authored.OutputTypeExpr) == "" {
		typeName := tags.NewTags(strings.TrimSpace(result.Tag)).Lookup("typeName")
		if typeName == nil || strings.TrimSpace(string(typeName.Values)) == "" {
			// An inferred DQL projection owns its generated helper. Retaining the
			// previous generated Input's type suppresses inference on regeneration.
			// Explicit typeName authority and unrelated binding tags stay intact.
			result.TypeExpr = ""
			result.OutputTypeExpr = ""
		}
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
	if authored.BodyNullPolicy != "" {
		result.BodyNullPolicy = authored.BodyNullPolicy
	}
	if authored.QueryListCSV {
		result.QueryListCSV = true
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
		if authored.TypeExpr != "" {
			// A typed DQL selector declaration owns its conversion contract.
			// Removing a former public-input bridge must not retain its codec
			// or destination type during native source/package reload.
			result.Codec = authoredCopy.Codec
			result.OutputTypeExpr = authoredCopy.OutputTypeExpr
			result.QueryListCSV = authoredCopy.QueryListCSV
		}
	}
	return result
}

func (l *Loader) paramIdentity(param *spec.Parameter) (string, error) {
	if param == nil || strings.TrimSpace(param.Name) == "" {
		return "", fmt.Errorf("parameter name is required")
	}
	return param.Identity(), nil
}

// MergeViews overlays views by canonical identity and rejects duplicate declarations.
func MergeViews(base, authored []*spec.View) ([]*spec.View, error) {
	result := make([]*spec.View, 0, len(authored)+len(base))
	seen := map[string]bool{}
	for _, view := range authored {
		if view == nil {
			continue
		}
		identity, err := view.Identity()
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
		identity, err := view.Identity()
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

// NormalizeIndependentViewParams adapts the original load-stage cardinality
// rule after package and DQL views have converged on the canonical component.
func NormalizeIndependentViewParams(component *spec.Component, declaredViews map[string]*spec.View) (map[string]string, error) {
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
