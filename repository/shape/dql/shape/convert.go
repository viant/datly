package shape

import (
	"fmt"

	"github.com/viant/datly/repository/shape/dql/ir"
	"github.com/viant/datly/repository/shape/typectx"
)

// FromIR builds typed shape document from IR.
func FromIR(doc *ir.Document) (*Document, error) {
	if doc == nil || doc.Root == nil {
		return nil, fmt.Errorf("dql shape: nil IR document")
	}
	root, ok := deepClone(doc.Root).(map[string]any)
	if !ok || root == nil {
		return nil, fmt.Errorf("dql shape: invalid IR root")
	}
	ret := &Document{Root: root}
	ret.TypeContext = typeContextFromRoot(root)
	ret.TypeResolutions = typeResolutionsFromRoot(root)
	for _, item := range asSlice(root["Routes"]) {
		routeMap := asMap(item)
		if routeMap == nil {
			continue
		}
		route := &Route{
			Name:        asString(routeMap["Name"]),
			URI:         asString(routeMap["URI"]),
			Method:      asString(routeMap["Method"]),
			Description: asString(routeMap["Description"]),
		}
		if view := asMap(routeMap["View"]); view != nil {
			route.ViewRef = asString(view["Ref"])
		}
		ret.Routes = append(ret.Routes, route)
	}
	resourceMap := asMap(root["Resource"])
	if resourceMap != nil {
		resource := &Resource{}
		for _, item := range asSlice(resourceMap["Views"]) {
			viewMap := asMap(item)
			if viewMap == nil {
				continue
			}
			view := &View{
				Name:   asString(viewMap["Name"]),
				Table:  asString(viewMap["Table"]),
				Module: asString(viewMap["Module"]),
			}
			if connector := asMap(viewMap["Connector"]); connector != nil {
				view.ConnectorRef = asString(connector["Ref"])
			}
			resource.Views = append(resource.Views, view)
		}
		ret.Resource = resource
	}
	return ret, nil
}

// ToIR converts shape document back to IR.
func ToIR(doc *Document) (*ir.Document, error) {
	if doc == nil || doc.Root == nil {
		return nil, fmt.Errorf("dql shape: nil document")
	}
	root, ok := deepClone(doc.Root).(map[string]any)
	if !ok || root == nil {
		return nil, fmt.Errorf("dql shape: invalid root")
	}
	if doc.TypeContext != nil {
		root["TypeContext"] = map[string]any{
			"DefaultPackage": doc.TypeContext.DefaultPackage,
			"Imports":        importsToAny(doc.TypeContext.Imports),
		}
	}
	if len(doc.TypeResolutions) > 0 {
		root["TypeResolutions"] = typeResolutionsToAny(doc.TypeResolutions)
	}
	return &ir.Document{Root: root}, nil
}

func typeContextFromRoot(root map[string]any) *typectx.Context {
	raw := asMap(root["TypeContext"])
	if raw == nil {
		return nil
	}
	ret := &typectx.Context{DefaultPackage: asString(raw["DefaultPackage"])}
	for _, item := range asSlice(raw["Imports"]) {
		importMap := asMap(item)
		if importMap == nil {
			continue
		}
		pkg := asString(importMap["Package"])
		if pkg == "" {
			continue
		}
		ret.Imports = append(ret.Imports, typectx.Import{
			Alias:   asString(importMap["Alias"]),
			Package: pkg,
		})
	}
	if ret.DefaultPackage == "" && len(ret.Imports) == 0 {
		return nil
	}
	return ret
}

func importsToAny(imports []typectx.Import) []any {
	if len(imports) == 0 {
		return nil
	}
	result := make([]any, 0, len(imports))
	for _, item := range imports {
		if item.Package == "" {
			continue
		}
		result = append(result, map[string]any{
			"Alias":   item.Alias,
			"Package": item.Package,
		})
	}
	return result
}

func typeResolutionsFromRoot(root map[string]any) []typectx.Resolution {
	items := asSlice(root["TypeResolutions"])
	if len(items) == 0 {
		return nil
	}
	result := make([]typectx.Resolution, 0, len(items))
	for _, item := range items {
		resolutionMap := asMap(item)
		if resolutionMap == nil {
			continue
		}
		resolution := typectx.Resolution{
			Expression:  asString(resolutionMap["Expression"]),
			Target:      asString(resolutionMap["Target"]),
			ResolvedKey: asString(resolutionMap["ResolvedKey"]),
			MatchKind:   asString(resolutionMap["MatchKind"]),
		}
		if provenanceMap := asMap(resolutionMap["Provenance"]); provenanceMap != nil {
			resolution.Provenance = typectx.Provenance{
				Package: asString(provenanceMap["Package"]),
				File:    asString(provenanceMap["File"]),
				Kind:    asString(provenanceMap["Kind"]),
			}
		}
		if resolution.Expression == "" && resolution.ResolvedKey == "" {
			continue
		}
		result = append(result, resolution)
	}
	return result
}

func typeResolutionsToAny(resolutions []typectx.Resolution) []any {
	result := make([]any, 0, len(resolutions))
	for _, item := range resolutions {
		if item.Expression == "" && item.ResolvedKey == "" {
			continue
		}
		result = append(result, map[string]any{
			"Expression":  item.Expression,
			"Target":      item.Target,
			"ResolvedKey": item.ResolvedKey,
			"MatchKind":   item.MatchKind,
			"Provenance": map[string]any{
				"Package": item.Provenance.Package,
				"File":    item.Provenance.File,
				"Kind":    item.Provenance.Kind,
			},
		})
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func deepClone(value any) any {
	switch actual := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(actual))
		for k, v := range actual {
			out[k] = deepClone(v)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(actual))
		for k, v := range actual {
			out[fmt.Sprint(k)] = deepClone(v)
		}
		return out
	case []any:
		out := make([]any, len(actual))
		for i, item := range actual {
			out[i] = deepClone(item)
		}
		return out
	default:
		return actual
	}
}

func asMap(raw any) map[string]any {
	if value, ok := raw.(map[string]any); ok {
		return value
	}
	if value, ok := raw.(map[any]any); ok {
		out := make(map[string]any, len(value))
		for k, item := range value {
			out[fmt.Sprint(k)] = item
		}
		return out
	}
	return nil
}

func asSlice(raw any) []any {
	if value, ok := raw.([]any); ok {
		return value
	}
	return nil
}

func asString(raw any) string {
	if raw == nil {
		return ""
	}
	if value, ok := raw.(string); ok {
		return value
	}
	return fmt.Sprint(raw)
}
