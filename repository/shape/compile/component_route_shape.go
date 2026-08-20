package compile

import (
	"strings"

	"github.com/viant/datly/repository/shape"
	"github.com/viant/datly/repository/shape/plan"
)

func ensureDQLComponentRouteWithLayout(result *plan.Result, source *shape.Source, layout compilePathLayout) {
	if result == nil || len(result.Components) > 0 {
		return
	}
	root := firstPlannedView(result.Views)
	if root == nil {
		return
	}

	settings := extractRuleSettings(source, result.Directives)
	method := httpMethod(settings)
	uri := strings.TrimSpace(settings.URI)
	if uri == "" {
		namespace := ""
		if source != nil && strings.TrimSpace(source.Path) != "" {
			namespace, _ = dqlToRouteNamespaceWithLayout(source.Path, layout)
		}
		if namespace != "" {
			uri = inferDefaultURI(namespace)
		}
		if uri == "" && source != nil {
			uri = normalizeURI(source.Name)
		}
	}
	if uri == "" {
		return
	}

	name := root.Name
	if source != nil && strings.TrimSpace(source.Name) != "" {
		name = strings.TrimSpace(source.Name)
	}
	result.Components = []*plan.ComponentRoute{{
		Name:      name,
		ViewName:  strings.TrimSpace(root.Name),
		RoutePath: normalizeURI(uri),
		Method:    method,
		Connector: strings.TrimSpace(root.Connector),
		SourceURL: strings.TrimSpace(root.SQLURI),
	}}
}

func firstPlannedView(views []*plan.View) *plan.View {
	for _, item := range views {
		if item != nil {
			return item
		}
	}
	return nil
}
