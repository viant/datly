package report

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

type reportIdentity struct {
	key      spec.Key
	route    spec.RouteRef
	typeName string
}

func (d *reportDeriver) derivedIdentity(component *spec.Component, route *spec.Route, routeCount int) (reportIdentity, error) {
	suffix := ""
	if routeCount > 1 {
		if strings.TrimSpace(route.Name) == "" {
			return reportIdentity{}, fmt.Errorf("report component %s has multiple GET routes; each route requires a name", component.Key.String())
		}
		suffix = typecatalog.ExportedFieldName(route.Name)
	}
	base := typecatalog.ExportedFieldName(component.Key.Name + suffix + "Cube")
	if base == "" {
		return reportIdentity{}, fmt.Errorf("report component %s has no derivable cube identity", component.Key.String())
	}
	return reportIdentity{
		key:   spec.Key{Kind: spec.KindComponent, Scope: component.Key.Scope, Name: base},
		route: spec.RouteRef{Method: http.MethodPost, Path: strings.TrimRight(route.Path, "/") + "/cube"}, typeName: base + "Input",
	}, nil
}

func (d *reportDeriver) derivedComponent(source *spec.Component, sourceRoute *spec.Route, identity reportIdentity, metadata *metadata, input *inputCompilation) *spec.Component {
	settings := &spec.Settings{InputType: input.descriptor.Key()}
	if source.Settings != nil {
		settings.OutputType = source.Settings.OutputType
	}
	var exposure []*spec.MCPExposure
	if metadata.settings.MCPTool == nil || *metadata.settings.MCPTool {
		exposure = []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: identity.key.Name, Description: source.Description}}
	}
	return &spec.Component{
		Key: identity.key, Name: identity.key.Name, Description: strings.TrimSpace(source.Description + " cube"),
		Settings: settings, TypeContext: source.TypeContext.Clone(), Parameters: cloneParams(input.params),
		Routes: []*spec.Route{{
			Method: identity.route.Method, Path: identity.route.Path, Name: sourceRoute.Name + " Cube", MCP: exposure,
			APIKeyHeader: sourceRoute.APIKeyHeader, APIKeyValue: sourceRoute.APIKeyValue,
		}},
	}
}

func (s Source) identity() string {
	if s.Component == nil {
		return ""
	}
	return s.Component.Key.String()
}

func (s Source) reportEnabled() bool {
	component := s.Component
	return component != nil && component.Settings != nil && component.Settings.Report != nil && component.Settings.Report.Enabled
}

func (s Source) groupable() bool {
	return s.Component != nil && s.Component.RootView != nil && s.Component.RootView.Groupable != nil && *s.Component.RootView.Groupable
}

func (s Source) eligibleRoutes() []*spec.Route {
	if !s.reportEnabled() || !s.groupable() {
		return nil
	}
	result := make([]*spec.Route, 0, len(s.Component.Routes))
	for _, route := range s.Component.Routes {
		if route != nil && strings.EqualFold(route.Method, http.MethodGet) {
			result = append(result, route)
		}
	}
	return result
}

func (d *reportDeriver) index(source Source) error {
	if source.Component == nil {
		return fmt.Errorf("report source component is required")
	}
	identity := source.Component.Key.String()
	if d.keys[identity] {
		return fmt.Errorf("duplicate report source component %s", identity)
	}
	d.keys[identity] = true
	for _, route := range source.Component.Routes {
		ref := d.routeIdentity(route)
		if ref == "" {
			return fmt.Errorf("component %s has an incomplete route", identity)
		}
		if owner := d.routes[ref]; owner != "" {
			return fmt.Errorf("route %s is shared by components %s and %s", ref, owner, identity)
		}
		d.routes[ref] = identity
	}
	return nil
}

func (d *reportDeriver) routeIdentity(route *spec.Route) string {
	if route == nil {
		return ""
	}
	return (spec.RouteRef{Method: route.Method, Path: route.Path}).String()
}

func (d *reportDeriver) reportPackage(component *spec.Component) string {
	if component != nil && component.TypeContext != nil && strings.TrimSpace(component.TypeContext.DefaultPackage) != "" {
		return strings.TrimSpace(component.TypeContext.DefaultPackage)
	}
	if component == nil {
		return ""
	}
	return strings.TrimSpace(component.Key.Scope)
}

func cloneParams(source []*spec.Parameter) []*spec.Parameter {
	result := make([]*spec.Parameter, len(source))
	for i, param := range source {
		result[i] = param.Clone()
	}
	return result
}

func cloneHolders(source map[string][]string) map[string][]string {
	result := make(map[string][]string, len(source))
	for name, holders := range source {
		result[name] = append([]string(nil), holders...)
	}
	return result
}
