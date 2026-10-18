package dql

import (
	"fmt"
	"github.com/viant/datly/spec"
	"reflect"
)

// ComponentRoutes materializes the normalized declarations for one contract
// owner. Shared route settings must reach every generated/runtime contract;
// retaining only the first route here would make aliases disappear on reload.
func (p *DirectivePlan) ComponentRoutes(name, handler string) []*spec.Route {
	plans := p.Routes
	if len(plans) == 0 && p.Route != nil {
		plans = []*RoutePlan{p.Route}
	}
	var result []*spec.Route
	for _, plan := range plans {
		for _, method := range plan.Methods {
			route := &spec.Route{Name: name, Path: plan.URI, Method: method, Handler: handler, RequestBodyMode: plan.RequestBodyMode, APIKeyHeader: plan.APIKeyHeader, APIKeyValue: plan.APIKeyValue, Internal: p.Internal || p.MCPOnly}
			if p.MCP != nil {
				route.MCP = []*spec.MCPExposure{p.MCP.Clone()}
			}
			if exposure := p.routeMCP[spec.RouteRef{Method: method, Path: plan.URI}]; exposure != nil {
				route.MCP = []*spec.MCPExposure{exposure.Clone()}
			}
			result = append(result, route)
		}
	}
	return result
}

func normalizeRouteMCP(settings *componentSettings, routes *routeDirective) (map[spec.RouteRef]*spec.MCPExposure, error) {
	if len(settings.routeMCP) == 0 {
		return nil, nil
	}
	declared := map[spec.RouteRef]bool{}
	if routes != nil {
		for _, route := range routes.routes {
			for _, method := range route.Methods {
				declared[spec.RouteRef{Method: method, Path: route.URI}] = true
			}
		}
	}
	result := map[spec.RouteRef]*spec.MCPExposure{}
	for _, item := range settings.routeMCP {
		var err error
		if !declared[item.ref] {
			err = fmt.Errorf("mcp_route target %s is not a declared route", item.ref.String())
		} else if prior := result[item.ref]; prior != nil && !reflect.DeepEqual(prior, item.exposure) {
			err = fmt.Errorf("conflicting mcp_route exposure for %s", item.ref.String())
		} else if settings.MCP != nil && !reflect.DeepEqual(settings.MCP, item.exposure) {
			err = fmt.Errorf("mcp_route exposure for %s conflicts with global mcp", item.ref.String())
		}
		if err != nil {
			return nil, &sourceError{offset: item.span.Start, end: item.span.End, err: err}
		}
		result[item.ref] = item.exposure.Clone()
	}
	return result, nil
}
