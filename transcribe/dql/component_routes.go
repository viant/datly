package dql

import "github.com/viant/datly/spec"

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
			result = append(result, route)
		}
	}
	return result
}
