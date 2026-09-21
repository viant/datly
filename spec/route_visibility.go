package spec

// PublicRoute reports whether a route should be published through external
// protocol catalogs. Internal routes remain available to component invocation
// and operational warmup.
func PublicRoute(route *Route) bool {
	return route != nil && !route.Internal
}

// MCPRoute reports whether a route participates in the MCP catalog. An
// internal route with an explicit MCP exposure is intentionally MCP-only: it
// remains invokable by the component runtime but is omitted from public HTTP
// routing.
func MCPRoute(route *Route) bool {
	return route != nil && (!route.Internal || len(route.MCP) > 0)
}
