package spec

// PublicRoute reports whether a route should be published through external
// protocol catalogs. Internal routes remain available to component invocation
// and operational warmup.
func PublicRoute(route *Route) bool {
	return route != nil && !route.Internal
}
