package function

func init() {
	_registry.Register("use_connector", &connector{})
	_registry.Register("use_cache", &cache{})
	_registry.Register("limit", &limit{})
	_registry.Register("cardinality", &cardinality{})
	_registry.Register("allow_nulls", &allownulls{})
}
