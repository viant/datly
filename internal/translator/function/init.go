package function

func init() {
	_registry.Register(&connector{})
	_registry.Register(&cache{})
	_registry.Register(&limit{})
	_registry.Register(&orderBy{})
	_registry.Register(&cardinality{})
	_registry.Register(&allownulls{})
	_registry.Register(&matchStrategy{})
	_registry.Register(&batchSize{})
	_registry.Register(&partitioner{})
}
