package cache

var cacheRegistry ServiceRegistry

//Data returns cache registry singleton
func Registry() ServiceRegistry {
	if cacheRegistry != nil {
		return cacheRegistry
	}
	cacheRegistry = New()
	return cacheRegistry
}
