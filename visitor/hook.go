package visitor

var hookRegistry ServiceRegistry

//Registry returns visitor registry singleton
func Registry() ServiceRegistry {
	if hookRegistry != nil {
		return hookRegistry
	}
	hookRegistry = New()
	return hookRegistry
}
