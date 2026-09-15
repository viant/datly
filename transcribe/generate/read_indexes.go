package generate

// ReadIndexSource is generated application support owned by the input package.
// Keeping it there lets Input.Init use typed indexes without importing its
// handler package. Linked contracts use a component-owned free accessor.
type ReadIndexSource struct {
	Package              string
	TypeName, CacheField string
	Source               MutationSource
}
