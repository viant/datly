package spec

// ViewBindings are the resource/cache controls derived from authored SQL-ish
// view-control calls: the connector the view reads from and its cache identity
// and warmup mode. Unlike ViewControls (query shaping), bindings are never
// overlaid by the runtime selector and flow unchanged into the prepared
// artifact, where the cache and warmup paths consume them. "Bindings" here
// refers to the view's resource bindings, distinct from per-request parameter
// binding.
type ViewBindings struct {
	Connector   string `json:"connector,omitempty"`
	CacheName   string `json:"cacheName,omitempty"`
	CacheWarmup string `json:"cacheWarmup,omitempty"`
}

func (b *ViewBindings) Clone() *ViewBindings {
	if b == nil {
		return nil
	}
	cloned := *b
	return &cloned
}

func (b *ViewBindings) IsZero() bool {
	return b == nil || (b.Connector == "" && b.CacheName == "" && b.CacheWarmup == "")
}
