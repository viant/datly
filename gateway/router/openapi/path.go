package openapi

import (
	openapi "github.com/viant/datly/gateway/router/openapi/openapi3"
	"sync"
)

type PathsBuilder struct {
	mux   sync.Mutex
	paths openapi.Paths
}

func (b *PathsBuilder) AddPath(URI string, path *openapi.PathItem) {
	b.mux.Lock()
	defer b.mux.Unlock()

	b.paths[URI] = path
}
