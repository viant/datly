package cache

import (
	"github.com/viant/datly/internal/cache/memory"
	xcache "github.com/viant/xdatly/cache"
)

// NewMemory creates the built-in backend for explicit composition without
// exposing its internal implementation type to applications. Zero capacity
// means unlimited entries. Register it with New under an explicit name.
func NewMemory(capacity int) (xcache.Cache, error) {
	return memory.New(capacity)
}
