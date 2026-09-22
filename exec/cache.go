package exec

import (
	"context"
	"errors"
	"fmt"
)

var ErrCacheViewNotFound = errors.New("cached view not found")

type CacheInvalidation struct {
	View       string `json:"view"`
	Scope      string `json:"scope"`
	Generation string `json:"generation,omitempty"`
	Error      string `json:"error,omitempty"`
}

// ReaderCacheManager is a server-owned capability. HTTP callers require an
// independent administrator policy before this operation is invoked.
type ReaderCacheManager interface {
	CacheViews() []string
	InvalidateCache(context.Context, string, string) ([]CacheInvalidation, error)
}

// CacheServiceInvalidator lets supplied cache services support administration
// without importing Datly's internal generation implementation.
type CacheServiceInvalidator interface {
	InvalidateCache(context.Context, string) (string, error)
}

func ValidateCacheScope(scope string) error {
	switch scope {
	case "all", "lazy", "warmup":
		return nil
	default:
		return fmt.Errorf("invalid cache scope %q: expected all, lazy, or warmup", scope)
	}
}
