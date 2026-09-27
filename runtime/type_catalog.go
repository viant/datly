package runtime

import (
	"context"
	"fmt"

	"github.com/viant/datly/typecatalog"
)

type typeCatalogContextKey struct{}

// WithTypeCatalog gives native handlers read-only access to this runtime
// generation's linked type authority. The runtime owns a detached snapshot;
// callers cannot replace it through request data or mutate it through context.
func WithTypeCatalog(catalog *typecatalog.Catalog) Option {
	return func(options *options) error {
		if catalog == nil {
			return fmt.Errorf("runtime type catalog is required")
		}
		if options.types != nil && options.types != catalog {
			return fmt.Errorf("runtime type catalogs conflict")
		}
		options.types = catalog
		return nil
	}
}

// TypeCatalog returns a detached view of the generation that owns the current
// native component invocation. A bare context has no type authority.
func TypeCatalog(ctx context.Context) (*typecatalog.Catalog, bool, error) {
	if ctx == nil {
		return nil, false, nil
	}
	catalog, ok := ctx.Value(typeCatalogContextKey{}).(*typecatalog.Catalog)
	if !ok || catalog == nil {
		return nil, false, nil
	}
	copy, err := catalog.Clone()
	return copy, err == nil, err
}
