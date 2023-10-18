package repository

import (
	"context"
	"github.com/viant/datly/repository/version"
	"github.com/viant/datly/view"
	"time"
)

// Resources represents a resource
type Resources interface {
	AddResource(key string, aResource *view.Resource)
	Has(key string) bool
	Lookup(key string) (*version.Resource, error)
	Substitutes() map[string]view.Substitutes
	IsCheckDue(t time.Time) bool
	SyncChanges(ctx context.Context) (bool, error)
}
