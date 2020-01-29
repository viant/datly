package db

import (
	"context"
	"github.com/viant/dsc"
)

//Service represents database/datastore service
type Service interface {
	Manager(ctx context.Context, name string) (dsc.Manager, error)
}
