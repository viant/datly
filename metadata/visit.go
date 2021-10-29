package metadata

import "context"

//Visit represent an object visitor
type Visit func(ctx context.Context, value interface{}) error
