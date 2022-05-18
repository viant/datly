package visitor

import "context"

type Codec interface {
	Value(ctx context.Context, raw string) (interface{}, error)
}
