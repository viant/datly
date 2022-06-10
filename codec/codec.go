package codec

import "context"

type Codec interface {
	Value(ctx context.Context, raw string, options ...interface{}) (interface{}, error)
}
