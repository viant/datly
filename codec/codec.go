package codec

import "context"

type Codec interface {
	Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error)
}
