package coding

import "context"

type Encoder interface {
	Encode(ctx context.Context, instance interface{}) ([]byte, error)
}

