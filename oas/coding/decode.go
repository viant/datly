package coding

import "context"

type Decoder interface {
	Decode(ctx context.Context, bs []byte, instancePtr interface{}) error
}
