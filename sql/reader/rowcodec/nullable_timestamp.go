package rowcodec

import (
	"context"
	"fmt"
	"time"

	xcodec "github.com/viant/xdatly/codec"
)

// nullableTimestampCodec converts database timestamp text from expression
// columns to the generated *time.Time field. SQL NULL is handled by Decode
// before calling Value and remains a nil pointer.
type nullableTimestampCodec struct{}

func (nullableTimestampCodec) Value(_ context.Context, raw any, _ ...xcodec.Option) (any, error) {
	value, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("timestamp source must be text, got %T", raw)
	}
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -0700 MST",
		"2006-01-02 15:04:05.999999999 -0700",
		"2006-01-02 15:04:05.999999999-07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		parsed, err := time.ParseInLocation(layout, value, time.UTC)
		if err == nil {
			return &parsed, nil
		}
	}
	return nil, fmt.Errorf("invalid timestamp value")
}
