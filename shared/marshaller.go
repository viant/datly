package shared

import (
	"context"
	"fmt"
)

// Unmarshal converts data to destination, destination has to be a pointer to desired output type
type Unmarshal func(data []byte, destination interface{}) error

// Marshal converts source to byte array
type Marshal func(src interface{}) ([]byte, error)

// XLSUnmarshaller decodes an XLS/XLSX request body into the receiver.
type XLSUnmarshaller interface {
	UnmarshalXLS(ctx context.Context, data []byte) error
}

// DecodeXLS dispatches to an XLS/XLSX-aware request-body decoder on dest.
func DecodeXLS(ctx context.Context, data []byte, dest interface{}) error {
	switch actual := dest.(type) {
	case XLSUnmarshaller:
		return actual.UnmarshalXLS(ctx, data)
	default:
		return fmt.Errorf("xlsx request body is not supported for %T", dest)
	}
}
