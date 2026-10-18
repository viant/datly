package xlsbody

import (
	"context"
	"fmt"
	"reflect"
)

// unmarshalXLSBody retains the legacy context-aware body contract. Bindly
// supplies a pointer to the declared body type, including **T for *T bodies.
func Unmarshal(ctx context.Context, raw []byte, target any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	value := reflect.ValueOf(target)
	for value.IsValid() && value.Kind() == reflect.Pointer {
		if value.IsNil() {
			if !value.CanSet() {
				break
			}
			value.Set(reflect.New(value.Type().Elem()))
		}
		if decoder, ok := value.Interface().(interface {
			UnmarshalXLS(context.Context, []byte) error
		}); ok {
			return decoder.UnmarshalXLS(ctx, raw)
		}
		value = value.Elem()
	}
	return fmt.Errorf("XLSX body type %T does not implement UnmarshalXLS", target)
}
