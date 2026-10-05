package provider

import (
	"context"
	"github.com/google/uuid"
	"github.com/viant/bindly/locator"
	"reflect"
	"strings"
	"time"
)

// Generator supplies the original declared scalar generators through ordinary
// invocation binding. Explicit generator registrations retain their authority.
func Generator() locator.Provider {
	return Named("generator", func(_ context.Context, _ reflect.Type, name string) (any, bool, error) {
		switch strings.ToLower(name) {
		case "nil":
			return nil, true, nil
		case "true":
			return true, true, nil
		case "false":
			return false, true, nil
		case "zero":
			return 0, true, nil
		case "one":
			return 1, true, nil
		case "empty":
			return "", true, nil
		case "now", "current_time":
			return time.Now(), true, nil
		case "uuid":
			return uuid.New().String(), true, nil
		}
		return nil, false, nil
	})
}
