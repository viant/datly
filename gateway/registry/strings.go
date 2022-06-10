package registry

import (
	"context"
	"strings"
)

type AsStrings struct {
}

func (s *AsStrings) Value(ctx context.Context, raw string, options ...interface{}) (interface{}, error) {
	return strings.Split(raw, ","), nil
}
