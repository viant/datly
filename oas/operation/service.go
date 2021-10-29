package operation

import (
	"context"
	"github.com/viant/datly/oas/spec"
)

type Service interface {
	Do(ctx context.Context, request *spec.Request, response *spec.Response)
}
