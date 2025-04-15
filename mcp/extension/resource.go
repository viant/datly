package extension

import (
	"context"
	"github.com/viant/mcp/schema"
)

type ResourceHandlerFunc func(ctx context.Context, request *schema.ReadResourceRequestParams) ([]schema.ReadResourceResultContentsElem, error)
type Resource struct {
	schema.Resource
	Handler ResourceHandlerFunc
}

type Resources []*Resource
