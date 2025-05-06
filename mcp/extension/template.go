package extension

import (
	"context"
	"github.com/viant/mcp-protocol/schema"
)

type ResourceTemplateHandlerFunc func(ctx context.Context, request *schema.ReadResourceRequestParams) ([]schema.ReadResourceResultContentsElem, error)
type ResourceTemplate struct {
	schema.ResourceTemplate
	Handler ResourceTemplateHandlerFunc
}

type ResourceTemplates []*ResourceTemplate
