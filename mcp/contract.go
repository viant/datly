package mcp

import (
	"context"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/mcp/invocation"
	"github.com/viant/datly/mcp/tool"
	"github.com/viant/mcp-protocol/schema"
)

// DescribeTool applies this caller's contract after catalog authorization. Tools
// without component plans (such as resource bridges) retain their own metadata.
func (s *Service) DescribeTool(ctx context.Context, metadata schema.Tool) (schema.Tool, bool, error) {
	if s.lazy != nil {
		return s.lazy.service().DescribeTool(ctx, metadata)
	}
	if s.contractResolver == nil {
		return metadata, false, nil
	}
	plan, ok := s.catalog.Tool(metadata.Name)
	if !ok {
		return metadata, false, nil
	}
	ctx = invocation.WithMCPContext(ctx, s.client)
	scope := invocation.AuthorizationScope(ctx)
	defer scope.Close()
	resolve := s.contractResolver.ResolveContract
	if discovery, ok := s.contractResolver.(exec.DiscoveryContractResolver); ok {
		resolve = discovery.ResolveDiscoveryContract
	}
	effective, err := resolve(ctx, plan.Target(), scope)
	if err != nil {
		return schema.Tool{}, false, err
	}
	updated, err := plan.EffectiveMetadata(effective)
	if err != nil {
		return schema.Tool{}, false, err
	}
	dynamic := effective.Dynamic
	for _, variant := range plan.PathVariants() {
		selected, e := resolve(ctx, variant.Plan.Target(), scope)
		if e != nil {
			return schema.Tool{}, false, e
		}
		alternative, e := variant.Plan.EffectiveMetadata(selected)
		if e != nil {
			return schema.Tool{}, false, e
		}
		updated, e = tool.MergePathMetadata(updated, alternative)
		if e != nil {
			return schema.Tool{}, false, e
		}
		dynamic = dynamic || selected.Dynamic
	}
	metadata.InputSchema = updated.InputSchema
	// The upstream handler may have removed output schema for an old protocol.
	if metadata.OutputSchema != nil {
		metadata.OutputSchema = updated.OutputSchema
	}
	return metadata, dynamic, nil
}
