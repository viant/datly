package contract

import (
	"context"
	"fmt"

	"github.com/viant/bindly"
	"github.com/viant/datly/exec"
	"github.com/viant/x/shape"
)

// ResolveDiscoveryRepresentations evaluates each provider once in the current
// operation and composes complete output branches through the ordinary owner
// traversal and bounded variant cache. It never retains policies or binders.
func (p *Plan) ResolveDiscoveryRepresentations(ctx context.Context, binder bindly.Binder) (*shape.Representation, []*shape.Representation, []*exec.OutputDiscoverySchemas, error) {
	owners, err := p.resolveOwners(ctx, binder)
	if err != nil {
		return nil, nil, nil, err
	}
	selected, err := p.resolvePolicies(ctx, owners)
	if err != nil {
		return nil, nil, nil, err
	}
	var representation *shape.Representation
	if selected != nil {
		representation = selected.representation
	}
	combinations := [][]resolvedOwner{owners}
	alternatives := false
	for i, owner := range owners {
		if owner.policy == nil || len(owner.policy.DiscoveryAlternatives) == 0 {
			continue
		}
		alternatives = true
		// The registration's existing variant budget also bounds a discovery
		// expansion. No-cache registrations cannot admit a combinatorial catalog.
		if p.capacity == 0 || len(owner.policy.DiscoveryAlternatives) > p.capacity/len(combinations) {
			return nil, nil, nil, fmt.Errorf("output discovery alternatives exceed contract variant capacity %d at %s", p.capacity, owner.owner.path)
		}
		next := make([][]resolvedOwner, 0, len(combinations)*len(owner.policy.DiscoveryAlternatives))
		for _, alternative := range owner.policy.DiscoveryAlternatives {
			if err := ctx.Err(); err != nil {
				return nil, nil, nil, err
			}
			if alternative == nil || len(alternative.DiscoveryAlternatives) != 0 {
				return nil, nil, nil, fmt.Errorf("output discovery alternative at %s must be a non-nil leaf policy", owner.owner.path)
			}
			for _, combination := range combinations {
				branch := append([]resolvedOwner(nil), combination...)
				policy := *alternative
				policy.NoCache = policy.NoCache || owner.policy.NoCache
				branch[i].policy = &policy
				next = append(next, branch)
			}
		}
		combinations = next
	}
	if !alternatives {
		return representation, nil, nil, nil
	}
	// A distinct selected variant occupies a retained slot too. At the expansion
	// boundary, count native retained identities before preparing any schemas;
	// overlapping branches and uncached/inactive policies need no extra slots.
	if len(combinations) == p.capacity && selected != nil && selected.name != "" {
		names := map[string]bool{selected.name: true}
		for _, combination := range combinations {
			if err := ctx.Err(); err != nil {
				return nil, nil, nil, err
			}
			_, name, err := selectPolicies(combination)
			if err != nil {
				return nil, nil, nil, err
			}
			if name != "" {
				names[name] = true
			}
		}
		if len(names) > p.capacity {
			return nil, nil, nil, fmt.Errorf("output discovery alternatives and selected policy exceed contract variant capacity %d", p.capacity)
		}
	}
	result := make([]*shape.Representation, 0, len(combinations))
	schemas := make([]*exec.OutputDiscoverySchemas, 0, len(combinations))
	for _, combination := range combinations {
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		branch, err := p.resolvePolicies(ctx, combination)
		if err != nil {
			return nil, nil, nil, err
		}
		var metadata *shape.Representation
		if branch != nil {
			metadata = branch.representation
		}
		document, err := p.prepareSchema(branch)
		if err != nil {
			return nil, nil, nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, nil, err
		}
		result = append(result, metadata)
		schemas = append(schemas, document)
	}
	return representation, result, schemas, nil
}
