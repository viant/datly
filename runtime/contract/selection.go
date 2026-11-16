package contract

import (
	"context"

	"github.com/viant/bindly"
	"github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

// MetadataIdentity belongs to a selected immutable metadata variant, not an
// authorization grant. Empty/NoCache policies remain operation-local even when
// their selected representation happens to be unchanged.
type MetadataIdentity struct {
	Name      string
	Cacheable bool
	selected  *variant
}

func identityFor(owners []resolvedOwner) (MetadataIdentity, error) {
	_, name, err := selectPolicies(owners)
	if err != nil {
		return MetadataIdentity{}, err
	}
	result := MetadataIdentity{Name: name, Cacheable: true}
	for _, owner := range owners {
		if owner.policy != nil && (owner.policy.NoCache || owner.policy.Field != nil && owner.policy.Name == "") {
			result.Cacheable = false
		}
	}
	return result, nil
}

// PrepareProjection keeps the same canonical walker and variant budget for a
// native derived policy even when its linked input has no Contract methods.
// Registration calls this before publication; it is never a request mutation.
func (p *Plan) PrepareProjection() error {
	if p.walk != nil {
		return nil
	}
	var err error
	p.walk, err = shape.CompileRewrite(p.original)
	return err
}

func (p *Plan) ResolvePreparedInputSelection(ctx context.Context, binder bindly.Binder, extra *xcontract.Policy) (*shape.Representation, any, MetadataIdentity, error) {
	owners, err := p.resolveOwners(ctx, binder)
	if err != nil {
		return nil, nil, MetadataIdentity{}, err
	}
	if extra != nil {
		owners = append([]resolvedOwner{{owner: contractOwner{typ: p.original}, policy: extra, canonical: true}}, owners...)
	}
	identity, err := identityFor(owners)
	if err != nil {
		return nil, nil, identity, err
	}
	selected, err := p.resolvePolicies(ctx, owners)
	if err != nil {
		return nil, nil, identity, err
	}
	identity.selected = selected
	metadata, prepared, err := p.prepareInput(ctx, selected)
	return metadata, prepared, identity, err
}

func (p *Plan) ResolveRepresentationSelection(ctx context.Context, binder bindly.Binder) (*shape.Representation, MetadataIdentity, error) {
	owners, err := p.resolveOwners(ctx, binder)
	if err != nil {
		return nil, MetadataIdentity{}, err
	}
	identity, err := identityFor(owners)
	if err != nil {
		return nil, identity, err
	}
	selected, err := p.resolvePolicies(ctx, owners)
	if err != nil || selected == nil {
		return nil, identity, err
	}
	return selected.representation, identity, nil
}

// PrepareProjection attaches one registration-owned immutable artifact to the
// same bounded input variant. Uncached variants never enter shared retention.
// Successful preparation retains no callback or operation state; failures retry.
func (i MetadataIdentity) PrepareProjection(ctx context.Context, prepare func() (any, error)) (any, error) {
	if prepare == nil {
		return nil, nil
	}
	var transient inputPreparation
	artifact := &transient
	if i.selected != nil {
		artifact = &i.selected.projection
	}
	artifact.mu.Lock()
	defer artifact.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if artifact.ready {
		return artifact.value, nil
	}
	value, err := prepare()
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	artifact.value, artifact.ready = value, true
	return value, nil
}
