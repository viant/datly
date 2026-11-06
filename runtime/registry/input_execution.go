package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bodyprovider "github.com/viant/bindly/provider/body"
	"github.com/viant/bindly/xform"
	"github.com/viant/datly/internal/selectorcompanion"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

// WithEffectiveInput compiles caller-effective transport admission into the
// original typed destination. Business handlers and their method sets retain
// canonical identity. The existing Bindly body decoder owns JSON and presence.
func (c *RouteInputContract) WithEffectiveInput(effective reflect.Type) (*RouteInputContract, error) {
	return c.withEffectiveInput(effective, nil)
}

func (c *RouteInputContract) WithInputRepresentation(metadata *xshape.Representation) (*RouteInputContract, error) {
	if metadata == nil || !metadata.Changed() {
		return c, nil
	}
	if metadata.Original() != c.Type() {
		return nil, fmt.Errorf("input representation does not match linked type")
	}
	return c.withEffectiveInput(c.Type(), metadata)
}
func (c *RouteInputContract) withEffectiveInput(effective reflect.Type, representation *xshape.Representation) (*RouteInputContract, error) {

	if effective == c.Type() && representation == nil {
		return c, nil
	}
	metadata, err := c.metadataFields(effective, representation, c.fields)
	if err != nil {
		return nil, err
	}
	bindings := make([]bindly.BindingSpec, 0, len(c.fields))
	retained := make([]InputField, 0, len(c.fields))
	wireTypes := map[string]reflect.Type{}
	for i, original := range c.fields {
		updated := metadata[i]
		binding := cloneBindingSpec(original.binding)
		if updated.metadataField != nil {
			binding, err = EnumBinding(binding, *updated.metadataField)
			if err != nil {
				return nil, err
			}
		}
		transport := (spec.BindSource{Kind: binding.Location.Kind}).RequestValue()
		if transport {
			hidden, err := updated.Internal()
			if err != nil {
				return nil, err
			}
			wasHidden, err := original.Internal()
			if err != nil {
				return nil, err
			}
			if hidden && !wasHidden && !original.verifiedJWT {
				continue
			}
			if required, changed := updated.EffectiveRequired(); changed {
				binding.Required = &required
			}
		}
		if transport && updated.representation != nil {
			if configured, ok := binding.Transformer.(interface {
				WithRepresentation(*xshape.Representation) (xform.Transformer, error)
			}); ok {
				binding.Transformer, err = configured.WithRepresentation(updated.representation)
				if err != nil {
					return nil, err
				}
			} else if binding.Transformer != nil || original.SourceType() != original.destinationType || binding.Location.Kind != "body" {
				return nil, fmt.Errorf("effective input %s requires its decoder's representation support", original.path)
			} else {
				binding.SourceType = reflect.TypeFor[json.RawMessage]()
				binding.Transformer = &inputRepresentation{target: original.destinationType, raw: true, metadata: updated.representation}
			}
		} else if transport && original.destinationType != updated.destinationType {
			var decoder xform.Transformer
			if configured, ok := binding.Transformer.(interface {
				WithEffectiveType(reflect.Type) (xform.Transformer, error)
			}); ok {
				decoder, err = configured.WithEffectiveType(updated.destinationType)
				if err != nil {
					return nil, err
				}
			} else if binding.Transformer != nil || original.SourceType() != original.destinationType {
				return nil, fmt.Errorf("effective input %s requires its codec's wire projection", original.path)
			}
			projection, err := xshape.CompileProjection(updated.destinationType, original.destinationType)
			if err != nil {
				return nil, err
			}
			binding.SourceType = updated.destinationType
			transform := &inputRepresentation{projection: projection, target: updated.destinationType, decoder: decoder}
			if binding.Location.Kind == "body" {
				binding.SourceType = reflect.TypeFor[json.RawMessage]()
				transform.raw = true
			}
			binding.Transformer = transform
		}
		if original.wireType != nil {
			wireTypes[original.path] = original.wireType
		}
		bindings = append(bindings, binding)
		retained = append(retained, original)
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		return nil, err
	}
	plan, err := injector.CompilePlan(c.inputType, bindings...)
	if err != nil {
		return nil, err
	}
	var selectors *selectorcompanion.BindingPlan
	if c.selectors != nil {
		selectors, err = c.selectors.ForPublicPlan(plan, bindings)
		if err != nil {
			return nil, err
		}
	}
	contract, err := NewInputContract(c.inputType, c.contract.projection, RouteInput{Route: c.route, Plan: plan, Bindings: bindings, WireTypes: wireTypes, Selectors: selectors})
	if err != nil {
		return nil, err
	}
	result, ok := contract.ForRoute(c.route)
	if !ok {
		return nil, fmt.Errorf("effective input route missing")
	}
	for i := range result.fields {
		binding := result.fields[i].binding
		result.fields[i] = retained[i]
		result.fields[i].binding = binding
	}
	return result, nil
}

type inputRepresentation struct {
	metadata   *xshape.Representation
	decoder    xform.Transformer
	projection *xshape.ValueProjection
	target     reflect.Type
	raw        bool
}

func (p *inputRepresentation) WireSourceType() reflect.Type { return p.target }

func (p *inputRepresentation) Transform(ctx context.Context, resolver locator.Resolver, value any) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.decoder != nil {
		var err error
		value, err = p.decoder.Transform(ctx, resolver, value)
		if err != nil {
			return nil, err
		}
	} else if p.raw && value != nil {
		raw, ok := value.(json.RawMessage)
		if !ok {
			return nil, fmt.Errorf("effective JSON body requires native raw source, got %T", value)
		}
		if err := bodyprovider.ValidateRequired(raw, p.target, p.metadata); err != nil {
			return nil, err
		}
		source, err := bodyprovider.New(raw, "application/json", nil, bodyprovider.WithRepresentation(p.metadata))
		if err != nil {
			return nil, err
		}
		value, _, err = source.Value(ctx, p.target, "")
		if err != nil {
			return nil, err
		}
	}
	if p.projection == nil {
		return value, nil
	}
	return p.projection.Apply(value)
}
