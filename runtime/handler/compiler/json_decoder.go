package compiler

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	bodyprovider "github.com/viant/bindly/provider/body"
	"github.com/viant/bindly/xform"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

// configuredJSONDecoder keeps a configured decoder inside the native body
// source, including its filtering and presence ownership. The effective type
// changes; the configured decoder implementation does not.
type configuredJSONDecoder struct {
	metadataDecoder xcontract.JSONDecoder
	metadata        *xshape.Representation
	projection      *xshape.ValueProjection
	wireTarget      reflect.Type
	target          reflect.Type
	decoder         bodyprovider.Decoder
	required        bool
}

func (c *Compiler) configureJSONDecoder(bindings []bindly.BindingSpec) error {
	if c.input.Component == nil || c.input.Component.Settings == nil {
		return nil
	}
	name := strings.TrimSpace(c.input.Component.Settings.JSONUnmarshalType)
	if name == "" {
		return nil
	}
	if c.input.TypeLookup == nil {
		return fmt.Errorf("JSON decoder %s requires type authority", name)
	}
	typ, err := c.input.TypeLookup(name)
	if err != nil {
		return err
	}
	if typ == nil {
		return fmt.Errorf("JSON decoder %s was not found", name)
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	instance := reflect.New(typ).Interface()
	decoder, ok := instance.(bodyprovider.Decoder)
	if !ok {
		if unmarshal, valid := instance.(interface{ Unmarshal([]byte, any) error }); valid {
			decoder = bodyprovider.DecoderFunc(unmarshal.Unmarshal)
		} else {
			return fmt.Errorf("JSON decoder %s must implement Decode or Unmarshal([]byte,any) error", name)
		}
	}
	metadataDecoder, _ := instance.(xcontract.JSONDecoder)
	for i := range bindings {
		binding := &bindings[i]
		if binding.Location.Kind != "body" {
			continue
		}
		field, err := xshape.Linked(c.input.InputType).StructField(binding.Path)
		if err != nil {
			return err
		}
		// A field codec is a distinct explicit owner, not a global JSON decoder.
		if field.Tag.Get("codec") != "" {
			continue
		}
		if parameter, ok := binding.Extension.(*spec.Parameter); ok && parameter != nil && parameter.Codec != nil {
			continue
		}
		binding.SourceType = reflect.TypeFor[json.RawMessage]()
		binding.Transformer = &configuredJSONDecoder{target: field.Type, decoder: decoder, metadataDecoder: metadataDecoder}
		if err = binding.ValidateBodyNullPolicy(field.Type); err != nil {
			return err
		}
	}
	return nil
}

func (d *configuredJSONDecoder) WireSourceType() reflect.Type {
	if d.wireTarget != nil {
		return d.wireTarget
	}
	return d.target
}
func (d *configuredJSONDecoder) WithEffectiveType(target reflect.Type) (xform.Transformer, error) {
	copy := *d
	copy.target = target
	copy.required = true
	return &copy, nil
}
func (d *configuredJSONDecoder) Transform(ctx context.Context, _ locator.Resolver, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	raw, ok := value.(json.RawMessage)
	if !ok {
		return nil, fmt.Errorf("configured JSON decoder requires native raw input, got %T", value)
	}
	if d.required {
		if err := bodyprovider.ValidateRequired(raw, d.target, d.metadata); err != nil {
			return nil, err
		}
	}
	registry := bodyprovider.NewDecoderRegistry()
	if d.metadata != nil {
		registry.Register("application/json", metadataBodyDecoder{Decoder: d.decoder, JSONDecoder: d.metadataDecoder})
	} else {
		registry.Register("application/json", d.decoder)
	}
	source, err := bodyprovider.New(raw, "application/json", nil, bodyprovider.WithDecoders(registry), bodyprovider.WithRepresentation(d.metadata))
	if err != nil {
		return nil, err
	}
	result, _, err := source.Value(ctx, d.target, "")
	if err == nil && d.projection != nil {
		return d.projection.Apply(result)
	}
	return result, err
}

// WithRepresentation keeps the configured codec as owner. Legacy codecs use
// their existing reflected projection when possible; metadata-aware codecs keep
// the original receiver and native presence layout even with embedded methods.
func (d *configuredJSONDecoder) WithRepresentation(metadata *xshape.Representation) (xform.Transformer, error) {
	if metadata == nil || !metadata.Changed() {
		return d, nil
	}
	if metadata.Original() != d.target {
		return nil, fmt.Errorf("configured decoder metadata type mismatch")
	}
	copy := *d
	copy.required = true
	if d.metadataDecoder != nil {
		copy.metadata = metadata
		return &copy, nil
	}
	effective, err := metadata.Reflected()
	if err != nil {
		return nil, fmt.Errorf("configured decoder must implement contract.JSONDecoder for this representation: %w", err)
	}
	copy.projection, err = xshape.CompileProjection(effective, d.target)
	if err != nil {
		return nil, err
	}
	copy.wireTarget = d.target
	copy.target = effective
	return &copy, nil
}

type metadataBodyDecoder struct {
	bodyprovider.Decoder
	xcontract.JSONDecoder
}
