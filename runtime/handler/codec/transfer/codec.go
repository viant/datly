// Package transfer implements the built-in transform codec from the original
// Datly transfer tags using the reusable datly/transform planner.
package transfer

import (
	"context"
	"fmt"
	"reflect"

	"github.com/viant/datly/transform"
	xcodec "github.com/viant/xdatly/codec"
)

const Name = "transform"

// Factory exposes the normal xdatly codec factory. Named nested codecs are
// supplied explicitly; no codec names are hardcoded in transfer evaluation.
type Factory struct {
	Codecs map[string]xcodec.Instance
}

var _ xcodec.Factory = Factory{}

func (f Factory) New(config *xcodec.Config, _ ...xcodec.Option) (xcodec.Instance, error) {
	if config == nil {
		return nil, fmt.Errorf("transform codec config is required")
	}
	if config.SourceType == nil || config.DestinationType == nil {
		return nil, fmt.Errorf("transform source and destination types are required")
	}
	if len(config.Args) != 0 {
		return nil, fmt.Errorf("transform codec takes mappings from destination transfer tags, not arguments")
	}
	tags, err := transform.Tags(config.DestinationType)
	if err != nil {
		return nil, err
	}
	mappings := make([]transform.Mapping, 0, len(tags))
	for _, tag := range tags {
		path, err := transform.CompileSelector(tag.From)
		if err != nil {
			return nil, fmt.Errorf("transfer field %s: %w", tag.To, err)
		}
		mapping := transform.Mapping{From: path, To: tag.To, Required: true}
		if tag.Codec != "" {
			nested := f.Codecs[tag.Codec]
			if nested == nil {
				return nil, fmt.Errorf("transfer field %s requires codec %q", tag.To, tag.Codec)
			}
			mapping.Transform = func(ctx context.Context, value any) (any, error) {
				return nested.Value(ctx, value)
			}
		}
		mappings = append(mappings, mapping)
	}
	plan, err := transform.CompileFor(config.SourceType, config.DestinationType, mappings)
	if err != nil {
		return nil, err
	}
	pointerDepth := 0
	for destination := config.DestinationType; destination.Kind() == reflect.Pointer; destination = destination.Elem() {
		pointerDepth++
	}
	return &instance{plan: plan, pointerDepth: pointerDepth}, nil
}

type instance struct {
	plan         *transform.Plan
	pointerDepth int
}

var _ xcodec.Instance = (*instance)(nil)

func (i *instance) Value(ctx context.Context, raw interface{}, _ ...xcodec.Option) (interface{}, error) {
	if i == nil || i.plan == nil {
		return nil, fmt.Errorf("transform codec is not initialized")
	}
	value, err := i.plan.Value(ctx, raw)
	if err != nil {
		return value, err
	}
	for depth := 0; depth < i.pointerDepth; depth++ {
		result := reflect.New(reflect.TypeOf(value))
		result.Elem().Set(reflect.ValueOf(value))
		value = result.Interface()
	}
	return value, nil
}
