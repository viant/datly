package transform

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/body"
	"github.com/viant/bindly/state"
)

// Transform is an optional, caller-supplied value codec. Plans never choose
// codecs by name or retain invocation state.
type Transform func(context.Context, any) (any, error)

// Mapping connects a compiled source path to a typed destination field.
type Mapping struct {
	From      *Path
	To        string
	Required  bool
	Transform Transform
}

type entry struct {
	mapping Mapping
	name    string
}

// Plan is immutable and safe for concurrent invocations. Bindly owns all
// destination field access, pointer allocation, and JSON type conversion.
type Plan struct {
	source      reflect.Type
	destination reflect.Type
	injector    *bindly.Injector
	bindings    *bindly.Plan
	entries     []entry
}

func Compile(destination reflect.Type, mappings []Mapping) (*Plan, error) {
	return CompileFor(nil, destination, mappings)
}

// CompileFor additionally validates statically known source fields against a
// declared source type; no first-invocation source shape is cached.
func CompileFor(source, destination reflect.Type, mappings []Mapping) (*Plan, error) {
	for destination != nil && destination.Kind() == reflect.Pointer {
		destination = destination.Elem()
	}
	if destination == nil || destination.Kind() != reflect.Struct {
		return nil, fmt.Errorf("transform destination must be a struct")
	}
	if len(mappings) == 0 {
		return nil, fmt.Errorf("transform requires at least one mapping")
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		return nil, err
	}
	result := &Plan{source: source, destination: destination, injector: injector}
	specs := make([]bindly.BindingSpec, 0, len(mappings))
	for i, mapping := range mappings {
		if mapping.From == nil || mapping.To == "" {
			return nil, fmt.Errorf("mapping[%d] requires source and destination paths", i)
		}
		if source != nil {
			bound, err := mapping.From.ForType(source)
			if err != nil {
				return nil, fmt.Errorf("mapping[%d]: %w", i, err)
			}
			mapping.From = bound
		}
		name := "value" + strconv.Itoa(i)
		specs = append(specs, bindly.BindingSpec{
			Path: mapping.To, Location: state.Location{Kind: "body", In: name},
		})
		result.entries = append(result.entries, entry{mapping: mapping, name: name})
	}
	result.bindings, err = injector.CompilePlan(destination, specs...)
	if err != nil {
		return nil, fmt.Errorf("compile transform destination: %w", err)
	}
	return result, nil
}

// Apply reads one source value per mapping, then asks Bindly to populate the
// typed destination. JSON encoding preserves json.Number IDs and makes scalar
// conversion strict (a quoted number cannot populate an integer field).
func (p *Plan) Apply(ctx context.Context, source, destination any) error {
	if p == nil || p.bindings == nil {
		return fmt.Errorf("transform plan is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.source != nil {
		actual := reflect.TypeOf(source)
		if actual == nil || (!actual.AssignableTo(p.source) && !(actual.Kind() == reflect.Pointer && actual.Elem() == p.source)) {
			return fmt.Errorf("transform source must be %s, got %T", p.source, source)
		}
	}
	target := reflect.ValueOf(destination)
	if !target.IsValid() || target.Kind() != reflect.Pointer || target.IsNil() || target.Elem().Type() != p.destination {
		return fmt.Errorf("transform destination must be *%s", p.destination)
	}
	values := make(map[string]any, len(p.entries))
	for _, item := range p.entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		value, found, err := item.mapping.From.Select(source)
		if err != nil {
			return fmt.Errorf("source %q: %w", item.mapping.From, err)
		}
		if !found || value == nil {
			if item.mapping.Required {
				return fmt.Errorf("source path %q is missing for output %q", item.mapping.From, item.mapping.To)
			}
			continue
		}
		if item.mapping.Transform != nil && value != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			value, err = item.mapping.Transform(ctx, value)
			if err != nil {
				return fmt.Errorf("source %q: %w", item.mapping.From, err)
			}
		}
		values[item.name] = value
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return fmt.Errorf("encode transform values: %w", err)
	}
	sourceProvider, err := body.New(encoded, "application/json", nil, body.WithExactFieldNames())
	if err != nil {
		return err
	}
	injector, err := p.injector.ForScope(sourceProvider)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := injector.Bind(ctx, destination, bindly.WithPlan(p.bindings)); err != nil {
		var binding *bindly.BindingError
		if errors.As(err, &binding) {
			cause := err
			for next := errors.Unwrap(cause); next != nil; next = errors.Unwrap(cause) {
				cause = next
			}
			for _, item := range p.entries {
				if item.mapping.To != binding.Path {
					continue
				}
				return fmt.Errorf("source path %q cannot populate output %q: %w", item.mapping.From, binding.Path, cause)
			}
		}
		return err
	}
	return nil
}

// Value allocates and populates a new destination value for codec use.
func (p *Plan) Value(ctx context.Context, source any) (any, error) {
	if p == nil {
		return nil, fmt.Errorf("transform plan is required")
	}
	destination := reflect.New(p.destination)
	if err := p.Apply(ctx, source, destination.Interface()); err != nil {
		return nil, err
	}
	return destination.Elem().Interface(), nil
}
