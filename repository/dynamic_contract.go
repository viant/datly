package repository

import (
	"context"
	"fmt"
	"github.com/viant/datly/repository/content"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/tags"
	"mime"
	"net/http"
	"reflect"
	"strings"
	"sync"

	"github.com/viant/bindly"
	dynamic "github.com/viant/datly/service/contract"
	"github.com/viant/x/shape"
)

type contractPlanKey struct {
	method, path, direction string
	typ                     reflect.Type
}
type contractPlanEntry struct {
	once sync.Once
	plan *dynamic.Plan
	err  error
}

// ResolveRepresentation resolves caller metadata in this component generation.
// Cache namespaces include the declared route and direction. Requests and policy
// closures are never retained in the component cache.
func (c *Component) ResolveRepresentation(ctx context.Context, binder bindly.Binder, method, path, direction string, typ reflect.Type) (*shape.Representation, error) {
	if typ == nil {
		return nil, nil
	}
	key := contractPlanKey{method, path, direction, typ}
	actual, _ := c.contractPlanStore().LoadOrStore(key, &contractPlanEntry{})
	entry := actual.(*contractPlanEntry)
	entry.once.Do(func() { entry.plan, entry.err = dynamic.New(typ, 128) })
	if entry.err != nil {
		return nil, entry.err
	}
	return entry.plan.ResolveRepresentation(ctx, binder)
}

// EffectiveContract contains immutable metadata selected for one invocation.
// Nil members preserve the existing linked-type paths.
type EffectiveContract struct {
	Input, Body, Output *shape.Representation
}

func (c *Component) ResolveContract(ctx context.Context, binder bindly.Binder, method, path string) (*EffectiveContract, error) {
	if selected, ok := ctx.Value(resolvedContractKey{}).(resolvedContract); ok && selected.component == c && selected.method == method && selected.path == path {
		return selected.effective, nil
	}
	result := &EffectiveContract{}
	var err error
	if c.Input.Type.Schema != nil {
		result.Input, err = c.ResolveRepresentation(ctx, binder, method, path, "input", c.Input.Type.Schema.Type())
		if err != nil {
			return nil, err
		}
	}
	if c.Input.Body.Schema != nil {
		if p := c.Input.Type.Parameters.LookupByLocation("body", ""); p != nil && result.Input != nil {
			result.Body, err = result.Input.Lookup(p.Name)
		} else if result.Input != nil {
			mapping := map[string]string{}
			for _, p := range c.Input.Type.Parameters {
				if p.In != nil && p.In.Kind == "body" && p.In.Name != "" {
					mapping[state.SanitizeTypeName(p.In.Name)] = p.Name
				}
			}
			result.Body, err = dynamic.Project(c.Input.Body.Schema.Type(), result.Input, func(path string) string {
				first, rest, _ := strings.Cut(path, ".")
				canonical := mapping[first]
				if canonical == "" {
					return ""
				}
				if rest != "" {
					canonical += "." + rest
				}
				return canonical
			})
		} else if c.Input.Type.Schema == nil || c.Input.Type.Schema.Type() == nil {
			result.Body, err = c.ResolveRepresentation(ctx, binder, method, path, "body", c.Input.Body.Schema.Type())
		}
		if err != nil {
			return nil, err
		}
	}
	if c.Output.Type.Schema != nil {
		result.Output, err = c.ResolveRepresentation(ctx, binder, method, path, "output", c.Output.Type.Schema.Type())
	}
	return result, err
}

// ValidateContractCodec rejects unsupported representation owners before any
// business operation runs. Unchanged linked contracts use the configured path.
func (c *Component) ValidateContractCodec(effective *EffectiveContract, request *http.Request) error {
	if effective == nil {
		return nil
	}
	if effective.Body != nil && effective.Body.Changed() {
		mediaType, _, _ := mime.ParseMediaType(request.Header.Get("Content-Type"))
		if mediaType != "" && mediaType != content.JSONContentType {
			return fmt.Errorf("dynamic contract input requires JSON")
		}
		if c.Content.Marshaller.JSON.ContractDecoder() == nil {
			return fmt.Errorf("configured JSON decoder does not support contract metadata")
		}
		if len(c.UnmarshallerInterceptors()) > 0 {
			return fmt.Errorf("dynamic contract does not support legacy JSON interceptors")
		}
	}
	if effective.Output != nil && effective.Output.Changed() {
		if c.Output.Format(request.URL.Query()) != content.JSONFormat {
			return fmt.Errorf("dynamic contract output requires JSON")
		}
		if c.Content.Marshaller.JSON.ContractEncoder() == nil {
			return fmt.Errorf("configured JSON encoder does not support contract metadata")
		}
	}
	return nil
}

type resolvedContractKey struct{}
type resolvedContract struct {
	component    *Component
	method, path string
	effective    *EffectiveContract
}

// WithResolvedContract shares one policy selection within a single invocation,
// across an MCP adapter and its routed HTTP handler. It is not an auth cache.
func WithResolvedContract(ctx context.Context, c *Component, method, path string, effective *EffectiveContract) context.Context {
	return context.WithValue(ctx, resolvedContractKey{}, resolvedContract{c, method, path, effective})
}

// HasDynamicContract checks linked hook ownership without needing a request scope.
func (c *Component) HasDynamicContract(method, path string) (bool, error) {
	candidates := []struct {
		direction string
		typ       reflect.Type
	}{}
	if c.Input.Type.Schema != nil {
		candidates = append(candidates, struct {
			direction string
			typ       reflect.Type
		}{"input", c.Input.Type.Schema.Type()})
	}
	if c.Output.Type.Schema != nil {
		candidates = append(candidates, struct {
			direction string
			typ       reflect.Type
		}{"output", c.Output.Type.Schema.Type()})
	}
	if c.Input.Type.Schema == nil && c.Input.Body.Schema != nil {
		candidates = append(candidates, struct {
			direction string
			typ       reflect.Type
		}{"body", c.Input.Body.Schema.Type()})
	}
	for _, candidate := range candidates {
		if candidate.typ == nil {
			continue
		}
		key := contractPlanKey{method, path, candidate.direction, candidate.typ}
		actual, _ := c.contractPlanStore().LoadOrStore(key, &contractPlanEntry{})
		entry := actual.(*contractPlanEntry)
		entry.once.Do(func() { entry.plan, entry.err = dynamic.New(candidate.typ, 128) })
		if entry.err != nil {
			return false, entry.err
		}
		if entry.plan.Dynamic() {
			return true, nil
		}
	}
	return false, nil
}

// EffectiveParameter projects caller metadata without mutating native bindings.
func EffectiveParameter(parameter *state.Parameter, metadata *shape.Representation) (*state.Parameter, bool) {
	if metadata == nil || parameter.In == nil {
		return parameter, false
	}
	switch parameter.In.Kind {
	case state.KindQuery, state.KindHeader, state.KindPath, state.KindRequestBody, state.KindForm:
	default:
		return parameter, false
	}
	for metadata.Original().Kind() == reflect.Pointer {
		metadata = metadata.Elem()
	}
	original, ok := metadata.Original().FieldByName(parameter.Name)
	if !ok || len(original.Index) != 1 {
		return parameter, false
	}
	field := metadata.Field(original.Index[0])
	if field.Tag == original.Tag {
		return parameter, false
	}
	if field.Tag.Get("internal") == "true" || strings.Split(field.Tag.Get("json"), ",")[0] == "-" {
		return parameter, true
	}
	copy := *parameter
	copy.Tag = string(field.Tag)
	before, beforeExplicit := fieldRequiredness(original.Tag)
	after, afterExplicit := fieldRequiredness(field.Tag)
	if before != after || beforeExplicit != afterExplicit {
		copy.Required = &after
	}
	return &copy, false
}

// Components are also embedded in legacy route values. Keep the cache behind a
// pointer so copying route configuration never copies a used synchronization primitive.
type contractPlanCache struct {
	generation int64
	plans      sync.Map
}

var contractPlanInit sync.Mutex

func (c *Component) contractPlanStore() *sync.Map {
	contractPlanInit.Lock()
	defer contractPlanInit.Unlock()
	if c.contractPlans == nil || c.contractPlans.generation != c.SCN {
		c.contractPlans = &contractPlanCache{generation: c.SCN}
	}
	return &c.contractPlans.plans
}

func decoderRepresentation(metadata *shape.Representation, destination any) (*shape.Representation, error) {
	typ := reflect.TypeOf(destination)
	if typ == nil || typ.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("contract decoder destination must be a pointer")
	}
	typ = typ.Elem()
	for metadata != nil && metadata.Original() != typ && metadata.Original().Kind() == reflect.Pointer {
		metadata = metadata.Elem()
	}
	if metadata == nil || metadata.Original() != typ {
		return nil, fmt.Errorf("contract metadata does not match decoder destination %T", destination)
	}
	return metadata, nil
}

func fieldRequiredness(tag reflect.StructTag) (bool, bool) {
	if value, ok := tag.Lookup("optional"); ok && value != "false" {
		return false, true
	}
	if value, ok := tag.Lookup("required"); ok {
		return value == "" || value == "true", true
	}
	if parsed, err := tags.ParseStateTags(tag, nil); err == nil && parsed.Parameter != nil && parsed.Parameter.Required != nil {
		return *parsed.Parameter.Required, true
	}
	return false, false
}
