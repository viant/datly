package registry

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/viant/bindly"
	docs "github.com/viant/datly/documentation"
	"github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
	xcodec "github.com/viant/xdatly/codec"
)

// RouteInput is compiler output used to construct one immutable route contract.
type RouteInput struct {
	Route    spec.RouteRef
	Plan     *bindly.Plan
	Bindings []bindly.BindingSpec
}

// InputField is one canonical route-effective component input field.
type InputField struct {
	owner           reflect.Type
	origin          spec.RouteRef
	documentation   *docs.Snapshot
	path            string
	destinationType reflect.Type
	binding         bindly.BindingSpec
	anonymous       bool
	dependency      *spec.RouteRef
	verifiedJWT     bool
	wireSchema      *spec.WireSchema
	wireSchemas     map[string]*spec.WireSchema
}

// RouteInputContract is the exact binding contract for one component route.
type RouteInputContract struct {
	route     spec.RouteRef
	inputType reflect.Type
	plan      *bindly.Plan
	contract  *InputContract
	fields    []InputField
	replay    *bindly.ReplayPlan
	replayErr error
}

// InputContract owns the canonical component input type and its route-effective
// plans. It contains no provider or invocation state.
type InputContract struct {
	typeOf     reflect.Type
	projection *bindly.Projection
	routes     map[string]*RouteInputContract
}

func NewInputContract(inputType reflect.Type, projection *bindly.Projection, routes ...RouteInput) (*InputContract, error) {
	inputType = (xshape.Runtime{}).Indirect(inputType)
	if inputType == nil || inputType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("component input type must be a struct, got %v", inputType)
	}
	if projection == nil {
		return nil, fmt.Errorf("component input projection is required")
	}
	if projection.TargetType() != inputType {
		return nil, fmt.Errorf("component input projection targets %v, want %v", projection.TargetType(), inputType)
	}
	if len(routes) == 0 {
		return nil, fmt.Errorf("component input contract requires at least one route")
	}
	result := &InputContract{
		typeOf: inputType, projection: projection,
		routes: make(map[string]*RouteInputContract, len(routes)),
	}
	for _, route := range routes {
		key := route.Route.String()
		if key == "" {
			return nil, fmt.Errorf("route input contract requires METHOD:/path identity")
		}
		if route.Plan == nil {
			return nil, fmt.Errorf("route input contract %s requires a Bindly plan", key)
		}
		if _, ok := result.routes[key]; ok {
			return nil, fmt.Errorf("duplicate route input contract %s", key)
		}
		fields := make([]InputField, 0, len(route.Bindings))
		for _, binding := range route.Bindings {
			destinationField, err := xshape.Linked(inputType).StructField(binding.Path)
			if err != nil {
				return nil, fmt.Errorf("route input contract %s: %w", key, err)
			}
			destinationType := destinationField.Type
			anonymous, err := inputFieldAnonymous(destinationField, binding.Extension)
			if err != nil {
				return nil, fmt.Errorf("route input contract %s field %s: %w", key, binding.Path, err)
			}
			binding = cloneBindingSpec(binding)
			if binding.SourceType == nil {
				binding.SourceType = destinationType
			}
			var dependency *spec.RouteRef
			if binding.Location.Kind == "component" {
				target, err := spec.ParseRouteRef(binding.Location.In)
				if err != nil {
					return nil, fmt.Errorf("route %s input %s: %w", key, binding.Path, err)
				}
				dependency = &target
			}
			verifiedJWT := false
			if transformer, ok := binding.Transformer.(interface{ Codec() xcodec.Instance }); ok {
				verifiedJWT = auth.VerifiesJWT(transformer.Codec())
			}
			wireSchema, wireSchemas, err := bindingWireSchema(binding)
			if err != nil {
				return nil, fmt.Errorf("route input contract %s field %s: %w", key, binding.Path, err)
			}
			fields = append(fields, InputField{
				owner: inputType, origin: route.Route, path: binding.Path, destinationType: destinationType, binding: binding, anonymous: anonymous, dependency: dependency, verifiedJWT: verifiedJWT,
				wireSchema: wireSchema, wireSchemas: wireSchemas,
			})
		}
		var replayPaths []string
		for _, field := range fields {
			if (spec.BindSource{Kind: field.binding.Location.Kind}).RequestValue() {
				replayPaths = append(replayPaths, field.path)
			}
		}
		replay, replayErr := route.Plan.Replay(replayPaths...)
		if replayErr == nil {
			var verifiedPaths []string
			for _, field := range fields {
				if field.verifiedJWT {
					verifiedPaths = append(verifiedPaths, field.path)
				}
			}
			replay, replayErr = replay.Prepare(projection, verifiedPaths...)
		}

		result.routes[key] = &RouteInputContract{
			route: route.Route, inputType: inputType, plan: route.Plan, contract: result, fields: fields, replay: replay, replayErr: replayErr,
		}
	}
	return result, nil
}

func (c *InputContract) Type() reflect.Type {
	if c == nil {
		return nil
	}
	return c.typeOf
}

// Resolver binds the canonical immutable projection to one component input.
func (c *InputContract) Resolver(target any) bindly.ValueResolver {
	if c == nil {
		return nil
	}
	return c.projection.Resolver(target)
}

func (c *InputContract) ForRoute(route spec.RouteRef) (*RouteInputContract, bool) {
	if c == nil {
		return nil, false
	}
	result, ok := c.routes[route.String()]
	return result, ok
}

func (c *RouteInputContract) Route() spec.RouteRef {
	if c == nil {
		return spec.RouteRef{}
	}
	return c.route
}

func (c *RouteInputContract) Type() reflect.Type {
	if c == nil {
		return nil
	}
	return c.inputType
}

func (c *RouteInputContract) Plan() *bindly.Plan {
	if c == nil {
		return nil
	}
	return c.plan
}

func (c *RouteInputContract) Resolver(target any) bindly.ValueResolver {
	if c == nil || c.contract == nil {
		return nil
	}
	return c.contract.Resolver(target)
}

// Without projects a detached input through the canonical Bindly alias owner.
func (c *RouteInputContract) Without(target any, names ...string) (any, error) {
	if c == nil || c.contract == nil {
		return nil, fmt.Errorf("input contract is required")
	}
	return c.contract.projection.Without(target, names...)
}

// WithOptionalBindings returns a detached route contract with matching bindings
// marked optional. It is used by trusted runtime flows that intentionally remove
// server-owned inputs while preserving all other canonical binding semantics.
func (c *RouteInputContract) WithOptionalBindings(names ...string) (*RouteInputContract, error) {
	if c == nil || c.contract == nil {
		return nil, fmt.Errorf("input contract is required")
	}
	optional := map[string]bool{}
	for _, name := range names {
		if normalized := normalizeInputName(name); normalized != "" {
			optional[normalized] = true
		}
	}
	if len(optional) == 0 {
		return c, nil
	}
	bindings := make([]bindly.BindingSpec, 0, len(c.fields))
	matched := map[string]int{}
	for _, field := range c.fields {
		binding := cloneBindingSpec(field.binding)
		keys := inputFieldMatchKeys(field)
		for _, key := range keys {
			if optional[key] {
				value := false
				binding.Required = &value
				matched[key]++
				break
			}
		}
		bindings = append(bindings, binding)
	}
	for name := range optional {
		switch matched[name] {
		case 0:
			return nil, fmt.Errorf("optional input binding %q is not defined for route %s", name, c.route.String())
		case 1:
		default:
			return nil, fmt.Errorf("optional input binding %q is ambiguous for route %s", name, c.route.String())
		}
	}
	injector, err := bindly.NewInjector()
	if err != nil {
		return nil, err
	}
	plan, err := injector.CompilePlan(c.inputType, bindings...)
	if err != nil {
		return nil, err
	}
	contract, err := NewInputContract(c.inputType, c.contract.projection, RouteInput{Route: c.route, Plan: plan, Bindings: bindings})
	if err != nil {
		return nil, err
	}
	result, ok := contract.ForRoute(c.route)
	if !ok {
		return nil, fmt.Errorf("rebuilt route input contract not found: %s", c.route.String())
	}
	return result, nil
}

func (c *RouteInputContract) Fields() []InputField {
	if c == nil {
		return nil
	}
	result := make([]InputField, len(c.fields))
	for index, field := range c.fields {
		result[index] = field
		result[index].binding = cloneBindingSpec(field.binding)
		result[index].wireSchema = field.wireSchema.Clone()
		result[index].wireSchemas = cloneWireSchemas(field.wireSchemas)
	}
	return result
}

func (f InputField) Path() string                  { return f.path }
func (f InputField) DestinationType() reflect.Type { return f.destinationType }
func (f InputField) SourceType() reflect.Type      { return f.binding.SourceType }
func (f InputField) Binding() bindly.BindingSpec   { return cloneBindingSpec(f.binding) }
func (f InputField) Anonymous() bool               { return f.anonymous }
func (f InputField) WireSchema() *spec.WireSchema  { return f.wireSchema.Clone() }
func (f InputField) WireSchemas() map[string]*spec.WireSchema {
	return cloneWireSchemas(f.wireSchemas)
}

func inputFieldMatchKeys(field InputField) []string {
	var result []string
	appendKey := func(value string) {
		normalized := normalizeInputName(value)
		if normalized == "" {
			return
		}
		for _, existing := range result {
			if existing == normalized {
				return
			}
		}
		result = append(result, normalized)
	}
	appendKey(field.path)
	appendKey(field.binding.Name)
	appendKey(field.binding.Location.In)
	if parameter, ok := field.binding.Extension.(*spec.Parameter); ok && parameter != nil {
		appendKey(parameter.Name)
		appendKey(parameter.Source.Name)
	}
	return result
}

func normalizeInputName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	var builder strings.Builder
	for _, r := range value {
		switch {
		case r == '_' || r == '-' || r == '.' || r == ' ':
			continue
		default:
			builder.WriteRune(r)
		}
	}
	return strings.ToLower(builder.String())
}

func cloneBindingSpec(binding bindly.BindingSpec) bindly.BindingSpec {
	binding.Required = cloneBool(binding.Required)
	binding.Cacheable = cloneBool(binding.Cacheable)
	for _, limit := range []**int{&binding.MinAllowedRecords, &binding.MaxAllowedRecords, &binding.ExpectedReturned} {
		if *limit != nil {
			value := **limit
			*limit = &value
		}
	}
	return binding
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func bindingWireSchema(binding bindly.BindingSpec) (*spec.WireSchema, map[string]*spec.WireSchema, error) {
	var result *spec.WireSchema
	if param, ok := binding.Extension.(*spec.Parameter); ok && param != nil {
		result = param.WireSchema.Clone()
	}
	if result == nil {
		if provider, ok := binding.Transformer.(interface{ WireSchema() *spec.WireSchema }); ok {
			result = provider.WireSchema()
		}
	}
	if err := validateWireSchema(result); err != nil {
		return nil, nil, err
	}
	var nested map[string]*spec.WireSchema
	if param, ok := binding.Extension.(*spec.Parameter); ok && param != nil {
		nested = cloneWireSchemas(param.WireSchemas)
	}
	for key, schema := range nested {
		if err := validateWireSchema(schema); err != nil {
			return nil, nil, fmt.Errorf("nested wire schema %s: %w", key, err)
		}
	}
	return result.Clone(), nested, nil
}

func cloneWireSchemas(source map[string]*spec.WireSchema) map[string]*spec.WireSchema {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]*spec.WireSchema, len(source))
	for key, schema := range source {
		result[key] = schema.Clone()
	}
	return result
}

func validateWireSchema(schema *spec.WireSchema) error {
	if schema == nil {
		return nil
	}
	switch schema.Type {
	case "string":
	default:
		return fmt.Errorf("unsupported wire schema type %q", schema.Type)
	}
	switch schema.Format {
	case "", "date":
	default:
		return fmt.Errorf("unsupported wire schema format %q", schema.Format)
	}
	return nil
}

func inputFieldAnonymous(field reflect.StructField, extension any) (bool, error) {
	value, ok := field.Tag.Lookup("anonymous")
	if !ok {
		if param, isParam := extension.(*spec.Parameter); isParam && param != nil {
			value, ok = reflect.StructTag(param.Tag).Lookup("anonymous")
		}
	}
	if !ok || strings.TrimSpace(value) == "" {
		return false, nil
	}
	result, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false, fmt.Errorf("parse anonymous tag %q: %w", value, err)
	}
	return result, nil
}

// Dependency is the canonical exact target of a compiled component input.
func (f InputField) Dependency() (spec.RouteRef, bool) {
	if f.dependency == nil {
		return spec.RouteRef{}, false
	}
	return *f.dependency, true
}

// VerifiesJWT reports evidence from the constructed verifier codec, captured
// during registration independently of mutable source parameter metadata.
func (f InputField) VerifiesJWT() bool { return f.verifiedJWT }

// ReplayPlan is compiled from canonical external input authority at registration.
func (c *RouteInputContract) ReplayPlan() (*bindly.ReplayPlan, error) {
	if c == nil {
		return nil, fmt.Errorf("route input contract is required")
	}
	return c.replay, c.replayErr
}
