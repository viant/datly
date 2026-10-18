package registry

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

type coverageInput struct {
	Value         string
	First, Second *coverageInput
}

func coverageValue(required bool) bindly.BindingSpec {
	return bindly.BindingSpec{Path: "Value", Name: "Value", Location: state.Location{Kind: "query", In: "q"}, SourceType: reflect.TypeFor[string](), Required: &required}
}
func coverageEdge(field, target, scope string) bindly.BindingSpec {
	return bindly.BindingSpec{Path: field, Name: field, Location: state.Location{Kind: "component", In: "GET:" + target}, Scope: scope}
}
func coverageContract(t *testing.T, path string, bindings ...bindly.BindingSpec) *InputContract {
	t.Helper()
	injector, e := bindly.NewInjector()
	if e != nil {
		t.Fatal(e)
	}
	plan, e := injector.CompilePlan(reflect.TypeFor[coverageInput](), bindings...)
	if e != nil {
		t.Fatal(e)
	}
	projection, e := plan.Projection()
	if e != nil {
		t.Fatal(e)
	}
	c, e := NewInputContract(reflect.TypeFor[coverageInput](), projection, RouteInput{Route: spec.RouteRef{Method: "GET", Path: path}, Plan: plan, Bindings: bindings})
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func coverageCatalog(t *testing.T, nodes map[string][]bindly.BindingSpec) *InputCatalog {
	t.Helper()
	var entries []*RegisteredComponent
	for path, bindings := range nodes {
		entries = append(entries, &RegisteredComponent{Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: path}, Routes: []*spec.Route{{Method: "GET", Path: path}}}, Input: coverageContract(t, path, bindings...)})
	}
	c, e := NewInputCatalog(entries)
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func coverageFields(t *testing.T, c *InputCatalog) ([]InputField, error) {
	t.Helper()
	return c.Fields(spec.RouteRef{Method: "GET", Path: "/root"})
}
func TestScopedTransportCoverageDefaultCompleteOrderAndNested(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(map[bool]string{false: "default first", true: "scope first"}[reverse], func(t *testing.T) {
			root := []bindly.BindingSpec{coverageValue(true), coverageEdge("First", "/child", "lazy")}
			if reverse {
				root[0], root[1] = root[1], root[0]
			}
			c := coverageCatalog(t, map[string][]bindly.BindingSpec{"/root": root, "/child": {coverageValue(false), coverageEdge("First", "/nested", "")}, "/nested": {coverageValue(false), coverageEdge("First", "/empty", "later")}, "/empty": {}})
			fields, e := coverageFields(t, c)
			if e != nil || len(fields) != 1 || !transportRequired(fields[0]) || fields[0].Path() != "Value" || fields[0].origin.Path != "/root" {
				t.Fatal("default projection changed", e)
			}
			key := spec.Key{Kind: spec.KindComponent, Name: "/root"}
			exact, e := c.FieldsFor(key, spec.RouteRef{Method: "GET", Path: "/root"})
			if e != nil || len(exact) != 1 || exact[0].verifiedJWT {
				t.Fatal(e)
			}
			var wg sync.WaitGroup
			for range 24 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					f, e := coverageFields(t, c)
					if e != nil || len(f) != 1 || !transportRequired(f[0]) {
						t.Error(e)
					}
					*f[0].binding.Required = false
				}()
			}
			wg.Wait()
			again, e := coverageFields(t, c)
			if e != nil || !transportRequired(again[0]) {
				t.Fatal("stored requiredness mutated", e)
			}
		})
	}
}
func TestScopedTransportCoverageMetadataConflicts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*bindly.BindingSpec)
	}{
		{"new location", func(b *bindly.BindingSpec) { b.Location.In = "other" }}, {"source type", func(b *bindly.BindingSpec) { b.SourceType = reflect.TypeFor[int]() }}, {"data type", func(b *bindly.BindingSpec) { b.DataType = "string" }}, {"cardinality", func(b *bindly.BindingSpec) { b.Cardinality = "many" }}, {"min", func(b *bindly.BindingSpec) { v := 1; b.MinAllowedRecords = &v }}, {"max", func(b *bindly.BindingSpec) { v := 2; b.MaxAllowedRecords = &v }}, {"expected", func(b *bindly.BindingSpec) { v := 1; b.ExpectedReturned = &v }}, {"body null", func(b *bindly.BindingSpec) { b.BodyNullPolicy = "allow" }}, {"default", func(b *bindly.BindingSpec) { b.DefaultValue = "new" }}, {"error code", func(b *bindly.BindingSpec) { b.ErrorCode = 401 }}, {"error message", func(b *bindly.BindingSpec) { b.ErrorMessage = "new" }}, {"cacheability", func(b *bindly.BindingSpec) { v := false; b.Cacheable = &v }}, {"resource", func(b *bindly.BindingSpec) { b.ResourceRef = "new" }}, {"uri", func(b *bindly.BindingSpec) { b.URI = "/other" }}, {"priority", func(b *bindly.BindingSpec) { b.Priority = 1 }}, {"scope", func(b *bindly.BindingSpec) { b.Scope = "lazy" }}, {"when", func(b *bindly.BindingSpec) { b.When = "true" }}, {"with", func(b *bindly.BindingSpec) { b.With = "Value" }}, {"public name", func(b *bindly.BindingSpec) { b.Name = "Other" }}, {"extension", func(b *bindly.BindingSpec) { b.Extension = struct{}{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			child := coverageValue(false)
			tc.change(&child)
			c := coverageCatalog(t, map[string][]bindly.BindingSpec{"/root": {coverageValue(true), coverageEdge("First", "/child", "lazy")}, "/child": {coverageValue(false)}})
			c.routes["GET:/child"][0].contract.fields[0].binding = child
			if _, e := coverageFields(t, c); e == nil {
				t.Fatal("incompatible field admitted")
			}
		})
	}
}
func TestScopedTransportCoverageTopologyRejections(t *testing.T) {
	for _, tc := range []struct {
		name  string
		nodes map[string][]bindly.BindingSpec
	}{
		{"new field", map[string][]bindly.BindingSpec{"/root": {coverageEdge("First", "/child", "lazy")}, "/child": {coverageValue(false)}}},
		{"required upgrade", map[string][]bindly.BindingSpec{"/root": {coverageValue(false), coverageEdge("First", "/child", "lazy")}, "/child": {coverageValue(true)}}},
		{"two scopes cannot cover", map[string][]bindly.BindingSpec{"/root": {coverageEdge("First", "/child", "lazy"), coverageEdge("Second", "/other", "later")}, "/child": {coverageValue(false)}, "/other": {coverageValue(false)}}},
		{"missing", map[string][]bindly.BindingSpec{"/root": {coverageValue(true), coverageEdge("First", "/missing", "lazy")}}},
		{"self cycle", map[string][]bindly.BindingSpec{"/root": {coverageValue(true), coverageEdge("First", "/root", "lazy")}}},
		{"two node cycle", map[string][]bindly.BindingSpec{"/root": {coverageValue(true), coverageEdge("First", "/child", "lazy")}, "/child": {coverageEdge("First", "/root", "")}}},
		{"default scope ancestor cycle", map[string][]bindly.BindingSpec{"/root": {coverageValue(true), coverageEdge("First", "/child", "")}, "/child": {coverageEdge("First", "/nested", "lazy")}, "/nested": {coverageEdge("First", "/child", "")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, e := coverageFields(t, coverageCatalog(t, tc.nodes)); e == nil {
				t.Fatal("invalid graph admitted")
			}
		})
	}
	for _, mode := range []string{"when", "with", "scope+when", "scope+with", "async", "group"} {
		t.Run(mode, func(t *testing.T) {
			c := coverageCatalog(t, map[string][]bindly.BindingSpec{"/root": {coverageValue(true), coverageEdge("First", "/child", "lazy")}, "/child": {coverageValue(false)}})
			field := &c.routes["GET:/root"][0].contract.fields[1]
			switch mode {
			case "when":
				field.binding.Scope = ""
				field.binding.When = "true"
			case "with":
				field.binding.Scope = ""
				field.binding.With = "Value"
			case "scope+when":
				field.binding.When = "true"
			case "scope+with":
				field.binding.With = "Value"
			case "async":
				field.binding.Async = true
			case "group":
				field.binding.ResolutionGroup = &bindly.ResolutionGroupSpec{Name: "group"}
			}
			if _, e := coverageFields(t, c); e == nil {
				t.Fatal("conditional dependency admitted")
			}
		})
	}
}
func TestScopedTransportCoverageUsesRetainedAuthorityAndAmbiguity(t *testing.T) {
	c := coverageCatalog(t, map[string][]bindly.BindingSpec{"/root": {coverageValue(true), coverageEdge("First", "/child", "lazy")}, "/child": {coverageValue(false)}})
	root := &c.routes["GET:/root"][0].contract.fields[0]
	root.binding.When = "true"
	if _, e := coverageFields(t, c); e == nil {
		t.Fatal("conditional default supplied authority")
	}
	c = coverageCatalog(t, map[string][]bindly.BindingSpec{"/root": {coverageValue(true), coverageEdge("First", "/child", "lazy"), coverageEdge("Second", "/child", "")}, "/child": {coverageValue(false)}})
	copy := c.routes["GET:/child"][0]
	copy.component.Name = "duplicate"
	c.routes["GET:/child"] = append(c.routes["GET:/child"], copy)
	if _, e := coverageFields(t, c); e == nil {
		t.Fatal("ambiguous target admitted")
	}
}

type opaqueCoverageTransformer struct{ calls int }

func (t *opaqueCoverageTransformer) Transform(context.Context, locator.Resolver, any) (any, error) {
	t.calls++
	return nil, nil
}
func (t *opaqueCoverageTransformer) WireSourceType() reflect.Type {
	t.calls++
	panic("new comparator called unknown source method")
}
func TestScopedTransportCoverageOpaqueTransformersAndRetainedMetadata(t *testing.T) {
	for _, mode := range []string{"opaque", "typed nil", "wire type", "destination", "nullable", "wire maps", "anonymous", "public tags", "visibility", "parameter codec args", "parameter validation"} {
		t.Run(mode, func(t *testing.T) {
			c := coverageCatalog(t, map[string][]bindly.BindingSpec{"/root": {coverageValue(true), coverageEdge("First", "/child", "lazy")}, "/child": {coverageValue(false)}})
			a := &c.routes["GET:/root"][0].contract.fields[0]
			child := &c.routes["GET:/child"][0].contract.fields[0]
			unknown := &opaqueCoverageTransformer{}
			switch mode {
			case "opaque":
				a.binding.Transformer = unknown
				child.binding.Transformer = unknown
			case "typed nil":
				a.binding.Transformer = (*opaqueCoverageTransformer)(nil)
				child.binding.Transformer = (*opaqueCoverageTransformer)(nil)
			case "wire type":
				child.wireType = reflect.TypeFor[int]()
			case "destination":
				child.destinationType = reflect.TypeFor[int]()
			case "nullable":
				child.wireSchema = &spec.WireSchema{Type: "string", Nullable: true}
			case "wire maps":
				child.wireSchemas = map[string]*spec.WireSchema{"Value": {Type: "string", Format: "date"}}
			case "anonymous":
				child.anonymous = true
			case "public tags", "visibility":
				tag := reflect.StructTag(`mcp:"name=Other" validate:"required"`)
				if mode == "visibility" {
					tag = reflect.StructTag(`internal:"true"`)
				}
				child.owner = reflect.StructOf([]reflect.StructField{{Name: "Value", Type: reflect.TypeFor[string](), Tag: tag}})
			case "parameter codec args":
				a.binding.Extension = &spec.Parameter{Name: "Value", Codec: &spec.Codec{Body: "JwtClaim"}}
				child.binding.Extension = &spec.Parameter{Name: "Value", Codec: &spec.Codec{Body: "JwtClaim", Args: []string{"new"}}}
			case "parameter validation":
				a.binding.Extension = &spec.Parameter{Name: "Value"}
				child.binding.Extension = &spec.Parameter{Name: "Value", QueryListCSV: true}
			}
			if _, e := coverageFields(t, c); e == nil {
				t.Fatal("incompatible metadata admitted")
			}
			if unknown.calls != 0 {
				t.Fatal("projection executed unknown transformer methods")
			}
		})
	}
}

// The existing unconditional group edge remains part of default projection;
// only its child's later scoped edge creates a coverage obligation.
func TestScopedTransportCoveragePreservesDefaultGroupAndProjection(t *testing.T) {
	for _, async := range []bool{false, true} {
		edge := coverageEdge("First", "/child", "")
		if !async {
			edge.ResolutionGroup = &bindly.ResolutionGroupSpec{Name: "existing", After: []string{"Value"}}
		}
		edge.Async = async
		c := coverageCatalog(t, map[string][]bindly.BindingSpec{"/root": {coverageValue(true), edge}, "/child": {coverageEdge("First", "/nested", "lazy")}, "/nested": {coverageValue(false)}})
		got, e := coverageFields(t, c)
		if e != nil {
			t.Fatal("existing default group/async rejected", e)
		}
		baseline := coverageCatalog(t, map[string][]bindly.BindingSpec{"/root": {coverageValue(true), edge}, "/child": {}})
		want, e := coverageFields(t, baseline)
		if e != nil || !reflect.DeepEqual(got, want) {
			t.Fatal("default projection changed", e)
		}
	}
}

type codecTransformer struct{ opaqueCoverageTransformer }

func TestScopedTransportCoverageRejectsSameNamedForeignTransformer(t *testing.T) {
	c := coverageCatalog(t, map[string][]bindly.BindingSpec{"/root": {coverageValue(true), coverageEdge("First", "/child", "lazy")}, "/child": {coverageValue(false)}})
	for _, ref := range []string{"GET:/root", "GET:/child"} {
		f := &c.routes[ref][0].contract.fields[0]
		f.binding.Transformer = &codecTransformer{}
		f.verifiedJWT = true
	}
	if _, e := coverageFields(t, c); e == nil {
		t.Fatal("same-named foreign transformer admitted")
	}
}

func TestScopedJWTMetadataIgnoresResolvedDeclarationMarker(t *testing.T) {
	parent := &spec.Parameter{Name: "Jwt", Declaration: "define", Codec: &spec.Codec{Body: "JwtClaim"}}
	child := &spec.Parameter{Name: "Jwt", Codec: &spec.Codec{Body: "JwtClaim"}}
	a := InputField{binding: bindly.BindingSpec{Extension: parent}}
	b := InputField{binding: bindly.BindingSpec{Extension: child}}
	if err := coverageParameter(a, b, true); err != nil {
		t.Fatal(err)
	}
	if parent.Declaration != "define" || child.Declaration != "" {
		t.Fatal("comparison mutated original parameters")
	}
	child.Codec = &spec.Codec{Body: "JwtClaim", Args: []string{"different"}}
	if err := coverageParameter(a, b, true); err == nil {
		t.Fatal("changed authentication codec accepted")
	}
	child.Codec = &spec.Codec{Body: "JwtClaim"}
	child.TypeExpr = "int"
	if err := coverageParameter(a, b, true); err == nil {
		t.Fatal("different transport metadata accepted")
	}
}
