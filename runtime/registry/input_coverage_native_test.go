package registry_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/viant/bindly"
	"github.com/viant/bindly/locator"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/exec"
	"github.com/viant/datly/gateway/openapi"
	"github.com/viant/datly/gateway/openapi/openapi3"
	"github.com/viant/datly/internal/testharness/mcpclient"
	"github.com/viant/datly/mcp"
	"github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	xcodec "github.com/viant/xdatly/codec"
)

type coverageJWTChild struct{ Jwt *jwt.Claims }
type coverageJWTParent struct {
	Jwt   *jwt.Claims
	Lazy  *coverageResult
	Query string
}
type coverageResult struct {
	ID int `json:"id"`
}
type coverageInvoker struct{ calls int }

func (i *coverageInvoker) InvokeComponent(context.Context, exec.ComponentRequest) (any, error) {
	i.calls++
	return &coverageResult{ID: 7}, nil
}
func coverageFactory(t *testing.T) *auth.Service {
	t.Helper()
	key, e := rsa.GenerateKey(rand.Reader, 2048)
	if e != nil {
		t.Fatal(e)
	}
	pub, e := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if e != nil {
		t.Fatal(e)
	}
	service, e := auth.New(t.Context(), &auth.Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "coverage-local-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub})}}}})
	if e != nil {
		t.Fatal(e)
	}
	return service
}
func compiledCoveragePair(t *testing.T, required bool, reverse bool) ([]*registry.RegisteredComponent, *auth.Service) {
	t.Helper()
	factory := coverageFactory(t)
	optional := false
	jwtParam := func(required bool) *spec.Parameter {
		return &spec.Parameter{Name: "Jwt", TypeExpr: "string", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Required: &required, ErrorStatusCode: 401, Codec: &spec.Codec{Body: "JwtClaim"}}
	}
	parent := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "public", Name: "Parent"}, Routes: []*spec.Route{{Method: "GET", Path: "/coverage", MCP: []*spec.MCPExposure{{Kind: spec.MCPExposureTool, Name: "ScopedSearch"}}}}, Parameters: []*spec.Parameter{jwtParam(required), {Name: "Lazy", Source: spec.BindSource{Kind: "component", Name: "GET:/coverage-child"}, Scope: "lazy"}, {Name: "Query", Source: spec.BindSource{Kind: "query", Name: "query"}, Required: &optional}}}
	if reverse {
		parent.Parameters[0], parent.Parameters[1] = parent.Parameters[1], parent.Parameters[0]
	}
	child := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "private", Name: "Child"}, Routes: []*spec.Route{{Method: "GET", Path: "/coverage-child"}}, Parameters: []*spec.Parameter{jwtParam(false)}}
	var entries []*registry.RegisteredComponent
	for _, c := range []*spec.Component{parent, child} {
		in := reflect.TypeFor[coverageJWTParent]()
		if c == child {
			in = reflect.TypeFor[coverageJWTChild]()
		}
		a, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: c, InputType: in, OutputType: reflect.TypeFor[coverageResult](), CodecFactory: factory})
		if e != nil {
			t.Fatal(e)
		}
		entry, e := a.Registration(registry.RegisteredComponent{Handler: handler.HandlerFunc(func(context.Context, handler.Invocation) (any, error) {
			t.Error("metadata executed handler")
			return nil, nil
		})})
		if e != nil {
			t.Fatal(e)
		}
		entries = append(entries, entry)
	}
	return entries, factory
}
func TestScopedTransportGenuineCompiledJWTMCPAndOpenAPI(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		entries, _ := compiledCoveragePair(t, true, reverse)
		catalog, e := registry.NewInputCatalog(entries)
		if e != nil {
			t.Fatal(e)
		}
		fields, e := catalog.FieldsFor(entries[0].Component.Key, spec.RouteRef{Method: "GET", Path: "/coverage"})
		if e != nil {
			t.Fatal(e)
		}
		if len(fields) != 2 {
			t.Fatal("scoped child added fields", len(fields))
		}
		jwtFields := 0
		for _, f := range fields {
			if f.Binding().Location.Kind == "header" {
				jwtFields++
				if !f.VerifiesJWT() || f.Binding().Required == nil || !*f.Binding().Required {
					t.Fatal("JWT authority weakened")
				}
			}
		}
		if jwtFields != 1 {
			t.Fatal("header count")
		}
		doc, e := (openapi.Generator{}).Generate(t.Context(), openapi.Request{Info: openapi3.Info{Title: "Scoped", Version: "1"}, Components: entries, Routes: []spec.RouteRef{{Method: "GET", Path: "/coverage"}}})
		if e != nil {
			t.Fatal(e)
		}
		op := doc.Paths["/coverage"].Get
		if doc.Components.SecuritySchemes["BearerJWT"] == nil || (op.Security == nil || len(*op.Security) != 1) || len(op.Parameters) != 1 || op.Parameters[0].Name != "query" || op.Parameters[0].Required {
			t.Fatal("OpenAPI transport/security changed")
		}
		invoker := &coverageInvoker{}
		service, e := mcp.New(mcp.Config{Components: entries, Invoker: invoker})
		if e != nil {
			t.Fatal(e)
		}
		client := mcpclient.New(t, service, schema.LatestProtocolVersion)
		tools, e := client.ListTools(t.Context(), nil)
		if e != nil || len(tools.Tools) != 1 {
			t.Fatal(e)
		}
		if invoker.calls != 0 {
			t.Fatal("metadata executed component")
		}
		properties := tools.Tools[0].InputSchema.Properties
		if len(properties) != 1 || properties["Query"] == nil {
			t.Fatal("MCP fields changed", properties)
		}
	}
}

type forgedCoverageCarrier struct {
	codec xcodec.Instance
	calls atomic.Int64
}

func (c *forgedCoverageCarrier) Codec() xcodec.Instance { c.calls.Add(1); return c.codec }
func (c *forgedCoverageCarrier) Transform(context.Context, locator.Resolver, any) (any, error) {
	c.calls.Add(1)
	panic("forged Transform")
}
func (c *forgedCoverageCarrier) WireSourceType() reflect.Type {
	c.calls.Add(1)
	panic("forged source method")
}
func TestScopedTransportForgedCarrierZeroAdditionalMethodsAndOfficialNilRegistration(t *testing.T) {
	entries, factory := compiledCoveragePair(t, true, false)
	route, _ := entries[0].Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/coverage"})
	original := route.Fields()
	var jwtField registry.InputField
	for _, f := range original {
		if f.VerifiesJWT() {
			jwtField = f
		}
	}
	instance, e := factory.New(&xcodec.Config{Body: "JwtClaim", SourceType: reflect.TypeFor[string](), DestinationType: reflect.TypeFor[*jwt.Claims]()})
	if e != nil {
		t.Fatal(e)
	}
	carrier := &forgedCoverageCarrier{codec: instance}
	injector, e := bindly.NewInjector()
	if e != nil {
		t.Fatal(e)
	}
	childRoute := spec.RouteRef{Method: "GET", Path: "/coverage-child"}
	binding := jwtField.Binding()
	binding.Transformer = carrier
	optional := false
	binding.Required = &optional
	plan, e := injector.CompilePlan(reflect.TypeFor[coverageJWTChild](), binding)
	if e != nil {
		t.Fatal(e)
	}
	projection, e := plan.Projection()
	if e != nil {
		t.Fatal(e)
	}
	child, e := registry.NewInputContract(reflect.TypeFor[coverageJWTChild](), projection, registry.RouteInput{Route: childRoute, Plan: plan, Bindings: []bindly.BindingSpec{binding}})
	if e != nil {
		t.Fatal(e)
	}
	entries[1].Input = child
	carrier.calls.Store(0)
	catalog, e := registry.NewInputCatalog(entries)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = catalog.Fields(spec.RouteRef{Method: "GET", Path: "/coverage"}); e == nil {
		t.Fatal("forged real-codec carrier admitted")
	}
	if carrier.calls.Load() != 0 {
		t.Fatal("coverage executed unknown methods")
	}
	// Existing registration calls official Codec() before a catalog can exist.
	// This is a pre-catalog witness, not a helper execution or constructor fix.
	binding = jwtField.Binding()
	binding.Transformer = reflect.Zero(reflect.TypeOf(binding.Transformer)).Interface().(interface {
		Transform(context.Context, locator.Resolver, any) (any, error)
	})
	plan, e = injector.CompilePlan(reflect.TypeFor[coverageJWTChild](), binding)
	if e != nil {
		t.Fatal(e)
	}
	projection, e = plan.Projection()
	if e != nil {
		t.Fatal(e)
	}
	panicked := false
	func() {
		defer func() { panicked = recover() != nil }()
		_, _ = registry.NewInputContract(reflect.TypeFor[coverageJWTChild](), projection, registry.RouteInput{Route: childRoute, Plan: plan, Bindings: []bindly.BindingSpec{binding}})
	}()
	if !panicked {
		t.Fatal("official typed nil unexpectedly passed registration")
	}
}

type nonVerifierCodec struct{}

func (nonVerifierCodec) Value(context.Context, any, ...xcodec.Option) (any, error) {
	panic("metadata executed codec")
}

type nonVerifierFactory struct{}

func (nonVerifierFactory) New(*xcodec.Config, ...xcodec.Option) (xcodec.Instance, error) {
	return nonVerifierCodec{}, nil
}
func TestScopedTransportRejectsOfficialUnverifiedAndCodecArguments(t *testing.T) {
	for _, mode := range []string{"unverified", "unverified default", "codec args", "codec output"} {
		t.Run(mode, func(t *testing.T) {
			entries, _ := compiledCoveragePair(t, true, false)
			childRoute := spec.RouteRef{Method: "GET", Path: "/coverage-child"}
			route, _ := entries[1].Input.ForRoute(childRoute)
			binding := route.Fields()[0].Binding()
			if mode == "unverified" || mode == "unverified default" {
				index := 1
				inputType := reflect.TypeFor[coverageJWTChild]()
				if mode == "unverified default" {
					index = 0
					inputType = reflect.TypeFor[coverageJWTParent]()
				}
				a, e := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: entries[index].Component, InputType: inputType, OutputType: reflect.TypeFor[coverageResult](), CodecFactory: nonVerifierFactory{}})
				if e != nil {
					t.Fatal(e)
				}
				entries[index].Input = a.Input
			} else {
				param := *(binding.Extension.(*spec.Parameter))
				codec := *param.Codec
				param.Codec = &codec
				if mode == "codec args" {
					codec.Args = []string{"not original"}
				} else {
					codec.OutputType = "different"
				}
				binding.Extension = &param
				injector, e := bindly.NewInjector()
				if e != nil {
					t.Fatal(e)
				}
				plan, e := injector.CompilePlan(reflect.TypeFor[coverageJWTChild](), binding)
				if e != nil {
					t.Fatal(e)
				}
				projection, e := plan.Projection()
				if e != nil {
					t.Fatal(e)
				}
				owner, e := registry.NewInputContract(reflect.TypeFor[coverageJWTChild](), projection, registry.RouteInput{Route: childRoute, Plan: plan, Bindings: []bindly.BindingSpec{binding}})
				if e != nil {
					t.Fatal(e)
				}
				entries[1].Input = owner
			}
			catalog, e := registry.NewInputCatalog(entries)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = catalog.Fields(spec.RouteRef{Method: "GET", Path: "/coverage"}); e == nil {
				t.Fatal("unsupported verifier/codec metadata admitted")
			}
		})
	}
}
