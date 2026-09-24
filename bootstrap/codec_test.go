package bootstrap

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/viant/datly/runtime/auth"
	structqlcodec "github.com/viant/datly/runtime/handler/codec/structql"
	transfercodec "github.com/viant/datly/runtime/handler/codec/transfer"
	handlercompiler "github.com/viant/datly/runtime/handler/compiler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/dql"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	"github.com/viant/x"
	xcodec "github.com/viant/xdatly/codec"
)

type Amber int

var amberCalls atomic.Int32
var amberLink = reflect.TypeFor[Amber]()

func (*Amber) New(config *xcodec.Config, options ...xcodec.Option) (xcodec.Instance, error) {
	if config.SourceType != reflect.TypeFor[string]() || config.DestinationType != reflect.TypeFor[string]() ||
		xcodec.NewOptions(options).LookupType == nil {
		return nil, fmt.Errorf("codec compilation lost source, destination, or lookup")
	}
	amberCalls.Add(1)
	return fallbackCodec{}, nil
}

type Indigo int

func (*Indigo) Value(_ context.Context, value interface{}, _ ...xcodec.Option) (interface{}, error) {
	return value, nil
}

type DQLTransformSource struct{ Name string }
type DQLTransformDestination struct {
	Name string `transfer:"from=Name"`
}

type fallbackCodec struct{}

func (fallbackCodec) Value(context.Context, interface{}, ...xcodec.Option) (interface{}, error) {
	return nil, nil
}

type fallbackCodecFactory struct {
	called bool
	config *xcodec.Config
}

func (f *fallbackCodecFactory) New(config *xcodec.Config, _ ...xcodec.Option) (xcodec.Instance, error) {
	f.called = true
	f.config = config
	return fallbackCodec{}, nil
}

func TestCodecFactoryProvidesStructQLAndDelegatesCustomCodecs(t *testing.T) {
	type event struct{ ID int }
	type helper struct{ Values []int }
	fallback := &fallbackCodecFactory{}
	factory := newCodecFactory(fallback)
	if _, err := factory.New(&xcodec.Config{
		Body: "STRUCTQL", SourceType: reflect.TypeOf([]event{}), DestinationType: reflect.TypeOf(helper{}),
		Args: []string{"SELECT ARRAY_AGG(ID) AS Values FROM `/` LIMIT 1"},
	}); err != nil || fallback.called {
		t.Fatalf("built-in StructQL = %v, fallback called = %v", err, fallback.called)
	}
	if _, err := factory.New(&xcodec.Config{Body: "custom"}); err != nil || !fallback.called {
		t.Fatalf("custom codec = %v, fallback called = %v", err, fallback.called)
	}
}

func TestBuildArtifactPreservesPackageCodecSourceAndDestinationTypes(t *testing.T) {
	type input struct {
		Values []string `bind:"Values,kind=query,in=value,dataType=string" codec:"custom"`
	}
	factory := &fallbackCodecFactory{}
	artifact, err := BuildArtifact(ArtifactInput{
		Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/values"}}},
		InputType: reflect.TypeOf(input{}), CodecFactory: factory,
	})
	if err != nil {
		t.Fatal(err)
	}
	if artifact == nil || len(artifact.Component.Parameters) != 1 || artifact.Component.Parameters[0].TypeExpr != "string" {
		t.Fatalf("artifact = %+v", artifact)
	}
	if factory.config == nil || factory.config.SourceType != reflect.TypeOf("") || factory.config.DestinationType != reflect.TypeOf([]string{}) {
		t.Fatalf("codec config = %+v", factory.config)
	}
}

func TestBuildArtifactDiscoversExactLinkedCodecType(t *testing.T) {
	_ = amberLink
	reflected, err := ReflectPackages([]string{"github.com/viant/datly/bootstrap"})
	if err != nil {
		t.Fatal(err)
	}
	type input struct {
		Value string `bind:"Value,kind=query,in=value,dataType=string" codec:"Amber"`
	}
	before := amberCalls.Load()
	artifact, err := BuildArtifact(ArtifactInput{
		Component: &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "github.com/viant/datly/bootstrap", Name: "CodecProbe"}, Routes: []*spec.Route{{Method: "GET", Path: "/codec-probe"}}},
		InputType: reflect.TypeFor[input](), Types: reflected.Types,
	})
	if err != nil {
		t.Fatal(err)
	}
	if artifact == nil || amberCalls.Load() != before+1 {
		t.Fatalf("compiled artifact=%v, factory calls=%d", artifact, amberCalls.Load()-before)
	}
}

func TestCodecTypeLookupErrorsAndFallback(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	if err := catalog.RegisterAll(typecatalog.TypeOriginPackage,
		x.NewType(reflect.TypeFor[structqlcodec.Factory]()),
		x.NewType(reflect.TypeFor[transfercodec.Factory]()),
		x.NewType(reflect.TypeFor[Amber]()),
		x.NewType(reflect.TypeFor[ReflectedOrdinaryHandler]()),
	); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	factory := newCodecFactory(nil)
	for _, test := range []struct{ name, want string }{
		{"Factory", "ambiguous type"},
		{"MissingCodec", "not registered"},
		{"github.com/viant/datly/bootstrap.ReflectedOrdinaryHandler", "implements neither"},
	} {
		_, err := factory.New(&xcodec.Config{Body: test.name}, xcodec.WithTypeLookup(resolver.Type))
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("codec %q err=%v, want %q", test.name, err, test.want)
		}
	}
}

func TestCodecTypeLookupUsesDirectInstanceWithoutArguments(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	if err := catalog.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[Indigo]())); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	factory := newCodecFactory(nil)
	instance, err := factory.New(&xcodec.Config{Body: "Indigo"}, xcodec.WithTypeLookup(resolver.Type))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := instance.(*Indigo); !ok {
		t.Fatalf("instance type = %T", instance)
	}
	if got, err := instance.Value(context.Background(), "blue"); err != nil || got != "blue" {
		t.Fatalf("Value = %v, %v", got, err)
	}
	if _, err := factory.New(&xcodec.Config{Body: "Indigo", Args: []string{"ignored"}}, xcodec.WithTypeLookup(resolver.Type)); err == nil || !strings.Contains(err.Error(), "does not accept codec arguments") {
		t.Fatalf("configured direct instance error = %v", err)
	}
}

func TestCodecFallbackWithMissingCatalogType(t *testing.T) {
	lookup, err := typecatalog.NewResolver(typecatalog.NewCatalog(), typecatalog.PackageAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	if typ, err := lookup.Type("custom"); err != nil || typ != nil {
		t.Fatalf("missing catalog type = %v, %v", typ, err)
	}
	fallback := &fallbackCodecFactory{}
	if _, err := newCodecFactory(fallback).New(&xcodec.Config{Body: "custom"}, xcodec.WithTypeLookup(lookup.Type)); err != nil || !fallback.called {
		t.Fatalf("fallback called=%v err=%v", fallback.called, err)
	}
}

func TestBuildArtifactJWTUsesFallbackWhenTypeLookupIsPresent(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	public, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	service, err := auth.New(context.Background(), &auth.Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "test-public-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: public})}}}})
	if err != nil {
		t.Fatal(err)
	}
	type input struct {
		Claims *jwt.Claims `bind:"Claims,kind=header,in=Authorization,dataType=string" codec:"JwtClaim"`
	}
	artifact, err := BuildArtifact(ArtifactInput{
		Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/jwt-codec-probe"}}},
		InputType: reflect.TypeFor[input](), Types: typecatalog.NewCatalog(), CodecFactory: service,
	})
	if err != nil || artifact == nil {
		t.Fatalf("JWT artifact=%v err=%v", artifact, err)
	}
}

func TestDQLImportedTransformFactoryCompilesWithoutRegistration(t *testing.T) {
	const packagePath = "github.com/viant/datly/bootstrap"
	const transferPath = "github.com/viant/datly/runtime/handler/codec/transfer"
	prepared := dql.PrepareSource(`#setting($_ = $route('/transform-probe', 'GET'))
#import('transfer','github.com/viant/datly/runtime/handler/codec/transfer')
#define($_ = $Value<DQLTransformSource,DQLTransformDestination>(query/value).WithCodec('transfer.Factory'))
SELECT 1`)
	component, err := dql.ParsePreparedComponentSource(packagePath, "TransformProbe", prepared)
	if err != nil {
		t.Fatal(err)
	}
	if len(component.Parameters) != 1 || component.Parameters[0].Codec == nil || component.Parameters[0].Codec.Body != "transfer.Factory" {
		t.Fatalf("codec declaration = %+v", component.Parameters)
	}
	// Exercise the real selected-package scan, not a manually populated codec
	// catalog. These types are linked by the typed calls below.
	scanned, err := ReflectPackages([]string{packagePath, transferPath})
	if err != nil {
		t.Fatal(err)
	}
	catalog := scanned.Types
	resolution := &typecatalog.ResolutionContext{PackagePath: packagePath, Imports: []typecatalog.PackageImport{{Alias: "transfer", Package: transferPath}}}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, resolution)
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := resolver.Type(component.Parameters[0].Codec.Body)
	if err != nil || resolved != reflect.TypeFor[transfercodec.Factory]() {
		t.Fatalf("authored codec type = %v, %v", resolved, err)
	}
	type input struct{ Value DQLTransformDestination }
	codecs, err := (handlercompiler.ParamCodecCompiler{
		Component: component, InputType: reflect.TypeFor[input](), Factory: newCodecFactory(nil), LookupType: resolver.Type,
	}).Build()
	if err != nil {
		t.Fatal(err)
	}
	if codecs["Value"].SourceType != reflect.TypeFor[DQLTransformSource]() || codecs["Value"].Instance == nil {
		t.Fatalf("compiled codec = %+v", codecs["Value"])
	}
	got, err := codecs["Value"].Instance.Value(context.Background(), DQLTransformSource{Name: "Ada"})
	transformed, ok := got.(DQLTransformDestination)
	if err != nil || !ok || transformed.Name != "Ada" {
		t.Fatalf("transform value = %#v, %v", got, err)
	}
}
