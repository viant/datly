package bootstrap

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/testdata/linkedcodec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	xcodec "github.com/viant/xdatly/codec"
)

const linkedCodecPackage = "github.com/viant/datly/transcribe/testdata/linkedcodec"

type linkedCodecInput struct {
	Values []int `parameter:"Values,kind=query,in=value,dataType=[]string" codec:"github.com/viant/datly/transcribe/testdata/linkedcodec.QueryList,strict"`
}

func TestReferencedLinkedCodecArtifact(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	artifact, err := BuildArtifact(ArtifactInput{
		Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/codec-probe"}}},
		InputType: reflect.TypeFor[linkedCodecInput](), Types: catalog,
	})
	require.NoError(t, err)
	require.NotNil(t, artifact)
	_, found, err := catalog.Resolve(typecatalog.PackageAuthority, linkedCodecPackage+".QueryList")
	require.NoError(t, err)
	require.False(t, found)
}

type errorCodecFactory struct{ err error }

func (f errorCodecFactory) New(*xcodec.Config, ...xcodec.Option) (xcodec.Instance, error) {
	return nil, f.err
}

func TestReferencedCodecPrecedenceAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name, reference, wantError string
		explicit                   reflect.Type
		sourceOnly                 bool
	}{
		{name: "linked", reference: linkedCodecPackage + ".Upper"},
		{name: "explicit authority", reference: linkedCodecPackage + ".Upper", explicit: reflect.TypeFor[Indigo]()},
		{name: "incompatible authority", reference: linkedCodecPackage + ".Upper", explicit: reflect.TypeFor[linkedcodec.Unrelated](), wantError: "implements neither"},
		{name: "unlinked authority", reference: linkedCodecPackage + ".Upper", sourceOnly: true, wantError: "not linked"},
		{name: "missing", reference: linkedCodecPackage + ".Missing", wantError: "not registered"},
		{name: "no guessed alias", reference: "linkedcodec.Upper", wantError: "not registered"},
		{name: "no global short search", reference: "Upper", wantError: "not registered"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := typecatalog.NewCatalog()
			if tc.explicit != nil {
				require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, x.NewType(tc.explicit, x.WithPkgPath(linkedCodecPackage), x.WithName("Upper"))))
			}
			if tc.sourceOnly {
				require.NoError(t, catalog.Register(typecatalog.TypeOriginPackage, &x.Type{PkgPath: linkedCodecPackage, Name: "Upper", Definition: "struct{}"}))
			}
			component := &spec.Component{Parameters: []*spec.Parameter{{Name: "Value", Codec: &spec.Codec{Body: tc.reference}}}}
			compiler := &artifactCompiler{input: ArtifactInput{Types: catalog}}
			lookup, err := compiler.codecTypeLookup(component, nil)
			require.NoError(t, err)
			factory := newCodecFactory(nil)
			factory.lookup = lookup
			instance, err := factory.New(&xcodec.Config{Body: tc.reference})
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
				return
			}
			require.NoError(t, err)
			value, err := instance.Value(context.Background(), "value")
			require.NoError(t, err)
			if tc.explicit == nil {
				require.Equal(t, "VALUE", value)
			} else {
				require.Equal(t, "value", value)
			}
		})
	}
}

func TestCodecDependencyDoesNotShadowSiblingFactory(t *testing.T) {
	component := &spec.Component{Parameters: []*spec.Parameter{{Codec: &spec.Codec{Body: linkedCodecPackage + ".Upper"}}}}
	compiler := &artifactCompiler{input: ArtifactInput{}}
	lookup, err := compiler.codecTypeLookup(component, nil)
	require.NoError(t, err)
	fallbackError := errors.New("recognized custom codec configuration failure")
	factory := newCodecFactory(errorCodecFactory{fallbackError})
	factory.lookup = lookup
	_, err = factory.New(&xcodec.Config{Body: "Unrelated"})
	require.ErrorIs(t, err, fallbackError, "do not register unrelated sibling types or rewrite construction errors")
	_, err = factory.New(&xcodec.Config{Body: "Upper"})
	require.NoError(t, err, "explicit dependency is available consistently before any codec is built")
	require.NoError(t, compiler.input.Types.Register(typecatalog.TypeOriginPackage, x.NewType(reflect.TypeFor[linkedcodec.Unrelated]())))
	_, err = factory.New(&xcodec.Config{Body: "Unrelated"})
	require.ErrorContains(t, err, "implements neither", "existing catalog types precede fallback")
}

func TestCodecConstructionFailuresRemainSpecific(t *testing.T) {
	compiler := &artifactCompiler{input: ArtifactInput{InputType: reflect.TypeFor[linkedCodecInput]()}}
	lookup, err := compiler.codecTypeLookup(&spec.Component{}, nil)
	require.NoError(t, err)
	factory := newCodecFactory(nil)
	factory.lookup = lookup
	config := &xcodec.Config{Body: linkedCodecPackage + ".QueryList", SourceType: reflect.TypeFor[[]string](), DestinationType: reflect.TypeFor[[]int](), Args: []string{"bad"}}
	_, err = factory.New(config, xcodec.WithTypeLookup(lookup))
	require.ErrorContains(t, err, "invalid QueryList mode")
	config.Args = []string{"strict"}
	instance, err := factory.New(config, xcodec.WithTypeLookup(lookup))
	require.NoError(t, err)
	value, err := instance.Value(context.Background(), []string{"1,2", "3"})
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, value)
	_, err = (&auth.Service{}).New(&xcodec.Config{Body: "missing"})
	require.ErrorContains(t, err, `codec "missing" is not registered`)
	_, err = (&auth.Service{}).New(&xcodec.Config{Body: "JwtClaim"})
	require.ErrorContains(t, err, "JWTValidator is not initialized")
}

func TestCodecNestedContractCollection(t *testing.T) {
	type summary struct {
		Name string `codec:"github.com/viant/datly/transcribe/testdata/linkedcodec.Upper"`
	}
	type child struct {
		Values []int `codec:"github.com/viant/datly/transcribe/testdata/linkedcodec.QueryList"`
	}
	type row struct{ Child *child }
	type output struct {
		Data []*row
		Meta *summary
	}
	names, err := CodecReferences(&spec.Component{}, nil, reflect.TypeFor[output]())
	require.NoError(t, err)
	require.Equal(t, []string{linkedCodecPackage + ".QueryList", linkedCodecPackage + ".Upper"}, names)
}

func TestCodecConcurrentCatalogIsolation(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	for i := 0; i < 8; i++ {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			t.Parallel()
			_, err := BuildArtifact(ArtifactInput{Component: &spec.Component{Routes: []*spec.Route{{Method: "GET", Path: "/codec"}}}, InputType: reflect.TypeFor[linkedCodecInput](), Types: catalog})
			require.NoError(t, err)
			_, found, err := catalog.Resolve(typecatalog.PackageAuthority, linkedCodecPackage+".QueryList")
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}

func TestReferencedCodecAmbiguity(t *testing.T) {
	catalog := typecatalog.NewCatalog()
	require.NoError(t, catalog.RegisterAll(typecatalog.TypeOriginPackage,
		x.NewType(reflect.TypeFor[Indigo](), x.WithPkgPath("example.com/a"), x.WithName("Shared")),
		x.NewType(reflect.TypeFor[linkedcodec.Upper](), x.WithPkgPath("example.com/b"), x.WithName("Shared")),
	))
	compiler := &artifactCompiler{input: ArtifactInput{Types: catalog}}
	lookup, err := compiler.codecTypeLookup(&spec.Component{}, nil)
	require.NoError(t, err)
	_, err = lookup("Shared")
	require.ErrorContains(t, err, "ambiguous type")
}
