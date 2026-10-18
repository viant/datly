package readergraph

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	xshape "github.com/viant/x/shape"
)

func TestReaderMetadataSignatureUsesTrueAliasClosureIdentity(t *testing.T) {
	pkg := reflect.TypeFor[aliasRuntimeInput]().PkgPath()
	reflected, err := Signature(xshape.Linked(reflect.TypeFor[aliasRuntimeInput]()).Descriptor(), xshape.Linked(reflect.TypeFor[aliasRuntimeOutput]()).Descriptor(), nil)
	require.NoError(t, err)
	for _, tc := range []struct {
		name, typ, tag string
		equal          bool
		errorText      string
	}{
		{name: "true alias", typ: "*aliasRuntimeAlias", equal: true},
		{name: "defined nominal", typ: "*aliasRuntimeDefined"},
		{name: "wrapper", typ: "[]*aliasRuntimeAlias"},
		{name: "binding tag", typ: "*aliasRuntimeAlias", tag: "GET:/different"},
		{name: "lookup error", typ: "*Missing", errorText: "lookup missing"},
		{name: "cycle", typ: "*CycleA", errorText: "cyclic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			location := tc.tag
			if location == "" {
				location = "GET:/systemconfig"
			}
			source := "package readergraph\n type aliasRuntimeTarget struct { ID int }; type aliasRuntimeAlias = aliasRuntimeTarget; type aliasRuntimeDefined aliasRuntimeTarget; type CycleA = CycleB; type CycleB = CycleA\n type aliasRuntimeRow struct { ID int }\n type aliasRuntimeInput struct { SysConfig " + tc.typ + " `parameter:\"SysConfig,kind=component,in=" + location + "\"` }\n type aliasRuntimeOutput struct { Data []*aliasRuntimeRow `parameter:\"Data,kind=output,in=view\"` }"
			loaded, err := loaderast.LoadPackageFS(context.Background(), fstest.MapFS{"root/go.mod": {Data: []byte("module " + pkg + "\n")}, "root/model.go": {Data: []byte(source)}}, "root")
			require.NoError(t, err)
			types := map[string]*x.Type{"int": x.NewType(reflect.TypeFor[int]())}
			for _, typ := range loaded.Types {
				d := &x.Type{Name: typ.Name, PkgPath: loaded.PkgPath, SynteticType: typ}
				types[d.Key()] = d
			}
			lookup := func(name string) (*x.Type, error) {
				if d := types[name]; d != nil {
					return d, nil
				}
				return nil, fmt.Errorf("lookup missing %s", name)
			}
			actual, err := Signature(types[pkg+".aliasRuntimeInput"], types[pkg+".aliasRuntimeOutput"], lookup)
			if tc.errorText != "" {
				require.ErrorContains(t, err, tc.errorText)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.equal, reflected == actual)
			}
		})
	}
}

type signatureAnonymousInput struct {
	Nested struct{ Value int }
}

func TestReaderMetadataSignatureRetainsAnonymousClosure(t *testing.T) {
	input := xshape.Linked(reflect.TypeFor[signatureAnonymousInput]()).Descriptor()
	output := xshape.Linked(reflect.TypeFor[aliasRuntimeOutput]()).Descriptor()
	descriptors := []*x.Type{input, output, xshape.Linked(reflect.TypeFor[aliasRuntimeRow]()).Descriptor(), x.NewType(reflect.TypeFor[int]())}
	lookup := func(key string) (*x.Type, error) {
		require.NotEmpty(t, key, "anonymous closure key must not be parsed as an alias")
		for _, descriptor := range descriptors {
			if descriptor.Key() == key {
				return descriptor, nil
			}
		}
		return nil, fmt.Errorf("unknown descriptor %s", key)
	}
	before, err := Signature(input, output, nil)
	require.NoError(t, err)
	after, err := Signature(input, output, lookup)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
