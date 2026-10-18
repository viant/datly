package readergraph

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
	xshape "github.com/viant/x/shape"
)

func TestReaderMetadataContractUsesExactGoAliasIdentity(t *testing.T) {
	const pkg = "example.com/reader"
	source := `package reader
 type Target struct { ID int }
 type Alias = Target
 type Defined Target
 type CycleA = CycleB
 type CycleB = CycleA
 type Row struct { ID int }
 type Input struct { SysConfig *Target ` + "`parameter:\"SysConfig,kind=component,in=GET:/systemconfig\"`" + ` }
 type Output struct { Data []*Row ` + "`parameter:\"Data,kind=output,in=view\"`" + ` }
 `
	loaded, err := loaderast.LoadPackageFS(context.Background(), fstest.MapFS{"root/go.mod": {Data: []byte("module " + pkg + "\n")}, "root/types.go": {Data: []byte(source)}}, "root")
	require.NoError(t, err)
	types := map[string]*x.Type{}
	for _, typ := range loaded.Types {
		d := &x.Type{Name: typ.Name, PkgPath: loaded.PkgPath, SynteticType: typ}
		types[d.Key()] = d
	}
	lookup := func(name string) (*x.Type, error) {
		if d := types[name]; d != nil {
			return d, nil
		}
		return nil, fmt.Errorf("alias lookup missing %s", name)
	}
	for _, tc := range []struct{ name, want, fail string }{
		{"true alias", "*" + pkg + ".Alias", ""},
		{"same shaped defined type", "*" + pkg + ".Defined", "differs"},
		{"pointer wrapper differs", pkg + ".Alias", "differs"},
		{"lookup error", "*" + pkg + ".Missing", "alias lookup missing"},
		{"alias cycle", "*" + pkg + ".CycleA", "cyclic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &spec.Component{Key: spec.Key{Scope: pkg}, Settings: &spec.Settings{}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, RootView: &spec.View{Name: "rows", TypeName: pkg + ".Row"}, Parameters: []*spec.Parameter{
				{Name: "SysConfig", TypeExpr: tc.want, Source: spec.BindSource{Kind: "component", Name: "GET:/systemconfig"}},
				{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
			}}
			err := Contracts(c, types[pkg+".Input"], types[pkg+".Output"], xshape.Lookup(lookup))
			if tc.fail == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.fail)
			}
		})
	}
}

type aliasRuntimeTarget struct{ ID int }
type aliasRuntimeRow struct{ ID int }
type aliasRuntimeInput struct {
	SysConfig *aliasRuntimeTarget `parameter:"SysConfig,kind=component,in=GET:/systemconfig"`
}
type aliasRuntimeOutput struct {
	Data []*aliasRuntimeRow `parameter:"Data,kind=output,in=view"`
}

func TestReaderMetadataAliasRequiresLookupAuthority(t *testing.T) {
	pkg := reflect.TypeFor[aliasRuntimeInput]().PkgPath()
	c := &spec.Component{Key: spec.Key{Scope: pkg}, Settings: &spec.Settings{}, Routes: []*spec.Route{{Method: "GET", Path: "/records"}}, RootView: &spec.View{Name: "rows", TypeName: pkg + ".aliasRuntimeRow"}, Parameters: []*spec.Parameter{
		{Name: "SysConfig", TypeExpr: "*" + pkg + ".UnprovenAlias", Source: spec.BindSource{Kind: "component", Name: "GET:/systemconfig"}},
		{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}},
	}}
	require.ErrorContains(t, Contracts(c, xshape.Linked(reflect.TypeFor[aliasRuntimeInput]()).Descriptor(), xshape.Linked(reflect.TypeFor[aliasRuntimeOutput]()).Descriptor(), nil), "differs")
}
