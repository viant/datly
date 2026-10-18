package link

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/x"
	loaderast "github.com/viant/x/loader/ast"
)

func TestContractDestinationAliasComparisonPreservesExactResponsibilities(t *testing.T) {
	const pkg = "example.com/contracts"
	loaded, err := loaderast.LoadPackageFS(context.Background(), fstest.MapFS{"root/go.mod": {Data: []byte("module " + pkg + "\n")}, "root/model.go": {Data: []byte("package contracts\ntype Target struct { ID int }; type Alias = Target; type Defined Target; type CycleA = CycleB; type CycleB = CycleA")}}, "root")
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
		return nil, fmt.Errorf("lookup missing %s", name)
	}
	component := func(alias, expression string) *spec.Component {
		return &spec.Component{Key: spec.Key{Scope: pkg}, TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: alias, Package: pkg}}}, Parameters: []*spec.Parameter{{Name: "Config", Source: spec.BindSource{Kind: "body", Name: "config"}, TypeExpr: expression}}}
	}
	for _, tc := range []struct {
		name, want string
		equal      bool
		codec      bool
		wire       string
		errorText  string
	}{
		{name: "true alias with own import context", want: "*other.Target", equal: true},
		{name: "defined nominal", want: "*other.Defined"},
		{name: "wrapper changed", want: "other.Target"},
		{name: "codec destination alias", want: "*other.Target", equal: true, codec: true, wire: "string"},
		{name: "codec wire changed", want: "*other.Target", codec: true, wire: "[]byte"},
		{name: "missing lookup", want: "*other.Missing", errorText: "lookup missing"},
		{name: "alias cycle", want: "*other.CycleA", errorText: "cyclic"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old, newer := component("old", "*old.Alias"), component("other", tc.want)
			if tc.codec {
				old.Parameters[0].OutputTypeExpr = old.Parameters[0].TypeExpr
				newer.Parameters[0].OutputTypeExpr = newer.Parameters[0].TypeExpr
				old.Parameters[0].TypeExpr = "string"
				newer.Parameters[0].TypeExpr = tc.wire
				old.Parameters[0].Codec = &spec.Codec{Body: "Decode"}
				newer.Parameters[0].Codec = &spec.Codec{Body: "Decode"}
			}
			beforeOld, _ := json.Marshal(old)
			beforeNew, _ := json.Marshal(newer)
			equal, err := (&Contracts{PackageComponent: old, CompiledComponent: newer, Lookup: lookup}).EqualParamsChecked(false)
			if tc.errorText != "" {
				require.ErrorContains(t, err, tc.errorText)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.equal, equal)
			}
			afterOld, _ := json.Marshal(old)
			afterNew, _ := json.Marshal(newer)
			require.Equal(t, string(beforeOld), string(afterOld))
			require.Equal(t, string(beforeNew), string(afterNew))
		})
	}
}
