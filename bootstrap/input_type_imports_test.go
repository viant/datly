package bootstrap

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
)

func TestExplicitInputTypeRetainsReflectedImportAuthority(t *testing.T) {
	type input struct{ Value *time.Time }
	fields, err := xshape.New(linkedContractType(reflect.TypeFor[input]()), nil).Fields()
	require.NoError(t, err)
	for _, tc := range []struct {
		name, declared string
		collision      bool
		matches        bool
	}{
		{name: "missing context named pointer", declared: "*time.Time", matches: true},
		{name: "explicit differing type", declared: "string"},
		{name: "explicit wrapper difference", declared: "time.Time"},
		{name: "existing alias collision", declared: "*time.Time", collision: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &spec.Component{}
			if tc.collision {
				c.TypeContext = &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "time", Package: "example.com/other"}}}
			}
			p := &spec.Parameter{TypeExpr: tc.declared}
			r := &packageComponentResolver{component: c}
			require.NoError(t, r.resolveParamType(inputContract, fields[0], p))
			require.Equal(t, tc.declared, p.TypeExpr, "import completion must not replace declared binding datatype")
			require.NotNil(t, c.TypeContext)
			imports := map[string]string{}
			for _, item := range c.TypeContext.Imports {
				imports[item.Alias] = item.Package
			}
			if tc.collision {
				require.Equal(t, "example.com/other", imports["time"])
				require.Equal(t, "time", imports["time2"])
			} else {
				require.Equal(t, "time", imports["time"])
			}
			canonical, err := (xshape.Resolver{Imports: imports}).Canonical(p.TypeExpr)
			require.NoError(t, err)
			actual, err := fields[0].CanonicalType()
			require.NoError(t, err)
			require.Equal(t, tc.matches, canonical == actual, "different explicit datatype/wrapper or colliding alias must remain detectable")
		})
	}
}
