package transcribe

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
	"reflect"
	"testing"
)

func TestExplicitOutputUsesCanonicalSummaryBinding(t *testing.T) {
	for _, declared := range []string{"summary", "derived", "view", "body"} {
		for _, authored := range []string{"summary", "derived"} {
			t.Run(declared+"/"+authored, func(t *testing.T) {
				typ := reflect.StructOf([]reflect.StructField{{Name: "Meta", Type: reflect.TypeFor[int](), Tag: reflect.StructTag(`parameter:"Meta,kind=output,in=` + declared + `"`)}})
				catalog := typecatalog.NewCatalog()
				descriptor := x.NewType(typ, x.WithPkgPath("example.com/binding"), x.WithName("Output"))
				require.NoError(t, catalog.LinkRuntimeAll(typecatalog.TypeOriginPackage, descriptor))
				resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
				require.NoError(t, err)
				result := &Result{Component: &spec.Component{Parameters: []*spec.Parameter{{Name: "Meta", Source: spec.BindSource{Kind: "output", Name: authored}}}}, TypeResolver: resolver}
				err = linkExplicitOutputContract(result, "example.com/binding.Output", nil)
				if declared == "summary" || declared == "derived" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "incompatible binding metadata")
				}
			})
		}
	}
}
