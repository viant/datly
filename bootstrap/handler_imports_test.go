package bootstrap

import (
	"testing"

	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	xshape "github.com/viant/x/shape"
)

func TestComponentRetainsFactoryOnlyImportAuthority(t *testing.T) {
	imports := map[string]xshape.SourceImport{"xdatly": {Path: componentPackagePath, Explicit: true}, "handlers": {Path: "example.com/private/handlers", Explicit: true}}
	for _, test := range []struct {
		reference string
		want      int
		fail      bool
	}{{"handlers.NewHandler", 1, false}, {"NewHandler", 0, false}, {"example.com/private/handlers.NewHandler", 0, false}, {"missing.NewHandler", 0, true}} {
		actual, err := componentContractImports("xdatly.Component[Input,Output]", dtag.Component{Handler: test.reference}, imports)
		if (err != nil) != test.fail || len(actual) != test.want {
			t.Fatalf("reference=%s imports=%v err=%v", test.reference, actual, err)
		}
		if len(actual) == 1 && actual[0] != (spec.ImportSpec{Alias: "handlers", Package: "example.com/private/handlers"}) {
			t.Fatalf("factory import authority=%v", actual)
		}
	}
}
