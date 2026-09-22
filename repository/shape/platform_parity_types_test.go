package shape_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
)

func TestNormalizeTypeSignature(t *testing.T) {
	assert.Equal(t, "struct{ Id int; Name string }", normalizeTypeSignature("  struct{  Id int;  Name string  } "))
}

func TestTypeNameFromDataType(t *testing.T) {
	assert.Equal(t, "TvAffiliateStationView", typeNameFromDataType("*tvaffiliatestation.TvAffiliateStationView"))
	assert.Equal(t, "Output", typeNameFromDataType("*Output"))
	assert.Equal(t, "struct", typeNameFromDataType("struct{Id int}"))
}

func TestCompareTypeParity(t *testing.T) {
	legacy := []typeIR{{
		Name:       "TvAffiliateStationView",
		DataType:   "*tvaffiliatestation.TvAffiliateStationView",
		Package:    "tvaffiliatestation",
		ModulePath: "github.vianttech.com/viant/platform/pkg/platform/tvaffiliatestation",
	}}
	shapeTypes := []typeIR{{
		Name:       "TvAffiliateStationView",
		DataType:   "*tvaffiliatestation.TvAffiliateStationView",
		Package:    "tvaffiliatestation",
		ModulePath: "github.vianttech.com/viant/platform/pkg/platform/tvaffiliatestation",
	}}
	assert.Empty(t, compareTypeParity(legacy, shapeTypes))
}

func TestNormalizeShapeTypes(t *testing.T) {
	planned := &plan.Result{
		TypeContext: &typectx.Context{
			PackagePath: "github.vianttech.com/viant/platform/pkg/platform/tvaffiliatestation",
			PackageName: "tvaffiliatestation",
		},
		Views: []*plan.View{
			{
				Name:        "tvAffiliateStation",
				Module:      "platform/tvaffiliatestation",
				SchemaType:  "*tvaffiliatestation.TvAffiliateStationView",
				Cardinality: "many",
			},
		},
	}
	actual := normalizeShapeTypes(planned, "/Users/awitas/go/src/github.vianttech.com/viant/platform/dql/platform/tvaffiliatestation/tvaffiliatestation.dql")
	if assert.Len(t, actual, 1) {
		assert.Equal(t, "TvAffiliateStationView", actual[0].Name)
		assert.Equal(t, "github.vianttech.com/viant/platform/pkg/platform/tvaffiliatestation", actual[0].ModulePath)
		assert.Equal(t, "Many", actual[0].Cardinality)
	}
}

func TestTypeImports_UsesTypeContextPackagePath(t *testing.T) {
	planned := &plan.Result{
		TypeContext: &typectx.Context{
			PackagePath: "github.vianttech.com/viant/platform/pkg/platform/tvaffiliatestation",
			PackageName: "tvaffiliatestation",
		},
	}
	byAlias, byPkg := typeImports(planned)
	assert.Equal(t, "github.vianttech.com/viant/platform/pkg/platform/tvaffiliatestation", byAlias["tvaffiliatestation"])
	assert.Equal(t, "github.vianttech.com/viant/platform/pkg/platform/tvaffiliatestation", byPkg["tvaffiliatestation"])
}

func TestCompareTypeContextParity(t *testing.T) {
	legacy := &typeCtxIR{
		DefaultPackage: "github.vianttech.com/viant/platform/pkg/platform/tvaffiliatestation",
		PackageDir:     "pkg/platform/tvaffiliatestation",
		PackageName:    "tvaffiliatestation",
		PackagePath:    "github.vianttech.com/viant/platform/pkg/platform/tvaffiliatestation",
	}
	shape := &typeCtxIR{
		DefaultPackage: "github.vianttech.com/viant/platform/pkg/platform/tvaffiliatestation",
		PackageDir:     "pkg/platform/tvaffiliatestation",
		PackageName:    "tvaffiliatestation",
		PackagePath:    "github.vianttech.com/viant/platform/pkg/platform/tvaffiliatestation",
	}
	assert.Empty(t, compareTypeContextParity(legacy, shape))
}
