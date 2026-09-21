package bootstrap

import (
	"reflect"
	"testing"

	"github.com/viant/datly/typecatalog"
)

type ReflectedPackageInput struct {
	ID int
}

type ReflectedPackageOutput struct {
	Rows []ReflectedPackageRow
}

type ReflectedPackageRow struct {
	ID int
}

type ReflectedOrdinaryHandler struct {
	Name string
}

var reflectedOrdinaryHandlerLink = reflect.TypeFor[ReflectedOrdinaryHandler]()

var reflectedLocalWireOutputOneLink = reflectedLocalWireOutputOne()
var reflectedLocalWireOutputTwoLink = reflectedLocalWireOutputTwo()

func reflectedLocalWireOutputOne() any {
	type wireOutput struct {
		ID int
	}
	return wireOutput{}
}

func reflectedLocalWireOutputTwo() any {
	type wireOutput struct {
		Name string
	}
	return wireOutput{}
}

func TestRegisterContractTypesIgnoresFunctionLocalHelperTypes(t *testing.T) {
	_, _ = reflectedLocalWireOutputOne(), reflectedLocalWireOutputTwo()

	contracts := map[reflect.Type]bool{}
	collectContractTypes(contracts, reflect.TypeFor[ReflectedPackageInput]())
	collectContractTypes(contracts, reflect.TypeFor[ReflectedPackageOutput]())
	if contracts[reflect.TypeOf(reflectedLocalWireOutputOneLink)] || contracts[reflect.TypeOf(reflectedLocalWireOutputTwoLink)] {
		t.Fatalf("function-local helper leaked into contract closure: %+v", contracts)
	}
	if err := registerContractTypes(typecatalog.NewCatalog(), contracts); err != nil {
		t.Fatal(err)
	}
}

func TestReflectPackagesIncludesOrdinaryExportedTypes(t *testing.T) {
	_ = reflectedOrdinaryHandlerLink
	reflected, err := ReflectPackages([]string{"github.com/viant/datly/bootstrap"})
	if err != nil {
		t.Fatal(err)
	}
	const key = "github.com/viant/datly/bootstrap.ReflectedOrdinaryHandler"
	resolved, ok, err := reflected.Types.Resolve(typecatalog.PackageAuthority, key)
	if err != nil || !ok || resolved == nil || resolved.Type != reflect.TypeFor[ReflectedOrdinaryHandler]() {
		t.Fatalf("ordinary package type %s: resolved=%#v found=%v err=%v", key, resolved, ok, err)
	}
}
