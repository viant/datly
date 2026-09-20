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
