package bootstrap

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestGoShapeRecordCountMetadata(t *testing.T) {
	type input struct {
		Rows []int `parameter:"Rows,kind=body,in=rows,minAllowedRecords=1,maxAllowedRecords=3,expectedReturned=2"`
	}
	component, err := (ContractResolver{Component: &spec.Component{}, InputType: linkedContractType(reflect.TypeOf(input{}))}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	p := component.Parameters[0]
	if p.MinAllowedRecords == nil || *p.MinAllowedRecords != 1 || p.MaxAllowedRecords == nil || *p.MaxAllowedRecords != 3 || p.ExpectedReturned == nil || *p.ExpectedReturned != 2 {
		t.Fatalf("lost constraints: %+v", p)
	}
	cloned := p.Clone()
	*cloned.MinAllowedRecords = 99
	*cloned.MaxAllowedRecords = 99
	*cloned.ExpectedReturned = 99
	if *p.MinAllowedRecords != 1 || *p.MaxAllowedRecords != 3 || *p.ExpectedReturned != 2 {
		t.Fatal("clone aliased limits")
	}
}
