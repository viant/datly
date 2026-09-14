package output

import (
	"context"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestSingletonReaderCSV(t *testing.T) {
	type row struct {
		ID int `json:"id"`
	}
	plan, err := (Compiler{}).Compile(CompileInput{Component: &spec.Component{RootView: &spec.View{Cardinality: spec.CardinalityOne}}, Type: reflect.TypeOf(&row{})})
	if err != nil {
		t.Fatal(err)
	}
	result, err := plan.Encode(context.Background(), "csv", &row{ID: 7})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Data) == 0 {
		t.Fatal("empty CSV")
	}
}
