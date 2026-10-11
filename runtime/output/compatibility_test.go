package output_test

import (
	"context"
	shared "github.com/viant/datly/output"
	legacy "github.com/viant/datly/runtime/output"
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestExistingOutputAPIUsesSharedCompiledAuthority(t *testing.T) {
	type row struct {
		ID     int
		Secret string
	}
	component := &spec.Component{Settings: &spec.Settings{CaseFormat: "lc", Output: &spec.OutputSettings{Exclude: []string{"Secret"}}}}
	plan, err := (legacy.Compiler{}).Compile(legacy.CompileInput{Component: component, Type: reflect.TypeFor[row]()})
	if err != nil {
		t.Fatal(err)
	}
	var canonical *shared.Plan = plan
	result, err := canonical.Encode(context.Background(), "json", row{ID: 7, Secret: "private"})
	if err != nil || string(result.Data) != `{"id":7}` {
		t.Fatalf("existing caller lost canonical encoding: %s %v", result.Data, err)
	}
	fields, err := plan.SelectorFieldNames(reflect.TypeFor[row](), "")
	if err != nil || len(fields) != 1 || fields[0].PublicName != "id" || fields[0].GoName != "ID" {
		t.Fatalf("selector contract differs: %+v %v", fields, err)
	}
	if legacy.ErrSchemaUnavailable != shared.ErrSchemaUnavailable {
		t.Fatal("schema error identity changed")
	}
}
