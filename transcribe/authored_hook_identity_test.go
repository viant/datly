package transcribe

import (
	"testing"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
)

func TestGeneratedEntityIdentityUsesViewDestination(t *testing.T) {
	for _, tc := range []struct{ name, destination, target, want string }{
		{"root", "example.com/entities", "example.com/api", "example.com/entities"},
		{"child", "example.com/items", "example.com/api", "example.com/items"},
		{"default", "", "example.com/api", "example.com/api"},
		{"explicit without fallback", "example.com/entities", "", "example.com/entities"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := &plan.RecordPlan{Identity: "entity", InputPath: plan.FieldPath{"Input", "Records"}, Cardinality: spec.CardinalityMany, Entity: &plan.EntityPlan{Owned: true}}
			generated := &gen.Plan{Views: []gen.ViewPlan{{Identity: record.Identity, Name: "Row", Type: "Row", Package: tc.destination, Ownership: gen.ViewGenerated}}}
			generation := &handlerGeneration{input: &gen.Input{TargetPackage: tc.target}}
			var records []handlergo.RecordType
			if err := generation.appendRecordType(&records, &plan.Plan{Operation: plan.OperationPost, Root: record}, generated, record, "[]*Row"); err != nil {
				t.Fatal(err)
			}
			if record.Entity.Type != (spec.TypeRef{Package: tc.want, Name: "Row"}) {
				t.Errorf("generated entity metadata=%+v; want %s.Row", record.Entity.Type, tc.want)
			}
			compilation := &entityHookCompilation{generation: generation, generated: generated, types: map[string]string{"Input.Records": "[]*Row"}}
			identity, err := compilation.identity(record)
			if err != nil || identity != tc.want+".Row" {
				t.Errorf("authored hook identity=%s err=%v; want %s.Row", identity, err, tc.want)
			}
		})
	}
}
