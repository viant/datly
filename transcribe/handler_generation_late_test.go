package transcribe

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	"github.com/viant/datly/transcribe/handler/golang"
	model "github.com/viant/datly/transcribe/testdata/mutationlate"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestGenericMutationRejectsActualNativeLateEffects(t *testing.T) {
	for _, tc := range []struct {
		name      string
		typ       reflect.Type
		operation plan.Operation
		want      string
	}{
		{"insert hook", reflect.TypeOf(model.InsertRow{}), plan.OperationPost, "native OnInsert"},
		{"update hook", reflect.TypeOf(model.UpdateRow{}), plan.OperationPut, "native OnUpdate"},
		{"default generator", reflect.TypeOf(model.DefaultRow{}), plan.OperationPost, "native default generators"},
		{"wrong signature is not native hook", reflect.TypeOf(model.WrongSignature{}), plan.OperationPost, ""},
		{"autoincrement is sequencing", reflect.TypeOf(model.AutoRow{}), plan.OperationPost, ""},
		{"insert hook on update-only policy", reflect.TypeOf(model.InsertRow{}), plan.OperationPut, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model.Calls.Store(0)
			catalog := typecatalog.NewCatalog()
			if err := catalog.Register(typecatalog.TypeOriginPackage, &x.Type{Name: tc.typ.Name(), PkgPath: tc.typ.PkgPath(), Type: tc.typ}); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: "example.com/generated", Imports: []typecatalog.PackageImport{{Alias: "model", Package: tc.typ.PkgPath()}}})
			if err != nil {
				t.Fatal(err)
			}
			imports := []spec.ImportSpec{{Alias: "model", Package: tc.typ.PkgPath()}}
			g := newHandlerGeneration(nil, &gen.Input{TargetPackage: "example.com/generated", TypeResolver: resolver}, Options{})
			record := &plan.RecordPlan{Identity: "rows", InputPath: plan.FieldPath{"Input", "Rows"}, Table: "records", Cardinality: spec.CardinalityMany, Keys: []plan.KeyPart{{Field: "Id", Type: spec.TypeRef{Name: "int64", Pointer: true}}}, Write: plan.WritePolicy{ValuePath: plan.FieldPath{"Input", "Rows"}}}
			if tc.operation == plan.OperationPost {
				record.Write.Missing = plan.ActionInsert
				record.Write.Allowed = []plan.Action{plan.ActionInsert}
			} else {
				record.Write.Existing = plan.ActionUpdate
				record.Write.Allowed = []plan.Action{plan.ActionUpdate}
			}
			base := "model." + tc.typ.Name()
			valueType := "[]*" + base
			if err = g.refineLinkedEntity(record, base, imports); err != nil {
				t.Fatal(err)
			}
			if err = g.refineLateWriteEffects(record, &gen.Plan{}, valueType); err != nil {
				t.Fatal(err)
			}
			semantic := &plan.Plan{Operation: tc.operation, Input: plan.ContractRef{Path: record.InputPath, Cardinality: spec.CardinalityMany}, Root: record}
			config := golang.Config{Package: "generated", PackagePath: "example.com/generated", Factory: "NewRows", InputType: "Input", OutputType: "Output", Imports: imports, Records: []golang.RecordType{{Identity: record.Identity, Path: record.InputPath, Value: valueType}}}
			entities, err := golang.EntitySupport(semantic, config)
			if err != nil {
				t.Fatal(err)
			}
			frames, err := golang.MutationFrameSupport(semantic, config, entities)
			if err != nil {
				t.Fatal(err)
			}
			actions, err := golang.MutationActionSupport(semantic, config, entities, frames)
			if tc.want != "" {
				if err == nil || !strings.Contains(err.Error(), tc.want) || actions != nil {
					t.Fatalf("action preflight asset%v err%v", actions, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if model.Calls.Load() != 0 {
				t.Fatal("late write hook ran during generation")
			}
			clone := semantic.Clone()
			clone.Root.Entity.LateWrite.DefaultFields = append(clone.Root.Entity.LateWrite.DefaultFields, "other")
			if len(clone.Root.Entity.LateWrite.DefaultFields) == len(record.Entity.LateWrite.DefaultFields) {
				t.Fatal("late effects clone not detached")
			}
		})
	}
}
