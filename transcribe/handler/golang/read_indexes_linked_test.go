package golang

import (
	"go/ast"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestReadIndexesWithForeignInputContract(t *testing.T) {
	const modelPackage = "github.com/viant/datly/transcribe/testdata/mutationhooks"
	semantic := rootSemanticPlan(plan.OperationPatch, false)
	root := semantic.Root
	key := plan.KeyPart{Field: "ID", Source: "id", Type: spec.TypeRef{Name: "int"}}
	root.Keys = []plan.KeyPart{key}
	root.Sequence = nil
	root.InputPath = plan.FieldPath{"Input", "Rows"}
	root.Table = "records"
	root.Write.ValuePath = root.InputPath
	semantic.Input.Path = root.InputPath
	root.Entity = &plan.EntityPlan{Type: spec.TypeRef{Package: modelPackage, Name: "Row"}, MarkerField: "Has", MarkerPointer: true, Keys: root.Keys, Fields: []plan.EntityField{{Name: "ID", Type: key.Type, Identity: true, Writable: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}}}
	root.Current.InputPath = plan.FieldPath{"Input", "Previous"}
	root.Current.Keys = root.Keys
	root.Current.Fields = []plan.CurrentField{{Current: plan.FieldRef{Field: "ID", Type: key.Type}, Entity: plan.FieldRef{Field: "ID", Type: key.Type}, Conversion: plan.LinkDirect}, {Current: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Entity: plan.FieldRef{Field: "Name", Type: spec.TypeRef{Name: "string"}}, Conversion: plan.LinkDirect}}
	read := plan.ReadCollection{Name: "Previous", InputPath: root.Current.InputPath, Type: spec.TypeRef{Package: modelPackage, Name: "Row"}, Keys: root.Keys}
	for _, field := range root.Current.Fields {
		read.Fields = append(read.Fields, field.Current)
	}
	semantic.ReadCollections = []plan.ReadCollection{read}
	config := Config{Package: "events", PackagePath: "github.com/viant/datly/syncfixture", Factory: "NewEventsHandler", InputType: "model.Input", OutputType: "model.Output", Imports: []spec.ImportSpec{{Alias: "model", Package: modelPackage}}, Records: rootRecordTypes(semantic, "[]*model.Row", "[]*model.Row")}
	config.ReadIndexes = &ReadIndexConfig{Package: config.Package, PackagePath: config.PackagePath, InputType: config.InputType}
	asset, err := MutationProgram(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	files, err := asset.Files()
	if err != nil {
		t.Fatal(err)
	}
	var products []*ast.File
	for _, file := range files {
		if file != asset.Entities.File {
			products = append(products, file)
		}
	}
	(entitySyncFixture{entity: asset.Entities, products: products, source: `package events
import("context";"fmt";"testing";h "github.com/viant/xdatly/handler";model "github.com/viant/datly/transcribe/testdata/mutationhooks")
type readFields struct{};func(readFields)Has(name string)bool{return name=="ID"||name=="Name"}
type projection struct{};func(projection)RootHolder()string{return ""};func(projection)DirectOutput()bool{return true};func(projection)Fields(int,...h.ReadStep)(h.FieldSet,error){return readFields{},nil}
type metadata struct{};func(metadata)Projection(string)(h.ReadProjection,error){return projection{},nil}
func TestForeignInputAdapter(t *testing.T){
 input:=&model.Input{Rows:[]*model.Row{{Name:"request",Has:&model.RowHas{Name:true}}},Previous:[]*model.Row{{ID:0,Name:"stored"}}}
 definition:=NewEventsHandler().(*EventsHandlerDefinition)
 called:=0
 definition.ResolveIdentity=func(_ context.Context,actual *model.Input,indexes *EventsHandlerReadIndexes)error{
  called++;if actual!=input||!indexes.PreviousByID.Has(0)||len(indexes.Previous.GroupByName()["stored"])!=1{return fmt.Errorf("foreign typed read access failed")}
  actual.Rows[0].ID=indexes.PreviousByID[0].ID;actual.Rows[0].Has.ID=true;return nil
 }
 program,err:=definition.Capture(h.WithReadMetadata(context.Background(),metadata{}),input);if err!=nil{t.Fatal(err)}
 if called!=1||program.(*_newEventsHandlerProgram).original.Roots[0].Has("ID"){t.Fatal("adapter repeated or changed original presence")}
}
`}).run(t)
}
