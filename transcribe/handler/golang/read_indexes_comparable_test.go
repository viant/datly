package golang

import (
	"go/ast"
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
	fields "github.com/viant/datly/transcribe/testdata/indexfields"
	"github.com/viant/x"
)

func TestReadIndexesNamedAuxiliaryFields(t *testing.T) {
	const pkg = "github.com/viant/datly/transcribe/testdata/indexfields"
	fixture := partialIdentityFixture{operation: plan.OperationPost}
	semantic := fixture.semantic()
	semantic.Root.Entity.Hooks = spec.TypeRef{}
	semantic.Root.Entity.HooksBind = false
	semantic.ReadCollections = []plan.ReadCollection{{Name: "Aux", InputPath: plan.FieldPath{"Input", "Aux"}, Type: spec.TypeRef{Package: pkg, Name: "Row"}, Keys: []plan.KeyPart{{Field: "ID", Type: spec.TypeRef{Name: "int"}}}, Fields: []plan.FieldRef{{Field: "ID", Type: spec.TypeRef{Name: "int"}}, {Field: "Names", Type: spec.TypeRef{Package: pkg, Name: "Names"}}, {Field: "Counts", Type: spec.TypeRef{Package: pkg, Name: "Counts"}}, {Field: "Alias", Type: spec.TypeRef{Package: pkg, Name: "Alias"}}, {Field: "Label", Type: spec.TypeRef{Package: pkg, Name: "Label"}}, {Field: "Dynamic", Type: spec.TypeRef{Name: "any"}}}}}
	config := fixture.config(semantic)
	config.Imports = []spec.ImportSpec{{Alias: "fields", Package: pkg}}
	types := map[string]*x.Type{}
	for _, typ := range []reflect.Type{reflect.TypeFor[fields.Names](), reflect.TypeFor[fields.Counts](), reflect.TypeFor[fields.Label]()} {
		types[typ.PkgPath()+"."+typ.Name()] = x.NewType(typ)
	}
	types[pkg+".Alias"] = x.NewType(reflect.TypeFor[fields.Names]())
	config.ReadIndexes = &ReadIndexConfig{Package: config.Package, PackagePath: config.PackagePath, InputType: config.InputType, Types: func(name string) (*x.Type, error) { return types[name], nil }}
	asset, err := MutationProgram(semantic, config)
	if err != nil {
		t.Fatal(err)
	}
	files, err := asset.Files()
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range asset.Indexes.File.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			switch fn.Name.Name {
			case "IndexByNames", "GroupByNames", "IndexByCounts", "GroupByCounts", "IndexByAlias", "GroupByAlias", "IndexByDynamic", "GroupByDynamic":
				t.Fatalf("noncomparable helper emitted: %s", fn.Name)
			}
		}
	}
	(entitySyncFixture{entity: asset.Entities, products: files, source: `package events
import("context";"strings";"testing";h "github.com/viant/xdatly/handler";fields "github.com/viant/datly/transcribe/testdata/indexfields")
type Marker struct{Id,TenantId,Name bool};type Record struct{Id *int64;TenantId int64;Name string;Has *Marker};type Input struct{Events []*Record;Aux []*fields.Row};type Output struct{Data []*Record}
type fieldSet struct{reduced bool};func(f fieldSet)Has(name string)bool{return !f.reduced||name!="Counts"}
type projection struct{reduced bool};func(projection)RootHolder()string{return ""};func(projection)DirectOutput()bool{return true};func(p projection)Fields(int,...h.ReadStep)(h.FieldSet,error){return fieldSet{p.reduced},nil}
type metadata struct{reduced bool};func(m metadata)Projection(string)(h.ReadProjection,error){return projection{m.reduced},nil}
func TestNamedAuxiliary(t *testing.T){
 input:=&Input{Aux:[]*fields.Row{{ID:1,Names:fields.Names{"one"},Counts:fields.Counts{"one":1},Label:fields.Label("a"),Dynamic:map[string]int{"x":1}}}}
 indexes,err:=BuildEventsHandlerReadIndexes(h.WithReadMetadata(context.Background(),metadata{}),input);if err!=nil{t.Fatal(err)}
 if !indexes.AuxByID.Has(1)||len(indexes.Aux.GroupByLabel()[fields.Label("a")])!=1{t.Fatal("comparable named helpers missing")}
 indexes.Aux[0].Names[0]="changed";indexes.Aux[0].Counts["one"]=99
 if input.Aux[0].Names[0]!="one"||input.Aux[0].Counts["one"]!=1{t.Fatal("nonindexed fields were not detached")}
 if _,err=BuildEventsHandlerReadIndexes(h.WithReadMetadata(context.Background(),metadata{true}),input);err==nil||!strings.Contains(err.Error(),"Counts"){t.Fatalf("unloaded nonindexed field accepted: %v",err)}
}
`}).run(t)
}
