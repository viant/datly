package transcribe

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os/exec"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	semantic "github.com/viant/datly/transcribe/handler/ast"
	gotarget "github.com/viant/datly/transcribe/handler/golang"
	model "github.com/viant/datly/transcribe/testdata/mutationhooks"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

func TestGeneratedMutationHookPrerequisiteSQLite(t *testing.T) {
	const target = "github.com/viant/datly/hookprerequisite/generated"
	pkg := reflect.TypeOf(model.Row{}).PkgPath()
	key := semantic.KeyPart{Field: "ID", Source: "id", Type: spec.TypeRef{Name: "int"}}
	child := &semantic.RecordPlan{Identity: "child", InputPath: semantic.FieldPath{"Input", "Rows", "Children"}, Table: "children", Cardinality: spec.CardinalityMany, Keys: []semantic.KeyPart{key}, Write: semantic.WritePolicy{ValuePath: semantic.FieldPath{"Input", "Rows", "Children"}, Missing: semantic.ActionInsert, Allowed: []semantic.Action{semantic.ActionInsert}, Order: 1}, Entity: &semantic.EntityPlan{Type: spec.TypeRef{Package: pkg, Name: "Child"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Package: pkg, Name: "ChildHas"}, Keys: []semantic.KeyPart{key}, Hooks: spec.TypeRef{Package: pkg, Name: "ChildHooks"}, HooksBind: true, Fields: []semantic.EntityField{{Name: "ID", Type: spec.TypeRef{Name: "int"}, Identity: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}, {Name: "Self", Type: spec.TypeRef{Name: "[]*model.Child"}, Relation: true, Self: true}}}}
	rootRecord := &semantic.RecordPlan{Identity: "root", InputPath: semantic.FieldPath{"Input", "Rows"}, Table: "records", Cardinality: spec.CardinalityMany, Keys: []semantic.KeyPart{key}, Write: semantic.WritePolicy{ValuePath: semantic.FieldPath{"Input", "Rows"}, Missing: semantic.ActionInsert, Allowed: []semantic.Action{semantic.ActionInsert}}, Entity: &semantic.EntityPlan{Type: spec.TypeRef{Package: pkg, Name: "Row"}, MarkerField: "Has", MarkerPointer: true, MarkerType: spec.TypeRef{Package: pkg, Name: "RowHas"}, Keys: []semantic.KeyPart{key}, Hooks: spec.TypeRef{Package: pkg, Name: "RootHooks"}, HooksBind: true, Fields: []semantic.EntityField{{Name: "ID", Type: spec.TypeRef{Name: "int"}, Identity: true}, {Name: "Name", Type: spec.TypeRef{Name: "string"}, Writable: true}, {Name: "Children", Type: spec.TypeRef{Name: "[]*model.Child"}, Relation: true}}}, Relations: []*semantic.RelationPlan{{Identity: "Children", FieldPath: semantic.FieldPath{"Children"}, Cardinality: spec.CardinalityMany, Child: child}}}
	plan := &semantic.Plan{Operation: semantic.OperationPost, Root: rootRecord, Input: semantic.ContractRef{Path: semantic.FieldPath{"Input", "Rows"}}, Output: &semantic.ContractRef{Path: semantic.FieldPath{"Output", "Data"}}}
	rootRecord.Relations[0].Links = []semantic.KeyLink{{Parent: key, Child: semantic.KeyPart{Field: "ParentID", Source: "parent_id", Type: spec.TypeRef{Name: "int"}}, Conversion: semantic.LinkDirect}}
	child.Entity.Fields[2].Writable = true
	rootRecord.Entity.Fields[2].Writable = true
	config := gotarget.Config{Package: "hooks", PackagePath: target, Factory: "NewHookHarness", InputType: "model.Input", OutputType: "model.Output", Imports: []spec.ImportSpec{{Alias: "model", Package: pkg}}, Records: []gotarget.RecordType{{Identity: "root", Path: rootRecord.InputPath, Value: "[]*model.Row"}, {Identity: "child", Path: child.InputPath, Value: "[]*model.Child"}}}
	hooks, err := gotarget.MutationHookSupport(plan, config)
	if err != nil {
		t.Fatal(err)
	}
	entities, err := gotarget.EntitySupport(plan, config)
	if err != nil {
		t.Fatal(err)
	}
	harness, err := parser.ParseFile(token.NewFileSet(), "harness.go", mutationHookContract, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	imports := map[string]*ast.ImportSpec{}
	var declarations []ast.Decl
	for _, file := range []*ast.File{harness, hooks.File} {
		for _, declaration := range file.Decls {
			if imported, ok := declaration.(*ast.GenDecl); ok && imported.Tok == token.IMPORT {
				for _, item := range imported.Specs {
					entry := item.(*ast.ImportSpec)
					path, _ := strconv.Unquote(entry.Path.Value)
					imports[path] = entry
				}
				continue
			}
			declarations = append(declarations, declaration)
		}
	}
	paths := make([]string, 0, len(imports))
	for path := range imports {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	importDeclaration := &ast.GenDecl{Tok: token.IMPORT, Lparen: 1}
	for _, path := range paths {
		importDeclaration.Specs = append(importDeclaration.Specs, imports[path])
	}
	harness.Decls = append([]ast.Decl{importDeclaration}, declarations...)
	catalog := typecatalog.NewCatalog()
	for _, typ := range []reflect.Type{reflect.TypeOf(model.Input{}), reflect.TypeOf(model.Output{})} {
		if err = catalog.Register(typecatalog.TypeOriginPackage, x.NewType(typ)); err != nil {
			t.Fatal(err)
		}
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, &typecatalog.ResolutionContext{PackagePath: target, Imports: []typecatalog.PackageImport{{Alias: "model", Package: pkg}}})
	if err != nil {
		t.Fatal(err)
	}
	input := gen.Input{Component: &spec.Component{Name: "Hooks", Routes: []*spec.Route{{Method: "POST", Path: "/hooks"}}, TypeContext: &spec.TypeContext{Imports: []spec.ImportSpec{{Alias: "model", Package: pkg}}}}, TargetPackage: target, TypeResolver: resolver, Contracts: gen.ContractReferences{Input: &gen.ContractReference{Expression: "model.Input", DescriptorKey: pkg + ".Input"}, Output: &gen.ContractReference{Expression: "model.Output", DescriptorKey: pkg + ".Output"}}, ContractHandler: &gen.ContractHandlerAsset{Factory: "NewHookHarness", File: harness}, EntitySupport: &gen.EntitySupportAsset{File: entities.File, CaptureFunction: entities.CaptureFunction}}
	dir := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/hookprerequisite"}).Write(t, dir)
	if _, err = gen.New(input).Generate(dir + "/generated"); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, dir, "generated/hooks_test.go", mutationHookRuntime)
	command := exec.Command("go", "test", "-race", "-mod=mod", "./...")
	command.Dir = dir
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated hooks prerequisite: %v\n%s", err, output)
	}
}

// The test harness deliberately composes only the generated hook prerequisite;
// it is an ordinary Contract, not a fake or completed mutation Program.
const mutationHookContract = `package hooks
import("context";"fmt";xhandler "github.com/viant/xdatly/handler";model "github.com/viant/datly/transcribe/testdata/mutationhooks")
type hookContract struct{}
func NewHookHarness()xhandler.Contract[model.Input,model.Output]{return &hookContract{}}
func(*hookContract)CaptureInput(ctx context.Context,input *model.Input)(any,error){return _newHookHarnessCaptureInput(ctx,input)}
type loadedFields struct{}
func(loadedFields)Has(name string)bool{return name=="ID"||name=="Name"}
func(*hookContract)Exec(ctx context.Context,session xhandler.Session,input *model.Input,output *model.Output)error{
 captured,found,err:=session.Binder().Lookup(ctx,xhandler.InputSnapshotKey);if err!=nil{return err};if !found{return fmt.Errorf("original snapshot missing")}
 original:=captured.(*_newHookHarnessOriginalInput)
 if len(input.Rows)!=1||len(input.Previous)!=1||len(input.PreviousChildren)!=2{return fmt.Errorf("bound current input unavailable")}
 root:=input.Rows[0];child:=root.Children[0];self:=child.Self[0]
 frames:=&_newHookHarnessMutationFrames{
 Role0:[]*_newHookHarnessMutationHooksFrame0{{Entity:root,State:xhandler.EntityState[model.Row,xhandler.NoParent]{Previous:input.Previous[0],PreviousFields:loadedFields{},Original:original.records0[root]}}},
 Role1:[]*_newHookHarnessMutationHooksFrame1{
 {Entity:child,State:xhandler.EntityState[model.Child,model.Row]{Parent:root,Previous:input.PreviousChildren[0],PreviousFields:loadedFields{},Original:original.records1[child]}},
 {Entity:self,State:xhandler.EntityState[model.Child,model.Row]{Parent:root,SelfParent:child,Previous:input.PreviousChildren[1],PreviousFields:loadedFields{},Original:original.records1[self]}},
 },
 }
 hooks:=&_newHookHarnessMutationHooks{}
 if err:=hooks.Prepare(ctx,session.Binder());err!=nil{return err}
 if err:=hooks.Init(ctx,frames);err!=nil{return err}
 if err:=hooks.Validate(ctx,frames);err!=nil{return err}
 value,found,err:=session.Binder().Lookup(ctx,xhandler.DMLKey);if err!=nil{return err};if !found{return fmt.Errorf("DML missing")}
 if err:=value.(xhandler.DML).Execute("UPDATE records SET name=? WHERE id=?",root.Name,root.ID);err!=nil{return err}
 output.Data=input.Rows;return nil
}
`

const mutationHookRuntime = `package hooks
import(
 "context";"net/http/httptest";"reflect";"strings";"testing";"strconv";"fmt"
 "github.com/viant/bindly/locator"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 requestprovider "github.com/viant/bindly/provider/request"
 xhandler "github.com/viant/xdatly/handler"
 "github.com/viant/x"
 model "github.com/viant/datly/transcribe/testdata/mutationhooks"
)
type logger struct{events []string}
func(l *logger)Debug(message string,_ ...any){l.events=append(l.events,message)}
func(*logger)Info(string,...any){};func(*logger)Warn(string,...any){};func(*logger)Error(string,...any){}
func TestGeneratedTypedHookFrames(t *testing.T){
 for _,test:=range []struct{suffix string;logger,expectName bool;want string;events []string}{
 {"ok",true,true,"database:ok",[]string{"root init","child init","child init","root validate","child validate","child validate"}},
 {"ok",true,false,"database:ok",[]string{"root init","child init","child init","root validate","child validate","child validate"}},
 {"reject",true,true,"database",[]string{"root init","child init","child init","root validate"}},
 {"ok",false,true,"database",nil},
 }{t.Run(fmt.Sprintf("%s/logger=%v/originalName=%v",test.suffix,test.logger,test.expectName),func(t *testing.T){
 ctx:=context.Background();db:=sqlite.New(t);if err:=db.ExecStatements(ctx,"CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)","CREATE TABLE children(id INTEGER PRIMARY KEY,name TEXT)","INSERT INTO records VALUES(1,'database')","INSERT INTO children VALUES(10,'child database'),(11,'self database')");err!=nil{t.Fatal(err)}
 exports:=x.NewRegistry();if err:=RegisterHooksFactories(exports);err!=nil{t.Fatal(err)};builder,err:=bootstrap.NewArtifactBuilder(exports);if err!=nil{t.Fatal(err)}
 component:=&spec.Component{Key:spec.Key{Kind:spec.KindComponent,Scope:reflect.TypeOf(Component{}).PkgPath(),Name:"Hooks"},Routes:[]*spec.Route{{Method:"POST",Path:"/hooks",Handler:"NewHookHarness"}}}
 artifact,err:=builder.Build(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(model.Input{}),OutputType:reflect.TypeOf(model.Output{})});if err!=nil{t.Fatal(err)}
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db.DB}});if err!=nil{t.Fatal(err)}
 log:=&logger{};capabilities:=xhandler.Capabilities{};if test.logger{capabilities.Logger=log}
 registered,err:=artifact.Registration(registry.RegisteredComponent{DataSource:dml.Source{DB:db.DB},Providers:[]locator.Provider{views},Capabilities:capabilities});if err!=nil{t.Fatal(err)}
 runtime,err:=druntime.NewRuntime([]*registry.RegisteredComponent{registered});if err!=nil{t.Fatal(err)}
 body:="{\"data\":[{\"id\":1,\"name\":\"client\",\"children\":[{\"id\":10,\"name\":\"client child\",\"self\":[{\"id\":11,\"name\":\"client self\"}]}]}]}"
 if !test.expectName{body=strings.Replace(body,"\"name\":\"client\",","",1)}
 request:=httptest.NewRequest("POST","/hooks?suffix="+test.suffix+"&expectName="+strconv.FormatBool(test.expectName),strings.NewReader(body));request.Header.Set("Content-Type","application/json")
 scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 _,err=runtime.ExecuteRoute(ctx,"POST","/hooks",scope)
 if (err!=nil)!=(test.suffix=="reject"||!test.logger){t.Fatalf("hook error=%v",err)}
 if !reflect.DeepEqual(log.events,test.events){t.Fatalf("hook sequence=%v want=%v error=%v",log.events,test.events,err)}
 db.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT id,name FROM records"},[]model.Row{{ID:1,Name:test.want}})
 })}
}
`
