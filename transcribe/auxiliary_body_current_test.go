package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
)

// An auxiliary request graph and its independent leaf reads share public row
// identities; Current SQL must not acquire the request's relation topology.
func TestAuxiliaryGeneratedBodySharesIndependentCurrentRows(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/auxbodyfixture"}).Write(t, root)
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx,
		"CREATE TABLE scopes(id INTEGER PRIMARY KEY)",
		"CREATE TABLE groups(id INTEGER PRIMARY KEY, scope_id INTEGER REFERENCES scopes(id))",
		"CREATE TABLE items(id INTEGER PRIMARY KEY, group_id INTEGER REFERENCES groups(id), value INTEGER)"); err != nil {
		t.Fatal(err)
	}
	text := `#package('github.com/viant/datly/auxbodyfixture/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/scope-items','PATCH'))
#setting($_ = $case_format('lc'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Scopes<[]*Scope>(body/data))
#define($_ = $Data<[]*Scope>(output/body))
#define($_ = $CurScopes<?>(view/CurScopes).Cardinality('Many').TypeName('Scope').WithColumnType('id','int') /* SELECT id FROM scopes WHERE id=1 */)
#define($_ = $CurGroups<?>(view/CurGroups).Cardinality('Many').TypeName('Group').WithColumnType('id','int').WithColumnType('scope_id','int') /* SELECT id,scope_id FROM groups WHERE scope_id=1 */)
#define($_ = $CurItems<?>(view/CurItems).Cardinality('Many').TypeName('Item').WithColumnType('id','int').WithColumnType('group_id','int').WithColumnType('value','int') /* SELECT id,group_id,value FROM items WHERE group_id=2 */)
SELECT s.id, type(s,'Scope'), CAST(s.id AS int),
 g.id,g.scope_id,type(g,'Group','Groups'),CAST(g.id AS int),CAST(g.scope_id AS int),
 i.id,i.group_id,i.value,i.should_delete,type(i,'Item','Items'),
 CAST(i.id AS int),CAST(i.group_id AS int),CAST(i.value AS int),
 CAST(i.should_delete AS bool),internal_transient(i.should_delete),delete_marker(i.should_delete),writer_action_policy(i,'insert-delete')
FROM (SELECT id FROM (scopes)) s
LEFT JOIN (SELECT id,scope_id FROM (groups)) g ON g.scope_id=s.id
LEFT JOIN (SELECT id,group_id,value,'' AS should_delete FROM items) i ON i.group_id=g.id
`
	source := &Source{Name: "scopes", Scope: "github.com/viant/datly/auxbodyfixture/source", Connector: "main", Text: text, ColumnRefiner: column.New(column.Connections{"main": db.DB})}
	var first map[string]string
	for pass := 0; pass < 2; pass++ {
		result, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
		if err != nil {
			t.Fatal(err)
		}
		shared := map[string]bool{}
		for _, v := range result.Result.Plan.Views {
			if v.Ownership == gen.ViewSharedBody {
				if len(v.Fields) != 0 || v.SharedBodyIdentity == "" {
					t.Fatalf("Current became an independent shape owner: %+v", v)
				}
				shared[v.Name] = true
			}
		}
		for _, name := range []string{"Scope", "Group", "Item"} {
			if !shared[name] {
				t.Fatal("body row identity not shared", name)
			}
		}
		snapshot := map[string]string{}
		for _, f := range result.Result.Files {
			data, e := os.ReadFile(f.Path)
			if e != nil {
				t.Fatal(e)
			}
			snapshot[f.Path] = string(data)
		}
		if pass == 0 {
			first = snapshot
		} else if !reflect.DeepEqual(first, snapshot) {
			t.Fatal("regeneration drift")
		}
	}
	if source.Text != text {
		t.Fatal("source changed")
	}
	for _, sql := range []string{"SELECT id FROM scopes WHERE id=1", "SELECT id,scope_id FROM groups WHERE scope_id=1", "SELECT id,group_id,value FROM items WHERE group_id=2"} {
		found := false
		for path, raw := range first {
			if strings.HasSuffix(path, ".sql") && strings.TrimSpace(raw) == sql {
				found = true
			}
		}
		if !found {
			t.Fatalf("independent SQL changed or missing: %s; artifacts=%v", sql, first)
		}
	}
	input, err := os.ReadFile(filepath.Join(root, "generated/input.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"CurScopes", "CurGroups", "CurItems"} {
		if !strings.Contains(string(input), name) {
			t.Fatal("Current input lost", name)
		}
	}
	writeSourceFile(t, root, "generated/auxiliary_native_test.go", auxiliarySharedBodyRuntime)
	command := exec.CommandContext(ctx, "go", "test", "-mod=readonly", "-count=1", "-run", "^TestAuxiliarySharedNativeActions$", "./generated")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generated native graph: %v\n%s", err, output)
	}

}

const auxiliarySharedBodyRuntime = `package generated
import (
 "context"
 "database/sql"
 "fmt"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 _ "github.com/mattn/go-sqlite3"
 "github.com/viant/bindly/locator"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 dexec "github.com/viant/datly/exec"
 "github.com/viant/datly/spec"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/datly/runtime/registry"
 writer "github.com/viant/datly/runtime/handler/writer"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 dtag "github.com/viant/datly/tag"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
func(i *Input)Init(context.Context)error{
 for _,s:=range i.CurScopes {if s.Groups!=nil||s.Has!=nil{panic(fmt.Sprintf("Current gained body relations/presence Groups=%v Has=%v",s.Groups,s.Has))}}
 for _,g:=range i.CurGroups {if g.Items!=nil||g.Has!=nil{panic("Current child gained body relations/presence")}}
 for _,i:=range i.CurItems {if i.ShouldDelete||i.Has!=nil{panic("Current gained deletion intent/presence")}}
 return nil
}
func TestAuxiliarySharedNativeActions(t *testing.T){
 for _,tc:=range []struct{name,body string;count int}{
 {"insert","{\"data\":[{\"id\":1,\"groups\":[{\"id\":2,\"items\":[{\"groupId\":2,\"value\":99}]}]}]}",2},
 {"delete","{\"data\":[{\"id\":1,\"groups\":[{\"id\":2,\"items\":[{\"id\":3}]}]}]}",0},
 }{t.Run(tc.name,func(t *testing.T){
 ctx:=context.Background();db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 for _,q:=range []string{
 "CREATE TABLE scopes(id INTEGER PRIMARY KEY)","CREATE TABLE groups(id INTEGER PRIMARY KEY,scope_id INTEGER REFERENCES scopes(id))","CREATE TABLE items(id INTEGER PRIMARY KEY,group_id INTEGER REFERENCES groups(id),value INTEGER)",
 "INSERT INTO scopes VALUES(1)","INSERT INTO groups VALUES(2,1)","INSERT INTO items VALUES(3,2,10)",
 "CREATE TRIGGER no_scope_insert BEFORE INSERT ON scopes BEGIN SELECT RAISE(ABORT,'auxiliary scope insert'); END",
 "CREATE TRIGGER no_scope_update BEFORE UPDATE ON scopes BEGIN SELECT RAISE(ABORT,'auxiliary scope update'); END",
 "CREATE TRIGGER no_scope_delete BEFORE DELETE ON scopes BEGIN SELECT RAISE(ABORT,'auxiliary scope delete'); END",
 "CREATE TRIGGER no_group_insert BEFORE INSERT ON groups BEGIN SELECT RAISE(ABORT,'auxiliary group insert'); END",
 "CREATE TRIGGER no_group_update BEFORE UPDATE ON groups BEGIN SELECT RAISE(ABORT,'auxiliary group update'); END",
 "CREATE TRIGGER no_group_delete BEFORE DELETE ON groups BEGIN SELECT RAISE(ABORT,'auxiliary group delete'); END",
 } {if _,err=db.Exec(q);err!=nil{t.Fatal(err)}}
 holder:=reflect.TypeFor[ScopesComponent]();field,_:=holder.FieldByName("Contract");tag,ok,err:=dtag.ParseComponent(field.Tag);if err!=nil||!ok{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"}
 component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err=resources.Register(ScopesDatlyResourceNamespace,ScopesDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
 native,err:=writer.New(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil{t.Fatal(err)}
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[Output](),Handler:native,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)};defer rt.Shutdown(ctx)
 request:=httptest.NewRequest("PATCH","/scope-items",strings.NewReader(tc.body));request.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 // Insert/delete policy freezes removal intent at capture. A typed composing
 // caller prepares internal deletion intent before invoking this writer.
 if tc.name=="delete" {
  item:=&Item{};item.SetId(3);item.SetShouldDelete(true)
  group:=&Group{};group.SetId(2);group.Items=[]*Item{item}
  parent:=&Scope{};parent.SetId(1);parent.Groups=[]*Group{group}
  input:=&Input{};input.SetScopes([]*Scope{parent})
  _,err=rt.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:component.Key,Route:spec.RouteRef{Method:"PATCH",Path:"/scope-items"}},Input:input})
 } else { _,err=rt.ExecuteRoute(ctx,"PATCH","/scope-items",scope) }
 if err!=nil{t.Fatal(err)}
 var count int;if err=db.QueryRow("SELECT COUNT(*) FROM items").Scan(&count);err!=nil||count!=tc.count{t.Fatalf("items=%d: %v",count,err)}
 for _,table:=range []string{"scopes","groups"}{if err=db.QueryRow("SELECT COUNT(*) FROM "+table).Scan(&count);err!=nil||count!=1{t.Fatalf("auxiliary %s changed",table)}}
 })}
}
`
