package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	gen "github.com/viant/datly/transcribe/generate"
)

const auxiliaryGraphDQL = `#package('github.com/viant/datly/auxownerfixture/generated')
#import('model','github.com/viant/datly/auxownerfixture/model')
#import('local','github.com/viant/datly/auxownerfixture/generated')
#setting($_ = $connector('main'))
#setting($_ = $route('/records','PATCH'))
#setting($_ = $case_format('lc'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Records<[]*Record>(body/data).Cardinality('Many'))
#define($_ = $Data<[]*Record>(output/body).Cardinality('Many'))
#define($_ = $Current<[]*Record>(view/Current).Cardinality('Many').TypeName('Record') /* SELECT id,type_id FROM records WHERE id=1 */)
#define($_ = $CurrentChildren<[]*Child>(view/CurrentChildren).Cardinality('Many').TypeName('Child') /* SELECT id,type_id FROM children WHERE id=2 */)
#define($_ = $CurrentFlights<[]*model.Flight>(view/CurrentFlights).Cardinality('Many') /* SELECT id,type_id FROM flights WHERE type_id=1 */)
SELECT r.*,type(r,'Record'),lifecycle_type(r,'BatchLifecycle'),
 CAST(r.id AS int),CAST(r.type_id AS int),CAST(r.note AS string),tag(r.note,'sqlx:"-"'),
 c.*,type(c,'local.Child','Children'),CAST(c.id AS int),CAST(c.type_id AS int),CAST(c.note AS string),tag(c.note,'sqlx:"-"'),
 f.*,type(f,'model.Flight','Flights'),CAST(f.id AS int),CAST(f.type_id AS int)
FROM (SELECT id,type_id,NULL AS note FROM (records)) r
LEFT JOIN (SELECT id,type_id,NULL AS note FROM (children)) c ON c.type_id=r.id
LEFT JOIN (SELECT id,type_id FROM (flights)) f ON f.type_id=r.id`

func TestGeneratorAuxiliaryCanonicalBodyGraph(t *testing.T) {
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/auxownerfixture"}).Write(t, root)
	writeSourceFile(t, root, "model/flight.go", "package model\ntype Flight struct { Id int `sqlx:\"id,primaryKey\"`; TypeId int `sqlx:\"type_id\"`; Has *FlightHas `setMarker:\"true\" sqlx:\"-\" json:\"-\"` }\ntype FlightHas struct { Id, TypeId bool }\n")
	pkg, err := loaderast.LoadPackageFS(t.Context(), os.DirFS(root), "model")
	if err != nil {
		t.Fatal(err)
	}
	catalog := typecatalog.NewCatalog()
	if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
		t.Fatal(err)
	}
	db := sqlite.New(t)
	if err := db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY,type_id INTEGER)", "CREATE TABLE children(id INTEGER PRIMARY KEY,type_id INTEGER REFERENCES records(id))", "CREATE TABLE flights(id INTEGER PRIMARY KEY,type_id INTEGER REFERENCES records(id))"); err != nil {
		t.Fatal(err)
	}
	source := &Source{Name: "records", Scope: "github.com/viant/datly/auxownerfixture/source", Connector: "main", Text: auxiliaryGraphDQL, Types: catalog, ColumnRefiner: column.New(column.Connections{"main": db.DB})}
	childSource := *source
	childSource.Name = "Child"
	childSource.Text = auxiliaryGraphChildDQL
	if _, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: &childSource, Destination: root}); err != nil {
		t.Fatal(err)
	}
	base := &spec.Component{Name: "records", Routes: []*spec.Route{{Method: "PATCH", Path: "/records", Handler: "OldFactory"}}}
	source.PackageComponent = base
	source.LinkedHandler = func() (rhandler.TypedHandler, error) { t.Fatal("inherited factory invoked"); return nil, nil }
	var prior map[string]string
	for pass := 0; pass < 3; pass++ {
		generated, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
		if err != nil {
			t.Fatal(err)
		}
		if generated.Result.Plan.Handler != "" {
			t.Fatalf("inherited handler survived canonical generation: %s", generated.Result.Plan.Handler)
		}
		if base.Routes[0].Handler != "OldFactory" || source.LinkedHandler == nil {
			t.Fatal("caller execution authority mutated")
		}
		counts := map[string]int{}
		shared := map[string]int{}
		linked := 0
		for _, view := range generated.Result.Plan.Views {
			switch view.Ownership {
			case gen.ViewGenerated:
				counts[view.Name]++
				if (view.Name == "Record" || view.Name == "Child") && len(view.SetMarkerFields) == 0 {
					t.Fatal("missing graph presence", view.Name)
				}
			case gen.ViewLinked:
				linked++
				if len(view.Fields) != 0 || len(view.SetMarkerFields) != 0 || !strings.HasSuffix(view.Type, ".Flight") {
					t.Fatalf("linked authority changed: %+v", view)
				}
			case gen.ViewSharedBody:
				shared[view.Name]++
				if len(view.Fields) != 0 {
					t.Fatal("Current owns fields")
				}
			}
		}
		if linked != 2 || counts["Flight"] != 0 || counts["FlightHas"] != 0 {
			t.Fatal("external type redefined", linked, counts)
		}
		for _, name := range []string{"Record", "Child"} {
			if counts[name] != 1 || shared[name] != 1 {
				t.Fatalf("ownership %s: owners=%d reads=%d", name, counts[name], shared[name])
			}
		}
		snapshot := map[string]string{}
		for _, file := range generated.Result.Files {
			data, err := os.ReadFile(file.Path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), "type Flight struct") || strings.Contains(string(data), "type FlightHas struct") {
				t.Fatal("external definition emitted")
			}
			if strings.HasSuffix(file.Path, "router.go") && (strings.Contains(string(data), "handler=") || !strings.Contains(string(data), `mutation:"patch"`)) {
				t.Fatalf("route not native: %s", data)
			}
			snapshot[file.Path] = string(data)
		}
		if pass == 0 {
			writeSourceFile(t, root, "generated/lifecycle.go", auxiliaryGraphHooks)
			continue
		}
		if pass == 2 && !reflect.DeepEqual(prior, snapshot) {
			t.Fatal("artifact drift")
		}
		prior = snapshot
	}
	if source.Text != auxiliaryGraphDQL {
		t.Fatal("source changed")
	}
	for _, sql := range []string{"SELECT id,type_id FROM records WHERE id=1", "SELECT id,type_id FROM children WHERE id=2"} {
		found := false
		for path, raw := range prior {
			if strings.HasSuffix(path, ".sql") && strings.TrimSpace(raw) == sql {
				found = true
			}
		}
		if !found {
			t.Fatal("Current SQL changed", sql)
		}
	}
	writeSourceFile(t, root, "generated/auxiliary_graph_runtime_test.go", auxiliaryGraphRuntime)
	cmd := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated graph: %v\n%s", err, output)
	}
	// SQL/schema changes remain the authority for local body fields and Has.
	if err := db.ExecStatements(t.Context(), "ALTER TABLE children ADD COLUMN label TEXT"); err != nil {
		t.Fatal(err)
	}
	source.Text = strings.Replace(source.Text, "SELECT id,type_id,NULL AS note FROM (children)", "SELECT id,type_id,label,NULL AS note FROM (children)", 1)
	source.Text = strings.Replace(source.Text, "c.*,type(c,", "c.*,CAST(c.label AS string),type(c,", 1)
	changed, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
	if err != nil {
		t.Fatal(err)
	}
	label, marker := false, false
	for _, view := range changed.Result.Plan.Views {
		if view.Name != "Child" || view.Ownership != gen.ViewGenerated {
			continue
		}
		for _, field := range view.Fields {
			if field.Name == "Label" && field.Type == "string" {
				label = true
			}
		}
		for _, field := range view.SetMarkerFields {
			if field == "Label" {
				marker = true
			}
		}
	}
	if !label || !marker {
		t.Fatal("SQL/schema field failed to regenerate body and presence", label, marker)
	}

}

const auxiliaryGraphHooks = `package generated
import("context";"fmt";h "github.com/viant/xdatly/handler")
var ownerInitCalls,ownerValidateCalls,ownerFinalizeCalls,graphCommits int
type BatchLifecycle struct{Invoker h.ComponentInvoker ` + "`bind:\"kind=component_invoker,required\"`" + `}
func(l *BatchLifecycle)Init(ctx context.Context,input *Input,state h.LifecycleContext[Input,h.NoParent,Output])error{
 ownerInitCalls++
 if state.Previous!=nil||state.Parent!=nil||state.Original!=nil||state.PreviousFields!=nil {return fmt.Errorf("not a canonical Input lifecycle")}
 if len(input.Current)!=1||input.Current[0].Id!=1||input.Current[0].TypeId!=7||len(input.CurrentChildren)!=1||input.CurrentChildren[0].Id!=2 {return fmt.Errorf("Current bindings lost")}
 if input.Current[0].Has!=nil||len(input.Current[0].Children)!=0||len(input.Current[0].Flights)!=0||input.Current[0].Note!=""||input.CurrentChildren[0].Has!=nil||input.CurrentChildren[0].Note!="" {return fmt.Errorf("Current synthesized body fields")}
 if len(input.Records)!=1{return fmt.Errorf("body lost")};row:=input.Records[0]
 if row.Has==nil||!row.Has.Note||!row.Has.Children||row.Has.Flights!=(len(row.Flights)>0)||len(row.Children)!=1||row.Children[0].Has==nil||!row.Children[0].Has.Note {return fmt.Errorf("graph presence lost: %+v",row.Has)}
 if len(row.Flights)>0&&(len(row.Flights)!=1||row.Flights[0].Has==nil||!row.Flights[0].Has.Id) {return fmt.Errorf("linked presence lost")}
 state.Output.Data=input.Records
 child:=&ChildInput{};child.SetRows([]*WriteRow{{Id:2,TypeId:2,Has:&WriteRowHas{Id:true,TypeId:true}}})
 if _,err:=l.Invoker.Invoke(ctx,h.ComponentRequest{Target:h.ComponentTarget{Scope:"github.com/viant/datly/auxownerfixture/generated",Name:"Child",Method:"PATCH",Path:"/write"},Input:child});err!=nil {return err}
 if graphCommits!=ownerInitCalls-1{return fmt.Errorf("child completed caller buffer")}
 return nil
}
func(*BatchLifecycle)Validate(context.Context,*Input,h.LifecycleContext[Input,h.NoParent,Output])error{ownerValidateCalls++;return nil}
func(*BatchLifecycle)Finalize(context.Context,*Input,*Output,h.Outcome)error{ownerFinalizeCalls++;return nil}
`

const auxiliaryGraphRuntime = `package generated
import (
 "context"
 "database/sql"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 _ "github.com/mattn/go-sqlite3"
 "github.com/viant/bindly/locator"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 druntime "github.com/viant/datly/runtime"
 "github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/runtime/handler/writer"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 viewprovider "github.com/viant/datly/sql/reader/provider"
 dtag "github.com/viant/datly/tag"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
func borrowedRuntimeEntry(t *testing.T,db *sql.DB,resources *resource.Store,holder,input,output reflect.Type)*registry.RegisteredComponent{
 t.Helper();field,ok:=holder.FieldByName("Contract");if !ok{t.Fatal("generated holder missing Contract")}
 tag,ok,err:=dtag.ParseComponent(field.Tag);if err!=nil||!ok{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:input.Name(),OutputType:output.Name()}
 component,err:=source.Resolve(input,output);if err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:input,OutputType:output,Resources:resources});if err!=nil{t.Fatal(err)}
 metadata,err:=writer.Compile(artifact.Component,input,output,"patch");if err!=nil{t.Fatal(err)};if metadata.Root.Auxiliary {t.Fatal("typed child lost physical writer")}
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
 native,err:=writer.New(artifact.Component,input,output,"patch");if err!=nil{t.Fatal(err)}
 return &registry.RegisteredComponent{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:output,Handler:native,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db,OnCommit:func(context.Context){graphCommits++}}}
}

func TestGeneratedAuxiliaryGraph(t *testing.T){
 ctx:=context.Background();db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 for _,query:=range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY,type_id INTEGER)","INSERT INTO records VALUES(1,7),(2,8)","CREATE TABLE children(id INTEGER PRIMARY KEY,type_id INTEGER)","INSERT INTO children VALUES(2,1)","CREATE TABLE flights(id INTEGER PRIMARY KEY,type_id INTEGER)","CREATE TRIGGER no_write BEFORE UPDATE ON records BEGIN SELECT RAISE(ABORT,'auxiliary write');END"}{if _,err=db.Exec(query);err!=nil{t.Fatal(err)}}
 for _,name:=range []string{"Note"}{field,ok:=reflect.TypeFor[Record]().FieldByName(name);if !ok||field.Tag.Get("sqlx")!="-"||field.Tag.Get("view")!=""||field.Tag.Get("on")!=""{t.Fatalf("transient became relation: %s",name)}}
 holder:=reflect.TypeFor[RecordsComponent]();field,_:=holder.FieldByName("Contract");tag,ok,err:=dtag.ParseComponent(field.Tag);if err!=nil||!ok{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"}
 component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err=resources.Register(RecordsDatlyResourceNamespace,RecordsDatlyResources);err!=nil{t.Fatal(err)};if err=resources.Register(ChildDatlyResourceNamespace,ChildDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
 metadata,err:=writer.Compile(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil{t.Fatal(err)}
 if !metadata.Root.Auxiliary||len(metadata.Root.Relations)!=2{t.Fatal("not an auxiliary graph")}
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
 native,err:=writer.New(artifact.Component,reflect.TypeFor[Input](),reflect.TypeFor[Output](),"patch");if err!=nil{t.Fatal(err)}
 child:=borrowedRuntimeEntry(t,db,resources,reflect.TypeFor[ChildComponent](),reflect.TypeFor[ChildInput](),reflect.TypeFor[ChildOutput]())
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[Output](),Handler:native,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db,OnCommit:func(context.Context){graphCommits++}}},child},druntime.WithResources(resources));if err!=nil{t.Fatal(err)};defer rt.Shutdown(ctx)
 for i,body:=range []string{"{\"data\":[{\"id\":1,\"typeId\":99,\"note\":\"request-only\",\"children\":[{\"id\":2,\"typeId\":1,\"note\":\"child-only\"}],\"flights\":[{\"id\":3,\"typeId\":1}]}]}","{\"data\":[{\"id\":1,\"typeId\":99,\"note\":\"request-only\",\"children\":[{\"id\":2,\"typeId\":1,\"note\":\"child-only\"}]}]}"} {
 request:=httptest.NewRequest("PATCH","/records",strings.NewReader(body));request.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 output,err:=rt.ExecuteRoute(ctx,"PATCH","/records",scope);if err!=nil{t.Fatal(err)}
 if ownerInitCalls!=i+1||ownerValidateCalls!=i+1||ownerFinalizeCalls!=i+1||graphCommits!=i+1||len(output.(*Output).Data)!=1||output.(*Output).Data[0].Note!="request-only"{t.Fatal("canonical lifecycle/output lost")}
 }
 var childValue int;if err=db.QueryRow("SELECT type_id FROM children WHERE id=2").Scan(&childValue);err!=nil||childValue!=2 {t.Fatalf("typed child write: %d %v",childValue,err)}
 var count,total int;if err=db.QueryRow("SELECT COUNT(*),SUM(type_id) FROM records").Scan(&count,&total);err!=nil||count!=2||total!=15{t.Fatalf("auxiliary DML: %d %d %v",count,total,err)}
 if err=db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table'").Scan(&count);err!=nil||count!=3{t.Fatalf("auxiliary allocated storage: %d %v",count,err)}
}
`

const auxiliaryGraphChildDQL = `#package('github.com/viant/datly/auxownerfixture/generated')
#setting($_ = $file_prefix('child_'))
#setting($_ = $connector('main'))
#setting($_ = $route('/write','PATCH'))
#setting($_ = $case_format('lc'))
#setting($_ = $input_type('ChildInput'))
#setting($_ = $output_type('ChildOutput'))
#define($_ = $Rows<[]*WriteRow>(body/data))
#define($_ = $Data<[]*WriteRow>(output/body))
SELECT c.*,type(c,'WriteRow'),CAST(c.id AS int),CAST(c.type_id AS int)
FROM children c`
