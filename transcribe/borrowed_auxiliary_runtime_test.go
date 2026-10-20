package transcribe

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBorrowedAuxiliaryOwnerComposesNativeChildSQLite(t *testing.T) {
	root, owner, child, _ := borrowedAuxiliaryOwnerFixture(t)
	writeSourceFile(t, root, "generated/lifecycle.go", borrowedAuxiliaryCompositionHooks)
	// Re-link the authored hook through the same public generation path. Both
	// body shapes and all child Current/marker metadata remain SQL-derived.
	for _, source := range []*Source{owner, child} {
		_, err := (Generator{Operation: "patch"}).Generate(t.Context(), GenerationRequest{Source: source, Destination: root})
		require.NoError(t, err)
	}
	writeSourceFile(t, root, "generated/borrowed_auxiliary_runtime_test.go", borrowedAuxiliaryCompositionRuntime)
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "-v", "-run", "^TestBorrowedAuxiliaryComposition$", "./generated")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generated composed native SQLite runtime: %v\n%s\nfixture=%s", err, output, filepath.Join(root, "generated"))
	}
	t.Log(string(output))
}

const borrowedAuxiliaryCompositionHooks = `package generated
import("context";"fmt";h "github.com/viant/xdatly/handler")
var borrowedOwnerCalls,borrowedChildCalls,borrowedCommits int
var borrowedFull,borrowedSelected []*Record
var borrowedParentOutcome h.Outcome
type BatchLifecycle struct{Invoker h.ComponentInvoker ` + "`bind:\"kind=component_invoker,required\"`" + `}
func(l *BatchLifecycle)Init(ctx context.Context,input *Input,state h.LifecycleContext[Input,h.NoParent,Output])error{
 borrowedOwnerCalls++
 if state.Previous!=nil||state.Parent!=nil||state.Original!=nil||state.PreviousFields!=nil{return fmt.Errorf("auxiliary parent acquired physical entity execution")}
 if len(input.Current)!=1||input.Current[0].Id!=1||input.Current[0].Kind!=7||len(input.Candidates)!=1||input.Candidates[0].Id!=2||input.Candidates[0].Kind!=8{return fmt.Errorf("original parent Current SQL changed")}
 evidence,ok:=h.ReadMetadataFromContext(ctx);if !ok{return fmt.Errorf("read evidence missing")}
 for _,name:=range []string{"Current","Candidates"}{projection,err:=evidence.Projection(name);if err!=nil{return err};fields,err:=projection.Fields(0);if err!=nil{return err};if !fields.Has("Id")||!fields.Has("Kind")||fields.Has("Note")||fields.Has("Caller")||fields.Has("Has"){return fmt.Errorf("invalid physical read evidence: %s",name)}}
 for _,row:=range append(append([]*Record{},input.Current...),input.Candidates...){if row.Has!=nil||row.Note!=""||row.Caller!=nil{return fmt.Errorf("independent read acquired body-only fields")}}
 borrowedFull=input.Records
 state.Output.Data=input.Records
 for _,row:=range input.Records{if row!=nil&&row.Note!="skip"{borrowedSelected=append(borrowedSelected,row)}}
 if len(borrowedSelected)!=2||borrowedSelected[0]!=input.Records[0]||borrowedSelected[1]!=input.Records[3]{return fmt.Errorf("selected pointer order changed")}
 if input.Records[1].Id!=0||input.Records[3].Id!=0{return fmt.Errorf("parent allocated request rows")}
 child:=&ChildInput{};child.SetRecords(borrowedSelected)
 borrowedChildCalls++
 _,err:=l.Invoker.Invoke(ctx,h.ComponentRequest{Target:h.ComponentTarget{Scope:"github.com/viant/datly/auxownerfixture/generated",Name:"Child",Method:"PATCH",Path:"/internal/write"},Input:child})
 if err!=nil{return err}
 if borrowedCommits!=0{return fmt.Errorf("child committed outside caller buffer")}
 if child.Records[0]!=borrowedSelected[0]||child.Records[1]!=borrowedSelected[1]||input.Records[1].Id!=0||input.Records[3].Id<=2{return fmt.Errorf("child allocation or shared pointers lost")}
 return nil
}
func(*BatchLifecycle)Validate(context.Context,*Input,h.LifecycleContext[Input,h.NoParent,Output])error{return nil}
func(*BatchLifecycle)Finalize(_ context.Context,_ *Input,_ *Output,outcome h.Outcome)error{borrowedParentOutcome=outcome;return nil}
`

const borrowedAuxiliaryCompositionRuntime = `package generated
import(
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
 h "github.com/viant/xdatly/handler"
 _ "github.com/viant/sqlx/metadata/product/sqlite"
)
func borrowedRuntimeEntry(t *testing.T,db *sql.DB,resources *resource.Store,holder,input,output reflect.Type)*registry.RegisteredComponent{
 t.Helper();field,ok:=holder.FieldByName("Contract");if !ok{t.Fatal("generated holder missing Contract")}
 tag,ok,err:=dtag.ParseComponent(field.Tag);if err!=nil||!ok{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"generated",PackagePath:holder.PkgPath(),Tag:tag,InputType:input.Name(),OutputType:output.Name()}
 component,err:=source.Resolve(input,output);if err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:input,OutputType:output,Resources:resources});if err!=nil{t.Fatal(err)}
 metadata,err:=writer.Compile(artifact.Component,input,output,"patch");if err!=nil{t.Fatal(err)}
 if holder==reflect.TypeFor[OwnerComponent](){if !metadata.Root.Auxiliary||metadata.Sequence!=nil||metadata.Root.Sequence!=nil||len(metadata.Root.Relations)!=0{t.Fatal("parent acquired physical write/allocation plan")}}
 if holder==reflect.TypeFor[ChildComponent](){if metadata.Root.Auxiliary||metadata.Sequence==nil||metadata.Root.Sequence==nil{t.Fatal("child missing physical allocation plan")}}
 views,err:=viewprovider.New(viewprovider.Config{Dependencies:artifact.ViewDependencies,Input:artifact.Input,SQL:&dsql.SQLComponent{DB:db}});if err!=nil{t.Fatal(err)}
 native,err:=writer.New(artifact.Component,input,output,"patch");if err!=nil{t.Fatal(err)}
 return &registry.RegisteredComponent{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:output,Handler:native,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db,OnCommit:func(context.Context){borrowedCommits++}}}
}
func TestBorrowedAuxiliaryComposition(t *testing.T){
 for _,fail:=range []bool{false,true}{name:="insert-and-update";if fail{name="late-write-rolls-back-both"};t.Run(name,func(t *testing.T){
 ctx:=context.Background();db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
 for _,query:=range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY,type_id INTEGER)","INSERT INTO records VALUES(1,7),(2,8)"}{if _,err=db.Exec(query);err!=nil{t.Fatal(err)}}
 if fail{for _,query:=range []string{
 "CREATE TRIGGER fail_late_insert BEFORE INSERT ON records WHEN NEW.type_id=90 AND EXISTS(SELECT 1 FROM records WHERE id=1 AND type_id=70) BEGIN SELECT RAISE(ABORT,'late composed product write');END",
 "CREATE TRIGGER fail_late_update BEFORE UPDATE ON records WHEN NEW.id=1 AND NEW.type_id=70 AND EXISTS(SELECT 1 FROM records WHERE type_id=90) BEGIN SELECT RAISE(ABORT,'late composed product write');END",
 }{if _,err=db.Exec(query);err!=nil{t.Fatal(err)}}}
 borrowedOwnerCalls,borrowedChildCalls,borrowedCommits=0,0,0;borrowedFull,borrowedSelected=nil,nil;borrowedParentOutcome=h.Outcome{}
 resources:=resource.New();if err=resources.Register(OwnerDatlyResourceNamespace,OwnerDatlyResources);err!=nil{t.Fatal(err)};if err=resources.Register(ChildDatlyResourceNamespace,ChildDatlyResources);err!=nil{t.Fatal(err)}
 owner:=borrowedRuntimeEntry(t,db,resources,reflect.TypeFor[OwnerComponent](),reflect.TypeFor[Input](),reflect.TypeFor[Output]())
 child:=borrowedRuntimeEntry(t,db,resources,reflect.TypeFor[ChildComponent](),reflect.TypeFor[ChildInput](),reflect.TypeFor[ChildOutput]())
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{owner,child},druntime.WithResources(resources));if err!=nil{t.Fatal(err)};defer rt.Shutdown(ctx)
 request:=httptest.NewRequest("PATCH","/records",strings.NewReader("{\"data\":[{\"id\":1,\"kind\":70,\"note\":\"update\"},{\"kind\":999,\"note\":\"skip\"},{\"id\":2,\"kind\":888,\"note\":\"skip\"},{\"kind\":90,\"note\":\"insert\"}]}"));request.Header.Set("Content-Type","application/json")
 scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 value,callErr:=rt.ExecuteRoute(ctx,"PATCH","/records",scope)
 if fail{if callErr==nil||!strings.Contains(callErr.Error(),"late composed product write"){t.Fatalf("missing real late SQL failure: %v",callErr)};if borrowedCommits!=0||borrowedParentOutcome.State()!=h.TransactionRolledBack||borrowedParentOutcome.Error==nil{t.Fatalf("rollback outcome=%+v commits=%d",borrowedParentOutcome,borrowedCommits)}}else{
 if callErr!=nil{t.Fatal(callErr)};output,ok:=value.(*Output);if !ok||len(output.Data)!=4{t.Fatalf("parent output=%T",value)}
 for i,row:=range output.Data{if row!=borrowedFull[i]{t.Fatal("parent replaced full request pointer",i)}}
 if borrowedCommits!=1||!borrowedParentOutcome.CommitConfirmed(){t.Fatalf("completion=%+v commits=%d",borrowedParentOutcome,borrowedCommits)}
 }
 if borrowedOwnerCalls!=1||borrowedChildCalls!=1||len(borrowedSelected)!=2||borrowedFull[1].Id!=0||borrowedFull[3].Id<=2{t.Fatalf("owner/child/selection/allocation: %d %d %+v",borrowedOwnerCalls,borrowedChildCalls,borrowedFull)}
 var count,kind,total int;if err=db.QueryRow("SELECT COUNT(*),SUM(type_id) FROM records").Scan(&count,&total);err!=nil{t.Fatal(err)}
 if fail{if count!=2||total!=15{t.Fatalf("partial composed write escaped rollback: rows=%d sum=%d",count,total)}}else{if count!=3||total!=168{t.Fatalf("native insert/update or parent exclusion: rows=%d sum=%d",count,total)};if err=db.QueryRow("SELECT type_id FROM records WHERE id=1").Scan(&kind);err!=nil||kind!=70{t.Fatalf("native child update missing: %d %v",kind,err)};if err=db.QueryRow("SELECT type_id FROM records WHERE id=?",borrowedFull[3].Id).Scan(&kind);err!=nil||kind!=90{t.Fatalf("native child allocated insert missing: %d %v",kind,err)}}
 if err=db.QueryRow("SELECT type_id FROM records WHERE id=2").Scan(&kind);err!=nil||kind!=8{t.Fatalf("unselected parent row persisted: %d %v",kind,err)}
 tables,err:=db.Query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name");if err!=nil{t.Fatal(err)};var names []string;for tables.Next(){var name string;if err=tables.Scan(&name);err!=nil{t.Fatal(err)};names=append(names,name)};if err=tables.Close();err!=nil{t.Fatal(err)}
 t.Logf("actual SQLite tables after native child allocation: %v",names)
 })}
}
`
