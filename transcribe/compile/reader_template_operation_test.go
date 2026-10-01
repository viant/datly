package compile_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestOperationGetPreservesConditionalLocksGeneratedRuntime(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)", "INSERT INTO records VALUES(1,'committed')", "CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER,name TEXT)", "INSERT INTO children VALUES(1,1,'committed')"); err != nil {
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	repo := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	root := t.TempDir()
	const module = "github.com/viant/datly/templatefixture"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	if err := os.MkdirAll(filepath.Join(root, "source"), 0755); err != nil {
		t.Fatal(err)
	}
	source := `#package('api/records')
#setting($_ = $route('/records','GET'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $LockRows<bool>(query/lock).Optional())
#define($_ = $ChildLock<bool>(query/childLock).Optional())
#define($_ = $Excluded<[]int>(query/excluded).Optional())
#define($_ = $OuterLock<bool>(query/outerLock).Optional())
#define($_ = $Data<[]*Row>(output/view))
SELECT rows.id AS Identifier,rows.name,children.id,children.parent_id,children.name,type(rows,'Row'),type(children,'Child')
FROM (
 SELECT r.id,r.name FROM records r WHERE 1=1
 #foreach($id in $Excluded)
 AND r.id<>$id
 #end
 ORDER BY r.id
 #if($LockRows) ${View.ForUpdate()} #end
) rows LEFT JOIN (
 SELECT c.id,c.parent_id,c.name FROM children c ORDER BY c.id
 #if($ChildLock) ${View.ForUpdate()} #end
) children ON children.parent_id=rows.id
ORDER BY rows.name DESC,rows.id
#if($OuterLock) ${View.ForUpdate()} #end`
	if err := os.WriteFile(filepath.Join(root, "source", "reader.dql"), []byte(source), 0644); err != nil {
		t.Fatal(err)
	}
	command := testharness.SourceGoCommand(t, repo, "run", "./cmd/datly", "transcribe", "get", "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), module+"/source")
	command.Dir = repo
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("operation get: %v\n%s", err, output)
	}
	sql, err := os.ReadFile(filepath.Join(root, "api", "records", "sql", "reader.sql"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sql), `#if($LockRows) ${View.ForUpdate()} #end`) {
		t.Fatalf("generated source dropped runtime lock: %s", sql)
	}
	if err := os.WriteFile(filepath.Join(root, "api", "records", "locking_runtime_test.go"), []byte(generatedLockRuntime), 0644); err != nil {
		t.Fatal(err)
	}
	command = exec.CommandContext(ctx, "go", "test", "-mod=mod", "./api/records", "-count=1")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("generated native runtime: %v\n%s", err, output)
	}
}

const generatedLockRuntime = `package records
import("context";"reflect";"strings";"testing"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/internal/testharness/sqlite"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/reader"
 "github.com/viant/datly/sql/builder"
 sqltemplate "github.com/viant/datly/sql/template"
 dtag "github.com/viant/datly/tag"
 "github.com/viant/sqlx/metadata/database"
 "github.com/viant/sqlx/metadata/info"
 xhandler "github.com/viant/xdatly/handler"
)
type binder struct{}
func(binder)Bind(context.Context,any)error{return nil}
func(binder)Lookup(context.Context,xhandler.ValueKey)(any,bool,error){return nil,false,nil}
func TestGeneratedConditionalLockContract(t *testing.T){
 ctx:=context.Background();db:=sqlite.New(t);if err:=db.ExecStatements(ctx,"CREATE TABLE records(id INTEGER PRIMARY KEY,name TEXT)","INSERT INTO records VALUES(1,'committed')","CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER,name TEXT)","INSERT INTO children VALUES(1,1,'committed')");err!=nil{t.Fatal(err)}
 holder:=reflect.TypeFor[ReaderComponent]();field,_:=holder.FieldByName("Contract");tag,_,err:=dtag.ParseComponent(field.Tag);if err!=nil{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:"ReaderComponent",FieldName:field.Name,PackageName:"records",PackagePath:holder.PkgPath(),Tag:tag,InputType:"Input",OutputType:"Output"}
 component,err:=source.Resolve(reflect.TypeFor[Input](),reflect.TypeFor[Output]());if err!=nil{t.Fatal(err)}
 resources:=resource.New();if err=resources.Register(ReaderDatlyResourceNamespace,ReaderDatlyResources);err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Resources:resources});if err!=nil{t.Fatal(err)}
 newExecution:=func(sql *dsql.SQLComponent)*reader.Execution{execution,err:=reader.NewExecution(reader.Config{Component:artifact.Component,InputType:reflect.TypeFor[Input](),OutputType:reflect.TypeFor[Output](),Plan:artifact.Reader,SQL:sql});if err!=nil{t.Fatal(err)};return execution}
 ordinary:=&Input{};ordinary.SetLockRows(false);rows,err:=newExecution(&dsql.SQLComponent{DB:db.DB}).Read(ctx,ordinary,binder{},nil);if err!=nil{t.Fatal(err)};if len(rows.(*Output).Data)!=1{t.Fatal("nonlocking public read changed")}
 filtered:=&Input{};filtered.SetExcluded([]int{1});filteredRows,err:=newExecution(&dsql.SQLComponent{DB:db.DB}).Read(ctx,filtered,binder{},nil);if err!=nil{t.Fatal(err)};if len(filteredRows.(*Output).Data)!=0{t.Fatal("generated loop did not retain its bindings/semantics")}
 locked:=&Input{};locked.SetLockRows(true);if _,err=newExecution(&dsql.SQLComponent{DB:db.DB}).Read(ctx,locked,binder{},nil);err==nil||!strings.Contains(err.Error(),"active transaction"){t.Fatalf("lock without transaction: %v",err)}
 childLocked:=&Input{};childLocked.SetChildLock(true);if _,err=newExecution(&dsql.SQLComponent{DB:db.DB}).Read(ctx,childLocked,binder{},nil);err==nil||!strings.Contains(err.Error(),"active transaction"){t.Fatalf("child lock without transaction: %v",err)}
 tx,err:=db.DB.BeginTx(ctx,nil);if err!=nil{t.Fatal(err)};defer tx.Rollback();if _,err=tx.ExecContext(ctx,"INSERT INTO records VALUES(2,'uncommitted')");err!=nil{t.Fatal(err)}
 rows,err=newExecution(&dsql.SQLComponent{DB:db.DB,Tx:tx}).Read(ctx,locked,binder{},nil);if err!=nil{t.Fatal(err)};if len(rows.(*Output).Data)!=2{t.Fatal("reader did not share caller transaction")};if _,err=tx.ExecContext(ctx,"INSERT INTO children VALUES(2,1,'uncommitted')");err!=nil{t.Fatal(err)}
 rows,err=newExecution(&dsql.SQLComponent{DB:db.DB,Tx:tx}).Read(ctx,childLocked,binder{},nil);if err!=nil{t.Fatal(err)};matchedChildren:=false;for _,row:=range rows.(*Output).Data{children:=reflect.ValueOf(row).Elem().FieldByName("Children");if children.IsValid()&&children.Len()==2{matchedChildren=true}};if !matchedChildren{t.Fatalf("child reader did not share caller tx: %#v",rows)}
 if err=tx.Rollback();err!=nil{t.Fatal(err)}
 program,err:=(sqltemplate.Compiler{Source:artifact.Reader.Root.View.Spec.Source.SQL,InputType:reflect.TypeFor[Input]()}).Compile();if err!=nil{t.Fatal(err)}

 outerLocked:=&Input{};outerLocked.SetOuterLock(true)
 for _,input:=range []*Input{locked,outerLocked}{for _,active:=range []bool{false,true}{query,err:=builder.NewBuilder().Build(ctx,builder.WithBuilderTemplate(program),builder.WithBuilderInput(reflect.ValueOf(input)),builder.WithBuilderDialect(&info.Dialect{Product:database.Product{Name:"mysql"},Placeholder:"?"}),builder.WithBuilderTransactionActive(active));if !active{if err==nil{t.Fatal("MySQL lock without transaction succeeded")};continue};if err!=nil{t.Fatal(err)}
 normalized:=strings.Join(strings.Fields(query.SQL)," ")
 if input==outerLocked{want:="SELECT rows.id, rows.name FROM ( SELECT r.id,r.name FROM records r WHERE 1=1 ORDER BY r.id ) rows ORDER BY rows.name DESC, rows.id FOR UPDATE";explicitDirection:=strings.Replace(want,"rows.id FOR UPDATE","rows.id ASC FOR UPDATE",1);if normalized!=want&&normalized!=explicitDirection{t.Fatalf("wrong complete MySQL SQL order: got %s want %s",normalized,want)}}else if !strings.Contains(normalized,"FOR UPDATE"){t.Fatalf("missing MySQL lock clause: %s",normalized)}
 }}

}
`
