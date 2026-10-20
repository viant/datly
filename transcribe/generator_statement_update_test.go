package transcribe

import (
	"context"
	"encoding/json"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/typecatalog"

	loaderast "github.com/viant/x/loader/ast"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestGeneratorBufferedSQLXUpdateContract(t *testing.T) {
	root := os.Getenv("DATLY_UPDATE_FIXTURE")
	if root == "" {
		root = t.TempDir()
	} else if err := os.MkdirAll(root, 0755); err != nil {
		t.Fatal(err)
	}
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	if err := os.MkdirAll(filepath.Join(root, "model"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "model", "record.go"), []byte("package model\ntype Record struct { ID int `sqlx:\"ID,primaryKey\"`; Name *string `sqlx:\"NAME\"`; Enabled bool `sqlx:\"ENABLED\"`; Count int `sqlx:\"COUNT\"`; Has *RecordHas `sqlx:\"-\" setMarker:\"true\"` }; type RecordHas struct { ID, Name, Enabled, Count bool }\n"), 0644); err != nil {
		t.Fatal(err)
	}
	const source = `#package('pkg/records')
#import('model','github.com/viant/datly/genfixture/model')
#setting($_ = $route('/records/batch','POST'))
#define($_ = $Table<string>(param/table))
#define($_ = $Row<*model.Record>(param/row))
$dml.Update($Input.Table, $Input.Row)`
	catalog := typecatalog.NewCatalog()
	pkg, err := loaderast.LoadPackageFS(context.Background(), os.DirFS(root), "model")
	if err != nil {
		t.Fatal(err)
	}
	if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
		t.Fatal(err)
	}
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Records", Scope: "github.com/viant/datly/genfixture/pkg/records", Connector: "main", Text: source, Types: catalog}}
	generated, err := (Generator{Operation: "post"}).Generate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if generated.Result.Plan.ContractHandler == nil || generated.Result.Plan.MutationHandler != nil || generated.Result.Plan.VeltyHandler != nil || generated.Result.Plan.Settings.Mutation != "" {
		t.Fatal("Update acquired a competing writer")
	}
	p := filepath.Join(root, "pkg/records", "handler.go")
	before, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(before), "Update(input.Table, input.Row)") {
		t.Fatalf("native Update was not generated: %s", before)
	}
	if _, err := (Generator{Operation: "post"}).Generate(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(p)
	if err != nil || string(before) != string(after) {
		t.Fatal("Update generation is unstable")
	}
	runtime := updateRuntimeFixture
	compiled, err := NewCompiler().Compile(context.Background(), request.Source)
	if err != nil {
		t.Fatal(err)
	}
	componentJSON, err := json.Marshal(compiled.Component)
	if err != nil {
		t.Fatal(err)
	}
	runtime = strings.ReplaceAll(runtime, "RecordsInput", generated.Result.Plan.Input.Type)
	runtime = strings.ReplaceAll(runtime, "RecordsOutput", generated.Result.Plan.Output.Type)
	runtime = strings.ReplaceAll(runtime, "COMPONENT_JSON", strconv.Quote(string(componentJSON)))
	if err := os.WriteFile(filepath.Join(root, "pkg/records/update_runtime_test.go"), []byte(runtime), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-race", "-v", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("generated update owner regression: %v\n%s", err, output)
	} else {
		t.Logf("generated runtime: %s", output)
	}

}

const updateRuntimeFixture = `package records
import("context";"errors";"testing";
"github.com/viant/datly/sql/dml";"github.com/viant/datly/internal/testharness/sqlite";
xhandler "github.com/viant/xdatly/handler";
"github.com/viant/datly/genfixture/model")
type probe struct {table string; row any; calls int; err error}
func(p *probe)Update(table string,row any)error{p.table=table;p.row=row;p.calls++;return p.err}
func TestGeneratedIdentityAndErrors(t *testing.T){
 marker:=errors.New("original"); row:=&model.Record{Has:&model.RecordHas{Name:true}}
 p:=&probe{err:marker};in:=&RecordsInput{Table:"records",Row:row};out:=&RecordsOutput{}
 if err:=NewRecordsHandlerQueueStatement(p,in,out);err!=marker{t.Fatalf("error identity: %v",err)}
 if p.calls!=1||p.table!="records"||p.row!=row||p.row.(*model.Record).Has!=row.Has{t.Fatal("pointer/table/Has changed")}
 in.Row=nil;p.err=nil;if err:=NewRecordsHandlerQueueStatement(p,in,out);err!=nil{t.Fatal(err)}
 if p.calls!=2||p.row.(*model.Record)!=nil{t.Fatal("nil payload was restricted")}
 if NewRecordsHandlerQueueStatement(p,nil,out)==nil||NewRecordsHandlerQueueStatement(p,in,nil)==nil||NewRecordsHandlerQueueStatement(struct{}{},in,out)==nil{t.Fatal("missing guards")}
}
type parent struct {ID int ` + "`sqlx:\"ID,primaryKey\"`" + `}
type child struct {ID int ` + "`sqlx:\"ID,primaryKey\"`" + `;ParentID int ` + "`sqlx:\"PARENT_ID,required,refTable=records,refColumn=ID\"`" + `}
func TestGeneratedBufferedUpdate(t *testing.T){
 for _, mode:=range []string{"commit","caller rollback","late failure"}{t.Run(mode,func(t *testing.T){
 ctx:=context.Background();h:=sqlite.New(t)
 if err:=h.ExecStatements(ctx,"PRAGMA foreign_keys=ON","CREATE TABLE records(ID INTEGER PRIMARY KEY AUTOINCREMENT,NAME TEXT DEFAULT 'seed',ENABLED BOOLEAN DEFAULT 1,COUNT INTEGER DEFAULT 9)","CREATE TABLE children(ID INTEGER PRIMARY KEY,PARENT_ID INTEGER REFERENCES records(ID))","INSERT INTO records(ID,NAME) VALUES(100,'untouched')");err!=nil{t.Fatal(err)}
 var options []dml.Option
 if mode=="caller rollback"{tx,err:=h.DB.BeginTx(ctx,nil);if err!=nil{t.Fatal(err)};defer tx.Rollback();options=append(options,dml.WithTx(tx))}
 owner:=dml.NewData(h.DB,options...);if err:=owner.BeginInvocation();err!=nil{t.Fatal(err)};defer owner.Complete(ctx,errors.New("cleanup"));if err:=owner.Start(ctx);err!=nil{t.Fatal(err)}
 p:=&parent{};if err:=owner.Allocate(ctx,"records",[]*parent{p},"ID");err!=nil{t.Fatal(err)};if p.ID<=100{t.Fatalf("allocation=%d",p.ID)}
 first:=owner.ComponentData(dml.ComponentImperative,"").(*dml.Data);if err:=first.Insert("records",p);err!=nil{t.Fatal(err)};first.SealComponent()
 name:="queued";row:=&model.Record{ID:p.ID,Name:&name,Enabled:true,Count:88,Has:&model.RecordHas{Name:true}}
 middle:=owner.ComponentData(dml.ComponentImperative,"").(*dml.Data)
 if err:=NewRecordsHandlerQueueStatement(middle,&RecordsInput{Table:"records",Row:row},&RecordsOutput{});err!=nil{t.Fatal(err)}
 // Drain observes the original record and Has marker, including explicit NULL/zero/false.
 row.Name=nil;row.Enabled=false;row.Count=0;row.Has.Enabled=true;row.Has.Count=true
 middle.SealComponent();last:=owner.ComponentData(dml.ComponentImperative,"").(*dml.Data);c:=&child{ID:1,ParentID:p.ID}
 result,err:=last.FrameworkValidator().Validate(ctx,c,xhandler.ValidationOptions{Action:xhandler.WriteInsert,Shallow:true});if err!=nil||result.Failed{t.Fatalf("buffered FK: %+v %v",result,err)}
 _,tx:=owner.InvocationTransaction();var count int;if err:=tx.QueryRowContext(ctx,"SELECT COUNT(*) FROM records WHERE ID=?",p.ID).Scan(&count);err!=nil||count!=0{t.Fatalf("early flush count=%d err=%v",count,err)}
 if err:=last.Insert("children",c);err!=nil{t.Fatal(err)};last.SealComponent()
 if mode=="late failure"{if err:=owner.Execute("INSERT INTO children(ID,PARENT_ID) VALUES(2,-999)");err!=nil{t.Fatal(err)}}
 err=owner.Complete(ctx,nil)
 if mode=="late failure"{if err==nil{t.Fatal("late failure succeeded")};h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS count FROM records"},[]struct{Count int}{{1}});h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS count FROM children"},[]struct{Count int}{{0}});return}
 if err!=nil{t.Fatal(err)}
 if mode=="caller rollback" {if err:=tx.Rollback();err!=nil{t.Fatal(err)};h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS count FROM records"},[]struct{Count int}{{1}});return}
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME,ENABLED,COUNT FROM records WHERE ID=?",Args:[]any{p.ID}},[]struct{Name *string;Enabled bool;Count int}{{nil,false,0}})
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME FROM records WHERE ID=100"},[]struct{Name string}{{"untouched"}})
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT COUNT(*) AS count FROM children"},[]struct{Count int}{{1}})
 // Omitted fields stay unchanged; a queued pointer mutation is read at drain.
 d:=dml.NewData(h.DB);if err:=d.BeginInvocation();err!=nil{t.Fatal(err)}
 late:="early";sparse:=&model.Record{ID:p.ID,Name:&late,Count:77,Enabled:true,Has:&model.RecordHas{Name:true}}
 if err:=NewRecordsHandlerQueueStatement(d,&RecordsInput{Table:"records",Row:sparse},&RecordsOutput{});err!=nil{t.Fatal(err)};late="late"
 if err:=d.Complete(ctx,nil);err!=nil{t.Fatal(err)}
 h.AssertQuery(t,ctx,sqlite.Query{SQL:"SELECT NAME,ENABLED,COUNT FROM records WHERE ID=?",Args:[]any{p.ID}},[]struct{Name string;Enabled bool;Count int}{{"late",false,0}})
 })}
}
`

func TestCanonicalUpdateKeepsLegacyRouting(t *testing.T) {
	for _, tc := range []struct {
		name, call string
		native     bool
	}{
		{"native", `$dml.Update($Input.Table,$Input.Row)`, true},
		{"legacy", `$dml.Update("records",$Row)`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := `#package('pkg/records')
#setting($_ = $route('/records/update','POST'))
#define($_ = $Table<string>(param/table))
#define($_ = $Row<string>(param/row))
` + tc.call
			got, err := NewCompiler().Compile(context.Background(), &Source{Name: "Records", Scope: "example.com/pkg/records", Connector: "main", Text: source})
			if err != nil {
				t.Fatal(err)
			}
			if (got.StatementProgram != "") != tc.native {
				t.Fatalf("native=%v program=%s", tc.native, got.StatementProgram)
			}
		})
	}
}
