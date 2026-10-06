package transcribe

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	_ "github.com/viant/sqlx/metadata/product/mysql"
	loaderast "github.com/viant/x/loader/ast"
)

func TestWriterActionPolicyNativeGenerationAndEvolution(t *testing.T) {
	runWriterActionPolicyNative(t, false, false, false)
}

func TestWriterActionPolicyNativeGenerationMySQL(t *testing.T) {
	if os.Getenv("DATLY_PLAN47_MYSQL_DSN") == "" {
		t.Skip("requires an exclusively owned disposable Plan47 MySQL schema")
	}
	runWriterActionPolicyNative(t, true, false, false)
}

func TestWriterActionPolicyNativeLinkedLifecycleMySQL(t *testing.T) {
	if os.Getenv("DATLY_PLAN47_MYSQL_DSN") == "" {
		t.Skip("requires an exclusively owned disposable Plan47 MySQL schema")
	}
	runWriterActionPolicyNative(t, true, true, true)
}

func TestWriterActionPolicyNativeLifecycleSQLite(t *testing.T) {
	runWriterActionPolicyNative(t, false, true, false)
}

func TestWriterActionPolicyNativeLinkedLifecycleSQLite(t *testing.T) {
	runWriterActionPolicyNative(t, false, true, true)
}

func TestWriterActionPolicyNativeSamePackageAliasLinkedLifecycleSQLite(t *testing.T) {
	runWriterActionPolicyNative(t, false, true, true, "same-package")
}

func TestWriterActionPolicyNativeNestedLinkedLifecycleSQLite(t *testing.T) {
	runWriterActionPolicyNative(t, false, true, true, "nested")
}

func runWriterActionPolicyNative(t *testing.T, mysqlDialect, hooks, linked bool, layouts ...string) {
	ctx := context.Background()
	db := actionPolicyNativeDB{DB: testharness.NewSQLiteHarness(t).DB}
	if mysqlDialect {
		cfg, err := mysql.ParseDSN(os.Getenv("DATLY_PLAN47_MYSQL_DSN"))
		if err != nil {
			t.Fatal("invalid owned MySQL configuration")
		}
		if !strings.HasPrefix(cfg.DBName, "plan47_root_") {
			t.Fatal("requires owned Plan47 schema")
		}
		cfg.MultiStatements = true
		cfg.Params = map[string]string{"foreign_key_checks": "1"}
		db.DB, err = sql.Open("mysql", cfg.FormatDSN())
		if err != nil {
			t.Fatal("owned MySQL open failed")
		}
		t.Cleanup(func() { db.DB.Close() })
		db.DB.SetMaxOpenConns(4)
	}
	schema := []string{"CREATE TABLE owners(name TEXT PRIMARY KEY)", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT NOT NULL,owner TEXT REFERENCES owners(name))"}
	if mysqlDialect {
		schema = []string{"CREATE TABLE owners(name VARCHAR(32) PRIMARY KEY) ENGINE=InnoDB", "CREATE TABLE records(id BIGINT PRIMARY KEY AUTO_INCREMENT,title VARCHAR(255) NOT NULL,owner VARCHAR(32),FOREIGN KEY(owner) REFERENCES owners(name)) ENGINE=InnoDB"}
	}
	if err := db.ExecStatements(ctx, schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "example.com/actionpolicy"}).Write(t, root)
	text := `#package('example.com/actionpolicy/generated')

#setting($_ = $connector('main'))
#setting($_ = $route('/records','PATCH'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $case_format('lc'))

#define($_ = $Data<[]*Record>(output/body))

SELECT r.*, type(r,'Record'), writer_action_policy(r,'insert-delete'),
       CAST(r.should_delete AS bool), delete_marker(r.should_delete)
FROM (SELECT records.*,0 AS should_delete FROM records) r
`
	generatedDir := "generated"
	generatedPackage := "example.com/actionpolicy/generated"
	if len(layouts) > 0 && layouts[0] == "nested" {
		generatedDir = "nested/generated"
		generatedPackage = "example.com/actionpolicy/nested/generated"
		text = strings.Replace(text, "example.com/actionpolicy/generated", generatedPackage, 1)
	}
	if hooks {
		text = strings.Replace(text, "type(r,'Record'),", "type(r,'Record'), lifecycle_type(r,'Lifecycle'),", 1)
	}
	var catalog *typecatalog.Catalog
	if len(layouts) > 0 && layouts[0] == "same-package" {
		writeSourceFile(t, root, filepath.Join(generatedDir, "lifecycle.go"), actionPolicyNativeHooks)
		pkg, err := loaderast.LoadPackageFS(ctx, os.DirFS(root), generatedDir)
		if err != nil {
			t.Fatal(err)
		}
		catalog = typecatalog.NewCatalog()
		if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
			t.Fatal(err)
		}
		text = strings.Replace(text, "#package('"+generatedPackage+"')", "#package('"+generatedPackage+"')\n#import('same','"+generatedPackage+"')", 1)
		text = strings.Replace(text, "lifecycle_type(r,'Lifecycle')", "lifecycle_type(r,'same.Lifecycle')", 1)
	}
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Records", Scope: "example.com/actionpolicy/source", Text: text, Types: catalog, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	snapshot := func() map[string][32]byte {
		result := map[string][32]byte{}
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			relative, _ := filepath.Rel(root, path)
			result[relative] = sha256.Sum256(content)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	if hooks {
		writeSourceFile(t, root, filepath.Join(generatedDir, "lifecycle.go"), actionPolicyNativeHooks)
	}
	first := snapshot()
	if len(first) == 0 {
		t.Fatal("no Go artifacts generated")
	}
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	second := snapshot()
	if len(first) != len(second) {
		t.Fatal("regeneration changed artifact count")
	}
	for name, digest := range first {
		if second[name] != digest {
			t.Fatalf("unstable artifact %s", name)
		}
	}
	policyFound := false
	for name := range first {
		content, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(content), "writerActionPolicy=insert-delete") {
			policyFound = true
		}
	}
	if !policyFound {
		t.Fatal("generated linked metadata lost explicit policy")
	}
	if err := db.ExecStatements(ctx, "ALTER TABLE records ADD COLUMN notes TEXT"); err != nil {
		t.Fatal(err)
	}
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}

	evolved := snapshot()
	changed := false
	notesFound := false
	for name, digest := range evolved {
		if first[name] != digest {
			changed = true
		}
		content, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(content), "Notes") {
			notesFound = true
		}
	}
	if !changed || !notesFound {
		t.Fatal("wildcard SQL did not regenerate newly added field")
	}
	runtimeSource := mutationPredicateRuntime
	start := strings.Index(runtimeSource, " for _,test:=range []useCase{")
	end := strings.Index(runtimeSource[start:], " }{\n") + start
	cases := ` for _,test:=range []useCase{
 {desc:"omitted identity uses native allocation",input:input{body:"{\"Data\":[{\"title\":\"allocated\"}]}"},expect:expect{title:"keep",remaining:2}},
 {desc:"explicit zero identity uses native allocation",input:input{body:"{\"Data\":[{\"id\":0,\"title\":\"allocated\"}]}"},expect:expect{title:"keep",remaining:2}},
 {desc:"same physical identity replacement",input:input{body:"{\"Data\":[{\"id\":1,\"shouldDelete\":true},{\"id\":1,\"title\":\"changed\"}]}"},expect:expect{title:"changed",remaining:1}},
 {desc:"reversed input still deletes before inserting",input:input{body:"{\"Data\":[{\"id\":1,\"title\":\"changed\"},{\"id\":1,\"shouldDelete\":true}]}"},expect:expect{title:"changed",remaining:1}},
 {desc:"caller commit preserves ownership",input:input{body:"{\"Data\":[{\"id\":1,\"shouldDelete\":true},{\"id\":1,\"title\":\"changed\"}]}"},expect:expect{title:"changed",remaining:1}},
 {desc:"caller rollback preserves ownership",input:input{body:"{\"Data\":[{\"id\":1,\"shouldDelete\":true},{\"id\":1,\"title\":\"changed\"}]}"},expect:expect{title:"keep",remaining:1}},
 {desc:"cancelled request performs no mutation",input:input{body:"{\"Data\":[{\"id\":1,\"shouldDelete\":true},{\"id\":1,\"title\":\"changed\"}]}"},expect:expect{bindingFailure:true,title:"keep",remaining:1}},
 {desc:"missing delete cannot insert replacement",input:input{body:"{\"Data\":[{\"id\":2,\"shouldDelete\":true},{\"id\":2,\"title\":\"changed\"}]}"},expect:expect{identityFailure:true,title:"keep",remaining:1}},
 {desc:"FK failure preserves deleted row and events",input:input{body:"{\"Data\":[{\"id\":1,\"shouldDelete\":true},{\"id\":1,\"title\":\"changed\",\"owner\":\"missing\"}]}"},expect:expect{bindingFailure:true,title:"keep",remaining:1}},
 {desc:"late insert trigger failure rolls back deletion",input:input{body:"{\"Data\":[{\"id\":1,\"shouldDelete\":true},{\"id\":1,\"title\":\"late failure\"}]}"},expect:expect{bindingFailure:true,title:"keep",remaining:1}},
 {desc:"unpaired insert reaches real uniqueness constraint",input:input{body:"{\"Data\":[{\"id\":1,\"title\":\"changed\"}]}"},expect:expect{bindingFailure:true,title:"keep",remaining:1}},
`
	runtimeSource = runtimeSource[:start] + cases + runtimeSource[end:]
	runtimeSource = strings.Replace(runtimeSource, `CREATE TABLE records(id TEXT PRIMARY KEY,title TEXT,owner TEXT,attempt INTEGER);INSERT INTO records VALUES('one','keep','old',0)`, `PRAGMA foreign_keys=ON;CREATE TABLE owners(name TEXT PRIMARY KEY);INSERT INTO owners VALUES('old');CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT NOT NULL,owner TEXT REFERENCES owners(name),notes TEXT);INSERT INTO records VALUES(1,'keep','old',NULL);CREATE TABLE audit(action TEXT);CREATE TRIGGER record_deleted AFTER DELETE ON records BEGIN INSERT INTO audit VALUES('delete');END;CREATE TRIGGER record_inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES('insert');END;CREATE TRIGGER reject_late BEFORE INSERT ON records WHEN NEW.title='late failure' BEGIN SELECT RAISE(ABORT,'late insert failure');END`, 1)
	runtimeSource = strings.Replace(runtimeSource, `if component.RootView.MutationPredicateGroup==nil{t.Fatal("generated mutation predicate metadata was lost in bootstrap")}`, `if component.RootView.WriterActionPolicy!="insert-delete"{t.Fatal("generated action policy lost in bootstrap")}`, 1)

	runtimeSource = strings.Replace(runtimeSource, `if !test.expect.conflict && !test.expect.bindingFailure`, `var auditCount int;if e:=db.QueryRow("SELECT COUNT(*) FROM audit").Scan(&auditCount);e!=nil{t.Fatal(e)};want:=2;if test.expect.bindingFailure{want=0;if !strings.Contains(err.Error(),"UNIQUE constraint"){t.Fatalf("wrong failure stage: %v",executionErr)}};if auditCount!=want{t.Fatalf("audit count=%d want=%d",auditCount,want)};if want==2{rows,e:=db.Query("SELECT action FROM audit ORDER BY rowid");if e!=nil{t.Fatal(e)};defer rows.Close();for _,action:=range []string{"delete","insert"}{var actual string;if !rows.Next(){t.Fatal("missing event")};if e:=rows.Scan(&actual);e!=nil||actual!=action{t.Fatalf("action=%q want=%q err=%v",actual,action,e)}};if rows.Next(){t.Fatal("extra event")}}
   if !test.expect.conflict && !test.expect.bindingFailure`, 1)
	runtimeSource = strings.Replace(runtimeSource, `output,err:=rt.ExecuteRoute(ctx,"PATCH","/records",scope)`, `output,err:=rt.ExecuteRoute(ctx,"PATCH","/records",scope);executionErr:=err`, 1)
	runtimeSource = strings.Replace(runtimeSource, `!strings.Contains(err.Error(),"UNIQUE constraint")`, `executionErr==nil||(strings.Contains(test.desc,"FK")&&!strings.Contains(executionErr.Error(),"field Owner failed refKey validation"))||(strings.Contains(test.desc,"late insert")&&!strings.Contains(executionErr.Error(),"late insert failure"))||(strings.HasPrefix(test.desc,"cancelled")&&!errors.Is(executionErr,context.Canceled))||(!strings.HasPrefix(test.desc,"cancelled")&&!strings.Contains(test.desc,"FK")&&!strings.Contains(test.desc,"late insert")&&!strings.Contains(executionErr.Error(),"UNIQUE constraint"))`, 1)
	runtimeSource = strings.Replace(runtimeSource, `"github.com/viant/datly/runtime/registry"`, `"github.com/viant/datly/runtime/registry";"github.com/viant/datly/runtime/handler/engine";"github.com/viant/datly/spec"`, 1)
	runtimeSource = strings.Replace(runtimeSource, `views,err:=viewprovider.New`, `var callerTx *sql.Tx;caller:=strings.HasPrefix(test.desc,"caller");if caller{callerTx,err=db.BeginTx(ctx,nil);if err!=nil{t.Fatal(err)};defer callerTx.Rollback()};views,err:=viewprovider.New`, 1)
	runtimeSource = strings.Replace(runtimeSource, `SQL:&dsql.SQLComponent{DB:db}`, `SQL:&dsql.SQLComponent{DB:db,Tx:callerTx}`, 1)
	runtimeSource = strings.Replace(runtimeSource, `output,err:=rt.ExecuteRoute(ctx,"PATCH","/records",scope);executionErr:=err`, `var output any;var completion xhandler.Outcome;commitCalls:=0;if caller{input,ok:=artifact.Input.ForRoute(spec.RouteRef{Method:"PATCH",Path:"/records"});if !ok{t.Fatal("caller route missing")};output,err=engine.New().Execute(ctx,engine.Request{Input:input,Handler:handler,Scope:scope,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db,Tx:callerTx,OnCommit:func(context.Context){commitCalls++}},Completion:func(outcome xhandler.Outcome){completion=outcome}})}else{output,err=rt.ExecuteRoute(ctx,"PATCH","/records",scope)};executionErr:=err`, 1)
	runtimeSource = strings.Replace(runtimeSource, `var count int;`, `if caller{if err!=nil||completion.State()!=xhandler.TransactionCallerPending||completion.CommitConfirmed()||commitCalls!=0{t.Fatalf("caller ownership lost outcome=%+v commits=%d err=%v",completion,commitCalls,err)};var pendingTitle string;var pendingAudit int;if e:=callerTx.QueryRow("SELECT title FROM records WHERE id=1").Scan(&pendingTitle);e!=nil||pendingTitle!="changed"{t.Fatalf("pending replacement=%q err=%v",pendingTitle,e)};if e:=callerTx.QueryRow("SELECT COUNT(*) FROM audit").Scan(&pendingAudit);e!=nil||pendingAudit!=2{t.Fatalf("pending audits=%d err=%v",pendingAudit,e)};if strings.Contains(test.desc,"rollback"){err=callerTx.Rollback()}else{err=callerTx.Commit()};if err!=nil{t.Fatalf("caller transaction completed early: %v",err)}}
   var count int;`, 1)
	runtimeSource = strings.Replace(runtimeSource, `want:=2;if test.expect.bindingFailure`, `want:=2;if strings.Contains(test.desc,"caller rollback"){want=0};if test.expect.bindingFailure`, 1)
	runtimeSource = strings.Replace(runtimeSource, `var output any;var completion`, `if strings.HasPrefix(test.desc,"cancelled"){cancelled,cancel:=context.WithCancel(ctx);cancel();ctx=cancelled};var output any;var completion`, 1)
	runtimeSource = strings.Replace(runtimeSource, `want:=2;if strings.Contains(test.desc,"caller rollback")`, `want:=2;if test.expect.identityFailure{want=0};if strings.Contains(test.desc,"caller rollback")`, 1)

	runtimeSource = strings.Replace(runtimeSource, `want:=2;if test.expect.identityFailure`, `want:=2;if strings.Contains(test.desc,"native allocation"){want=1};if test.expect.identityFailure`, 1)
	runtimeSource = strings.Replace(runtimeSource, `SELECT title FROM records").Scan(&title)`, `SELECT title FROM records WHERE id=1").Scan(&title)`, 1)
	runtimeSource = strings.Replace(runtimeSource, `if !test.expect.conflict && !test.expect.bindingFailure`, `if strings.Contains(test.desc,"native allocation"){var allocated int64;var action string;if e:=db.QueryRow("SELECT id FROM records WHERE title='allocated'").Scan(&allocated);e!=nil||allocated<=1{t.Fatalf("native allocation id=%d err=%v",allocated,e)};if e:=db.QueryRow("SELECT action FROM audit").Scan(&action);e!=nil||action!="insert"{t.Fatalf("allocation audit=%q err=%v",action,e)};wire,e:=json.Marshal(output);if e!=nil{t.Fatal(e)};var response map[string]any;if e=json.Unmarshal(wire,&response);e!=nil{t.Fatal(e)};rows,ok:=response["Data"].([]any);if !ok||len(rows)!=1{t.Fatalf("allocation output contract=%s",wire)};row,ok:=rows[0].(map[string]any);if !ok||row["id"]!=float64(allocated)||row["title"]!="allocated"{t.Fatalf("allocated SQL/output mismatch id=%d response=%s",allocated,wire)}}
 if !test.expect.conflict && !test.expect.bindingFailure`, 1)
	if mysqlDialect {
		runtimeSource = strings.Replace(runtimeSource, `_ "github.com/mattn/go-sqlite3"`, `"os";"github.com/go-sql-driver/mysql";_ "github.com/viant/sqlx/metadata/product/mysql"`, 1)
		runtimeSource = strings.Replace(runtimeSource, `db,err:=sql.Open("sqlite3",":memory:")`, `cfg,e:=mysql.ParseDSN(os.Getenv("DATLY_PLAN47_MYSQL_DSN"));if e!=nil||!strings.HasPrefix(cfg.DBName,"plan47_root_"){t.Fatal("owned schema required")};cfg.MultiStatements=true;cfg.Params=map[string]string{"foreign_key_checks":"1"};db,err:=sql.Open("mysql",cfg.FormatDSN())`, 1)
		runtimeSource = strings.Replace(runtimeSource, `db.SetMaxOpenConns(1)`, `db.SetMaxOpenConns(4)`, 1)
		runtimeSource = strings.Replace(runtimeSource, `PRAGMA foreign_keys=ON;CREATE TABLE owners(name TEXT PRIMARY KEY);INSERT INTO owners VALUES('old');CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT NOT NULL,owner TEXT REFERENCES owners(name),notes TEXT);INSERT INTO records VALUES(1,'keep','old',NULL);CREATE TABLE audit(action TEXT);CREATE TRIGGER record_deleted AFTER DELETE ON records BEGIN INSERT INTO audit VALUES('delete');END;CREATE TRIGGER record_inserted AFTER INSERT ON records BEGIN INSERT INTO audit VALUES('insert');END;CREATE TRIGGER reject_late BEFORE INSERT ON records WHEN NEW.title='late failure' BEGIN SELECT RAISE(ABORT,'late insert failure');END`, `DROP TABLE IF EXISTS audit;DROP TABLE IF EXISTS records;DROP TABLE IF EXISTS owners;CREATE TABLE owners(name VARCHAR(32) PRIMARY KEY) ENGINE=InnoDB;INSERT INTO owners VALUES('old');CREATE TABLE records(id BIGINT PRIMARY KEY AUTO_INCREMENT,title VARCHAR(255) NOT NULL,owner VARCHAR(32),notes TEXT,FOREIGN KEY(owner) REFERENCES owners(name)) ENGINE=InnoDB;INSERT INTO records VALUES(1,'keep','old',NULL);CREATE TABLE audit(id BIGINT PRIMARY KEY AUTO_INCREMENT,action VARCHAR(32)) ENGINE=InnoDB;CREATE TRIGGER record_deleted AFTER DELETE ON records FOR EACH ROW INSERT INTO audit(action) VALUES('delete');CREATE TRIGGER record_inserted AFTER INSERT ON records FOR EACH ROW INSERT INTO audit(action) VALUES('insert');CREATE TRIGGER reject_late BEFORE INSERT ON records FOR EACH ROW BEGIN IF NEW.title='late failure' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='late insert failure';END IF;END`, 1)
		runtimeSource = strings.Replace(runtimeSource, `ORDER BY rowid`, `ORDER BY id`, 1)
		runtimeSource = strings.Replace(runtimeSource, `"UNIQUE constraint"`, `"Duplicate entry"`, 1)
	}

	if hooks {
		runtimeSource = strings.Replace(runtimeSource, `ctx:=context.Background();`, `trace:=&actionPolicyNativeHookTrace{};ctx:=context.WithValue(context.Background(),actionPolicyNativeHookKey{},trace);`, 1)
		runtimeSource = strings.Replace(runtimeSource, `rt,err:=druntime.NewRuntime`, `if handler.NewPhaseObserver()==nil{t.Fatal("declared native lifecycle not linked")};rt,err:=druntime.NewRuntime`, 1)
		runtimeSource = strings.Replace(runtimeSource, `if !test.expect.conflict && !test.expect.bindingFailure`, `if executionErr==nil{expectedRows:=2;if strings.Contains(test.desc,"native allocation"){expectedRows=1};if trace.Init!=expectedRows||trace.Sequence!=expectedRows||trace.Queue!=expectedRows||trace.Validate==0||trace.Phases==0{t.Fatalf("native hook callbacks=%+v expectedRows=%d",trace,expectedRows)};if !reflect.DeepEqual(trace.InitOriginal,trace.SequenceOriginal){t.Fatalf("Original changed across allocation/init=%v sequence=%v",trace.InitOriginal,trace.SequenceOriginal)};if strings.Contains(test.desc,"native allocation"){expectedHasID:=strings.HasPrefix(test.desc,"explicit zero");if len(trace.OriginalHasID)!=1||trace.OriginalHasID[0]!=expectedHasID{t.Fatalf("original suppliedID marker=%v expected=%v",trace.OriginalHasID,expectedHasID)}}}
 if !test.expect.conflict && !test.expect.bindingFailure`, 1)
	}

	if linked {
		runtimeSource = strings.Replace(runtimeSource, `component,err:=source.Resolve`, `reflected,e:=bootstrap.ReflectSelectedPackages([]string{"`+generatedPackage+`"},nil);if e!=nil||len(reflected.Components)!=1{t.Fatalf("native linked package discovery routes=%+v err=%v",reflected,e)};source=reflected.Components[0];component,err:=source.Resolve`, 1)
	}
	writeSourceFile(t, root, filepath.Join(generatedDir, "action_policy_runtime_test.go"), runtimeSource)
	command := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := command.CombinedOutput(); err != nil {
		if hooks {
			for name := range snapshot() {
				content, _ := os.ReadFile(filepath.Join(root, name))
				if strings.Contains(string(content), "Lifecycle") {
					t.Logf("native file %s\n%s", name, content)
				}
			}
		}
		t.Fatalf("native generated evolved package: %v\n%s", err, output)
	}
	if request.Source.Text != text {
		t.Fatal("generation changed authored DQL")
	}
}

// Every table here belongs to the separately leased generic test catalog.
type actionPolicyNativeDB struct{ DB *sql.DB }

func (db actionPolicyNativeDB) ExecStatements(ctx context.Context, statements ...string) error {
	for _, statement := range statements {
		if _, err := db.DB.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

const actionPolicyNativeHooks = `package generated
import("context";"encoding/json";h "github.com/viant/xdatly/handler")
type Lifecycle struct{}
type actionPolicyNativeHookKey struct{}
type actionPolicyNativeHookTrace struct{Init,Validate,Sequence,Queue,Phases int;InitOriginal,SequenceOriginal []string;OriginalHasID []bool}
func actionPolicyNativeProof(original h.OriginalPresence)(string,bool){available:=original!=nil&&original.Available();fields:=map[string]bool{};for _,name:=range []string{"Id","Title","Owner","Notes","ShouldDelete"}{fields[name]=original!=nil&&original.Has(name)};data,err:=json.Marshal(struct{Available bool;Fields map[string]bool}{available,fields});if err!=nil{panic(err)};return string(data),fields["Id"]}
func (*Lifecycle)Init(ctx context.Context,_ *Record,state h.LifecycleContext[Record,h.NoParent,Output])error{trace:=ctx.Value(actionPolicyNativeHookKey{}).(*actionPolicyNativeHookTrace);trace.Init++;proof,present:=actionPolicyNativeProof(state.Original);trace.InitOriginal=append(trace.InitOriginal,proof);trace.OriginalHasID=append(trace.OriginalHasID,present);return nil}
func (*Lifecycle)Validate(ctx context.Context,_ *Record,_ h.LifecycleContext[Record,h.NoParent,Output])error{ctx.Value(actionPolicyNativeHookKey{}).(*actionPolicyNativeHookTrace).Validate++;return nil}
func (*Lifecycle)AfterSequence(ctx context.Context,_ *Record,state h.LifecycleContext[Record,h.NoParent,Output])error{trace:=ctx.Value(actionPolicyNativeHookKey{}).(*actionPolicyNativeHookTrace);trace.Sequence++;proof,_:=actionPolicyNativeProof(state.Original);trace.SequenceOriginal=append(trace.SequenceOriginal,proof);return nil}
func (*Lifecycle)AfterQueue(ctx context.Context,_ *Record,_ h.LifecycleContext[Record,h.NoParent,Output])error{ctx.Value(actionPolicyNativeHookKey{}).(*actionPolicyNativeHookTrace).Queue++;return nil}
func (*Lifecycle)ObservePhase(ctx context.Context,event h.PhaseEvent){if trace,ok:=ctx.Value(actionPolicyNativeHookKey{}).(*actionPolicyNativeHookTrace);ok&&event.Boundary==h.PhaseBegin{trace.Phases++}}
`

func TestWriterActionPolicyNativeGeneratorRejectsGraphScopedRecovery(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE messages(id TEXT PRIMARY KEY,turn_id TEXT,sequence INTEGER,title TEXT)"); err != nil {
		t.Fatal(err)
	}
	source := &Source{Name: "messages", Scope: "github.com/viant/datly/scopedfixture/source", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: scopedSequenceDQL}
	compiled, err := NewCompiler().Compile(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	for _, position := range []string{"ancestor", "sibling"} {
		t.Run(position, func(t *testing.T) {
			candidate, err := compiled.projectClone()
			if err != nil {
				t.Fatal(err)
			}
			leaf := &spec.View{Name: "Associations", WriterActionPolicy: "insert-delete", Source: &spec.ViewSource{Table: "items"}}
			if position == "ancestor" {
				candidate.Component.RootView.Relations = append(candidate.Component.RootView.Relations, &spec.Relation{Name: "Associations", Holder: "Associations", View: leaf})
			} else {
				scoped := candidate.Component.RootView
				candidate.Component.RootView = &spec.View{Name: "Root", Auxiliary: true, Relations: []*spec.Relation{{Name: "Scoped", Holder: "Scoped", View: scoped}, {Name: "Associations", Holder: "Associations", View: leaf}}}
			}
			_, err = (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Compiled: candidate, Destination: t.TempDir()})
			if err == nil || !strings.Contains(err.Error(), "graph-wide scoped recovery") {
				t.Fatalf("%s recovery admitted or failed at wrong gate: %v", position, err)
			}
		})
	}
}
