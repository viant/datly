package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestBorrowedSQLRowStockCLI(t *testing.T) {
	for _, encoding := range []string{"inline", "embed"} {
		t.Run(encoding, func(t *testing.T) { borrowedSQLRowStockCLI(t, encoding) })
	}
}

func borrowedSQLRowStockCLI(t *testing.T, encoding string) {
	borrowedSQLRowStockCLIAt(t, encoding, t.TempDir())
}

func TestBorrowedSQLRowStockCLIRootAlias(t *testing.T) {
	for _, encoding := range []string{"inline", "embed"} {
		t.Run(encoding, func(t *testing.T) {
			parent := t.TempDir()
			physical := filepath.Join(parent, "physical")
			alias := filepath.Join(parent, "alias")
			if err := os.Mkdir(physical, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(physical, alias); err != nil {
				t.Fatal(err)
			}
			borrowedSQLRowStockCLIAt(t, encoding, alias)
		})
	}
}

func borrowedSQLRowStockCLIAt(t *testing.T, encoding, root string) {
	ctx := context.Background()
	const module = "github.com/viant/datly/borrowedfixture"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	dsn := filepath.Join(root, "exclusive-test.db")
	db := sqlite.New(t, sqlite.WithDSN(dsn))
	t.Logf("EXCLUSIVE_SQLITE owner=%s path=%s", t.Name(), dsn)
	if err := db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER REFERENCES parents(id),name TEXT)", "CREATE UNIQUE INDEX pair_unique ON records(parent_id,name)"); err != nil {
		t.Fatal(err)
	}
	sourceDir := filepath.Join(root, "source")
	ownerDir := filepath.Join(sourceDir, "private")
	if err := os.MkdirAll(ownerDir, 0755); err != nil {
		t.Fatal(err)
	}
	owner := `#package('` + module + `/api')
#setting($_ = $route('/private','PATCH'))
#setting($_ = $file_prefix('private_'))
#setting($_ = $input_type('PrivateInput'))
#setting($_ = $output_type('PrivateOutput'))
#define($_ = $Rows<[]*Row>(body/data).Cardinality('Many').Required())
SELECT Records.*,type(Records,'Row') FROM records Records`
	borrower := `#package('` + module + `/api')
#setting($_ = $route('/public','PATCH'))
#setting($_ = $file_prefix('public_'))
#setting($_ = $input_type('PublicInput'))
#setting($_ = $output_type('PublicOutput'))
#setting($_ = $borrow_sql_row('Rows/Records','` + module + `/api','Row','private/Private.dql','Private','Rows'))
#define($_ = $Rows<[]*PublicView>(body/data).Cardinality('Many').Required())
SELECT p.*,Records.*,type(p,'PublicView'),type(Records,'Row') FROM (parents) p JOIN (records) Records ON Records.parent_id=p.id`
	if encoding == "embed" {
		owner += " WHERE ${embed:sql/records.sql}"
		borrower += " WHERE ${embed:sql/records.sql}"
		for _, dir := range []string{sourceDir, ownerDir} {
			if err := os.MkdirAll(filepath.Join(dir, "sql"), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "sql", "records.sql"), []byte("1=1"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := os.WriteFile(filepath.Join(ownerDir, "Private.dql"), []byte(owner), 0600); err != nil {
		t.Fatal(err)
	}
	invoke := func(pkg string, want string) {
		t.Helper()
		args := []string{"transcribe", "patch", "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", dsn, pkg}
		var out, diag bytes.Buffer
		code := run(ctx, args, &out, &diag)
		t.Logf("STOCK_CLI package=%s code=%d stdout=%s diagnostic=%s", pkg, code, out.String(), diag.String())
		if want != "" {
			if code != 1 || !strings.Contains(diag.String(), want) {
				t.Fatalf("expected %s: %d %s", want, code, diag.String())
			}
		} else if code != 0 {
			t.Fatalf("CLI failed: %d %s", code, diag.String())
		}
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "Public.dql"), []byte(borrower), 0600); err != nil {
		t.Fatal(err)
	}
	invoke(module+"/source", "generate owner first")
	if entries, _ := os.ReadDir(filepath.Join(root, "api")); len(entries) != 0 {
		t.Fatal("missing owner emitted borrower artifacts")
	}
	invoke(module+"/source/private", "")
	apiSnapshot := func() map[string]string {
		t.Helper()
		result := map[string]string{}
		err := filepath.WalkDir(filepath.Join(root, "api"), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			relative, _ := filepath.Rel(filepath.Join(root, "api"), path)
			result[relative] = fmt.Sprintf("%o:%x", info.Mode(), sha256.Sum256(data))
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	repeat := func(pkg string) {
		t.Helper()
		before := apiSnapshot()
		for n := 0; n < 2; n++ {
			invoke(pkg, "")
			if after := apiSnapshot(); !reflect.DeepEqual(before, after) {
				t.Fatalf("%s repeat %d changed generated bytes/modes", pkg, n)
			}
		}
	}
	repeat(module + "/source/private")
	if err := os.WriteFile(filepath.Join(sourceDir, "Public.dql"), []byte(borrower), 0600); err != nil {
		t.Fatal(err)
	}
	invoke(module+"/source", "")
	repeat(module + "/source")

	if encoding == "embed" {
		// This is supported stock DQL authoring: a direct physical owner and
		// auxiliary borrower retain captured predicate resources. Whole-query
		// embeds remain auxiliary and cannot establish initial owner authority.
		for _, invalid := range []struct{ name, path, body, want string }{
			{"owner_union", filepath.Join(ownerDir, "sql", "records.sql"), "1=1 UNION SELECT id,parent_id,name FROM records", "physical projection requires one direct unqualified source"},
			{"missing_borrower_resource", filepath.Join(sourceDir, "sql", "records.sql"), "", "read SQL resource"},
		} {
			t.Run(invalid.name, func(t *testing.T) {
				original, err := os.ReadFile(invalid.path)
				if err != nil {
					t.Fatal(err)
				}
				info, err := os.Stat(invalid.path)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.WriteFile(invalid.path, original, info.Mode()); err != nil {
						t.Fatal(err)
					}
				}()
				if invalid.body == "" {
					err = os.Remove(invalid.path)
				} else {
					err = os.WriteFile(invalid.path, []byte(invalid.body), info.Mode())
				}
				if err != nil {
					t.Fatal(err)
				}
				before := apiSnapshot()
				invoke(module+"/source", invalid.want)
				if !reflect.DeepEqual(before, apiSnapshot()) {
					t.Fatal("resource ownership rejection published artifacts")
				}
			})
		}
	}

	receipt, err := os.ReadFile(filepath.Join(root, "api", "public_borrowed_authority.json"))
	if err != nil {
		t.Fatal(err)
	}
	var authorities []struct {
		RowFile, HasFile string
		Files            []struct{ Path string }
		Helpers          []struct{ Path string }
	}
	if err = json.Unmarshal(receipt, &authorities); err != nil || len(authorities) != 1 || len(authorities[0].Helpers) == 0 {
		t.Fatal("actual helper provenance missing", err)
	}
	for _, protected := range []struct{ name, path, want string }{
		{"row_symlink_escape", authorities[0].RowFile, "not regular"},
		{"helper_symlink_escape", authorities[0].Helpers[0].Path, "not regular"},
		{"source_symlink_escape", filepath.Join(sourceDir, "Public.dql"), "found 0"},
	} {
		t.Run(protected.name, func(t *testing.T) {
			original, err := os.ReadFile(protected.path)
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(protected.path)
			if err != nil {
				t.Fatal(err)
			}
			external := filepath.Join(t.TempDir(), "outside-closure")
			if err = os.WriteFile(external, original, info.Mode()); err != nil {
				t.Fatal(err)
			}
			if err = os.Remove(protected.path); err != nil {
				t.Fatal(err)
			}
			if err = os.Symlink(external, protected.path); err != nil {
				t.Fatal(err)
			}
			defer func() {
				os.Remove(protected.path)
				if err := os.WriteFile(protected.path, original, info.Mode()); err != nil {
					t.Fatal(err)
				}
			}()
			before := apiSnapshot()
			invoke(module+"/source", protected.want)
			if !reflect.DeepEqual(before, apiSnapshot()) {
				t.Fatal("symlink rejection changed published artifacts")
			}
			if data, err := os.ReadFile(external); err != nil || !bytes.Equal(data, original) {
				t.Fatal("symlink rejection changed external target", err)
			}
		})
	}
	type mutation struct {
		name, path string
		body       func([]byte) []byte
		remove     bool
	}
	mutations := []mutation{{name: "missing_helper", path: authorities[0].Helpers[0].Path, remove: true}, {name: "stale_row", path: authorities[0].RowFile, body: func(b []byte) []byte {
		return bytes.Replace(b, []byte("type Row struct {"), []byte("type Row struct { Unexpected bool;"), 1)
	}}, {name: "stale_has", path: authorities[0].HasFile, body: func(b []byte) []byte {
		return bytes.Replace(b, []byte("type RowHas struct {"), []byte("type RowHas struct { Unexpected bool;"), 1)
	}}, {name: "wrong_body_path", path: filepath.Join(sourceDir, "Public.dql"), body: func(b []byte) []byte { return bytes.Replace(b, []byte("'Rows/Records'"), []byte("'Rows/Missing'"), 1) }}, {name: "conflicting_type", path: filepath.Join(sourceDir, "Public.dql"), body: func(b []byte) []byte {
		return bytes.Replace(b, []byte("type(Records,'Row')"), []byte("type(Records,'OtherRow')"), 1)
	}}, {name: "wrong_owner_name", path: filepath.Join(sourceDir, "Public.dql"), body: func(b []byte) []byte {
		return bytes.Replace(b, []byte("'Private','Rows'"), []byte("'Missing','Rows'"), 1)
	}}}

	physicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range authorities[0].Files {
		if strings.HasPrefix(file.Path, filepath.Join(physicalRoot, "api")+string(os.PathSeparator)) && strings.HasSuffix(file.Path, ".sql") {
			mutations = append(mutations, mutation{name: "stale_owner_resource", path: file.Path, body: func(b []byte) []byte { result := append([]byte(nil), b...); result[len(result)-1] ^= 1; return result }})
			break
		}
	}
	mutations = append(mutations, mutation{name: "missing_owner_holder", path: filepath.Join(root, "api", "private_router.go"), remove: true})
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			original, err := os.ReadFile(mutation.path)
			if err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(mutation.path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := os.WriteFile(mutation.path, original, info.Mode()); err != nil {
					t.Fatal(err)
				}
			}()
			if mutation.remove {
				err = os.Remove(mutation.path)
			} else {
				err = os.WriteFile(mutation.path, mutation.body(original), info.Mode())
			}
			if err != nil {
				t.Fatal(err)
			}
			before := apiSnapshot()
			invoke(module+"/source", "borrow_sql_row")
			if after := apiSnapshot(); !reflect.DeepEqual(before, after) {
				t.Fatal("rejected authority published artifacts")
			}
		})
	}
	command := testharness.SourceGoCommand(t, "../..", "run", "./cmd/datly", "transcribe", "patch", "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", dsn, module+"/source")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("actual stock executable failed: %v\n%s", err, output)
	}
	if err := os.WriteFile(filepath.Join(root, "api", "borrowed_runtime_test.go"), []byte(borrowedSQLRowRuntime), 0600); err != nil {
		t.Fatal(err)
	}
	build := testharness.SourceGoCommand(t, root, "test", "-mod=mod", "-v", "./api")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("shared generated package failed: %v\n%s", err, output)
	} else {
		t.Logf("NATIVE_SHARED_ROW_CONSUMER %s", output)
	}

	// Generic SDK serialization controls; no original050 component setting.
	omittedBorrower := strings.Replace(borrower, "#setting($_ = $route", "#setting($_ = $writer_omit_empty(true))\n#setting($_ = $route", 1)
	if err := os.WriteFile(filepath.Join(sourceDir, "Public.dql"), []byte(omittedBorrower), 0600); err != nil {
		t.Fatal(err)
	}
	beforeOmission := apiSnapshot()
	invoke(module+"/source", "leaf contracts differ")
	if !reflect.DeepEqual(beforeOmission, apiSnapshot()) {
		t.Fatal("omission mismatch published files")
	}
	omittedOwner := strings.Replace(owner, "#setting($_ = $route", "#setting($_ = $writer_omit_empty(true))\n#setting($_ = $route", 1)
	if err := os.WriteFile(filepath.Join(ownerDir, "Private.dql"), []byte(omittedOwner), 0600); err != nil {
		t.Fatal(err)
	}
	invoke(module+"/source/private", "")
	invoke(module+"/source", "")
	repeat(module + "/source")
	checkOmission := testharness.SourceGoCommand(t, root, "test", "-mod=mod", "-run", "^$", "./api")
	if output, err := checkOmission.CombinedOutput(); err != nil {
		t.Fatalf("matching omission shared package: %v\n%s", err, output)
	}
	if err := os.WriteFile(filepath.Join(ownerDir, "Private.dql"), []byte(owner), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "Public.dql"), []byte(borrower), 0600); err != nil {
		t.Fatal(err)
	}
	invoke(module+"/source/private", "")
	invoke(module+"/source", "")
	changes := []struct {
		name string
		sql  []string
	}{
		{"column", []string{"ALTER TABLE records ADD COLUMN added_note TEXT"}},
		{"fk", []string{"CREATE TABLE other_parents(id INTEGER PRIMARY KEY)", "DROP TABLE records", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER REFERENCES other_parents(id),name TEXT,added_note TEXT)"}},
		{"scalar", []string{"DROP TABLE records", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER REFERENCES other_parents(id),name INTEGER,added_note TEXT)"}},
		{"nullable", []string{"DROP TABLE records", "CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER NOT NULL REFERENCES other_parents(id),name INTEGER,added_note TEXT)"}},
	}
	for _, change := range changes {
		t.Run(change.name, func(t *testing.T) {
			before := apiSnapshot()
			if err := db.ExecStatements(ctx, change.sql...); err != nil {
				t.Fatal(err)
			}
			invoke(module+"/source", "borrow_sql_row declaration")
			if after := apiSnapshot(); !reflect.DeepEqual(before, after) {
				t.Fatal("stale borrower rejection published files")
			}
			invoke(module+"/source/private", "")
			repeat(module + "/source/private")
			invoke(module+"/source", "")
			repeat(module + "/source")
			check := testharness.SourceGoCommand(t, root, "test", "-mod=mod", "-run", "^$", "./api")
			if output, err := check.CombinedOutput(); err != nil {
				t.Fatalf("evolved shared package: %v\n%s", err, output)
			}
			t.Logf("EVOLVED_SHARED_ROW %s stable owner and borrower; no generated Go edits", change.name)
		})
	}
	for path, expected := range map[string]string{filepath.Join(ownerDir, "Private.dql"): owner, filepath.Join(sourceDir, "Public.dql"): borrower} {
		actual, err := os.ReadFile(path)
		if err != nil || string(actual) != expected {
			t.Fatalf("authored source changed: %s %v", path, err)
		}
	}
	if encoding == "embed" {
		for _, dir := range []string{sourceDir, ownerDir} {
			path := filepath.Join(dir, "sql", "records.sql")
			actual, err := os.ReadFile(path)
			if err != nil || string(actual) != "1=1" {
				t.Fatalf("authored resource changed: %s %v", path, err)
			}
		}
	}
}

// Generic SDK prerequisite only: no original050 component settings or Compose.
const borrowedSQLRowRuntime = `package api
import (
 "context"
 "encoding/json"
 "net/http/httptest"
 "path/filepath"
 "reflect"
 "strings"
 "testing"
 "github.com/viant/bindly/locator"
 "github.com/viant/bindly/resource"
 requestprovider "github.com/viant/bindly/provider/request"
 "github.com/viant/datly/bootstrap"
 dexec "github.com/viant/datly/exec"
 "github.com/viant/datly/internal/testharness/sqlite"
 druntime "github.com/viant/datly/runtime"
 rh "github.com/viant/datly/runtime/handler"
 "github.com/viant/datly/runtime/handler/engine"
 hcompiler "github.com/viant/datly/runtime/handler/compiler"
 structqlcodec "github.com/viant/datly/runtime/handler/codec/structql"
 "github.com/viant/datly/runtime/handler/writer"
 "github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/dml"
 dtag "github.com/viant/datly/tag"
)
func borrowedArtifact(t *testing.T,holder,input,output reflect.Type,resources *resource.Store)*bootstrap.Artifact{
 t.Helper();field,ok:=holder.FieldByName("Contract");if !ok{t.Fatal("holder missing")};tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"api",PackagePath:holder.PkgPath(),Tag:tag,InputType:input.Name(),OutputType:output.Name()}
 component,err:=source.Resolve(input,output);if err!=nil{t.Fatal(err)}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:input,OutputType:output,Resources:resources});if err!=nil{t.Fatal(err)};return artifact
}
func bindPublic(t *testing.T,body string)*PublicInput{
 t.Helper();resources:=resource.New();if err:=resources.Register(PublicDatlyResourceNamespace,PublicDatlyResources);err!=nil{t.Fatal(err)}
 holder:=reflect.TypeFor[PublicComponent]();field,_:=holder.FieldByName("Contract");tag,present,err:=dtag.ParseComponent(field.Tag);if err!=nil||!present{t.Fatal(err)}
 source:=&bootstrap.RouteSource{HolderType:holder.Name(),FieldName:field.Name,PackageName:"api",PackagePath:holder.PkgPath(),Tag:tag,InputType:"PublicInput",OutputType:"PublicOutput"}
 component,err:=source.Resolve(reflect.TypeFor[PublicInput](),reflect.TypeFor[PublicOutput]());if err!=nil{t.Fatal(err)}
 compiled,err:=hcompiler.New(hcompiler.Input{Component:component,InputType:reflect.TypeFor[PublicInput](),Resources:resources,CodecFactory:structqlcodec.Factory{}}).Compile();if err!=nil{t.Fatal(err)}
 contract,ok:=compiled.Input.ForRoute(spec.RouteRef{Method:"PATCH",Path:"/public"});if !ok{t.Fatal("route missing")}
 request:=httptest.NewRequest("PATCH","/public",strings.NewReader(body));request.Header.Set("Content-Type","application/json");scope,err:=requestprovider.New(request);if err!=nil{t.Fatal(err)};defer scope.Close()
 result,err:=engine.New().Execute(context.Background(),engine.Request{Input:contract,Scope:scope,Handler:rh.HandlerFunc(func(_ context.Context,inv rh.Invocation)(any,error){return inv.Input,nil})});if err!=nil{t.Fatal(err)};return result.(*PublicInput)
}
func TestBorrowedPublicBindingAndPointerIdentity(t *testing.T){
 for _,tc:=range []struct{name,body string;present bool}{{"omitted","{}",false},{"null","{\"id\":null,\"parentId\":null,\"name\":null}",true},{"zero","{\"id\":0,\"parentId\":0,\"name\":\"\"}",true},{"client","{\"id\":17,\"parentId\":9,\"name\":\"client\"}",true}}{t.Run(tc.name,func(t *testing.T){
 input:=bindPublic(t,"{\"data\":[{\"id\":1,\"Records\":["+tc.body+",null]}]}");if len(input.Rows)!=1||len(input.Rows[0].Records)!=2||input.Rows[0].Records[1]!=nil{t.Fatal("nested pointer/nil slots changed")}
 public:=input.Rows[0].Records[0];child:=&PrivateInput{Rows:input.Rows[0].Records};if child.Rows[0]!=public||child.Rows[0].Has!=public.Has{t.Fatal("pointer/Has identity changed")}
 for _,name:=range []string{"Id","ParentId","Name"}{present:=public.Has!=nil&&reflect.ValueOf(*public.Has).FieldByName(name).Bool();if present!=tc.present{t.Fatalf("%s suppliedness=%v",name,present)}}
 beforeHas:=public.Has;if beforeHas==nil{public.Has=&RowHas{}};public.Has.Name=true // labelled producer observation only
 if child.Rows[0].Has!=public.Has{t.Fatal("working marker pointer lost")};raw,err:=json.Marshal(input);if err!=nil||strings.Contains(string(raw),"Has"){t.Fatal("hidden markers entered transport",err)}
 })}
 for _,value:=range []string{"",",\"Records\":null",",\"Records\":[]",",\"Records\":[null]"}{input:=bindPublic(t,"{\"data\":[{\"id\":1"+value+"}]}");rows:=input.Rows[0].Records;if value==",\"Records\":[]"{if rows==nil||len(rows)!=0{t.Fatal("empty collapsed")}}else if value==",\"Records\":[null]"{if len(rows)!=1||rows[0]!=nil{t.Fatal("nil occurrence changed")}}else if rows!=nil{t.Fatal("omitted/null changed")}}
}
func TestBorrowedPhysicalChildNativeBackfill(t *testing.T){
 ctx:=context.Background();dsn:=filepath.Join(t.TempDir(),"native-child.db");db:=sqlite.New(t,sqlite.WithDSN(dsn));t.Logf("EXCLUSIVE_SQLITE owner=%s path=%s",t.Name(),dsn);db.DB.SetMaxOpenConns(1)
 if err:=db.ExecStatements(ctx,"CREATE TABLE parents(id INTEGER PRIMARY KEY)","CREATE TABLE records(id INTEGER PRIMARY KEY AUTOINCREMENT,parent_id INTEGER REFERENCES parents(id),name TEXT)","INSERT INTO parents VALUES(1)","INSERT INTO records(id,parent_id,name) VALUES(7,1,'before')");err!=nil{t.Fatal(err)}
 resources:=resource.New();if err:=resources.Register(PrivateDatlyResourceNamespace,PrivateDatlyResources);err!=nil{t.Fatal(err)}
 artifact:=borrowedArtifact(t,reflect.TypeFor[PrivateComponent](),reflect.TypeFor[PrivateInput](),reflect.TypeFor[PrivateOutput](),resources)
 handler,err:=writer.New(artifact.Component,reflect.TypeFor[PrivateInput](),reflect.TypeFor[PrivateOutput](),"patch");if err!=nil{t.Fatal(err)}
 views,err:=artifact.NewViewProvider(bootstrap.ViewRuntimeConfig{SQL:&dsql.SQLComponent{DB:db.DB}});if err!=nil{t.Fatal(err)}
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[PrivateOutput](),Handler:handler,Providers:[]locator.Provider{views},DataSource:dml.Source{DB:db.DB}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
 input:=bindPublic(t,"{\"data\":[{\"id\":1,\"Records\":[{\"parentId\":1,\"name\":\"new\"},{\"id\":7,\"parentId\":1,\"name\":\"client\"}]}]}")
 child:=&PrivateInput{Rows:input.Rows[0].Records,Has:&PrivateInputHas{Rows:true}};row:=child.Rows[0];marker:=row.Has;client:=child.Rows[1];clientID:=client.Id
 _,err=rt.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:artifact.Component.Key,Route:spec.RouteRef{Method:"PATCH",Path:"/private"}},Input:child});if err!=nil{t.Fatal(err)}
 if input.Rows[0].Records[0]!=row||child.Rows[0]!=row||row.Has!=marker||row.Id==nil||*row.Id<=7{t.Fatal("native public pointer ID backfill failed")};if client.Id!=clientID||*client.Id!=7{t.Fatal("client identity changed")}
 var inserted int;if err=db.DB.QueryRow("SELECT count(*) FROM records WHERE id=? AND name='new'",*row.Id).Scan(&inserted);err!=nil||inserted!=1{t.Fatal("native child did not insert",err)}
 var beforeSequence int;if err=db.DB.QueryRow("SELECT seq FROM sqlite_sequence WHERE name='records'").Scan(&beforeSequence);err!=nil{t.Fatal(err)}
 aliasPublic:=bindPublic(t,"{\"data\":[{\"id\":1,\"Records\":[{\"parentId\":1,\"name\":\"alias\"}]}]}")
 alias:=aliasPublic.Rows[0].Records[0];aliasMarker:=alias.Has;aliasPublic.Rows[0].Records=append(aliasPublic.Rows[0].Records,alias)
 aliasedChild:=&PrivateInput{Rows:aliasPublic.Rows[0].Records,Has:&PrivateInputHas{Rows:true}}
 callerTx,err:=db.DB.BeginTx(ctx,nil);if err!=nil{t.Fatal(err)};defer callerTx.Rollback()
 callerViews,err:=artifact.NewViewProvider(bootstrap.ViewRuntimeConfig{SQL:&dsql.SQLComponent{DB:db.DB,Tx:callerTx}});if err!=nil{t.Fatal(err)}
 callerRuntime,err:=druntime.NewRuntime([]*registry.RegisteredComponent{{Component:artifact.Component,Input:artifact.Input,Output:artifact.Output,OutputType:reflect.TypeFor[PrivateOutput](),Handler:handler,Providers:[]locator.Provider{callerViews},DataSource:dml.Source{DB:db.DB,Tx:callerTx}}},druntime.WithResources(resources));if err!=nil{t.Fatal(err)}
 _,duplicateErr:=callerRuntime.InvokeComponent(ctx,dexec.ComponentRequest{Target:dexec.ComponentTarget{Component:artifact.Component.Key,Route:spec.RouteRef{Method:"PATCH",Path:"/private"}},Input:aliasedChild})
 if duplicateErr==nil||!strings.Contains(strings.ToLower(duplicateErr.Error()),"unique"){t.Fatalf("actual duplicate INSERT failure required: %v",duplicateErr)}
 if aliasPublic.Rows[0].Records[0]!=alias||aliasPublic.Rows[0].Records[1]!=alias||alias.Has!=aliasMarker||alias.Id==nil||*alias.Id!=beforeSequence+1{t.Fatal("aliased occurrence ID/Has identity changed")}
 var afterSequence int;if err=callerTx.QueryRow("SELECT seq FROM sqlite_sequence WHERE name='records'").Scan(&afterSequence);err!=nil{t.Fatal(err)}
 if afterSequence!=beforeSequence+2{t.Fatalf("E=2 occurrence reservation tail lost: before%d after%d",beforeSequence,afterSequence)}
 t.Logf("NATIVE_ALIAS_E_U empty occurrences=2 unique cells=1 first ID=%d pending caller sequence tail=%d failure=%s",*alias.Id,afterSequence,duplicateErr)
 if err=callerTx.Rollback();err!=nil{t.Fatal(err)}
 var restoredSequence int;if err=db.DB.QueryRow("SELECT seq FROM sqlite_sequence WHERE name='records'").Scan(&restoredSequence);err!=nil||restoredSequence!=beforeSequence{t.Fatalf("caller rollback reservation cleanup: seq=%d err=%v",restoredSequence,err)}
}

`
