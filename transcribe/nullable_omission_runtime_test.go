package transcribe

import (
	"context"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
)

func TestGeneratedNullableWriterDefaultsAndSchemaEvolution(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx, `CREATE TABLE records(id INTEGER PRIMARY KEY,nullable_number INTEGER,nullable_bool BOOLEAN,nullable_text TEXT,nullable_value TEXT,nonnull TEXT NOT NULL DEFAULT '')`))
	root := t.TempDir()
	const module = "github.com/viant/datly/nullablefixture"
	(testharness.GeneratedModule{Path: module}).Write(t, root)
	// SQLite result metadata is nullable even for NOT NULL physical columns.
	// The explicit JSON name here tests authored policy; nonnullable default
	// omission is independently covered using canonical SQL metadata.
	source := &Source{Name: "Records", Scope: module + "/records", Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `#package('records')
#setting($_ = $route('/records','PATCH'))
#setting($_ = $case_format('lc'))
#define($_ = $Records<[]*RecordsView>(body/data))
#define($_ = $CurrentRecords<?>(view/CurrentRecords).Cardinality('Many') /* SELECT id FROM records WHERE id IN (#foreach($row in $Records)$row.Id#if($foreach.HasNext),#end#end) */)
#define($_ = $Data<[]*RecordsView>(output/body))
SELECT r.*,CAST(r.id AS int),CAST(r.nullable_number AS *int),CAST(r.nullable_bool AS *bool),CAST(r.nullable_text AS *string),CAST(r.nullable_value AS string),CAST(r.nonnull AS string),tag(r.nonnull,'json:"nonnull"')
FROM records r`}
	request := Request{Source: source, Destination: root, Generation: GenerationOptions{Operation: "patch"}}
	_, err := NewCompiler().Transcribe(ctx, request)
	require.NoError(t, err)
	// A new nullable physical column must acquire omission without a DQL edit.
	require.NoError(t, db.ExecStatements(ctx, `ALTER TABLE records ADD COLUMN extra_text TEXT`))
	_, err = NewCompiler().Transcribe(ctx, request)
	require.NoError(t, err)
	snapshot := func() map[string]string {
		files := map[string]string{}
		require.NoError(t, filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.IsDir() && filepath.Ext(path) == ".go" {
				data, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				files[path] = string(data)
			}
			return nil
		}))
		return files
	}
	before := snapshot()
	_, err = NewCompiler().Transcribe(ctx, request)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual(before, snapshot()), "regeneration changed emitted Go")
	require.NoError(t, os.WriteFile(filepath.Join(root, "records", "nullable_runtime_test.go"), []byte(nullableWriterRuntimeSource), 0600))
	command := exec.CommandContext(ctx, "go", "test", "-race", "-mod=mod", "-count=1", "-timeout=60s", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
}

const nullableWriterRuntimeSource = `package records
import (
 "context"
 "database/sql"
 "encoding/json"
 "net/http/httptest"
 "reflect"
 "strings"
 "testing"
 "github.com/viant/datly/bootstrap"
 gateway "github.com/viant/datly/gateway/http"
 druntime "github.com/viant/datly/runtime"
 rhandler "github.com/viant/datly/runtime/handler"
 "github.com/viant/datly/runtime/registry"
 "github.com/viant/datly/spec"
 "github.com/viant/datly/sql/dml"
 _ "github.com/mattn/go-sqlite3"
)
type transportInput struct { Data []*RecordsView ` + "`" + `parameter:"Data,kind=body,in=data,required" json:"data"` + "`" + ` }
func TestNullableDefaultsHTTPAndDML(t *testing.T){
 ctx:=context.Background()
 component:=&spec.Component{Key:spec.Key{Kind:spec.KindComponent,Scope:"example.com/nullable",Name:"Records"},Settings:&spec.Settings{CaseFormat:"lc"},Routes:[]*spec.Route{{Method:"PATCH",Path:"/records"}}}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(transportInput{}),OutputType:reflect.TypeOf([]*RecordsView{})});if err!=nil{t.Fatal(err)}
 var bound *RecordsView
 entry,err:=artifact.Registration(registry.RegisteredComponent{Handler:rhandler.HandlerFunc(func(_ context.Context,inv rhandler.Invocation)(any,error){bound=inv.Input.(*transportInput).Data[0];return []*RecordsView{bound},nil})});if err!=nil{t.Fatal(err)}
 rt,err:=druntime.NewRuntime([]*registry.RegisteredComponent{entry});if err!=nil{t.Fatal(err)};defer rt.Shutdown(ctx)
 for _,tc:=range []struct{name,body string;present,zero bool}{
  {"absent",` + "`" + `{"data":[{"id":1}]}` + "`" + `,false,false},
  {"null",` + "`" + `{"data":[{"id":1,"nullableNumber":null,"nullableBool":null,"nullableText":null,"nullableValue":null}]}` + "`" + `,true,false},
  {"zero",` + "`" + `{"data":[{"id":1,"nullableNumber":0,"nullableBool":false,"nullableText":"","nullableValue":""}]}` + "`" + `,true,true},
 }{t.Run(tc.name,func(t *testing.T){
  req:=httptest.NewRequest("PATCH","/records",strings.NewReader(tc.body));req.Header.Set("Content-Type","application/json");rec:=httptest.NewRecorder();gateway.NewHandler(rt,nil,"").ServeHTTP(rec,req);if rec.Code!=200{t.Fatalf("HTTP %d: %s",rec.Code,rec.Body.String())}
  var rows []map[string]any;if err=json.Unmarshal(rec.Body.Bytes(),&rows);err!=nil||len(rows)!=1{t.Fatalf("response %s: %v",rec.Body.String(),err)}
  if _,ok:=rows[0]["nonnull"];!ok{t.Fatal("nonnullable zero disappeared")};if _,ok:=rows[0]["nullableValue"];ok{field,_:=reflect.TypeOf(RecordsView{}).FieldByName("NullableValue");raw,_:=json.Marshal(bound);t.Fatalf("nullable value-shaped zero retained; tag=%s std=%s http=%s",field.Tag,raw,rec.Body.String())};if _,ok:=rows[0]["extraText"];ok{t.Fatal("new nullable column not inferred")}
  for _,name:=range []string{"NullableNumber","NullableBool","NullableText","NullableValue"}{if reflect.ValueOf(*bound.Has).FieldByName(name).Bool()!=tc.present{t.Fatalf("presence %s: %+v",name,bound.Has)}}
  for _,name:=range []string{"nullableNumber","nullableBool","nullableText"}{if _,ok:=rows[0][name];ok!=tc.zero{t.Fatalf("pointer zero/null policy %s: %v",name,rows)}}
  ordinary,err:=json.Marshal(bound);if err!=nil{t.Fatal(err)};var object map[string]any;if err=json.Unmarshal(ordinary,&object);err!=nil{t.Fatal(err)};if _,ok:=object["nullableValue"];ok{t.Fatal("encoding/json retained nullable zero")}
  db,err:=sql.Open("sqlite3",":memory:");if err!=nil{t.Fatal(err)};defer db.Close();db.SetMaxOpenConns(1)
  if _,err=db.Exec("CREATE TABLE records(id INTEGER PRIMARY KEY,nullable_number INTEGER,nullable_bool BOOLEAN,nullable_text TEXT,nullable_value TEXT,nonnull TEXT NOT NULL DEFAULT '',extra_text TEXT);INSERT INTO records VALUES(1,7,1,'before','before','keep','extra')");err!=nil{t.Fatal(err)}
  data:=dml.NewData(db);if err=data.BeginInvocation();err!=nil{t.Fatal(err)};if err=data.Update("records",bound);err!=nil{t.Fatal(err)};if err=data.Complete(ctx,nil);err!=nil{t.Fatal(err)}
  var number sql.NullInt64;var flag sql.NullBool;var text sql.NullString;var nonnull,extra string
  if err=db.QueryRow("SELECT nullable_number,nullable_bool,nullable_text,nonnull,extra_text FROM records WHERE id=1").Scan(&number,&flag,&text,&nonnull,&extra);err!=nil{t.Fatal(err)}
  if nonnull!="keep"||extra!="extra"{t.Fatal("omitted physical columns changed")}
  if !tc.present {if number.Int64!=7||!flag.Bool||text.String!="before"{t.Fatal("absent values wrote to database")}} else if tc.zero {if !number.Valid||number.Int64!=0||!flag.Valid||flag.Bool||!text.Valid||text.String!=""{t.Fatal("explicit zero values lost")}} else if number.Valid||flag.Valid||text.Valid{t.Fatal("explicit null values lost")}
 })}
}
`
