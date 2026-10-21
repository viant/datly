package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/column"
)

func preparedRuleColumns(groups bool) []*spec.Column {
	columns := []*spec.Column{
		{Name: "tenant_id", Source: "tenant_id", Type: spec.TypeRef{Name: "string"}, PrimaryKey: true, NotNull: true, Tag: `sqlx:"tenant_id,refTable=tenants,refColumn=id"`},
		{Name: "id", Source: "id", Type: spec.TypeRef{Name: "int64"}, PrimaryKey: true, NotNull: true},
		{Name: "exposures", Source: "exposures", Type: spec.TypeRef{Name: "string"}},
	}
	if groups {
		columns = append(columns, &spec.Column{Name: "groups", Source: "groups", Type: spec.TypeRef{Name: "string"}, Nullable: true})
	}
	return columns
}

func TestPreparedColumnsGenerateReaderAndExposureWriter(t *testing.T) {
	for _, operation := range []string{"get", "patch"} {
		t.Run(operation, func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			groups := operation == "get"
			columns := preparedRuleColumns(groups)
			body := "#define($_ = $Items<[]*Record>(output/view))"
			if !groups {
				body = "#define($_ = $Data<[]*Record>(output/body))"
			}
			text := "#package('records')\n#setting($_ = $input_type('RecordsInput'))\n#setting($_ = $output_type('RecordsOutput'))\n#setting($_ = $route('/records','" + strings.ToUpper(operation) + "'))\n#setting($_ = $connector('catalog'))\n" + body + "\nSELECT r.*, type(r,'Record') FROM rules r"
			source := &Source{Name: "Records", Scope: "example.com/generated/source", Text: text, PreparedColumns: func(_ context.Context, request column.PreparedColumnRequest) (*column.PreparedColumnResult, error) {
				if request.Connector != "catalog" || !strings.Contains(request.Source.SQL, "rules") {
					t.Fatalf("unexpected prepared source: %+v", request)
				}
				return &column.PreparedColumnResult{Table: "rules", Columns: columns}, nil
			}}
			generator := Generator{Operation: operation}
			request := GenerationRequest{Source: source, Destination: root}
			got, err := generator.Generate(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Fatal("missing generated package")
			}
			viewsPath := filepath.Join(root, "records", "views.go")
			before, err := os.ReadFile(viewsPath)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(before), "primaryKey") {
				t.Fatal("lost prepared composite keys")
			}
			if !strings.Contains(string(before), "refTable=tenants") {
				t.Fatal("lost prepared reference constraint")
			}
			if groups && !strings.Contains(string(before), "Groups") {
				t.Fatal("optional group column missing")
			}
			if !groups && strings.Contains(string(before), "Groups") {
				t.Fatal("exposure writer gained group column")
			}
			// Regenerate through ordinary source-package discovery as well as the
			// direct Source API. No database or schema connector is configured.
			sourceDir := filepath.Join(root, "source")
			if err = os.MkdirAll(sourceDir, 0700); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(sourceDir, "Records.dql"), []byte(text), 0600); err != nil {
				t.Fatal(err)
			}
			project, err := (&Discovery{BaseDir: root, Include: []string{"example.com/generated/source"}, PreparedColumns: source.PreparedColumns}).Compile(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if len(project.Components) != 1 {
				t.Fatal("missing discovered component")
			}
			if _, err = generator.Generate(context.Background(), GenerationRequest{Compiled: project.Components[0], Destination: root}); err != nil {
				t.Fatal(err)
			}
			after, err := os.ReadFile(viewsPath)
			if err != nil || string(after) != string(before) {
				t.Fatal("regeneration changed Go shapes", err)
			}
			if source.Text != text || columns[0].Name != "tenant_id" {
				t.Fatal("compiler changed caller metadata")
			}
			if groups {
				if err = os.WriteFile(filepath.Join(root, "records", "prepared_runtime_test.go"), []byte(preparedReaderRuntime), 0600); err != nil {
					t.Fatal(err)
				}
			}
			command := exec.Command("go", "test", "-mod=mod", "./records")
			command.Dir = root
			command.Env = append(os.Environ(), "GOWORK=off")
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("generated package failed: %v\n%s", err, output)
			}
		})
	}
}

func TestPreparedColumnsRejectMixedDiscoveryBeforeUse(t *testing.T) {
	called := false
	source := &Source{ColumnRefiner: column.New(nil), PreparedColumns: func(context.Context, column.PreparedColumnRequest) (*column.PreparedColumnResult, error) {
		called = true
		return nil, nil
	}}
	_, err := NewCompiler().Compile(context.Background(), source)
	if err == nil || !strings.Contains(err.Error(), "mutually exclusive") || called {
		t.Fatalf("mixed discovery was not rejected early: %v", err)
	}
}

// This exercises generated row types and their actual retained SQL with a
// database/sql fake. No database, schema creation or DML is involved.
const preparedReaderRuntime = `package records
import("context";"database/sql";"database/sql/driver";"errors";"io";"strings";"testing";read "github.com/viant/sqlx/io/read")
type connector struct{groups bool; query *string}
func(c connector)Connect(context.Context)(driver.Conn,error){return connection{c},nil}
func(c connector)Driver()driver.Driver{return fakeDriver{}}
type fakeDriver struct{}
func(fakeDriver)Open(string)(driver.Conn,error){return nil,errors.New("no database")}
type connection struct{connector}
func(c connection)Prepare(q string)(driver.Stmt,error){*c.query=q;return statement{c.groups},nil}
func(connection)Close()error{return nil}
func(connection)Begin()(driver.Tx,error){return nil,errors.New("no transactions")}
type statement struct{groups bool}
func(statement)Close()error{return nil}
func(statement)NumInput()int{return 0}
func(statement)Exec([]driver.Value)(driver.Result,error){return nil,errors.New("no writes")}
func(s statement)Query([]driver.Value)(driver.Rows,error){return &rows{groups:s.groups},nil}
type rows struct{groups,done bool}
func(r *rows)Columns()[]string{c:=[]string{"tenant_id","id","exposures"};if r.groups{c=append(c,"groups")};return c}
func(r *rows)Close()error{return nil}
func(r *rows)Next(v []driver.Value)error{if r.done{return io.EOF};r.done=true;copy(v,[]driver.Value{"owner",int64(7),"[exposure]"});if r.groups{v[3]="[group]"};return nil}
func TestPreparedReaderOptionalColumns(t *testing.T){
 var query string
 var source string
 // Generated root resource remains the authored wildcard.
 for _,name:=range []string{"sql/records.sql","sql/r.sql","sql/component.sql"}{if b,e:=RecordsDatlyResources.ReadFile(name);e==nil{source=string(b);break}}
 if source==""||!strings.Contains(source,"*"){t.Fatalf("wildcard SQL not retained: %q",source)}
 for _,groups:=range []bool{false,true}{
  db:=sql.OpenDB(connector{groups:groups,query:&query})
  reader,e:=read.New(context.Background(),db,source,func()interface{}{return new(Record)});if e!=nil{t.Fatal(e)}
  var got *Record
  e=reader.QueryAll(context.Background(),func(v interface{})error{got=v.(*Record);return nil});db.Close();if e!=nil{t.Fatal(e)}
  if got==nil||got.TenantId!="owner"||got.Id!=7||got.Exposures!="[exposure]"{t.Fatalf("lost base fields: %+v",got)}
  if groups&&(got.Groups==nil||*got.Groups!="[group]"){t.Fatal("lost existing group condition")}
  if !groups&&got.Groups!=nil{t.Fatal("absent column fabricated a group")}
  if query!=source{t.Fatal("runtime rewrote source")}
 }
}
`
