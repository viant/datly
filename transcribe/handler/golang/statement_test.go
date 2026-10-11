package golang

import (
	"bytes"
	"go/format"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestLowerBufferedStatementTypedQueue(t *testing.T) {
	selector := func(root, name, typ string) plan.StatementSelector {
		return plan.StatementSelector{Path: plan.FieldPath{root, name}, Type: spec.TypeRef{Name: typ}, Addressable: true}
	}
	value := &plan.StatementPlan{SQL: "INSERT INTO records(name) VALUES (?)", LastInsertID: selector("Input", "ID", "int64"), Arguments: []plan.StatementSelector{selector("Input", "Name", "string")}}
	file, err := StatementQueue(value, Config{Package: "example", Factory: "NewRecord", InputType: "Input", OutputType: "Output"})
	if err != nil {
		t.Fatal(err)
	}
	var source bytes.Buffer
	if err := format.Node(&source, token.NewFileSet(), file); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for name, content := range map[string]string{"go.mod": "module example\n\ngo 1.24\n", "statement.go": source.String(), "statement_test.go": `package example
import("errors";"testing")
type Input struct{ID int64;Name string};type Output struct{}
type probe struct{sql string;args []any;dest any;calls int;failure error}
func(p *probe)ExecuteWithResult(sql string,dest any,args ...any)error{p.sql,p.args,p.dest=sql,args,dest;p.calls++;return p.failure}
func TestQueue(t *testing.T){in:=&Input{Name:"record"};out:=&Output{};p:=&probe{}
if err:=NewRecordQueueStatement(p,in,out);err!=nil{t.Fatal(err)}
if p.sql!="INSERT INTO records(name) VALUES (?)"||p.dest!=&in.ID||len(p.args)!=1||p.args[0]!="record"||in.ID!=0{t.Fatal("typed queue contract changed")}
failure:=errors.New("closed");p.failure=failure;if err:=NewRecordQueueStatement(p,in,out);!errors.Is(err,failure){t.Fatal("queue failure swallowed")}
if err:=NewRecordQueueStatement(struct{}{},in,out);err==nil{t.Fatal("unsupported capability accepted")}
if err:=NewRecordQueueStatement(p,nil,out);err==nil{t.Fatal("nil input accepted")}
if err:=NewRecordQueueStatement(p,in,nil);err==nil{t.Fatal("nil output accepted")}
if p.calls!=2{t.Fatal("rejected operation executed")}
}`} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("go", "test", "-race", "-count=1", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("typed generated statement: %v\n%s", err, output)
	}
}
