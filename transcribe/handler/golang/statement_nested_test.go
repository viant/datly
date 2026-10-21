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

func TestLowerNestedStatementPointerGuards(t *testing.T) {
	value := &plan.StatementPlan{SQL: "INSERT INTO records(id) VALUES (1)", LastInsertID: plan.StatementSelector{Path: plan.FieldPath{"Output", "Row", "Id"}, Type: spec.TypeRef{Name: "int"}, Addressable: true, PointerParents: []plan.FieldPath{{"Output", "Row"}, {"Output", "Row", "Embedded"}}}}
	file, err := StatementQueue(value, Config{Package: "example", Factory: "NewRecord", InputType: "Input", OutputType: "Output"})
	if err != nil {
		t.Fatal(err)
	}
	var source bytes.Buffer
	if err = format.Node(&source, token.NewFileSet(), file); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	const runtime = `package example
import("strings";"testing")
type Embedded struct{Id int};type Row struct{*Embedded};type Input struct{};type Output struct{Row *Row}
type probe struct{calls int;destination any}
func(p *probe)ExecuteWithResult(_ string,destination any,args ...any)error{p.calls++;p.destination=destination;return nil}
func TestGuards(t *testing.T){in:=&Input{};p:=&probe{};for _,out:=range []*Output{nil,{}, {Row:&Row{}}}{err:=NewRecordQueueStatement(p,in,out);if err==nil||p.calls!=0{t.Fatal("uninitialized parent queued")};if out!=nil&&!strings.Contains(err.Error(),"Output.Row"){t.Fatal(err)}}
 row:=&Row{Embedded:&Embedded{Id:-1}};out:=&Output{Row:row};if err:=NewRecordQueueStatement(p,in,out);err!=nil{t.Fatal(err)};if p.calls!=1||p.destination!=&row.Id||out.Row!=row||row.Id!=-1{t.Fatal("direct promoted destination ownership changed")};*p.destination.(*int)=42;if row.Id!=42||out.Row!=row{t.Fatal("original object not updated")}}
`
	for name, body := range map[string]string{"go.mod": "module example\n\ngo 1.24\n", "statement.go": source.String(), "statement_test.go": runtime} {
		if err = os.WriteFile(filepath.Join(dir, name), []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
	}
	cmd := exec.Command("go", "test", "-race", "-count=1", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("nested generated pointer guards: %v\n%s", err, output)
	}
}
