package golang

import (
	"bytes"
	"go/format"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

func TestLowerBatchConflictCapabilityLiteral(t *testing.T) {
	field := func(root, name, typ string) plan.StatementSelector {
		return plan.StatementSelector{Path: plan.FieldPath{root, name}, Type: spec.TypeRef{Name: typ}, Addressable: true}
	}
	suffix := "ON DUPLICATE KEY UPDATE WEIGHT = VALUES(WEIGHT), UPDATED_USER = VALUES(UPDATED_USER), UPDATED = CURRENT_TIMESTAMP"
	for _, method := range []string{"Insert", "Delete", "InsertConflict"} {
		p := &plan.StatementPlan{Method: strings.TrimSuffix(method, "Conflict"), BatchTable: field("Input", "Table", "string"), BatchRows: field("Input", "Rows", "[]*Record"), BatchSize: field("Input", "BatchSize", "int"), BatchAffected: field("Output", "Affected", "int64")}
		name := p.Method + "WithBatchSize"
		if method == "InsertConflict" {
			p.BatchOnDuplicateKeySQL = &suffix
			name += "AndOnDuplicateKeySQL"
		}
		file, err := StatementQueue(p, Config{Package: "example", Factory: "NewRecord", InputType: "Input", OutputType: "Output"})
		if err != nil {
			t.Fatal(err)
		}
		var source bytes.Buffer
		if err = format.Node(&source, token.NewFileSet(), file); err != nil {
			t.Fatal(err)
		}
		text := source.String()
		if !strings.Contains(text, "capability."+name+"(") || !strings.Contains(text, name+"(string, any, int, *int64") {
			t.Fatalf("capability changed: %s", text)
		}
		if method == "InsertConflict" {
			if !strings.Contains(text, `"`+suffix+`"`) || !strings.Contains(text, "*int64, string) error") {
				t.Fatalf("literal not emitted: %s", text)
			}
		} else if strings.Contains(text, "OnDuplicateKey") {
			t.Fatal("old batch API changed")
		}
		fixture := `package example
import "testing"
type Record struct{ID int};type Input struct{Table string;Rows []*Record;BatchSize int};type Output struct{Affected int64}
type probe struct{method string;suffix string;rows any;size int;dest *int64}
func(p *probe)InsertWithBatchSize(table string,rows any,size int,dest *int64)error{p.method="Insert";p.rows=rows;p.size=size;p.dest=dest;return nil}
func(p *probe)DeleteWithBatchSize(table string,rows any,size int,dest *int64)error{p.method="Delete";p.rows=rows;p.size=size;p.dest=dest;return nil}
func(p *probe)InsertWithBatchSizeAndOnDuplicateKeySQL(table string,rows any,size int,dest *int64,suffix string)error{p.method="InsertConflict";p.rows=rows;p.size=size;p.dest=dest;p.suffix=suffix;return nil}
func TestGeneratedBatch(t *testing.T){in:=&Input{Table:"weights",Rows:[]*Record{{1}},BatchSize:137};out:=&Output{};p:=&probe{}
if err:=NewRecordQueueStatement(p,in,out);err!=nil{t.Fatal(err)}
if p.method!="EXPECTED_METHOD"||p.size!=137||p.dest!=&out.Affected||p.rows.([]*Record)[0]!=in.Rows[0]{t.Fatal("typed batch arguments changed")}
if p.method=="InsertConflict"&&p.suffix!="ON DUPLICATE KEY UPDATE WEIGHT = VALUES(WEIGHT), UPDATED_USER = VALUES(UPDATED_USER), UPDATED = CURRENT_TIMESTAMP"{t.Fatal("literal changed")}
if err:=NewRecordQueueStatement(struct{}{},in,out);err==nil{t.Fatal("missing capability accepted")}
}`
		fixture = strings.ReplaceAll(fixture, "EXPECTED_METHOD", method)
		dir := t.TempDir()
		for filename, content := range map[string]string{"go.mod": "module example\n\ngo 1.24\n", "statement.go": text, "statement_test.go": fixture} {
			if err = os.WriteFile(filepath.Join(dir, filename), []byte(content), 0644); err != nil {
				t.Fatal(err)
			}
		}
		cmd := exec.Command("go", "test", "-count=1", "./...")
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("generated batch compile/runtime: %v\n%s", err, output)
		}

	}
}
