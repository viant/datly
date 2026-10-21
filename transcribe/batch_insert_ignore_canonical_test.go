package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
)

// Exercise public source compilation before emission: direct AST/emitter tests
// do not cover BufferedBatch's statement-program recognition.
func TestCanonicalBatchInsertIgnore(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/genfixture"}).Write(t, root)
	if err := os.MkdirAll(filepath.Join(root, "model"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "model/record.go"), []byte("package model\ntype Record struct { Subscription int `sqlx:\"report_suite_subscription_id\"`; Campaign int `sqlx:\"campaign_id\"` }\n"), 0644); err != nil {
		t.Fatal(err)
	}
	catalog := typecatalog.NewCatalog()
	pkg, err := loaderast.LoadPackageFS(ctx, os.DirFS(root), "model")
	if err != nil {
		t.Fatal(err)
	}
	if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
		t.Fatal(err)
	}
	const source = `#package('pkg/weights')
#import('model','github.com/viant/datly/genfixture/model')
#setting($_ = $route('/weights/upsert','POST'))
#define($_ = $Table<string>(param/table))
#define($_ = $Rows<[]*model.Record>(param/rows))
#define($_ = $BatchSize<int>(param/batchSize))
#define($_ = $Affected<int64>(output/body))
$dml.InsertIgnore($Input.Table,$Input.Rows,$Input.BatchSize,$Output.Affected)`
	request := GenerationRequest{Destination: root, Source: &Source{Name: "Weights", Scope: "github.com/viant/datly/genfixture/pkg/weights", Connector: "main", Text: source, Types: catalog}}
	compiled, err := NewCompiler().Compile(ctx, request.Source)
	if err != nil {
		t.Fatal(err)
	}
	if compiled.StatementProgram == "" || compiled.VeltyHandler != nil || compiled.GoHandler != nil {
		t.Fatal("INSERT IGNORE source did not select the sole native statement owner")
	}
	request.Compiled = compiled
	request.Source = nil
	generated, err := (Generator{Operation: "post"}).Generate(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if generated.Result.Plan.ContractHandler == nil || generated.Result.Plan.MutationHandler != nil || generated.Result.Plan.VeltyHandler != nil || generated.Result.Plan.Settings.Mutation != "" {
		t.Fatal("ignore batch acquired competing persistence")
	}
	path := filepath.Join(root, "pkg/weights/handler.go")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(before), "InsertIgnoreWithBatchSize(input.Table, input.Rows, input.BatchSize, &output.Affected)") {
		t.Fatalf("missing guarded ignore capability: %s", before)
	}
	if _, err = (Generator{Operation: "post"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil || string(before) != string(after) {
		t.Fatal("canonical repeat changed generated handler")
	}
	const runtime = `package weights
import("testing";"github.com/viant/datly/genfixture/model")
type conflictProbe struct{input *WeightsInput;calls int}
func(p *conflictProbe)InsertIgnoreWithBatchSize(table string,rows any,size int,affected *int64)error{
 p.calls++; actual:=rows.([]*model.Record)
 if table!="REPORT_SUITE_CAMPAIGN_MAPPING"||size!=137||len(actual)!=137||actual[0]!=p.input.Rows[0]||actual[136]!=p.input.Rows[136]{panic("source collection ownership/cardinality changed")}
 *affected=137;return nil
}
func TestGeneratedConflictCommand(t *testing.T){in:=&WeightsInput{Table:"REPORT_SUITE_CAMPAIGN_MAPPING",Rows:make([]*model.Record,137),BatchSize:137};for i:=range in.Rows{in.Rows[i]=&model.Record{Subscription:31,Campaign:i+1}};out:=&WeightsOutput{};p:=&conflictProbe{input:in};if err:=NewWeightsHandlerQueueStatement(p,in,out);err!=nil{t.Fatal(err)};if p.calls!=1||out.Affected!=137{t.Fatal("collection was split or affected destination lost")}}
`
	if err = os.WriteFile(filepath.Join(root, "pkg/weights/conflict_runtime_test.go"), []byte(runtime), 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("canonical generated native ignore command: %v\n%s", err, output)
	}
}
