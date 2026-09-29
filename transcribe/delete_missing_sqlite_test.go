package transcribe

import (
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"os/exec"
	"strings"
	"testing"
)

func TestGeneratedOnDeleteNotFoundSQLite(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id TEXT PRIMARY KEY,title TEXT,owner TEXT,attempt INTEGER)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: "github.com/viant/datly/predicatefixture"}).Write(t, root)
	source := strings.Replace(mutationPredicateDQL, "mutation_predicate(r,7)", "delete_not_found(r,'ignore')", 1)
	request := GenerationRequest{Destination: root, Source: &Source{Name: "records", Scope: "github.com/viant/datly/predicatefixture/source", Text: source, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}}
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, request); err != nil {
		t.Fatal(err)
	}
	runtimeSource := mutationPredicateRuntime
	runtimeSource = strings.Replace(runtimeSource, "if component.RootView.MutationPredicateGroup==nil{t.Fatal(\"generated mutation predicate metadata was lost in bootstrap\")}", "if component.RootView.OnDeleteNotFound!=\"ignore\"{t.Fatal(\"generated onDeleteNotFound metadata was lost in bootstrap\")}", 1)
	start := strings.Index(runtimeSource, " for _,test:=range []useCase{")
	end := strings.Index(runtimeSource[start:], " }{\n") + start
	cases := ` for _,test:=range []useCase{
 {desc:"known delete",input:input{body:"{\"Data\":[{\"id\":\"one\",\"shouldDelete\":true}]}"},expect:expect{remaining:0}},
 {desc:"missing delete no-op",input:input{body:"{\"Data\":[{\"id\":\"missing\",\"shouldDelete\":true}]}"},expect:expect{title:"keep",remaining:1}},
 {desc:"mixed known and missing deletes",input:input{body:"{\"Data\":[{\"id\":\"one\",\"shouldDelete\":true},{\"id\":\"missing\",\"shouldDelete\":true}]}"},expect:expect{remaining:0}},
 {desc:"incomplete identity still fails",input:input{body:"{\"Data\":[{\"shouldDelete\":true}]}"},expect:expect{bindingFailure:true,title:"keep",remaining:1}},
`
	runtimeSource = runtimeSource[:start] + cases + runtimeSource[end:]
	writeSourceFile(t, root, "generated/predicate_test.go", runtimeSource)
	run := func() ([]byte, error) {
		command := exec.Command("go", "test", "-mod=mod", "-count=1", "./generated")
		command.Dir = root
		return command.CombinedOutput()
	}
	if output, err := run(); err != nil {
		t.Fatalf("generated deletion runtime: %v\n%s", err, output)
	}
	// An excluded existing row must be a no-op, never an unscoped deletion.
	scoped := request
	scopedSource := *request.Source
	scoped.Source = &scopedSource
	scoped.Source.Text = strings.Replace(source, "FROM records) r", "FROM records WHERE owner='other') r", 1)
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, scoped); err != nil {
		t.Fatal(err)
	}
	scopedRuntime := strings.ReplaceAll(runtimeSource, "expect:expect{remaining:0}", `expect:expect{title:"keep",remaining:1}`)
	writeSourceFile(t, root, "generated/predicate_test.go", scopedRuntime)
	if output, err := run(); err != nil {
		t.Fatalf("scoped no-op: %v\n%s", err, output)
	}
	// A declared mutation guard retains strict matching and execution conflicts.
	guarded := request
	guardedSource := *request.Source
	guarded.Source = &guardedSource
	guarded.Source.Text = strings.Replace(mutationPredicateDQL, "mutation_predicate(r,7)", "mutation_predicate(r,7),delete_not_found(r,'ignore')", 1)
	if _, err := (Generator{Operation: "patch"}).Generate(ctx, guarded); err != nil {
		t.Fatal(err)
	}
	writeSourceFile(t, root, "generated/predicate_test.go", mutationPredicateRuntime)
	if output, err := run(); err != nil {
		t.Fatalf("guarded deletion: %v\n%s", err, output)
	}
	for _, policy := range []string{"invalid", "ignore"} {
		invalid := request
		copy := *request.Source
		invalid.Source = &copy
		invalid.Destination = t.TempDir()
		if policy == "invalid" {
			invalid.Source.Text = strings.Replace(source, "'ignore'", "'invalid'", 1)
		} else {
			invalid.Source.Text = strings.Replace(source, "delete_marker(r.should_delete)", "CAST(r.should_delete AS bool)", 1)
		}
		if _, err := (Generator{Operation: "patch"}).Generate(ctx, invalid); err == nil {
			t.Fatal("invalid policy or marker-free policy was accepted")
		}
	}
}
