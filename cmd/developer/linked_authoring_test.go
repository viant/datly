package developer_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/project/build"
)

func TestNativeLinkedProjectBinaryTranscribesCustomPredicate(t *testing.T) {
	// The generated consumer is a standalone module outside the caller workspace.
	t.Setenv("GOWORK", "off")
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	(testharness.GeneratedModule{Path: "example.com/linkedauthor"}).Write(t, root)
	service := build.Service{}
	if err = service.Init(context.Background(), build.InitRequest{Dir: root}); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{"security", "source"} {
		if err = os.MkdirAll(filepath.Join(root, dir), 0755); err != nil {
			t.Fatal(err)
		}
	}
	predicate := `package security
import("context";"github.com/viant/xdatly/predicate")
type Threshold struct{}
func(*Threshold)Compute(ctx context.Context,value any)(*predicate.Criteria,error){return &predicate.Criteria{Expression:"id >= ?",Placeholders:[]any{value}},nil}
`
	if err = os.WriteFile(filepath.Join(root, "security/predicate.go"), []byte(predicate), 0644); err != nil {
		t.Fatal(err)
	}
	dql := `#package('api/records')
#import('security','example.com/linkedauthor/security')
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#setting($_ = $route('/records','GET'))
#setting($_ = $connector('main'))
#define($_ = $Minimum<int>(query/min).Value('1').WithPredicate(0,'handler','security.Threshold'))
#define($_ = $Records<[]*Record>(output/view))
SELECT records.*,type(records,'Record')
FROM (SELECT id FROM records ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("WHERE")}) records
`
	if err = os.WriteFile(filepath.Join(root, "source/Records.dql"), []byte(dql), 0644); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	links, err := service.SyncLinks(ctx, build.LinkRequest{Dir: root})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(links.Added, ","), "example.com/linkedauthor/security") {
		t.Fatalf("native predicate discovery missing: %+v", links)
	}
	(testharness.GeneratedModule{}).WriteSums(t, root)
	compiled, err := service.Build(ctx, build.Request{Dir: root})
	if err != nil {
		t.Fatal(err)
	}
	db := sqlite.New(t)
	if err = db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER)", "INSERT INTO records VALUES(1),(2)"); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(ctx, compiled.Binary, "transcribe", "get", "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), "example.com/linkedauthor/source")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("custom-linked authoring CLI: %v\n%s", err, output)
	}
	input, err := os.ReadFile(filepath.Join(root, "api/records/input.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(input), "Threshold") {
		t.Fatalf("custom predicate identity dropped by transcription: %s", input)
	}
	for _, name := range []string{"run", "start"} {
		output, err := exec.CommandContext(ctx, compiled.Binary, name, "-h").CombinedOutput()
		if err != nil || !strings.Contains(string(output), "-conf") {
			t.Fatalf("custom runtime command changed: %s %v\n%s", name, err, output)
		}
	}
}
