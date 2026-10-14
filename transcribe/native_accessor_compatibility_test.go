package transcribe

import (
	"bytes"
	"context"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/transcribe/gobuild"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeWriterPreservesScalarConvenienceSetter(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	request := Request{Source: &Source{Name: "Records", Scope: "example.com/records", GoBuild: &gobuild.Context{Dir: root}, Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#setting($_ = $route('/records','POST'))
#define($_ = $Records<[]*Record>(body/data).Cardinality('Many'))
#define($_ = $Data<[]*Record>(output/body))
SELECT r.*,type(r,'Record') FROM records r`}, Destination: root, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePost, Go: GoHandlerOptions{Execution: GoExecutionMutation}}}}
	if _, err := NewCompiler().Transcribe(ctx, request); err != nil {
		t.Fatal(err)
	}
	authored := `package records
func(r *Record) SetLabel(value string){r.Label=&value;if r.Has==nil{r.Has=&RecordHas{}};r.Has.Label=true}
`
	path := filepath.Join(root, "generated", "record.go")
	if err := os.WriteFile(path, []byte(authored), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewCompiler().Transcribe(ctx, request); err != nil {
		t.Fatal(err)
	}
	snapshot := func() map[string][]byte {
		t.Helper()
		out := map[string][]byte{}
		err := filepath.WalkDir(filepath.Join(root, "generated"), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			out[path], err = os.ReadFile(path)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	unchanged := func(before map[string][]byte) {
		t.Helper()
		after := snapshot()
		if len(before) != len(after) {
			t.Fatal("artifact set changed")
		}
		for name, value := range before {
			if !bytes.Equal(value, after[name]) {
				t.Fatalf("artifact changed: %s", name)
			}
		}
	}
	stable := snapshot()
	if _, err := NewCompiler().Transcribe(ctx, request); err != nil {
		t.Fatal(err)
	}
	unchanged(stable)
	for _, tc := range []struct {
		name, source string
		withoutBuild bool
	}{
		{"bad body", strings.Replace(authored, "r.Label=&value", "r.Label=value", 1), false},
		{"bad caller", authored + "func originalCaller(){var value *string; (&Record{}).SetLabel(value)}\n", false},
		{"no build context", authored, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(tc.source), 0600); err != nil {
				t.Fatal(err)
			}
			before := snapshot()
			candidate := request
			source := *request.Source
			candidate.Source = &source
			if tc.withoutBuild {
				source.GoBuild = nil
			}
			_, err := NewCompiler().Transcribe(ctx, candidate)
			if err == nil {
				t.Fatal("invalid accessor publication succeeded")
			}
			expected := "cannot use"
			if tc.withoutBuild {
				expected = "authored accessor"
			}
			if !strings.Contains(err.Error(), expected) {
				t.Fatalf("unexpected rejection: %v", err)
			}
			unchanged(before)
		})
	}
	if err := os.WriteFile(path, []byte(authored), 0600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != authored {
		t.Fatal("original authored setter changed")
	}
	test := `package records
import "testing"
func TestOriginalCaller(t *testing.T){r:=&Record{};r.SetLabel("");if r.Label==nil||*r.Label!=""||r.Has==nil||!r.Has.Label{t.Fatal("zero scalar pointer presence changed")}}
`
	if err := os.WriteFile(filepath.Join(root, "generated", "original_caller_test.go"), []byte(test), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "consumer"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "consumer", "original.go"), []byte(`package consumer
import records "example.com/generated/generated"
func OriginalCall(){r:=&records.Record{};r.SetLabel("")}
`), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "-timeout", "45s", "./...")
	command.Dir = root
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("original caller: %v\n%s", err, output)
	}
}

func TestNativeConvenienceAccessorRejectsSplitDestinations(t *testing.T) {
	ctx := context.Background()
	db := sqlite.New(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE records(id INTEGER PRIMARY KEY,label TEXT)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	source := &Source{Name: "Records", Scope: "example.com/records", Connector: "main", GoBuild: &gobuild.Context{Dir: root}, ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#import('rows','example.com/generated/entities')
#setting($_ = $route('/records','POST'))
#define($_ = $Records<[]*rows.Record>(body/data).Cardinality('Many'))
#define($_ = $Data<[]*rows.Record>(output/body))
SELECT r.*,type(r,'rows.Record') FROM records r`}
	request := Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePost, Go: GoHandlerOptions{Execution: GoExecutionMutation}}}}
	if _, err := NewCompiler().Transcribe(ctx, request); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "entities", "record_accessors.go")
	if err := os.WriteFile(path, []byte("package entities\nfunc(r *Record)SetLabel(value string){r.Label=&value;if r.Has==nil{r.Has=&RecordHas{}};r.Has.Label=true}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot := func() map[string][]byte {
		t.Helper()
		out := map[string][]byte{}
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			out[path], err = os.ReadFile(path)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	before := snapshot()
	_, err := NewCompiler().Transcribe(ctx, request)
	if err == nil || !strings.Contains(err.Error(), "single generated destination package") {
		t.Fatalf("split accessor was not rejected: %v", err)
	}
	after := snapshot()
	if len(before) != len(after) {
		t.Fatal("rejected split generation changed artifacts")
	}
	for name, data := range before {
		if !bytes.Equal(data, after[name]) {
			t.Fatalf("rejected split generation changed %s", name)
		}
	}
}
