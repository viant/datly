package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
)

func TestTranscribeGetHandlerArguments(t *testing.T) {
	for _, variadic := range []bool{false, true} {
		t.Run(fmt.Sprint(variadic), func(t *testing.T) {
			root := t.TempDir()
			const module = "example.com/predicateargs"
			(testharness.GeneratedModule{Path: module}).Write(t, root)
			db := sqlite.New(t)
			if err := db.ExecStatements(context.Background(), "CREATE TABLE records(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			for _, dir := range []string{"source", "security"} {
				if err := os.MkdirAll(filepath.Join(root, dir), 0755); err != nil {
					t.Fatal(err)
				}
			}
			signature, options, contract := "", "", "Handler"
			if variadic {
				signature = ", args ...any"
				options = ",'alias','r','enabled',true,'limit',7"
				contract = "VariadicHandler"
			}
			handler := `package security
import("context"; "github.com/viant/xdatly/predicate")
type Scope struct{}
var _ predicate.` + contract + ` = (*Scope)(nil)
func(*Scope)Compute(ctx context.Context,value any` + signature + `)(*predicate.Criteria,error){return &predicate.Criteria{Expression:"r.id >= ?",Placeholders:[]any{value}},nil}
`
			if err := os.WriteFile(filepath.Join(root, "security/predicate.go"), []byte(handler), 0644); err != nil {
				t.Fatal(err)
			}
			dql := `#package('api/records')
#import('security','example.com/predicateargs/security')
#setting($_ = $route('/records','GET'))
#define($_ = $Minimum<int>(query/min).WithPredicate(0,'handler','security.Scope'` + options + `))
SELECT records.*, CAST(records.id AS int)
FROM (SELECT r.id FROM records r ${predicate.Builder().CombineAnd($predicate.FilterGroup(0,"AND")).Build("WHERE")}) records`
			source := filepath.Join(root, "source/Records.dql")
			if err := os.WriteFile(source, []byte(dql), 0644); err != nil {
				t.Fatal(err)
			}
			args := []string{"transcribe", "get", "-dir", root, "-schema", "-connector", "main", "-driver", "sqlite3", "-dsn", filepath.Join(db.TempDir, "test.db"), module + "/source"}
			var previous []byte
			for i := 0; i < 2; i++ {
				var out, diagnostic bytes.Buffer
				if code := run(context.Background(), args, &out, &diagnostic); code != 0 {
					t.Fatalf("transcribe get exit %d: %s %s", code, out.String(), diagnostic.String())
				}
				input, err := os.ReadFile(filepath.Join(root, "api/records/input.go"))
				if err != nil {
					t.Fatal(err)
				}
				if i > 0 && !bytes.Equal(previous, input) {
					t.Fatal("regeneration changed input contract")
				}
				previous = input
				if variadic && !strings.Contains(string(input), "handlerArgs=") {
					t.Fatalf("typed arguments not emitted: %s", input)
				}
			}
			retained, err := os.ReadFile(source)
			if err != nil || string(retained) != dql {
				t.Fatalf("authored source changed: %v", err)
			}
			command := exec.Command("go", "test", "-mod=mod", "./...")
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("generated old/new contract compilation: %v\n%s", err, output)
			}
		})
	}
}
