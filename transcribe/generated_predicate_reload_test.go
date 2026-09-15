package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/typecatalog"
)

// The subprocess links the emitted contracts and authored predicate once. Reload
// changes persisted SQL/metadata with a stable Go ABI, as application.Manager's
// trusted stage callback requires; it does not compile arbitrary Go at runtime.
func TestGeneratedCustomPredicatePublicReloadSQLite(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	module := "github.com/viant/datly/testfixture/predicatereload"
	var stages []string
	var contracts map[string]string
	for revision := 1; revision <= 2; revision++ {
		stage := filepath.Join(root, fmt.Sprint(revision))
		if err := os.MkdirAll(stage, 0755); err != nil {
			t.Fatal(err)
		}
		(testharness.GeneratedModule{Path: module}).Write(t, stage)
		// No #package or persisted DQL: reload must recover the emitted
		// Go package's destination from its existing ownership manifest.
		source := &Source{Scope: module + "/records", Name: "Records", Connector: "main", Types: typecatalog.NewCatalog(), Text: fmt.Sprintf(`#setting($_ = $route('/records','GET'))
#setting($_ = $mcp('records.query'))
#define($_ = $Minimum<int>(query/min).WithTag('json:"minimum"').Required().WithStatusCode(422).WithErrorMessage('minimum required').WithPredicate(0,'handler','%s/records.Threshold'))
SELECT id, CAST(records.id AS int) FROM records
WHERE id <= %d
${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("AND")}
ORDER BY id`, module, revision+2)}
		compiler := NewCompiler()
		compiled, err := compiler.Compile(ctx, source)
		if err != nil {
			t.Fatal(err)
		}
		input, dir, err := generationInput(stage, "records", compiled)
		if err != nil {
			t.Fatal(err)
		}
		input.SQLResources = true
		generated, err := compiler.generateInputAt(ctx, stage, dir, compiled, input)
		if err != nil {
			t.Fatal(err)
		}
		dir = filepath.Join(stage, dir)
		plan := generated.Result.Plan
		got := map[string]string{}
		for _, name := range []string{plan.Input.Destination, plan.Output.Destination, plan.ViewDest, plan.RouterDest} {
			data, err := os.ReadFile(filepath.Join(dir, name))
			if err != nil {
				t.Fatal(err)
			}
			got[name] = string(data)
		}
		if revision == 1 {
			contracts = got
		} else {
			for name, data := range got {
				if data != contracts[name] {
					t.Fatalf("Go contract %s changed between resource generations", name)
				}
			}
		}
		for name, path := range map[string]string{"predicate.go": "testdata/predicate_reload/predicate.go.txt", "reload_test.go": "testdata/predicate_reload/reload_test.go.txt"} {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
				t.Fatal(err)
			}
		}
		stages = append(stages, stage)
	}
	for _, protocol := range []string{"http", "mcp-registry", "mcp-session", "mcp-stateless"} {
		t.Run(protocol, func(t *testing.T) {
			command := exec.Command("go", "test", "-mod=mod", "-race", "-count=1", "-timeout=120s", "-v", "-run", "^TestPublicReload$/^"+protocol+"$", "./records")
			command.Dir = stages[0]
			command.Env = append(os.Environ(), "DATLY_RELOAD_STAGE="+stages[1], "GOWORK=off")
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("generated predicate public reload: %v\n%s", err, output)
			}
			if !strings.Contains(string(output), "TestPublicReload/"+protocol) {
				t.Fatalf("fixture did not execute: %s", output)
			}
			t.Logf("compiled emitted package acceptance:\n%s", output)
		})
	}
}
