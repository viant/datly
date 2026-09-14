package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
)

func TestTranscribedMutationProgramSQLite(t *testing.T) {
	for _, operation := range []WriteOperation{WritePost, WritePut, WritePatch} {
		t.Run(string(operation), func(t *testing.T) {
			ctx := context.Background()
			harness := testharness.NewSQLiteHarness(t)
			if err := harness.ExecStatements(ctx, "CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			current := ""
			typeExpr, cardinality := "[]*EventsView", ".Cardinality('Many')"
			if operation == WritePut {
				typeExpr, cardinality = "*EventsView", ""
			}
			intent := HandlerOptions{Target: HandlerGo, Operation: operation, Go: GoHandlerOptions{Execution: GoExecutionMutation}}
			if operation == WritePatch || operation == WritePut {
				intent.Current = "CurrentEvents"
				current = `#define($_ = $CurrentEvents<?>(view/CurrentEvents).Cardinality('Many') /*
SELECT ID, NAME FROM EVENTS
WHERE ID IN (#foreach($event in $Events)$event.Id#if($foreach.HasNext),#end#end)
*/)`
				if operation == WritePut {
					current = strings.Replace(current, "ID IN (#foreach($event in $Events)$event.Id#if($foreach.HasNext),#end#end)", "ID = $Events.Id", 1)
				}
			}
			dql := `#setting($_ = $route('/events', '` + strings.ToUpper(string(operation)) + `'))
#define($_ = $Events<` + typeExpr + `>(body/Data)` + cardinality + `.Required())
` + current + `
#define($_ = $Status<int>(output/status).Output())
#define($_ = $Data<` + typeExpr + `>(output/body))
SELECT ID, NAME FROM EVENTS`
			generated, err := NewCompiler().Transcribe(ctx, Request{Source: &Source{
				Scope: "example.com/generated/events", Name: "Events", Connector: "main", Text: dql,
				Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": harness.DB}),
			}, Destination: root, Options: Options{Handler: intent}})
			if err != nil {
				t.Fatal(err)
			}
			if generated.Result.Plan.MutationHandler == nil || generated.Result.Plan.ContractHandler != nil {
				t.Fatal("generic mutation policy was not persisted")
			}
			source := strings.ReplaceAll(generatedGoWriteRuntimeSource(operation, operation != WritePost), "github.com/viant/datly/runtime/handler/custom", "github.com/viant/datly/runtime/handler/mutation")
			source = strings.ReplaceAll(source, "customhandler", "mutationhandler")
			if err = os.WriteFile(filepath.Join(root, "generated", "program_runtime_test.go"), []byte(source), 0644); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("go", "test", "-mod=mod", "-race", "-timeout", "45s", "./...")
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("transcribed mutation policy: %v\n%s", err, output)
			}
		})
	}
}
