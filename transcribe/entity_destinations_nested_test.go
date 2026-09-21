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
)

func TestGeneratedNestedEntityDestinations(t *testing.T) {
	for _, target := range []HandlerTarget{HandlerGo, HandlerVelty} {
		t.Run(string(target), func(t *testing.T) {
			ctx := context.Background()
			h := testharness.NewSQLiteHarness(t)
			if err := h.ExecStatements(ctx, "CREATE TABLE EVENTS(ID INTEGER PRIMARY KEY AUTOINCREMENT,NAME TEXT NOT NULL)", "CREATE TABLE ITEMS(ID INTEGER PRIMARY KEY AUTOINCREMENT,EVENT_ID INTEGER,NAME TEXT NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			source := &Source{Name: "Events", Scope: "nested", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: `#package('api/events')
#import('contracts','example.com/generated/contracts')
#import('rows','example.com/generated/entities')
#import('items','example.com/generated/items')
#setting($_ = $route('/events','POST'))
#setting($_ = $input_type('contracts.Request'))
#setting($_ = $output_type('contracts.Response'))
#define($_ = $Events<[]*rows.Event>(body/Data).Cardinality('Many').Required())
#define($_ = $Data<[]*rows.Event>(output/body))
SELECT e.*,i.*,type(e,'rows.Event'),dest(e,'event.go'),type(i,'items.Item'),dest(i,'item.go'),CAST(e.ID AS *int64),CAST(e.NAME AS string),CAST(i.ID AS *int64),CAST(i.EVENT_ID AS *int64),CAST(i.NAME AS string)
FROM EVENTS e LEFT JOIN ITEMS i ON e.ID=i.EVENT_ID`}
			options := Options{Handler: HandlerOptions{Target: target, Operation: WritePost}}
			if target == HandlerGo {
				options.Handler.Go.Execution = GoExecutionMutation
			}
			var generated *GeneratedPackage
			for i := 0; i < 2; i++ {
				var err error
				generated, err = NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root, Options: options})
				if err != nil {
					for _, name := range []string{"entities/event.go", "entities/.datly-gen.json"} {
						data, _ := os.ReadFile(filepath.Join(root, name))
						t.Logf("%s: %s", name, data)
					}
					t.Fatal(err)
				}
			}
			for _, file := range []string{"entities/entity_methods.go", "items/entity_methods.go"} {
				if _, err := os.Stat(filepath.Join(root, file)); err != nil {
					t.Fatal(err)
				}
			}
			holder := ""
			for _, view := range generated.Result.Plan.Views {
				if view.Name == "Event" {
					for _, field := range view.Fields {
						if strings.Contains(field.Type, "Item") {
							holder = field.Name
						}
					}
				}
			}
			if holder == "" {
				t.Fatal("child holder not found")
			}
			runtimeSource := generatedWriteRuntimeSource(WritePost)
			if target == HandlerGo {
				runtimeSource = generatedGoWriteRuntimeSource(WritePost)
				runtimeSource = strings.ReplaceAll(runtimeSource, "runtime/handler/custom", "runtime/handler/mutation")
			}
			runtimeSource = strings.NewReplacer("EventsInput", "Request", "EventsOutput", "Response").Replace(runtimeSource)
			runtimeSource = strings.Replace(runtimeSource, "defer db.Close()", "defer db.Close()\n if _,err=db.Exec(\"CREATE TABLE ITEMS(ID INTEGER PRIMARY KEY AUTOINCREMENT,EVENT_ID INTEGER,NAME TEXT NOT NULL)\");err!=nil{t.Fatal(err)}", 1)
			runtimeSource = strings.Replace(runtimeSource, `{"Data":[{"name":"one"},{"name":"two"}]}`, `{"Data":[{"name":"one","`+holder+`":[{"name":"first child"}]},{"name":"two","`+holder+`":[{"name":"second child"}]}]}`, 1)
			runtimeSource = strings.Replace(runtimeSource, "var count int", `var childCount int
 if err=db.QueryRowContext(ctx,"SELECT COUNT(*) FROM ITEMS i JOIN EVENTS e ON e.ID=i.EVENT_ID WHERE i.ID IS NOT NULL").Scan(&childCount);err!=nil||childCount!=2{t.Fatalf("cross-package child writes=%d err=%v",childCount,err)}
 var count int`, 1)
			if err := os.WriteFile(filepath.Join(root, "api/events/runtime_test.go"), []byte(runtimeSource), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("go", "test", "-mod=mod", "./...")
			cmd.Dir = root
			cmd.Env = append(os.Environ(), "GOWORK=off")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("nested generated runtime: %v\n%s", err, out)
			}
		})
	}
}
