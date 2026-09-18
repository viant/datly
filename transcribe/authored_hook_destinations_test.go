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
	"github.com/viant/datly/project/build"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
)

func TestAuthoredHookDestinations(t *testing.T) {
	for _, tc := range []struct {
		name                string
		rootHook, childHook bool
		invalid             string
	}{{"root", true, false, ""}, {"child", false, true, ""}, {"root_and_child", true, true, ""}, {"stale_root_identity", true, false, "root"}, {"stale_child_parent", false, true, "child"}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE EVENTS(ID INTEGER PRIMARY KEY AUTOINCREMENT,NAME TEXT NOT NULL)", "CREATE TABLE ITEMS(ID INTEGER PRIMARY KEY AUTOINCREMENT,EVENT_ID INTEGER,NAME TEXT NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			writeSourceFile(t, root, "hooks/root/hooks.go", authoredRootHookSource)
			writeSourceFile(t, root, "hooks/child/hooks.go", authoredChildHookSource)
			if tc.invalid == "root" {
				writeSourceFile(t, root, "hooks/root/hooks.go", strings.ReplaceAll(authoredRootHookSource, "example.com/generated/entities", "example.com/generated/api/events"))
			}
			if tc.invalid == "child" {
				writeSourceFile(t, root, "hooks/child/hooks.go", strings.ReplaceAll(authoredChildHookSource, "example.com/generated/entities", "example.com/generated/api/events"))
			}
			control := ""
			if tc.rootHook {
				control += ",lifecycle_type(e,'rh.RootHooks')"
			}
			if tc.childHook {
				control += ",lifecycle_type(i,'ch.ChildHooks')"
			}
			source := &Source{Name: "Events", Scope: "authored-hooks", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#package('api/events')
#import('contracts','example.com/generated/contracts')
#import('rows','example.com/generated/entities')
#import('items','example.com/generated/items')
#import('rh','example.com/generated/hooks/root')
#import('ch','example.com/generated/hooks/child')
#setting($_ = $route('/events','POST'))
#setting($_ = $input_type('contracts.Request'))
#setting($_ = $output_type('contracts.Response'))
#define($_ = $Events<[]*rows.Event>(body/Data).Cardinality('Many').Required())
#define($_ = $Data<[]*rows.Event>(output/body))
SELECT e.*,i.*,type(e,'rows.Event'),dest(e,'event.go'),type(i,'items.Item'),dest(i,'item.go'),CAST(e.ID AS *int64),CAST(e.NAME AS string),CAST(i.ID AS *int64),CAST(i.EVENT_ID AS *int64),CAST(i.NAME AS string)` + control + `
FROM EVENTS e LEFT JOIN ITEMS i ON e.ID=i.EVENT_ID`}
			for iteration := 0; iteration < 2; iteration++ {
				if iteration > 0 {
					writeSourceFile(t, root, "hooks/root/hooks.go", authoredRootHookSource+"\n// authored root edit preserved\n")
					writeSourceFile(t, root, "hooks/child/hooks.go", strings.ReplaceAll(authoredChildHookSource, "child-v1", "child-v2")+"\n// authored child edit preserved\n")
				}
				catalog := typecatalog.NewCatalog()
				for _, directory := range []string{"hooks/root", "hooks/child"} {
					pkg, err := loaderast.LoadPackageFS(ctx, os.DirFS(root), directory)
					if err != nil {
						t.Fatal(err)
					}
					if err = catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
						t.Fatal(err)
					}
				}
				source.Types = catalog
				generated, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePost, Go: GoHandlerOptions{Execution: GoExecutionMutation}, Hooks: HookOptions{Scaffold: true}}}})
				if tc.invalid != "" {
					if err == nil || !strings.Contains(err.Error(), "incompatible signature") {
						t.Fatalf("stale package identity accepted: %v", err)
					}
					for _, directory := range []string{"api/events", "entities", "items"} {
						if _, statErr := os.Stat(filepath.Join(root, directory)); !os.IsNotExist(statErr) {
							t.Fatal("invalid hook emitted artifacts", statErr)
						}
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if generated.Result.Plan.HookScaffold != nil {
					t.Fatal("authored lifecycle inferred an additional scaffold")
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
					t.Fatal("child holder absent")
				}
				runtimeSource := generatedGoWriteRuntimeSource(WritePost)
				runtimeSource = strings.NewReplacer("EventsInput", "Request", "EventsOutput", "Response").Replace(runtimeSource)
				lifecycleMetadata := `component.TypeContext=&spec.TypeContext{Imports:[]spec.ImportSpec{{Alias:"rh",Package:"example.com/generated/hooks/root"},{Alias:"ch",Package:"example.com/generated/hooks/child"}}}`
				if tc.rootHook {
					lifecycleMetadata += `;component.RootView.EntityHooks="rh.RootHooks"`
				}
				runtimeSource = strings.Replace(runtimeSource, "artifact, err :=", lifecycleMetadata+"\n\tartifact, err :=", 1)
				runtimeSource = strings.Replace(runtimeSource, "defer db.Close()", "defer db.Close()\n if _,err=db.Exec(\"CREATE TABLE ITEMS(ID INTEGER PRIMARY KEY AUTOINCREMENT,EVENT_ID INTEGER,NAME TEXT NOT NULL)\");err!=nil{t.Fatal(err)}", 1)
				runtimeSource = strings.Replace(runtimeSource, `{"Data":[{"name":"one"},{"name":"two"}]}`, `{"Data":[{"name":"one","`+holder+`":[{"name":"first"}]},{"name":"two","`+holder+`":[{"name":"second"}]}]}`, 1)
				runtimeSource = strings.Replace(runtimeSource, `"/events", strings.NewReader`, `"/events?suffix=authored", strings.NewReader`, 1)
				rootWant, childWant := "one,two", "first,second"
				if tc.rootHook {
					rootWant = "one:authored,two:authored"
				}
				if tc.childHook {
					suffix := fmt.Sprintf(":child-v%d", iteration+1)
					childWant = "first" + suffix + ",second" + suffix
				}
				extra := fmt.Sprintf(`var names string
 if err=db.QueryRowContext(ctx,"SELECT GROUP_CONCAT(NAME,',') FROM (SELECT NAME FROM EVENTS ORDER BY ID)").Scan(&names);err!=nil||names!=%q{t.Fatalf("root hooks: %%q %%v",names,err)}
 if err=db.QueryRowContext(ctx,"SELECT GROUP_CONCAT(NAME,',') FROM (SELECT NAME FROM ITEMS ORDER BY ID)").Scan(&names);err!=nil||names!=%q{t.Fatalf("child hooks: %%q %%v",names,err)}
 var childCount int
 if err=db.QueryRowContext(ctx,"SELECT COUNT(*) FROM ITEMS i JOIN EVENTS e ON e.ID=i.EVENT_ID").Scan(&childCount);err!=nil||childCount!=2{t.Fatalf("child relation rows=%%d err=%%v",childCount,err)}
 var count int`, rootWant, childWant)
				if tc.rootHook {
					runtimeSource = strings.Replace(runtimeSource, `"context"`, `"context";rh "example.com/generated/hooks/root"`, 1)
					extra = `if rh.InitCalls!=2||rh.ValidateCalls!=2||rh.FinalizeCalls!=1{t.Fatalf("root calls %d/%d/%d",rh.InitCalls,rh.ValidateCalls,rh.FinalizeCalls)}` + "\n" + extra
				}
				if tc.childHook {
					runtimeSource = strings.Replace(runtimeSource, `"context"`, `"context";ch "example.com/generated/hooks/child"`, 1)
					extra = `if ch.InitCalls!=2||ch.ValidateCalls!=2{t.Fatalf("child calls %d/%d",ch.InitCalls,ch.ValidateCalls)}` + "\n" + extra
				}
				runtimeSource = strings.Replace(runtimeSource, "var count int", extra, 1)
				writeSourceFile(t, root, "api/events/runtime_test.go", runtimeSource)
				cmd := exec.Command("go", "test", "-mod=mod", "-count=1", "-race", "./...")
				cmd.Dir = root
				cmd.Env = os.Environ()
				if output, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("authored hooks runtime: %v\n%s", err, output)
				}
				if iteration > 0 && tc.rootHook && tc.childHook {
					(testharness.GeneratedModule{}).WriteSums(t, root)
					service := build.Service{}
					if err := service.Init(ctx, build.InitRequest{Dir: root}); err != nil {
						t.Fatal(err)
					}
					built, err := service.Build(ctx, build.Request{Dir: root, Env: os.Environ()})
					if err != nil {
						t.Fatalf("automatic hook build: %v", err)
					}
					if built.Components != 1 {
						t.Fatalf("automatic hook discovery: %+v", built)
					}
				}
				if iteration > 0 {
					for _, name := range []string{"root", "child"} {
						data, err := os.ReadFile(filepath.Join(root, "hooks", name, "hooks.go"))
						if err != nil || !strings.Contains(string(data), "authored "+name+" edit preserved") {
							t.Fatal("hook edits lost", err)
						}
					}
				}
			}
		})
	}
}

const authoredRootHookSource = `package roothooks
import (
 "context"
 "fmt"
 entities "example.com/generated/entities"
 contracts "example.com/generated/contracts"
 h "github.com/viant/xdatly/handler"
)
type RootHooks struct {Suffix string ` + "`parameter:\"Suffix,kind=query,in=suffix\"`" + `}
var InitCalls,ValidateCalls,FinalizeCalls int
func(hook *RootHooks)Init(ctx context.Context,row *entities.Event,state h.LifecycleContext[entities.Event,h.NoParent,contracts.Response])error{
 if hook.Suffix!="authored" || state.Original==nil || !state.Original.Has("Name") {return fmt.Errorf("root hook binding or original presence missing")}
 InitCalls++;row.SetName(row.Name+":"+hook.Suffix);return nil
}
func(*RootHooks)Validate(ctx context.Context,row *entities.Event,state h.LifecycleContext[entities.Event,h.NoParent,contracts.Response])error{ValidateCalls++;return nil}
func(*RootHooks)Finalize(ctx context.Context,input *contracts.Request,output *contracts.Response,outcome h.Outcome)error{
 if input==nil||output==nil||len(output.Data)!=2{return fmt.Errorf("root completion contracts missing")};FinalizeCalls++;return nil
}
`
const authoredChildHookSource = `package childhooks
import (
 "context"
 "fmt"
 contracts "example.com/generated/contracts"
 entities "example.com/generated/entities"
 items "example.com/generated/items"
 h "github.com/viant/xdatly/handler"
)
type ChildHooks struct{}
var InitCalls,ValidateCalls int
func(*ChildHooks)Init(ctx context.Context,row *items.Item,state h.LifecycleContext[items.Item,entities.Event,contracts.Response])error{
 if state.Parent==nil||state.Parent.Name==""||state.Original==nil||!state.Original.Has("Name"){return fmt.Errorf("typed child parent or original presence missing")}
 InitCalls++;row.SetName(row.Name+":child-v1");return nil
}
func(*ChildHooks)Validate(ctx context.Context,row *items.Item,state h.LifecycleContext[items.Item,entities.Event,contracts.Response])error{ValidateCalls++;return nil}
`
