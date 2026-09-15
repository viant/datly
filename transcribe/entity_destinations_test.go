package transcribe

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/project/build"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"

	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
)

func TestGeneratedEntityDestinations(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options HandlerOptions
	}{
		{"reader", HandlerOptions{}},
		{"Go", HandlerOptions{Target: HandlerGo, Operation: WritePost, Hooks: HookOptions{Scaffold: true}}},
		{"GoSplit", HandlerOptions{Target: HandlerGo, Operation: WritePost, Hooks: HookOptions{Scaffold: true}}},
		{"Velty", HandlerOptions{Target: HandlerVelty, Operation: WritePost}},
		{"mutation", HandlerOptions{Target: HandlerGo, Operation: WritePost, Go: GoHandlerOptions{Execution: GoExecutionMutation}, Hooks: HookOptions{Scaffold: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE EVENTS(ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL, EXTRA INTEGER)"); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/testfixture/entitydest"}).Write(t, root)
			verb, body := "GET", ""
			if tc.options.Target != "" {
				verb = "POST"
				body = "#define($_ = $Events<[]*rows.Event>(body/Data).Cardinality('Many').Required())\n"
			}
			header := fmt.Sprintf(`#package('api/events')
#import('contracts','example.com/generated/contracts')
#import('rows','example.com/generated/entities')
#setting($_ = $route('/events','%s'))
#setting($_ = $input_type('contracts.Request'))
#setting($_ = $output_type('contracts.Response'))
%s`, verb, body)
			if tc.options.Target != "" {
				header += "#define($_ = $Status<int>(output/status).Output())\n#define($_ = $Data<[]*rows.Event>(output/body))\n"
			}
			if tc.name == "GoSplit" {
				header = "#import('requests','example.com/generated/requests')\n" + strings.Replace(header, "contracts.Request", "requests.Request", 1)
			}
			header = strings.ReplaceAll(header, "example.com/generated", "github.com/viant/datly/testfixture/entitydest")
			catalog := typecatalog.NewCatalog()
			transcribe := func(projection string) (*GeneratedPackage, error) {
				if tc.options.Target != "" {
					projection += `,tag(e.NAME,'invariant:"Details"')`
					if strings.Contains(projection, "e.EXTRA") {
						projection += `,tag(e.EXTRA,'invariant:"Details"')`
					}
				}
				return NewCompiler().Transcribe(ctx, Request{Source: &Source{Scope: "test", Name: "Events", Text: header + "SELECT " + projection + ",CAST(e.NAME AS string),CAST(e.ID AS *int64),type(e,'rows.Event'),dest(e,'event.go') FROM EVENTS e", Connector: "main", Types: catalog, ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB})}, Destination: root, Options: Options{Handler: tc.options}})
			}
			run := func(projection string) *GeneratedPackage {
				t.Helper()
				result, err := transcribe(projection)
				if err != nil {
					t.Fatal(err)
				}
				return result
			}

			run("e.ID,e.NAME,e.EXTRA,CAST(e.EXTRA AS int)")
			var lifecyclePath, lifecycleText string
			if tc.options.Target == HandlerGo && tc.options.Go.Execution != GoExecutionMutation {
				lifecycleDir := "contracts"
				if tc.name == "GoSplit" {
					lifecycleDir = "requests"
				}
				lifecyclePath = filepath.Join(root, lifecycleDir, "events_hooks.go")
				data, err := os.ReadFile(lifecyclePath)
				if err != nil {
					t.Fatal(err)
				}
				lifecycleText = string(data) + "\n// retained lifecycle customization\n"
				if err = os.WriteFile(lifecyclePath, []byte(lifecycleText), 0600); err != nil {
					t.Fatal(err)
				}
			}
			shape := filepath.Join(root, "entities/event.go")
			original, err := os.ReadFile(shape)
			if err != nil {
				t.Fatal(err)
			}
			edited, err := (xshape.SourceParser{}).AppendStructFields(original, []byte("package entities\ntype Event struct {AuthoredNote string `sqlx:\"-\" json:\"-\"`}\n"))
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(shape, edited, 0600); err != nil {
				t.Fatal(err)
			}
			custom := "package entities\nfunc(e *Event) Authored()string{return e.AuthoredNote}\n"
			if tc.options.Target != "" {
				custom += "func(e *Event) SetName(v string){e.Name=v;if e.Has==nil{e.Has=&EventHas{}};e.Has.Name=true;e.AuthoredNote=\"setter preserved\"}\n"
			}
			method := filepath.Join(root, "entities/authored.go")
			if err = os.WriteFile(method, []byte(custom), 0600); err != nil {
				t.Fatal(err)
			}
			run("e.ID,e.NAME,e.EXTRA,CAST(e.EXTRA AS *int)")
			data, err := os.ReadFile(shape)
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := (xshape.SourceParser{}).Parse(data)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, field := range parsed.Fields {
				if field.Owner == "Event" && reflect.DeepEqual(field.Names, []string{"Extra"}) {
					found = field.TypeExpr == "*int"
				}
			}
			if !found {
				t.Fatal("CAST did not update relocated entity")
			}
			if tc.options.Target != "" {
				testPath := filepath.Join(root, "entities/invariant_test.go")
				if err = os.WriteFile(testPath, []byte(entityDestinationInvariant), 0600); err != nil {
					t.Fatal(err)
				}
				cmd := exec.Command("go", "test", "-mod=mod", "-race", "./entities")
				cmd.Dir = root
				cmd.Env = append(os.Environ(), "GOWORK=off")
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("relocated invariant: %v\n%s", err, out)
				}
				if err = os.Remove(testPath); err != nil {
					t.Fatal(err)
				}
			}
			result := run("e.ID,e.NAME")
			before, err := os.ReadFile(shape)
			if err != nil {
				t.Fatal(err)
			}
			run("e.ID,e.NAME")
			after, err := os.ReadFile(shape)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) || !strings.Contains(string(after), "AuthoredNote") || strings.Contains(string(after), "Extra ") {
				t.Fatal("regeneration lost edits, retained dropped field or changed stable output")
			}
			if lifecyclePath != "" {
				data, err := os.ReadFile(lifecyclePath)
				if err != nil || string(data) != lifecycleText {
					t.Fatal("relocated lifecycle hook edits changed")
				}
			}
			after, err = os.ReadFile(method)
			if err != nil || string(after) != custom {
				t.Fatal("authored methods changed")
			}
			runtimeSource := entityDestinationReader
			if tc.options.Target != "" {
				runtimeSource = generatedWriteRuntimeSource(WritePost)
				if tc.options.Target == HandlerGo {
					runtimeSource = generatedGoWriteRuntimeSource(WritePost)
				}
				if tc.options.Go.Execution == GoExecutionMutation {
					runtimeSource = strings.ReplaceAll(runtimeSource, "runtime/handler/custom", "runtime/handler/mutation")
				}
				runtimeSource = strings.NewReplacer("EventsInput", "Request", "EventsOutput", "Response").Replace(runtimeSource)
				runtimeSource += `
func TestEntityMethods(t *testing.T){e:=&Event{};e.SetName("changed");if e.GetName()!="changed"||e.Authored()!="setter preserved"||e.Has==nil||!e.Has.Name{t.Fatal("entity setters/markers lost")}}
`
			}
			testFile := filepath.Join(root, "api/events/runtime_test.go")
			if err = os.WriteFile(testFile, []byte(runtimeSource), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("go", "test", "-mod=mod", "-race", "./...")
			cmd.Dir = root
			cmd.Env = append(os.Environ(), "GOWORK=off")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("runtime: %v\n%s", err, out)
			}
			if tc.options.Go.Execution == GoExecutionMutation {
				(testharness.GeneratedModule{}).WriteSums(t, root)
				service := build.Service{}
				if err = service.Init(ctx, build.InitRequest{Dir: root}); err != nil {
					t.Fatal(err)
				}
				built, err := service.Build(ctx, build.Request{Dir: root, Env: append(os.Environ(), "GOWORK=off")})
				if err != nil {
					t.Fatal(err)
				}
				if built.Components != 1 || built.Factories != 1 {
					t.Fatalf("automatic build missed generated writer: %+v", built)
				}
			}
			reloaded, err := (&Discovery{BaseDir: root, Include: []string{"github.com/viant/datly/testfixture/entitydest/api/events"}}).Compile(ctx)
			if err != nil || len(reloaded.Components) != 1 {
				t.Fatalf("generated package reload: %v", err)
			}
			if tc.options.Target != "" {
				for _, shapePlan := range result.Result.Plan.ShapePackages {
					if shapePlan.Package == "github.com/viant/datly/testfixture/entitydest/entities" && shapePlan.EntitySupport == nil {
						t.Fatal("entity method package missing")
					}
				}
			}
			if tc.options.Target != "" {
				methodFile := filepath.Join(root, "entities/events_entity_methods_gen.go")
				generatedBytes, err := os.ReadFile(methodFile)
				if err != nil {
					t.Fatal(err)
				}
				customBytes := append(append([]byte(nil), generatedBytes...), []byte("\n// direct edit must not be overwritten\n")...)
				if err = os.WriteFile(methodFile, customBytes, 0600); err != nil {
					t.Fatal(err)
				}
				if _, err = transcribe("e.ID,e.NAME"); err == nil || !strings.Contains(err.Error(), "manually changed") {
					t.Fatalf("edited method artifact accepted: %v", err)
				}
				after, _ := os.ReadFile(methodFile)
				if string(after) != string(customBytes) {
					t.Fatal("edited method artifact lost")
				}
				if err = os.WriteFile(methodFile, generatedBytes, 0600); err != nil {
					t.Fatal(err)
				}
				cyclic := strings.Replace(custom, "package entities", "package entities\nimport api \"github.com/viant/datly/testfixture/entitydest/api/events\"\nvar _ = api.EventsComponent{}", 1)
				if err = os.WriteFile(method, []byte(cyclic), 0600); err != nil {
					t.Fatal(err)
				}
				if _, err = transcribe("e.ID,e.NAME"); err == nil || !strings.Contains(err.Error(), "cycle") {
					t.Fatalf("entity import cycle accepted: %v", err)
				}
				after, _ = os.ReadFile(method)
				if string(after) != cyclic {
					t.Fatal("cycle failure changed authored code")
				}
			}

		})
	}
}

const entityDestinationReader = `package events
import (
 "context"
 "reflect"
 "testing"
 "github.com/viant/bindly/resource"
 "github.com/viant/datly/bootstrap"
 "github.com/viant/datly/internal/testharness/sqlite"
 "github.com/viant/datly/spec"
 dsql "github.com/viant/datly/sql"
 "github.com/viant/datly/sql/reader"
)
func TestGeneratedReader(t *testing.T){
 ctx:=context.Background();h:=sqlite.New(t)
 if err:=h.ExecStatements(ctx,"CREATE TABLE EVENTS(ID INTEGER PRIMARY KEY,NAME TEXT)","INSERT INTO EVENTS VALUES(1,'read')");err!=nil{t.Fatal(err)}
 resources:=resource.New();if err:=resources.Register(EventsDatlyResourceNamespace,EventsDatlyResources);err!=nil{t.Fatal(err)}
 component:=&spec.Component{Name:"Events",Key:spec.Key{Kind:spec.KindComponent,Name:"Events"},Routes:[]*spec.Route{{Method:"GET",Path:"/events"}}}
 artifact,err:=bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component:component,InputType:reflect.TypeOf(Request{}),OutputType:reflect.TypeOf(Response{}),Resources:resources});if err!=nil{t.Fatal(err)}
 execution,err:=reader.NewExecution(reader.Config{Component:artifact.Component,InputType:reflect.TypeOf(Request{}),OutputType:reflect.TypeOf(Response{}),Plan:artifact.Reader,SQL:&dsql.SQLComponent{DB:h.DB}});if err!=nil{t.Fatal(err)}
 actual,err:=execution.Read(ctx,&Request{},nil,nil);if err!=nil{t.Fatal(err)}
 rows:=actual.(*Response).Data
 if len(rows)!=1 || rows[0].Name!="read" {t.Fatalf("typed reader result: %+v",rows)}
}
`

const entityDestinationInvariant = `package entities
import "testing"
func TestRelocatedBackfill(t *testing.T){
 value:=41
 previous:=&Event{Name:"previous",Extra:&value}
 current:=&Event{}
 current.SetName("changed")
 if err:=current.BackfillDetailsIfNeeded(previous,nil);err!=nil{t.Fatal(err)}
 if current.Name!="changed" || current.Extra==nil || *current.Extra!=41 || current.Has.Extra || !current.Has.Name {t.Fatalf("backfill/presence changed: %+v",current)}
 *current.Extra=7
 if *previous.Extra!=41{t.Fatal("backfill aliases the previous value")}
}
`
