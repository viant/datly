package transcribe

import (
	"bytes"
	"context"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	tcolumn "github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

func TestMutationHookScaffoldCreateOnceSQLite(t *testing.T) {
	for _, tc := range []struct {
		name, destination, filename string
		existing                    bool
	}{
		{name: "fresh default destination", filename: "lifecycle.go"},
		{name: "enable on existing package", destination: "entity_hooks.go", filename: "entity_hooks.go", existing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE EVENTS(ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)"); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			source := &Source{Name: "Events", Scope: "example.com/generated/events", Connector: "main", Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#setting($_ = $route('/events','POST'))
#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many').Required())
#define($_ = $Status<int>(output/status).Output())
#define($_ = $Data<[]*EventsView>(output/body))
SELECT ID, NAME FROM EVENTS`}
			options := Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePost, Go: GoHandlerOptions{Execution: GoExecutionMutation}}}
			run := func() *GeneratedPackage {
				t.Helper()
				result, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root, Options: options})
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			if tc.existing {
				without := run()
				if without.Result.Plan.HookScaffold != nil {
					t.Fatal("scaffold is not opt-in")
				}
				if _, err := os.Stat(filepath.Join(root, "generated", tc.filename)); !os.IsNotExist(err) {
					t.Fatal("disabled scaffold wrote a file", err)
				}
			}
			options.Handler.Hooks = HookOptions{Scaffold: true, Destination: tc.destination}
			first := run()
			if first.Result.Plan.HookScaffold == nil || len(first.Result.Plan.HookScaffold.EntityHooks) != 1 {
				t.Fatal("typed mutation scaffold missing")
			}
			path := filepath.Join(root, "generated", tc.filename)
			content, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(content), "func (input *") || strings.Contains(string(content), "func (output *") {
				t.Fatal("direct contract lifecycle was scaffolded")
			}
			hookRef, err := (xshape.Resolver{}).Reference(first.Result.Plan.HookScaffold.EntityHooks[0].Type)
			if err != nil {
				t.Fatal(err)
			}
			content, err = (xshape.SourceParser{}).AppendStructFields(content, []byte("package events;type "+hookRef.BaseName+" struct { Input *EventsInput `bind:\"kind=input\"`; InitCount,ValidateCount,SequenceCount,QueueCount int }"))
			if err != nil {
				t.Fatal(err)
			}
			file, err := parser.ParseFile(token.NewFileSet(), "hooks.go", content, parser.ParseComments)
			if err != nil {
				t.Fatal(err)
			}
			checks := map[string]string{
				"Init":          `if hooks.Input==nil { panic("hook was not bound") };if state.Parent!=nil || state.SelfParent!=nil || state.Original==nil {panic("root state authority changed")};hooks.InitCount++;return nil`,
				"Validate":      `if hooks.InitCount==0 { panic("Init did not precede Validate") };hooks.ValidateCount++;return nil`,
				"AfterSequence": `if hooks.ValidateCount!=hooks.InitCount { panic("validation phase incomplete") };hooks.SequenceCount++;return nil`,
				"AfterQueue":    `if hooks.SequenceCount!=hooks.InitCount { panic("sequence phase incomplete") };hooks.QueueCount++;return nil`,
				"Finalize":      `if hooks.Input!=input { panic("finalizer input identity changed") };if outcome.CommitConfirmed() && hooks.QueueCount!=hooks.InitCount {panic("finalized before queue hooks")};return nil`,
			}
			for _, decl := range file.Decls {
				if method, ok := decl.(*ast.FuncDecl); ok {
					body, err := parser.ParseFile(token.NewFileSet(), "body.go", "package p;func f(){scaffoldEvents=append(scaffoldEvents,\""+method.Name.Name+"\");"+checks[method.Name.Name]+"}", 0)
					if err != nil {
						t.Fatal(err)
					}
					method.Body = body.Decls[0].(*ast.FuncDecl).Body
				}
			}
			var text bytes.Buffer
			if err = format.Node(&text, token.NewFileSet(), file); err != nil {
				t.Fatal(err)
			}
			custom := text.String() + "\nvar scaffoldEvents []string\n// application customization remains owned by the author\n"
			if err = os.WriteFile(path, []byte(custom), 0644); err != nil {
				t.Fatal(err)
			}
			run()
			after, err := os.ReadFile(path)
			if err != nil || string(after) != custom {
				t.Fatal("scaffold was overwritten", err)
			}
			manifest, err := os.ReadFile(filepath.Join(root, "generated", ".datly-gen.json"))
			if err != nil || strings.Contains(string(manifest), tc.filename) {
				t.Fatal("user scaffold entered generated ownership", err)
			}
			consumer := strings.ReplaceAll(generatedGoWriteRuntimeSource(WritePost, false), "github.com/viant/datly/runtime/handler/custom", "github.com/viant/datly/runtime/handler/mutation")
			consumer = strings.ReplaceAll(consumer, "customhandler", "mutationhandler")
			consumer = strings.Replace(consumer, "var count int", `counts:=map[string]int{};for _,phase:=range scaffoldEvents{counts[phase]++};for _,phase:=range []string{"Init","Validate","AfterSequence","AfterQueue"}{if counts[phase]!=2{t.Fatalf("scaffold phase %s calls=%d",phase,counts[phase])}};if counts["Finalize"]!=1{t.Fatalf("finalizer calls=%d",counts["Finalize"])}
	var count int`, 1)
			if err = os.WriteFile(filepath.Join(root, "generated", "scaffold_runtime_test.go"), []byte(consumer), 0644); err != nil {
				t.Fatal(err)
			}
			command := exec.Command("go", "test", "-mod=mod", "./...")
			command.Dir = root
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("scaffolded SQLite program: %v\n%s", err, output)
			}
			t.Run("returned plan rejects changed Init before writes", func(t *testing.T) {
				// Retain the real root compiler's independent evidence, then alter
				// only a returned plan AST as in the independent review's probe.
				returned := *first.Result.Plan
				hook := *returned.HookScaffold
				returned.HookScaffold = &hook
				cloned, err := (&gen.HookScaffoldAsset{File: hook.File}).Clone()
				if err != nil {
					t.Fatal(err)
				}
				hook.File = cloned.File
				for _, declaration := range hook.File.Decls {
					if method, ok := declaration.(*ast.FuncDecl); ok && method.Name.Name == "Init" {
						method.Type.Params.List = method.Type.Params.List[:1]
					}
				}
				fresh := filepath.Join(root, "tampered-plan")
				for _, destination := range []string{fresh, filepath.Join(root, "generated")} {
					if err = returned.ValidateDestination(destination); err == nil || !strings.Contains(err.Error(), "compiler-approved evidence") {
						t.Fatalf("ValidateDestination=%v", err)
					}
					if _, err = gen.EmitScaffold(destination, &returned); err == nil || !strings.Contains(err.Error(), "compiler-approved evidence") {
						t.Fatalf("EmitScaffold=%v", err)
					}
				}
				if _, err = os.Stat(fresh); !os.IsNotExist(err) {
					t.Fatalf("rejected plan created destination: %v", err)
				}
				after, err := os.ReadFile(path)
				if err != nil || string(after) != custom {
					t.Fatal("rejected plan changed authored hook", err)
				}
				afterManifest, err := os.ReadFile(filepath.Join(root, "generated", ".datly-gen.json"))
				if err != nil || !bytes.Equal(afterManifest, manifest) {
					t.Fatal("rejected plan changed manifest", err)
				}
			})
			bad := strings.Replace(custom, "xhandler.NoParent", "EventsView", 1)
			if bad == custom {
				t.Fatal("invalid signature fixture did not change the hook")
			}
			if err = os.WriteFile(path, []byte(bad), 0644); err != nil {
				t.Fatal(err)
			}
			beforeManifest, err := os.ReadFile(filepath.Join(root, "generated", ".datly-gen.json"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root, Options: options}); err == nil {
				t.Fatal("incompatible authored hook signature accepted")
			}
			actual, err := os.ReadFile(path)
			if err != nil || string(actual) != bad {
				t.Fatal("failed hook preflight modified authored source", err)
			}
			afterManifest, err := os.ReadFile(filepath.Join(root, "generated", ".datly-gen.json"))
			if err != nil || !bytes.Equal(beforeManifest, afterManifest) {
				t.Fatal("failed hook preflight changed manifest", err)
			}
		})
	}
}

func TestMutationHookScaffoldOptionNormalization(t *testing.T) {
	if _, err := normalizeOptions(Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePost, Go: GoHandlerOptions{Execution: GoExecutionMutation}, Hooks: HookOptions{Scaffold: true}}}); err != nil {
		t.Fatal(err)
	}
}
