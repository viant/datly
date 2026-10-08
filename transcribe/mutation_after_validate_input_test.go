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
	"github.com/viant/datly/typecatalog"
	xshape "github.com/viant/x/shape"
)

func TestTranscribedAfterValidateInputCanonicalSQLite(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE EVENTS(ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	source := &Source{Name: "Events", Scope: "example.com/generated/events", Connector: "main", Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#package('example.com/generated/generated')
#setting($_ = $route('/events','POST'))
#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many').Required())
#define($_ = $Status<int>(output/status).Output())
#define($_ = $Data<[]*EventsView>(output/body))
SELECT e.*, lifecycle_type(e,'PreparationRules') FROM EVENTS e`}
	options := Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePost, Go: GoHandlerOptions{Execution: GoExecutionMutation}, Hooks: HookOptions{Scaffold: true}}}
	run := func() *GeneratedPackage {
		t.Helper()
		result, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root, Options: options})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	first := run()
	hookRef, err := (xshape.Resolver{}).Reference(first.Result.Plan.HookScaffold.EntityHooks[0].Type)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "generated", "lifecycle.go")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "lifecycle.go", content, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		method, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		counter := ""
		if method.Name.Name == "Init" {
			counter = "preparationInitCalls"
		}
		if method.Name.Name == "Validate" {
			counter = "preparationValidateCalls"
		}
		if counter != "" {
			body, err := parser.ParseFile(token.NewFileSet(), "body.go", "package p;func f(){"+counter+"++;return nil}", 0)
			if err != nil {
				t.Fatal(err)
			}
			method.Body = body.Decls[0].(*ast.FuncDecl).Body
		}
	}
	var authored bytes.Buffer
	if err := format.Node(&authored, token.NewFileSet(), file); err != nil {
		t.Fatal(err)
	}
	custom := authored.String() + "\nvar preparationInitCalls,preparationValidateCalls,preparationBatchCalls int\nfunc (hooks *" + hookRef.BaseName + ") AfterValidateInput(ctx context.Context,input *EventsInput,output *EventsOutput)error{if preparationInitCalls!=2 || preparationValidateCalls!=2{panic(\"incomplete initial validation\")};for _,row:=range input.Events{if row.Id!=nil && *row.Id!=0{panic(\"allocation preceded preparation\")}};preparationBatchCalls++;return nil}\n"
	if err := os.WriteFile(path, []byte(custom), 0644); err != nil {
		t.Fatal(err)
	}
	run()
	after, err := os.ReadFile(path)
	if err != nil || string(after) != custom {
		t.Fatal("authored preparation overwritten", err)
	}
	// Evolve the authoring schema and regenerate the SQL-derived body without Go shape edits.
	if err := db.ExecStatements(ctx, "ALTER TABLE EVENTS ADD COLUMN NOTE TEXT"); err != nil {
		t.Fatal(err)
	}
	run()
	foundNote := false
	err = filepath.Walk(filepath.Join(root, "generated"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(path, ".go") {
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if strings.Contains(string(data), "Note ") {
				foundNote = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !foundNote {
		t.Fatal("schema evolution absent from generated body")
	}
	consumer := strings.Replace(generatedGoWriteRuntimeSource(WritePost, false), "package events", "package generated", 1)
	consumer = strings.Replace(consumer, "CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL)", "CREATE TABLE EVENTS (ID INTEGER PRIMARY KEY AUTOINCREMENT, NAME TEXT NOT NULL, NOTE TEXT)", 1)
	consumer = strings.Replace(consumer, "var count int", "if preparationBatchCalls!=1 {t.Fatalf(\"preparation calls=%d\",preparationBatchCalls)}\n var count int", 1)
	if err := os.WriteFile(filepath.Join(root, "generated", "preparation_runtime_test.go"), []byte(consumer), 0644); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=readonly", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("transcribed preparation runtime: %v\n%s", err, output)
	}
	malformed := strings.Replace(custom, "input *EventsInput,output *EventsOutput", "input *EventsView,output *EventsOutput", 1)
	if err := os.WriteFile(path, []byte(malformed), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root, Options: options}); err == nil {
		t.Fatal("wrong canonical preparation input accepted")
	}
}
