package transcribe

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/genpatch"
	"github.com/viant/datly/transcribe/column"
)

const publicGenerationModule = "github.com/viant/datly/publicgen"

func TestCompilerTranscribeRoutesPublicGeneration(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	(testharness.GeneratedModule{Path: publicGenerationModule}).Write(t, root)
	for _, operation := range []string{"get", "patch"} {
		source := &Source{
			Name:          "Orders",
			Scope:         publicGenerationModule + "/source/" + operation,
			Text:          publicGenerationDQL(strings.ToUpper(operation), "api/orders/"+operation),
			Connector:     "main",
			ColumnRefiner: column.New(column.Connections{"main": db.DB}),
		}
		generated, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root, Generation: GenerationOptions{Operation: operation}})
		if err != nil {
			t.Fatal(operation, err)
		}
		if operation == "get" {
			if generated.Result.Plan.MutationHandler != nil || generated.Result.Plan.HookScaffold != nil {
				t.Fatal("GET public generation used mutation scaffolding")
			}
			continue
		}
		if generated.Result.Plan.MutationHandler != nil || generated.Result.Plan.Settings.Mutation != "patch" || generated.Result.Plan.HookScaffold != nil {
			t.Fatal("PATCH public generation did not use universal mutation metadata")
		}
	}
	compilePublicGeneratedProject(t, root)
}

func TestCompilerTranscribeRejectsInvalidPublicGenerationBeforeWrites(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, genpatch.Schema...); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		request Request
		want    string
	}{
		{
			name: "invalid operation",
			request: Request{Generation: GenerationOptions{Operation: "delete"},
				Source: publicGenerationSource(db, publicGenerationDQL("PATCH", "api/orders"))},
			want: "get, patch, post or put",
		},
		{
			name: "invalid language",
			request: Request{Generation: GenerationOptions{Operation: "patch", Language: "python"},
				Source: publicGenerationSource(db, publicGenerationDQL("PATCH", "api/orders"))},
			want: "unsupported gen language",
		},
		{
			name: "mixed handler options",
			request: Request{Generation: GenerationOptions{Operation: "patch"},
				Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch}},
				Source:  publicGenerationSource(db, publicGenerationDQL("PATCH", "api/orders"))},
			want: "low-level transcription options",
		},
		{
			name: "mixed contract options",
			request: Request{Generation: GenerationOptions{Operation: "patch"},
				Options: Options{Contracts: ContractsLinked},
				Source:  publicGenerationSource(db, publicGenerationDQL("PATCH", "api/orders"))},
			want: "low-level transcription options",
		},
		{
			name: "mixed linked component",
			request: Request{Generation: GenerationOptions{Operation: "patch"},
				Component: &bootstrap.RouteSource{},
				Source:    publicGenerationSource(db, publicGenerationDQL("PATCH", "api/orders"))},
			want: "linked component contracts",
		},
		{
			name: "required package",
			request: Request{Generation: GenerationOptions{Operation: "patch"},
				Source: publicGenerationSource(db, strings.TrimPrefix(publicGenerationDQL("PATCH", "api/orders"), "#package('api/orders')\n"))},
			want: "explicit #package",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			(testharness.GeneratedModule{Path: publicGenerationModule}).Write(t, root)
			test.request.Destination = root
			_, err := NewCompiler().Transcribe(ctx, test.request)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v, want %q", err, test.want)
			}
			if _, statErr := os.Stat(filepath.Join(root, "api")); !os.IsNotExist(statErr) {
				t.Fatalf("generation wrote before rejection: %v", statErr)
			}
		})
	}
}

func publicGenerationSource(db *testharness.Harness, text string) *Source {
	return &Source{Name: "Orders", Scope: publicGenerationModule + "/source", Text: text, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": db.DB})}
}

func publicGenerationDQL(method, pkg string) string {
	return "#package('" + pkg + "')\n" + strings.Replace(strings.TrimPrefix(genpatch.DQL, genpatch.PackageDirective+"\n"), "'PATCH'", "'"+method+"'", 1)
}

func compilePublicGeneratedProject(t testing.TB, root string) {
	t.Helper()
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "./...")
	command.Dir = root
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generated public project compile: %v\n%s", err, output)
	}
}
