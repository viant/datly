package transcribe

import (
	"context"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	dtag "github.com/viant/datly/tag"
	tcolumn "github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func canonicalRootFixture(t *testing.T, mode string) string {
	t.Helper()
	root := t.TempDir()
	if mode == "canonical" {
		physical, err := filepath.EvalSymlinks(root)
		if err != nil {
			t.Fatal(err)
		}
		return physical
	}
	if mode == "alias" {
		physical := filepath.Join(root, "physical")
		alias := filepath.Join(root, "alias")
		if err := os.Mkdir(physical, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(physical, alias); err != nil {
			t.Fatal(err)
		}
		return alias
	}
	return root // Keep native default macOS /var alias when the caller selects it.
}

func TestCanonicalRootLifecycleRegeneration(t *testing.T) {
	for _, mode := range []string{"canonical", "default", "alias"} {
		for _, entry := range []string{"generator", "compiler", "package"} {
			t.Run(mode+"/"+entry, func(t *testing.T) {
				ctx := context.Background()
				root := canonicalRootFixture(t, mode)
				testharness.WriteGeneratedGoMod(t, root)
				dsn := filepath.Join(t.TempDir(), "lifecycle-alias.db")
				db := sqlite.New(t, sqlite.WithDSN(dsn))
				t.Logf("EXCLUSIVE_SQLITE owner=%s path=%s", t.Name(), dsn)
				if err := db.ExecStatements(ctx, "CREATE TABLE EVENTS(ID INTEGER PRIMARY KEY AUTOINCREMENT,NAME TEXT NOT NULL)"); err != nil {
					t.Fatal(err)
				}
				source := &Source{Name: "Events", Scope: "github.com/viant/datly/transcribe", Connector: "main", Types: typecatalog.NewCatalog(), ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": db.DB}), Text: `#package('example.com/generated/generated')
#setting($_ = $route('/events','POST'))
#define($_ = $Events<[]*EventsView>(body/Data).Cardinality('Many').Required())
#define($_ = $Data<[]*EventsView>(output/body))
SELECT e.*,lifecycle_type(e,'EventRules') FROM EVENTS e`}
				options := Options{Contracts: ContractsGenerated, Handler: HandlerOptions{Target: HandlerGo, Operation: WritePost, Go: GoHandlerOptions{Execution: GoExecutionMutation}, Hooks: HookOptions{Scaffold: true}}}
				invoke := func() (*GeneratedPackage, error) {
					switch entry {
					case "generator":
						return (Generator{Operation: "post"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
					case "compiler":
						return NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root, Options: options})
					default:
						return (&PackageCompilation{Source: source, Component: &bootstrap.RouteSource{FieldName: "Events", PackagePath: source.Scope, InputType: "OtherPackageCompileInput", OutputType: "PackageCompileOutput", Tag: dtag.Component{Method: "POST", Path: "/events", Connector: "main"}}, InputType: reflect.TypeFor[OtherPackageCompileInput](), OutputType: reflect.TypeFor[PackageCompileOutput](), Options: options}).Transcribe(ctx, root)
					}
				}
				first, err := invoke()
				if err != nil {
					t.Fatal("initial lifecycle generation", err)
				}
				if first.Result.Plan.HookScaffold == nil {
					t.Fatal("no lifecycle scaffold")
				}
				hook := filepath.Join(root, "generated", first.Result.Plan.HookScaffold.Destination)
				before, err := os.ReadFile(hook)
				if err != nil {
					t.Fatal(err)
				}
				before = append(before, []byte("\n// authored lifecycle alias preservation\n")...)
				if err = os.WriteFile(hook, before, 0644); err != nil {
					t.Fatal(err)
				}
				for attempt := 0; attempt < 2; attempt++ {
					if _, err = invoke(); err != nil {
						t.Fatal("lifecycle regeneration", err)
					}
					if after, err := os.ReadFile(hook); err != nil || string(after) != string(before) {
						t.Fatal("authored lifecycle bytes changed", err)
					}
				}
				command := testharness.SourceGoCommand(t, root, "test", "-mod=readonly", "-run", "^$", "./generated")
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("native lifecycle package build: %v\n%s", err, output)
				}
			})
		}
	}
}

func TestCanonicalRootProjectPublication(t *testing.T) {
	for _, mode := range []string{"canonical", "default", "alias"} {
		t.Run(mode, func(t *testing.T) {
			root := canonicalRootFixture(t, mode)
			testharness.WriteGeneratedGoMod(t, root)
			users := compileProjectSource(t, "Users", "/users", "SELECT id FROM users")
			orders := compileProjectSource(t, "Orders", "/orders", "SELECT id FROM orders")
			project := &ProjectGeneration{Components: []*Result{orders, users}}
			for attempt := 0; attempt < 2; attempt++ {
				generated, err := project.Generate(context.Background(), root)
				if err != nil {
					t.Fatal("project generation and manifest persistence", err)
				}
				if len(generated.Manifest.Components) != 2 || len(readPersistedProjectManifest(t, root).Components) != 2 {
					t.Fatal("project metadata missing")
				}
				for _, component := range generated.Manifest.Components {
					for _, relative := range component.Artifacts {
						if filepath.IsAbs(relative) || relative == ".." || strings.HasPrefix(relative, "../") {
							t.Fatal("manifest artifact escaped project", relative)
						}
						if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(relative))); err != nil {
							t.Fatal("manifest path does not identify published artifact", err)
						}
					}
				}
			}
			command := testharness.SourceGoCommand(t, root, "test", "-mod=readonly", "-run", "^$", "./...")
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("native project package build: %v\n%s", err, output)
			}
		})
	}
}

func TestCanonicalRootMissingDestinationRejectsBeforeEmission(t *testing.T) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	t.Chdir(root)
	compiled := compileProjectSource(t, "Users", "/users", "SELECT id FROM users")
	compiled.Source.Types = typecatalog.NewCatalog()
	for _, destination := range []string{"", " "} {
		if _, _, err := generationInput(destination, "generated", compiled); err == nil || !strings.Contains(err.Error(), "project root is required") {
			t.Fatal("empty destination acquired current-directory authority", err)
		}
		if _, err := (&ProjectGeneration{Components: []*Result{compiled}}).Generate(context.Background(), destination); err == nil || !strings.Contains(err.Error(), "project directory is required") {
			t.Fatal("project empty destination acquired current-directory authority", err)
		}
	}
	for _, path := range []string{"generated", projectMetadataDir} {
		if _, err := os.Lstat(filepath.Join(root, path)); !os.IsNotExist(err) {
			t.Fatal("missing destination emitted into current directory", path, err)
		}
	}
}
