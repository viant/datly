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

	"github.com/viant/datly/internal/testharness"
	"golang.org/x/mod/modfile"
)

const exposureAppModule = "github.com/viant/datly/testfixture/exposure/app"
const exposurePrivateModule = "github.com/viant/datly/testfixture/exposure/private"

// Original Datly repository/shape is the pipeline baseline (impl/149,152).
// Two independently emitted modules are linked once; only SQL resources change
// on reload. Public selection is runtime policy over the complete discovery set.
func (fixture *exposureWorkspace) stages(ctx context.Context) []string {
	t := fixture.t
	root := t.TempDir()
	var stages []string
	var contracts map[string]string
	revisions := 2
	if fixture.documents {
		revisions = 3
	}
	for revision := 1; revision <= revisions; revision++ {
		authored := t.TempDir()
		fixture.modules(authored)
		stage := filepath.Join(root, fmt.Sprint(revision))
		fixture.modules(stage)
		childText := fmt.Sprintf(`#setting($_ = $route('/internal-records','GET'))
#setting($_ = $mcp('records.lookup'))
#define($_ = $Tenant<int>(query/tenant).WithTag('json:"tenant"').Required())
SELECT id, CAST(records.id AS int) FROM records WHERE tenant = $Tenant AND (id = %d OR id = %d) ORDER BY id`, revision, revision+10)
		if fixture.documents {
			childText = strings.Replace(childText, "SELECT id,", "SELECT tenant, CAST(records.tenant AS int), id,", 1)
		}
		writeSourceFile(t, authored, "private/records/Records.sql", childText)
		// First publish the dependency's real emitted descriptors for the importing
		// DQL declaration. Discovery remains the package/type authority owner.
		child, err := (&Discovery{BaseDir: filepath.Join(authored, "private"), Include: []string{exposurePrivateModule + "/records"}, Connector: "main"}).Compile(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(child.Components) != 1 {
			t.Fatalf("child discovery: %d", len(child.Components))
		}
		fixture.emit(ctx, stage, "private", child.Components[0])
		if err = os.CopyFS(filepath.Join(authored, "private", "records"), os.DirFS(filepath.Join(stage, "private", "records"))); err != nil {
			t.Fatal(err)
		}
		parentText := fmt.Sprintf(`#setting($_ = $route('/records','GET'))
#setting($_ = $mcp('records.public'))
#define($_ = $Tenant<int>(query/tenant).WithTag('json:"tenant"').Required())
#define($_ = $Dependency<*%s/records.RecordsOutput>(component/GET:/internal-records).WithTag('json:"-"').Required())
#set($SelectedId = $Dependency.Data[0].Id)
SELECT id, CAST(records.id AS int) FROM records WHERE id = $SelectedId ORDER BY id`, exposurePrivateModule)
		if fixture.documents {
			parentText = strings.Replace(parentText, `#define($_ = $Tenant<int>(query/tenant).WithTag('json:"tenant"').Required())`+"\n", "", 1)
		}
		if fixture.documents && revision == 3 {
			parentText = "#settings($_ = $format('tabular_json'))\n" + parentText
		}
		writeSourceFile(t, authored, "app/records/Records.sql", parentText)
		project, err := (&Discovery{BaseDir: filepath.Join(authored, "app"), ModuleDirs: []string{filepath.Join(authored, "private")}, Include: []string{exposureAppModule + "/...", exposurePrivateModule + "/..."}, Connector: "main"}).Compile(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(project.Components) != 2 {
			t.Fatalf("full discovery components=%d", len(project.Components))
		}
		for _, compiled := range project.Components {
			if compiled.Component.Name != "Records" {
				t.Fatalf("component identity: %+v", compiled.Component.Key)
			}
			switch compiled.Source.Scope {
			case exposureAppModule + "/records":
				fixture.emit(ctx, stage, "app", compiled)
			case exposurePrivateModule + "/records": // Already emitted from this revision's DQL.
			default:
				t.Fatalf("unexpected package authority: %s", compiled.Source.Scope)
			}
		}
		got := map[string]string{}
		for _, module := range []string{"app", "private"} {
			entries, err := os.ReadDir(filepath.Join(stage, module, "records"))
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if strings.HasSuffix(entry.Name(), ".go") {
					data, err := os.ReadFile(filepath.Join(stage, module, "records", entry.Name()))
					if err != nil {
						t.Fatal(err)
					}
					got[module+"/"+entry.Name()] = string(data)
				}
			}
		}
		if revision == 1 {
			contracts = got
		} else if revision <= 2 && !reflect.DeepEqual(contracts, got) {
			for name, data := range got {
				if data != contracts[name] {
					t.Errorf("Go changed %s:\nOLD %s\nNEW %s", name, contracts[name], data)
				}
			}
			t.FailNow()
		}
		stages = append(stages, stage)
	}
	names := []string{"reload_test.go", "public_test.go"}
	if fixture.documents {
		names = append(names, "openapi_test.go")
	}
	for _, name := range names {
		data, err := os.ReadFile("testdata/multimodule_reload/" + name + ".txt")
		if err != nil {
			t.Fatal(err)
		}
		writeSourceFile(t, stages[0], "app/records/"+name, string(data))
	}
	return stages
}

func TestGeneratedMultiModuleSelectivePublicReloadSQLite(t *testing.T) {
	t.Parallel()
	fixture := &exposureWorkspace{t: t}
	stages := fixture.stages(context.Background())
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "-timeout=120s", "-v", "-run", "^TestSelectiveReload$", "./records")
	command.Dir = filepath.Join(stages[0], "app")
	command.Env = append(os.Environ(), "GOWORK=off", "DATLY_EXPOSURE_STAGE="+stages[1])
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("emitted multi-module acceptance: %v\n%s", err, output)
	}
	for _, protocol := range []string{"http", "mcp-registry", "mcp-session", "mcp-stateless"} {
		if !strings.Contains(string(output), "--- PASS: TestSelectiveReload/"+protocol) {
			t.Fatalf("consumer did not execute %s: %s", protocol, output)
		}
	}
	t.Logf("compiled emitted modules:\n%s", output)
}

type exposureWorkspace struct {
	t         *testing.T
	documents bool
}

func (w *exposureWorkspace) modules(root string) {
	w.t.Helper()
	for _, module := range []struct{ name, path string }{{"app", exposureAppModule}, {"private", exposurePrivateModule}} {
		dir := filepath.Join(root, module.name)
		if err := os.MkdirAll(dir, 0755); err != nil {
			w.t.Fatal(err)
		}
		content, err := (testharness.GeneratedModule{Path: module.path}).Content()
		if err != nil {
			w.t.Fatal(err)
		}
		file, err := modfile.Parse("go.mod", []byte(content), nil)
		if err != nil {
			w.t.Fatal(err)
		}
		if module.name == "app" {
			if err = file.AddRequire(exposurePrivateModule, "v0.0.0"); err != nil {
				w.t.Fatal(err)
			}
			if err = file.AddReplace(exposurePrivateModule, "", "../private", ""); err != nil {
				w.t.Fatal(err)
			}
		}
		data, err := file.Format()
		if err != nil {
			w.t.Fatal(err)
		}
		writeSourceFile(w.t, root, module.name+"/go.mod", string(data))
	}
}
func (w *exposureWorkspace) emit(ctx context.Context, stage, module string, compiled *Result) {
	w.t.Helper()
	root := filepath.Join(stage, module)
	input, dir, err := generationInput(root, "records", compiled)
	if err != nil {
		w.t.Fatal(err)
	}
	input.SQLResources = true
	if _, err = NewCompiler().generateInputAt(ctx, root, dir, compiled, input); err != nil {
		w.t.Fatal(err)
	}
}

func TestGeneratedOpenAPIMultiModuleReloadSQLite(t *testing.T) {
	t.Parallel()
	fixture := &exposureWorkspace{t: t, documents: true}
	stages := fixture.stages(context.Background())
	command := exec.Command("go", "test", "-mod=mod", "-count=1", "-timeout=120s", "-v", "-run", "^TestOpenAPIReload$", "./records")
	command.Dir = filepath.Join(stages[0], "app")
	command.Env = append(os.Environ(), "GOWORK=off", "DATLY_EXPOSURE_STAGE="+stages[1], "DATLY_EXPOSURE_INVALID_STAGE="+stages[2])
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("emitted OpenAPI acceptance: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "--- PASS: TestOpenAPIReload") {
		t.Fatalf("consumer did not execute: %s", output)
	}
	t.Logf("emitted OpenAPI acceptance:\n%s", output)
}
