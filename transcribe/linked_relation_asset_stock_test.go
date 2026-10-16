package transcribe_test

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/gobuild"
)

const linkedAssetsModule = "example.com/linkedassets"

func writeLinkedAssetFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	(testharness.GeneratedModule{Path: linkedAssetsModule}).Write(t, root)
	files := map[string]string{
		"rows/rows.go": `package rows
type Root struct {
	Id int ` + "`sqlx:\"id\"`" + `
}
type Parent struct {
	Id int ` + "`sqlx:\"id\"`" + `
	Children []*Child ` + "`sqlx:\"-\" on:\"Id:p.id=ParentId:c.parent_id\" view:\",table=children\" sql:\"uri=queries/children.sql\"`" + `
}
type Child struct {
	Id int ` + "`sqlx:\"id\"`" + `
	ParentId int ` + "`sqlx:\"parent_id\"`" + `
	Label string ` + "`sqlx:\"label\"`" + `
	Details []*Detail ` + "`sqlx:\"-\" on:\"Id:c.id=ChildId:d.child_id\" view:\",table=details\" sql:\"uri=queries/details.sql\"`" + `
}
type Detail struct {
	Id int ` + "`sqlx:\"id\"`" + `
	ChildId int ` + "`sqlx:\"child_id\"`" + `
	Label string ` + "`sqlx:\"label\"`" + `
}
`,
		"rows/queries/children.sql": "SELECT c.id,c.parent_id,c.label FROM children c ORDER BY c.id\n",
		"rows/queries/details.sql":  "SELECT d.id,d.child_id,d.label FROM details d ORDER BY d.id\n",
		"dql/rows.dql": `#package('example.com/linkedassets/rows')
#import('rows','example.com/linkedassets/rows')
#setting($_ = $route('/parents','GET'))
#setting($_ = $connector('main'))
#setting($_ = $input_type('Input'))
#setting($_ = $output_type('Output'))
#define($_ = $Rows<[]*rows.Root>(body/data).Optional())
#define($_ = $Current<[]*rows.Parent>(view/Current).Cardinality('Many') /* SELECT p.id FROM parents p ORDER BY p.id */)
#define($_ = $Data<[]*rows.Root>(output/view))
SELECT r.id, type(r,'rows.Root') FROM roots r ORDER BY r.id
`,
	}
	for name, content := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func compileLinkedAssetFixture(t *testing.T, root string) (*transcribe.Result, *gen.Plan) {
	t.Helper()
	schemaDB, err := sql.Open("sqlite3", filepath.Join(root, "schema.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = schemaDB.Exec("CREATE TABLE IF NOT EXISTS roots(id INTEGER PRIMARY KEY); CREATE TABLE IF NOT EXISTS parents(id INTEGER PRIMARY KEY); CREATE TABLE IF NOT EXISTS children(id INTEGER PRIMARY KEY,parent_id INTEGER,label TEXT); CREATE TABLE IF NOT EXISTS details(id INTEGER PRIMARY KEY,child_id INTEGER,label TEXT);"); err != nil {
		t.Fatal(err)
	}
	defer schemaDB.Close()
	project, err := (&transcribe.Discovery{
		BaseDir: root, Include: []string{linkedAssetsModule + "/dql"},
		SourceFile: "rows.dql", GoBuild: &gobuild.Context{Dir: root}, Connector: "main",
		ColumnRefiner: column.New(column.Connections{"main": schemaDB}),
	}).Compile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(project.Components) != 1 {
		t.Fatalf("expected one stock-discovered component, got %d", len(project.Components))
	}
	result := project.Components[0]
	plan, err := gen.New(gen.Input{
		Component: result.Component, Declarations: result.Declarations, Views: result.Views,
		ViewBindings: result.ViewBindings, TypeResolver: result.TypeResolver,
		ProjectRoot: root, TargetPackage: result.Component.TypeContext.PackagePath,
		Resources: result.Source.Resources, SQLResources: true,
	}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	return result, plan
}

func generateStockLinkedAssetFixture(t *testing.T, root string) *transcribe.GeneratedProject {
	t.Helper()
	schemaDB, err := sql.Open("sqlite3", filepath.Join(root, "schema.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer schemaDB.Close()
	if _, err = schemaDB.Exec("CREATE TABLE IF NOT EXISTS roots(id INTEGER PRIMARY KEY); CREATE TABLE IF NOT EXISTS parents(id INTEGER PRIMARY KEY); CREATE TABLE IF NOT EXISTS children(id INTEGER PRIMARY KEY,parent_id INTEGER,label TEXT); CREATE TABLE IF NOT EXISTS details(id INTEGER PRIMARY KEY,child_id INTEGER,label TEXT);"); err != nil {
		t.Fatal(err)
	}
	discovery := &transcribe.Discovery{BaseDir: root, Include: []string{linkedAssetsModule + "/dql"}, SourceFile: "rows.dql", GoBuild: &gobuild.Context{Dir: root}, Connector: "main", ColumnRefiner: column.New(column.Connections{"main": schemaDB})}
	generated, err := discovery.Generate(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	return generated
}

func generatedFileSnapshot(t *testing.T, root string) map[string]string {
	t.Helper()
	result := map[string]string{}
	err := filepath.WalkDir(filepath.Join(root, "rows"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		result[filepath.ToSlash(rel)] = string(content)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestStockDiscoveryPlansAndEmitsNestedLinkedRelationAssets(t *testing.T) {
	root := writeLinkedAssetFixture(t)
	_, plan := compileLinkedAssetFixture(t, root)
	if plan.ViewByType("Parent") == nil || plan.ViewByType("Parent").Ownership != gen.ViewLinked {
		t.Fatalf("Current row did not retain linked ownership: %#v", plan.ViewByType("Parent"))
	}
	assets := map[string]string{}
	if plan.Resources != nil {
		for _, file := range plan.Resources.Files {
			assets[file.Path] = file.Content
		}
	}
	for path, expected := range map[string]string{
		"queries/children.sql": "SELECT c.id,c.parent_id,c.label FROM children c ORDER BY c.id\n",
		"queries/details.sql":  "SELECT d.id,d.child_id,d.label FROM details d ORDER BY d.id\n",
	} {
		if got := assets[path]; got != expected {
			t.Fatalf("linked relation asset %s = %q, want exact source bytes %q", path, got, expected)
		}
	}

	// Stock generation must be stable across reload/generation passes.
	firstProject := generateStockLinkedAssetFixture(t, root)
	firstFiles := generatedFileSnapshot(t, root)
	secondProject := generateStockLinkedAssetFixture(t, root)
	secondFiles := generatedFileSnapshot(t, root)
	if len(firstProject.Components) != 1 || len(secondProject.Components) != 1 {
		t.Fatalf("stock generation component counts: %d then %d", len(firstProject.Components), len(secondProject.Components))
	}
	if len(firstFiles) != len(secondFiles) {
		t.Fatalf("generation file count changed: %d -> %d", len(firstFiles), len(secondFiles))
	}
	for path, content := range firstFiles {
		if secondFiles[path] != content {
			t.Fatalf("unstable two-pass generation at %s", path)
		}
	}
	for _, path := range []string{"rows/queries/children.sql", "rows/queries/details.sql"} {
		if firstFiles[path] != assets[strings.TrimPrefix(path, "rows/")] {
			t.Fatalf("generated %s differs from exact linked source bytes", path)
		}
	}
	if strings.TrimSpace(assets["queries/children.sql"]) == "" {
		t.Fatal("fixture must carry a nonempty SQL asset")
	}
	inputHook := `package rows
import ("context"; "fmt")
var LinkedInputInitObserved bool
var LinkedInputInitSummary string
func (input *Input) Init(context.Context) error {
 if len(input.Current) != 1 || input.Current[0] == nil || input.Current[0].Id != 1 || len(input.Current[0].Children) != 1 || input.Current[0].Children[0].Id != 11 || len(input.Current[0].Children[0].Details) != 1 || input.Current[0].Children[0].Details[0].Id != 111 || input.Current[0].Children[0].Label != "child" || input.Current[0].Children[0].Details[0].Label != "detail" { return fmt.Errorf("linked Current rows not bound before Init") }
 LinkedInputInitObserved = true
 LinkedInputInitSummary = fmt.Sprintf("parents=%d parent_id=%d children=%d child_id=%d child_label=%s details=%d detail_id=%d detail_label=%s", len(input.Current), input.Current[0].Id, len(input.Current[0].Children), input.Current[0].Children[0].Id, input.Current[0].Children[0].Label, len(input.Current[0].Children[0].Details), input.Current[0].Children[0].Details[0].Id, input.Current[0].Children[0].Details[0].Label)
 return nil
}
`
	if err := os.WriteFile(filepath.Join(root, "rows", "input_init.go"), []byte(inputHook), 0600); err != nil {
		t.Fatal(err)
	}
	inputSource, err := os.ReadFile(filepath.Join(root, "rows", "input.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(inputSource), "Current []*Parent") {
		t.Fatalf("generated Input lacks native Current binding: %s", inputSource)
	}
	const runtimeTest = `package rows_test
import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/bootstrap/connector"
	"github.com/viant/datly/standalone"
	"github.com/viant/datly/standalone/config"
 "example.com/linkedassets/rows"
)
func TestLinkedNestedAssetsHydrateAfterReload(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "linked.db")
	db, err := sql.Open("sqlite3", dsn); if err != nil { t.Fatal(err) }; defer db.Close()
	_, err = db.Exec("CREATE TABLE roots(id INTEGER PRIMARY KEY);\n"+
		"CREATE TABLE parents(id INTEGER PRIMARY KEY);\n"+
		"CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER,label TEXT);\n"+
		"CREATE TABLE details(id INTEGER PRIMARY KEY,child_id INTEGER,label TEXT);\n"+
		"INSERT INTO roots VALUES(99);\n"+
		"INSERT INTO parents VALUES(1);\n"+
		"INSERT INTO children VALUES(11,1,'child');\n"+
		"INSERT INTO details VALUES(111,11,'detail');")
	if err != nil { t.Fatal(err) }; if err = db.Close(); err != nil { t.Fatal(err) }
	root, err := filepath.Abs(".."); if err != nil { t.Fatal(err) }
	ctx := context.Background()
	var diagnostics bytes.Buffer
	server, err := standalone.New(ctx, standalone.Options{Diagnostics:&diagnostics, Config:&config.Config{BaseDir:root, GoBootstrap:&config.Packages{Packages:[]string{"example.com/linkedassets/rows"}}, Connectors:[]connector.Config{{Name:"main", Driver:"sqlite3", DSN:dsn}}, Connector:"main"}})
	if err != nil { t.Fatal(err) }; defer server.Shutdown(ctx)
	if err = server.Reload(ctx, 1); err != nil { t.Fatal(err) }
	rec := httptest.NewRecorder(); request := httptest.NewRequest("GET", "/parents", strings.NewReader("{\"data\":[{\"id\":1}]}")); request.Header.Set("Content-Type", "application/json"); server.ServeHTTP(rec, request)
	if rec.Code != 200 { t.Fatalf("GET /parents: %d %s; diagnostics: %s", rec.Code, rec.Body.String(), diagnostics.String()) }
	var body struct { Data []struct { Id int ` + "`json:\"id\"`" + `; Children []struct { Id int ` + "`json:\"id\"`" + `; Details []struct { Id int ` + "`json:\"id\"`" + `; Label string ` + "`json:\"label\"`" + ` } ` + "`json:\"details\"`" + ` } ` + "`json:\"children\"`" + ` } ` + "`json:\"data\"`" + ` }
	if err = json.Unmarshal(rec.Body.Bytes(), &body); err != nil { t.Fatalf("invalid response JSON: %v", err) }
	if len(body.Data) != 1 || body.Data[0].Id != 99 { t.Fatalf("root reader output mismatch: %s; diagnostics: %s", rec.Body.String(), diagnostics.String()) }; if !rows.LinkedInputInitObserved { t.Fatalf("authored Input.Init did not observe native Current hydration: %s; diagnostics: %s", rec.Body.String(), diagnostics.String()) }; if rows.LinkedInputInitSummary != "parents=1 parent_id=1 children=1 child_id=11 child_label=child details=1 detail_id=111 detail_label=detail" { t.Fatalf("unexpected nested hydration summary: %s", rows.LinkedInputInitSummary) }; t.Logf("authored Input.Init observed linked Current: %s", rows.LinkedInputInitSummary)
}
`
	runtimePath := filepath.Join(root, "rows", "linked_relation_runtime_test.go")
	if err := os.WriteFile(runtimePath, []byte(runtimeTest), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-v", "-mod=mod", "-race", "-count=1", "-run", "^TestLinkedNestedAssetsHydrateAfterReload$", "./rows")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off", "GO111MODULE=on")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generated native linked-resource SQLite reload: %v\n%s", err, output)
	}
	t.Logf("named native reload test output:\n%s", output)
}
