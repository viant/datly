package generate

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/x"
)

type linkedResourceChild struct {
	Id       int    `sqlx:"id"`
	ParentId int    `sqlx:"parent_id"`
	Label    string `sqlx:"label"`
}
type linkedResourceScopedParent struct {
	Id       int                    `sqlx:"id"`
	Children []*linkedResourceChild `view:"children,table=children" on:"Id:id=ParentId:c.parent_id" sql:"uri=queries/children.sql"`
}
type linkedResourceParent struct {
	Id       int                    `sqlx:"id"`
	Children []*linkedResourceChild `view:"children,table=children" on:"Id:id=ParentId:parent_id" sql:"uri=queries/children.sql"`
}

func TestLinkedChildSQLResourcesNativeRuntime(t *testing.T) {
	testLinkedChildSQLResourcesNativeRuntime(t, false, false)
}
func TestImportedLinkedChildSQLResourcesNativeRuntime(t *testing.T) {
	testLinkedChildSQLResourcesNativeRuntime(t, true, false)
}
func TestImportedLinkedChildSQLRetainsOriginalResource(t *testing.T) {
	testLinkedChildSQLResourcesNativeRuntime(t, true, true)
}
func testLinkedChildSQLResourcesNativeRuntime(t *testing.T, imported, wrapped bool) {
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	pkg := "example.com/generated/parents"
	dir := filepath.Join(root, "parents")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	rows := `package parents
 type Child struct { Id int ` + "`sqlx:\"id\"`" + `; ParentId int ` + "`sqlx:\"parent_id\"`" + `; Label string ` + "`sqlx:\"label\"`" + ` }
 type Row struct { Id int ` + "`sqlx:\"id\"`" + `; Children []*Child ` + "`view:\"children,table=children\" on:\"Id:id=ParentId:parent_id\" sql:\"uri=queries/children.sql\"`" + ` }
`
	if wrapped {
		rows = strings.ReplaceAll(rows, "ParentId:parent_id", "ParentId:c.parent_id")
	}
	if err := os.WriteFile(filepath.Join(dir, "rows.go"), []byte(rows), 0600); err != nil {
		t.Fatal(err)
	}
	catalog := typecatalog.NewCatalog()
	rowType := reflect.TypeFor[linkedResourceParent]()
	if wrapped {
		rowType = reflect.TypeFor[linkedResourceScopedParent]()
	}
	descriptor := x.NewType(rowType, x.WithName("Row"), x.WithPkgPath(pkg))
	if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	child := &spec.View{Name: "children", Source: &spec.ViewSource{SQL: "SELECT c.id,c.parent_id,c.label FROM children c ORDER BY c.id", Table: "children"}, Columns: []*spec.Column{{Name: "Id", Source: "id", Type: spec.TypeRef{Name: "int"}}, {Name: "ParentId", Source: "parent_id", Type: spec.TypeRef{Name: "int"}}, {Name: "Label", Source: "label", Type: spec.TypeRef{Name: "string"}}}}
	originalChildSQL := child.Source.SQL
	if wrapped {
		child.Source.SQL = "SELECT * FROM (" + originalChildSQL + ") children"
		if err := os.MkdirAll(filepath.Join(dir, "queries"), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "queries/children.sql"), []byte(originalChildSQL), 0600); err != nil {
			t.Fatal(err)
		}
	}
	parent := &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT p.id FROM parents p ORDER BY p.id", Table: "parents"}, Columns: []*spec.Column{{Name: "Id", Source: "id", Type: spec.TypeRef{Name: "int"}}}, Relations: []*spec.Relation{{Name: "Children", Holder: "Children", View: child, Cardinality: spec.CardinalityMany}}}
	component := &spec.Component{Name: "Parents", Key: spec.Key{Kind: spec.KindComponent, Scope: pkg, Name: "Parents"}, Settings: &spec.Settings{DefaultConnector: "main"}, Routes: []*spec.Route{{Method: "GET", Path: "/parents"}}, RootView: parent, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}, TypeExpr: "[]*" + pkg + ".Row"}}}
	target, packageName, outputDir := pkg, "parents", dir
	if imported {
		target, packageName, outputDir = "example.com/generated/reader", "reader", filepath.Join(root, "reader")
	}
	plan, err := New(Input{Component: component, TargetPackage: target, PackageName: packageName, ProjectRoot: root, TypeResolver: resolver, SQLResources: true, Views: ViewReferences{RootViewPath: &ViewReference{DescriptorKey: descriptor.Key()}}}).Plan()
	if err != nil {
		t.Fatal(err)
	}
	var childSQL string
	for _, file := range plan.Resources.Files {
		if file.Path == "queries/children.sql" {
			childSQL = file.Content
		}
	}
	if childSQL != originalChildSQL {
		t.Fatalf("linked child SQL missing or rewritten: %q", childSQL)
	}
	if _, err = EmitScaffold(outputDir, plan); err != nil {
		t.Fatal(err)
	}
	actual, err := os.ReadFile(filepath.Join(dir, "rows.go"))
	if err != nil || string(actual) != rows {
		t.Fatal("generation changed linked rows")
	}
	const fixture = `package fixture_test
 import (
 "context"; "database/sql"; "encoding/json"; "net/http/httptest"; "path/filepath"; "testing"
 _ "modernc.org/sqlite"
 _ "example.com/generated/parents"
 "github.com/viant/datly/bootstrap/connector"
 "github.com/viant/datly/standalone"
 "github.com/viant/datly/standalone/config"
 )
 func TestLinkedChildren(t *testing.T){
 root,err:=filepath.Abs(".");if err!=nil{t.Fatal(err)}
 for _,eager:=range []bool{false,true}{
 dsn:=filepath.Join(t.TempDir(),"parents.db");db,err:=sql.Open("sqlite",dsn);if err!=nil{t.Fatal(err)}
 _,err=db.Exec("CREATE TABLE parents(id INTEGER PRIMARY KEY); CREATE TABLE children(id INTEGER PRIMARY KEY,parent_id INTEGER,label TEXT); INSERT INTO parents VALUES(1),(2),(3); INSERT INTO children VALUES(11,1,'first'),(12,1,'second'),(21,2,'third')");if err!=nil{t.Fatal(err)};if err=db.Close();err!=nil{t.Fatal(err)}
 ctx:=context.Background();server,err:=standalone.New(ctx,standalone.Options{Config:&config.Config{BaseDir:root,GoBootstrap:&config.Packages{Packages:[]string{"example.com/generated/parents"},EagerComponents:eager},Connectors:[]connector.Config{{Name:"main",Driver:"sqlite",DSN:dsn}},Connector:"main"}});if err!=nil{t.Fatal(err)};if err=server.Reload(ctx,1);err!=nil{t.Fatal(err)}
 rec:=httptest.NewRecorder();server.ServeHTTP(rec,httptest.NewRequest("GET","/parents",nil));if rec.Code!=200{t.Fatalf("%d %s",rec.Code,rec.Body.String())}
 var result struct {Data []struct {Id int;Children []struct {Id int;ParentId int;Label string}}};if err=json.Unmarshal(rec.Body.Bytes(),&result);err!=nil{t.Fatal(err)}
 if len(result.Data)!=3||len(result.Data[0].Children)!=2||result.Data[0].Children[0].Id!=11||result.Data[0].Children[1].Label!="second"||result.Data[1].Children[0].ParentId!=2||len(result.Data[2].Children)!=0{t.Fatalf("child relation contract: %s",rec.Body.String())}
 if err=server.Shutdown(ctx);err!=nil{t.Fatal(err)}
 }
 }
`
	runtimeFixture := fixture
	if imported {
		runtimeFixture = strings.ReplaceAll(runtimeFixture, "example.com/generated/parents", target)
	}
	if err = os.WriteFile(filepath.Join(root, "runtime_test.go"), []byte(runtimeFixture), 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("go", "test", "-mod=mod", "-race", "-timeout=120s", ".")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("generated linked child runtime: %v\n%s", err, output)
	}
	if strings.Contains(child.Source.SQL, "uri=") {
		t.Fatal("resource packaging changed canonical query")
	}
}

func TestLinkedChildSQLDestinationMustRetainCompiledURI(t *testing.T) {
	const pkg = "example.com/generated/parents"
	catalog := typecatalog.NewCatalog()
	descriptor := x.NewType(reflect.TypeFor[linkedResourceParent](), x.WithName("Row"), x.WithPkgPath(pkg))
	if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
		t.Fatal(err)
	}
	resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
	if err != nil {
		t.Fatal(err)
	}
	component := &spec.Component{Name: "Parents", Settings: &spec.Settings{Generation: &spec.GenerationSettings{SQLFiles: map[string]string{"children": "queries/renamed.sql"}}}, RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id FROM parents"}, Relations: []*spec.Relation{{Name: "Children", Holder: "Children", View: &spec.View{Name: "children", Source: &spec.ViewSource{SQL: "SELECT id,parent_id,label FROM children"}}}}}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}, TypeExpr: "[]*" + pkg + ".Row"}}}
	_, err = New(Input{Component: component, TargetPackage: pkg, PackageName: "parents", TypeResolver: resolver, SQLResources: true, Views: ViewReferences{RootViewPath: &ViewReference{DescriptorKey: descriptor.Key()}}}).Plan()
	if err == nil || !strings.Contains(err.Error(), "differs from uneditable URI") {
		t.Fatalf("compiled URI override accepted: %v", err)
	}
	field, _ := reflect.TypeFor[linkedResourceParent]().FieldByName("Children")
	if field.Tag.Get("sql") != "uri=queries/children.sql" {
		t.Fatal("linked URI changed")
	}
}

func TestLinkedChildResourcesRetainAuthoredFilesystemAuthority(t *testing.T) {
	const pkg = "example.com/generated/parents"
	for _, changed := range []bool{false, true} {
		t.Run(map[bool]string{false: "retain", true: "reject changed SQL"}[changed], func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			dir := filepath.Join(root, "parents")
			for _, folder := range []string{"sql", "queries"} {
				if err := os.MkdirAll(filepath.Join(dir, folder), 0755); err != nil {
					t.Fatal(err)
				}
			}
			source := "package parents\nimport \"embed\"\nconst ParentsDatlyResourceNamespace = " + `"` + readableResourceNamespace(pkg, "Parents") + `"` + "\n//go:embed sql/parents.sql queries/children.sql\nvar ParentsDatlyResources embed.FS\n"
			const query = "SELECT id,parent_id,label FROM children"
			for file, body := range map[string]string{"resources.go": source, "sql/parents.sql": "SELECT id FROM parents", "queries/children.sql": query} {
				if err := os.WriteFile(filepath.Join(dir, file), []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			catalog := typecatalog.NewCatalog()
			descriptor := x.NewType(reflect.TypeFor[linkedResourceParent](), x.WithName("Row"), x.WithPkgPath(pkg))
			if err := catalog.Register(typecatalog.TypeOriginPackage, descriptor); err != nil {
				t.Fatal(err)
			}
			resolver, err := typecatalog.NewResolver(catalog, typecatalog.PackageAuthority, nil)
			if err != nil {
				t.Fatal(err)
			}
			canonical := query
			if changed {
				canonical += " WHERE parent_id=1"
			}
			component := &spec.Component{Name: "Parents", RootView: &spec.View{Name: "parents", Source: &spec.ViewSource{SQL: "SELECT id FROM parents"}, Relations: []*spec.Relation{{Name: "Children", Holder: "Children", View: &spec.View{Name: "children", Source: &spec.ViewSource{SQL: canonical}}}}}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}, TypeExpr: "[]*" + pkg + ".Row"}}}
			plan, err := New(Input{Component: component, TargetPackage: pkg, PackageName: "parents", ProjectRoot: root, TypeResolver: resolver, SQLResources: true, Views: ViewReferences{RootViewPath: &ViewReference{DescriptorKey: descriptor.Key()}}}).Plan()
			if changed {
				if err == nil || !strings.Contains(err.Error(), "application-owned embedded filesystem") {
					t.Fatalf("authored SQL overwrite accepted: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if !plan.Resources.authoredSource() {
					t.Fatal("authored resource declaration lost")
				}
			}
			current, err := os.ReadFile(filepath.Join(dir, "resources.go"))
			if err != nil || string(current) != source {
				t.Fatal("authored filesystem changed")
			}
			current, err = os.ReadFile(filepath.Join(dir, "queries/children.sql"))
			if err != nil || string(current) != query {
				t.Fatal("authored SQL changed")
			}
		})
	}
}
