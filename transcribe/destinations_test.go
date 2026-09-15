package transcribe

import (
	"context"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/project/build"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
	tcolumn "github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	linkedcontract "github.com/viant/datly/transcribe/testdata/linkedcontract"
)

func TestDQLDestinationsSingleAndProject(t *testing.T) {
	for _, project := range []bool{false, true} {
		for _, separate := range []bool{false, true} {
			name := "single"
			if project {
				name = "project"
			}
			if separate {
				name += "_separate"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				root := t.TempDir()
				testharness.WriteGeneratedGoMod(t, root)
				h := sqlite.New(t)
				if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER, name TEXT)", "INSERT INTO records VALUES(1,'first')"); err != nil {
					t.Fatal(err)
				}
				input, output, row := "Request", "Response", "RecordRow"
				directives := ""
				if separate {
					directives = "#import('dto','example.com/generated/contracts')\n#import('rows','example.com/generated/models')\n"
					input = "dto.Request"
					output = "dto.Response"
					row = "rows.RecordRow"
				}
				source := &Source{Name: "Records", Scope: "test", Connector: "main", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Text: "#package('example.com/generated/api/records')\n" + directives + "#setting($_ = $route('/records','GET'))\n#setting($_ = $input_type('" + input + "'))\n#setting($_ = $output_type('" + output + "'))\n#setting($_ = $input_dest('request.go'))\n#setting($_ = $output_dest('response.go'))\n#define($_ = $Search<string>(query/search).Optional())\nSELECT r.*, type(r, '" + row + "'), dest(r, 'rows.go') FROM records r"}
				run := func() {
					t.Helper()
					if project {
						compiled, err := NewCompiler().Compile(ctx, source)
						if err != nil {
							t.Fatal(err)
						}
						if _, err = (&ProjectGeneration{Components: []*Result{compiled}}).Generate(ctx, root); err != nil {
							t.Fatal(err)
						}
					} else {
						if _, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root}); err != nil {
							t.Fatal(err)
						}
					}
				}
				run()
				inputDir, viewDir := "api/records", "api/records"
				if separate {
					inputDir = "contracts"
					viewDir = "models"
				}
				for _, file := range []string{inputDir + "/request.go", inputDir + "/response.go", viewDir + "/rows.go", "api/records/router.go"} {
					if _, err := os.Stat(filepath.Join(root, file)); err != nil {
						t.Fatal(err)
					}
				}
				authored := filepath.Join(root, "api/records/authored.go")
				if err := os.WriteFile(authored, []byte("package records\nconst Kept = 42\n"), 0600); err != nil {
					t.Fatal(err)
				}
				run()
				command := exec.Command("go", "test", "./...")
				command.Dir = root
				command.Env = append(os.Environ(), "GOWORK=off")
				if output, err := command.CombinedOutput(); err != nil {
					t.Fatalf("build: %v\n%s", err, output)
				}
				routes, err := bootstrap.DiscoverComponentsFromPackages(ctx, root, []string{"example.com/generated/api/records"}, nil)
				if err != nil || len(routes) != 1 {
					t.Fatalf("reload routes=%d err=%v", len(routes), err)
				}
				reloaded, err := (&Discovery{BaseDir: root, Include: []string{"example.com/generated/api/records"}, Connector: "main", ColumnRefiner: source.ColumnRefiner}).Compile(ctx)
				if err != nil || len(reloaded.Components) != 1 {
					t.Fatalf("reload compilation: %v", err)
				}
				if project && separate {
					(testharness.GeneratedModule{}).WriteSums(t, root)
					service := build.Service{}
					if err := service.Init(ctx, build.InitRequest{Dir: root}); err != nil {
						t.Fatal(err)
					}
					// Init adds the linked host imports; resolve the disposable module's
					// graph before invoking the read-only build selection.
					dependencies := exec.CommandContext(ctx, "go", "list", "-mod=mod", "-deps", "./...")
					dependencies.Dir = root
					dependencies.Env = append(os.Environ(), "GOWORK=off")
					if output, err := dependencies.CombinedOutput(); err != nil {
						t.Fatalf("resolve generated dependencies: %v\n%s", err, output)
					}
					built, err := service.Build(ctx, build.Request{Dir: root, Env: append(os.Environ(), "GOWORK=off")})
					if err != nil {
						t.Fatalf("automatic build: %v", err)
					}
					if built.Components != 1 || built.Types < 3 {
						t.Fatalf("automatic discovery: %+v", built)
					}
				}
				if body, err := os.ReadFile(authored); err != nil || !strings.Contains(string(body), "Kept = 42") {
					t.Fatal("authored file changed")
				}
			})
		}
	}
}

func TestDQLDestinationsSharedPackageAndCollisions(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	source := func(name string) *Result {
		return compileProjectSource(t, name, "/"+strings.ToLower(name), "#package('example.com/generated/api/shared')\n#setting($_ = $file_prefix('"+strings.ToLower(name)+"_'))\nSELECT id FROM "+strings.ToLower(name))
	}
	first, second := source("Users"), source("Orders")
	initial, err := (&ProjectGeneration{Components: []*Result{first, second}}).Generate(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "api/shared")
	kept := filepath.Join(dir, "authored.go")
	os.WriteFile(kept, []byte("package shared\ntype Authored struct {Value string}\n"), 0600)
	before, err := os.ReadFile(filepath.Join(dir, "orders_router.go"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = (&ProjectGeneration{Components: []*Result{first}}).Generate(ctx, root); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "orders_router.go"))
	if string(after) != string(before) {
		t.Fatal("other component overwritten")
	}
	if len(initial.Manifest.Components) != 2 {
		t.Fatal(initial.Manifest)
	}
	command := exec.Command("go", "test", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("shared build: %v\n%s", err, output)
	}
	routes, err := bootstrap.DiscoverComponentsFromPackages(ctx, root, []string{"example.com/generated/api/shared"}, nil)
	if err != nil || len(routes) != 2 {
		t.Fatalf("routes=%d err=%v", len(routes), err)
	}
	reloaded, err := (&Discovery{BaseDir: root, Include: []string{"example.com/generated/api/shared"}}).Compile(ctx)
	if err != nil || len(reloaded.Components) != 2 {
		t.Fatalf("shared resource reload: %v", err)
	}
	// A distinct file cannot shadow a declaration authored by a different owner.
	collision := source("Authored")
	collision.Component.Settings = &spec.Settings{InputType: "Authored"}
	if _, err = (&ProjectGeneration{Components: []*Result{collision}}).Generate(ctx, root); err == nil || !strings.Contains(err.Error(), "collid") {
		t.Fatalf("collision error=%v", err)
	}
	if data, _ := os.ReadFile(kept); !strings.Contains(string(data), "Value string") {
		t.Fatal("authored type overwritten")
	}
}

func TestDQLDestinationsRejectInvalidBeforeWriting(t *testing.T) {
	cases := []struct{ name, directives string }{
		{"escape", "#package('../outside')"},
		{"external", "#package('example.net/foreign/api')"},
		{"file_escape", "#package('api')\n#setting($_ = $input_dest('../input.go'))"},
		{"conflicting_packages", "#package('api')\n#package('elsewhere')"},
		{"ambiguous_import", "#package('api')\n#import('dto','example.com/generated/one')\n#import('dto','example.com/generated/two')\n#setting($_ = $input_type('dto.Request'))"},
		{"conflicting_shape", "#package('api')\n#import('dto','example.com/generated/contracts')\n#setting($_ = $input_type('dto.Request'))\n#setting($_ = $input_dest('elsewhere/request.go'))"},
		{"cycle", "#package('api')\n#import('dto','example.com/generated/contracts')\n#setting($_ = $output_type('dto.Response'))"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			_, err := NewCompiler().Transcribe(context.Background(), Request{Destination: root, Source: &Source{Name: "Records", Text: tc.directives + "\n#setting($_ = $route('/records','GET'))\nSELECT id FROM records"}})
			if err == nil {
				t.Fatal("invalid destination accepted")
			}
			if _, err = os.Stat(filepath.Join(root, "api")); !os.IsNotExist(err) {
				t.Fatal("invalid generation wrote files")
			}
		})
	}
}

func TestDQLDestinationsPreserveEditsAndCASTDrop(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx, "CREATE TABLE records(id INTEGER,name TEXT)"); err != nil {
		t.Fatal(err)
	}
	header := "#package('example.com/generated/api')\n#import('rows','example.com/generated/models')\n#setting($_ = $route('/records','GET'))\n"
	source := &Source{Name: "Records", ColumnRefiner: tcolumn.New(tcolumn.Connections{"main": h.DB}), Connector: "main", Text: header + "SELECT r.*,type(r,'rows.RecordRow'),dest(r,'rows.go') FROM records r"}
	run := func() error {
		_, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root})
		return err
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	shapePath := filepath.Join(root, "models/rows.go")
	body, err := os.ReadFile(shapePath)
	if err != nil {
		t.Fatal(err)
	}
	edited := strings.Replace(string(body), "type RecordRow struct {", "type RecordRow struct {\n UserNote string", 1)
	if edited == string(body) {
		t.Fatal("shape missing")
	}
	if err = os.WriteFile(shapePath, []byte(edited), 0600); err != nil {
		t.Fatal(err)
	}
	source.Text = header + "SELECT r.id, CAST(r.id AS *int),type(r,'rows.RecordRow'),dest(r,'rows.go') FROM records r"
	if err = run(); err != nil {
		t.Fatal(err)
	}
	body, err = os.ReadFile(shapePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "UserNote") || strings.Contains(string(body), "Name ") || !strings.Contains(string(body), "*int") {
		t.Fatalf("regenerated shape: %s", body)
	}
	before := string(body)
	source.Text = strings.Replace(source.Text, "example.com/generated/models", "example.com/generated/newmodels", 1)
	if err = run(); err == nil || !strings.Contains(err.Error(), "migration") {
		t.Fatalf("silent relocation: %v", err)
	}
	body, _ = os.ReadFile(shapePath)
	if string(body) != before {
		t.Fatal("relocation changed shape")
	}
}

func TestDQLDestinationsSymlinkAndNestedModule(t *testing.T) {
	for _, link := range []bool{false, true} {
		t.Run(map[bool]string{false: "module", true: "symlink"}[link], func(t *testing.T) {
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			if link {
				if err := os.Symlink(t.TempDir(), filepath.Join(root, "api")); err != nil {
					t.Fatal(err)
				}
			} else {
				os.Mkdir(filepath.Join(root, "api"), 0700)
				os.WriteFile(filepath.Join(root, "api/go.mod"), []byte("module example.com/other\n"), 0600)
			}
			_, err := NewCompiler().Transcribe(context.Background(), Request{Destination: root, Source: &Source{Name: "Records", Text: "#package('api/records')\n#setting($_ = $route('/records','GET'))\nSELECT id FROM records"}})
			if err == nil {
				t.Fatal("invalid destination accepted")
			}
		})
	}
}

func TestDQLDestinationsFilePathsAndSuppliedHandlers(t *testing.T) {
	for _, kind := range []string{"Go", "GoQualified", "Velty"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			testharness.WriteGeneratedGoMod(t, root)
			source := &Source{Name: "Orders", Text: `#package('api/orders')
#setting($_ = $route('/orders','POST'))
#setting($_ = $input_type('Request'))
#setting($_ = $output_type('Response'))
#setting($_ = $input_dest('contracts/request.go'))
#setting($_ = $output_dest('contracts/response.go'))
#define($_ = $Name<string>(query/name))
#define($_ = $Result<string>(output/body))
SELECT 1`}
			if kind == "Velty" {
				source.VeltyHandler = &gen.VeltyHandlerAsset{Template: `#set($Output.Result = $Input.Name)`}
			} else {
				file, err := parser.ParseFile(token.NewFileSet(), "handler.go", `package authored
import "context"
func HandleOrders(ctx context.Context,input *Request)(*Response,error){return &Response{Result:input.Name},nil}
`, parser.ParseComments)
				if err != nil {
					t.Fatal(err)
				}
				if kind == "GoQualified" {
					file, err = parser.ParseFile(token.NewFileSet(), "handler.go", `package authored
import ("context";dto "example.com/generated/contracts")
func HandleOrders(ctx context.Context,input *dto.Request)(*dto.Response,error){return &dto.Response{Result:input.Name},nil}
`, parser.ParseComments)
					if err != nil {
						t.Fatal(err)
					}
				}
				source.GoHandler = &gen.GoHandlerAsset{Entry: "HandleOrders", File: file}
			}
			for i := 0; i < 2; i++ {
				if _, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root}); err != nil {
					t.Fatal(err)
				}
			}
			for _, file := range []string{"contracts/request.go", "contracts/response.go", "api/orders/router.go"} {
				if _, err := os.Stat(filepath.Join(root, file)); err != nil {
					t.Fatal(err)
				}
			}
			command := exec.Command("go", "test", "-mod=mod", "./...")
			command.Dir = root
			command.Env = append(os.Environ(), "GOWORK=off")
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("handler build: %v\n%s", err, output)
			}
		})
	}
}

func TestDQLDestinationsRejectComponentMove(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	source := &Source{Name: "Records", Text: "#package('api/records')\n#setting($_ = $route('/records','GET'))\nSELECT id FROM records"}
	if _, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root}); err != nil {
		t.Fatal(err)
	}
	source.Text = strings.Replace(source.Text, "api/records", "api/moved", 1)
	if _, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root}); err == nil || !strings.Contains(err.Error(), "migration") {
		t.Fatalf("move error=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "api/moved")); !os.IsNotExist(err) {
		t.Fatal("move wrote artifacts")
	}
}

func TestDQLDestinationsRejectRetainedImportCycle(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	source := &Source{Name: "Records", Text: "#package('api')\n#import('rows','example.com/generated/models')\n#setting($_ = $route('/records','GET'))\nSELECT id,type(records,'rows.RecordRow'),dest(records,'rows.go') FROM records"}
	run := func() error {
		_, err := NewCompiler().Transcribe(ctx, Request{Source: source, Destination: root})
		return err
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "models/rows.go")
	body, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	edited := strings.Replace(string(body), "package models", "package models\nimport api \"example.com/generated/api\"", 1) + "\ntype AuthoredCycle = api.RecordsInput\n"
	if err = os.WriteFile(file, []byte(edited), 0600); err != nil {
		t.Fatal(err)
	}
	if err = run(); err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("retained cycle accepted: %v", err)
	}
	after, _ := os.ReadFile(file)
	if string(after) != edited {
		t.Fatal("invalid generation changed user edits")
	}
}

func TestDQLDestinationsKeepLinkedTypesInTheirPackage(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	const pkg = "github.com/viant/datly/transcribe/testdata/linkedcontract"
	compilation := &PackageCompilation{
		Source:    &Source{Scope: pkg, Name: "Users", Text: "#package('api/users')\n#setting($_ = $route('/users','GET'))\nSELECT id FROM users"},
		Component: &bootstrap.RouteSource{FieldName: "Users", PackagePath: pkg, InputType: "ComponentInput", OutputType: "ComponentOutput", Tag: dtag.Component{Method: "GET", Path: "/users", View: "Users"}},
		InputType: reflect.TypeOf(linkedcontract.ComponentInput{}), OutputType: reflect.TypeOf(linkedcontract.ComponentOutput{}),
	}
	generated, err := compilation.Transcribe(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if generated.Result.Plan.Input.Ownership != gen.ContractLinked || generated.Result.Plan.Output.Ownership != gen.ContractLinked {
		t.Fatal("linked contracts changed ownership")
	}
	if len(generated.Result.Plan.ShapePackages) != 0 {
		t.Fatal("linked types were relocated")
	}
	command := exec.Command("go", "test", "-mod=mod", "./...")
	command.Dir = root
	command.Env = append(os.Environ(), "GOWORK=off")
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("linked build: %v\n%s", err, output)
	}
	compilation.Source.Text = strings.Replace(compilation.Source.Text, "#setting($_ = $route", "#setting($_ = $input_dest('contracts/request.go'))\n#setting($_ = $route", 1)
	if _, err = compilation.Transcribe(ctx, root); err == nil || !strings.Contains(err.Error(), "cannot move") {
		t.Fatalf("linked move accepted: %v", err)
	}
}
