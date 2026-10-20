package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/transcribe/column"
	"github.com/viant/datly/typecatalog"
	loaderast "github.com/viant/x/loader/ast"
)

func TestGeneratorAuxiliaryDeleteRejectsInvalidLifecycle(t *testing.T) {
	const module = "github.com/viant/datly/auxiliarydeletefixture"
	for _, mode := range []string{"missing foreign hook", "wrong canonical Input"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, "CREATE TABLE parents(id INTEGER PRIMARY KEY)"); err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			(testharness.GeneratedModule{Path: module}).Write(t, root)
			hookPackage := "example.com/missing/hooks"
			hookName := "MissingLifecycle"
			want := "cannot scaffold package"
			catalog := typecatalog.NewCatalog()
			var authoredPath, authored string
			if mode == "wrong canonical Input" {
				hookPackage = module + "/hooks"
				hookName = "WrongLifecycle"
				want = "incompatible signature"
				authored = `package hooks
import("context";h "github.com/viant/xdatly/handler")
type WrongInput struct{}
type WrongOutput struct{}
type WrongLifecycle struct{}
func(*WrongLifecycle)Init(context.Context,*WrongInput,h.LifecycleContext[WrongInput,h.NoParent,WrongOutput])error{return nil}
func(*WrongLifecycle)Validate(context.Context,*WrongInput,h.LifecycleContext[WrongInput,h.NoParent,WrongOutput])error{return nil}
`
				authoredPath = filepath.Join(root, "hooks/lifecycle.go")
				writeSourceFile(t, root, "hooks/lifecycle.go", authored)
				pkg, err := loaderast.LoadPackageFS(ctx, os.DirFS(root), "hooks")
				if err != nil {
					t.Fatal(err)
				}
				if err := catalog.RegisterPackage(typecatalog.TypeOriginPackage, pkg); err != nil {
					t.Fatal(err)
				}
			}
			source := &Source{Name: "Requests", Scope: "example.com/source", Connector: "main", Types: catalog, ColumnRefiner: column.New(column.Connections{"main": db.DB}), Text: `#package('api/requests')
#import('hooks','` + hookPackage + `')
#setting($_ = $route('/requests','DELETE'))
#setting($_ = $input_type('RequestsInput'))
#setting($_ = $output_type('RequestsOutput'))
#define($_ = $Data<*Request>(body/data).Optional())
#define($_ = $Writes<*Request>(output/body).WithTag('json:"-"'))
SELECT request.*,type(request,'Request'),CAST(request.IDS AS []int),lifecycle_type(request,'hooks.` + hookName + `')
FROM (SELECT json_group_array(id) AS IDS FROM (parents)) request`}
			_, err := (Generator{Operation: "patch"}).Generate(ctx, GenerationRequest{Source: source, Destination: root})
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("DELETE auxiliary lifecycle must reach %s validation: %v", want, err)
			}
			for _, routeError := range []string{"requires the generated Go mutation lifecycle", "conflicts with authored route method", "DELETE requires explicit"} {
				if strings.Contains(err.Error(), routeError) {
					t.Fatalf("transport gate masked lifecycle error: %v", err)
				}
			}
			if _, err := os.Stat(filepath.Join(root, "api/requests")); !os.IsNotExist(err) {
				t.Fatalf("invalid lifecycle emitted generated package: %v", err)
			}
			if authoredPath != "" {
				data, err := os.ReadFile(authoredPath)
				if err != nil || string(data) != authored {
					t.Fatalf("invalid authored lifecycle was changed: %v", err)
				}
			}
		})
	}
}
