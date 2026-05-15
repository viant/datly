package xgen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	shapeload "github.com/viant/datly/repository/shape/load"
	shapeplan "github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

func TestComponentCodegen_PreservesExplicitOutputViewOneCardinality(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "vendor", "user_acl")

	component := &shapeload.Component{
		Method:   "GET",
		URI:      "/v1/api/shape/dev/auth/user-acl",
		RootView: "user_acl",
		Output: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name: "Data",
					In:   state.NewOutputLocation("view"),
					Tag:  `anonymous:"true"`,
					Schema: &state.Schema{
						Cardinality: state.One,
					},
				},
			},
		},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views, &view.View{
		Name:     "user_acl",
		Table:    "USER_ACL",
		Template: &view.Template{SourceURL: "user_acl/user_acl.sql"},
		Schema:   &state.Schema{Name: "UserAclView", DataType: "*UserAclView", Cardinality: state.Many},
		Columns: []*view.Column{
			{Name: "UserID", DataType: "int"},
		},
	})

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "user_acl",
		PackagePath: "github.com/acme/project/shape/dev/vendor/user_acl",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    false,
		WithContract: false,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	data, err := os.ReadFile(result.FilePath)
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}
	generated := string(data)
	if !strings.Contains(generated, "Data *UserAclView `parameter:\",kind=output,in=view\" view:\"user_acl\" sql:\"uri=user_acl/user_acl.sql\" anonymous:\"true\"`") {
		t.Fatalf("expected one-cardinality output view to generate pointer field, got:\n%s", generated)
	}
	if strings.Contains(generated, "Data []*UserAclView") {
		t.Fatalf("expected output view not to generate slice field, got:\n%s", generated)
	}
}
