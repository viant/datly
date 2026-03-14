package transcriber

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	shape "github.com/viant/datly/repository/shape"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

func TestGenerateComponentCodegen_AppliesHooks(t *testing.T) {
	ctx := context.Background()
	sourcePath := filepath.Join(t.TempDir(), "sample.dql")
	dql := `
#setting($_ = $connector('dev'))
#setting($_ = $route('/v1/api/original', 'GET'))
#define($_ = $Name<string>(query/name).Optional())
#define($_ = $Data<?>(output/view))
SELECT ID, NAME FROM VENDOR`
	if err := os.WriteFile(sourcePath, []byte(dql), 0o644); err != nil {
		t.Fatalf("write dql error = %v", err)
	}

	planned, err := shapeCompile.New().Compile(ctx, &shape.Source{
		Name: "sample",
		Path: sourcePath,
		DQL:  dql,
	}, shape.WithTypeContextPackageDir("generated/sample"), shape.WithTypeContextPackageName("sample"))
	if err != nil {
		t.Fatalf("compile error = %v", err)
	}
	artifact, err := shapeLoad.New().LoadComponent(ctx, planned, shape.WithLoadTypeContextPackages(true))
	if err != nil {
		t.Fatalf("load component error = %v", err)
	}
	component, ok := shapeLoad.ComponentFrom(artifact)
	if !ok || component == nil {
		t.Fatalf("unexpected component artifact %T", artifact.Component)
	}
	resource := artifact.Resource
	root, err := resource.View(component.RootView)
	if err != nil {
		t.Fatalf("root view error = %v", err)
	}
	root.Template = view.NewTemplate("SELECT ID, NAME FROM VENDOR")

	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte("module example.com/project\n\ngo 1.25.0\n"), 0o644); err != nil {
		t.Fatalf("write go.mod error = %v", err)
	}
	component.TypeContext.PackagePath = "example.com/project/generated/sample"

	settingsCalls := 0
	contractCalls := 0
	viewSQLCalls := 0
	_, err = GenerateComponentCodegen(CodegenConfig{
		SourcePath: sourcePath,
		DQL:        dql,
		ProjectDir: projectDir,
		APIPrefix:  "/v1/api/custom",
		Hooks: &Hooks{
			Settings: []SettingsHook{
				func(_ context.Context, payload *SettingsPayload) error {
					settingsCalls++
					payload.Component.URI = "/v1/api/hooked"
					return nil
				},
			},
			Contract: []ContractHook{
				func(_ context.Context, payload *ContractPayload) error {
					contractCalls++
					payload.Resource.AddParameters(&state.Parameter{Name: "Limit", In: state.NewQueryLocation("limit"), Schema: &state.Schema{DataType: "int"}})
					return nil
				},
			},
			ViewSQL: []ViewSQLHook{
				func(_ context.Context, payload *ViewSQLPayload) error {
					viewSQLCalls++
					if payload.Root {
						payload.SQL = "SELECT ID FROM VENDOR"
					}
					return nil
				},
			},
		},
	}, resource, component)
	if err != nil {
		t.Fatalf("GenerateComponentCodegen() error = %v", err)
	}

	if settingsCalls != 1 {
		t.Fatalf("settings hook calls = %d, want 1", settingsCalls)
	}
	if contractCalls != 1 {
		t.Fatalf("contract hook calls = %d, want 1", contractCalls)
	}
	if viewSQLCalls == 0 {
		t.Fatalf("view SQL hook was not called")
	}
	if component.URI != "/v1/api/hooked" {
		t.Fatalf("component URI = %q, want hooked URI", component.URI)
	}
	if resource.Parameters.Lookup("Limit") == nil {
		t.Fatalf("contract hook parameter not added")
	}
	if root.Template.Source != "SELECT ID FROM VENDOR" {
		t.Fatalf("root SQL = %q, want rewritten SQL", root.Template.Source)
	}
}

func TestApplyHooks_AppliesWithoutCodegen(t *testing.T) {
	ctx := context.Background()
	dql := `
#setting($_ = $connector('dev'))
#setting($_ = $route('/v1/api/original', 'GET'))
SELECT ID, NAME FROM VENDOR`
	planned, err := shapeCompile.New().Compile(ctx, &shape.Source{
		Name: "sample",
		DQL:  dql,
	}, shape.WithTypeContextPackageDir("generated/sample"), shape.WithTypeContextPackageName("sample"))
	if err != nil {
		t.Fatalf("compile error = %v", err)
	}
	artifact, err := shapeLoad.New().LoadComponent(ctx, planned, shape.WithLoadTypeContextPackages(true))
	if err != nil {
		t.Fatalf("load component error = %v", err)
	}
	component, ok := shapeLoad.ComponentFrom(artifact)
	if !ok || component == nil {
		t.Fatalf("unexpected component artifact %T", artifact.Component)
	}
	resource := artifact.Resource
	root, err := resource.View(component.RootView)
	if err != nil {
		t.Fatalf("root view error = %v", err)
	}
	root.Template = view.NewTemplate("SELECT ID, NAME FROM VENDOR")

	err = ApplyHooks(ctx, &CodegenConfig{
		SourcePath: "sample.dql",
		DQL:        dql,
		APIPrefix:  "/v1/api/custom",
		Hooks: &Hooks{
			Settings: []SettingsHook{
				func(_ context.Context, payload *SettingsPayload) error {
					payload.Component.URI = "/v1/api/hooked"
					return nil
				},
			},
			ViewSQL: []ViewSQLHook{
				func(_ context.Context, payload *ViewSQLPayload) error {
					if payload.Root {
						payload.SQL = "SELECT ID FROM VENDOR"
					}
					return nil
				},
			},
		},
	}, resource, component)
	if err != nil {
		t.Fatalf("ApplyHooks() error = %v", err)
	}
	if component.URI != "/v1/api/hooked" {
		t.Fatalf("component URI = %q, want hooked URI", component.URI)
	}
	if root.Template.Source != "SELECT ID FROM VENDOR" {
		t.Fatalf("root SQL = %q, want rewritten SQL", root.Template.Source)
	}
}

func TestResolveRoute_UsesParser(t *testing.T) {
	method, uri := ResolveRoute("/tmp/sample.dql", "#setting($_ = $route('/v1/api/x', 'POST'))\nSELECT 1", "/v1/api")
	if method != "POST" || uri != "/v1/api/x" {
		t.Fatalf("ResolveRoute() = %s %s", method, uri)
	}
}

func TestPrepareResourceForCodegen_PreservesDeclaredOnlyAndAddsDeps(t *testing.T) {
	resource := &view.Resource{
		Parameters: state.Parameters{
			&state.Parameter{Name: "Body", In: state.NewBodyLocation(""), Schema: &state.Schema{Name: "BodyView"}},
			&state.Parameter{Name: "CurID", In: state.NewParameterLocation("Body"), Schema: &state.Schema{DataType: "int"}},
		},
		Views: []*view.View{
			{
				Name: "root",
				Template: view.NewTemplate("SELECT 1", view.WithTemplateParameters(
					&state.Parameter{Name: "Body", In: state.NewBodyLocation(""), Schema: &state.Schema{Name: "BodyView"}},
				)),
			},
			{
				Name: "child",
				Template: view.NewTemplate("SELECT * WHERE id = $criteria.In(\"ID\", $CurID.Values)",
					view.WithTemplateParameters(
						&state.Parameter{Name: "CurID", In: state.NewParameterLocation("Body"), Schema: &state.Schema{DataType: "int"}},
					),
					view.WithTemplateDeclaredParametersOnly(true),
				),
			},
		},
	}
	component := &shapeLoad.Component{RootView: "root"}

	PrepareResourceForCodegen(resource, component)

	child := resource.Views[1]
	if !child.Template.DeclaredParametersOnly {
		t.Fatalf("child template should remain declared-only")
	}
	if child.Template.Parameters.Lookup("Body") != nil {
		t.Fatalf("declared-only template unexpectedly received Body param")
	}
	if resource.Views[0].Template.Parameters.Lookup("CurID") == nil {
		t.Fatalf("root template did not receive dependent parameter")
	}
}

func TestResolveTypeOutput_UsesModulePath(t *testing.T) {
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte("module example.com/project\n\ngo 1.25.0\n"), 0o644); err != nil {
		t.Fatalf("write go.mod error = %v", err)
	}
	pkgPath, pkgDir, pkgName := ResolveTypeOutput(projectDir, "example.com/project/generated/vendor")
	if pkgPath != "example.com/project/generated/vendorsrc" {
		t.Fatalf("pkgPath = %q", pkgPath)
	}
	if !strings.HasSuffix(filepath.ToSlash(pkgDir), "generated/vendorsrc") {
		t.Fatalf("pkgDir = %q", pkgDir)
	}
	if pkgName != "vendorsrc" {
		t.Fatalf("pkgName = %q", pkgName)
	}
}

func TestGenerateComponentCodegen_LeavesInputShapeStable(t *testing.T) {
	ctx := context.Background()
	sourcePath := filepath.Join(t.TempDir(), "shape.dql")
	dql := "#setting($_ = $connector('dev'))\n#setting($_ = $route('/v1/api/a', 'GET'))\n#define($_ = $ID<int>(query/id))\nSELECT 1 AS ID"
	if err := os.WriteFile(sourcePath, []byte(dql), 0o644); err != nil {
		t.Fatalf("write dql error = %v", err)
	}
	planned, err := shapeCompile.New().Compile(ctx, &shape.Source{Name: "sample", Path: sourcePath, DQL: dql}, shape.WithTypeContextPackageDir("generated/sample"), shape.WithTypeContextPackageName("sample"))
	if err != nil {
		t.Fatalf("compile error = %v", err)
	}
	artifact, err := shapeLoad.New().LoadComponent(ctx, planned, shape.WithLoadTypeContextPackages(true))
	if err != nil {
		t.Fatalf("load error = %v", err)
	}
	component, _ := shapeLoad.ComponentFrom(artifact)
	projectDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(projectDir, "go.mod"), []byte("module example.com/project\n\ngo 1.25.0\n"), 0o644); err != nil {
		t.Fatalf("write go.mod error = %v", err)
	}
	component.TypeContext.PackagePath = "example.com/project/generated/sample"
	before := len(component.InputParameters())
	_, err = GenerateComponentCodegen(CodegenConfig{SourcePath: sourcePath, DQL: dql, ProjectDir: projectDir, Hooks: &Hooks{}}, artifact.Resource, component)
	if err != nil {
		t.Fatalf("GenerateComponentCodegen error = %v", err)
	}
	after := len(component.InputParameters())
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("input parameter count changed: %d -> %d", before, after)
	}
}
