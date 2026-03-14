package command

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/cmd/options"
	"github.com/viant/datly/repository/shape"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view"
	extension "github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"gopkg.in/yaml.v3"
)

func TestPatchBasicOne_LoadedComponentHasMutableExecHelpers(t *testing.T) {
	source := filepath.Join("..", "..", "e2e", "v1", "dql", "dev", "events", "patch_basic_one.dql")
	data, err := os.ReadFile(source)
	require.NoError(t, err)

	planned, err := shapeCompile.New().Compile(context.Background(), &shape.Source{
		Name: "patch_basic_one",
		Path: source,
		DQL:  string(data),
	})
	require.NoError(t, err)

	artifact, err := shapeLoad.New().LoadComponent(context.Background(), planned)
	require.NoError(t, err)

	component, ok := shapeLoad.ComponentFrom(artifact)
	require.True(t, ok)
	require.NotNil(t, component)

	root := lookupNamedView(artifact.Resource, component.RootView)
	require.NotNil(t, root)
	assert.Equal(t, view.ModeExec, root.Mode)
	require.NotNil(t, root.Template)
	assert.True(t, root.Template.UseParameterStateType)
	require.NotNil(t, root.Template.Parameters.Lookup("CurFoosId"))
	require.NotNil(t, root.Template.Parameters.Lookup("CurFoos"))
	assert.Equal(t, state.Many, root.Template.Parameters.Lookup("CurFoos").Schema.Cardinality)

	input := component.InputParameters()
	require.Nil(t, input.Lookup("CurFoosId"))
	require.Nil(t, input.Lookup("CurFoos"))

	curFoos, err := artifact.Resource.View("CurFoos")
	require.NoError(t, err)
	require.NotNil(t, curFoos)
	require.NotNil(t, curFoos.Template)
	assert.Equal(t, "foos/cur_foos.sql", curFoos.Template.SourceURL)
}

func TestPatchBasicOne_LoadedComponentHasMutableExecHelpers_WithTypeContextPackages(t *testing.T) {
	source := filepath.Join("..", "..", "e2e", "v1", "dql", "dev", "events", "patch_basic_one.dql")
	data, err := os.ReadFile(source)
	require.NoError(t, err)

	planned, err := shapeCompile.New().Compile(context.Background(), &shape.Source{
		Name: "patch_basic_one",
		Path: source,
		DQL:  string(data),
	}, transcribeCompileOptions(&options.Transcribe{
		Project:    filepath.Join("..", "..", "e2e", "v1"),
		Module:     filepath.Join("..", "..", "e2e", "v1"),
		TypeOutput: filepath.Join("..", "..", "e2e", "v1", "shape"),
		Namespace:  "dev/basic/foos",
	})...)
	require.NoError(t, err)

	artifact, err := shapeLoad.New().LoadComponent(context.Background(), planned, shape.WithLoadTypeContextPackages(true))
	require.NoError(t, err)

	component, ok := shapeLoad.ComponentFrom(artifact)
	require.True(t, ok)
	require.NotNil(t, component)

	root := lookupNamedView(artifact.Resource, component.RootView)
	require.NotNil(t, root)
	require.NotNil(t, root.Template)
	require.NotNil(t, root.Template.Parameters.Lookup("CurFoos"))
	assert.Equal(t, state.Many, root.Template.Parameters.Lookup("CurFoos").Schema.Cardinality)

	curFoos, err := artifact.Resource.View("CurFoos")
	require.NoError(t, err)
	require.NotNil(t, curFoos)
	require.NotNil(t, curFoos.Template)
	require.True(t, curFoos.Template.DeclaredParametersOnly)
	require.NotNil(t, curFoos.Template.Parameters.Lookup("CurFoosId"))
	require.Nil(t, curFoos.Template.Parameters.Lookup("Foos"))
}

func TestTranscribeSharedResourceRefs_IncludesConnectors(t *testing.T) {
	resource := &view.Resource{
		Connectors: []*view.Connector{view.NewRefConnector("dev")},
	}

	refs := transcribeSharedResourceRefs(resource)
	require.Equal(t, []string{view.ResourceConnectors}, refs)
}

func TestEnsureSharedResourceRefsYAML_AppendsWith(t *testing.T) {
	data, err := ensureSharedResourceRefsYAML([]byte("Resource: {}\nRoutes: []\n"), []string{view.ResourceConnectors})
	require.NoError(t, err)
	assert.True(t, strings.Contains(string(data), "With:\n    - connectors"))
}

func TestNormalizeParameterTypeNameTags_AppendsTypeName(t *testing.T) {
	params := state.Parameters{
		&state.Parameter{
			Name:   "Foos",
			Tag:    `anonymous:"true"`,
			Schema: &state.Schema{Name: "FoosView"},
		},
	}

	normalized := normalizeParameterTypeNameTags(params)
	require.Len(t, normalized, 1)
	assert.Equal(t, `anonymous:"true" typeName:"FoosView"`, normalized[0].Tag)
}

func TestPreserveTemplateParameters_AppendsMutableHelpers(t *testing.T) {
	aView := &view.View{
		Name: "foos",
		Template: view.NewTemplate("SELECT 1", view.WithTemplateParameters(
			&state.Parameter{Name: "Foos", In: state.NewBodyLocation(""), Schema: &state.Schema{Name: "FoosView"}},
		)),
	}

	params := state.Parameters{
		&state.Parameter{Name: "Foos", In: state.NewBodyLocation(""), Schema: &state.Schema{Name: "FoosView"}},
		&state.Parameter{Name: "CurFoosId", In: state.NewParameterLocation("Foos"), Schema: &state.Schema{DataType: "int"}},
		&state.Parameter{Name: "CurFoos", In: state.NewViewLocation("CurFoos"), Schema: &state.Schema{Name: "FoosView"}},
		&state.Parameter{Name: "Meta", In: state.NewOutputLocation("summary"), Schema: &state.Schema{Name: "MetaView"}},
	}

	preserveTemplateParameters(aView, params)

	require.NotNil(t, aView.Template.Parameters.Lookup("Foos"))
	require.NotNil(t, aView.Template.Parameters.Lookup("CurFoosId"))
	require.NotNil(t, aView.Template.Parameters.Lookup("CurFoos"))
	require.Nil(t, aView.Template.Parameters.Lookup("Meta"))
}

func TestPreserveTemplateParameters_SkipsDeclaredOnlyTemplate(t *testing.T) {
	aView := &view.View{
		Name: "CurFoos",
		Template: view.NewTemplate("SELECT 1",
			view.WithTemplateParameters(
				&state.Parameter{Name: "CurFoosId", In: state.NewParameterLocation("Foos"), Schema: &state.Schema{DataType: "int"}},
			),
			view.WithTemplateDeclaredParametersOnly(true),
		),
	}

	params := state.Parameters{
		&state.Parameter{Name: "Foos", In: state.NewBodyLocation(""), Schema: &state.Schema{Name: "FoosView"}},
	}

	preserveTemplateParameters(aView, params)

	require.NotNil(t, aView.Template.Parameters.Lookup("CurFoosId"))
	require.Nil(t, aView.Template.Parameters.Lookup("Foos"))
}

func TestPrepareResourceForTranscribeCodegen_DeclaredOnlyTemplateKeepsOnlyUsedParams(t *testing.T) {
	resource := &view.Resource{
		Parameters: state.Parameters{
			&state.Parameter{Name: "Foos", In: state.NewBodyLocation(""), Schema: &state.Schema{Name: "FoosView"}},
			&state.Parameter{Name: "CurFoosId", In: state.NewParameterLocation("Foos"), Schema: &state.Schema{DataType: "int"}},
		},
		Views: []*view.View{
			{
				Name: "foos",
				Template: view.NewTemplate("SELECT 1", view.WithTemplateParameters(
					&state.Parameter{Name: "Foos", In: state.NewBodyLocation(""), Schema: &state.Schema{Name: "FoosView"}},
				)),
			},
			{
				Name: "CurFoos",
				Template: view.NewTemplate(`SELECT * FROM FOOS WHERE $criteria.In("ID", $CurFoosId.Values)`,
					view.WithTemplateParameters(
						&state.Parameter{Name: "CurFoosId", In: state.NewParameterLocation("Foos"), Schema: &state.Schema{DataType: "int"}},
					),
					view.WithTemplateDeclaredParametersOnly(true),
				),
			},
		},
	}
	component := &shapeLoad.Component{RootView: "foos"}

	prepareResourceForTranscribeCodegen(resource, component)

	curFoos := lookupNamedView(resource, "CurFoos")
	require.NotNil(t, curFoos)
	require.NotNil(t, curFoos.Template)
	require.NotNil(t, curFoos.Template.Parameters.Lookup("CurFoosId"))
	require.Nil(t, curFoos.Template.Parameters.Lookup("Foos"))
}

func TestDependentTemplateParameters_AppendsParentSourceParameter(t *testing.T) {
	params := state.Parameters{
		&state.Parameter{Name: "CurFoosId", In: state.NewParameterLocation("Foos"), Schema: &state.Schema{DataType: "int"}},
	}
	resourceParams := state.Parameters{
		&state.Parameter{Name: "Foos", In: state.NewBodyLocation(""), Schema: &state.Schema{Name: "FoosView"}},
		&state.Parameter{Name: "CurFoosId", In: state.NewParameterLocation("Foos"), Schema: &state.Schema{DataType: "int"}},
	}

	deps := dependentTemplateParameters(params, resourceParams)

	require.Len(t, deps, 1)
	require.Equal(t, "Foos", deps[0].Name)
}

func TestAlignGeneratedPackageAliases_UsesGeneratedPackageName(t *testing.T) {
	const pkgPath = "github.com/viant/datly/e2e/v1/shape/dev/events/patch_basic_one"
	resource := &view.Resource{
		Views: []*view.View{
			{
				Name: "foos",
				Schema: &state.Schema{
					Package:     "foos",
					PackagePath: pkgPath,
					Name:        "FoosView",
					DataType:    "*FoosView",
					Cardinality: state.Many,
				},
				Template: view.NewTemplate("SELECT 1", view.WithTemplateParameters(
					&state.Parameter{
						Name:   "Foos",
						Schema: &state.Schema{Package: "foos", PackagePath: pkgPath, Name: "FoosView", DataType: "*FoosView", Cardinality: state.One},
					},
				)),
			},
		},
		Types: []*view.TypeDefinition{
			{
				Name:       "FoosView",
				Package:    "foos",
				ModulePath: pkgPath,
				Schema:     &state.Schema{Package: "foos", PackagePath: pkgPath, Name: "FoosView", DataType: "*FoosView", Cardinality: state.Many},
			},
		},
		Parameters: state.Parameters{
			&state.Parameter{
				Name:   "Foos",
				Schema: &state.Schema{Package: "foos", PackagePath: pkgPath, Name: "FoosView", DataType: "*FoosView", Cardinality: state.One},
			},
		},
	}
	component := &shapeLoad.Component{
		TypeContext: &typectx.Context{PackageName: "foos", PackagePath: pkgPath},
		Input: []*plan.State{
			{Parameter: state.Parameter{Name: "Foos", Schema: &state.Schema{Package: "foos", PackagePath: pkgPath, Name: "FoosView", DataType: "*FoosView", Cardinality: state.One}}},
		},
	}

	alignGeneratedPackageAliases(resource, component, filepath.Join("..", "..", "e2e", "v1", "shape", "dev", "events", "patch_basic_one"), pkgPath, "patch_basic_one")

	require.Equal(t, "patch_basic_one", component.TypeContext.PackageName)
	require.Equal(t, filepath.ToSlash(filepath.Clean(filepath.Join("..", "..", "e2e", "v1", "shape", "dev", "events", "patch_basic_one"))), component.TypeContext.PackageDir)
	require.Equal(t, "patch_basic_one", component.Input[0].Schema.Package)
	require.Equal(t, "*patch_basic_one.FoosView", component.Input[0].Schema.DataType)
	require.Equal(t, "patch_basic_one", resource.Views[0].Schema.Package)
	require.Equal(t, "*patch_basic_one.FoosView", resource.Views[0].Schema.DataType)
	require.Equal(t, "patch_basic_one", resource.Views[0].Template.Parameters[0].Schema.Package)
	require.Equal(t, "*patch_basic_one.FoosView", resource.Views[0].Template.Parameters[0].Schema.DataType)
	require.Equal(t, "patch_basic_one", resource.Parameters[0].Schema.Package)
	require.Equal(t, "*patch_basic_one.FoosView", resource.Parameters[0].Schema.DataType)
	require.Equal(t, "patch_basic_one", resource.Types[0].Package)
	require.Equal(t, "patch_basic_one", resource.Types[0].Schema.Package)
	require.Equal(t, "*patch_basic_one.FoosView", resource.Types[0].Schema.DataType)
}

func TestGenerateTranscribeTypes_RealignsGeneratedPackageAlias(t *testing.T) {
	source := filepath.Join("..", "..", "e2e", "v1", "dql", "dev", "events", "patch_basic_one.dql")
	data, err := os.ReadFile(source)
	require.NoError(t, err)

	planned, err := shapeCompile.New().Compile(context.Background(), &shape.Source{
		Name:      "patch_basic_one",
		Path:      source,
		DQL:       string(data),
		Connector: "dev",
	}, transcribeCompileOptions(&options.Transcribe{
		Project:    filepath.Join("..", "..", "e2e", "v1"),
		Module:     filepath.Join("..", "..", "e2e", "v1"),
		TypeOutput: filepath.Join("..", "..", "e2e", "v1", "shape"),
		Namespace:  "dev/basic/foos",
	})...)
	require.NoError(t, err)

	artifact, err := shapeLoad.New().LoadComponent(context.Background(), planned, shape.WithLoadTypeContextPackages(true))
	require.NoError(t, err)
	component, ok := shapeLoad.ComponentFrom(artifact)
	require.True(t, ok)

	svc := &Service{}
	result, err := svc.generateTranscribeTypes(source, string(data), &options.Transcribe{
		Project:    filepath.Join("..", "..", "e2e", "v1"),
		Module:     filepath.Join("..", "..", "e2e", "v1"),
		TypeOutput: filepath.Join("..", "..", "e2e", "v1", "shape"),
		Namespace:  "dev/basic/foos",
	}, artifact.Resource, component)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "patch_basic_one", result.PackageName)

	alignGeneratedPackageAliases(artifact.Resource, component, result.PackageDir, result.PackagePath, result.PackageName)

	root := lookupNamedView(artifact.Resource, component.RootView)
	require.NotNil(t, root)
	require.Equal(t, result.PackageName, root.Schema.Package)
	require.Equal(t, "*patch_basic_one.FoosView", root.Schema.DataType)
	require.Equal(t, result.PackageName, artifact.Resource.Parameters.Lookup("Foos").Schema.Package)
	require.Equal(t, "*patch_basic_one.FoosView", artifact.Resource.Parameters.Lookup("Foos").Schema.DataType)
}

func TestTranscribe_PatchBasicOneRouteYAMLUsesGeneratedPackageName(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)
	repoRoot := filepath.Clean(filepath.Join(cwd, "..", ".."))
	project := filepath.Join(repoRoot, "e2e", "v1")
	tempRepo := t.TempDir()

	svc := New()
	err = svc.Transcribe(context.Background(), &options.Options{
		Transcribe: &options.Transcribe{
			Source:     []string{filepath.Join(project, "dql", "dev", "events", "patch_basic_one.dql")},
			Repository: tempRepo,
			Project:    project,
			Module:     project,
			TypeOutput: filepath.Join(project, "shape"),
			Namespace:  "dev/basic/foos",
			APIPrefix:  "/v1/api/shape",
		},
	})
	require.NoError(t, err)

	routeYAML := filepath.Join(tempRepo, "Datly", "routes", "patch_basic_one.yaml")
	data, err := os.ReadFile(routeYAML)
	require.NoError(t, err)
	text := string(data)
	require.Contains(t, text, "Package: patch_basic_one")
	require.Contains(t, text, "DataType: '*patch_basic_one.FoosView'")
	require.Contains(t, text, "caseformat: lowerCamel")
	require.NotContains(t, text, "Package: foos\n")
}

func TestTranscribe_PatchBasicOneRouteYAMLBuildsNamedTemplateState(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)
	repoRoot := filepath.Clean(filepath.Join(cwd, "..", ".."))
	project := filepath.Join(repoRoot, "e2e", "v1")
	tempRepo := t.TempDir()

	svc := New()
	err = svc.Transcribe(context.Background(), &options.Options{
		Transcribe: &options.Transcribe{
			Source:     []string{filepath.Join(project, "dql", "dev", "events", "patch_basic_one.dql")},
			Repository: tempRepo,
			Project:    project,
			Module:     project,
			TypeOutput: filepath.Join(project, "shape"),
			Namespace:  "dev/basic/foos",
			APIPrefix:  "/v1/api/shape",
		},
	})
	require.NoError(t, err)

	routeYAML := filepath.Join(tempRepo, "Datly", "routes", "patch_basic_one.yaml")
	data, err := os.ReadFile(routeYAML)
	require.NoError(t, err)

	currentDir, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(filepath.Dir(routeYAML)))
	defer func() {
		_ = os.Chdir(currentDir)
	}()

	payload := &shapeRuleFile{}
	require.NoError(t, yaml.Unmarshal(data, payload))
	require.NotNil(t, payload.Resource)
	payload.Resource.Connectors = nil
	payload.Resource.AddConnector("dev", "sqlite3", "file::memory:?cache=shared")
	rootView := lookupNamedView(payload.Resource, "foos")
	require.NotNil(t, rootView)
	rootView.Connector = view.NewRefConnector("dev")
	curFoosView := lookupNamedView(payload.Resource, "CurFoos")
	require.NotNil(t, curFoosView)
	curFoosView.Connector = view.NewRefConnector("dev")
	payload.Resource.SetTypes(extension.Config.Types)
	require.NoError(t, payload.Resource.Init(context.Background(), payload.Resource.TypeRegistry(), extension.Config.Codecs, nil, nil, extension.Config.Predicates))

	root := lookupNamedView(payload.Resource, "foos")
	require.NotNil(t, root)
	require.NotNil(t, root.Template)
	require.NotNil(t, root.Template.StateType())

	rType := root.Template.StateType().Type()
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	field, ok := rType.FieldByName("Foos")
	require.True(t, ok)
	require.Equal(t, reflect.Ptr, field.Type.Kind())
	require.Equal(t, reflect.Struct, field.Type.Elem().Kind())
	require.Contains(t, field.Type.String(), "NAME")
	require.Contains(t, field.Type.String(), "QUANTITY")
	require.Contains(t, field.Type.String(), "ID")
}

func TestTranscribe_PatchBasicOneRouteYAMLPreservesNamedHelperParamTypes(t *testing.T) {
	cwd, err := os.Getwd()
	require.NoError(t, err)
	repoRoot := filepath.Clean(filepath.Join(cwd, "..", ".."))
	project := filepath.Join(repoRoot, "e2e", "v1")
	tempRepo := t.TempDir()

	svc := New()
	err = svc.Transcribe(context.Background(), &options.Options{
		Transcribe: &options.Transcribe{
			Source:     []string{filepath.Join(project, "dql", "dev", "events", "patch_basic_one.dql")},
			Repository: tempRepo,
			Project:    project,
			Module:     project,
			TypeOutput: filepath.Join(project, "shape"),
			Namespace:  "dev/basic/foos",
			APIPrefix:  "/v1/api/shape",
		},
	})
	require.NoError(t, err)

	routeYAML := filepath.Join(tempRepo, "Datly", "routes", "patch_basic_one.yaml")
	data, err := os.ReadFile(routeYAML)
	require.NoError(t, err)

	payload := &shapeRuleFile{}
	require.NoError(t, yaml.Unmarshal(data, payload))
	require.NotNil(t, payload.Resource)

	curFoosID := payload.Resource.Parameters.Lookup("CurFoosId")
	require.NotNil(t, curFoosID)
	require.NotNil(t, curFoosID.Schema)
	require.Equal(t, "*patch_basic_one.FoosView", curFoosID.Schema.DataType)

	require.NotNil(t, curFoosID.Output)
	require.NotNil(t, curFoosID.Output.Schema)
	require.Equal(t, `*struct { Values []int "json:\",omitempty\"" }`, curFoosID.Output.Schema.DataType)

	curFoos := lookupNamedView(payload.Resource, "CurFoos")
	require.NotNil(t, curFoos)
	require.NotNil(t, curFoos.Template)
	curFoosParam := curFoos.Template.Parameters.Lookup("CurFoosId")
	require.NotNil(t, curFoosParam)
	require.NotNil(t, curFoosParam.Schema)
	require.Equal(t, "*patch_basic_one.FoosView", curFoosParam.Schema.DataType)
}

func TestGenerateTranscribeTypes_MetaFormatPreservesChildSummaryType(t *testing.T) {
	source := filepath.Join("..", "..", "e2e", "v1", "dql", "dev", "vendorsrv", "meta_format.dql")
	data, err := os.ReadFile(source)
	require.NoError(t, err)

	project := filepath.Join("..", "..", "e2e", "v1")
	shapeOutput := filepath.Join(project, "shape")
	transcribeOpts := &options.Transcribe{
		Project:    project,
		Module:     project,
		TypeOutput: shapeOutput,
		Namespace:  "dev/vendor/meta-format",
	}
	transcribeOpts.Connectors = []string{"dev|mysql|root:dev@tcp(localhost:3306)/dev?parseTime=true"}

	planned, err := shapeCompile.New().Compile(context.Background(), &shape.Source{
		Name:      "meta_format",
		Path:      source,
		DQL:       string(data),
		Connector: "dev",
	}, transcribeCompileOptions(transcribeOpts)...)
	require.NoError(t, err)

	artifact, err := shapeLoad.New().LoadComponent(context.Background(), planned, shape.WithLoadTypeContextPackages(true))
	require.NoError(t, err)
	component, ok := shapeLoad.ComponentFrom(artifact)
	require.True(t, ok)

	applyConnectorsToResource(artifact.Resource, transcribeOpts.Connectors)
	discoverColumns(context.Background(), artifact.Resource)
	prepareResourceForTranscribeCodegen(artifact.Resource, component)

	products := lookupNamedView(artifact.Resource, "products")
	require.NotNil(t, products)
	require.NotNil(t, products.Template)
	require.NotNil(t, products.Template.Summary)
	require.NotNil(t, products.Template.Summary.Schema)
	summaryType := products.Template.Summary.Schema.Type()
	require.NotNil(t, summaryType)
	if summaryType.Kind() == reflect.Ptr {
		summaryType = summaryType.Elem()
	}
	field, ok := summaryType.FieldByName("VendorId")
	require.True(t, ok)
	require.Equal(t, reflect.TypeOf((*int)(nil)), field.Type)

	svc := &Service{}
	result, err := svc.generateTranscribeTypes(source, string(data), transcribeOpts, artifact.Resource, component)
	require.NoError(t, err)
	require.NotNil(t, result)

	outputSource, err := os.ReadFile(result.OutputFilePath)
	require.NoError(t, err)
	assert.Contains(t, string(outputSource), `type ProductsMetaView struct {`)
	assert.Contains(t, string(outputSource), `VendorId *int`)
	assert.NotContains(t, string(outputSource), `VendorId string`)
}
