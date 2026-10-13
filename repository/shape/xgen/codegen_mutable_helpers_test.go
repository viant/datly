package xgen

import (
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	shapeload "github.com/viant/datly/repository/shape/load"
	shapeplan "github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/shared"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

type BasicFoos struct {
	Id   *int
	Name *string
}

type Foos struct {
	Id              *int
	Name            *string
	FoosPerformance []*FoosPerformance `view:",table=FOOS_PERFORMANCE" on:"Id:ID=FooId:FOO_ID"`
}

type FoosPerformance struct {
	Id    *int
	FooId *int
	Name  *string
}

func TestComponentCodegen_MutableComponent_GeneratesPatchHelpers(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "generate_patch_basic_one")

	component := &shapeload.Component{
		Method:   "PATCH",
		URI:      "/v1/api/dev/basic/foos",
		RootView: "Foos",
		Input: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "Foos",
					In:     state.NewBodyLocation(""),
					Tag:    `anonymous:"true"`,
					Schema: &state.Schema{Name: "Foos", DataType: "*Foos", Cardinality: state.One},
				},
			},
			{
				Parameter: state.Parameter{
					Name:   "CurFoosId",
					In:     state.NewParameterLocation("Foos"),
					Schema: state.NewSchema(reflect.TypeOf(&struct{ Values []int }{})),
					Tag:    `codec:"structql,uri=foos/cur_foos_id.sql"`,
				},
			},
			{
				Parameter: state.Parameter{
					Name:   "CurFoos",
					In:     state.NewViewLocation("CurFoos"),
					Tag:    `view:"CurFoos" sql:"uri=foos/cur_foos.sql"`,
					Schema: &state.Schema{Name: "Foos", DataType: "*Foos", Cardinality: state.Many},
				},
			},
		},
		Output: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "Foos",
					In:     state.NewOutputLocation("body"),
					Tag:    `anonymous:"true"`,
					Schema: &state.Schema{Name: "Foos", DataType: "*Foos", Cardinality: state.One},
				},
			},
		},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views,
		&view.View{
			Name: "Foos",
			Mode: view.ModeExec,
			Schema: &state.Schema{
				Name:        "Foos",
				DataType:    "*Foos",
				Cardinality: state.One,
			},
			Columns: []*view.Column{
				{Name: "ID", DataType: "int"},
				{Name: "NAME", DataType: "string", Nullable: true},
			},
		},
		&view.View{
			Name: "CurFoos",
			Mode: view.ModeQuery,
			Schema: &state.Schema{
				Name:        "Foos",
				DataType:    "*Foos",
				Cardinality: state.Many,
			},
			Columns: []*view.Column{
				{Name: "ID", DataType: "int"},
				{Name: "NAME", DataType: "string", Nullable: true},
			},
		},
	)

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "generate_patch_basic_one",
		PackagePath: "github.com/acme/project/shape/dev/generate_patch_basic_one",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	inputSource := mustReadCodegenFile(t, result.InputFilePath)
	if !strings.Contains(inputSource, `CurFoosById map[int]Foos`) {
		t.Fatalf("expected generated input to include indexed helper map:\n%s", inputSource)
	}
	if !strings.Contains(inputSource, "CurFoosId *struct {") || !strings.Contains(inputSource, "Values []int") {
		t.Fatalf("expected generated input to preserve helper ids struct type:\n%s", inputSource)
	}

	initSource := mustReadCodegenFile(t, filepath.Join(packageDir, "input_init.go"))
	if !strings.Contains(initSource, `i.CurFoosById = make(map[int]Foos, len(i.CurFoos))`) {
		t.Fatalf("expected generated init helper to allocate CurFoosById:\n%s", initSource)
	}
	if !strings.Contains(initSource, `i.CurFoosById[item.Id] = item`) {
		t.Fatalf("expected generated init helper to populate CurFoosById:\n%s", initSource)
	}

	validateSource := mustReadCodegenFile(t, filepath.Join(packageDir, "input_validate.go"))
	if !strings.Contains(validateSource, `_, err := aValidator.Validate(ctx, value, append(options, validator.WithValidation(validation))...)`) {
		t.Fatalf("expected generated validate helper to call validator service:\n%s", validateSource)
	}
	if !strings.Contains(validateSource, `case Foos:`) || !strings.Contains(validateSource, `_, ok := i.CurFoosById[actual.Id]`) {
		t.Fatalf("expected generated validate helper to use CurFoosById marker provider:\n%s", validateSource)
	}

	outputSource := mustReadCodegenFile(t, result.OutputFilePath)
	if !strings.Contains(outputSource, `response.Status `+"`"+`parameter:",kind=output,in=status" json:",omitempty"`+"`") {
		t.Fatalf("expected mutable output to embed response status:\n%s", outputSource)
	}
	if !strings.Contains(outputSource, `Violations validator.Violations `+"`"+`json:",omitempty"`+"`") {
		t.Fatalf("expected mutable output to include validation violations:\n%s", outputSource)
	}
	if !strings.Contains(outputSource, `func (o *FoosOutput) setError(err error) {`) {
		t.Fatalf("expected mutable output to include setError helper:\n%s", outputSource)
	}

	if result.VeltyFilePath == "" {
		t.Fatalf("expected mutable component to emit velty artifact path")
	}
	veltySource := mustReadCodegenFile(t, result.VeltyFilePath)
	for _, fragment := range []string{
		`$sequencer.Allocate("FOOS", $Unsafe.Foos, "Id")`,
		`#set($CurFoosById = $Unsafe.CurFoos.IndexBy("Id"))`,
		`$sql.Update($Unsafe.Foos, "FOOS");`,
		`$sql.Insert($Unsafe.Foos, "FOOS");`,
	} {
		if !strings.Contains(veltySource, fragment) {
			t.Fatalf("expected generated velty body to include %q:\n%s", fragment, veltySource)
		}
	}
	foundVelty := false
	for _, generated := range result.GeneratedFiles {
		if generated == result.VeltyFilePath {
			foundVelty = true
			break
		}
	}
	if !foundVelty {
		t.Fatalf("expected generated files to include velty artifact: %v", result.GeneratedFiles)
	}
	curIDsPath := filepath.Join(packageDir, "foos", "cur_foos_id.sql")
	if _, err := os.Stat(curIDsPath); err != nil {
		t.Fatalf("expected generated current-ids SQL at %s, files=%v", curIDsPath, result.GeneratedFiles)
	}
	curIDsSQL := mustReadCodegenFile(t, curIDsPath)
	if !strings.Contains(curIDsSQL, `SELECT ARRAY_AGG(Id) AS Values`) {
		t.Fatalf("expected generated current-ids SQL:\n%s", curIDsSQL)
	}
	curViewPath := filepath.Join(packageDir, "foos", "cur_foos.sql")
	if _, err := os.Stat(curViewPath); err != nil {
		t.Fatalf("expected generated current-view SQL at %s, files=%v", curViewPath, result.GeneratedFiles)
	}
	curViewSQL := mustReadCodegenFile(t, curViewPath)
	if !strings.Contains(curViewSQL, `SELECT * FROM FOOS`) {
		t.Fatalf("expected generated current-view SQL:\n%s", curViewSQL)
	}
}

func TestComponentCodegen_MutableComponent_DSQLParity_BasicOne(t *testing.T) {
	result, packageDir := generateMutableFixture(t, mutableFixtureSpec{
		packageName:  "generate_patch_basic_one",
		method:       "PATCH",
		uri:          "/v1/api/dev/basic/foos",
		bodySchema:   &state.Schema{Name: "Foos", DataType: "*Foos", Cardinality: state.One},
		outputSchema: &state.Schema{Name: "Foos", DataType: "*Foos", Cardinality: state.One},
		views: []*view.View{
			{
				Name:      "Foos",
				Mode:      view.ModeExec,
				Connector: &view.Connector{Connection: view.Connection{DBConfig: view.DBConfig{Reference: shared.Reference{Ref: "dev"}}}},
				Schema: func() *state.Schema {
					s := state.NewSchema(reflect.TypeOf(&BasicFoos{}))
					s.Name, s.DataType, s.Cardinality = "Foos", "*Foos", state.One
					return s
				}(),
				Columns: []*view.Column{{Name: "ID", DataType: "int"}, {Name: "NAME", DataType: "string", Nullable: true}},
			},
			{
				Name:     "CurFoos",
				Mode:     view.ModeQuery,
				Template: &view.Template{Source: "SELECT * FROM FOOS\nWHERE $criteria.In(\"ID\", $Unsafe.CurFoosId.Values)"},
				Schema: func() *state.Schema {
					s := state.NewSchema(reflect.TypeOf(&BasicFoos{}))
					s.Name, s.DataType, s.Cardinality = "Foos", "*Foos", state.Many
					return s
				}(),
				Columns: []*view.Column{{Name: "ID", DataType: "int"}, {Name: "NAME", DataType: "string", Nullable: true}},
			},
		},
		extraInput: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "CurFoosId",
					In:     state.NewParameterLocation("Foos"),
					Schema: state.NewSchema(reflect.TypeOf(&struct{ Values []int }{})),
					Tag:    `codec:"structql,uri=foos/cur_foos_id.sql"`,
				},
			},
			{
				Parameter: state.Parameter{
					Name:   "CurFoos",
					In:     state.NewViewLocation("CurFoos"),
					Tag:    `view:"CurFoos" sql:"uri=foos/cur_foos.sql"`,
					Schema: &state.Schema{Name: "Foos", DataType: "*Foos", Cardinality: state.Many},
				},
			},
		},
	})
	assertMutableDSQLParity(t, result.VeltyFilePath, "/Users/awitas/go/src/github.com/viant/datly/e2e/local/dql/generate_patch_basic_one/patch_basic_one.sql")
	assertMutableSQLFileParity(t, filepath.Join(packageDir, "foos", "cur_foos_id.sql"), "/Users/awitas/go/src/github.com/viant/datly/e2e/local/pkg/dev/generate_patch_basic_one/foos/cur_foos_id.sql")
	assertMutableSQLFileParity(t, filepath.Join(packageDir, "foos", "cur_foos.sql"), "/Users/awitas/go/src/github.com/viant/datly/e2e/local/pkg/dev/generate_patch_basic_one/foos/cur_foos.sql")
}

func TestComponentCodegen_MutableComponent_DSQLParity_BasicMany(t *testing.T) {
	result, packageDir := generateMutableFixture(t, mutableFixtureSpec{
		packageName:  "generate_patch_basic_many",
		method:       "PATCH",
		uri:          "/v1/api/dev/basic/foos-many",
		bodySchema:   &state.Schema{Name: "Foos", DataType: "*Foos", Cardinality: state.Many},
		outputSchema: &state.Schema{Name: "Foos", DataType: "*Foos", Cardinality: state.Many},
		views: []*view.View{
			{
				Name:      "Foos",
				Mode:      view.ModeExec,
				Connector: &view.Connector{Connection: view.Connection{DBConfig: view.DBConfig{Reference: shared.Reference{Ref: "dev"}}}},
				Schema: func() *state.Schema {
					s := state.NewSchema(reflect.TypeOf(&BasicFoos{}))
					s.Name, s.DataType, s.Cardinality = "Foos", "*Foos", state.Many
					return s
				}(),
				Columns: []*view.Column{{Name: "ID", DataType: "int"}, {Name: "NAME", DataType: "string", Nullable: true}},
			},
			{
				Name:     "CurFoos",
				Mode:     view.ModeQuery,
				Template: &view.Template{Source: "SELECT * FROM FOOS\nWHERE $criteria.In(\"ID\", $Unsafe.CurFoosId.Values)"},
				Schema: func() *state.Schema {
					s := state.NewSchema(reflect.TypeOf(&BasicFoos{}))
					s.Name, s.DataType, s.Cardinality = "Foos", "*Foos", state.Many
					return s
				}(),
				Columns: []*view.Column{{Name: "ID", DataType: "int"}, {Name: "NAME", DataType: "string", Nullable: true}},
			},
		},
		extraInput: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "CurFoosId",
					In:     state.NewParameterLocation("Foos"),
					Schema: state.NewSchema(reflect.TypeOf(&struct{ Values []int }{})),
					Tag:    `codec:"structql,uri=foos/cur_foos_id.sql"`,
				},
			},
			{
				Parameter: state.Parameter{
					Name:   "CurFoos",
					In:     state.NewViewLocation("CurFoos"),
					Tag:    `view:"CurFoos" sql:"uri=foos/cur_foos.sql"`,
					Schema: &state.Schema{Name: "Foos", DataType: "*Foos", Cardinality: state.Many},
				},
			},
		},
	})
	assertMutableDSQLParity(t, result.VeltyFilePath, "/Users/awitas/go/src/github.com/viant/datly/e2e/local/dql/generate_patch_basic_many/patch_basic_many.sql")
	assertMutableSQLFileParity(t, filepath.Join(packageDir, "foos", "cur_foos_id.sql"), "/Users/awitas/go/src/github.com/viant/datly/e2e/local/pkg/dev/generate_patch_basic_many/foos/cur_foos_id.sql")
	assertMutableSQLFileParity(t, filepath.Join(packageDir, "foos", "cur_foos.sql"), "/Users/awitas/go/src/github.com/viant/datly/e2e/local/pkg/dev/generate_patch_basic_many/foos/cur_foos.sql")
}

func TestComponentCodegen_MutableComponent_DSQLParity_ManyMany(t *testing.T) {
	result, packageDir := generateMutableFixture(t, mutableFixtureSpec{
		packageName:  "generate_patch_many_many",
		method:       "PATCH",
		uri:          "/v1/api/dev/basic/foos-many-many",
		bodySchema:   &state.Schema{Name: "Foos", DataType: "*Foos", Cardinality: state.Many},
		outputSchema: &state.Schema{Name: "Foos", DataType: "*Foos", Cardinality: state.Many},
		views: []*view.View{
			{
				Name:      "Foos",
				Mode:      view.ModeExec,
				Connector: &view.Connector{Connection: view.Connection{DBConfig: view.DBConfig{Reference: shared.Reference{Ref: "dev"}}}},
				Schema: func() *state.Schema {
					s := state.NewSchema(reflect.TypeOf(&Foos{}))
					s.Name, s.DataType, s.Cardinality = "Foos", "*Foos", state.Many
					return s
				}(),
				Columns: []*view.Column{{Name: "ID", DataType: "int"}, {Name: "NAME", DataType: "string", Nullable: true}},
			},
			{
				Name:     "CurFoos",
				Mode:     view.ModeQuery,
				Template: &view.Template{Source: "SELECT * FROM FOOS\nWHERE $criteria.In(\"ID\", $Unsafe.CurFoosId.Values)"},
				Schema: func() *state.Schema {
					s := state.NewSchema(reflect.TypeOf(&Foos{}))
					s.Name, s.DataType, s.Cardinality = "Foos", "*Foos", state.Many
					return s
				}(),
				Columns: []*view.Column{{Name: "ID", DataType: "int"}, {Name: "NAME", DataType: "string", Nullable: true}},
			},
			{
				Name:     "CurFoosPerformance",
				Mode:     view.ModeQuery,
				Template: &view.Template{Source: "SELECT * FROM FOOS_PERFORMANCE\nWHERE $criteria.In(\"ID\", $Unsafe.CurFoosFoosPerformanceId.Values)"},
				Schema: func() *state.Schema {
					s := state.NewSchema(reflect.TypeOf(&FoosPerformance{}))
					s.Name, s.DataType, s.Cardinality = "FoosPerformance", "*FoosPerformance", state.Many
					return s
				}(),
				Columns: []*view.Column{{Name: "ID", DataType: "int"}, {Name: "FOO_ID", DataType: "int"}, {Name: "NAME", DataType: "string", Nullable: true}},
			},
		},
		extraInput: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "CurFoosId",
					In:     state.NewParameterLocation("Foos"),
					Schema: state.NewSchema(reflect.TypeOf(&struct{ Values []int }{})),
					Tag:    `codec:"structql,uri=foos/cur_foos_id.sql"`,
				},
			},
			{
				Parameter: state.Parameter{
					Name:   "CurFoos",
					In:     state.NewViewLocation("CurFoos"),
					Tag:    `view:"CurFoos" sql:"uri=foos/cur_foos.sql"`,
					Schema: &state.Schema{Name: "Foos", DataType: "*Foos", Cardinality: state.Many},
				},
			},
			{
				Parameter: state.Parameter{
					Name:   "CurFoosFoosPerformanceId",
					In:     state.NewParameterLocation("Foos"),
					Schema: state.NewSchema(reflect.TypeOf(&struct{ Values []int }{})),
					Tag:    `codec:"structql,uri=foos/cur_foos_foos_performance_id.sql"`,
				},
			},
			{
				Parameter: state.Parameter{
					Name:   "CurFoosPerformance",
					In:     state.NewViewLocation("CurFoosPerformance"),
					Tag:    `view:"CurFoosPerformance" sql:"uri=foos/cur_foos_performance.sql"`,
					Schema: &state.Schema{Name: "FoosPerformance", DataType: "*FoosPerformance", Cardinality: state.Many},
				},
			},
		},
	})
	assertMutableDSQLParity(t, result.VeltyFilePath, "/Users/awitas/go/src/github.com/viant/datly/e2e/local/dql/generate_patch_many_many/patch_basic_many_many.sql")
	assertMutableSQLFileParity(t, filepath.Join(packageDir, "foos", "cur_foos_id.sql"), "/Users/awitas/go/src/github.com/viant/datly/e2e/local/pkg/dev/generate_patch_basic_many/foos/cur_foos_id.sql")
	assertMutableSQLFileParity(t, filepath.Join(packageDir, "foos", "cur_foos.sql"), "/Users/awitas/go/src/github.com/viant/datly/e2e/local/pkg/dev/generate_patch_basic_many/foos/cur_foos.sql")
	if !strings.Contains(mustReadCodegenFile(t, filepath.Join(packageDir, "foos", "cur_foos_foos_performance_id.sql")), "SELECT ARRAY_AGG(Id) AS Values FROM  `/FoosPerformance` LIMIT 1") {
		t.Fatalf("expected nested current-ids helper SQL")
	}
	if !strings.Contains(mustReadCodegenFile(t, filepath.Join(packageDir, "foos", "cur_foos_performance.sql")), "SELECT * FROM FOOS_PERFORMANCE") {
		t.Fatalf("expected nested current-view helper SQL")
	}
}

func TestComponentCodegen_MutableComponent_UsesResourceViewKeyTypeForIndexMap(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "vendorsvc", "update")

	type legacyRecords struct {
		Id string
	}

	component := &shapeload.Component{
		Method:   "POST",
		URI:      "/v1/api/shape/dev/auth/products/",
		RootView: "ProductUpdate",
		Input: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "Ids",
					In:     state.NewBodyLocation("Ids"),
					Schema: state.NewSchema(reflect.TypeOf([]int{})),
				},
			},
			{
				Parameter: state.Parameter{
					Name:   "Records",
					In:     state.NewViewLocation("Records"),
					Tag:    `view:"Records" sql:"uri=product_update/Records.sql"`,
					Schema: &state.Schema{Name: "RecordsView", DataType: "*RecordsView", Cardinality: state.Many},
				},
			},
		},
		Output: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "Status",
					In:     state.NewOutputLocation("status"),
					Schema: state.NewSchema(reflect.TypeOf("")),
				},
			},
		},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views,
		&view.View{
			Name: "ProductUpdate",
			Mode: view.ModeExec,
			Schema: func() *state.Schema {
				s := state.NewSchema(reflect.TypeOf(struct{}{}))
				s.Name, s.DataType = "ProductUpdateView", "*ProductUpdateView"
				return s
			}(),
		},
		&view.View{
			Name: "Records",
			Mode: view.ModeQuery,
			Schema: func() *state.Schema {
				s := state.NewSchema(reflect.TypeOf([]*legacyRecords{}))
				s.Name, s.DataType, s.Cardinality = "RecordsView", "*RecordsView", state.Many
				return s
			}(),
			Columns: []*view.Column{
				{Name: "ID", DataType: "int"},
			},
		},
	)

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "update",
		PackagePath: "github.com/acme/project/shape/dev/vendorsvc/update",
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
	inputSource := mustReadCodegenFile(t, result.InputFilePath)
	if !strings.Contains(inputSource, `RecordsById map[int]*RecordsView`) {
		t.Fatalf("expected generated input to use resource view key type for index map:\n%s", inputSource)
	}
	initSource := mustReadCodegenFile(t, filepath.Join(packageDir, "input_init.go"))
	if !strings.Contains(initSource, `i.RecordsById = make(map[int]*RecordsView, len(i.Records))`) {
		t.Fatalf("expected generated init helper to use int map key:\n%s", initSource)
	}
	if !strings.Contains(initSource, `i.RecordsById[item.Id] = item`) {
		t.Fatalf("expected generated init helper to index by int key:\n%s", initSource)
	}
}

func mustReadCodegenFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

type mutableFixtureSpec struct {
	packageName  string
	method       string
	uri          string
	bodySchema   *state.Schema
	outputSchema *state.Schema
	views        []*view.View
	extraInput   []*shapeplan.State
}

func generateMutableFixture(t *testing.T, spec mutableFixtureSpec) (*ComponentCodegenResult, string) {
	t.Helper()
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", spec.packageName)
	component := &shapeload.Component{
		Method:   spec.method,
		URI:      spec.uri,
		RootView: "Foos",
		Input: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "Foos",
					In:     state.NewBodyLocation(""),
					Tag:    `anonymous:"true"`,
					Schema: spec.bodySchema,
				},
			},
		},
		Output: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "Foos",
					In:     state.NewOutputLocation("body"),
					Tag:    `anonymous:"true"`,
					Schema: spec.outputSchema,
				},
			},
		},
	}
	component.Input = append(component.Input, spec.extraInput...)
	resource := view.EmptyResource()
	resource.Views = append(resource.Views, spec.views...)
	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: spec.packageName,
		PackagePath: "github.com/acme/project/shape/dev/" + spec.packageName,
	}
	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}
	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	return result, packageDir
}

func assertMutableDSQLParity(t *testing.T, actualPath, expectedPath string) {
	t.Helper()
	actual := normalizeMutableSQL(mustReadCodegenFile(t, actualPath))
	expected := normalizeMutableSQL(mustReadCodegenFile(t, expectedPath))
	if actual != expected {
		t.Fatalf("mutable DSQL mismatch\nexpected:\n%s\n\nactual:\n%s", mustReadCodegenFile(t, expectedPath), mustReadCodegenFile(t, actualPath))
	}
}

func assertMutableSQLFileParity(t *testing.T, actualPath, expectedPath string) {
	t.Helper()
	actual := normalizeMutableSQL(mustReadCodegenFile(t, actualPath))
	expected := normalizeMutableSQL(mustReadCodegenFile(t, expectedPath))
	if actual != expected {
		t.Fatalf("mutable helper SQL mismatch for %s\nexpected:\n%s\n\nactual:\n%s", actualPath, mustReadCodegenFile(t, expectedPath), mustReadCodegenFile(t, actualPath))
	}
}

func normalizeMutableSQL(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	lines := strings.Split(value, "\n")
	out := make([]string, 0, len(lines))
	ws := regexp.MustCompile(`\s+`)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		line = ws.ReplaceAllString(line, " ")
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

func TestComponentCodegen_MutableComponent_MergesRootTemplateHelpersIntoInput(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "shape", "dev", "patch_basic_one")

	component := &shapeload.Component{
		Method:   "PATCH",
		URI:      "/v1/api/shape/dev/basic/foos",
		RootView: "Foos",
		Input: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "Foos",
					In:     state.NewBodyLocation(""),
					Tag:    `anonymous:"true"`,
					Schema: &state.Schema{Name: "FoosView", DataType: "*FoosView", Cardinality: state.One},
				},
			},
		},
		Output: []*shapeplan.State{
			{
				Parameter: state.Parameter{
					Name:   "Foos",
					In:     state.NewOutputLocation("body"),
					Tag:    `anonymous:"true"`,
					Schema: &state.Schema{Name: "FoosView", DataType: "*FoosView", Cardinality: state.One},
				},
			},
		},
	}

	resource := view.EmptyResource()
	resource.Views = append(resource.Views,
		&view.View{
			Name: "Foos",
			Mode: view.ModeExec,
			Schema: &state.Schema{
				Name:        "FoosView",
				DataType:    "*FoosView",
				Cardinality: state.One,
			},
			Columns: []*view.Column{
				{Name: "ID", DataType: "int"},
				{Name: "NAME", DataType: "string", Nullable: true},
				{Name: "QUANTITY", DataType: "int", Nullable: true},
			},
			Template: &view.Template{
				UseParameterStateType: true,
				Parameters: state.Parameters{
					{
						Name:   "Foos",
						In:     state.NewBodyLocation(""),
						Tag:    `anonymous:"true"`,
						Schema: &state.Schema{Name: "FoosView", DataType: "*FoosView", Cardinality: state.One},
					},
					{
						Name:   "CurFoosId",
						In:     state.NewParameterLocation("Foos"),
						Schema: state.NewSchema(reflect.TypeOf(&struct{ Values []int }{})),
						Tag:    `codec:"structql,uri=foos/cur_foos_id.sql"`,
					},
					{
						Name:   "CurFoos",
						In:     state.NewViewLocation("CurFoos"),
						Tag:    `view:"CurFoos" sql:"uri=foos/cur_foos.sql"`,
						Schema: &state.Schema{Name: "FoosView", DataType: "*FoosView", Cardinality: state.Many},
					},
				},
			},
		},
		&view.View{
			Name: "CurFoos",
			Mode: view.ModeQuery,
			Schema: &state.Schema{
				Name:        "FoosView",
				DataType:    "*FoosView",
				Cardinality: state.Many,
			},
			Columns: []*view.Column{
				{Name: "ID", DataType: "int"},
				{Name: "NAME", DataType: "string", Nullable: true},
				{Name: "QUANTITY", DataType: "int", Nullable: true},
			},
		},
	)

	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "patch_basic_one",
		PackagePath: "github.com/acme/project/shape/dev/patch_basic_one",
	}

	codegen := &ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithEmbed:    true,
		WithContract: true,
	}

	result, err := codegen.Generate()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}

	inputSource := mustReadCodegenFile(t, result.InputFilePath)
	for _, fragment := range []string{
		`CurFoosId *struct {`,
		`Values []int`,
		`CurFoos `,
		`CurFoosById map[int]`,
	} {
		if !strings.Contains(inputSource, fragment) {
			t.Fatalf("expected generated input to include %q:\n%s", fragment, inputSource)
		}
	}

	initSource := mustReadCodegenFile(t, filepath.Join(packageDir, "input_init.go"))
	if !strings.Contains(initSource, `i.CurFoosById = make(map[int]FoosView, len(i.CurFoos))`) {
		t.Fatalf("expected generated init helper to index CurFoos:\n%s", initSource)
	}
}
