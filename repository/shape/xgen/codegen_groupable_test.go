package xgen

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
)

type codegenMetaView struct {
	PageCnt *int
}

type codegenProductsMetaView struct {
	VendorId *int
}

type staleCodegenProductsMetaView struct {
	VendorId string
}

type codegenProductsView struct {
	VendorId *int
}

type codegenVendorView struct {
	ID           int
	Products     []*codegenProductsView
	ProductsMeta *codegenProductsMetaView
}

func TestComponentCodegen_ColumnFieldTag_EmitsGroupableTag(t *testing.T) {
	groupable := true
	codegen := &ComponentCodegen{}
	aView := &view.View{
		ColumnsConfig: map[string]*view.ColumnConfig{
			"REGION": {Name: "REGION", Groupable: &groupable},
		},
	}
	column := &view.Column{Name: "REGION", DataType: "string"}

	tag := codegen.columnFieldTag(aView, column)
	assert.Contains(t, tag, `groupable:"true"`)
	assert.Contains(t, tag, `sqlx:"REGION"`)
}

func TestComponentCodegen_GeneratesSelectorHolderOutsideBusinessInput(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "pkg", "dev", "vendor")
	component := &load.Component{
		Name:     "Vendor",
		Method:   "GET",
		URI:      "/v1/api/dev/vendors-grouping",
		RootView: "Vendor",
		Report: &dqlshape.ReportDirective{
			Enabled:    true,
			Input:      "VendorReportInput",
			Dimensions: "Dims",
			Measures:   "Metrics",
			Filters:    "Predicates",
			OrderBy:    "Sort",
			Limit:      "Take",
			Offset:     "Skip",
		},
		Directives: &dqlshape.Directives{
			InputDest:  "vendor_input.go",
			OutputDest: "vendor_output.go",
			RouterDest: "vendor_router.go",
		},
		Input: []*plan.State{
			{Parameter: state.Parameter{Name: "VendorIDs", In: state.NewQueryLocation("vendorIDs"), Schema: state.NewSchema(reflect.TypeOf([]int{}))}},
			{Parameter: state.Parameter{Name: "Fields", In: state.NewQueryLocation("_fields"), Schema: state.NewSchema(reflect.TypeOf([]string{}))}, QuerySelector: "vendor"},
			{Parameter: state.Parameter{Name: "OrderBy", In: state.NewQueryLocation("_orderby"), Schema: state.NewSchema(reflect.TypeOf(""))}, QuerySelector: "vendor"},
		},
	}
	resource := &view.Resource{
		Views: []*view.View{
			{
				Name:      "Vendor",
				Groupable: true,
				Selector: &view.Config{
					Constraints: &view.Constraints{
						OrderBy:       true,
						OrderByColumn: map[string]string{"accountId": "ACCOUNT_ID"},
					},
				},
				Template: &view.Template{SourceURL: "vendor/vendor.sql"},
				Columns:  []*view.Column{{Name: "ACCOUNT_ID", DataType: "int"}},
			},
		},
	}
	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "vendor",
		PackagePath: "github.com/acme/project/pkg/dev/vendor",
	}

	result, err := (&ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithContract: true,
	}).Generate()
	require.NoError(t, err)

	inputSource, err := os.ReadFile(result.InputFilePath)
	require.NoError(t, err)
	assert.Contains(t, string(inputSource), "type VendorInput struct {")
	assert.Contains(t, string(inputSource), "VendorIDs []int")
	assert.NotContains(t, string(inputSource), "Fields []string")
	assert.NotContains(t, string(inputSource), "OrderBy string")

	routerSource, err := os.ReadFile(result.RouterFilePath)
	require.NoError(t, err)
	assert.Contains(t, string(routerSource), "ViewSelect struct {")
	assert.Contains(t, string(routerSource), `querySelector:"vendor"`)
	assert.Contains(t, string(routerSource), `report=true`)
	assert.Contains(t, string(routerSource), `reportInput=VendorReportInput`)
	assert.Contains(t, string(routerSource), `reportDimensions=Dims`)
	assert.Contains(t, string(routerSource), `Fields []string `+"`"+`parameter:"`)
	assert.Contains(t, string(routerSource), `in=_fields`)
	assert.Contains(t, string(routerSource), `OrderBy string `+"`"+`parameter:"`)
	assert.Contains(t, string(routerSource), `in=_orderby`)

	outputSource, err := os.ReadFile(result.OutputFilePath)
	require.NoError(t, err)
	assert.Contains(t, string(outputSource), `repository.WithReport(&repository.Report{Enabled: true, Input: "VendorReportInput", Dimensions: "Dims", Measures: "Metrics", Filters: "Predicates", OrderBy: "Sort", Limit: "Take", Offset: "Skip"})`)
	assert.Contains(t, string(outputSource), `view:"Vendor,groupable=true`)
	assert.Contains(t, string(outputSource), `selectorOrderBy=true`)
	assert.Contains(t, string(outputSource), `selectorOrderByColumns={accountId:ACCOUNT_ID}`)
}

func TestComponentCodegen_GeneratesSummaryMetadata(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "pkg", "dev", "vendor")
	vendorSchema := state.NewSchema(reflect.TypeOf(codegenVendorView{}))
	vendorSchema.Name = "VendorView"
	vendorSchema.DataType = "*VendorView"
	productsSchema := state.NewSchema(reflect.TypeOf(codegenProductsView{}))
	productsSchema.Name = "ProductsView"
	productsSchema.DataType = "*ProductsView"
	metaSchema := state.NewSchema(reflect.TypeOf(codegenMetaView{}))
	metaSchema.Name = "MetaView"
	metaSchema.DataType = "*MetaView"
	productsMetaSchema := state.NewSchema(reflect.TypeOf(codegenProductsMetaView{}))
	productsMetaSchema.Name = "ProductsMetaView"
	productsMetaSchema.DataType = "*ProductsMetaView"
	component := &load.Component{
		Name:     "Vendor",
		Method:   "GET",
		URI:      "/v1/api/dev/meta/vendors-format",
		RootView: "vendor",
		Directives: &dqlshape.Directives{
			OutputDest: "vendor.go",
		},
		Output: []*plan.State{
			{
				Parameter: state.Parameter{
					Name:   "Meta",
					In:     state.NewOutputLocation("summary"),
					Schema: metaSchema,
				},
			},
			{
				Parameter: state.Parameter{
					Name: "Data",
					In:   state.NewOutputLocation("view"),
					Schema: &state.Schema{
						Name:        "VendorView",
						DataType:    "*VendorView",
						Cardinality: state.Many,
					},
				},
			},
		},
	}
	resource := &view.Resource{
		Views: []*view.View{
			{
				Name: "vendor",
				Template: &view.Template{
					SourceURL: "vendor/vendor.sql",
					Summary: &view.TemplateSummary{
						Name:      "Meta",
						SourceURL: "vendor/vendor_summary.sql",
						Schema:    metaSchema,
					},
				},
				Schema: vendorSchema,
				With: []*view.Relation{
					{
						Holder:      "Products",
						Cardinality: state.Many,
						Of: &view.ReferenceView{
							View: view.View{
								Name: "products",
								Template: &view.Template{
									SourceURL: "vendor/products.sql",
									Summary: &view.TemplateSummary{
										Name:      "ProductsMeta",
										SourceURL: "vendor/products_summary.sql",
										Schema:    productsMetaSchema,
									},
								},
								Schema: productsSchema,
							},
							On: []*view.Link{{Field: "VendorId", Column: "VENDOR_ID"}},
						},
						On: []*view.Link{{Field: "Id", Column: "ID"}},
					},
				},
				Columns: []*view.Column{{Name: "ID", DataType: "int"}},
			},
			{
				Name: "products",
				Template: &view.Template{
					SourceURL: "vendor/products.sql",
					Summary: &view.TemplateSummary{
						Name:      "ProductsMeta",
						SourceURL: "vendor/products_summary.sql",
						Schema:    productsMetaSchema,
					},
				},
				Schema:  productsSchema,
				Columns: []*view.Column{{Name: "VENDOR_ID", DataType: "int"}},
			},
		},
	}
	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "vendor",
		PackagePath: "github.com/acme/project/pkg/dev/vendor",
	}

	result, err := (&ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithContract: true,
	}).Generate()
	require.NoError(t, err)

	outputSource, err := os.ReadFile(result.OutputFilePath)
	require.NoError(t, err)
	source := string(outputSource)
	assert.Contains(t, source, `Meta MetaView`)
	assert.Contains(t, source, `parameter:",kind=output,in=summary"`)
	assert.Contains(t, source, `view:"vendor,summaryURI=vendor/vendor_summary.sql"`)
	assert.Contains(t, source, `type MetaView struct {`)
	assert.Contains(t, source, `ProductsMeta *ProductsMetaView`)
	assert.Contains(t, source, `view:",summaryURI=vendor/products_summary.sql"`)
	assert.Contains(t, source, `type ProductsMetaView struct {`)
}

func TestComponentCodegen_PrefersStandaloneChildSummarySchemaOverStaleRelationCopy(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "pkg", "dev", "vendor")
	vendorSchema := state.NewSchema(reflect.TypeOf(codegenVendorView{}))
	vendorSchema.Name = "VendorView"
	vendorSchema.DataType = "*VendorView"
	productsSchema := state.NewSchema(reflect.TypeOf(codegenProductsView{}))
	productsSchema.Name = "ProductsView"
	productsSchema.DataType = "*ProductsView"
	staleSummarySchema := state.NewSchema(reflect.TypeOf(staleCodegenProductsMetaView{}))
	staleSummarySchema.Name = "ProductsMetaView"
	staleSummarySchema.DataType = "*ProductsMetaView"
	refinedSummarySchema := state.NewSchema(reflect.TypeOf(codegenProductsMetaView{}))
	refinedSummarySchema.Name = "ProductsMetaView"
	refinedSummarySchema.DataType = "*ProductsMetaView"
	component := &load.Component{
		Name:     "Vendor",
		Method:   "GET",
		URI:      "/v1/api/dev/meta/vendors-format",
		RootView: "vendor",
	}
	resource := &view.Resource{
		Views: []*view.View{
			{
				Name:     "vendor",
				Template: &view.Template{SourceURL: "vendor/vendor.sql"},
				Schema:   vendorSchema,
				With: []*view.Relation{
					{
						Holder:      "Products",
						Cardinality: state.Many,
						Of: &view.ReferenceView{
							View: view.View{
								Name: "products",
								Template: &view.Template{
									SourceURL: "vendor/products.sql",
									Summary: &view.TemplateSummary{
										Name:      "ProductsMeta",
										SourceURL: "vendor/products_summary.sql",
										Schema:    staleSummarySchema,
									},
								},
								Schema: productsSchema,
							},
							On: []*view.Link{{Field: "VendorId", Column: "VENDOR_ID"}},
						},
						On: []*view.Link{{Field: "Id", Column: "ID"}},
					},
				},
			},
			{
				Name: "products",
				Template: &view.Template{
					SourceURL: "vendor/products.sql",
					Summary: &view.TemplateSummary{
						Name:      "ProductsMeta",
						SourceURL: "vendor/products_summary.sql",
						Schema:    refinedSummarySchema,
					},
				},
				Schema: productsSchema,
			},
		},
	}
	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "vendor",
		PackagePath: "github.com/acme/project/pkg/dev/vendor",
	}

	result, err := (&ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithContract: true,
	}).Generate()
	require.NoError(t, err)

	outputSource, err := os.ReadFile(result.OutputFilePath)
	require.NoError(t, err)
	source := string(outputSource)
	assert.Contains(t, source, `type ProductsMetaView struct {`)
	assert.Contains(t, source, `VendorId *int`)
	assert.NotContains(t, source, `VendorId string`)
}

func TestComponentCodegen_WritesReferencedSQLArtifacts(t *testing.T) {
	projectDir := t.TempDir()
	packageDir := filepath.Join(projectDir, "pkg", "dev", "vendor")
	component := &load.Component{
		Name:     "Vendor",
		Method:   "GET",
		URI:      "/v1/api/dev/vendors",
		RootView: "vendor",
	}
	resource := &view.Resource{
		Views: []*view.View{
			{
				Name: "vendor",
				Template: &view.Template{
					SourceURL: "vendor/vendor.sql",
					Source:    "SELECT * FROM VENDOR",
					Summary: &view.TemplateSummary{
						Name:      "Meta",
						SourceURL: "vendor/vendor_summary.sql",
						Source:    "SELECT COUNT(*) AS TOTAL",
					},
				},
				With: []*view.Relation{
					{
						Of: &view.ReferenceView{
							View: view.View{
								Name: "products",
								Template: &view.Template{
									SourceURL: "vendor/products.sql",
									Source:    "SELECT * FROM PRODUCT",
								},
							},
						},
					},
				},
			},
		},
	}
	ctx := &typectx.Context{
		PackageDir:  packageDir,
		PackageName: "vendor",
		PackagePath: "github.com/acme/project/pkg/dev/vendor",
	}

	result, err := (&ComponentCodegen{
		Component:    component,
		Resource:     resource,
		TypeContext:  ctx,
		ProjectDir:   projectDir,
		WithContract: true,
	}).Generate()
	require.NoError(t, err)

	for path, expected := range map[string]string{
		filepath.Join(packageDir, "vendor", "vendor.sql"):         "SELECT * FROM VENDOR",
		filepath.Join(packageDir, "vendor", "vendor_summary.sql"): "SELECT COUNT(*) AS TOTAL",
		filepath.Join(packageDir, "vendor", "products.sql"):       "SELECT * FROM PRODUCT",
	} {
		data, readErr := os.ReadFile(path)
		require.NoError(t, readErr)
		assert.Equal(t, expected, string(data))
		assert.Contains(t, result.GeneratedFiles, path)
	}
}
