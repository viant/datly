package shape_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	marshalconfig "github.com/viant/datly/gateway/router/marshal/config"
	marshaljson "github.com/viant/datly/gateway/router/marshal/json"
	shape "github.com/viant/datly/repository/shape"
	shapeCompile "github.com/viant/datly/repository/shape/compile"
	shapeLoad "github.com/viant/datly/repository/shape/load"
	shapePlan "github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
)

func TestEngine_LoadDQLViews(t *testing.T) {
	engine := shape.New(
		shape.WithCompiler(shapeCompile.New()),
		shape.WithLoader(shapeLoad.New()),
		shape.WithName("/v1/api/reports/orders"),
	)
	artifacts, err := engine.LoadDQLViews(context.Background(), "SELECT id FROM ORDERS t")
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.Len(t, artifacts.Views, 1)
	assert.Equal(t, "t", artifacts.Views[0].Name)
}

func TestEngine_LoadDQLResource(t *testing.T) {
	engine := shape.New(
		shape.WithCompiler(shapeCompile.New()),
		shape.WithLoader(shapeLoad.New()),
		shape.WithName("/v1/api/reports/orders"),
	)
	artifacts, err := engine.LoadDQLResource(context.Background(), "SELECT id FROM ORDERS t")
	require.NoError(t, err)
	require.NotNil(t, artifacts)
	require.NotNil(t, artifacts.Resource)
	require.Len(t, artifacts.Resource.Views, 1)
	assert.Equal(t, "t", artifacts.Resource.Views[0].Name)
}

func TestEngine_LoadDQLComponent(t *testing.T) {
	engine := shape.New(
		shape.WithCompiler(shapeCompile.New()),
		shape.WithLoader(shapeLoad.New()),
		shape.WithName("/v1/api/reports/orders"),
	)
	artifact, err := engine.LoadDQLComponent(context.Background(), "SELECT id FROM ORDERS t")
	require.NoError(t, err)
	require.NotNil(t, artifact)
	require.NotNil(t, artifact.Component)

	component, ok := shapeLoad.ComponentFrom(artifact)
	require.True(t, ok)
	assert.Equal(t, "/v1/api/reports/orders", component.Name)
	assert.Equal(t, "t", component.RootView)
}

func TestEngine_LoadDQLComponent_DeclarationMetadata(t *testing.T) {
	engine := shape.New(
		shape.WithCompiler(shapeCompile.New()),
		shape.WithLoader(shapeLoad.New()),
		shape.WithName("/v1/api/reports/orders"),
	)
	dql := `
#set($_ = $limit<?>(view/limit).WithPredicate('ByID','id = ?', 1).QuerySelector('items') /* SELECT id FROM ORDERS o */)
SELECT id FROM ORDERS t`
	artifact, err := engine.LoadDQLComponent(context.Background(), dql)
	require.NoError(t, err)
	require.NotNil(t, artifact)
	component, ok := shapeLoad.ComponentFrom(artifact)
	require.True(t, ok)
	require.NotNil(t, component.Declarations)
	require.NotNil(t, component.QuerySelectors)
	require.NotNil(t, component.Predicates)
	assert.Equal(t, []string{"o"}, component.QuerySelectors["items"])
	require.NotNil(t, component.Declarations["o"])
	assert.Equal(t, "items", component.Declarations["o"].QuerySelector)
	require.NotEmpty(t, component.Predicates["o"])
	assert.Equal(t, "ByID", component.Predicates["o"][0].Name)
}

func TestEngine_LoadDQLComponent_PreservesExplicitOutputViewOneCardinality(t *testing.T) {
	engine := shape.New(
		shape.WithCompiler(shapeCompile.New()),
		shape.WithLoader(shapeLoad.New()),
		shape.WithName("/v1/api/shape/dev/auth/user-acl"),
	)
	dql := `
#define($_ = $Data<?>(output/view).Cardinality('One').Embed())
SELECT 1 AS UserID`
	artifact, err := engine.LoadDQLComponent(context.Background(), dql)
	require.NoError(t, err)
	require.NotNil(t, artifact)

	component, ok := shapeLoad.ComponentFrom(artifact)
	require.True(t, ok)
	require.Len(t, component.Output, 1)
	require.NotNil(t, component.Output[0].Schema)
	assert.Equal(t, "One", string(component.Output[0].Schema.Cardinality))
}

type metaFormatOutput struct {
	Meta   *metaFormatMeta  `json:"meta,omitempty"`
	Data   []metaFormatData `json:"data,omitempty"`
	Status string           `json:"status,omitempty"`
}

type metaFormatMeta struct {
	PageCnt *int `json:"pageCnt,omitempty"`
	Cnt     int  `json:"cnt,omitempty"`
}

type metaFormatData struct {
	Id           int                     `json:"id,omitempty"`
	Name         *string                 `json:"name,omitempty"`
	AccountId    *int                    `json:"accountId,omitempty"`
	Products     []*metaFormatProduct    `json:"products,omitempty"`
	ProductsMeta *metaFormatProductsMeta `json:"productsMeta,omitempty"`
}

type metaFormatProduct struct {
	Id       int     `json:"id,omitempty"`
	Name     *string `json:"name,omitempty"`
	VendorId *int    `json:"vendorId,omitempty"`
}

type metaFormatProductsMeta struct {
	VendorId      *int `json:"vendorId,omitempty"`
	PageCnt       *int `json:"pageCnt,omitempty"`
	TotalProducts int  `json:"totalProducts,omitempty"`
}

func TestMetaFormatLiveLikeOutput_Marshal(t *testing.T) {
	name := "Acme"
	id := 1
	pageCnt := 2
	output := &metaFormatOutput{
		Meta: &metaFormatMeta{PageCnt: &pageCnt, Cnt: 3},
		Data: []metaFormatData{
			{
				Id:        1,
				Name:      &name,
				AccountId: &id,
				Products: []*metaFormatProduct{
					{Id: 10, Name: &name, VendorId: &id},
				},
				ProductsMeta: &metaFormatProductsMeta{VendorId: &id, PageCnt: &pageCnt, TotalProducts: 1},
			},
		},
		Status: "ok",
	}
	marshaller := marshaljson.New(&marshalconfig.IOConfig{CaseFormat: text.CaseFormatLowerCamel})
	_, err := marshaller.Marshal(output)
	require.NoError(t, err)
}

func TestDQLCompileLoad_MetaFormatPreservesSummariesWithoutLinkedTypes(t *testing.T) {
	dqlPath := filepath.Join("..", "..", "e2e", "v1", "dql", "dev", "vendorsrv", "meta_format.dql")
	dqlBytes, err := os.ReadFile(dqlPath)
	require.NoError(t, err)

	source := &shape.Source{
		Name: "meta_format",
		Path: dqlPath,
		DQL:  string(dqlBytes),
	}
	planResult, err := shapeCompile.New().Compile(
		context.Background(),
		source,
		shape.WithLinkedTypes(false),
		shape.WithTypeContextPackageDir(filepath.Join("e2e", "v1", "shape", "dev", "vendorsvc", "multi_summary")),
		shape.WithTypeContextPackageName("multi_summary"),
	)
	require.NoError(t, err)
	registry := source.EnsureTypeRegistry()
	require.NotNil(t, registry)
	if lookup := registry.Lookup("ProductsView"); lookup != nil {
		fmt.Printf("registry ProductsView: %T %v\n", lookup.Type, lookup.Type)
	} else {
		fmt.Printf("registry ProductsView: <nil>\n")
	}
	if lookup := registry.Lookup("github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/multi_summary.ProductsView"); lookup != nil {
		fmt.Printf("registry fq ProductsView: %T %v\n", lookup.Type, lookup.Type)
	} else {
		fmt.Printf("registry fq ProductsView: <nil>\n")
	}
	planned, ok := shapePlan.ResultFrom(planResult)
	require.True(t, ok)
	foundPlannedProductsType := false
	for _, item := range planned.Types {
		if item != nil && item.Name == "ProductsView" {
			foundPlannedProductsType = true
			assert.NotEmpty(t, item.DataType)
		}
	}
	var plannedProductsView *shapePlan.View
	for _, item := range planned.Views {
		if item != nil && item.Name == "products" {
			plannedProductsView = item
			break
		}
	}
	require.NotNil(t, plannedProductsView)
	require.NotNil(t, plannedProductsView.FieldType)
	t.Logf("planned products fieldType=%v elementType=%v", plannedProductsView.FieldType, plannedProductsView.ElementType)
	assert.False(t, foundPlannedProductsType)
	registered := registry.Lookup("github.com/viant/datly/e2e/v1/shape/dev/vendorsvc/multi_summary.ProductsMetaView")
	require.NotNil(t, registered)
	require.NotNil(t, registered.Type)
	registeredType := registered.Type
	if registeredType.Kind() == reflect.Ptr {
		registeredType = registeredType.Elem()
	}
	field, ok := registeredType.FieldByName("VendorId")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*int)(nil)), field.Type)

	resourceArtifacts, err := shapeLoad.New().LoadResource(context.Background(), planResult, shape.WithLoadTypeContextPackages(true))
	require.NoError(t, err)
	require.NotNil(t, resourceArtifacts)
	require.NotNil(t, resourceArtifacts.Resource)

	index := resourceArtifacts.Resource.Views.Index()
	root, err := index.Lookup("vendor")
	require.NoError(t, err)
	require.NotNil(t, root)
	t.Logf("root view: name=%s ref=%s schema=%v with=%d", root.Name, root.Ref, root.Schema != nil, len(root.With))
	for i, rel := range root.With {
		if rel == nil || rel.Of == nil {
			t.Logf("root relation[%d]: nil", i)
			continue
		}
		relSchemaType := "<nil>"
		if rel.Of.View.Schema != nil && rel.Of.View.Schema.Type() != nil {
			relSchemaType = rel.Of.View.Schema.Type().String()
		}
		t.Logf("root relation[%d]: holder=%s name=%s ref=%s schema=%v schemaType=%s summary=%v", i, rel.Holder, rel.Of.View.Name, rel.Of.View.Ref, rel.Of.View.Schema != nil, relSchemaType, rel.Of.View.Template != nil && rel.Of.View.Template.Summary != nil)
	}
	require.NotNil(t, root.Template)
	require.NotNil(t, root.Template.Summary)
	require.NotNil(t, root.Template.Summary.Schema)
	assert.Equal(t, "MetaView", root.Template.Summary.Schema.Name)

	child, err := index.Lookup("products")
	require.NoError(t, err)
	require.NotNil(t, child)
	require.NotNil(t, child.Template)
	require.NotNil(t, child.Template.Summary)
	require.NotNil(t, child.Template.Summary.Schema)
	assert.Equal(t, "ProductsMetaView", child.Template.Summary.Schema.Name)
	childViewSummaryType := child.Template.Summary.Schema.Type()
	require.NotNil(t, childViewSummaryType)
	if childViewSummaryType.Kind() == reflect.Ptr {
		childViewSummaryType = childViewSummaryType.Elem()
	}
	field, ok = childViewSummaryType.FieldByName("VendorId")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*int)(nil)), field.Type)
	require.NotEmpty(t, root.With)
	require.NotNil(t, root.With[0].Of)
	require.NotNil(t, root.With[0].Of.View.Template)
	require.NotNil(t, root.With[0].Of.View.Template.Summary)
	childSummaryType := root.With[0].Of.View.Template.Summary.Schema.Type()
	require.NotNil(t, childSummaryType)
	if childSummaryType.Kind() == reflect.Ptr {
		childSummaryType = childSummaryType.Elem()
	}
	field, ok = childSummaryType.FieldByName("VendorId")
	require.True(t, ok)
	assert.Equal(t, reflect.TypeOf((*int)(nil)), field.Type)

	componentArtifact, err := shapeLoad.New().LoadComponent(context.Background(), planResult, shape.WithLoadTypeContextPackages(true))
	require.NoError(t, err)
	component, ok := shapeLoad.ComponentFrom(componentArtifact)
	require.True(t, ok)
	foundProductsType := false
	for _, item := range componentArtifact.Resource.Types {
		if item != nil {
			t.Logf("resource type: name=%s dataType=%s fields=%d package=%s module=%s", item.Name, item.DataType, len(item.Fields), item.Package, item.ModulePath)
		}
		if item == nil || item.Name != "ProductsView" {
			continue
		}
		foundProductsType = true
		require.NotEmpty(t, item.Fields)
		break
	}
	require.True(t, foundProductsType)
	typeRegistry, err := initTypeRegistryForResource(componentArtifact.Resource)
	require.NoError(t, err)

	foundSummary := false
	for _, param := range component.Output {
		if param != nil && param.In != nil && param.In.Name == "summary" {
			foundSummary = true
			require.NotNil(t, param.Schema)
			assert.Equal(t, "MetaView", param.Schema.Name)
		}
	}
	assert.True(t, foundSummary)

	outputType, err := component.OutputReflectType("", typeRegistry.Lookup)
	require.NoError(t, err)
	require.NotNil(t, outputType)

	output := reflect.New(outputType).Elem()
	dataField := output.FieldByName("Data")
	require.True(t, dataField.IsValid())
	require.Equal(t, reflect.Slice, dataField.Kind())

	rowType := dataField.Type().Elem()
	fmt.Printf("output Data type: %T %v\n", dataField.Interface(), dataField.Type())
	rowValue := reflect.New(rowType)
	if rowType.Kind() == reflect.Ptr {
		rowValue = reflect.New(rowType.Elem())
	}
	row := rowValue.Elem()
	row.FieldByName("Id").SetInt(1)

	productsField := row.FieldByName("Products")
	require.True(t, productsField.IsValid())
	productType := productsField.Type().Elem()
	product := reflect.New(productType)
	if productType.Kind() == reflect.Ptr {
		product = reflect.New(productType.Elem())
	}
	product.Elem().FieldByName("Id").SetInt(10)
	if productType.Kind() == reflect.Ptr {
		productsField.Set(reflect.Append(productsField, product))
	} else {
		productsField.Set(reflect.Append(productsField, product.Elem()))
	}

	data := reflect.MakeSlice(dataField.Type(), 0, 1)
	if rowType.Kind() == reflect.Ptr {
		data = reflect.Append(data, rowValue)
	} else {
		data = reflect.Append(data, row)
	}
	dataField.Set(data)

	marshaller := marshaljson.New(&marshalconfig.IOConfig{CaseFormat: text.CaseFormatLowerCamel})
	_, err = marshaller.Marshal(output.Addr().Interface())
	require.NoError(t, err)
}

func TestDQLCompileLoad_DistrictPaginationInheritsNestedRelationTypeContextPackages(t *testing.T) {
	dqlPath := filepath.Join("..", "..", "e2e", "v1", "dql", "dev", "district", "district_pagination.sql")
	dqlPath, err := filepath.Abs(dqlPath)
	require.NoError(t, err)
	dqlBytes, err := os.ReadFile(dqlPath)
	require.NoError(t, err)

	source := &shape.Source{
		Name: "district_pagination",
		Path: dqlPath,
		DQL:  string(dqlBytes),
	}
	planResult, err := shapeCompile.New().Compile(
		context.Background(),
		source,
		shape.WithLinkedTypes(false),
		shape.WithTypeContextPackageDir(filepath.Join("e2e", "v1", "shape", "dev", "district", "pagination")),
		shape.WithTypeContextPackageName("pagination"),
	)
	require.NoError(t, err)

	componentArtifact, err := shapeLoad.New().LoadComponent(context.Background(), planResult, shape.WithLoadTypeContextPackages(true))
	require.NoError(t, err)

	component, ok := shapeLoad.ComponentFrom(componentArtifact)
	require.True(t, ok)
	require.NotNil(t, component)

	root, err := componentArtifact.Resource.Views.Index().Lookup(component.RootView)
	require.NoError(t, err)
	require.NotNil(t, root)
	require.NotNil(t, root.Schema)
	assert.Equal(t, "pagination", root.Schema.Package)
	assert.Equal(t, "github.com/viant/datly/e2e/v1/shape/dev/district/pagination", root.Schema.PackagePath)

	require.Len(t, root.With, 1)
	child := &root.With[0].Of.View
	require.NotNil(t, child.Schema)
	assert.Equal(t, "pagination", child.Schema.Package)
	assert.Equal(t, "github.com/viant/datly/e2e/v1/shape/dev/district/pagination", child.Schema.PackagePath)
	typeNames := map[string]bool{}
	for _, definition := range componentArtifact.Resource.Types {
		if definition == nil {
			continue
		}
		typeNames[definition.Name] = true
	}
	assert.True(t, typeNames["DistrictsView"])
	assert.True(t, typeNames["CitiesView"])

	_, err = initTypeRegistryForResource(componentArtifact.Resource)
	require.NoError(t, err)
}

func initTypeRegistryForResource(resource *view.Resource) (*xreflect.Types, error) {
	registry := extension.NewRegistry()
	imports := view.Imports{}
	for _, definition := range resource.Types {
		if definition != nil && definition.ModulePath != "" {
			imports.Add(definition.ModulePath)
			if definition.Package != "" {
				imports.AddWithAlias(definition.Package, definition.ModulePath)
			}
		}
	}
	for _, definition := range resource.Types {
		if definition == nil {
			continue
		}
		if err := definition.Init(context.Background(), registry.Types.Lookup, imports); err != nil {
			return nil, err
		}
		if err := registry.Types.Register(definition.Name, xreflect.WithReflectType(definition.Type())); err != nil {
			return nil, err
		}
		if definition.Package != "" {
			if err := registry.Types.Register(definition.Name, xreflect.WithPackage(definition.Package), xreflect.WithReflectType(definition.Type())); err != nil {
				return nil, err
			}
		}
	}
	return registry.Types, nil
}
