package repository

import (
	"context"
	"embed"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
)

type reportTestResource struct{}

func (r *reportTestResource) LookupParameter(name string) (*state.Parameter, error) { return nil, nil }
func (r *reportTestResource) AppendParameter(parameter *state.Parameter)            {}
func (r *reportTestResource) ViewSchema(ctx context.Context, name string) (*state.Schema, error) {
	return nil, nil
}
func (r *reportTestResource) ViewSchemaPointer(ctx context.Context, name string) (*state.Schema, error) {
	return nil, nil
}
func (r *reportTestResource) LookupType() xreflect.LookupType { return nil }
func (r *reportTestResource) LoadText(ctx context.Context, URL string) (string, error) {
	return "", nil
}
func (r *reportTestResource) Codecs() *codec.Registry                  { return codec.New() }
func (r *reportTestResource) CodecOptions() *codec.Options             { return codec.NewOptions(nil) }
func (r *reportTestResource) ExpandSubstitutes(value string) string    { return value }
func (r *reportTestResource) ReverseSubstitutes(value string) string   { return value }
func (r *reportTestResource) EmbedFS() *embed.FS                       { return nil }
func (r *reportTestResource) SetFSEmbedder(embedder *state.FSEmbedder) {}

func TestBuildReportMetadataAndComponent(t *testing.T) {
	resource := view.EmptyResource()
	columnResource := &reportTestResource{}
	rootView := view.NewView("vendor", "VENDOR")
	rootView.Groupable = true
	rootView.Selector = &view.Config{
		FieldsParameter:  &state.Parameter{Name: "fields", In: state.NewQueryLocation("_fields")},
		OrderByParameter: &state.Parameter{Name: "orderBy", In: state.NewQueryLocation("_orderby")},
		LimitParameter:   &state.Parameter{Name: "limit", In: state.NewQueryLocation("_limit")},
		OffsetParameter:  &state.Parameter{Name: "offset", In: state.NewQueryLocation("_offset")},
	}
	rootView.Columns = []*view.Column{
		view.NewColumn("AccountID", "int", reflect.TypeOf(0), false),
		view.NewColumn("UserCreated", "int", reflect.TypeOf(0), false),
		view.NewColumn("TotalSpend", "float64", reflect.TypeOf(float64(0)), false),
	}
	rootView.Columns[0].Groupable = true
	rootView.Columns[1].Groupable = true
	rootView.Columns[2].Aggregate = true
	for _, column := range rootView.Columns {
		require.NoError(t, column.Init(columnResource, text.CaseFormatUndefined, false))
	}
	rootView.SetResource(resource)
	resource.AddViews(rootView)

	inputType, err := state.NewType(state.WithParameters(state.Parameters{
		&state.Parameter{Name: "vendorIDs", In: state.NewQueryLocation("vendorIDs"), Schema: state.NewSchema(reflect.TypeOf([]int{})), Description: "Vendor IDs to include"},
		&state.Parameter{Name: "accountID", In: state.NewQueryLocation("accountID"), Schema: state.NewSchema(reflect.TypeOf(0)), Predicates: []*extension.PredicateConfig{{Name: "ByAccount"}}, Description: "Account identifier filter"},
		&state.Parameter{Name: "fields", In: state.NewQueryLocation("_fields"), Schema: state.NewSchema(reflect.TypeOf([]string{}))},
	}), state.WithResource(columnResource))
	require.NoError(t, err)
	inputType.Name = "VendorInput"

	component := &Component{
		Path:   contract.Path{Method: "GET", URI: "/v1/api/vendors"},
		Meta:   contract.Meta{Name: "vendors"},
		View:   rootView,
		Report: (&Report{Enabled: true}).normalize(),
		Contract: contract.Contract{
			Input: contract.Input{Type: *inputType},
		},
	}

	metadata, err := buildReportMetadata(component, component.Report)
	require.NoError(t, err)
	require.NotNil(t, metadata)
	assert.Equal(t, "VendorInputReportInput", metadata.InputName)
	require.Len(t, metadata.Dimensions, 2)
	require.Len(t, metadata.Measures, 1)
	require.Len(t, metadata.Filters, 1)
	assert.Equal(t, "AccountID", metadata.Dimensions[0].Name)
	assert.Equal(t, "TotalSpend", metadata.Measures[0].Name)
	assert.Equal(t, "accountID", metadata.Filters[0].Name)

	service := &Service{registry: NewRegistry("", nil, nil)}
	reportComponent, reportPath, err := service.buildReportComponent(component, &path.Path{
		Path: component.Path,
		View: &path.ViewRef{Ref: rootView.Name},
		ModelContextProtocol: contract.ModelContextProtocol{
			MCPTool: true,
		},
		Meta: contract.Meta{
			Name:        "vendors",
			Description: "Vendor listing",
		},
		Report: &path.Report{Enabled: true},
	})
	require.NoError(t, err)
	require.NotNil(t, reportComponent)
	require.NotNil(t, reportPath)
	assert.Equal(t, "POST", reportComponent.Method)
	assert.Equal(t, "/v1/api/vendors/report", reportComponent.URI)
	require.NotNil(t, reportComponent.Report)
	require.NotNil(t, reportComponent.View)
	assert.NotSame(t, component.View, reportComponent.View)
	assert.Equal(t, view.ModeHandler, reportComponent.View.Mode)
	assert.Nil(t, reportComponent.View.Template)
	assert.Equal(t, "/v1/api/vendors/report", reportPath.URI)
	assert.Equal(t, "POST", reportPath.Method)
	assert.True(t, reportPath.MCPTool)
	assert.Equal(t, "vendors Report", reportPath.Name)
	assert.Equal(t, "Vendor listing report", reportPath.Description)
	reportInputType, err := buildReportInputType(component, metadata, component.Report)
	require.NoError(t, err)
	require.NotNil(t, reportInputType)
	require.NotNil(t, reportInputType.Schema)
	require.NotNil(t, reportInputType.Schema.Type())
	bodyType := reportInputType.Schema.Type()
	if bodyType.Kind() == reflect.Ptr {
		bodyType = bodyType.Elem()
	}
	_, ok := bodyType.FieldByName("Dimensions")
	assert.True(t, ok)
	_, ok = bodyType.FieldByName("Measures")
	assert.True(t, ok)
	_, ok = bodyType.FieldByName("Filters")
	assert.True(t, ok)
	filtersField, ok := bodyType.FieldByName("Filters")
	require.True(t, ok)
	filterType := filtersField.Type
	require.Greater(t, filterType.NumField(), 0)
	filterField := filterType.Field(0)
	assert.True(t, strings.Contains(string(filterField.Tag), `desc:"Account identifier filter"`))
}

func TestService_InitComponentProviders_RegistersLocalGroupingReportRoute(t *testing.T) {
	ctx := context.Background()
	baseDir := filepath.Join("..", "e2e", "local", "regression")
	if _, err := os.Stat(filepath.Join(baseDir, "paths.yaml")); err != nil {
		t.Skipf("missing local regression fixture: %v", err)
	}
	service, err := New(ctx,
		WithComponentURL(baseDir),
		WithResourceURL(baseDir),
		WithNoPlugin(),
		WithRefreshDisabled(true),
	)
	require.NoError(t, err)
	reportPath := &contract.Path{Method: "POST", URI: "/v1/api/shape/dev/vendors-grouping/report"}
	provider, err := service.Registry().LookupProvider(ctx, reportPath)
	require.NoError(t, err)
	require.NotNil(t, provider)
	component, err := provider.Component(ctx)
	require.NoError(t, err)
	require.NotNil(t, component)
	require.NotNil(t, component.Report)
	assert.True(t, component.Report.Enabled)
	assert.Equal(t, "POST", component.Method)
	assert.Equal(t, "/v1/api/shape/dev/vendors-grouping/report", component.URI)
}
