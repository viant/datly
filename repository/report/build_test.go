package report

import (
	"context"
	"embed"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
)

type testResource struct{}

type explicitReportInput struct {
	Dimensions struct {
		AccountID   bool
		UserCreated bool
	}
	Measures struct {
		TotalSpend bool
	}
	Filters struct {
		AccountId int
	}
	OrderBy []string
	Limit   *int
	Offset  *int
}

func (r *testResource) LookupParameter(name string) (*state.Parameter, error) { return nil, nil }
func (r *testResource) AppendParameter(parameter *state.Parameter)            {}
func (r *testResource) ViewSchema(ctx context.Context, name string) (*state.Schema, error) {
	return nil, nil
}
func (r *testResource) ViewSchemaPointer(ctx context.Context, name string) (*state.Schema, error) {
	return nil, nil
}
func (r *testResource) LookupType() xreflect.LookupType { return nil }
func (r *testResource) LoadText(ctx context.Context, URL string) (string, error) {
	return "", nil
}
func (r *testResource) Codecs() *codec.Registry                { return codec.New() }
func (r *testResource) CodecOptions() *codec.Options           { return codec.NewOptions(nil) }
func (r *testResource) ExpandSubstitutes(value string) string  { return value }
func (r *testResource) ReverseSubstitutes(value string) string { return value }
func (r *testResource) EmbedFS() *embed.FS                     { return nil }
func (r *testResource) SetFSEmbedder(embedder *state.FSEmbedder) {
}

func TestAssembleMetadata(t *testing.T) {
	tests := []struct {
		name      string
		component *Component
		config    *Config
		assertion func(t *testing.T, got *Metadata, err error)
	}{
		{
			name:      "uses component report defaults",
			component: newComponentFixture(t, &Config{Enabled: true}),
			assertion: func(t *testing.T, got *Metadata, err error) {
				require.NoError(t, err)
				require.NotNil(t, got)
				assert.Equal(t, "VendorInputReportInput", got.InputName)
				assert.Equal(t, "Dimensions", got.DimensionsKey)
				assert.Equal(t, "Measures", got.MeasuresKey)
				assert.Equal(t, "Filters", got.FiltersKey)
				require.Len(t, got.Dimensions, 2)
				require.Len(t, got.Measures, 1)
				require.Len(t, got.Filters, 1)
				assert.Equal(t, "AccountID", got.Dimensions[0].Name)
				assert.Equal(t, "UserCreated", got.Dimensions[1].Name)
				assert.Equal(t, "TotalSpend", got.Measures[0].Name)
				assert.Equal(t, "accountID", got.Filters[0].Name)
				assert.Equal(t, "AccountId", got.Filters[0].FieldName)
			},
		},
		{
			name:      "uses explicit config names",
			component: newComponentFixture(t, &Config{Enabled: true}),
			config: &Config{
				Input:      "CustomReportInput",
				Dimensions: "Groups",
				Measures:   "Metrics",
				Filters:    "Predicates",
				OrderBy:    "Sort",
				Limit:      "PageSize",
				Offset:     "Cursor",
			},
			assertion: func(t *testing.T, got *Metadata, err error) {
				require.NoError(t, err)
				require.NotNil(t, got)
				assert.Equal(t, "CustomReportInput", got.InputName)
				assert.Equal(t, "Groups", got.DimensionsKey)
				assert.Equal(t, "Metrics", got.MeasuresKey)
				assert.Equal(t, "Predicates", got.FiltersKey)
				assert.Equal(t, "Sort", got.OrderBy)
				assert.Equal(t, "PageSize", got.Limit)
				assert.Equal(t, "Cursor", got.Offset)
				assert.Equal(t, "Groups", got.Dimensions[0].Section)
				assert.Equal(t, "Metrics", got.Measures[0].Section)
				assert.Equal(t, "Predicates", got.Filters[0].Section)
			},
		},
		{
			name:      "errors on missing view",
			component: &Component{Report: &Config{Enabled: true}},
			assertion: func(t *testing.T, got *Metadata, err error) {
				require.Error(t, err)
				assert.Nil(t, got)
				assert.Contains(t, err.Error(), "view was empty")
			},
		},
		{
			name:      "errors when no selectable columns",
			component: newComponentWithoutSelectableColumns(t),
			assertion: func(t *testing.T, got *Metadata, err error) {
				require.Error(t, err)
				assert.Nil(t, got)
				assert.Contains(t, err.Error(), "no selectable dimensions or measures")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := AssembleMetadata(test.component, test.config)
			test.assertion(t, got, err)
		})
	}
}

func TestBuildInputType(t *testing.T) {
	tests := []struct {
		name      string
		component *Component
		config    *Config
		assertion func(t *testing.T, got *state.Type, err error)
	}{
		{
			name:      "builds synthetic anonymous body input",
			component: newComponentFixture(t, &Config{Enabled: true}),
			assertion: func(t *testing.T, got *state.Type, err error) {
				require.NoError(t, err)
				require.NotNil(t, got)
				assert.Equal(t, "VendorInputReportInput", got.Name)
				require.Len(t, got.Parameters, 1)
				assert.True(t, got.Parameters[0].IsAnonymous())
				require.NotNil(t, got.Schema)
				rType := got.Schema.Type()
				require.NotNil(t, rType)
				assert.Equal(t, reflect.Ptr, rType.Kind())
				bodyType := rType.Elem()
				dimensions, ok := bodyType.FieldByName("Dimensions")
				require.True(t, ok)
				assert.Equal(t, `json:"dimensions,omitempty" desc:"Selected grouping dimensions"`, string(dimensions.Tag))
				measures, ok := bodyType.FieldByName("Measures")
				require.True(t, ok)
				assert.Equal(t, reflect.Struct, measures.Type.Kind())
				filters, ok := bodyType.FieldByName("Filters")
				require.True(t, ok)
				filterField, ok := filters.Type.FieldByName("AccountId")
				require.True(t, ok)
				assert.Contains(t, string(filterField.Tag), `json:"accountId,omitempty"`)
				assert.Contains(t, string(filterField.Tag), `desc:"Account identifier filter"`)
				limit, ok := bodyType.FieldByName("Limit")
				require.True(t, ok)
				assert.Equal(t, reflect.TypeOf((*int)(nil)), limit.Type)
			},
		},
		{
			name:      "uses explicit configured input type",
			component: newComponentWithExplicitInput(t),
			config:    (&Config{Input: "ExplicitReportInput"}).Normalize(),
			assertion: func(t *testing.T, got *state.Type, err error) {
				require.NoError(t, err)
				require.NotNil(t, got)
				require.NotNil(t, got.Type())
				require.NotNil(t, got.Type().Type())
				assert.Equal(t, reflect.TypeOf(explicitReportInput{}), got.Type().Type())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata, err := AssembleMetadata(test.component, test.config)
			require.NoError(t, err)
			got, err := BuildInputType(test.component, metadata, test.config)
			test.assertion(t, got, err)
		})
	}
}

func newComponentFixture(t *testing.T, reportCfg *Config) *Component {
	t.Helper()
	resource := view.EmptyResource()
	columnResource := &testResource{}
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

	return &Component{
		Name:       "vendors",
		InputName:  inputType.Name,
		Parameters: inputType.Parameters,
		View:       rootView,
		Resource:   rootView.Resource(),
		Report:     reportCfg,
	}
}

func newComponentWithoutSelectableColumns(t *testing.T) *Component {
	t.Helper()
	resource := view.EmptyResource()
	columnResource := &testResource{}
	rootView := view.NewView("vendor", "VENDOR")
	rootView.Groupable = false
	rootView.Columns = []*view.Column{
		view.NewColumn("PlainValue", "int", reflect.TypeOf(0), false),
	}
	for _, column := range rootView.Columns {
		require.NoError(t, column.Init(columnResource, text.CaseFormatUndefined, false))
	}
	rootView.SetResource(resource)
	resource.AddViews(rootView)

	inputType, err := state.NewType(state.WithParameters(nil), state.WithResource(columnResource))
	require.NoError(t, err)

	return &Component{
		Name:       "vendors",
		InputName:  inputType.Name,
		Parameters: inputType.Parameters,
		View:       rootView,
		Resource:   rootView.Resource(),
		Report:     &Config{Enabled: true},
	}
}

func newComponentWithExplicitInput(t *testing.T) *Component {
	t.Helper()
	component := newComponentFixture(t, &Config{Enabled: true, Input: "ExplicitReportInput"})
	resource := view.EmptyResource()
	require.NoError(t, resource.TypeRegistry().Register("ExplicitReportInput", xreflect.WithReflectType(reflect.TypeOf(explicitReportInput{}))))
	component.View.SetResource(resource)
	component.Resource = component.View.Resource()
	return component
}
