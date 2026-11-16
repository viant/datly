package output

import (
	"context"
	"encoding/json"
	dexec "github.com/viant/datly/exec"
	structjson "github.com/viant/structology/encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	xshape "github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
	legacyresponse "github.com/viant/xdatly/handler/response"
)

type selectorConvertedRows struct {
	Private string
	ID      int
}
type selectorConvertedPublic struct {
	Computed     string               `mcp:"name=display" json:"computed"`
	ID           int                  `json:"id"`
	Secret       string               `mcp:"-" json:"secret"`
	Child        *contractFormatChild `json:"child"`
	BackingState string               `json:"-" choice:"READY" choice:"PENDING" enumAccept:"literal" enumStyle:"literal"`
}
type selectorConvertedOutput struct {
	legacyresponse.Status
	Rows   []*selectorConvertedRows   `json:"-"`
	Public []*selectorConvertedPublic `json:"data"`
	Other  []*selectorConvertedPublic `json:"other"`
}

func convertedRootComponent() *spec.Component {
	return &spec.Component{RootView: &spec.View{Name: "records", Source: &spec.ViewSource{SQL: "SELECT 1 AS id"}}, Parameters: []*spec.Parameter{{Name: "Projection", Source: spec.BindSource{Kind: "query", Name: "fields"}, QuerySelector: &spec.QuerySelectorBinding{View: "records", Property: spec.SelectorPropertyFields}}, {Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}}, {Name: "Public", Source: spec.BindSource{Kind: "output", Name: "body"}}}}
}

func TestSelectorOutputRootUsesPublicBodyAndEffectiveCatalog(t *testing.T) {
	component := convertedRootComponent()
	base, err := (Compiler{}).Compile(CompileInput{Component: component, Type: reflect.TypeFor[selectorConvertedOutput]()})
	require.NoError(t, err)
	require.Equal(t, "Rows", base.rows.name)
	public, err := base.WithMCPOutputRoot()
	require.NoError(t, err)
	require.Equal(t, "Public", public.rows.name)
	require.Equal(t, "Rows", base.rows.name, "transport plan leaves original codec owner unchanged")
	public, err = public.WithMCPVisibility()
	require.NoError(t, err)
	root := public.SelectorOutputRoot()
	require.Equal(t, []string{"Public"}, root.Path)
	require.Equal(t, reflect.TypeFor[selectorConvertedPublic](), root.RowType)
	field, ok, err := root.Resolve("display")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []int{0}, field.Index)
	field.Index[0] = 99
	field, ok, err = root.Resolve("display")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []int{0}, field.Index)
	_, ok, err = root.Resolve("secret")
	require.NoError(t, err)
	require.False(t, ok)
	walk, err := xshape.CompileRewrite(base.Type())
	require.NoError(t, err)
	metadata, err := walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if f.Name != "ID" && f.Name != "Secret" {
			return nil, nil
		}
		copy := *f
		copy.Tag = `json:"-"`
		return &copy, nil
	})
	require.NoError(t, err)
	variant, err := public.WithRepresentation(metadata)
	require.NoError(t, err)
	variant, err = variant.WithMCPVisibility()
	require.NoError(t, err)
	_, ok, err = variant.SelectorOutputRoot().Resolve("id")
	require.NoError(t, err)
	require.False(t, ok)
	_, ok, err = public.SelectorOutputRoot().Resolve("id")
	require.NoError(t, err)
	require.True(t, ok, "variants do not mutate cached catalogs")
	value := &selectorConvertedOutput{Public: []*selectorConvertedPublic{{ID: 2, Computed: "shown", Secret: "PRIVATE", Child: &contractFormatChild{Visible: "nested-public", Secret: "PRIVATE_NESTED"}}}}
	child, ok, err := variant.SelectorOutputRoot().Resolve("child")
	require.NoError(t, err)
	require.True(t, ok)
	filter, err := structjson.NewFieldFilter(reflect.TypeFor[selectorConvertedOutput](), []structjson.FieldSelection{{Path: []string{"Public"}, Indexes: [][]int{child.Index}}})
	require.NoError(t, err)
	capture := dexec.CaptureOutputSelection(context.Background())
	scope, complete := dexec.ScopeOutputSelection(capture)
	dexec.BeginOutputSelection(scope)
	dexec.PublishOutputSelection(scope, value, filter)
	complete(value, nil)
	encoded, err := variant.Encode(capture, "json", value)
	require.NoError(t, err)
	require.JSONEq(t, `{"data":[{"child":{"visible":"nested-public"}}],"other":null}`, string(encoded.Data))
	for _, format := range []string{"csv", "xlsx"} {
		text := contractExportText(t, variant, capture, format, value)
		require.Contains(t, text, "nested-public")
		require.NotContains(t, text, "PRIVATE")
	}
	require.Equal(t, "PRIVATE_NESTED", value.Public[0].Child.Secret)
	// Hiding the whole holder removes its selectable fields as well.
	metadata, err = walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if f.Name != "Public" {
			return nil, nil
		}
		copy := *f
		copy.Tag = `json:"-"`
		return &copy, nil
	})
	require.NoError(t, err)
	hidden, err := public.WithRepresentation(metadata)
	require.NoError(t, err)
	_, ok, err = hidden.SelectorOutputRoot().Resolve("id")
	require.NoError(t, err)
	require.False(t, ok)
}

type opaqueConvertedRow struct{ ID int }

func (opaqueConvertedRow) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]int{"other": 1})
}

func TestSelectorOutputRootRejectsUnsupportedDeclarations(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		edit func(*spec.Component)
	}{
		{"ambiguous body", reflect.TypeFor[selectorConvertedOutput](), func(c *spec.Component) {
			c.Parameters = append(c.Parameters, &spec.Parameter{Name: "Other", Source: spec.BindSource{Kind: "output", Name: "body"}})
		}},
		{"missing holder", reflect.TypeFor[selectorConvertedOutput](), func(c *spec.Component) { c.Parameters[2].Name = "Missing" }},
		{"singleton", reflect.TypeFor[selectorConvertedOutput](), func(c *spec.Component) { c.RootView.Cardinality = spec.CardinalityOne }},
		{"wrong linked body", reflect.TypeFor[selectorConvertedOutput](), func(c *spec.Component) { c.Parameters[2].TypeExpr = "[]string" }},
		{"nested holder", reflect.TypeFor[selectorConvertedOutput](), func(c *spec.Component) { c.Parameters[2].Name = "Public.ID" }},
		{"opaque row", reflect.TypeFor[struct {
			Rows   []*selectorConvertedRows `json:"-"`
			Public []*opaqueConvertedRow
		}](), func(*spec.Component) {}},
		{"scalar body", reflect.TypeFor[struct {
			Rows   []*selectorConvertedRows `json:"-"`
			Public string
		}](), func(*spec.Component) {}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := convertedRootComponent()
			tc.edit(c)
			base, err := (Compiler{}).Compile(CompileInput{Component: c, Type: tc.typ})
			require.NoError(t, err)
			_, err = base.WithMCPOutputRoot()
			require.Error(t, err)
		})
	}
	c := convertedRootComponent()
	c.Parameters = c.Parameters[:2]
	base, err := (Compiler{}).Compile(CompileInput{Component: c, Type: reflect.TypeFor[selectorConvertedOutput]()})
	require.NoError(t, err)
	unchanged, err := base.WithMCPOutputRoot()
	require.NoError(t, err)
	require.Same(t, base, unchanged)
	require.Nil(t, unchanged.SelectorOutputRoot(), "missing declaration cannot guess a public holder")
}

func TestSelectorOutputRootCatalogAmbiguityFailsClosed(t *testing.T) {
	typ := reflect.TypeFor[struct {
		Rows   []*selectorConvertedRows `json:"-"`
		Public []struct {
			A int `json:"duplicate"`
			B int `json:"DUPLICATE"`
		}
	}]()
	base, err := (Compiler{}).Compile(CompileInput{Component: convertedRootComponent(), Type: typ})
	require.NoError(t, err)
	public, err := base.WithMCPOutputRoot()
	require.NoError(t, err)
	_, ok, err := public.SelectorOutputRoot().Resolve("duplicate")
	require.Error(t, err)
	require.False(t, ok)
}

func TestSelectorOutputRootLeavesOtherExecutionContractsUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  reflect.Type
		edit func(*spec.Component)
	}{
		{"visible multi-view", reflect.TypeFor[struct {
			Rows   []*selectorConvertedRows
			Public []*selectorConvertedPublic
			Other  []*selectorConvertedPublic
		}](), func(c *spec.Component) {
			c.Parameters = append(c.Parameters, &spec.Parameter{Name: "Other", Source: spec.BindSource{Kind: "output", Name: "view"}})
		}},
		{"writer", reflect.TypeFor[selectorConvertedOutput](), func(c *spec.Component) {
			c.Settings = &spec.Settings{Mutation: "post"}
			c.Parameters = append(c.Parameters, &spec.Parameter{Name: "Other", Source: spec.BindSource{Kind: "output", Name: "body"}})
		}},
		{"custom", reflect.TypeFor[selectorConvertedOutput](), func(c *spec.Component) {
			c.Routes = []*spec.Route{{Method: "GET", Path: "/custom", Handler: "factory"}}
			c.Parameters = append(c.Parameters, &spec.Parameter{Name: "Other", Source: spec.BindSource{Kind: "output", Name: "body"}})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := convertedRootComponent()
			tc.edit(c)
			base, err := (Compiler{}).Compile(CompileInput{Component: c, Type: tc.typ, DataField: "Rows"})
			require.NoError(t, err)
			actual, err := base.WithMCPOutputRoot()
			require.NoError(t, err)
			require.Same(t, base, actual)
			require.Nil(t, actual.SelectorOutputRoot())
		})
	}
}

func TestSelectorOutputRootRejectsLegacyTypeIndexTransfer(t *testing.T) {
	base, err := (Compiler{}).Compile(CompileInput{Component: convertedRootComponent(), Type: reflect.TypeFor[selectorConvertedOutput](), DataField: "Rows"})
	require.NoError(t, err)
	public, err := base.WithMCPOutputRoot()
	require.NoError(t, err)
	changed := reflect.TypeFor[struct {
		Rows   []*selectorConvertedRows `json:"-"`
		Public []struct {
			ID int `json:"id"`
		} `json:"data"`
		Other []*selectorConvertedPublic `json:"other"`
	}]()
	_, err = public.WithType(changed)
	require.ErrorContains(t, err, "canonical Contract representation")
	_, err = base.WithType(changed)
	require.NoError(t, err, "ordinary legacy output path retains its established type projection")
}

func TestSelectorOutputRootLeavesSingletonWithoutFieldsUnchanged(t *testing.T) {
	c := convertedRootComponent()
	c.Parameters = c.Parameters[1:]
	c.RootView.Cardinality = spec.CardinalityOne
	c.RootView.Selector = &spec.Selector{AllowFields: true}
	typ := reflect.TypeFor[struct {
		Rows   *selectorConvertedRows   `json:"-"`
		Public *selectorConvertedPublic `json:"data"`
	}]()
	base, err := (Compiler{}).Compile(CompileInput{Component: c, Type: typ, DataField: "Rows"})
	require.NoError(t, err)
	actual, err := base.WithMCPOutputRoot()
	require.NoError(t, err)
	require.Same(t, base, actual)
	require.Nil(t, actual.SelectorOutputRoot())
}

func TestSelectorOutputRootLeavesDeclaredFieldsSingletonCarrierUnchanged(t *testing.T) {
	c := convertedRootComponent()
	c.RootView.Cardinality = spec.CardinalityOne
	c.RootView.Selector = &spec.Selector{AllowFields: true}
	typ := reflect.TypeFor[struct {
		Rows   *selectorConvertedRows   `json:"-"`
		Public *selectorConvertedPublic `json:"data"`
	}]()
	base, err := (Compiler{}).Compile(CompileInput{Component: c, Type: typ, DataField: "Rows"})
	require.NoError(t, err)
	require.Equal(t, reflect.Struct, base.rows.sourceType.Kind())
	actual, err := base.WithMCPOutputRoot()
	require.NoError(t, err)
	require.Same(t, base, actual)
	require.Nil(t, actual.SelectorOutputRoot())
}

func TestSelectorOutputRootDirectHolderSharesVisibilityOwner(t *testing.T) {
	for _, tc := range []struct {
		name, key, value string
		visible          bool
	}{
		{"MCP name", "mcp", "name=items", true}, {"MCP hidden", "mcp", "-", false}, {"format ignored", "format", "ignore=true", false}, {"Contract hidden", "json", "-", false}, {"internal holder", "internal", "true", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := convertedRootComponent()
			c.Settings = &spec.Settings{CaseFormat: "lc"}
			base, err := (Compiler{}).Compile(CompileInput{Component: c, Type: reflect.TypeFor[selectorConvertedOutput](), DataField: "Rows"})
			require.NoError(t, err)
			public, err := base.WithMCPOutputRoot()
			require.NoError(t, err)
			walk, err := xshape.CompileRewrite(public.Type())
			require.NoError(t, err)
			metadata, err := walk.RewriteRepresentation(func(_ string, _ reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
				if f.Name != "Public" {
					return nil, nil
				}
				copy := *f
				copy.Tag = (&xcontract.Field{Name: f.Name, Tag: f.Tag}).SetTag(tc.key, tc.value).Tag
				return &copy, nil
			})
			require.NoError(t, err)
			public, err = public.WithRepresentation(metadata)
			require.NoError(t, err)
			public, err = public.WithMCPVisibility()
			require.NoError(t, err)
			_, exists, err := public.SelectorOutputRoot().Resolve("id")
			require.NoError(t, err)
			require.Equal(t, tc.visible, exists)
		})
	}
	c := convertedRootComponent()
	c.Settings = &spec.Settings{CaseFormat: "lc", Output: &spec.OutputSettings{Exclude: []string{"Public"}}}
	base, err := (Compiler{}).Compile(CompileInput{Component: c, Type: reflect.TypeFor[selectorConvertedOutput](), DataField: "Rows"})
	require.NoError(t, err)
	public, err := base.WithMCPOutputRoot()
	require.NoError(t, err)
	_, exists, err := public.SelectorOutputRoot().Resolve("id")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestSelectorOutputRootContractExposedBackingEnumCannotEscapeMask(t *testing.T) {
	c := convertedRootComponent()
	c.Settings = &spec.Settings{CaseFormat: "lc"}
	base, err := (Compiler{}).Compile(CompileInput{Component: c, Type: reflect.TypeFor[selectorConvertedOutput](), DataField: "Rows"})
	require.NoError(t, err)
	plan, err := base.WithMCPOutputRoot()
	require.NoError(t, err)
	walk, err := xshape.CompileRewrite(plan.Type())
	require.NoError(t, err)
	metadata, err := walk.RewriteRepresentation(func(_ string, parent reflect.Type, f *xshape.FieldMetadata) (*xshape.FieldMetadata, error) {
		if parent != reflect.TypeFor[selectorConvertedPublic]() || f.Name != "BackingState" {
			return nil, nil
		}
		copy := *f
		copy.Tag = (&xcontract.Field{Name: f.Name, Tag: f.Tag}).SetTag("json", "publicState").SetMCPName("publicState").Tag
		return &copy, nil
	})
	require.NoError(t, err)
	plan, err = plan.WithRepresentation(metadata)
	require.NoError(t, err)
	plan, err = plan.WithMCPVisibility()
	require.NoError(t, err)
	selected, ok, err := plan.SelectorOutputRoot().Resolve("id")
	require.NoError(t, err)
	require.True(t, ok)
	exposed, ok, err := plan.SelectorOutputRoot().Resolve("publicState")
	require.NoError(t, err)
	require.True(t, ok)
	filter, err := structjson.NewFieldFilter(plan.Type(), []structjson.FieldSelection{{Path: []string{"Public"}, Indexes: [][]int{selected.Index}}})
	require.NoError(t, err)
	require.True(t, filter.ExcludeIndexes(append([]int{2}, exposed.Index...)), "canonical mask already excludes the backing field")
	value := &selectorConvertedOutput{Public: []*selectorConvertedPublic{{ID: 2, BackingState: "READY"}}}
	capture := dexec.CaptureOutputSelection(context.Background())
	scope, complete := dexec.ScopeOutputSelection(capture)
	dexec.BeginOutputSelection(scope)
	dexec.PublishOutputSelection(scope, value, filter)
	complete(value, nil)
	encoded, err := plan.Encode(capture, "json", value)
	require.NoError(t, err)
	require.JSONEq(t, `{"data":[{"id":2}],"other":null}`, string(encoded.Data))
}
