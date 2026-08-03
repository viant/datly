package repository

import (
	"context"
	"go/format"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/xreflect"
)

func newEmbedFSTestComponent(t *testing.T) *Component {
	t.Helper()

	resource := view.EmptyResource()
	rootView := view.NewView("active_advertiser", "ACTIVE_ADVERTISER")
	rootView.Connector = &view.Connector{Connection: view.Connection{DBConfig: view.DBConfig{Name: "ci_ads"}}}
	rootView.Template = &view.Template{Source: "SELECT ID FROM ACTIVE_ADVERTISER"}
	rootView.Schema = state.NewSchema(reflect.TypeOf([]*struct {
		Id *int `sqlx:"ID"`
	}{}))
	resource.Types = []*view.TypeDefinition{
		{Name: "ActiveAdvertiserView", Package: "universalpixel", DataType: `struct{Id *int ` + "`sqlx:\"ID\"`" + `;}`},
	}
	require.NoError(t, resource.TypeRegistry().Register("ActiveAdvertiserView",
		xreflect.WithPackage("universalpixel"),
		xreflect.WithReflectType(reflect.TypeOf(struct {
			Id *int `sqlx:"ID"`
		}{}))))
	rootView.SetResource(resource)
	resource.AddViews(rootView)

	inputType, err := state.NewType(state.WithParameters(state.Parameters{
		&state.Parameter{Name: "AdvertiserId", In: state.NewQueryLocation("advertiserId"), Schema: state.NewSchema(reflect.TypeOf(0))},
	}), state.WithResource(&reportTestResource{}))
	require.NoError(t, err)
	inputType.Name = "ActiveAdvertiserInput"
	inputType.Package = "universalpixel"

	outputType, err := state.NewType(state.WithParameters(state.Parameters{
		&state.Parameter{Name: "Data", In: state.NewOutputLocation("view"), Schema: &state.Schema{Name: "ActiveAdvertiserView", Package: "universalpixel", Cardinality: state.Many}},
	}), state.WithResource(rootView.Resource()))
	require.NoError(t, err)
	outputType.Name = "ActiveAdvertiserOutput"
	outputType.Package = "universalpixel"

	return &Component{
		Path: contract.Path{Method: "GET", URI: "/v1/api/platform/universalpixel/activeadvertiser"},
		Meta: contract.Meta{Name: "ActiveAdvertiser"},
		View: rootView,
		Contract: contract.Contract{
			Input:  contract.Input{Type: *inputType},
			Output: contract.Output{Type: *outputType},
		},
	}
}

// The generated EmbedFS accessor used to be emitted from a raw string literal
// indented by one tab and without a trailing newline, so every generated reader
// was unformatted Go. Consumers that ran gofmt saw the file churn back on the
// next `datly gen`.
func TestGenerateOutputCode_EmbedFSAccessorIsGoFormatted(t *testing.T) {
	component := newEmbedFSTestComponent(t)

	code := component.GenerateOutputCode(context.Background(), false, true, map[string]string{})

	assert.Contains(t, code, "\nfunc (i *ActiveAdvertiserInput) EmbedFS() *embed.FS {\n\treturn &ActiveAdvertiserFS\n}\n",
		"EmbedFS accessor must be emitted at column 0 with a tab-indented body")
	assert.NotContains(t, code, "\n\tfunc (", "no top-level func may be indented")
	assert.True(t, strings.HasSuffix(code, "\n"), "generated file must end with a newline")
}

func TestGenerateOutputCode_IsGofmtStable(t *testing.T) {
	component := newEmbedFSTestComponent(t)

	code := component.GenerateOutputCode(context.Background(), false, true, map[string]string{})

	formatted, err := format.Source([]byte(code))
	require.NoError(t, err, "generated code must parse")
	assert.Equal(t, string(formatted), code, "generated code must already be gofmt-clean")
}

// Regenerating an unchanged component must not produce a different file, or
// consumers get spurious diffs on every codegen run.
func TestGenerateOutputCode_IsDeterministic(t *testing.T) {
	first := newEmbedFSTestComponent(t).GenerateOutputCode(context.Background(), false, true, map[string]string{})
	second := newEmbedFSTestComponent(t).GenerateOutputCode(context.Background(), false, true, map[string]string{})

	assert.Equal(t, first, second)
}
