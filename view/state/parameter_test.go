package state

import (
	"context"
	"embed"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xreflect"
)

type parameterNamedPatchFoos struct {
	ID int
}

type testResource struct {
	params map[string]*Parameter
}

func (t *testResource) LookupParameter(name string) (*Parameter, error) { return t.params[name], nil }
func (t *testResource) AppendParameter(parameter *Parameter)            {}
func (t *testResource) ViewSchema(context.Context, string) (*Schema, error) {
	return nil, nil
}
func (t *testResource) ViewSchemaPointer(context.Context, string) (*Schema, error) {
	return nil, nil
}
func (t *testResource) LookupType() xreflect.LookupType { return nil }
func (t *testResource) LoadText(context.Context, string) (string, error) {
	return "", nil
}
func (t *testResource) Codecs() *codec.Registry               { return nil }
func (t *testResource) CodecOptions() *codec.Options          { return nil }
func (t *testResource) ExpandSubstitutes(text string) string  { return text }
func (t *testResource) ReverseSubstitutes(text string) string { return text }
func (t *testResource) EmbedFS() *embed.FS                    { return nil }
func (t *testResource) SetFSEmbedder(*FSEmbedder)             {}

func TestParameter_initParamBasedParameter_ResolvesSourceSchemaEvenWithExplicitDataType(t *testing.T) {
	resource := &testResource{
		params: map[string]*Parameter{
			"Foos": {
				Name:   "Foos",
				In:     NewBodyLocation(""),
				Schema: NewSchema(reflect.TypeOf(&struct{ ID int }{})),
			},
		},
	}
	param := &Parameter{
		Name:           "CurFoosId",
		In:             NewParameterLocation("Foos"),
		Schema:         &Schema{DataType: `*struct { Values []int "json:\",omitempty\"" }`},
		PreserveSchema: true,
	}

	require.NoError(t, param.initParamBasedParameter(context.Background(), resource))
	require.NotNil(t, param.Schema)
	require.Equal(t, reflect.TypeOf(&struct{ ID int }{}), param.Schema.Type())
}

func TestParameters_ReflectType_QualifiedNamedDataTypeResolves(t *testing.T) {
	registry := xreflect.NewTypes()
	require.NoError(t, registry.Register("FoosView", xreflect.WithPackage("patch_basic_one"), xreflect.WithReflectType(reflect.TypeOf(parameterNamedPatchFoos{}))))

	params := Parameters{
		&Parameter{
			Name:   "Foos",
			In:     NewBodyLocation(""),
			Schema: &Schema{Name: "FoosView", Package: "patch_basic_one", DataType: "*patch_basic_one.FoosView", Cardinality: One},
		},
	}

	rType, err := params.ReflectType("patch_basic_one", registry.Lookup)
	require.NoError(t, err)
	field, ok := rType.FieldByName("Foos")
	require.True(t, ok)
	require.Equal(t, reflect.TypeOf(&parameterNamedPatchFoos{}), field.Type)
}

func TestParameter_buildTag_ParamDoesNotOverrideSourceDataType(t *testing.T) {
	param := &Parameter{
		Name:   "CurFoosId",
		In:     NewParameterLocation("Foos"),
		Schema: &Schema{Name: "CurFoosId", DataType: `*struct { Values []int "json:\",omitempty\"" }`},
		Output: &Codec{
			Name:   "structql",
			Schema: &Schema{Name: "CurFoosId", DataType: `*struct { Values []int "json:\",omitempty\"" }`},
		},
	}

	tag := string(param.buildTag("CurFoosId"))
	require.NotContains(t, tag, `dataType:"`)
	require.Contains(t, tag, `kind=param`)
	require.Contains(t, tag, `in=Foos`)
}
