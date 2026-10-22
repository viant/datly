package output

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/xeipuuv/gojsonschema"
)

func TestSelectableRowSchemaPreservesTypesAndEnvelopeRequirements(t *testing.T) {
	type row struct {
		Publisher   string `json:"publisher"`
		Genre       string `json:"genre"`
		Impressions int    `json:"impressions"`
	}
	type metadata struct {
		Trace string `json:"trace"`
	}
	type envelope struct {
		Data     []row    `json:"data"`
		Metadata metadata `json:"metadata"`
	}
	for _, kind := range []string{"static", "reader", "handler"} {
		t.Run(kind, func(t *testing.T) {
			input := CompileInput{Type: reflect.TypeFor[envelope](), DataField: "Data"}
			if kind == "reader" {
				input.Component = &spec.Component{RootView: &spec.View{Name: "rows", Selector: &spec.Selector{AllowFields: true}}}
			}
			if kind == "handler" {
				input.SelectableRows = true
			}
			plan, err := (Compiler{}).Compile(input)
			require.NoError(t, err)
			contract, err := plan.JSONSchema()
			require.NoError(t, err)
			validate := func(payload string) bool {
				checked, e := gojsonschema.Validate(gojsonschema.NewGoLoader(contract), gojsonschema.NewStringLoader(payload))
				require.NoError(t, e)
				return checked.Valid()
			}
			require.Equal(t, kind != "static", validate(`{"data":[{"publisher":"fixture","impressions":42}],"metadata":{"trace":"x"}}`))
			require.False(t, validate(`{"data":[{"impressions":"invalid"}],"metadata":{"trace":"x"}}`), "selection cannot remove scalar type checks")
			require.False(t, validate(`{"data":[],"metadata":{}}`), "envelope metadata remains required")
			require.False(t, validate(`{"metadata":{"trace":"x"}}`), "data holder remains required")
		})
	}
}
