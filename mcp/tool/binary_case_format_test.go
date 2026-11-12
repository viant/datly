package tool

import (
	"context"
	"mime/multipart"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
)

func TestBinarySyntheticArgumentsHonorCaseFormat(t *testing.T) {
	for _, tc := range []struct{ format, blob, payload string }{
		{"", "blob", "payload"}, {"lc", "blob", "payload"}, {"uc", "Blob", "Payload"}, {"lu", "blob", "payload"},
	} {
		t.Run(tc.format, func(t *testing.T) {
			compile := func(typ reflect.Type, bindings []bindly.BindingSpec) *Plan {
				plan, err := NewCompiler().Compile(Input{CaseFormat: tc.format, Exposure: &spec.MCPExposure{Kind: spec.MCPExposureTool, Name: "binary"}, Contract: testRouteContract(t, typ, bindings)})
				require.NoError(t, err)
				return plan
			}
			plan := compile(reflect.TypeFor[originalHybridInput](), []bindly.BindingSpec{{Path: "Body", Location: bindstate.Location{Kind: "body", In: "body"}}})
			require.Contains(t, plan.Metadata().InputSchema.Properties, tc.blob)
			scope, err := plan.Scope(map[string]interface{}{tc.blob: originalBlob("workbook")})
			require.NoError(t, err)
			defer scope.Close()
			value, found, err := scope.Body().Locate(nil).Value(context.Background(), reflect.TypeFor[originalHybridBody](), "")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []byte("workbook"), value.(originalHybridBody).Raw)

			plan = compile(reflect.TypeFor[originalMultipartInput](), []bindly.BindingSpec{{Path: "Files", Location: bindstate.Location{Kind: "form", In: "file"}}})
			props := plan.Metadata().InputSchema.Properties
			require.Contains(t, props, tc.payload)
			require.Contains(t, props, "Files", "explicit MCP names remain authoritative")
			scope, err = plan.Scope(map[string]interface{}{"Files": originalBlob("file"), tc.payload: map[string]interface{}{"ownerId": 9}})
			require.NoError(t, err)
			defer scope.Close()
			value, found, err = scope.Form().Locate(nil).Value(context.Background(), reflect.TypeFor[[]*multipart.FileHeader](), "file")
			require.NoError(t, err)
			require.True(t, found)
			require.Len(t, value, 1)
			value, found, err = scope.Form().Locate(nil).Value(context.Background(), reflect.TypeFor[string](), "ownerId")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, "9", value)
		})
	}
}
