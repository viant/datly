package registry_test

import (
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/bindly/provider/request"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
)

type unionHTTPRow struct {
	Fee *choice.ScalarValue `json:"fee" choice:"4237:AD_SERVING" choice:"4241:AD_SERVING" enumAccept:"scalar" enumStyle:"scalar" enumInteger:"passthrough" enumUnknownString:"preserve"`
}
type unionHTTPBody struct {
	Rows []*unionHTTPRow `json:"rows"`
}
type unionHTTPInput struct {
	Body *unionHTTPBody `parameter:",kind=body,required=true"`
}

func TestScalarUnionHTTPRequestBinding(t *testing.T) {
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: &spec.Component{Routes: []*spec.Route{{Method: "POST", Path: "/fees"}}}, InputType: reflect.TypeFor[unionHTTPInput](), OutputType: reflect.TypeFor[struct{}]()})
	require.NoError(t, err)
	plan, ok := artifact.Input.ForRoute(spec.RouteRef{Method: "POST", Path: "/fees"})
	require.True(t, ok)
	for _, tc := range []struct {
		raw     string
		integer int64
		str     string
		invalid bool
	}{
		{raw: "4237", integer: 4237}, {raw: "4241", integer: 4241}, {raw: "9007199254740993", integer: 9007199254740993},
		{raw: "9223372036854775807", integer: 9223372036854775807}, {raw: "-9223372036854775808", integer: -9223372036854775808},
		{raw: `"AD_SERVING"`, str: "AD_SERVING"}, {raw: `"unknown"`, str: "unknown"}, {raw: `"4241"`, str: "4241"}, {raw: "null"},
		{raw: "true", invalid: true}, {raw: "1.5", invalid: true}, {raw: "1e3", invalid: true}, {raw: "9223372036854775808", invalid: true}, {raw: "{}", invalid: true}, {raw: "[]", invalid: true},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/fees", strings.NewReader(`{"rows":[{"fee":`+tc.raw+`}]}`))
			req.Header.Set("Content-Type", "application/json")
			source, err := request.New(req)
			require.NoError(t, err)
			defer source.Close()
			root, err := bindly.NewInjector()
			require.NoError(t, err)
			scope, err := root.ForScope(source.Providers()...)
			require.NoError(t, err)
			var input unionHTTPInput
			err = scope.Bind(t.Context(), &input, bindly.WithPlan(plan.Plan()))
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, input.Body)
			require.Len(t, input.Body.Rows, 1)
			actual := input.Body.Rows[0].Fee
			if tc.raw == "null" {
				require.Nil(t, actual)
				return
			}
			require.NotNil(t, actual)
			if tc.str != "" {
				require.NotNil(t, actual.String)
				require.Equal(t, tc.str, *actual.String)
				require.Nil(t, actual.Integer)
			} else {
				require.NotNil(t, actual.Integer)
				require.Equal(t, tc.integer, *actual.Integer)
				require.Nil(t, actual.String)
			}
		})
	}
}
