package input

import (
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	bindstate "github.com/viant/bindly/state"
)

type metadataBody struct {
	Enabled bool    `json:"enabled"`
	Note    *string `json:"note"`
}

func TestMetadataOnlyRequestRetainsNativeJSONProjectionAndCallerOwnership(t *testing.T) {
	for _, root := range []string{"", "data"} {
		for _, noBody := range []bool{false, true} {
			t.Run(root+map[bool]string{false: "nil", true: "NoBody"}[noBody], func(t *testing.T) {
				plan, err := NewCompiler().Compile([]Argument{{PublicName: "Data", Source: bindstate.Location{Kind: "body", In: root}, SourceType: reflect.TypeFor[metadataBody]()}})
				require.NoError(t, err)
				req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, "http://localhost/v1/items", nil)
				require.NoError(t, err)
				req.Header = nil // A caller may supply a request without allocated headers.
				if noBody {
					req.Body = http.NoBody
				}
				originalBody := req.Body
				scope, err := plan.ScopeRequest(Arguments{"Data": map[string]interface{}{"enabled": false, "note": nil}}, req)
				require.NoError(t, err)
				defer scope.Close()
				value, found, err := scope.Body().Locate(nil).Value(t.Context(), reflect.TypeFor[metadataBody](), root)
				require.NoError(t, err)
				require.True(t, found)
				encoded, err := json.Marshal(value)
				require.NoError(t, err)
				require.JSONEq(t, `{"enabled":false,"note":null}`, string(encoded))
				value, found, err = scope.Request().Locate(nil).Value(t.Context(), reflect.TypeFor[*http.Request](), "")
				require.NoError(t, err)
				require.True(t, found)
				projected := value.(*http.Request)
				require.NotSame(t, req, projected)
				require.Equal(t, http.MethodDelete, projected.Method)
				require.Equal(t, "/v1/items", projected.URL.Path)
				require.Nil(t, req.Header, "native projection cannot allocate caller headers")
				require.Equal(t, "application/json", projected.Header.Get("Content-Type"))
				body, err := io.ReadAll(projected.Body)
				require.NoError(t, err)
				expected := `{"enabled":false,"note":null}`
				if root != "" {
					expected = `{"data":` + expected + `}`
				}
				require.JSONEq(t, expected, string(body))
				require.Equal(t, originalBody, req.Body)
				require.Empty(t, req.Header.Get("Content-Type"), "native projection cannot mutate caller request headers")
			})
		}
	}
}
