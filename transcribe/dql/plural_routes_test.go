package dql

import (
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestPluralRouteSharedMetadataAndPlaceholders(t *testing.T) {
	routes := []string{`#setting($_ = $route('/records/{id}','patch'))`, `#setting($_ = $route('/restore/{key}','PATCH'))`}
	metadata := `#setting($_ = $request_body_mode('on_demand'))
#setting($_ = $api_key('X-Key','test-key'))
#setting($_ = $internal(true))`
	for _, position := range []int{0, 1, 2} {
		parts := append([]string(nil), routes...)
		parts = append(parts[:position], append([]string{metadata}, parts[position:]...)...)
		source := strings.Join(parts, "\n") + "\nSELECT 1"
		prepared := PrepareSource(source)
		require.NoError(t, prepared.Err())
		require.Len(t, prepared.Directives.Routes, 2)
		component, err := ParsePreparedComponentSource("example.com/app", "Records", prepared)
		require.NoError(t, err)
		require.Len(t, component.Routes, 2)
		for _, route := range component.Routes {
			require.Equal(t, "PATCH", route.Method)
			require.Equal(t, "on_demand", route.RequestBodyMode)
			require.Equal(t, "X-Key", route.APIKeyHeader)
			require.Equal(t, "test-key", route.APIKeyValue)
			require.True(t, route.Internal)
		}
		found := map[string]bool{}
		for _, param := range component.Parameters {
			if param.Source.Kind == "path" {
				found[param.Source.Name] = true
			}
		}
		require.True(t, found["id"])
		require.True(t, found["key"])
	}
}
func TestPluralRouteRejections(t *testing.T) {
	for _, tc := range []struct{ name, source, want string }{
		{"duplicate", `#setting($_ = $route('/a','PATCH'))
#setting($_ = $route('/a','patch'))`, "declared more than once"},
		{"mcp ambiguity", `#setting($_ = $route('/a','PATCH'))
#setting($_ = $route('/b','PATCH'))
#setting($_ = $mcp('records'))`, "ambiguous across 2 routes"},
		{"second placeholder conflict", `#define($_ = $key<string>(query/key))
#setting($_ = $route('/a','PATCH'))
#setting($_ = $route('/b/{key}','PATCH'))`, "conflicts with declared query"},
		{"duplicate key metadata", `#setting($_ = $api_key('X-Key','one'))
#setting($_ = $route('/a','PATCH'))
#setting($_ = $api_key('X-Key','two'))`, "declared once"},
	} {
		t.Run(tc.name, func(t *testing.T) { require.ErrorContains(t, PrepareSource(tc.source+"\nSELECT 1").Err(), tc.want) })
	}
}

func TestPluralStaticRoutePathsRejected(t *testing.T) {
	source := `#setting($_ = $route('/a','GET'))
#setting($_ = $route('/b','GET'))
#setting($_ = $static_content('file:///tmp/fixture.txt','/content'))`
	prepared := PrepareSource(source)
	require.NoError(t, prepared.Err())
	_, err := ParsePreparedComponentSource("example.com/app", "Static", prepared)
	require.ErrorContains(t, err, "static declarations do not support multiple route paths")
}
