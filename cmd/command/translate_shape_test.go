package command

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/cmd/options"
)

func TestParseShapeRulePath(t *testing.T) {
	method, uri := parseShapeRulePath(`/* {"Method":"POST","URI":"/v1/api/orders"} */ SELECT 1`, "orders", "/v1/api")
	assert.Equal(t, "POST", method)
	assert.Equal(t, "/v1/api/orders", uri)

	method, uri = parseShapeRulePath(`SELECT 1`, "orders", "/v1/api")
	assert.Equal(t, "GET", method)
	assert.Equal(t, "/v1/api/orders", uri)
}

func TestRoutePathForShape(t *testing.T) {
	rule := &options.Rule{Project: "/repo", Source: []string{"/repo/dql/platform/campaign/post.dql"}}
	routeYAML, routeRoot, relDir, stem, err := routePathForShape(rule, "/repo/dev", "/repo/dql/platform/campaign/post.dql")
	require.NoError(t, err)
	assert.Equal(t, "/repo/dev/Datly/routes/platform/campaign/post.yaml", routeYAML)
	assert.Equal(t, "/repo/dev/Datly/routes", routeRoot)
	assert.Equal(t, filepath.ToSlash("platform/campaign"), relDir)
	assert.Equal(t, "post", stem)
}
