package compile

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitRouteKey(t *testing.T) {
	method, uri := splitRouteKey("POST:/v1/api/platform/acl/auth")
	assert.Equal(t, "POST", method)
	assert.Equal(t, "/v1/api/platform/acl/auth", uri)

	method, uri = splitRouteKey("/v1/api/platform/acl/auth")
	assert.Equal(t, "GET", method)
	assert.Equal(t, "/v1/api/platform/acl/auth", uri)
}
