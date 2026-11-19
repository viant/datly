package compile

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/repository/shape"
	dqlshape "github.com/viant/datly/repository/shape/dql/shape"
)

func TestExtractRuleSettings_RouteDirectiveOverridesHeader(t *testing.T) {
	source := &shape.Source{
		DQL: "/* {\"URI\":\"/v1/api/legacy\",\"Method\":\"GET\"} */\n" +
			"#settings($_ = $route('/v1/api/orders', 'POST', 'PATCH'))\n" +
			"SELECT 1",
	}

	settings := extractRuleSettings(source, &dqlshape.Directives{
		Route: &dqlshape.RouteDirective{
			URI:     "/v1/api/orders",
			Methods: []string{"POST", "PATCH"},
		},
	})
	assert.Equal(t, "/v1/api/orders", settings.URI)
	assert.Equal(t, "POST,PATCH", settings.Method)
}
