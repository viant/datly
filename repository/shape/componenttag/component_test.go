package componenttag

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCubeComposeOptionsRoundTrip(t *testing.T) {
	source := &Component{
		Name:                  "Metrics",
		Report:                true,
		ReportCompose:         true,
		ReportComposeMCP:      true,
		ReportComposeMaxCubes: 6,
		ReportComposeMaxLimit: 25,
	}
	tag := source.Tag()
	require.NotNil(t, tag)
	parsed, err := Parse(reflect.StructTag(`component:"` + string(tag.Values) + `"`))
	require.NoError(t, err)
	require.NotNil(t, parsed.Component)
	assert.True(t, parsed.Component.ReportCompose)
	assert.True(t, parsed.Component.ReportComposeMCP)
	assert.Equal(t, 6, parsed.Component.ReportComposeMaxCubes)
	assert.Equal(t, 25, parsed.Component.ReportComposeMaxLimit)
}
