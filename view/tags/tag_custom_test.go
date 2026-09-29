package tags

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTag_UpdateTag_NormalizesCustomTagQuotes(t *testing.T) {
	tag := &Tag{
		View: &View{
			Name:      "siteType",
			CustomTag: `'json:",omitempty"'`,
		},
	}

	actual := string(tag.UpdateTag(reflect.StructTag("")))
	require.Contains(t, actual, `json:",omitempty"`)
	require.NotContains(t, actual, `'json:",omitempty"`)
}
