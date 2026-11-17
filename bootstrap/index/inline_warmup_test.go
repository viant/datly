package index

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/tag"
)

func TestInlineWarmupDiscoveryUsesNativePresenceSemantics(t *testing.T) {
	for _, tc := range []struct {
		name     string
		policies []*spec.CacheWarmupSettings
		want     bool
	}{
		{"nil", nil, false}, {"empty", []*spec.CacheWarmupSettings{}, false}, {"all-nil", []*spec.CacheWarmupSettings{nil}, false},
		{"present", []*spec.CacheWarmupSettings{nil, {IndexColumn: "id"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			child := &spec.View{Source: &spec.ViewSource{Bindings: &spec.ViewBindings{InlineWarmups: tc.policies}}}
			canonical := &spec.Component{RootView: &spec.View{Relations: []*spec.Relation{{View: child}}}}
			require.Equal(t, tc.want, hasComponentWarmupConfiguration(canonical))
			value, err := (tag.View{Name: "child", InlineWarmups: tc.policies}).Value()
			require.NoError(t, err)
			typ := reflect.StructOf([]reflect.StructField{{Name: "Child", Type: reflect.TypeFor[[]struct{ ID int }](), Tag: reflect.StructTag(`view:"` + value + `"`)}})
			found, err := typeHasWarmupTag(typ, map[reflect.Type]bool{})
			require.NoError(t, err)
			require.Equal(t, tc.want, found)
		})
	}
}
