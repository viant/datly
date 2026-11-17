package data

import (
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
	"testing"
)

func TestInlineWarmupsAndSharedCasesDetachedPerView(t *testing.T) {
	zero := 0
	policy := &spec.CacheWarmupSettings{IndexColumn: "id", Limit: &zero, CaseRefs: []string{"recent"}}
	child := func(name string) *spec.View {
		return &spec.View{Name: name, Source: &spec.ViewSource{Bindings: &spec.ViewBindings{CacheName: "shared", InlineWarmups: []*spec.CacheWarmupSettings{policy}}}}
	}
	component := &spec.Component{Settings: &spec.Settings{Cache: &spec.CacheSettings{SharedCases: map[string][]*spec.CacheWarmupCase{"recent": {{Set: []*spec.CacheWarmupParam{{Name: "Period", Values: []string{"today"}, ExcludeDefault: true}}}}}}}, RootView: &spec.View{Name: "root", Relations: []*spec.Relation{{Name: "a", View: child("a")}, {Name: "b", View: child("b")}}}}
	view := FromComponent(component)
	require.Nil(t, view.Cache, "case definitions alone must not create root storage or policies")
	a, b := view.Relations[0].Of.View, view.Relations[1].Of.View
	a.Cache.Warmups[0].CaseRefs[0] = "changed"
	*a.Cache.Warmups[0].Limit = 9
	a.Cache.SharedCases["recent"][0].Set[0].Values[0] = "changed"
	require.Equal(t, []string{"recent"}, b.Cache.Warmups[0].CaseRefs)
	require.Zero(t, *b.Cache.Warmups[0].Limit)
	require.Equal(t, "today", b.Cache.SharedCases["recent"][0].Set[0].Values[0])
	require.Equal(t, []string{"recent"}, policy.CaseRefs)
	require.Zero(t, *policy.Limit)
	require.Equal(t, "today", component.Settings.Cache.SharedCases["recent"][0].Set[0].Values[0])
}
