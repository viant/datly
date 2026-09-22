package transcribe

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestCompileQuerySelectorUsesCanonicalViewName(t *testing.T) {
	for _, alias := range []string{"metaOrder", "chosenRows", "meta_order"} {
		t.Run(alias, func(t *testing.T) {
			result, err := NewCompiler().Compile(context.Background(), &Source{
				Name: "meta_order",
				Text: fmt.Sprintf(`#setting($_ = $route('/records', 'GET'))
#set($_ = $Fields<[]string>(query/fields).QuerySelector(%s).Optional())
SELECT %s.id FROM records %s`, alias, alias, alias),
			})
			require.NoError(t, err)
			require.Equal(t, alias, result.Component.RootView.Namespace)
			require.Equal(t, "meta_order", result.Component.Parameters[0].QuerySelector.View)
		})
	}
}

func TestCompileQuerySelectorRejectsUnprovenAlias(t *testing.T) {
	_, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "meta_order",
		Text: `#setting($_ = $route('/records', 'GET'))
#set($_ = $Fields<[]string>(query/fields).QuerySelector(metaOrder).Optional())
SELECT r.id FROM records r`,
	})
	require.ErrorContains(t, err, `query selector Fields: unknown view "metaOrder"`)
}

func TestResolveQuerySelectorViewGraph(t *testing.T) {
	for _, tc := range []struct {
		name, selector, childName, childAlias, want, errorPart string
	}{
		{name: "root alias", selector: "rootAlias", childName: "Details", childAlias: "d", want: "Records"},
		{name: "root default", childName: "Details", childAlias: "d", want: "Records"},
		{name: "child alias", selector: "d", childName: "Details", childAlias: "d", want: "Details"},
		{name: "canonical child", selector: "Details", childName: "Details", childAlias: "d", want: "Details"},
		{name: "duplicate namespace", selector: "rootAlias", childName: "Details", childAlias: "rootAlias", errorPart: "is ambiguous"},
		{name: "namespace collides with name", selector: "Records", childName: "Details", childAlias: "Records", errorPart: "is ambiguous"},
		{name: "unique alias ambiguous runtime name", selector: "d", childName: "Records", childAlias: "d", errorPart: "canonical view name"},
		{name: "unreferenced ambiguity", selector: "Records", childName: "Details", childAlias: "rootAlias", want: "Records"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selector := &spec.QuerySelectorBinding{View: tc.selector, Property: spec.SelectorPropertyFields}
			child := &spec.View{Name: tc.childName, Namespace: tc.childAlias}
			root := &spec.View{Name: "Records", Namespace: "rootAlias", Relations: []*spec.Relation{{View: child}, {View: child}}}
			component := &spec.Component{Name: "ReadRecords", RootView: root, Parameters: []*spec.Parameter{{Name: "Fields", QuerySelector: selector}}}
			err := resolveQuerySelectorViews(component)
			if tc.errorPart != "" {
				require.ErrorContains(t, err, tc.errorPart)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, component.Parameters[0].QuerySelector.View)
			require.Equal(t, spec.SelectorPropertyFields, component.Parameters[0].QuerySelector.Property)
			require.Equal(t, tc.selector, selector.View, "shared selector metadata must not be mutated")
		})
	}
}
