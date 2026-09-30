package transcribe

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/bootstrap"
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

func TestCompileExplicitBodyLimitAutoEnablesOnlyItsSelector(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "connector",
		Text: `#setting($_ = $route('/connector', 'POST'))
#define($_ = $Limit<int>(body/limit).WithTag('json:"limit"').Optional().QuerySelector('connector'))
#define($_ = $Rows<[]*Row>(output/view))
SELECT connector.id, set_limit(connector,25) FROM connectors connector`,
	})
	require.NoError(t, err)
	require.NotNil(t, result.Component.RootView.Selector)
	require.True(t, result.Component.RootView.Selector.AllowLimit)
	require.False(t, result.Component.RootView.Selector.AllowFields)
	require.False(t, result.Component.RootView.Selector.AllowOffset)
	require.NotNil(t, result.Component.RootView.Source.Controls.Limit)
	require.Equal(t, 25, *result.Component.RootView.Source.Controls.Limit)
}

func TestCompileTwoViewFieldSelectorsUseDistinctInputAliases(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "ParentRead",
		Text: `#setting($_ = $route('/parents', 'GET'))
#define($_ = $Fields<[]string>(query/parent_fields).Optional().QuerySelector('parents'))
#define($_ = $Fields<[]string>(query/child_fields).Optional().QuerySelector('children'))
#define($_ = $Rows<[]*Parent>(output/view))
SELECT parents.id,parents.name,children.id,children.parent_id,children.name,
       type(parents,'Parent'),type(children,'Child'),
       set_limit(parents,0),set_limit(children,0)
FROM parents parents
LEFT JOIN children children ON children.parent_id=parents.id`,
	})
	require.NoError(t, err)
	require.NotNil(t, result.Component)
	require.Len(t, result.Component.Parameters, 3)
}

type selectorProbeInput struct {
	Fields []string
}

type selectorProbeRow struct {
	ID int `sqlx:"id" json:"id"`
}

// An alias transcription cannot prove is deferred, because a linked Go output
// type may still contribute that view. The artifact build then resolves it
// against the completed graph and rejects it before the component can register.
func TestCompileQuerySelectorDefersUnprovenAliasToArtifactBuild(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Name: "meta_order",
		Text: `#setting($_ = $route('/records', 'GET'))
#set($_ = $Fields<[]string>(query/fields).QuerySelector(metaOrder).Optional())
SELECT r.id FROM records r`,
	})
	require.NoError(t, err)
	require.Equal(t, "metaOrder", result.Component.Parameters[0].QuerySelector.View)
	_, err = bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: result.Component, InputType: reflect.TypeFor[selectorProbeInput](), OutputType: reflect.TypeFor[[]*selectorProbeRow]()})
	require.ErrorContains(t, err, `unknown view "metaOrder"`)
}

type selectorLinkedProbeChild struct {
	ID       int    `sqlx:"id" json:"id"`
	ParentID int    `sqlx:"parent_id" json:"parentId"`
	Name     string `sqlx:"name" json:"name"`
}

type selectorLinkedProbeRow struct {
	ID       int                         `sqlx:"id" json:"id"`
	Name     string                      `sqlx:"name" json:"name"`
	Children []*selectorLinkedProbeChild `view:"children,table=children" on:"ID:id=ParentID:parent_id" json:"children,omitempty"`
}

type selectorLinkedProbeOutput struct {
	Rows []*selectorLinkedProbeRow `json:"rows"`
}

type selectorLinkedProbeInput struct {
	Fields      []string
	ChildFields []string
}

// Regression: a DQL reader whose nested view exists only in the linked Go
// output type must transcribe, and the artifact build must then bind the
// selector to that nested view while still canonicalizing the authored root.
func TestCompileQuerySelectorDeferredNestedViewResolvesAtArtifactBuild(t *testing.T) {
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/linked", Name: "Guard",
		Text: `#setting($_ = $route('/guard','GET'))
#define($_ = $Fields<[]string>(query/fields).Optional().QuerySelector('p'))
#define($_ = $Fields<[]string>(query/child_fields).Optional().QuerySelector('children'))
#define($_ = $Rows<?>(output/view))
SELECT p.id, p.name FROM parents p`,
	})
	require.NoError(t, err)
	var root, child *spec.QuerySelectorBinding
	for _, param := range spec.EffectiveParameters(result.Component.Parameters) {
		switch param.Source.Name {
		case "fields":
			root = param.QuerySelector
		case "child_fields":
			// Selector parameters are declared by canonical property name; the
			// artifact input contract requires unique field names.
			param.Name = "ChildFields"
			child = param.QuerySelector
		}
	}
	require.NotNil(t, root)
	require.NotNil(t, child)
	require.Equal(t, result.Component.RootView.CanonicalName(), root.View, "authored SQL alias is canonicalized at transcription")
	require.Equal(t, "children", child.View, "linked nested view is deferred, not rejected")
	artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: result.Component, InputType: reflect.TypeFor[selectorLinkedProbeInput](), OutputType: reflect.TypeFor[selectorLinkedProbeOutput](), DirectViewField: "Rows"})
	require.NoError(t, err)
	bound := map[string]bool{}
	for _, binding := range artifact.Reader.SelectorBindings {
		bound[binding.View.Spec.Name] = true
	}
	require.True(t, bound["children"], "selector bindings=%+v", artifact.Reader.SelectorBindings)
	require.Len(t, artifact.Reader.SelectorBindings, 2)
}

func TestCompileGoOnlySelectorWaitsForLinkedNestedView(t *testing.T) {
	selector := &spec.QuerySelectorBinding{View: "programs", Property: spec.SelectorPropertyFields}
	result, err := NewCompiler().Compile(context.Background(), &Source{
		Scope: "example.com/app/library", Name: "Read",
		PackageComponent: &spec.Component{
			Key:  spec.Key{Kind: spec.KindComponent, Scope: "example.com/app/library", Name: "Read"},
			Name: "Read", RootView: &spec.View{Name: "Read", Namespace: "scope"},
			Parameters: []*spec.Parameter{{Name: "Fields", QuerySelector: selector}},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "programs", result.Component.Parameters[0].QuerySelector.View)
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
