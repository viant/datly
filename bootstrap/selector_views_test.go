package bootstrap

import (
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

type selectorLinkedChild struct {
	ID       int    `sqlx:"id" json:"id"`
	ParentID int    `sqlx:"parent_id" json:"parentId"`
	Name     string `sqlx:"name" json:"name"`
}

type selectorLinkedRow struct {
	ID       int                    `sqlx:"id" json:"id"`
	Name     string                 `sqlx:"name" json:"name"`
	Children []*selectorLinkedChild `view:"children,table=children" on:"ID:id=ParentID:parent_id" json:"children,omitempty"`
}

type selectorLinkedOutput struct {
	Rows []*selectorLinkedRow `json:"rows"`
}

type selectorLinkedInput struct {
	Fields []string
}

// selectorComponent mirrors what transcription yields for a DQL reader whose
// nested view exists only in the linked Go output type.
func selectorComponent(target string) *spec.Component {
	optional := false
	return &spec.Component{
		Key:    spec.Key{Kind: spec.KindComponent, Scope: "example.com/selector", Name: "Guard"},
		Name:   "Guard",
		Routes: []*spec.Route{{Method: "GET", Path: "/guard"}},
		Parameters: []*spec.Parameter{
			{Name: "Fields", TypeExpr: "[]string", Required: &optional, Source: spec.BindSource{Kind: "query", Name: "fields"}, QuerySelector: &spec.QuerySelectorBinding{View: target, Property: spec.SelectorPropertyFields}},
			{Name: "Rows", Source: spec.BindSource{Kind: "output", Name: "view"}},
		},
		RootView: &spec.View{Name: "parents", Namespace: "p", Source: &spec.ViewSource{SQL: "SELECT id,name FROM parents"}},
	}
}

func TestResolveQuerySelectorViews(t *testing.T) {
	for _, tc := range []struct {
		name, selector, childName, childAlias, want, errorPart string
		strict                                                 bool
		deferred                                               bool
	}{
		{name: "root alias", selector: "rootAlias", childName: "Details", childAlias: "d", want: "Records"},
		{name: "root default", childName: "Details", childAlias: "d", want: "Records"},
		{name: "root default strict", strict: true, childName: "Details", childAlias: "d", want: "Records"},
		{name: "child alias", selector: "d", childName: "Details", childAlias: "d", want: "Details"},
		{name: "canonical child", selector: "Details", childName: "Details", childAlias: "d", want: "Details"},
		{name: "component name", selector: "ReadRecords", childName: "Details", childAlias: "d", want: "Records"},
		{name: "case insensitive alias", selector: "ROOTALIAS", childName: "Details", childAlias: "d", want: "Records"},
		{name: "duplicate namespace", selector: "rootAlias", childName: "Details", childAlias: "rootAlias", errorPart: "is ambiguous"},
		{name: "duplicate namespace strict", strict: true, selector: "rootAlias", childName: "Details", childAlias: "rootAlias", errorPart: "is ambiguous"},
		{name: "namespace collides with name", selector: "Records", childName: "Details", childAlias: "Records", errorPart: "is ambiguous"},
		{name: "unique alias ambiguous runtime name", selector: "d", childName: "Records", childAlias: "d", errorPart: "canonical view name"},
		{name: "unreferenced ambiguity", selector: "Records", childName: "Details", childAlias: "rootAlias", want: "Records"},
		{name: "unknown deferred", selector: "linkedLater", childName: "Details", childAlias: "d", want: "linkedLater", deferred: true},
		{name: "unknown strict", strict: true, selector: "linkedLater", childName: "Details", childAlias: "d", errorPart: `unknown view "linkedLater"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selector := &spec.QuerySelectorBinding{View: tc.selector, Property: spec.SelectorPropertyFields}
			child := &spec.View{Name: tc.childName, Namespace: tc.childAlias}
			root := &spec.View{Name: "Records", Namespace: "rootAlias", Relations: []*spec.Relation{{View: child}, {View: child}}}
			component := &spec.Component{Name: "ReadRecords", RootView: root, Parameters: []*spec.Parameter{{Name: "Fields", QuerySelector: selector}}}
			err := ResolveQuerySelectorViews(component, tc.strict)
			if tc.errorPart != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errorPart) {
					t.Fatalf("want error containing %q, got %v", tc.errorPart, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			resolved := component.Parameters[0].QuerySelector
			if resolved.View != tc.want || resolved.Property != spec.SelectorPropertyFields {
				t.Fatalf("resolved=%+v want view %q", resolved, tc.want)
			}
			if tc.deferred != (resolved == selector) {
				t.Fatalf("deferred=%v but selector identity preserved=%v", tc.deferred, resolved == selector)
			}
			if selector.View != tc.selector {
				t.Fatal("shared selector metadata must not be mutated")
			}
		})
	}
	if err := ResolveQuerySelectorViews(nil, true); err != nil {
		t.Fatal(err)
	}
	// Without any view graph a strict resolution cannot bind even the root default.
	if err := ResolveQuerySelectorViews(&spec.Component{Parameters: []*spec.Parameter{{Name: "Fields", QuerySelector: &spec.QuerySelectorBinding{}}}}, true); err == nil {
		t.Fatal("strict resolution bound a selector without a view graph")
	}
}

func TestResolveQuerySelectorViewsAutoEnablesDeclaredProperties(t *testing.T) {
	root := &spec.View{Name: "connector", Relations: []*spec.Relation{{View: &spec.View{Name: "child"}}}}
	properties := []spec.SelectorProperty{spec.SelectorPropertyFields, spec.SelectorPropertyOrderBy,
		spec.SelectorPropertyCriteria, spec.SelectorPropertyLimit, spec.SelectorPropertyOffset, spec.SelectorPropertyPage}
	component := &spec.Component{Name: "connector", RootView: root}
	for _, property := range properties {
		component.Parameters = append(component.Parameters, &spec.Parameter{Name: string(property), QuerySelector: &spec.QuerySelectorBinding{View: "connector", Property: property}})
	}
	if err := ResolveQuerySelectorViews(component, true); err != nil {
		t.Fatal(err)
	}
	selector := root.Selector
	if selector == nil || !selector.AllowFields || !selector.AllowOrderBy || !selector.AllowCriteria || !selector.AllowLimit || !selector.AllowOffset || !selector.AllowPage {
		t.Fatalf("explicit selector grants = %+v", selector)
	}
	if len(selector.Filterable) != 1 || selector.Filterable[0] != "*" {
		t.Fatalf("explicit criteria did not get its compiled-column scope: %+v", selector)
	}
	if root.Relations[0].View.Selector != nil {
		t.Fatalf("unrelated child gained selector permissions: %+v", root.Relations[0].View.Selector)
	}
}

// TestBuildArtifactResolvesSelectorAgainstLinkedOutputView proves the deferral
// contract end to end at the artifact seam: a target that exists only in the
// linked Go output relation graph resolves during the artifact build, while
// unknown and ambiguous targets are still rejected there, before registration.
func TestBuildArtifactResolvesSelectorAgainstLinkedOutputView(t *testing.T) {
	inputType := reflect.TypeFor[selectorLinkedInput]()
	outputType := reflect.TypeFor[selectorLinkedOutput]()
	build := func(component *spec.Component) (*Artifact, error) {
		return BuildArtifact(ArtifactInput{Component: component, InputType: inputType, OutputType: outputType, DirectViewField: "Rows"})
	}

	t.Run("linked nested view", func(t *testing.T) {
		component := selectorComponent("children")
		if err := ResolveQuerySelectorViews(component, false); err != nil {
			t.Fatal(err)
		}
		if component.Parameters[0].QuerySelector.View != "children" {
			t.Fatalf("transcription must defer the linked target, got %q", component.Parameters[0].QuerySelector.View)
		}
		artifact, err := build(component)
		if err != nil {
			t.Fatal(err)
		}
		if len(artifact.Reader.SelectorBindings) != 1 || artifact.Reader.SelectorBindings[0].View == nil || artifact.Reader.SelectorBindings[0].View.Spec.Name != "children" {
			t.Fatalf("selector bindings=%+v", artifact.Reader.SelectorBindings)
		}
	})
	t.Run("authored SQL alias canonicalized before build", func(t *testing.T) {
		component := selectorComponent("p")
		if err := ResolveQuerySelectorViews(component, false); err != nil {
			t.Fatal(err)
		}
		if component.Parameters[0].QuerySelector.View != "parents" {
			t.Fatalf("alias not canonicalized: %q", component.Parameters[0].QuerySelector.View)
		}
		artifact, err := build(component)
		if err != nil {
			t.Fatal(err)
		}
		if len(artifact.Reader.SelectorBindings) != 1 || artifact.Reader.SelectorBindings[0].View.Spec.Name != "parents" {
			t.Fatalf("selector bindings=%+v", artifact.Reader.SelectorBindings)
		}
	})
	t.Run("unknown target rejected at build", func(t *testing.T) {
		component := selectorComponent("missing")
		if err := ResolveQuerySelectorViews(component, false); err != nil {
			t.Fatal(err)
		}
		if _, err := build(component); err == nil || !strings.Contains(err.Error(), `unknown view "missing"`) {
			t.Fatalf("unknown selector target reached registration: %v", err)
		}
	})
	t.Run("ambiguous target rejected at build", func(t *testing.T) {
		component := selectorComponent("children")
		// The root view now shares the linked child's name.
		component.RootView.Name = "children"
		if err := ResolveQuerySelectorViews(component, false); err != nil {
			t.Fatal(err)
		}
		if _, err := build(component); err == nil || !strings.Contains(err.Error(), "ambiguous") {
			t.Fatalf("ambiguous selector target reached registration: %v", err)
		}
	})
	t.Run("handler owned output rejects unknown target", func(t *testing.T) {
		component := selectorComponent("missing")
		if err := ResolveQuerySelectorViews(component, false); err != nil {
			t.Fatal(err)
		}
		_, err := BuildArtifact(ArtifactInput{Component: component, InputType: inputType, OutputType: outputType, HandlerOwnedOutput: true})
		if err == nil || !strings.Contains(err.Error(), `unknown view "missing"`) {
			t.Fatalf("handler-owned output accepted an unknown selector target: %v", err)
		}
	})
	t.Run("handler owned output rejects linked-only target", func(t *testing.T) {
		// A handler-owned output links no reader views, so a target that would
		// only exist in the output relation graph cannot be served.
		component := selectorComponent("children")
		_, err := BuildArtifact(ArtifactInput{Component: component, InputType: inputType, OutputType: outputType, HandlerOwnedOutput: true})
		if err == nil || !strings.Contains(err.Error(), `unknown view "children"`) {
			t.Fatalf("handler-owned output accepted an unserved selector target: %v", err)
		}
	})
	t.Run("handler owned output accepts authored view", func(t *testing.T) {
		component := selectorComponent("p")
		artifact, err := BuildArtifact(ArtifactInput{Component: component, InputType: inputType, OutputType: outputType, HandlerOwnedOutput: true})
		if err != nil {
			t.Fatal(err)
		}
		if artifact.Component.Parameters[0].QuerySelector.View != "parents" {
			t.Fatalf("strict resolution did not canonicalize: %q", artifact.Component.Parameters[0].QuerySelector.View)
		}
	})
}
