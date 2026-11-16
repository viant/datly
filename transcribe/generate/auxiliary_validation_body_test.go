package generate

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestAuxiliaryBodyRootWithPrivateValidationBody(t *testing.T) {
	for _, tc := range []struct {
		name           string
		tag            string
		publicBody     bool
		wantOwner      bool
		privateType    string
		privateOutput  bool
		outputFallback bool
		privateFirst   bool
	}{
		{name: "private validation alongside public rows", tag: `internal:"true" json:"-" mcp:"-"`, publicBody: true, wantOwner: true},
		{name: "private validation before public rows", tag: `internal:"true"`, publicBody: true, privateFirst: true, wantOwner: true},
		{name: "private body alone cannot own rows", tag: `internal:"true" json:"-" mcp:"-"`},
		{name: "matching private body alone cannot own rows", tag: `internal:"true"`, privateType: "[]*OrdersView"},
		{name: "private request prevents output fallback", tag: `internal:"true"`, outputFallback: true},
		{name: "public output fallback owns rows", privateOutput: true, privateType: "[]*OrdersView", wantOwner: true},
		{name: "private output fallback cannot own rows", tag: `internal:"true"`, privateOutput: true, privateType: "[]*OrdersView"},
		{name: "conflicting public body remains ambiguous", publicBody: true},
		{name: "JSON hiding alone does not declare private ownership", tag: `json:"-"`, publicBody: true},
		{name: "MCP hiding alone does not declare private ownership", tag: `mcp:"-"`, publicBody: true},
		{name: "explicit noninternal body remains ambiguous", tag: `internal:"false"`, publicBody: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			component, root, child := setMarkerComponent()
			for _, view := range []*spec.View{root, child} {
				view.Auxiliary = true
				view.Source = &spec.ViewSource{Table: "records"}
			}
			root.EntityHooks = "BatchLifecycle"
			component.Parameters = nil
			if tc.publicBody {
				component.Parameters = append(component.Parameters, &spec.Parameter{
					Name: "Rows", Source: spec.BindSource{Kind: "body", Name: "data"}, TypeExpr: "[]*OrdersView",
				})
			}
			validation := &spec.Parameter{
				Name: "Submitted", Source: spec.BindSource{Kind: "body"}, TypeExpr: "*SubmittedBody", Tag: tc.tag,
			}
			if tc.privateType != "" {
				validation.TypeExpr = tc.privateType
			}
			if tc.privateOutput {
				validation.Source = spec.BindSource{Kind: "output", Name: "body"}
			}
			originalType, originalSource := validation.TypeExpr, validation.Source
			component.Parameters = append(component.Parameters, validation)
			if tc.privateFirst {
				component.Parameters[0], component.Parameters[1] = component.Parameters[1], component.Parameters[0]
			}
			if tc.outputFallback {
				component.Parameters = append(component.Parameters, &spec.Parameter{
					Name: "Response", Source: spec.BindSource{Kind: "output", Name: "body"}, TypeExpr: "[]*OrdersView",
				})
			}
			r := &planResolver{
				input:                  Input{Component: component, nativeMutationTargetSelected: true},
				plan:                   &Plan{Package: "example.com/rows", RootViewType: "OrdersView"},
				requireConcreteHelpers: true,
			}
			if got := r.auxiliaryBodyRoot(); (got == root) != tc.wantOwner {
				t.Fatalf("public SQL row owner matched=%v, want %v", got == root, tc.wantOwner)
			}
			if validation.Tag != tc.tag || validation.TypeExpr != originalType || validation.Source.Kind != originalSource.Kind || validation.Source.Name != originalSource.Name {
				t.Fatal("owner selection changed the private parameter binding or visibility")
			}
		})
	}
}

func TestPrivateValidationBodyRetainsBindingWithoutSQLTags(t *testing.T) {
	component, root, _ := setMarkerComponent()
	component.Settings = &spec.Settings{Mutation: "patch"}
	root.Source = &spec.ViewSource{Table: "records", SQL: "SELECT ID FROM records"}
	component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Submitted", Source: spec.BindSource{Kind: "body"}, TypeExpr: "*SubmittedBody", Tag: `internal:"true" json:"-" mcp:"-"`})
	fields, err := resolveInputFields(component, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range fields {
		if field.Name != "Submitted" {
			continue
		}
		tags := reflect.StructTag(field.Tag)
		if field.Type != "*SubmittedBody" || tags.Get("internal") != "true" || tags.Get("parameter") == "" || field.Source != "body" {
			t.Fatalf("private body binding lost: %+v", field)
		}
		if tags.Get("sql") != "" || tags.Get("view") != "" {
			t.Fatalf("private validation body acquired SQL owner tags: %+v", field)
		}
		return
	}
	t.Fatal("private validation input missing")
}

func TestPrivateValidationBodyDoesNotCompeteForLinkedPresence(t *testing.T) {
	component, root, child := setMarkerComponent()
	for _, view := range []*spec.View{root, child} {
		view.Auxiliary = true
		view.Source = &spec.ViewSource{Table: "records"}
	}
	root.EntityHooks = "BatchLifecycle"
	component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Submitted", Source: spec.BindSource{Kind: "body"}, TypeExpr: "*SubmittedBody", Tag: `internal:"true"`})
	rootID, _ := root.Identity()
	childID, _ := child.Identity()
	plan := &Plan{RootViewType: "OrdersView", Views: []ViewPlan{{Identity: rootID, Name: "OrdersView", Ownership: ViewGenerated}, {Identity: childID, Name: "ItemsView", Ownership: ViewLinked}}}
	r := &planResolver{input: Input{Component: component, nativeMutationTargetSelected: true}, plan: plan, requireConcreteHelpers: true}
	edges := map[borrowedPresenceEdge]bool{}
	if err := r.addNativeBodyPresenceEdges(map[string]*ViewPlan{rootID: &plan.Views[0], childID: &plan.Views[1]}, map[string]bool{childID: true}, edges); err != nil {
		t.Fatal(err)
	}
	if !edges[borrowedPresenceEdge{rootID, "Items", childID}] {
		t.Fatalf("public linked presence lost: %v", edges)
	}
}

func TestAuxiliaryPrivateValidationBodyCurrentSharing(t *testing.T) {
	for _, privateFirst := range []bool{false, true} {
		p, owner, current := ordinaryBodyPlanner()
		owner.Auxiliary = true
		owner.EntityHooks = "BatchLifecycle"
		p.plan.RootViewType = "Record"
		public := &spec.Parameter{Name: "Data", Source: spec.BindSource{Kind: "body", Name: "data"}, TypeExpr: "[]*Record"}
		private := &spec.Parameter{Name: "Submitted", Source: spec.BindSource{Kind: "body"}, TypeExpr: "*SubmittedBody", Tag: `internal:"true" json:"-" mcp:"-"`}
		parameters := []*spec.Parameter{public, private}
		if privateFirst {
			parameters[0], parameters[1] = parameters[1], parameters[0]
		}
		component := &spec.Component{RootView: owner, Parameters: parameters}
		r := &planResolver{input: Input{Component: component, nativeMutationTargetSelected: true}, plan: p.plan, requireConcreteHelpers: true}
		p.auxiliaryGraph = r.auxiliaryBodyGraph(r.auxiliaryBodyRoot())
		p.owners = map[string]*spec.View{"Record": owner}
		p.explicitReads = map[*spec.View]bool{current: true}
		p.ordinaryReads = map[*spec.View]bool{current: true}
		p.sharedBodyRows = map[*spec.View]*spec.View{}
		if err := p.assign(current, "Record", "record.go"); err != nil || p.sharedBodyRows[current] != owner {
			t.Fatalf("privateFirst=%v: current-row sharing failed: %v", privateFirst, err)
		}
		field, ok, err := resolveField(private, nil)
		if err != nil || !ok || field.Type != "*SubmittedBody" {
			t.Fatalf("private binding was lost: field=%+v, ok=%v, err=%v", field, ok, err)
		}
	}
}
