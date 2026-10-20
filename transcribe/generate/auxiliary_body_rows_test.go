package generate

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestAuxiliaryBodyRootCodecDestination(t *testing.T) {
	type useCase struct {
		description string
		input       string
		output      string
		expected    bool
		outputBody  bool
		noLifecycle bool
	}
	for _, tc := range []useCase{
		{description: "typed body", input: "[]*OrdersView", expected: true},
		{description: "whitespace fallback", input: "[]*OrdersView", output: "  ", expected: true},
		{description: "decoded body", input: "json.RawMessage", output: "[]*OrdersView", expected: true},
		{description: "decoded output body", input: "json.RawMessage", output: "[]*OrdersView", expected: true, outputBody: true},
		{description: "decoded other type", input: "json.RawMessage", output: "[]*Other"},
		{description: "raw body without destination", input: "json.RawMessage"},
		{description: "output overrides source", input: "[]*OrdersView", output: "[]*Other"},
		{description: "malformed destination", input: "[]*OrdersView", output: "["},
		{description: "codec cannot bypass lifecycle", input: "json.RawMessage", output: "[]*OrdersView", noLifecycle: true},
	} {
		t.Run(tc.description, func(t *testing.T) {
			component, root, child := setMarkerComponent()
			for _, view := range []*spec.View{root, child} {
				view.Auxiliary = true
				view.Source = &spec.ViewSource{Table: "records"}
			}
			root.EntityHooks = "BatchLifecycle"
			param := component.Parameters[0]
			param.TypeExpr, param.OutputTypeExpr = tc.input, tc.output
			param.Codec = &spec.Codec{Body: "DecodeRows"}
			if tc.outputBody {
				param.Source = spec.BindSource{Kind: "output", Name: "body"}
			}
			if tc.noLifecycle {
				root.EntityHooks = ""
			}
			plan := &Plan{Package: "example.com/rows", RootViewType: "OrdersView"}
			r := &planResolver{
				input: Input{Component: component, nativeMutationTargetSelected: true},
				plan:  plan, requireConcreteHelpers: true,
			}
			if got := r.auxiliaryBodyRoot(); (got == root) != tc.expected {
				t.Fatalf("body owner = %v, expected match %v", got, tc.expected)
			}
			if param.TypeExpr != tc.input || param.OutputTypeExpr != tc.output || param.Codec.Body != "DecodeRows" {
				t.Fatal("body ownership changed source/codec metadata")
			}
		})
	}
}

func TestAuxiliaryCodecBodyCurrentSharing(t *testing.T) {
	p, owner, current := ordinaryBodyPlanner()
	owner.Auxiliary = true
	owner.EntityHooks = "BatchLifecycle"
	p.plan.RootViewType = "Record"
	param := &spec.Parameter{
		Name: "Data", Source: spec.BindSource{Kind: "body", Name: "data"},
		TypeExpr: "json.RawMessage", OutputTypeExpr: "[]*Record",
		Codec: &spec.Codec{Body: "DecodeRows", Args: []string{"strict"}},
		Tag:   `wireType:"example.com/rows.RecordSlice"`,
	}
	component := &spec.Component{RootView: owner, Parameters: []*spec.Parameter{param}}
	r := &planResolver{input: Input{Component: component, nativeMutationTargetSelected: true}, plan: p.plan, requireConcreteHelpers: true}
	p.auxiliaryGraph = r.auxiliaryBodyGraph(r.auxiliaryBodyRoot())
	p.owners = map[string]*spec.View{"Record": owner}
	p.explicitReads = map[*spec.View]bool{current: true}
	p.ordinaryReads = map[*spec.View]bool{current: true}
	p.sharedBodyRows = map[*spec.View]*spec.View{}
	if err := p.assign(current, "Record", "record.go"); err != nil || p.sharedBodyRows[current] != owner {
		t.Fatalf("decoded body Current sharing: %v", err)
	}
	field, ok, err := resolveField(param, nil)
	if err != nil || !ok || field.Type != "[]*Record" {
		t.Fatalf("decoded field = %+v, ok=%v, err=%v", field, ok, err)
	}
	if param.TypeExpr != "json.RawMessage" || param.OutputTypeExpr != "[]*Record" ||
		param.Codec.Body != "DecodeRows" || len(param.Codec.Args) != 1 || param.Codec.Args[0] != "strict" ||
		!strings.Contains(field.Tag, `wireType:"example.com/rows.RecordSlice"`) {
		t.Fatal("codec body metadata was not retained")
	}
	component.Parameters = append(component.Parameters, &spec.Parameter{
		Name: "Other", Source: spec.BindSource{Kind: "body"}, TypeExpr: "[]*Other",
	})
	if r.auxiliaryBodyRoot() != nil {
		t.Fatal("mismatching additional body admitted")
	}
}

func TestAuxiliaryBodyGraphConcreteAdmission(t *testing.T) {
	for _, mode := range []string{"all auxiliary", "pending discovery", "pending final", "physical child", "derived", "self", "cycle"} {
		t.Run(mode, func(t *testing.T) {
			_, root, child := setMarkerComponent()
			for _, view := range []*spec.View{root, child} {
				view.Auxiliary = true
				view.Source = &spec.ViewSource{Table: "records"}
			}
			r := &planResolver{requireConcreteHelpers: true}
			switch mode {
			case "pending discovery", "pending final":
				child.Auxiliary = false
				child.Source = &spec.ViewSource{URI: "sql/child.sql"}
				r.requireConcreteHelpers = mode == "pending final"
			case "physical child":
				child.Auxiliary = false
			case "derived":
				root.Relations[0].Kind = spec.RelationKindDerived
			case "self":
				child.SelfReference = &spec.SelfReference{}
			case "cycle":
				child.Relations = []*spec.Relation{{Holder: "Parents", View: root}}
			}
			graph := r.auxiliaryBodyGraph(root)
			want := mode == "all auxiliary" || mode == "pending discovery"
			if (len(graph) == 2) != want {
				t.Fatalf("graph=%v mode=%s", graph, mode)
			}
		})
	}
}

func TestAuxiliaryBodyGraphSharingAndCollisions(t *testing.T) {
	for _, mode := range []string{"read", "competing body", "canonical type", "cast", "destination", "mapping", "read relation"} {
		t.Run(mode, func(t *testing.T) {
			p, owner, current := ordinaryBodyPlanner()
			owner.Auxiliary = true
			p.auxiliaryGraph = map[*spec.View]bool{owner: true}
			p.owners = map[string]*spec.View{"Record": owner}
			p.explicitReads = map[*spec.View]bool{current: true}
			p.ordinaryReads = map[*spec.View]bool{current: true}
			p.sharedBodyRows = map[*spec.View]*spec.View{}
			dest := "record.go"
			switch mode {
			case "competing body":
				p.auxiliaryGraph[current] = true
			case "canonical type":
				p.readTypes[current] = []string{"Other"}
			case "cast":
				current.Columns[0].Type.Name = "string"
			case "destination":
				dest = "other.go"
			case "mapping":
				owner.Columns = append(owner.Columns, &spec.Column{Name: "OtherId", Source: "id", Type: spec.TypeRef{Name: "int"}})
			case "read relation":
				current.Relations = []*spec.Relation{{Holder: "Children", View: &spec.View{Name: "Children"}}}
			}
			err := p.assign(current, "Record", dest)
			if mode == "read" {
				if err != nil || p.sharedBodyRows[current] != owner {
					t.Fatalf("share: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("invalid graph sharing admitted")
			}
			if mode == "competing body" && !strings.Contains(err.Error(), "compete") {
				t.Fatal(err)
			}
		})
	}
}

func TestAuxiliaryBodyLinkedHolderPresence(t *testing.T) {
	component, root, child := setMarkerComponent()
	for _, view := range []*spec.View{root, child} {
		view.Auxiliary = true
		view.Source = &spec.ViewSource{Table: "records"}
	}
	root.EntityHooks = "BatchLifecycle"
	rootID, _ := root.Identity()
	childID, _ := child.Identity()
	plan := &Plan{Package: "example.com/rows", RootViewType: "OrdersView", Views: []ViewPlan{{Identity: rootID, Name: "OrdersView", Ownership: ViewGenerated}, {Identity: childID, Name: "ItemsView", Ownership: ViewLinked}}}
	input := Input{Component: component, nativeMutationTargetSelected: true}
	r := &planResolver{input: input, plan: plan, requireConcreteHelpers: true}
	edges := map[borrowedPresenceEdge]bool{}
	planned := map[string]*ViewPlan{rootID: &plan.Views[0], childID: &plan.Views[1]}
	if err := r.addNativeBodyPresenceEdges(planned, map[string]bool{childID: true}, edges); err != nil {
		t.Fatal(err)
	}
	if !edges[borrowedPresenceEdge{rootID, "Items", childID}] {
		t.Fatalf("linked holder presence lost: %v", edges)
	}
	if len(plan.Views[1].Fields) != 0 || len(plan.Views[1].SetMarkerFields) != 0 {
		t.Fatal("linked package marker mutated")
	}
}
