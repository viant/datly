package handlergen

import (
	"reflect"
	"testing"

	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
)

func TestAuxiliaryLifecycleAccessorsFollowFinalOwnedNamedRows(t *testing.T) {
	semantic := &plan.Plan{Root: &plan.RecordPlan{Identity: "flights", Auxiliary: true, Entity: &plan.EntityPlan{HooksComponent: true}}}
	contracts := &gen.Plan{Package: "example.com/rows", Views: []gen.ViewPlan{
		{Identity: "flights", Name: "Flight", Type: "Flight", Ownership: gen.ViewGenerated, SetMarkerFields: []string{"Id", "CallerCampaign"}, Fields: []gen.Field{{Name: "Id", Type: "int"}, {Name: "CallerCampaign", Type: "*Campaign", Tag: `sqlx:"-"`}}},
		{Identity: "campaigns", Name: "Campaign", Type: "Campaign", Ownership: gen.ViewGenerated, SetMarkerFields: []string{"Name"}, Fields: []gen.Field{{Name: "Name", Type: "*string"}, {Name: "Next", Type: "*Flight"}}},
		{Identity: "unrelated", Name: "Other", Type: "Other", Ownership: gen.ViewGenerated, SetMarkerFields: []string{"Name"}, Fields: []gen.Field{{Name: "Name", Type: "string"}}},
	}}
	config := handlergo.Config{Package: "rows", PackagePath: contracts.Package}
	g := &Generation{input: &gen.Input{}}
	asset, err := g.entitySetters(semantic, config, contracts)
	if err != nil {
		t.Fatal(err)
	}
	var methods []string
	for _, method := range asset.Methods {
		methods = append(methods, method.Receiver+"."+method.Name)
	}
	want := []string{"Flight.GetId", "Flight.SetId", "Flight.GetCallerCampaign", "Flight.SetCallerCampaign", "Campaign.GetName", "Campaign.SetName"}
	if !reflect.DeepEqual(methods, want) {
		t.Fatalf("owned dependency accessor closure: got %v want %v", methods, want)
	}
	if err := g.applyEntitySetters(asset); err != nil {
		t.Fatal(err)
	}
	if g.input.EntitySupport == nil || g.input.EntitySupport.CaptureFunction != "" || g.input.EntitySupport.SnapshotType != "" || len(g.input.EntitySupport.Associations) != 0 {
		t.Fatal("auxiliary accessor integration acquired physical entity execution")
	}
	if semantic.Root.Entity.Owned || semantic.Root.Entity.MarkerField != "" {
		t.Fatal("accessor generation changed auxiliary lifecycle execution metadata")
	}
	contracts.Views[0].SetMarkerFields = nil
	if asset, err = g.entitySetters(semantic, config, contracts); err != nil || asset != nil {
		t.Fatalf("marker-free auxiliary command acquired methods: %v, %v", asset, err)
	}
}
