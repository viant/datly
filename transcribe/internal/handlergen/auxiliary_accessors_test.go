package handlergen

import (
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	plan "github.com/viant/datly/transcribe/handler/ast"
	handlergo "github.com/viant/datly/transcribe/handler/golang"
)

func TestAuxiliaryLifecycleAccessorsFollowFinalOwnedNamedRows(t *testing.T) {
	semantic := &plan.Plan{Root: &plan.RecordPlan{Identity: "flights", Auxiliary: true, Entity: &plan.EntityPlan{HooksComponent: true}}}
	contracts := &gen.Plan{Package: "example.com/rows", Imports: []spec.ImportSpec{{Alias: "external", Package: "example.com/external"}}, Views: []gen.ViewPlan{
		{Identity: "flights", Name: "Flight", Type: "Flight", Ownership: gen.ViewGenerated, SetMarkerFields: []string{"Id", "CallerCampaign", "Children", "Audience"}, Fields: []gen.Field{{Name: "Id", Type: "int"}, {Name: "CallerCampaign", Type: "*Campaign", Tag: `sqlx:"-"`}, {Name: "Children", Type: "[]*Campaign", Tag: `view:"campaigns"`}, {Name: "Audience", Type: "[]*external.Audience", Tag: `view:"audience"`}, {Name: "Unmarked", Type: "[]*Campaign", Tag: `view:"unmarked"`}}},
		{Identity: "campaigns", Name: "Campaign", Type: "Campaign", Ownership: gen.ViewGenerated, SetMarkerFields: []string{"Name"}, Fields: []gen.Field{{Name: "Name", Type: "*string"}, {Name: "Next", Type: "*Flight"}}},
		{Identity: "foreign", Name: "Audience", Type: "external.Audience", Package: "example.com/external", Ownership: gen.ViewLinked, SetMarkerFields: []string{"Name"}, Fields: []gen.Field{{Name: "Name", Type: "string"}}},
		{Identity: "unrelated", Name: "Other", Type: "Other", Ownership: gen.ViewGenerated, SetMarkerFields: []string{"Name"}, Fields: []gen.Field{{Name: "Name", Type: "string"}}},
	}}
	originalEntity := *semantic.Root.Entity
	originalMarkers := append([]string(nil), contracts.Views[0].SetMarkerFields...)
	config := handlergo.Config{Package: "rows", PackagePath: contracts.Package, Imports: []spec.ImportSpec{{Alias: "external", Package: "example.com/external"}}}
	g := &Generation{input: &gen.Input{}}
	asset, err := g.entitySetters(semantic, config, contracts)
	if err != nil {
		t.Fatal(err)
	}
	var methods []string
	for _, method := range asset.Methods {
		methods = append(methods, method.Receiver+"."+method.Name)
	}
	want := []string{"Flight.GetId", "Flight.SetId", "Flight.GetCallerCampaign", "Flight.SetCallerCampaign", "Flight.GetChildren", "Flight.SetChildren", "Flight.GetAudience", "Flight.SetAudience", "Campaign.GetName", "Campaign.SetName"}
	if !reflect.DeepEqual(methods, want) {
		t.Fatalf("owned dependency accessor closure: got %v want %v", methods, want)
	}
	for _, test := range []struct{ name, typ string }{{"GetChildren", "[]*Campaign"}, {"SetChildren", "[]*Campaign"}, {"GetAudience", "[]*external.Audience"}, {"SetAudience", "[]*external.Audience"}} {
		t.Run(test.name, func(t *testing.T) {
			found := false
			for _, method := range asset.Methods {
				if method.Receiver == "Flight" && method.Name == test.name {
					found = true
					if method.ValueType != test.typ {
						t.Fatalf("typed accessor: got %s want %s", method.ValueType, test.typ)
					}
				}
			}
			if !found {
				t.Fatal("missing marked relation accessor")
			}
		})
	}
	if err := g.applyEntitySetters(asset); err != nil {
		t.Fatal(err)
	}
	if g.input.EntitySupport == nil || g.input.EntitySupport.CaptureFunction != "" || g.input.EntitySupport.SnapshotType != "" || len(g.input.EntitySupport.Associations) != 0 {
		t.Fatal("auxiliary accessor integration acquired physical entity execution")
	}
	if !reflect.DeepEqual(*semantic.Root.Entity, originalEntity) || !semantic.Root.Auxiliary || !reflect.DeepEqual(contracts.Views[0].SetMarkerFields, originalMarkers) {
		t.Fatal("accessor generation changed auxiliary lifecycle execution metadata")
	}
	contracts.Views[0].SetMarkerFields = nil
	if asset, err = g.entitySetters(semantic, config, contracts); err != nil || asset != nil {
		t.Fatalf("marker-free auxiliary command acquired methods: %v, %v", asset, err)
	}
}
