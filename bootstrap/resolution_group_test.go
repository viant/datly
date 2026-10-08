package bootstrap

import (
	"github.com/viant/datly/spec"
	"reflect"
	"testing"
)

func TestResolutionGroupHolderCarrierPreservesReloadAndIsolation(t *testing.T) {
	type input struct {
		Current int `bind:"Current,kind=component,in=GET:/current,resolutionGroup=archive,resolutionAfter=Jwt|Data,dependsOn=AdvertiserId"`
	}
	source := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/holder", Name: "Archive"}}
	resolve := func() *spec.Component {
		actual, err := (ContractResolver{Component: source, InputType: linkedContractType(reflect.TypeFor[input]())}).Resolve()
		if err != nil {
			t.Fatal(err)
		}
		return actual
	}
	first := resolve()
	group := first.Parameters[0].ResolutionGroup
	if group == nil || group.Name != "archive" || group.After[1] != "Data" || group.DependsOn[0] != "AdvertiserId" {
		t.Fatal("generated holder carrier lost group metadata")
	}
	group.Name = "changed"
	group.After[0] = "changed"
	group.DependsOn[0] = "changed"
	again := resolve().Parameters[0].ResolutionGroup
	if again == nil || again.Name != "archive" || again.After[0] != "Jwt" || again.DependsOn[0] != "AdvertiserId" || len(source.Parameters) != 0 {
		t.Fatal("reload/source group metadata shared")
	}
}
