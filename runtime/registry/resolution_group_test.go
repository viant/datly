package registry

import (
	"github.com/viant/bindly"
	"testing"
)

func TestResolutionGroupContractBindingCloneIsolated(t *testing.T) {
	source := bindly.BindingSpec{ResolutionGroup: &bindly.ResolutionGroupSpec{Name: "archive", After: []string{"Jwt", "Data"}, DependsOn: []string{"AdvertiserId"}}}
	field := InputField{binding: cloneBindingSpec(source)}
	source.ResolutionGroup.After[0] = "caller mutation"
	returned := field.Binding()
	returned.ResolutionGroup.Name = "returned mutation"
	returned.ResolutionGroup.After[0] = "returned mutation"
	returned.ResolutionGroup.DependsOn[0] = "returned mutation"
	again := field.Binding().ResolutionGroup
	if again.Name != "archive" || again.After[0] != "Jwt" || again.DependsOn[0] != "AdvertiserId" {
		t.Fatal("compiled contract shares group metadata")
	}
}
