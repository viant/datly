package tag

import (
	"reflect"
	"testing"
)

func TestResolutionGroupTagIndexDetached(t *testing.T) {
	type input struct {
		Current int `bind:"Current,kind=component,in=GET:/current,resolutionGroup=archive,resolutionAfter=Jwt|Data,dependsOn=AdvertiserId"`
	}
	index, err := NewBindingIndex(reflect.TypeFor[input]())
	if err != nil {
		t.Fatal(err)
	}
	group := index.Fields()[0].Binding.ResolutionGroup
	if group == nil || group.Name != "archive" || group.After[1] != "Data" || group.DependsOn[0] != "AdvertiserId" {
		t.Fatal("native tag group metadata missing")
	}
	group.Name = "changed"
	group.After[0] = "changed"
	group.DependsOn[0] = "changed"
	again := index.Fields()[0].Binding.ResolutionGroup
	if again.Name != "archive" || again.After[0] != "Jwt" || again.DependsOn[0] != "AdvertiserId" {
		t.Fatal("tag index group metadata mutable")
	}
}
