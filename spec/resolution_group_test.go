package spec

import "testing"

func TestResolutionGroupComponentCloneIsolated(t *testing.T) {
	source := &Component{Parameters: []*Parameter{{Name: "Current", ResolutionGroup: &ResolutionGroupSpec{Name: "archive", After: []string{"Jwt", "Data"}, DependsOn: []string{"AdvertiserId"}}}}}
	clone := source.Clone()
	g := clone.Parameters[0].ResolutionGroup
	g.Name = "mutated"
	g.After[0] = "mutated"
	g.DependsOn[0] = "mutated"
	original := source.Parameters[0].ResolutionGroup
	if original.Name != "archive" || original.After[0] != "Jwt" || original.DependsOn[0] != "AdvertiserId" {
		t.Fatal("clone leaked group authoring metadata")
	}
}
