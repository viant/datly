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

// One canonical component carries both independent current-read grouping and
// explicit borrowed body provenance. Cloning must detach both authorities.
func TestCurrent040AndBorrowed050CloneAuthoritiesRemainIndependent(t *testing.T) {
	source := &Component{
		Parameters: []*Parameter{{Name: "Current", ResolutionGroup: &ResolutionGroupSpec{Name: "archive", After: []string{"Jwt", "Data"}, DependsOn: []string{"AdvertiserId"}}}},
		Settings:   &Settings{Generation: &GenerationSettings{BorrowedSQLRows: []BorrowedSQLRow{{BodyPath: "Adorders/Audience/Creatives", Package: "example.com/api", Type: "AudienceCreative", OwnerURI: "private/Owner.dql", OwnerName: "Owner", OwnerBodyPath: "Rows"}}}},
	}
	clone := source.Clone()
	if clone.Parameters[0].ResolutionGroup.Name != "archive" || clone.Settings.Generation.BorrowedSQLRows[0].OwnerURI != "private/Owner.dql" {
		t.Fatal("clone lost one of the admitted authorities")
	}
	clone.Parameters[0].ResolutionGroup.After[0] = "mutated"
	clone.Parameters[0].ResolutionGroup.DependsOn[0] = "mutated"
	clone.Settings.Generation.BorrowedSQLRows[0].BodyPath = "Mutated"
	if source.Parameters[0].ResolutionGroup.After[0] != "Jwt" || source.Parameters[0].ResolutionGroup.DependsOn[0] != "AdvertiserId" || source.Settings.Generation.BorrowedSQLRows[0].BodyPath != "Adorders/Audience/Creatives" {
		t.Fatal("detached clone leaked current grouping or borrowed body provenance")
	}
}
