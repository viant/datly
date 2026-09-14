package spec

import "testing"

func TestCacheWarmupSettingsClone_DeepCopiesCases(t *testing.T) {
	limit, maxCases := 10, 5
	source := &CacheWarmupSettings{
		Limit: &limit, MaxCases: &maxCases, FieldNames: []string{"ID"},
		IndexColumn:    "vendor_id",
		IndexParameter: "VendorID",
		Connector:      "warmup_db",
		Cases: []*CacheWarmupCase{
			{
				FieldNames: []string{"Name"},
				Set: []*CacheWarmupParam{
					{Name: "VendorID", Values: []string{"1", "2"}},
				},
			},
		},
	}

	cloned := source.Clone()
	if cloned == nil {
		t.Fatalf("expected clone")
	}
	cloned.IndexColumn = "category_id"
	cloned.Cases[0].Set[0].Name = "CategoryID"
	cloned.Cases[0].Set[0].Values[0] = "9"
	*cloned.Limit = 1
	*cloned.MaxCases = 1
	cloned.FieldNames[0] = "Changed"
	cloned.Cases[0].FieldNames[0] = "Changed"
	if *source.Limit != 10 || *source.MaxCases != 5 || source.FieldNames[0] != "ID" || source.Cases[0].FieldNames[0] != "Name" {
		t.Fatal("warmup controls were shared by clone")
	}

	if source.IndexColumn != "vendor_id" {
		t.Fatalf("source index column mutated: %s", source.IndexColumn)
	}
	if source.Cases[0].Set[0].Name != "VendorID" {
		t.Fatalf("source param name mutated: %s", source.Cases[0].Set[0].Name)
	}
	if source.Cases[0].Set[0].Values[0] != "1" {
		t.Fatalf("source param values mutated: %#v", source.Cases[0].Set[0].Values)
	}
}
