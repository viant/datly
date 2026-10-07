package dql

import "testing"

func TestResolutionGroupAuthoringPreservesMetadata(t *testing.T) {
	prepared := PrepareSource("#set($_ = $AdvertiserId<int>(path/advertiserId).WithResolutionGroup('archive_current','Jwt','Data'))\n#set($_ = $Current<int>(component/'GET:/current').WithResolutionGroup('archive_current','Jwt','Data').DependsOn('AdvertiserId'))\nSELECT 1")
	if err := prepared.Err(); err != nil {
		t.Fatal(err)
	}
	if len(prepared.Directives.Params) != 2 {
		t.Fatal("parameters missing")
	}
	root := prepared.Directives.Params[0].ResolutionGroup
	child := prepared.Directives.Params[1].ResolutionGroup
	if root == nil || child == nil || root.Name != "archive_current" || root.After[0] != "Jwt" || root.After[1] != "Data" || child.DependsOn[0] != "AdvertiserId" {
		t.Fatal("DQL resolution metadata lost")
	}
}
func TestResolutionGroupMalformedAuthoringRejected(t *testing.T) {
	for _, tail := range []string{".WithResolutionGroup('archive')", ".WithResolutionGroup('archive','Jwt','Data').WithResolutionGroup('archive','Jwt','Data')", ".DependsOn()", ".DependsOn('ID').DependsOn('ID')"} {
		if _, err := (&declarationOptionParser{paramName: "ID", tail: tail}).parse(); err == nil {
			t.Fatalf("invalid option accepted: %s", tail)
		}
	}
}
