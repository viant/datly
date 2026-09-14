package spec

import "testing"

func TestCORSCredentialWildcardRetainsOriginalParentRestriction(t *testing.T) {
	wildcard, allowed := []string{"*"}, []string{"https://allowed.example"}
	yes := true
	child := &CORS{AllowOrigins: &wildcard, AllowCredentials: &yes}
	parent := &CORS{AllowOrigins: &allowed}
	resolved := child.Resolve(parent)
	if resolved.AllowOrigins == nil || len(*resolved.AllowOrigins) != 1 || (*resolved.AllowOrigins)[0] != allowed[0] {
		t.Fatalf("credential wildcard broadened original parent policy: %+v", resolved.AllowOrigins)
	}
	(*resolved.AllowOrigins)[0] = "changed"
	if wildcard[0] != "*" || allowed[0] != "https://allowed.example" {
		t.Fatal("source policy mutated")
	}
	no := false
	child.AllowCredentials = &no
	if result := child.Resolve(parent); result.AllowOrigins == nil || (*result.AllowOrigins)[0] != "*" {
		t.Fatal("noncredentialed explicit wildcard should retain original behavior")
	}
}
