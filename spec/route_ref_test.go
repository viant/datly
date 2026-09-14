package spec

import "testing"

func TestParseRouteRef(t *testing.T) {
	actual, err := ParseRouteRef(" get:/v1/users ")
	if err != nil || actual.Method != "GET" || actual.Path != "/v1/users" || actual.String() != "GET:/v1/users" {
		t.Fatalf("ParseRouteRef() = (%+v, %v)", actual, err)
	}
	for _, invalid := range []string{"", "/v1/users", "GET:users"} {
		if _, err = ParseRouteRef(invalid); err == nil {
			t.Fatalf("ParseRouteRef(%q) succeeded", invalid)
		}
	}
}
