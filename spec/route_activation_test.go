package spec

import "testing"

func TestRelativeRouteActivationMatchesAllExpandedPaths(t *testing.T) {
	activation := &RouteActivation{URI: "/{id}"}
	for _, path := range []string{"/things/{id}", "/other/{id}"} {
		if !activation.Matches(path) {
			t.Fatalf("did not match %s", path)
		}
	}
	if activation.Matches("/things") {
		t.Fatal("matched base")
	}
	if (&RouteActivation{URI: "/things/{id}"}).Matches("/other/{id}") {
		t.Fatal("absolute activation matched another route")
	}
}
