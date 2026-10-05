package dql

import (
	"strings"
	"testing"
)

func TestRequestBodyModeRouteDirective(t *testing.T) {
	for _, mode := range []string{"", "eager", "on_demand"} {
		source := "#setting($_ = $route('/views/{id}','DELETE'))\n"
		if mode != "" {
			source += "#setting($_ = $request_body_mode('" + mode + "'))\n"
		}
		source += "SELECT ID FROM VIEWS"
		c, err := parseComponentSource("test", "delete", source)
		if err != nil {
			t.Fatal(err)
		}
		if c.Routes[0].RequestBodyMode != mode {
			t.Fatalf("lost route mode: %s", c.Routes[0].RequestBodyMode)
		}
	}
	for _, setting := range []string{"$request_body_mode('invalid')", "$request_body_mode(on_demand)", "$request_body_mode('')", "$request_body_mode('on_demand','eager')", "$request_body_mode('on_demand').Optional()"} {
		source := "#setting($_ = $route('/views','DELETE'))\n#setting($_ = " + setting + ")\nSELECT ID FROM VIEWS"
		if _, err := parseComponentSource("test", "delete", source); err == nil {
			t.Fatalf("invalid setting accepted: %s", setting)
		}
	}
	source := "#setting($_ = $route('/views','DELETE'))\n#setting($_ = $request_body_mode('on_demand'))\n#setting($_ = $request_body_mode('eager'))\nSELECT ID FROM VIEWS"
	if _, err := parseComponentSource("test", "delete", source); err == nil || !strings.Contains(err.Error(), "once") {
		t.Fatal("duplicate mode accepted")
	}
}
