package link

import (
	"testing"

	"github.com/viant/datly/spec"
)

func TestBodyNullPolicyOverlay(t *testing.T) {
	result := overlayParam(&spec.Parameter{Name: "View", Source: spec.BindSource{Kind: "body"}}, &spec.Parameter{Name: "View", Source: spec.BindSource{Kind: "body"}, BodyNullPolicy: "empty-record"})
	if result.BodyNullPolicy != "empty-record" {
		t.Fatal("overlay dropped policy")
	}
}
