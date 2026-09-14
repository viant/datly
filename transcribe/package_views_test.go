package transcribe

import (
	"strings"
	"testing"

	"github.com/viant/datly/spec"
)

func TestPackageIndependentViewRequiresPackageAuthority(t *testing.T) {
	view := &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/other", Name: "Audit"}, Name: "Audit"}
	actual, err := newPackageViewResolver(&spec.Component{Key: spec.Key{Scope: "example.com/contracts"}, Views: []*spec.View{view}}, nil).independent("Audit")
	if err == nil || actual != nil || !strings.Contains(err.Error(), `is not owned by package "example.com/contracts"`) {
		t.Fatalf("packageIndependentView() actual=%+v error=%v", actual, err)
	}
}

func TestPackageIndependentViewUsesMatchingPackageAuthority(t *testing.T) {
	other := &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/other", Name: "Audit"}, Name: "Audit"}
	want := &spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/contracts", Name: "Audit"}, Name: "Audit"}
	actual, err := newPackageViewResolver(&spec.Component{Key: spec.Key{Scope: "example.com/contracts"}, Views: []*spec.View{other, want}}, nil).independent("Audit")
	if err != nil || actual != want {
		t.Fatalf("packageIndependentView() actual=%+v error=%v", actual, err)
	}
}
