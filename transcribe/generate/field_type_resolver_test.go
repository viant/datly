package generate

import (
	"errors"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/x"
)

func TestFieldTypeResolverPrefersGeneratedLocalViewToDefaultPackage(t *testing.T) {
	lookups := 0
	resolver := &fieldTypeResolver{
		plan:          &Plan{RootViewType: "Event", Views: []ViewPlan{{Type: "Event", Ownership: ViewGenerated}}},
		context:       &spec.TypeContext{DefaultPackage: "example.com/contracts"},
		targetPackage: "example.com/generated/events",
		lookup: func(string) (*x.Type, error) {
			lookups++
			return nil, errors.New("local type reached package lookup")
		},
	}
	actual, err := resolver.resolve("[]*Event")
	if err != nil {
		t.Fatal(err)
	}
	if actual != "[]*Event" || lookups != 0 {
		t.Fatalf("resolved = %q, lookups = %d", actual, lookups)
	}
}
