package generate

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestSharedBodyRowPhysicalAliasAndUnknownTypes(t *testing.T) {
	for _, tc := range []struct {
		name                                       string
		bodyType, currentType                      string
		bodyExplicit, currentExplicit, provisional bool
		want                                       string
	}{
		{name: "aliased body cast retains physical mapping", bodyType: "int", currentType: "string", bodyExplicit: true},
		{name: "explicit any cannot bypass concrete body", bodyType: "int", currentType: "any", currentExplicit: true, want: "conflicts"},
		{name: "final unresolved current", bodyType: "int", currentType: "any", want: "unresolved"},
		{name: "final unresolved body", bodyType: "any", currentType: "string", want: "unresolved"},
		{name: "discovery defers unknown body before concrete Current cast", bodyType: "any", currentType: "string", currentExplicit: true, provisional: true},
		{name: "discovery may defer inferred unknown", bodyType: "any", currentType: "string", provisional: true},
		{name: "explicit any cannot bypass during discovery", bodyType: "int", currentType: "any", currentExplicit: true, provisional: true, want: "conflicts"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := &spec.View{Name: "Body", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "DisplayTarget", Source: "TARGET", DatabaseType: "TEXT", Type: spec.TypeRef{Name: tc.bodyType}, ExplicitType: tc.bodyExplicit}}}
			current := &spec.View{Name: "Current", Source: &spec.ViewSource{Table: "records"}, Columns: []*spec.Column{{Name: "TARGET", Source: "TARGET", DatabaseType: "TEXT", Type: spec.TypeRef{Name: tc.currentType}, ExplicitType: tc.currentExplicit}}}
			p := &viewPlanner{plan: &Plan{Package: "example.com/archive"}, names: map[*spec.View]string{owner: "Record"}, dests: map[*spec.View]string{owner: "record.go"}, readTypes: map[*spec.View][]string{current: {"Record"}}, requireConcrete: !tc.provisional}
			err := p.shareBodyRow(current, owner, "record.go")
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
