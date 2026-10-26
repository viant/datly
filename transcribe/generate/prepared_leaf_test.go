package generate

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestPreparedLeafPairBoundaries(t *testing.T) {
	type useCase struct {
		description string
		change      func(root, public, physical *spec.View)
		expected    bool
	}
	for _, tc := range []useCase{
		{description: "auxiliary request and physical actions", expected: true},
		{description: "writable parent", change: func(r, p, w *spec.View) { r.Auxiliary = false }},
		{description: "two physical roles", change: func(r, p, w *spec.View) { p.Auxiliary = false }},
		{description: "ordinary patch policy", change: func(r, p, w *spec.View) { w.WriterActionPolicy = "" }},
		{description: "different table", change: func(r, p, w *spec.View) { w.Source.Table = "other" }},
		{description: "inferred type", change: func(r, p, w *spec.View) { p.TypeName = "" }},
		{description: "nonleaf", change: func(r, p, w *spec.View) { p.Relations = []*spec.Relation{{View: &spec.View{Name: "nested"}}} }},
		{description: "unrelated declaration", change: func(r, p, w *spec.View) { r.Relations = r.Relations[:1] }},
	} {
		t.Run(tc.description, func(t *testing.T) {
			public := &spec.View{Name: "requested", TypeName: "Member", Auxiliary: true, Source: &spec.ViewSource{Table: "members"}}
			physical := &spec.View{Name: "prepared", TypeName: "Member", WriterActionPolicy: "insert-delete", Source: &spec.ViewSource{Table: "members"}}
			root := &spec.View{Name: "owners", Auxiliary: true, Relations: []*spec.Relation{{View: public}, {View: physical}}}
			if tc.change != nil {
				tc.change(root, public, physical)
			}
			require.Equal(t, tc.expected, PreparedLeafPair(root, public, physical))
			require.Equal(t, tc.expected, PreparedLeafPair(root, physical, public))
		})
	}
}
