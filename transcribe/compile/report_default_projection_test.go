package compile

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestReportDefaultProjectionProvenance(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		expected  bool
	}{
		{"wildcard", "SELECT r.* FROM (SELECT id, amount FROM records) r", true},
		{"metadata controls", "SELECT r.*, CAST(r.amount AS float64), optional(r.id) FROM (SELECT id, amount FROM records) r", true},
		{"explicit projection", "SELECT r.amount, r.id FROM (SELECT id, amount FROM records) r", false},
		{"mixed projection", "SELECT r.*, r.amount AS Value FROM (SELECT id, amount FROM records) r", false},
		{"table wildcard", "SELECT * FROM records", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view, err := NewReader().Compile(ReadInput{View: &spec.View{Name: "Records", Source: &spec.ViewSource{SQL: tc.sql}}, SQL: tc.sql})
			require.NoError(t, err)
			require.Equal(t, tc.expected, view.DefaultProjection)
			require.Equal(t, tc.expected, view.Clone().DefaultProjection)
			body, err := json.Marshal(view)
			require.NoError(t, err)
			var decoded spec.View
			require.NoError(t, json.Unmarshal(body, &decoded))
			require.False(t, decoded.DefaultProjection, "parser provenance is not a public artifact option")
		})
	}
}
