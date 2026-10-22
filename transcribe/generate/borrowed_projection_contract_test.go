package generate

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/spec"
)

func TestBorrowedLeafContractIgnoresOccurrenceProjectionProvenance(t *testing.T) {
	view := &spec.View{Name: "Records", TypeName: "Row", Source: &spec.ViewSource{Table: "records", SQL: "SELECT r.* FROM records r"}, Columns: []*spec.Column{{Name: "id", Type: spec.TypeRef{Name: "int"}}}}
	view.DefaultProjection = true
	view.CompileProjectionOrigins()
	identity, err := view.Identity()
	require.NoError(t, err)
	plan := &Plan{Views: []ViewPlan{{Identity: identity, Fields: []Field{{Name: "Id", Type: "int"}}}}}
	contract := func(v *spec.View) BorrowedLeafContract {
		result, err := BorrowedLeafContractFromPlan(plan, v, "example.com/api", "Row", "main", "", "main", "records")
		require.NoError(t, err)
		return result
	}
	owner := contract(view)
	for _, useCase := range []struct {
		description, sql string
		wildcard         bool
	}{
		{"explicit physical projection", "SELECT r.id FROM records r", false},
		{"another wildcard occurrence", "SELECT other.* FROM records other", true},
	} {
		t.Run(useCase.description, func(t *testing.T) {
			borrower := view.Clone()
			borrower.Source.SQL = useCase.sql
			borrower.DefaultProjection = useCase.wildcard
			borrower.CompileProjectionOrigins()
			require.NoError(t, CompareBorrowedLeafContracts(owner, contract(borrower)))
			require.Equal(t, useCase.sql, borrower.Source.SQL)
			require.Equal(t, useCase.wildcard, borrower.DefaultProjection)
			_, _, known := borrower.ProjectionOrigin("id")
			require.True(t, known, "comparison must preserve the live occurrence attribution")
			borrower.Columns[0].Nullable = true
			require.ErrorContains(t, CompareBorrowedLeafContracts(owner, contract(borrower)), "leaf contracts differ")
		})
	}
	require.True(t, view.DefaultProjection)
	_, _, known := view.ProjectionOrigin("id")
	require.True(t, known)
}
