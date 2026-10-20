package transcribe

import (
	"encoding/json"
	"fmt"
	"github.com/viant/datly/spec"
	"testing"

	"github.com/stretchr/testify/require"
	gen "github.com/viant/datly/transcribe/generate"
)

func TestBorrowedPurePhysicalTransparentWrapper(t *testing.T) {
	result, leaf, declaration := borrowedAuxiliaryNullComparisonFixture(t)
	leaf.Auxiliary = false
	direct, err := borrowedLeafContract(t.Context(), result, leaf, declaration, false)
	require.NoError(t, err)
	original := leaf.Source.SQL
	leaf.Source.SQL = "SELECT * FROM (SELECT * FROM (" + original + ") inner_body) outer_body"
	before, err := json.Marshal(result.Component)
	require.NoError(t, err)
	wrapped, err := borrowedLeafContract(t.Context(), result, leaf, declaration, false)
	require.NoError(t, err, "physical leaves must use the same exact transparent wrapper proof")
	require.NoError(t, gen.CompareBorrowedLeafContracts(direct, wrapped))
	after, err := json.Marshal(result.Component)
	require.NoError(t, err)
	require.JSONEq(t, string(before), string(after), "detached proof changed canonical SQL/columns")
	require.Equal(t, "SELECT * FROM (SELECT * FROM ("+original+") inner_body) outer_body", leaf.Source.SQL)
}

func TestBorrowedPurePhysicalWrapperRetainsNontransparentClauses(t *testing.T) {
	for _, tc := range []struct{ name, template string }{
		{"where", "SELECT * FROM (%s) wrapped WHERE id>0"},
		{"distinct", "SELECT DISTINCT * FROM (%s) wrapped"},
		{"group", "SELECT * FROM (%s) wrapped GROUP BY id"},
		{"having", "SELECT * FROM (%s) wrapped GROUP BY id HAVING COUNT(*)>0"},
		{"window", "SELECT * FROM (%s) wrapped WINDOW w AS (PARTITION BY id)"},
		{"qualify", "SELECT * FROM (%s) wrapped QUALIFY ROW_NUMBER() OVER (ORDER BY id)=1"},
		{"order", "SELECT * FROM (%s) wrapped ORDER BY id"},
		{"limit", "SELECT * FROM (%s) wrapped LIMIT 1"},
		{"offset", "SELECT * FROM (%s) wrapped LIMIT 2 OFFSET 1"},
		{"join", "SELECT * FROM (%s) wrapped JOIN parents p ON wrapped.id=p.id"},
		{"union", "SELECT * FROM (%s) wrapped UNION SELECT id,parent_id,name FROM records"},
		{"cte", "WITH extra AS (SELECT id FROM parents) SELECT * FROM (%s) wrapped"},
		{"except", "SELECT * EXCEPT(name) FROM (%s) wrapped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, leaf, declaration := borrowedAuxiliaryNullComparisonFixture(t)
			leaf.Auxiliary = false
			leaf.Source.SQL = fmt.Sprintf(tc.template, leaf.Source.SQL)
			before, err := json.Marshal(result.Component)
			require.NoError(t, err)
			_, err = borrowedLeafContract(t.Context(), result, leaf, declaration, false)
			require.Error(t, err, "nontransparent wrapper semantics were discarded")
			after, err := json.Marshal(result.Component)
			require.NoError(t, err)
			require.JSONEq(t, string(before), string(after), "rejected proof changed canonical source")
		})
	}
}

func TestBorrowedQualifiedPhysicalTransparentWrapper(t *testing.T) {
	for _, transient := range []bool{false, true} {
		t.Run(fmt.Sprint("transient=", transient), func(t *testing.T) {
			result, leaf, declaration := borrowedAuxiliaryNullComparisonFixture(t)
			leaf.Auxiliary = false
			if transient {
				leaf.Source.SQL = "SELECT r.id,r.parent_id,r.name,NULL AS note FROM records r"
				leaf.Columns = append(leaf.Columns, &spec.Column{Name: "note", Source: "note", Type: spec.TypeRef{Name: "*string"}, Nullable: true, Tag: `sqlx:"-"`})
			}
			direct, err := borrowedLeafContract(t.Context(), result, leaf, declaration, false)
			require.NoError(t, err)
			// Flight-shaped generated outer alias keeps the inner restricted projection.
			inner := leaf.Source.SQL
			leaf.Source.SQL = "SELECT f.* FROM (" + inner + ") f"
			before, err := json.Marshal(result.Component)
			require.NoError(t, err)
			wrapped, err := borrowedLeafContract(t.Context(), result, leaf, declaration, false)
			require.NoError(t, err)
			require.NoError(t, gen.CompareBorrowedLeafContracts(direct, wrapped))
			after, err := json.Marshal(result.Component)
			require.NoError(t, err)
			require.JSONEq(t, string(before), string(after), "qualified proof changed authored SQL/row")
			require.Equal(t, "SELECT f.* FROM ("+inner+") f", leaf.Source.SQL)
		})
	}
}

func TestBorrowedQualifiedPhysicalWrapperRetainsNontransparentClauses(t *testing.T) {
	for _, template := range []string{
		"SELECT other.* FROM (%s) f",
		"SELECT f.* FROM (%s)",
		"SELECT f[0].* FROM (%s) f",
		"SELECT f.* AS renamed FROM (%s) f",
		"SELECT f.* EXCEPT(name) FROM (%s) f",
		"SELECT f.* FROM (%s) f WHERE id>0",
		"SELECT f.* FROM (%s) f ORDER BY id",
		"SELECT f.* FROM (%s) f LIMIT 1",
		"SELECT f.* FROM (%s) f GROUP BY id",
		"SELECT DISTINCT f.* FROM (%s) f",
	} {
		t.Run(template, func(t *testing.T) {
			result, leaf, declaration := borrowedAuxiliaryNullComparisonFixture(t)
			leaf.Auxiliary = false
			leaf.Source.SQL = fmt.Sprintf(template, leaf.Source.SQL)
			before, err := json.Marshal(result.Component)
			require.NoError(t, err)
			_, err = borrowedLeafContract(t.Context(), result, leaf, declaration, false)
			require.Error(t, err, "qualified nontransparent wrapper was peeled")
			after, err := json.Marshal(result.Component)
			require.NoError(t, err)
			require.JSONEq(t, string(before), string(after))
		})
	}
}
