package sqlanalysis

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckedRejectsDeclarationCompositeIn(t *testing.T) {
	const source = `SELECT r.id FROM records r WHERE $criteria.CompositeIn($Unsafe.Alias(), $Unsafe.Keys($Rows))`
	_, _, err := Checked(source, nil)
	require.ErrorContains(t, err, "unsupported SQL criteria call")
}
