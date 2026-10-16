package dql

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSourceMemberSettings(t *testing.T) {
	prepared := PrepareSource(`#setting($_ = $source_input('Request'))
#setting($_ = $source_member('Request','origin'))
SELECT 1 AS Id`)
	require.NoError(t, prepared.Err())
	// Invalid or duplicate declarations fail at authoring rather than publication.
	for _, text := range []string{
		`#setting($_ = $source_member('Request','origin'))
#setting($_ = $source_member('Request','origin'))`,
		`#setting($_ = $source_member('pkg.Request','origin'))`,
		`#setting($_ = $source_input('Request'))
#setting($_ = $source_input('Request'))`,
		`#setting($_ = $source_member('Request','origin','bool',''))`,
	} {
		require.Error(t, PrepareSource(text+"\nSELECT 1 AS Id").Err())
	}
}
