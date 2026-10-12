package column

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlx/metadata/database"
)

func TestQualifiedBigQueryWildcardNamespace(t *testing.T) {
	for _, source := range []string{
		"viant-e2e.adrtbbid.ad_rtb_bidrequests_*",
		"`viant-e2e.adrtbbid.ad_rtb_bidrequests_*`",
		"[viant-e2e:adrtbbid.ad_rtb_bidrequests_*]",
	} {
		m := &discoveryMetadata{}
		session, table, err := m.tableSession(context.Background(), nil, &database.Product{Name: "BigQuery"}, source)
		require.NoError(t, err)
		require.Equal(t, "viant-e2e", session.Catalog)
		require.Equal(t, "adrtbbid", session.Schema)
		require.Equal(t, "ad_rtb_bidrequests_*", table)
		require.Empty(t, m.sessions)
	}
}
