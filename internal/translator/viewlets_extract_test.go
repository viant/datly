package translator

import (
	"strings"
	"testing"
)

func TestExtractRootViewletSQL_UsesFromAliasNotLastAliasOccurrence(t *testing.T) {
	sql := `
SELECT
    adConfig.*,
    set_limit(adConfig, 1)
FROM (
    ${embed:sql/ad_config/config.sql}
) adConfig
JOIN (
    SELECT 1
) agency ON 1 = 1`

	got := extractRootViewletSQL(sql, "adConfig")
	if !strings.Contains(got, "${embed:sql/ad_config/config.sql}") {
		t.Fatalf("expected embedded root sql, got: %s", got)
	}
}
