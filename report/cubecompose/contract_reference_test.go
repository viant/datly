package cubecompose

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// Contract admission consumes these native parser-owned references. Projections
// alone are insufficient: hidden inputs in predicates/order/late UNION branches
// must remain visible to the admission owner even when absent from output names.
func TestContractAdmissionReferencesCoverResolvedGrammar(t *testing.T) {
	catalog, err := NewCatalog(Field{Name: "key", Type: reflect.TypeFor[int](), Role: Dimension}, Field{Name: "secret", Type: reflect.TypeFor[float64](), Role: Measure}, Field{Name: "public", Type: reflect.TypeFor[float64](), Role: Measure}, Field{Name: "privateKey", Type: reflect.TypeFor[int](), Role: Dimension})
	require.NoError(t, err)
	for _, tc := range []struct {
		name, sql           string
		frames, secretFrame int
	}{
		{"alias", "SELECT t1.secret AS harmless FROM $CubeSQL1 AS t1 LIMIT 5", 1, 0},
		{"arithmetic", "SELECT t1.secret / NULLIF(t1.public,0) AS harmless FROM $CubeSQL1 AS t1 LIMIT 5", 1, 0},
		{"case", "SELECT CASE WHEN t1.secret > 0 THEN t1.public ELSE 0 END AS harmless FROM $CubeSQL1 AS t1 LIMIT 5", 1, 0},
		{"where", "SELECT t1.key FROM $CubeSQL1 AS t1 WHERE t1.secret > 0 LIMIT 5", 1, 0},
		{"order", "SELECT t1.key FROM $CubeSQL1 AS t1 ORDER BY t1.secret DESC LIMIT 5", 1, 0},
		{"having", "SELECT t1.key,SUM(t1.public) AS harmless FROM $CubeSQL1 AS t1 GROUP BY t1.key HAVING SUM(t1.secret)>0 LIMIT 5", 1, 0},
		{"join", "SELECT t1.key FROM $CubeSQL1 AS t1 JOIN $CubeSQL2 AS t2 ON t1.key=t2.key AND t2.privateKey=t1.privateKey LIMIT 5", 2, 1},
		{"late-union", "SELECT t1.public AS harmless FROM $CubeSQL1 AS t1 UNION ALL SELECT t2.secret AS harmless FROM $CubeSQL2 AS t2 LIMIT 5", 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, e := Compile(tc.sql, catalog, tc.frames, 10)
			require.NoError(t, e)
			protected := "secret"
			if tc.name == "join" {
				protected = "privateKey"
			}
			require.Contains(t, plan.Fields[tc.secretFrame], protected)
		})
	}
	_, err = Compile("SELECT ROW_NUMBER() OVER (ORDER BY t1.secret) AS harmless FROM $CubeSQL1 AS t1 LIMIT 5", catalog, 1, 10)
	require.Error(t, err, "unsupported windows must stay rejected by native grammar")
}
