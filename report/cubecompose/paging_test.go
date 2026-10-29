package cubecompose

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderPageOneRowLookaheadWithoutPlanMutation(t *testing.T) {
	for _, sql := range []string{
		"SELECT t1.total_spend AS amount FROM $CubeSQL1 AS t1 LIMIT 3",
		"SELECT t1.total_spend AS amount FROM $CubeSQL1 AS t1 UNION ALL SELECT t2.total_spend AS amount FROM $CubeSQL2 AS t2 LIMIT 3",
	} {
		count := 1
		if strings.Contains(sql, "UNION ALL") {
			count = 2
		}
		p, e := Compile(sql, unionCatalog(t), count, 3)
		require.NoError(t, e)
		frames := make([]Frame, count)
		for i := range frames {
			frames[i] = Frame{SQL: "SELECT SUM(amount) AS total_spend FROM facts"}
		}
		page, _, e := p.RenderPage(frames)
		require.NoError(t, e)
		require.True(t, strings.HasSuffix(page, "LIMIT 4"))
		require.Equal(t, 1, strings.Count(page, "LIMIT"))
		original, _, e := p.Render(frames)
		require.NoError(t, e)
		require.True(t, strings.HasSuffix(original, "LIMIT 3"))
		require.Equal(t, 3, p.Limit)
	}
}
