package cubecompose

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithPageDetachedNativeRendering(t *testing.T) {
	catalog, err := NewCatalog(Field{Name: "category", Type: reflect.TypeFor[string](), Role: Dimension}, Field{Name: "amount", Type: reflect.TypeFor[int](), Role: Measure})
	require.NoError(t, err)
	source := "SELECT category, SUM(value) AS amount FROM facts WHERE scope = ? GROUP BY category HAVING SUM(value) > ?"
	frames := []Frame{{SQL: source, Args: []any{7, 0}}}
	for _, tc := range []struct {
		name, sql string
		prefix    *int
		frames    []Frame
	}{
		{"flat", "SELECT t1.category AS key,t1.amount + 1 AS value FROM $CubeSQL1 AS t1 WHERE t1.amount > 0 ORDER BY value DESC,key ASC LIMIT 10", nil, frames},
		{"grouped", "SELECT t1.category AS key,SUM(t1.amount) AS value FROM $CubeSQL1 AS t1 WHERE t1.amount > 0 GROUP BY t1.category HAVING SUM(t1.amount) > 1 ORDER BY value DESC LIMIT 10", nil, frames},
		{"prefix", "SELECT t1.category AS key,t1.amount AS value FROM $CubeSQL1 AS t1 ORDER BY value DESC LIMIT 10", intPagePointer(5), frames},
		{"union", "SELECT t1.category AS key,t1.amount AS value FROM $CubeSQL1 AS t1 UNION ALL SELECT t2.category AS key,t2.amount AS value FROM $CubeSQL2 AS t2 ORDER BY key LIMIT 10 OFFSET 1", nil, append(append([]Frame{}, frames...), frames...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := CompileWithResultLimit(tc.sql, catalog, len(tc.frames), 100, tc.prefix)
			require.NoError(t, err)
			before, args, err := plan.Render(tc.frames)
			require.NoError(t, err)
			originalColumns := append([]Column(nil), plan.Columns...)
			for _, window := range []Page{{2, 0}, {2, 2}, {2, 4}, {2, 5}, {2, 10}} {
				page, err := plan.WithPage(window)
				require.NoError(t, err)
				rendered, _, err := page.RenderPage(tc.frames)
				require.NoError(t, err)
				require.Equal(t, len(tc.frames), strings.Count(rendered, "("+source+")"), "source SQL must remain byte-identical")
				require.Equal(t, window.Limit, page.Limit)
				require.Equal(t, window.Offset, page.Offset())
				if window.Offset > 0 {
					require.Contains(t, rendered, "OFFSET ")
				}
				if tc.prefix != nil && window.Offset >= *tc.prefix {
					require.Contains(t, rendered, "LIMIT 0")
				}
				page.Columns[0].Name = "mutated"
				page.Fields[0][0] = "mutated"
				require.Equal(t, originalColumns, plan.Columns)
				require.NotEqual(t, "mutated", plan.Fields[0][0])
			}
			after, afterArgs, err := plan.Render(tc.frames)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, args, afterArgs)
		})
	}
}

func TestWithPageBoundsAndOrder(t *testing.T) {
	plan, err := Compile("SELECT t1.category AS key FROM $CubeSQL1 AS t1 ORDER BY key LIMIT 10", unionCatalog(t), 1, 100)
	require.NoError(t, err)
	maxInt := int(^uint(0) >> 1)
	for _, page := range []Page{{0, 0}, {-1, 0}, {1, -1}, {101, 0}, {1, 100}, {maxInt, 1}, {1, maxInt}, {maxInt, maxInt}} {
		_, err = plan.WithPage(page)
		require.Error(t, err, "%+v", page)
	}
	for _, page := range []Page{{100, 0}, {1, 99}} {
		_, err = plan.WithPage(page)
		require.NoError(t, err)
	}
	unordered, err := Compile("SELECT t1.category AS key FROM $CubeSQL1 AS t1 LIMIT 10", unionCatalog(t), 1, 100)
	require.NoError(t, err)
	_, err = unordered.WithPage(Page{1, 1})
	require.Error(t, err)
	_, err = unordered.WithPage(Page{1, 0})
	require.NoError(t, err)
	var missing *Plan
	_, err = missing.WithPage(Page{1, 0})
	require.Error(t, err)
	huge, err := Compile("SELECT t1.category AS key FROM $CubeSQL1 AS t1 ORDER BY key LIMIT 10", unionCatalog(t), 1, maxInt)
	require.NoError(t, err)
	_, err = huge.WithPage(Page{maxInt, 0})
	require.Error(t, err)
}

func intPagePointer(value int) *int { return &value }
