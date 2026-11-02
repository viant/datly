package cubecompose

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
)

func TestStandalonePredicateParserShape(t *testing.T) {
	for _, condition := range []string{
		"(t1.key = 2 AND (t1.amount > 5 OR t1.label = 'all'))",
		"CASE WHEN t1.label = 'all' THEN t1.key = 2 ELSE t1.amount > 5 AND t1.key = 2 END",
	} {
		q, err := sqlparser.ParseQuery("SELECT t1.key FROM DATLYCUBESQL1 t1 WHERE " + condition)
		require.NoError(t, err)
		root, ok := q.Qualify.X.(*expr.Binary)
		require.True(t, ok)
		require.Equal(t, "", root.Op)
		require.NotNil(t, root.X)
		require.Nil(t, root.Y)
		t.Logf("standalone root: %#v; operand: %T", root, root.X)
		q, err = sqlparser.ParseQuery("SELECT t1.key FROM DATLYCUBESQL1 t1 WHERE t1.key = 2 AND " + condition)
		require.NoError(t, err)
		root = q.Qualify.X.(*expr.Binary)
		require.Equal(t, "AND", root.Op)
	}
}

type standaloneControls struct {
	Mode      string  `json:"mode"`
	Key       int64   `json:"key"`
	Threshold float64 `json:"threshold"`
}

func TestStandalonePredicatesNativeExecution(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	values, err := NewValuesContract(reflect.TypeFor[standaloneControls]())
	require.NoError(t, err)
	conditions := []string{
		"(t1.key = $Values.key AND (t1.amount > $Values.threshold OR t1.label = $Values.mode))",
		"((t1.key = $Values.key AND t1.amount > $Values.threshold) OR (t1.key = $Values.key AND t1.label = $Values.mode))",
		"CASE WHEN t1.label = $Values.mode THEN t1.key = $Values.key ELSE t1.amount > $Values.threshold AND t1.key = $Values.key END",
	}
	for _, condition := range conditions {
		for _, prefix := range []string{"", "t1.key = $Values.key AND "} {
			t.Run(prefix+condition, func(t *testing.T) {
				plan, err := CompileWithValues("SELECT t1.key FROM $CubeSQL1 t1 WHERE "+prefix+condition, caseCatalog(t), 1, 10, nil, values)
				require.NoError(t, err)
				require.ElementsMatch(t, []string{"key", "amount", "label"}, plan.Fields[0])
				for _, tc := range []struct {
					name, raw string
					amount    any
					count     int
				}{
					{"true", `{"key":2,"threshold":5,"mode":"other"}`, 12.0, 1},
					{"false", `{"key":2,"threshold":5,"mode":"other"}`, 1.0, 0},
					{"case-or-branch", `{"key":2,"threshold":5,"mode":"native"}`, nil, 1},
					{"key-mismatch", `{"key":3,"threshold":5,"mode":"native"}`, 12.0, 0},
					{"unknown", `{"key":2,"threshold":5,"mode":"other"}`, nil, 0},
					{"bound-text", `{"key":2,"threshold":5,"mode":"x'; DROP TABLE secret; --"}`, 12.0, 1},
				} {
					t.Run(tc.name, func(t *testing.T) {
						bound, err := plan.BindValues(context.Background(), json.RawMessage(tc.raw))
						require.NoError(t, err)
						rendered, args, err := bound.Render([]Frame{{SQL: "SELECT ? AS key, ? AS amount, ? AS label", Args: []any{int64(2), tc.amount, "native"}}})
						require.NoError(t, err)
						require.NotContains(t, rendered, "DROP TABLE")
						require.Equal(t, strings.Count(rendered, "?"), len(args))
						rows, err := db.Query(rendered, args...)
						require.NoError(t, err)
						count := 0
						for rows.Next() {
							var key int64
							require.NoError(t, rows.Scan(&key))
							require.Equal(t, int64(2), key)
							count++
						}
						require.NoError(t, rows.Err())
						require.NoError(t, rows.Close())
						require.Equal(t, tc.count, count)
					})
				}
			})
		}
	}
}

func TestStandalonePredicateRejectsUnsafeWrappers(t *testing.T) {
	plan := &Plan{catalog: caseCatalog(t), FrameCount: 1}
	qualified := &expr.Selector{Name: "t1", X: &expr.Ident{Name: "key"}}
	for _, invalid := range []*expr.Binary{
		{},
		{Y: qualified},
		{X: qualified, Y: qualified},
		{X: qualified, Op: " "},
		{X: qualified, Y: qualified, Op: "=; DROP TABLE secret"},
		{X: &expr.Ident{Name: "key"}},
		{X: &expr.Raw{}},
		{X: &expr.Binary{X: qualified, Y: qualified, Op: "malicious"}},
		{X: &expr.Parenthesis{}},
	} {
		_, err := plan.validateNode(invalid, nil)
		require.Error(t, err, "%#v", invalid)
	}
	for _, condition := range []string{
		"(key = 2 AND t1.amount > 5)",
		"CASE WHEN t1.key = 2 THEN secret = 1 ELSE t1.amount > 5 END",
		"(t1.key = 2 AND t1.secret = 1)",
		"CASE WHEN t1.key = 2 THEN t2.key = 1 ELSE t1.amount > 5 END",
		"(t1.key = 2 XOR t1.amount > 5)",
		"(t1.key = 2); DROP TABLE secret",
		"CASE WHEN t1.key = 2 THEN t1.amount > 5 END; DROP TABLE secret",
	} {
		_, err := Compile("SELECT t1.key FROM $CubeSQL1 t1 WHERE "+condition, caseCatalog(t), 1, 10)
		require.Error(t, err, condition)
	}
}
