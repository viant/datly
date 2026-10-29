package cubecompose

import (
	"database/sql"
	"reflect"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlparser/expr"
)

func caseCatalog(t *testing.T) *Catalog {
	t.Helper()
	c, e := NewCatalog(Field{Name: "key", Type: reflect.TypeFor[int64](), Role: Dimension}, Field{Name: "amount", Type: reflect.TypeFor[float64](), Role: Measure}, Field{Name: "label", Type: reflect.TypeFor[string](), Role: Dimension})
	require.NoError(t, e)
	return c
}

func TestCaseStructuredParserAndNativeExecution(t *testing.T) {
	db, e := sql.Open("sqlite3", ":memory:")
	require.NoError(t, e)
	defer db.Close()
	for _, tc := range []struct {
		name, expression string
		want             any
		typ              reflect.Type
	}{
		{"searched", "CASE WHEN t1.amount > 5 THEN t1.amount ELSE 0 END", float64(12), reflect.TypeFor[float64]()},
		{"parenthesized", "(CASE WHEN t1.amount > 5 THEN t1.amount ELSE NULL END)", float64(12), reflect.TypeFor[*float64]()},
		{"simple", "CASE t1.key WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END", "two", reflect.TypeFor[string]()},
		{"nested", "CASE WHEN t1.key = 2 THEN CASE WHEN t1.amount > 10 THEN 3.5 ELSE 1 END ELSE NULL END", float64(3.5), reflect.TypeFor[*float64]()},
		{"missing-else", "CASE WHEN t1.key = 1 THEN t1.amount END", nil, reflect.TypeFor[*float64]()},
		{"null-arm", "CASE WHEN t1.key = 2 THEN NULL ELSE t1.amount END", nil, reflect.TypeFor[*float64]()},
		{"ratio", "CASE WHEN t1.amount < 0 THEN NULL ELSE t1.amount / NULLIF(t1.amount, 0) END", float64(1), reflect.TypeFor[*float64]()},
		{"literal-not-sql", "CASE WHEN t1.key = 2 THEN 'x; DROP TABLE secret; --' ELSE '' END", "x; DROP TABLE secret; --", reflect.TypeFor[string]()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := "SELECT " + tc.expression + " AS result FROM $CubeSQL1 AS t1 LIMIT 3"
			// The pinned parser must expose traversable branches, including parenthesized CASE.
			parsed, e := sqlparser.ParseQuery(strings.ReplaceAll(query, "$CubeSQL1", "DATLYCUBESQL1"))
			require.NoError(t, e)
			sw, ok := unwrapParenthesis(parsed.List[0].Expr).(*expr.Switch)
			require.True(t, ok)
			require.NotEmpty(t, sw.Cases)
			p, e := Compile(query, caseCatalog(t), 1, 3)
			require.NoError(t, e)
			require.Equal(t, tc.typ, p.Columns[0].RuntimeType())
			require.Equal(t, 3, p.Limit)
			rendered, args, e := p.Render([]Frame{{SQL: "SELECT ? AS key, ? AS amount, ? AS label", Args: []any{2, 12.0, "native"}}})
			require.NoError(t, e)
			require.Equal(t, []any{2, 12.0, "native"}, args[len(args)-3:])
			require.NotContains(t, rendered, "DROP TABLE")
			t.Logf("SQL: %s; args: %#v", rendered, args)
			var got any
			require.NoError(t, db.QueryRow(rendered, args...).Scan(&got))
			require.Equal(t, tc.want, got)
		})
	}
}
func TestCaseTracksEveryBranchAndLeftJoinNullability(t *testing.T) {
	p, e := Compile("SELECT CASE t1.key WHEN 2 THEN t2.amount ELSE t1.amount END AS result FROM $CubeSQL1 AS t1 LEFT JOIN $CubeSQL2 AS t2 ON t1.key = t2.key LIMIT 2", caseCatalog(t), 2, 2)
	require.NoError(t, e)
	require.ElementsMatch(t, []string{"key", "amount"}, p.Fields[0])
	require.ElementsMatch(t, []string{"key", "amount"}, p.Fields[1])
	require.Equal(t, reflect.TypeFor[*float64](), p.Columns[0].RuntimeType())
	sql, args, e := p.Render([]Frame{{SQL: "SELECT ? AS key, ? AS amount", Args: []any{2, 4.0}}, {SQL: "SELECT ? AS key, ? AS amount", Args: []any{3, 99.0}}})
	require.NoError(t, e)
	require.Equal(t, []any{int64(2), 2, 4.0, 3, 99.0}, args)
	require.Contains(t, sql, "LEFT JOIN")
}
func TestCaseRejectsUnsafeOrIncompatibleBranches(t *testing.T) {
	for _, value := range []string{
		"CASE WHEN t1.key = 2 THEN NULL ELSE NULL END",
		"CASE WHEN t1.key = 1 THEN NULL END",
		"CASE WHEN t1.key = 1 THEN t1.missing ELSE 0 END",
		"CASE WHEN t2.key = 1 THEN 2 ELSE 0 END",
		"CASE WHEN t1.key = 1 THEN ? ELSE 0 END",
		"CASE WHEN t1.key = 1 THEN (SELECT amount FROM secret) ELSE 0 END",
		"CASE WHEN t1.key = 1 THEN secret(t1.amount) ELSE 0 END",
		"CASE WHEN t1.key = 1 THEN 'text' ELSE t1.amount END",
		"CASE t1.key WHEN 'text' THEN 1 ELSE 0 END",
		"CASE WHEN t1.amount THEN 1 ELSE 0 END",
		"CASE WHEN EXISTS(SELECT 1 FROM secret) THEN 1 ELSE 0 END",
		"CASE WHEN t1.key = 1 THEN 2 ELSE 0 END; DELETE FROM secret",
		"CASE WHEN t1.key = 1 THEN 2 ELSE 0 END UNION SELECT 1 FROM secret",
		"CASE WHEN t1.key = 1 THEN 2 END FROM secret --",
	} {
		t.Run(value, func(t *testing.T) {
			_, e := Compile("SELECT "+value+" AS result FROM $CubeSQL1 AS t1 LIMIT 2", caseCatalog(t), 1, 2)
			require.Error(t, e)
		})
	}
	p := &Plan{catalog: caseCatalog(t), FrameCount: 1, nullable: []bool{false}}
	for _, s := range []*expr.Switch{{Raw: "CASE WHEN 1 THEN unsafe() END"}, {Cases: []*expr.Case{nil}}, {Cases: []*expr.Case{{X: expr.Qualify{X: expr.NewIntLiteral("1")}}}}, {Cases: []*expr.Case{{X: expr.Qualify{X: &expr.Raw{}}, Y: expr.NewIntLiteral("1")}}}} {
		_, e := p.validateNode(s, nil)
		require.Error(t, e)
	}
	// CASE does not relax UNION projection, join-key, wildcard or limit restrictions.
	for _, q := range []string{
		"SELECT CASE WHEN t1.key=1 THEN 1 END AS value FROM $CubeSQL1 t1 LIMIT 3",
		"SELECT CASE WHEN t1.key=1 THEN 1 END AS value FROM $CubeSQL1 t1 UNION ALL SELECT 1 AS value FROM $CubeSQL2 t2 LIMIT 2",
		"SELECT t1.key FROM $CubeSQL1 t1 JOIN $CubeSQL2 t2 ON CASE WHEN t1.key=t2.key THEN 1 ELSE 0 END=1 LIMIT 2",
	} {
		frames := 1
		if strings.Contains(q, "$CubeSQL2") {
			frames = 2
		}
		_, e := Compile(q, caseCatalog(t), frames, 2)
		require.Error(t, e)
	}
}

func TestCaseRejectsUnrepresentableUnsignedMerge(t *testing.T) {
	c, e := NewCatalog(Field{Name: "key", Type: reflect.TypeFor[int64](), Role: Dimension}, Field{Name: "amount", Type: reflect.TypeFor[uint64](), Role: Measure})
	require.NoError(t, e)
	maximum := ^uint64(0)
	require.Greater(t, maximum, uint64(1<<63-1))
	_, e = Compile("SELECT CASE WHEN t1.key=1 THEN t1.amount ELSE 0 END AS value FROM $CubeSQL1 t1 LIMIT 1", c, 1, 1)
	require.ErrorContains(t, e, "unsigned")
	// No narrowing when all arms retain the same uint64 source contract.
	p, e := Compile("SELECT CASE WHEN t1.key=1 THEN t1.amount ELSE t1.amount END AS value FROM $CubeSQL1 t1 LIMIT 1", c, 1, 1)
	require.NoError(t, e)
	require.Equal(t, reflect.TypeFor[uint64](), p.Columns[0].RuntimeType())
	_, args, e := p.Render([]Frame{{SQL: "SELECT ? AS key, ? AS amount", Args: []any{1, maximum}}})
	require.NoError(t, e)
	require.Equal(t, maximum, args[len(args)-1])
}

// The pinned parser treats a slash touching the following function as part of
// that function's qualified name. Keep this fail-closed until sqlparser supplies
// a structured binary division; never recover it from Raw text here.
func TestCaseCompactDivisionParserBoundary(t *testing.T) {
	compact := "CASE WHEN t1.amount/NULLIF(t1.amount,0)=0 THEN NULL ELSE t1.amount END"
	_, err := Compile("SELECT "+compact+" AS value FROM $CubeSQL1 t1 LIMIT 1", caseCatalog(t), 1, 1)
	require.ErrorContains(t, err, "unsupported cube compose function")
	require.Contains(t, strings.ToLower(err.Error()), "amount/nullif")
	spaced := strings.Replace(compact, "amount/NULLIF", "amount / NULLIF", 1)
	plan, err := Compile("SELECT "+spaced+" AS value FROM $CubeSQL1 t1 LIMIT 1", caseCatalog(t), 1, 1)
	require.NoError(t, err)
	rendered, args, err := plan.Render([]Frame{{SQL: "SELECT 12.0 AS amount"}})
	require.NoError(t, err)
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	var value float64
	require.NoError(t, db.QueryRow(rendered, args...).Scan(&value))
	require.Equal(t, float64(12), value)
}

func TestCaseIntegerDivisionHasFractionCapableContract(t *testing.T) {
	for _, tc := range []struct {
		expression string
		typ        reflect.Type
	}{
		{"CASE WHEN t1.key > 0 THEN t1.key / 2 ELSE 0 END", reflect.TypeFor[*float64]()},
		{"CASE WHEN t1.key > 0 THEN t1.key / 2 END", reflect.TypeFor[*float64]()},
		{"CASE WHEN t1.key > 0 THEN t1.key / NULLIF(t1.key, 0) ELSE 0 END", reflect.TypeFor[*float64]()},
	} {
		p, e := Compile("SELECT "+tc.expression+" AS ratio FROM $CubeSQL1 t1 LIMIT 1", caseCatalog(t), 1, 1)
		require.NoError(t, e)
		require.Equal(t, tc.typ, p.Columns[0].RuntimeType())
		require.Equal(t, "float64", p.Columns[0].Type)
		rendered, _, e := p.Render([]Frame{{SQL: "SELECT 3 AS key"}})
		require.NoError(t, e)
		require.Contains(t, rendered, "t1.key / ")
		require.NotContains(t, rendered, "CAST")
	}
	// SQLite integer division is deliberately not a warehouse type oracle:
	// GoogleSQL's documented INT64 / INT64 result type is FLOAT64. Preserve SQL
	// semantics while providing a scanner capable of its fractional result.
	db, e := sql.Open("sqlite3", ":memory:")
	require.NoError(t, e)
	defer db.Close()
	var sqliteInteger int
	require.NoError(t, db.QueryRow("SELECT 3 / 2").Scan(&sqliteInteger))
	require.Equal(t, 1, sqliteInteger)
}

func TestCaseComputedNumericWideningNativeScanner(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()
	for _, tc := range []struct {
		name              string
		source            reflect.Type
		expression, frame string
		wantType          reflect.Type
		want              any
	}{
		{"direct int8", reflect.TypeFor[int8](), "CASE WHEN 1=1 THEN t1.amount ELSE t1.amount END", "SELECT 100 AS amount", reflect.TypeFor[int8](), int8(100)},
		{"divide zero int8", reflect.TypeFor[int8](), "CASE WHEN 1=1 THEN t1.amount / 0 ELSE 0 END", "SELECT 3 AS amount", reflect.TypeFor[*float64](), nil},
		{"divide field zero int8", reflect.TypeFor[int8](), "CASE WHEN 1=1 THEN t1.amount / (t1.amount - t1.amount) ELSE 0 END", "SELECT 3 AS amount", reflect.TypeFor[*float64](), nil},
		{"divide fraction int8", reflect.TypeFor[int8](), "CASE WHEN 1=1 THEN t1.amount / 2.0 ELSE 0 END", "SELECT 3 AS amount", reflect.TypeFor[*float64](), float64(1.5)},
		{"add int8", reflect.TypeFor[int8](), "CASE WHEN 1=1 THEN t1.amount + t1.amount ELSE t1.amount END", "SELECT 100 AS amount", reflect.TypeFor[int64](), int64(200)},
		{"subtract int8", reflect.TypeFor[int8](), "CASE WHEN 1=1 THEN t1.amount - (-t1.amount) ELSE t1.amount END", "SELECT 100 AS amount", reflect.TypeFor[int64](), int64(200)},
		{"multiply int8", reflect.TypeFor[int8](), "CASE WHEN 1=1 THEN t1.amount * t1.amount ELSE t1.amount END", "SELECT 100 AS amount", reflect.TypeFor[int64](), int64(10000)},
		{"sum int8", reflect.TypeFor[int8](), "CASE WHEN 1=1 THEN SUM(t1.amount) ELSE t1.amount END", "SELECT 100 AS amount UNION ALL SELECT 100 AS amount", reflect.TypeFor[*int64](), int64(200)},
		{"nullable int8", reflect.TypeFor[*int8](), "CASE WHEN 1=1 THEN t1.amount + t1.amount END", "SELECT 100 AS amount", reflect.TypeFor[*int64](), int64(200)},
		{"null int8", reflect.TypeFor[*int8](), "CASE WHEN 1=1 THEN t1.amount + t1.amount END", "SELECT NULL AS amount", reflect.TypeFor[*int64](), nil},
		{"negate int8", reflect.TypeFor[int8](), "CASE WHEN 1=1 THEN -t1.amount ELSE t1.amount END", "SELECT -128 AS amount", reflect.TypeFor[int64](), int64(128)},
		{"absolute int8", reflect.TypeFor[int8](), "CASE WHEN 1=1 THEN ABS(t1.amount) ELSE t1.amount END", "SELECT -128 AS amount", reflect.TypeFor[int64](), int64(128)},
		{"add uint8", reflect.TypeFor[uint8](), "CASE WHEN 1=1 THEN t1.amount + t1.amount ELSE t1.amount END", "SELECT 255 AS amount", reflect.TypeFor[uint64](), uint64(510)},
		{"sum uint8", reflect.TypeFor[uint8](), "CASE WHEN 1=1 THEN SUM(t1.amount) END", "SELECT 255 AS amount UNION ALL SELECT 255 AS amount", reflect.TypeFor[*uint64](), uint64(510)},
		{"add float32", reflect.TypeFor[float32](), "CASE WHEN 1=1 THEN t1.amount + t1.amount ELSE t1.amount END", "SELECT 3e38 AS amount", reflect.TypeFor[float64](), float64(6e38)},
		{"sum float32", reflect.TypeFor[float32](), "CASE WHEN 1=1 THEN SUM(t1.amount) END", "SELECT 3e38 AS amount UNION ALL SELECT 3e38 AS amount", reflect.TypeFor[*float64](), float64(6e38)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog, e := NewCatalog(Field{Name: "amount", Type: tc.source, Role: Measure})
			require.NoError(t, e)
			p, e := Compile("SELECT "+tc.expression+" AS value FROM $CubeSQL1 t1 LIMIT 1", catalog, 1, 1)
			require.NoError(t, e)
			require.Equal(t, tc.wantType, p.Columns[0].RuntimeType())
			query, args, e := p.Render([]Frame{{SQL: tc.frame}})
			require.NoError(t, e)
			dest := reflect.New(p.Columns[0].RuntimeType())
			require.NoError(t, db.QueryRow(query, args...).Scan(dest.Interface()))
			value := dest.Elem()
			if value.Kind() == reflect.Pointer {
				if tc.want == nil {
					require.True(t, value.IsNil())
					return
				}
				value = value.Elem()
			}
			require.Equal(t, tc.want, value.Interface())
		})
	}
	unsigned, e := NewCatalog(Field{Name: "amount", Type: reflect.TypeFor[uint64](), Role: Measure})
	require.NoError(t, e)
	_, e = Compile("SELECT CASE WHEN 1=1 THEN -t1.amount END AS value FROM $CubeSQL1 t1 LIMIT 1", unsigned, 1, 1)
	require.ErrorContains(t, e, "unsigned")
	_, e = Compile("SELECT CASE WHEN 1=1 THEN t1.amount - t1.amount END AS value FROM $CubeSQL1 t1 LIMIT 1", unsigned, 1, 1)
	require.ErrorContains(t, e, "subtract unsigned")
}
