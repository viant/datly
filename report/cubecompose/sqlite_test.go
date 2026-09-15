package cubecompose

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/testharness/sqlite"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"reflect"
	"strings"
	"testing"
)

func TestCompositionSQLiteMatrix(t *testing.T) {
	type row struct {
		ID    int64    `sqlx:"id"`
		Value *float64 `sqlx:"value"`
	}
	value := func(v float64) *float64 { return &v }
	for _, tc := range []struct {
		name, sql string
		periods   []string
		want      []row
	}{
		{"single cube", `SELECT t1.id, t1.amount AS value FROM $CubeSQL1 AS t1 ORDER BY t1.id`, []string{"previous"}, []row{{1, value(100)}, {2, value(80)}, {3, value(30)}}},
		{"inner join delta", `SELECT t1.id, t1.amount-t2.amount AS value FROM $CubeSQL1 AS t1 JOIN $CubeSQL2 AS t2 ON t1.id=t2.id ORDER BY t1.id`, []string{"previous", "current"}, []row{{1, value(60)}, {3, value(5)}}},
		{"left join nullable", `SELECT t1.id, t2.amount AS value FROM $CubeSQL1 AS t1 LEFT JOIN $CubeSQL2 AS t2 ON t1.id=t2.id ORDER BY t1.id`, []string{"previous", "current"}, []row{{1, value(40)}, {2, nil}, {3, value(25)}}},
		{"three cubes", `SELECT t1.id, t1.amount+COALESCE(t2.amount,0)+COALESCE(t3.amount,0) AS value FROM $CubeSQL1 AS t1 LEFT JOIN $CubeSQL2 AS t2 ON t1.id=t2.id LEFT JOIN $CubeSQL3 AS t3 ON t1.id=t3.id ORDER BY t1.id`, []string{"previous", "current", "benchmark"}, []row{{1, value(150)}, {2, value(100)}, {3, value(55)}}},
		{"bound literals and ranking", `SELECT t1.id, t1.amount-COALESCE(t2.amount,0) AS value FROM $CubeSQL1 AS t1 LEFT JOIN $CubeSQL2 AS t2 ON t1.id=t2.id WHERE t1.amount-COALESCE(t2.amount,0)>10 ORDER BY value DESC LIMIT 1`, []string{"previous", "current"}, []row{{2, value(80)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := sqlite.New(t)
			if err := h.ExecStatements(ctx, `CREATE TABLE facts(id INTEGER, period TEXT, amount REAL)`, `INSERT INTO facts VALUES(1,'previous',100),(2,'previous',80),(3,'previous',30),(1,'current',40),(3,'current',25),(1,'benchmark',10),(2,'benchmark',20)`); err != nil {
				t.Fatal(err)
			}
			catalog, err := NewCatalog(Field{Name: "id", Type: reflect.TypeFor[int64](), Role: Dimension}, Field{Name: "amount", Type: reflect.TypeFor[float64](), Role: Measure})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := Compile(tc.sql, catalog, len(tc.periods), 100)
			if err != nil {
				t.Fatal(err)
			}
			frames := make([]Frame, len(tc.periods))
			for i, period := range tc.periods {
				frames[i] = Frame{SQL: `SELECT id,SUM(amount) AS amount FROM facts WHERE period=? GROUP BY id`, Args: []any{period}}
			}
			query, args, err := plan.Render(frames)
			if err != nil {
				t.Fatal(err)
			}
			h.AssertQuery(t, ctx, sqlite.Query{SQL: query, Args: args}, tc.want)
		})
	}
}

func TestCompositionSQLiteEightFrames(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	catalog, err := NewCatalog(Field{Name: "id", Type: reflect.TypeFor[int64](), Role: Dimension})
	if err != nil {
		t.Fatal(err)
	}
	var joins []string
	frames := make([]Frame, 8)
	for i := range frames {
		frames[i] = Frame{SQL: "SELECT ? AS id", Args: []any{int64(7)}}
		if i > 0 {
			joins = append(joins, fmt.Sprintf("JOIN $CubeSQL%d AS t%d ON t1.id=t%d.id", i+1, i+1, i+1))
		}
	}
	plan, err := Compile("SELECT t1.id FROM $CubeSQL1 AS t1 "+strings.Join(joins, " "), catalog, 8, 100)
	if err != nil {
		t.Fatal(err)
	}
	query, args, err := plan.Render(frames)
	if err != nil {
		t.Fatal(err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: query, Args: args}, []struct {
		ID int64 `sqlx:"id"`
	}{{ID: 7}})
}

func TestCompositionSQLiteMultiKeyJoinPreventsCrossMatch(t *testing.T) {
	type row struct {
		ID     int64    `sqlx:"id"`
		Region string   `sqlx:"region"`
		Value  *float64 `sqlx:"value"`
	}
	value := func(v float64) *float64 { return &v }
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx,
		`CREATE TABLE facts(id INTEGER, region TEXT, period TEXT, amount REAL)`,
		`INSERT INTO facts VALUES
			(1,'EU','previous',100),
			(1,'US','previous',80),
			(1,'EU','current',40),
			(1,'US','current',25)`,
	); err != nil {
		t.Fatal(err)
	}
	catalog, err := NewCatalog(
		Field{Name: "id", Type: reflect.TypeFor[int64](), Role: Dimension},
		Field{Name: "region", Type: reflect.TypeFor[string](), Role: Dimension},
		Field{Name: "amount", Type: reflect.TypeFor[float64](), Role: Measure},
	)
	if err != nil {
		t.Fatal(err)
	}
	frames := []Frame{
		{SQL: `SELECT id,region,SUM(amount) AS amount FROM facts WHERE period=? GROUP BY id,region`, Args: []any{"previous"}},
		{SQL: `SELECT id,region,SUM(amount) AS amount FROM facts WHERE period=? GROUP BY id,region`, Args: []any{"current"}},
	}
	oneKey, err := Compile(`SELECT t1.id, t1.region, t1.amount-t2.amount AS value
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.id=t2.id
 ORDER BY t1.region, value`, catalog, 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	query, args, err := oneKey.Render(frames)
	if err != nil {
		t.Fatal(err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: query, Args: args}, []row{{1, "EU", value(60)}, {1, "EU", value(75)}, {1, "US", value(40)}, {1, "US", value(55)}})

	twoKeys, err := Compile(`SELECT t1.id, t1.region, t1.amount-t2.amount AS value
 FROM $CubeSQL1 AS t1
 JOIN $CubeSQL2 AS t2 ON t1.id=t2.id AND t1.region=t2.region
 ORDER BY t1.region`, catalog, 2, 100)
	if err != nil {
		t.Fatal(err)
	}
	query, args, err = twoKeys.Render(frames)
	if err != nil {
		t.Fatal(err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: query, Args: args}, []row{{1, "EU", value(60)}, {1, "US", value(55)}})
}

func TestCompositionMapsContractNamesToSQLColumns(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	catalog, err := NewCatalog(Field{Name: "AccountID", SQLName: "account_id", Type: reflect.TypeOf(int64(0)), Role: Dimension}, Field{Name: "TotalSpend", SQLName: "total_spend", Type: reflect.TypeOf(float64(0)), Role: Measure})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := Compile("SELECT t1.AccountID, t1.TotalSpend FROM $CubeSQL1 AS t1", catalog, 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	query, args, err := plan.Render([]Frame{{SQL: "SELECT 1 AS account_id, 150.0 AS total_spend"}})
	if err != nil {
		t.Fatal(err)
	}
	h.AssertQuery(t, ctx, sqlite.Query{SQL: query, Args: args}, []struct {
		AccountID  int64   `sqlx:"AccountID"`
		TotalSpend float64 `sqlx:"TotalSpend"`
	}{{1, 150}})
}
