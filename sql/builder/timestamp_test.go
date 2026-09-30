package builder

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

func TestBuilderTimestampKeysEvaluateWHEREAndORDERWithDialect(t *testing.T) {
	type input struct {
		AfterSecond string
		AfterNano   int64
	}
	source := `SELECT t.id FROM timestamps t WHERE (${View.TimestampSecondsUTC("t.value")} > $AfterSecond OR (${View.TimestampSecondsUTC("t.value")} = $AfterSecond AND ${View.TimestampNanoseconds("t.value")} > $AfterNano)) ORDER BY ${View.TimestampSecondsUTC("t.value")},${View.TimestampNanoseconds("t.value")},t.id`
	program, err := (sqltemplate.Compiler{Source: source, InputType: reflect.TypeFor[input]()}).Compile()
	if err != nil {
		t.Fatal(err)
	}
	for _, dialect := range []string{"sqlite", "mysql"} {
		query, err := NewBuilder().Build(context.Background(), WithBuilderTemplate(program), WithBuilderInput(reflect.ValueOf(input{AfterSecond: "2026-01-01 00:00:00", AfterNano: 1})), WithBuilderDialect(&info.Dialect{Product: database.Product{Name: dialect}, Placeholder: "?"}), WithBuilderParameterResolver(parameterResolver(map[string]any{"aftersecond": "2026-01-01 00:00:00", "afternano": int64(1)})))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(query.SQL, "$View") || strings.Contains(query.SQL, "TimestampSecondsUTC") || len(query.Args) != 3 {
			t.Fatalf("unevaluated timestamp query: args=%v", query.Args)
		}
		if dialect == "mysql" {
			if !strings.Contains(query.SQL, "DATE_SUB(") || !strings.Contains(query.SQL, "REGEXP_LIKE(") || strings.Contains(query.SQL, "GLOB") {
				t.Fatal("incorrect evaluated MySQL SQL")
			}
			continue
		}
		db := sqlite.New(t)
		if _, err = db.DB.Exec(`CREATE TABLE timestamps(id TEXT,value TEXT);INSERT INTO timestamps VALUES('old','2025-12-31T23:59:59.999999999Z'),('at','2026-01-01 00:00:00.000000001'),('next','2025-12-31 17:00:00.000000002-07:00'),('last','2026-01-01T00:00:00.999999999Z')`); err != nil {
			t.Fatal(err)
		}
		rows, err := db.DB.QueryContext(context.Background(), query.SQL, query.Args...)
		if err != nil {
			t.Fatal(err)
		}
		actual := []string{}
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			actual = append(actual, id)
		}
		if err = rows.Err(); err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()
		if !reflect.DeepEqual(actual, []string{"next", "last"}) {
			t.Fatalf("ordered IDs=%v", actual)
		}

	}
}
