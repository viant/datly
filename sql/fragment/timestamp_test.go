package fragment

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/sqlparser"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

func TestTimestampSQLiteWholeSecondsAndExactNanoseconds(t *testing.T) {
	db := sqlite.New(t).DB
	_, err := db.Exec(`CREATE TABLE timestamps(value TEXT)`)
	if err != nil {
		t.Fatal(err)
	}
	renderer := New(nil).WithDialect(&info.Dialect{Product: database.Product{Name: "sqlite"}})
	seconds, err := renderer.TimestampSecondsUTC("COALESCE(t.value,t.value)")
	if err != nil {
		t.Fatal(err)
	}
	nano, err := renderer.TimestampNanoseconds("COALESCE(t.value,t.value)")
	if err != nil {
		t.Fatal(err)
	}
	cases := []string{
		"1970-01-01 00:00:00", "1969-12-31T23:59:59.999999999Z",
		"2026-01-01 00:00:00.000000001", "2026-01-01T00:00:00.000000002Z",
		"2026-01-01 00:00:00.999999999+00:00", "2026-01-01T05:30:00.123456789+05:30",
		"2025-12-31 17:00:00.123456789 -0700 MST", "2026-01-01 00:00:00 +0000 UTC",
		"2000-02-29T23:59:59.999999999-02:00", "0001-01-01T00:00:00Z",
	}
	layouts := []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05.999999999 -0700 MST"}
	for _, raw := range cases {
		t.Run(raw, func(t *testing.T) {
			_, err := db.Exec(`DELETE FROM timestamps`)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(`INSERT INTO timestamps VALUES(?)`, raw); err != nil {
				t.Fatal(err)
			}
			var expected time.Time
			for _, layout := range layouts {
				expected, err = time.Parse(layout, raw)
				if err == nil {
					break
				}
			}
			if err != nil {
				t.Fatalf("fixture time: %v", err)
			}
			var second string
			var ns int64
			if err = db.QueryRow("SELECT "+seconds+","+nano+" FROM timestamps t").Scan(&second, &ns); err != nil {
				t.Fatal(err)
			}
			if second != expected.UTC().Format("2006-01-02 15:04:05") || ns != int64(expected.Nanosecond()) {
				t.Fatalf("keys=(%s,%d) want=(%s,%d)", second, ns, expected.UTC().Format("2006-01-02 15:04:05"), expected.Nanosecond())
			}
		})
	}
	for _, raw := range []any{nil, "", "not-a-time", "0000-00-00 00:00:00", "2026-02-30 00:00:00", "2026-01-01T00:00:00.badZ", "2026-01-01T00:00:00+aa:bb", "2026-01-01T00:00:00Ztrailing", "2026-01-01T00:00:00+24:00", "2026-01-01T00:00:00+00:60"} {
		if _, err = db.Exec(`DELETE FROM timestamps`); err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(`INSERT INTO timestamps VALUES(?)`, raw); err != nil {
			t.Fatal(err)
		}
		var second sql.NullString
		var ns sql.NullInt64
		if err = db.QueryRow("SELECT "+seconds+","+nano+" FROM timestamps t").Scan(&second, &ns); err != nil {
			t.Fatal(err)
		}
		if second.Valid || ns.Valid {
			t.Fatalf("malformed %v generated keys=(%v,%v)", raw, second, ns)
		}
	}
}

func TestTimestampExpressionRejectsExecutableAndUnboundedInput(t *testing.T) {
	renderer := New(nil).WithDialect(&info.Dialect{Product: database.Product{Name: "sqlite"}})
	for _, source := range []string{"t.value;DELETE FROM timestamps", "t.value -- comment", "t.value /* comment */", "$Injected", "?", "CAST(t.value AS CHAR)", "NOW()", "COALESCE(t.value,'2026-01-01')", "t.value FROM timestamps", "t.value AS injected", "(SELECT value FROM timestamps)", "t.*", "COALESCE(t.a)", strings.Repeat("x", 513)} {
		if _, err := renderer.TimestampSecondsUTC(source); err == nil {
			t.Fatalf("accepted executable expression %q", source)
		}
	}
	if _, err := New(nil).TimestampSecondsUTC("t.value"); err == nil {
		t.Fatal("missing dialect accepted")
	}
	if _, err := New(nil).WithDialect(&info.Dialect{Product: database.Product{Name: "postgres"}}).TimestampNanoseconds("t.value"); err == nil {
		t.Fatal("unknown dialect accepted")
	}
}

func TestTimestampMySQLActualExpressionSyntax(t *testing.T) {
	renderer := New(nil).WithDialect(&info.Dialect{Product: database.Product{Name: "mysql"}})
	seconds, err := renderer.TimestampSecondsUTC("COALESCE(r.completed_at,r.updated_at,r.created_at)")
	if err != nil {
		t.Fatal(err)
	}
	nano, err := renderer.TimestampNanoseconds("r.created_at")
	if err != nil {
		t.Fatal(err)
	}
	source := fmt.Sprintf("SELECT %s AS second_key,%s AS nano_key FROM records r ORDER BY %s,%s,r.id", seconds, nano, seconds, nano)
	if _, err = sqlparser.ParseQuery(source, sqlparser.WithStructuralValidation()); err != nil {
		t.Fatalf("emitted MySQL expression syntax: %v", err)
	}
	for _, fragment := range []string{"DATE_SUB(", "INTERVAL (", " MINUTE)", " AS DATETIME)", " AS SIGNED)", " AS DECIMAL(20,9))"} {
		if !strings.Contains(source, fragment) {
			t.Fatalf("missing MySQL syntax %q", fragment)
		}
	}
	if strings.Contains(source, "DATETIME(") || strings.Contains(source, "GLOB") || strings.Contains(source, " AS INTEGER)") {
		t.Fatal("SQLite syntax emitted for MySQL")
	}
}
