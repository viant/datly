package fragment

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

// This opt-in test accepts only an explicitly supplied disposable fixture
// description. It creates a connection-local temporary table and prints no DSN.
func TestTimestampKeysActualMySQL(t *testing.T) {
	path := os.Getenv("DATLY_TIMESTAMP_MYSQL_CLIENT_FILE")
	if path == "" {
		t.Skip("disposable MySQL fixture description not supplied")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("read disposable MySQL fixture description")
	}
	var cfg struct {
		User, Password, Database string
		Port                     int
	}
	if err = json.Unmarshal(body, &cfg); err != nil {
		t.Fatal("decode disposable MySQL fixture description")
	}
	if cfg.Port <= 0 || cfg.User == "" || cfg.Database == "" {
		t.Fatal("incomplete disposable MySQL fixture")
	}
	driverConfig := mysql.NewConfig()
	driverConfig.User = cfg.User
	driverConfig.Passwd = cfg.Password
	driverConfig.DBName = cfg.Database
	driverConfig.Net = "tcp"
	driverConfig.Addr = "127.0.0.1:" + strconv.Itoa(cfg.Port)
	db, err := sql.Open("mysql", driverConfig.FormatDSN())
	if err != nil {
		t.Fatal("open disposable MySQL fixture")
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	connection, err := db.Conn(ctx)
	if err != nil {
		t.Fatal("connect disposable MySQL fixture")
	}
	defer connection.Close()
	if _, err = connection.ExecContext(ctx, `CREATE TEMPORARY TABLE timestamp_sort_fixture(value VARCHAR(100))`); err != nil {
		t.Fatal(err)
	}
	renderer := New(nil).WithDialect(&info.Dialect{Product: database.Product{Name: "mysql"}})
	seconds, err := renderer.TimestampSecondsUTC("t.value")
	if err != nil {
		t.Fatal(err)
	}
	nanos, err := renderer.TimestampNanoseconds("t.value")
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		raw    any
		second string
		ns     int64
		valid  bool
	}{
		{"1970-01-01 00:00:00", "1970-01-01 00:00:00", 0, true},
		{"1969-12-31T23:59:59.999999999Z", "1969-12-31 23:59:59", 999999999, true},
		{"2026-01-01 00:00:00.000000001", "2026-01-01 00:00:00", 1, true},
		{"2026-01-01T00:00:00.000000002Z", "2026-01-01 00:00:00", 2, true},
		{"2026-01-01T05:30:00.123456789+05:30", "2026-01-01 00:00:00", 123456789, true},
		{"2025-12-31 17:00:00.123456789 -0700 MST", "2026-01-01 00:00:00", 123456789, true},
		{"2000-02-29T23:59:59.999999999-02:00", "2000-03-01 01:59:59", 999999999, true},
		{nil, "", 0, false}, {"not-a-time", "", 0, false}, {"0000-00-00 00:00:00", "", 0, false},
		{"2026-02-30 00:00:00", "", 0, false}, {"2026-01-01T00:00:00.badZ", "", 0, false},
		{"2026-01-01T00:00:00+24:00", "", 0, false}, {"2026-01-01T00:00:00+00:60", "", 0, false},
	}
	for _, tc := range cases {
		if _, err = connection.ExecContext(ctx, `DELETE FROM timestamp_sort_fixture`); err != nil {
			t.Fatal(err)
		}
		if _, err = connection.ExecContext(ctx, `INSERT INTO timestamp_sort_fixture VALUES(?)`, tc.raw); err != nil {
			t.Fatal(err)
		}
		var second sql.NullString
		var ns sql.NullInt64
		if err = connection.QueryRowContext(ctx, "SELECT "+seconds+","+nanos+" FROM timestamp_sort_fixture t").Scan(&second, &ns); err != nil {
			t.Fatal(err)
		}
		if second.Valid != tc.valid || ns.Valid != tc.valid || tc.valid && (second.String != tc.second || ns.Int64 != tc.ns) {
			t.Fatalf("source=%v keys=(%v,%v) want=(%s,%d,valid=%t)", tc.raw, second, ns, tc.second, tc.ns, tc.valid)
		}
	}
}
