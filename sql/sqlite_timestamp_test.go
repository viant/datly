//go:build cgo

package sql

import (
	"database/sql"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
)

func TestSQLiteLegacyTimestampCompatibility(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.Exec("CREATE TABLE timestamps (value DATETIME)"); err != nil {
		t.Fatal(err)
	}
	type useCase struct {
		desc   string
		input  any
		expect time.Time
		valid  bool
	}
	for _, tc := range []useCase{
		{"legacy UTC", "2026-01-02 00:00:00 +0000 UTC", time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), true},
		{"legacy offset and nanoseconds", "2026-01-02 03:04:05.123456789 +0200 EET", time.Date(2026, 1, 2, 1, 4, 5, 123456789, time.UTC), true},
		{"native offset format", "2026-01-02 00:00:00+00:00", time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), true},
		{"schema default format", "2026-01-02 00:00:00", time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), true},
		{"SQL NULL", nil, time.Time{}, false},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if _, err := db.Exec("DELETE FROM timestamps"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("INSERT INTO timestamps(value) VALUES (?)", tc.input); err != nil {
				t.Fatal(err)
			}
			var actual sql.NullTime
			if err := db.QueryRow("SELECT value FROM timestamps").Scan(&actual); err != nil {
				t.Fatal(err)
			}
			if actual.Valid != tc.valid || (tc.valid && !actual.Time.Equal(tc.expect)) {
				t.Fatalf("timestamp=%v expected=%v valid=%v", actual, tc.expect, tc.valid)
			}
		})
	}
	const originalWriteFormat = "2006-01-02 15:04:05.999999999-07:00"
	if sqlite3.SQLiteTimestampFormats[0] != originalWriteFormat {
		t.Fatal("timestamp write format changed")
	}
	value := time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.FixedZone("offset", 7200))
	if _, err := db.Exec("DELETE FROM timestamps"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("INSERT INTO timestamps(value) VALUES (?)", value); err != nil {
		t.Fatal(err)
	}
	var stored string
	if err := db.QueryRow("SELECT CAST(value AS TEXT) FROM timestamps").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if stored != value.Format(originalWriteFormat) {
		t.Fatalf("stored timestamp=%q", stored)
	}
}
