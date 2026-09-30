//go:build cgo

package sql

import "github.com/mattn/go-sqlite3"

func init() {
	// Earlier applications using modernc's SQLite driver persisted time.Time
	// arguments with Time.String(): numeric offset followed by the zone name.
	// mattn otherwise silently reads those DATETIME values as zero time. Append
	// a read format while retaining its first (write) format and existing order.
	const legacyTimeString = "2006-01-02 15:04:05.999999999 -0700 MST"
	for _, format := range sqlite3.SQLiteTimestampFormats {
		if format == legacyTimeString {
			return
		}
	}
	sqlite3.SQLiteTimestampFormats = append(sqlite3.SQLiteTimestampFormats, legacyTimeString)
}
