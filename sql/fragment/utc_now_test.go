package fragment

import (
	"reflect"
	"testing"

	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
)

func TestUTCNowUsesDatabaseClockWithoutBinding(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		fails      bool
	}{
		{"mysql", "UTC_TIMESTAMP()", false},
		{"sqlite", "DATETIME('now')", false},
		{"sqlite3", "DATETIME('now')", false},
		{"other", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bindings := &Bindings{}
			bindings.Append("prior")
			ctx := New(bindings).WithDialect(&info.Dialect{Product: database.Product{Name: tc.name}})
			got, err := ctx.UTCNow()
			if (err != nil) != tc.fails || got != tc.want || !reflect.DeepEqual(bindings.Args(), []any{"prior"}) {
				t.Fatalf("expression=%q error=%v bindings=%v", got, err, bindings.Args())
			}
		})
	}
	if _, err := New(&Bindings{}).UTCNow(); err == nil {
		t.Fatal("missing dialect selected a clock")
	}
}
