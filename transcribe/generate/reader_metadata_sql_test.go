package generate

import "testing"

func TestReaderSQLMaterializesGrouping(t *testing.T) {
	for _, tc := range []struct {
		name, query, asset string
		want               bool
	}{
		{"exact", "SELECT ID FROM records", "SELECT ID FROM records", true},
		{"grouped original", "SELECT * FROM (SELECT ID FROM records) records", "( SELECT ID FROM records )", true},
		{"outer filter", "SELECT * FROM (SELECT ID FROM records) records WHERE ID > 0", "( SELECT ID FROM records )", false},
		{"asset suffix", "SELECT * FROM (SELECT ID FROM records) records", "( SELECT ID FROM records ) WHERE ID > 0", false},
		{"inner filter", "SELECT * FROM (SELECT ID FROM records WHERE ID > 0) records", "( SELECT ID FROM records )", false},
		{"outer limit", "SELECT * FROM (SELECT ID FROM records) records LIMIT 1", "( SELECT ID FROM records )", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := readerSQLMaterializes(tc.query, tc.asset); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
