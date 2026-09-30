package readerbuilder

import "testing"

func TestSourceProjectionAllIsNarrowAndCompilerOwned(t *testing.T) {
	for _, test := range []struct {
		sql  string
		want bool
	}{
		{"SELECT * FROM STUDIO_WIDE_60", true},
		{"SELECT t.* FROM STUDIO_WIDE_60 t", true},
		{"SELECT * FROM STUDIO_WIDE_60 WHERE ID = 1", true},
		{"SELECT ID, FIELD_01 FROM STUDIO_WIDE_60", false},
		{"SELECT *, COUNT(*) AS total FROM STUDIO_WIDE_60", false},
		{"SELECT * EXCEPT(FIELD_02) FROM STUDIO_WIDE_60", false},
		{"SELECT * FROM STUDIO_WIDE_60 w JOIN OTHER o ON o.ID = w.ID", false},
		{"SELECT * FROM (SELECT ID FROM STUDIO_WIDE_60) selected", false},
	} {
		if actual := selectsAllPhysicalColumns(test.sql); actual != test.want {
			t.Errorf("SQL %q: all=%v want %v", test.sql, actual, test.want)
		}
	}
}
