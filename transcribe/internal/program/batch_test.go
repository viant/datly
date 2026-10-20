package program

import "testing"

func TestBufferedBatchClassification(t *testing.T) {
	for _, tc := range []struct {
		description, source string
		expected            bool
	}{
		{"insert", `$dml.Insert($Input.Table,$Input.Rows,$Input.Size,$Output.Affected)`, true},
		{"insert conflict", `$dml.Insert($Input.Table,$Input.Rows,$Input.Size,$Output.Affected,"ON DUPLICATE KEY UPDATE weight=VALUES(weight)")`, true},
		{"invalid conflict reaches native validation", `$dml.Insert($Input.Table,$Input.Rows,$Input.Size,$Output.Affected,$Input.SQL)`, true},
		{"delete", `$dml.Delete($Input.Table,$Input.Rows,$Input.Size,$Output.Affected)`, true},
		{"delete conflict unsupported", `$dml.Delete($Input.Table,$Input.Rows,$Input.Size,$Output.Affected,"suffix")`, false},
		{"ordinary insert", `$dml.Insert($Input.Table,$Input.Rows)`, false},
	} {
		t.Run(tc.description, func(t *testing.T) {
			if got := BufferedBatch(tc.source); got != tc.expected {
				t.Fatalf("classification=%v want %v", got, tc.expected)
			}
		})
	}
}
