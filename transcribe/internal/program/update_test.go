package program

import "testing"

func TestBufferedUpdateClassification(t *testing.T) {
	for _, source := range []string{`$dml.Update($Input.Table,$Input.Row)`, `$dml.Update($Input.Params.Table,$Input.Params.Row)`} {
		if !BufferedUpdate(source) {
			t.Fatalf("canonical Update rejected: %s", source)
		}
	}
	for _, source := range []string{`$dml.Update($Table,$Row)`, `$dml.Update("records",$Input.Row)`, `$dml.Update($Input.Table,$Output.Row)`, `$dml.Update($Input.Table,$Input.Row,1)`, `$dml.Update($Input.Table)`, `$dml.Update($Input.Table,$Input.Row.Method())`, `$dml.Update($Input.Table,$Input.Row) $dml.Update($Input.Table,$Input.Row)`} {
		if BufferedUpdate(source) {
			t.Fatalf("legacy/dynamic Update promoted: %s", source)
		}
	}
}
