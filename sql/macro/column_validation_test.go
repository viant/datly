package macro

import (
	"testing"
)

func TestParentKeyColumnValidation(t *testing.T) {
	for _, value := range []string{"id", "p.vendor_id", "schema.p.id", "_key", "p.id_2", "`p`.`vendor_id`"} {
		if err := validateColumn(value); err != nil {
			t.Errorf("valid column %q: %v", value, err)
		}
	}
	for _, value := range []string{"", "1", "true", "false", "null", "'id'", "p.*", "id AS alias", "id alias", "id, other", "id + 1", "COALESCE(id, 0)", "id OR 1=1", "id; DELETE FROM rows", "id -- comment", "id /* comment */", "p..id", "p.", "`unterminated", "id\x00"} {
		if err := validateColumn(value); err == nil {
			t.Errorf("invalid column accepted: %q", value)
		}
		if _, _, err := (ParentKeyExpander{}).Expand(ParentKeyCall{Method: "ColIn", Columns: []string{value}}); err == nil {
			t.Errorf("direct expansion accepted invalid column: %q", value)
		}
	}
}

func BenchmarkParentKeyColumnValidation(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := validateColumn("p.vendor_id"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParentKeyMacroExpansion(b *testing.B) {
	const source = `SELECT id FROM product p WHERE 1=1 $View.ParentCompositeJoinOn("AND","p.tenant","p.vendor_id")`
	rows := [][]interface{}{{1, 7}, {2, 8}}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, _, err := ExpandParentKeyCalls(source, nil, nil, rows); err != nil {
			b.Fatal(err)
		}
	}
}
