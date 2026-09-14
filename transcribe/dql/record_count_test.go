package dql

import "testing"

func TestRecordCountDeclarationMetadata(t *testing.T) {
	for _, tt := range []struct {
		option string
		valid  bool
	}{
		{".MinAllowedRecords(1).MaxAllowedRecords(3).ExpectedReturned(2)", true},
		{".MinAllowedRecords(-1)", false},
		{".ExpectedReturned('bad')", false},
		{".MaxAllowedRecords(1, 2)", false},
	} {
		t.Run(tt.option, func(t *testing.T) {
			component, err := parseComponentSource("example.com/demo", "Records", "#setting($_ = $route('/records', 'POST'))\n#define($_ = $Rows<[]int>(body/rows)"+tt.option+")\nSELECT 1")
			if !tt.valid {
				if err == nil {
					t.Fatal("invalid count accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			p := component.Parameters[0]
			if p.MinAllowedRecords == nil || *p.MinAllowedRecords != 1 || p.MaxAllowedRecords == nil || *p.MaxAllowedRecords != 3 || p.ExpectedReturned == nil || *p.ExpectedReturned != 2 {
				t.Fatalf("metadata=%+v", p)
			}
		})
	}
}
