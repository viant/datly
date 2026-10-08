package criteria

import "testing"

func BenchmarkLinkedMethodValidation(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		method := Method{Name: "schema.upper"}
		if err := method.Validate(); err != nil {
			b.Fatal(err)
		}
	}
}
func TestLinkedMethodNameGrammar(t *testing.T) {
	for _, test := range []struct {
		name  string
		valid bool
	}{
		{"upper", true}, {"schema.upper", true}, {" upper ", true}, {"schema . upper", false}, {"`upper`", true}, {"schema.`upper`", true}, {`"upper"`, false}, {"'upper'", false}, {"a..b", false}, {"a(b)", false}, {"a;drop", false}, {"a--comment", false}, {"a/*comment*/", false}, {"1abc", false}, {"a_b", true}, {"a$", true}, {"α", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := Method{Name: test.name}
			err := m.Validate()
			if (err == nil) != test.valid {
				t.Fatalf("valid=%v err=%v", test.valid, err)
			}
		})
	}
}
