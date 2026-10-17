package generate

import "testing"

func TestReplaceQualifiedContractBase(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"github.com/example-module/contracts.Output", "linked.Output"},
		{"*github.com/example-module/contracts.Output", "*linked.Output"},
		{"[]*github.com/example-module/contracts.Output", "[]*linked.Output"},
		{"[2]*github.com/example-module/contracts.Output", "[2]*linked.Output"},
		{"(github.com/example-module/contracts.Output)", "(linked.Output)"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			got, err := replaceContractBase(tc.input, "linked.Output")
			if err != nil || got != tc.want {
				t.Fatalf("got %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestReplaceContractBaseRejectsUnsupportedForms(t *testing.T) {
	for _, expression := range []string{"Box[T]", "github.com/example-module/contracts.Box[T]", "map[string]Output", "chan Output", "func() Output", "struct{ Value Output }", "interface{ Read() Output }"} {
		t.Run(expression, func(t *testing.T) {
			if got, err := replaceContractBase(expression, "linked.Output"); err == nil {
				t.Fatalf("accepted %q as %q", expression, got)
			}
		})
	}
}
