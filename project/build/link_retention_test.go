package build

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLinkCandidateRetainedTypeFor(t *testing.T) {
	for _, tc := range []struct {
		name, declaration string
		a, b              bool
	}{
		{"named", "var Keep = r.TypeFor[A]()", true, false},
		{"blank", "var _ = r.TypeFor[A]()", false, false},
		{"parenthesized", "var Keep = (r.TypeFor[A]())", true, false},
		{"blank_first", "var _, Keep = r.TypeFor[A](), r.TypeFor[B]()", false, true},
		{"blank_last", "var Keep, _ = r.TypeFor[A](), r.TypeFor[B]()", true, false},
		{"both_named", "var One, Two = r.TypeFor[A](), r.TypeFor[B]()", true, true},
		{"nested", "var Keep = func() int { _ = r.TypeFor[A](); return 1 }()", false, false},
		{"init", "func init() { _ = r.TypeFor[A]() }", false, false},
		{"unequal", "var One, Two = pair(r.TypeFor[A]())", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "types.go")
			if err := os.WriteFile(path, []byte("package sample\nimport r \"reflect\"\ntype A struct{}\ntype B struct{}\n"+tc.declaration), 0600); err != nil {
				t.Fatal(err)
			}
			candidate := &linkCandidate{holders: map[string]bool{}, reachable: map[string]bool{}}
			if err := candidate.inspect(path); err != nil {
				t.Fatal(err)
			}
			if candidate.reachable["A"] != tc.a || candidate.reachable["B"] != tc.b {
				t.Fatalf("retained types = %v; want A=%v B=%v", candidate.reachable, tc.a, tc.b)
			}
		})
	}
}
