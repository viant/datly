package sql

import "testing"

func TestNormalizeSelectProjection(t *testing.T) {
	useCases := []struct {
		name    string
		sqlText string
		expect  string
	}{
		{
			name:    "keeps unchanged projection",
			sqlText: "SELECT id, name FROM users",
			expect:  "SELECT id, name FROM users",
		},
		{
			name:    "removes allow nulls call",
			sqlText: "SELECT allow_nulls(name), id FROM users",
			expect:  "SELECT id FROM users",
		},
		{
			name:    "preserves distinct when normalizing",
			sqlText: "SELECT DISTINCT required(name) AS display_name, id FROM users",
			expect:  "SELECT DISTINCT name AS display_name, id FROM users",
		},
		{
			name:    "all removable items fall back to input",
			sqlText: "SELECT allow_nulls(name) FROM users",
			expect:  "SELECT allow_nulls(name) FROM users",
		},
		{
			name:    "unchanged projection returns input when nothing normalized",
			sqlText: "SELECT DISTINCT id, name FROM users",
			expect:  "SELECT DISTINCT id, name FROM users",
		},
		{
			name:    "parse failure returns input",
			sqlText: "SELECT FROM",
			expect:  "SELECT FROM",
		},
		{
			name:    "non select statement returns input",
			sqlText: "UPDATE users SET name = 'x'",
			expect:  "UPDATE users SET name = 'x'",
		},
		{
			name:    "union projection mismatch returns input",
			sqlText: "SELECT id, name FROM users UNION ALL SELECT id, name FROM users",
			expect:  "SELECT id, name FROM users UNION ALL SELECT id, name FROM users",
		},
		{
			name:    "no from returns input",
			sqlText: "SELECT 1",
			expect:  "SELECT 1",
		},
	}

	for _, useCase := range useCases {
		t.Run(useCase.name, func(t *testing.T) {
			if actual := normalizeSelectProjection(useCase.sqlText); actual != useCase.expect {
				t.Fatalf("unexpected normalized SQL: %q", actual)
			}
		})
	}
}

func TestSplitSelectionKind(t *testing.T) {
	kind, rest := splitSelectionKind("")
	if kind != "" || rest != "" {
		t.Fatalf("unexpected empty selection kind: %q %q", kind, rest)
	}

	kind, rest = splitSelectionKind("ALL tenant_id")
	if kind != "ALL" || rest != "tenant_id" {
		t.Fatalf("unexpected ALL selection kind: %q %q", kind, rest)
	}
	kind, rest = splitSelectionKind("DISTINCT tenant_id")
	if kind != "DISTINCT" || rest != "tenant_id" {
		t.Fatalf("unexpected DISTINCT selection kind: %q %q", kind, rest)
	}
	kind, rest = splitSelectionKind("ALL   ")
	if kind != "" || rest != "ALL   " {
		t.Fatalf("unexpected no-rest selection kind: %q %q", kind, rest)
	}
	kind, rest = splitSelectionKind("DISTINCT   ")
	if kind != "" || rest != "DISTINCT   " {
		t.Fatalf("unexpected no-rest DISTINCT selection kind: %q %q", kind, rest)
	}

	kind, rest = splitSelectionKind("DISTINCTX")
	if kind != "" || rest != "DISTINCTX" {
		t.Fatalf("unexpected non-match selection kind: %q %q", kind, rest)
	}
}
