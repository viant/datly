package sql

import (
	"testing"

	"github.com/viant/datly/spec"
)

func TestPrepareExecutableSQL_AppliesMissingViewControls(t *testing.T) {
	limit := 25
	offset := 10
	actual := PrepareExecutableSQL("SELECT id, name FROM users", &spec.ViewControls{
		OrderBy: "name DESC",
		Limit:   &limit,
		Offset:  &offset,
	})
	expected := "SELECT id, name FROM users ORDER BY name DESC LIMIT 25 OFFSET 10"
	if actual != expected {
		t.Fatalf("expected %q, got %q", expected, actual)
	}
}

func TestPrepareExecutableSQL_PreservesExistingClauses(t *testing.T) {
	limit := 25
	offset := 10
	actual := PrepareExecutableSQL("SELECT id, name FROM users ORDER BY id LIMIT 5 OFFSET 2", &spec.ViewControls{
		OrderBy: "name DESC",
		Limit:   &limit,
		Offset:  &offset,
	})
	expected := "SELECT id, name FROM users ORDER BY id LIMIT 5 OFFSET 2"
	if actual != expected {
		t.Fatalf("expected existing clauses preserved, got %q", actual)
	}
}

func TestPrepareExecutableSQL_InsertsOrderByBeforeExistingLimit(t *testing.T) {
	actual := PrepareExecutableSQL("SELECT id, name FROM users LIMIT 5", &spec.ViewControls{
		OrderBy: "name DESC",
	})
	expected := "SELECT id, name FROM users ORDER BY name DESC LIMIT 5"
	if actual != expected {
		t.Fatalf("expected %q, got %q", expected, actual)
	}
}

func TestPrepareExecutableSQL_InsertsLimitBeforeExistingOffset(t *testing.T) {
	limit := 25
	actual := PrepareExecutableSQL("SELECT id, name FROM users OFFSET 10", &spec.ViewControls{
		Limit: &limit,
	})
	expected := "SELECT id, name FROM users LIMIT 25 OFFSET 10"
	if actual != expected {
		t.Fatalf("expected %q, got %q", expected, actual)
	}
}

func TestPrepareExecutableSQL_InsertsOrderByAndLimitBeforeExistingOffset(t *testing.T) {
	limit := 25
	actual := PrepareExecutableSQL("SELECT id, name FROM users OFFSET 10", &spec.ViewControls{
		OrderBy: "name DESC",
		Limit:   &limit,
	})
	expected := "SELECT id, name FROM users ORDER BY name DESC LIMIT 25 OFFSET 10"
	if actual != expected {
		t.Fatalf("expected %q, got %q", expected, actual)
	}
}

func TestPrepareExecutableSQL_ReplacesPaginationTokenWithoutOuterDuplicatePagination(t *testing.T) {
	limit := 2
	offset := 1
	actual := PrepareExecutableSQL("SELECT id, name FROM (SELECT * FROM users ORDER BY id $PAGINATION) t", &spec.ViewControls{
		Limit:  &limit,
		Offset: &offset,
	})
	expected := "SELECT id, name FROM (SELECT * FROM users ORDER BY id  LIMIT 2 OFFSET 1) t"
	if actual != expected {
		t.Fatalf("expected %q, got %q", expected, actual)
	}
}

func TestNormalizeAuthoredSQL_EmptyString(t *testing.T) {
	if actual := NormalizeAuthoredSQL("   "); actual != "" {
		t.Fatalf("expected empty normalized SQL, got %q", actual)
	}
}
