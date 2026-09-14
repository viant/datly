package sql

import (
	"testing"

	"github.com/viant/datly/spec"
)

func TestViewControlHelpers(t *testing.T) {
	t.Run("apply view controls nil and empty passthrough", func(t *testing.T) {
		if actual := applyViewControls("", nil, false); actual != "" {
			t.Fatalf("unexpected empty passthrough: %q", actual)
		}
		if actual := applyViewControls("SELECT * FROM users", nil, false); actual != "SELECT * FROM users" {
			t.Fatalf("unexpected nil-controls passthrough: %q", actual)
		}
	})

	t.Run("append limit nil passthrough", func(t *testing.T) {
		if actual := appendLimit("SELECT * FROM users", nil); actual != "SELECT * FROM users" {
			t.Fatalf("unexpected nil limit append: %q", actual)
		}
	})

	t.Run("append limit before offset", func(t *testing.T) {
		limit := 10
		if actual := appendLimit("SELECT * FROM users OFFSET 5", &limit); actual != "SELECT * FROM users LIMIT 10 OFFSET 5" {
			t.Fatalf("unexpected limit append before offset: %q", actual)
		}
	})

	t.Run("prepare executable sql applies authored controls", func(t *testing.T) {
		limit := 5
		offset := 2
		controls := &spec.ViewControls{OrderBy: "name", Limit: &limit, Offset: &offset}
		actual := PrepareExecutableSQL("SELECT * FROM users", controls)
		expect := "SELECT * FROM users ORDER BY name LIMIT 5 OFFSET 2"
		if actual != expect {
			t.Fatalf("unexpected prepared executable SQL:\nactual: %q\nexpect: %q", actual, expect)
		}
	})

	t.Run("prepare executable sql skips appended pagination when token used", func(t *testing.T) {
		limit := 5
		offset := 2
		controls := &spec.ViewControls{Limit: &limit, Offset: &offset}
		actual := PrepareExecutableSQL("SELECT * FROM users $PAGINATION", controls)
		expect := "SELECT * FROM users  LIMIT 5 OFFSET 2"
		if actual != expect {
			t.Fatalf("unexpected token pagination SQL:\nactual: %q\nexpect: %q", actual, expect)
		}
	})

	t.Run("pagination token skips protected SQL text", func(t *testing.T) {
		limit := 5
		controls := &spec.ViewControls{Limit: &limit}
		actual := PrepareExecutableSQL("SELECT '$PAGINATION' AS marker FROM users $PAGINATION -- $PAGINATION", controls)
		expect := "SELECT '$PAGINATION' AS marker FROM users  LIMIT 5 -- $PAGINATION"
		if actual != expect {
			t.Fatalf("unexpected protected pagination SQL:\nactual: %q\nexpect: %q", actual, expect)
		}
	})

	t.Run("insert clause appends when no before clauses provided", func(t *testing.T) {
		if actual := insertClause("SELECT * FROM users", " WHERE active = 1"); actual != "SELECT * FROM users WHERE active = 1" {
			t.Fatalf("unexpected appended clause: %q", actual)
		}
	})
}
